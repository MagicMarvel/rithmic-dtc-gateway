//! Rithmic futures-option collection and a small HuntingFlow-style analytics API.
//!
//! The dealer-positioning numbers are estimates. Rithmic supplies trades, quotes,
//! reference data and OI; it does not identify customer/dealer inventory.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    env,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

const REPLAY_HISTORY_LIMIT: usize = 1_200;
// Two days of one-minute, strike-level frames keep the dashboard heatmap
// genuinely historical without retaining every three-second full snapshot.
const HEATMAP_HISTORY_LIMIT: usize = 2 * 24 * 60;
const HEATMAP_BUCKET_US: i64 = 60_000_000;
const MOMENTUM_LOOKBACK_US: i64 = 5 * 60 * 1_000_000;
const MOMENTUM_OVERRIDE_POINTS: f64 = 5.0;
// 18,000 compact points cover 15 hours at the default three-second cadence:
// enough for twenty 30-minute samples plus their complete 60-minute horizons.
const VALIDATION_HISTORY_LIMIT: usize = 18_000;
const TRIGGER_HOLD_US: i64 = 60 * 60 * 1_000_000;
const WALL_DIRECTION_BUFFER_POINTS: f64 = 1.0;
const WALL_RETAIN_STRENGTH_FRACTION: f64 = 0.60;
const WALL_CHALLENGER_CONFIRM_US: i64 = 2 * 60 * 1_000_000;

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
    time::{self, Duration},
};

pub use crate::market_gateway::{OptionContract, OptionType};
use crate::{
    market_data,
    market_gateway::{
        HistoricalRecord, HistoricalRequest, HistoryDataClient, MarketDataClient, MarketEvent,
    },
};

const UNDERLYING_ID: u32 = 0xf000_0000;

#[derive(Debug, Clone)]
pub struct OptionsConfig {
    pub root: String,
    pub exchange: String,
    pub underlying_symbol: String,
    pub listen_addr: String,
    pub max_expirations: usize,
    pub strikes_each_side: usize,
    pub max_contracts: usize,
    pub center_price: Option<f64>,
    pub risk_free_rate: f64,
    pub snapshot_secs: u64,
    pub data_dir: PathBuf,
}

impl OptionsConfig {
    pub fn from_env() -> Result<Self, String> {
        let root = env::var("RITHMIC_OPTIONS_ROOT").unwrap_or_else(|_| "ES".to_owned());
        let exchange = env::var("RITHMIC_OPTIONS_EXCHANGE").unwrap_or_else(|_| "CME".to_owned());
        let underlying_symbol = env::var("RITHMIC_OPTIONS_UNDERLYING_SYMBOL")
            .or_else(|_| env::var("RITHMIC_DTC_SYMBOL"))
            .map_err(|_| {
                "set RITHMIC_OPTIONS_UNDERLYING_SYMBOL to the traded futures contract".to_owned()
            })?;
        Ok(Self {
            root,
            exchange,
            underlying_symbol,
            listen_addr: env::var("OPTIONS_HTTP_LISTEN_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:11100".to_owned()),
            max_expirations: env_usize("OPTIONS_MAX_EXPIRATIONS", 1),
            strikes_each_side: env_usize("OPTIONS_STRIKES_EACH_SIDE", 12),
            max_contracts: env_usize("OPTIONS_MAX_CONTRACTS", 64),
            center_price: env::var("OPTIONS_CENTER_PRICE")
                .ok()
                .and_then(|v| v.parse().ok()),
            risk_free_rate: env_f64("OPTIONS_RISK_FREE_RATE", 0.05),
            snapshot_secs: env_usize("OPTIONS_SNAPSHOT_SECS", 3).max(1) as u64,
            data_dir: env::var("OPTIONS_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("data/options")),
        })
    }

    pub fn markets_from_env() -> Result<Vec<Self>, String> {
        let base = Self::from_env()?;
        // Additional markets are opt-in. Initializing unrelated option boards
        // serially can delay the configured primary market and makes one slow
        // entitlement path hold every market's snapshot loop hostage.
        let default = format!("{}:{}:{}", base.root, base.underlying_symbol, base.exchange);
        let value = env::var("RITHMIC_OPTIONS_MARKETS").unwrap_or(default);
        value
            .split(',')
            .filter(|item| !item.trim().is_empty())
            .map(|item| {
                let parts: Vec<_> = item.trim().split(':').collect();
                if parts.len() != 3 {
                    return Err(format!(
                        "invalid RITHMIC_OPTIONS_MARKETS entry {item:?}; expected ROOT:SYMBOL:EXCHANGE"
                    ));
                }
                let mut config = base.clone();
                config.root = parts[0].to_ascii_uppercase();
                config.underlying_symbol = parts[1].to_ascii_uppercase();
                config.exchange = parts[2].to_ascii_uppercase();
                if config.root != base.root {
                    config.data_dir = base.data_dir.join(config.root.to_ascii_lowercase());
                    config.center_price = None;
                }
                Ok(config)
            })
            .collect()
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &f64| v.is_finite())
        .unwrap_or(default)
}

fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}
fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

#[derive(Debug, Clone, Default)]
struct ContractState {
    bid: Option<f64>,
    ask: Option<f64>,
    oi: u64,
    buy_volume: f64,
    sell_volume: f64,
    unknown_volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrikeAnalytics {
    pub expiration: String,
    pub strike: f64,
    #[serde(skip_serializing_if = "is_zero_u64", default)]
    pub call_oi: u64,
    #[serde(skip_serializing_if = "is_zero_u64", default)]
    pub put_oi: u64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub call_volume: f64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub put_volume: f64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub call_buy_volume: f64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub call_sell_volume: f64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub call_unknown_volume: f64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub put_buy_volume: f64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub put_sell_volume: f64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub put_unknown_volume: f64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub call_net_flow: f64,
    #[serde(skip_serializing_if = "is_zero_f64", default)]
    pub put_net_flow: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_iv: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_iv: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_gex: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_gex: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_gex: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_flow_gex: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_flow_gex: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_gex: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsSnapshot {
    pub as_of_us: i64,
    pub session_started_us: i64,
    pub underlying: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub underlying_price: Option<f64>,
    pub contracts: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_gex: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_gex: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zero_gamma: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_wall: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_wall: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hvl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_trigger: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_trigger: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_zero_gamma: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_call_wall: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_put_wall: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_hvl: Option<f64>,
    pub regime: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub momentum_5m: Option<f64>,
    #[serde(default)]
    pub playbook: String,
    pub model: String,
    pub strikes: Vec<StrikeAnalytics>,
}

impl AnalyticsSnapshot {
    fn empty(config: &OptionsConfig) -> Self {
        let now = now_us();
        Self {
            as_of_us: now,
            session_started_us: now,
            underlying: config.underlying_symbol.clone(),
            underlying_price: None,
            contracts: 0,
            net_gex: None,
            flow_gex: None,
            confidence: None,
            zero_gamma: None,
            call_wall: None,
            put_wall: None,
            hvl: None,
            call_trigger: None,
            put_trigger: None,
            flow_zero_gamma: None,
            flow_call_wall: None,
            flow_put_wall: None,
            flow_hvl: None,
            regime: "awaiting-data".to_owned(),
            momentum_5m: None,
            playbook: "awaiting-momentum".to_owned(),
            model:
                "Black-76; calls dealer-long / puts dealer-short proxy; front1-strength25-sticky60-near-v5-momentum5-walls2"
                    .to_owned(),
            strikes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct HeatmapStrike {
    strike: f64,
    net_gex: Option<f64>,
    flow_gex: Option<f64>,
    confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct HeatmapExpiry {
    expiration: String,
    strikes: Vec<HeatmapStrike>,
}

#[derive(Debug, Clone, Serialize)]
struct HeatmapPoint {
    as_of_us: i64,
    underlying_price: f64,
    call_wall: Option<f64>,
    put_wall: Option<f64>,
    flow_call_wall: Option<f64>,
    flow_put_wall: Option<f64>,
    expirations: Vec<HeatmapExpiry>,
}

impl TryFrom<&AnalyticsSnapshot> for HeatmapPoint {
    type Error = ();

    fn try_from(snapshot: &AnalyticsSnapshot) -> Result<Self, Self::Error> {
        let underlying_price = snapshot.underlying_price.ok_or(())?;
        let mut by_expiry: BTreeMap<String, Vec<HeatmapStrike>> = BTreeMap::new();
        for row in &snapshot.strikes {
            by_expiry
                .entry(row.expiration.clone())
                .or_default()
                .push(HeatmapStrike {
                    strike: row.strike,
                    net_gex: row.net_gex,
                    flow_gex: row.flow_gex,
                    confidence: row.confidence,
                });
        }
        if by_expiry.is_empty() {
            return Err(());
        }
        Ok(Self {
            as_of_us: snapshot.as_of_us,
            underlying_price,
            call_wall: snapshot.call_wall,
            put_wall: snapshot.put_wall,
            flow_call_wall: snapshot.flow_call_wall,
            flow_put_wall: snapshot.flow_put_wall,
            expirations: by_expiry
                .into_iter()
                .map(|(expiration, strikes)| HeatmapExpiry {
                    expiration,
                    strikes,
                })
                .collect(),
        })
    }
}

fn push_heatmap_point(
    history: &mut VecDeque<HeatmapPoint>,
    snapshot: &AnalyticsSnapshot,
    limit: usize,
) {
    let Ok(point) = HeatmapPoint::try_from(snapshot) else {
        return;
    };
    let bucket = point.as_of_us.div_euclid(HEATMAP_BUCKET_US);
    if history
        .back()
        .is_some_and(|last| last.as_of_us.div_euclid(HEATMAP_BUCKET_US) == bucket)
    {
        *history.back_mut().expect("heatmap back exists") = point;
        return;
    }
    if history.len() == limit {
        history.pop_front();
    }
    history.push_back(point);
}

#[derive(Debug, Clone)]
struct ValidationPoint {
    as_of_us: i64,
    session_started_us: i64,
    underlying_price: f64,
    regime: String,
    call_trigger: Option<f64>,
    put_trigger: Option<f64>,
    call_wall: Option<f64>,
    put_wall: Option<f64>,
}

impl TryFrom<&AnalyticsSnapshot> for ValidationPoint {
    type Error = ();

    fn try_from(snapshot: &AnalyticsSnapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            as_of_us: snapshot.as_of_us,
            session_started_us: snapshot.session_started_us,
            underlying_price: snapshot.underlying_price.ok_or(())?,
            regime: snapshot.regime.clone(),
            call_trigger: snapshot.call_trigger,
            put_trigger: snapshot.put_trigger,
            call_wall: snapshot.call_wall,
            put_wall: snapshot.put_wall,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceBar {
    pub start_datetime_us: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ValidationRules {
    sample_minutes: u64,
    horizon_minutes: u64,
    touch_tolerance_points: f64,
    reaction_minutes: u64,
    continuation_or_reversion_points: f64,
    failure_points: f64,
    momentum_lookback_minutes: u64,
    momentum_override_points: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct LevelValidation {
    completed_signals: usize,
    touches: usize,
    effective: usize,
    failed: usize,
    pending_reactions: usize,
    touch_rate_pct: Option<f64>,
    effective_rate_pct: Option<f64>,
    superseded_before_touch: usize,
    actionable_signals: usize,
    actionable_touches: usize,
    actionable_effective: usize,
    actionable_failed: usize,
    actionable_pending_reactions: usize,
    actionable_touch_rate_pct: Option<f64>,
    actionable_effective_rate_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct TouchValidationReport {
    session_started_us: Option<i64>,
    through_us: Option<i64>,
    rules: ValidationRules,
    levels: BTreeMap<String, LevelValidation>,
}

#[derive(Clone, Copy)]
enum ValidationSide {
    Call,
    Put,
}

fn validation_level(snapshot: &ValidationPoint, name: &str) -> Option<f64> {
    match name {
        "call_trigger" => snapshot.call_trigger,
        "put_trigger" => snapshot.put_trigger,
        "call_wall" => snapshot.call_wall,
        "put_wall" => snapshot.put_wall,
        _ => None,
    }
}

fn touch_validation(history: &VecDeque<ValidationPoint>) -> TouchValidationReport {
    const SAMPLE_US: i64 = 60 * 60 * 1_000_000;
    const HORIZON_US: i64 = 60 * 60 * 1_000_000;
    const REACTION_US: i64 = 15 * 60 * 1_000_000;
    const TOLERANCE: f64 = 1.0;
    const REACTION_MOVE: f64 = 5.0;
    const FAILURE_MOVE: f64 = 3.0;

    let session_started_us = history.back().map(|snapshot| snapshot.session_started_us);
    let snapshots: Vec<_> = history
        .iter()
        .filter(|snapshot| Some(snapshot.session_started_us) == session_started_us)
        .collect();
    let through_us = snapshots.last().map(|snapshot| snapshot.as_of_us);
    let mut levels = BTreeMap::<String, LevelValidation>::new();
    let mut last_sample_us = i64::MIN;

    for (index, signal) in snapshots.iter().enumerate() {
        if signal.as_of_us.saturating_sub(last_sample_us) < SAMPLE_US {
            continue;
        }
        last_sample_us = signal.as_of_us;
        let Some(end_us) = signal.as_of_us.checked_add(HORIZON_US) else {
            continue;
        };
        if through_us.is_none_or(|through| through < end_us) {
            continue;
        }
        let spot = signal.underlying_price;
        for (name, level, side) in [
            ("call_trigger", signal.call_trigger, ValidationSide::Call),
            ("put_trigger", signal.put_trigger, ValidationSide::Put),
            ("call_wall", signal.call_wall, ValidationSide::Call),
            ("put_wall", signal.put_wall, ValidationSide::Put),
        ] {
            let Some(level) = level else { continue };
            let directionally_valid = match side {
                ValidationSide::Call => level > spot + TOLERANCE,
                ValidationSide::Put => level < spot - TOLERANCE,
            };
            if !directionally_valid {
                continue;
            }
            let window: Vec<_> = snapshots[index..]
                .iter()
                .copied()
                .take_while(|snapshot| snapshot.as_of_us <= end_us)
                .collect();
            let structural_short_gamma = signal.regime == "short-gamma";
            let event_index = window.windows(2).position(|pair| {
                let previous = pair[0].underlying_price;
                let current = pair[1].underlying_price;
                if structural_short_gamma {
                    match side {
                        ValidationSide::Call => previous < level && current >= level,
                        ValidationSide::Put => previous > level && current <= level,
                    }
                } else {
                    current >= level - TOLERANCE && current <= level + TOLERANCE
                }
            });
            let result = levels.entry(name.to_owned()).or_default();
            result.completed_signals += 1;
            let superseded_index = window
                .iter()
                .enumerate()
                .skip(1)
                .find(|(_, snapshot)| {
                    snapshot.regime != signal.regime
                        || validation_level(snapshot, name)
                            .is_none_or(|current| (current - level).abs() > 1e-9)
                })
                .map(|(position, _)| position);
            let event_index = event_index.map(|position| position + 1);
            let superseded_before_touch = superseded_index
                .is_some_and(|superseded| event_index.is_none_or(|event| superseded <= event));
            if superseded_before_touch {
                result.superseded_before_touch += 1;
            } else {
                result.actionable_signals += 1;
            }
            let Some(event_index) = event_index else {
                continue;
            };
            result.touches += 1;
            if !superseded_before_touch {
                result.actionable_touches += 1;
            }
            let event_us = window[event_index].as_of_us;
            let absolute_event_index = index + event_index;
            let prior_price = snapshots[..=absolute_event_index]
                .iter()
                .rev()
                .find(|snapshot| snapshot.as_of_us <= event_us - MOMENTUM_LOOKBACK_US)
                .map(|snapshot| snapshot.underlying_price);
            let momentum = prior_price.map(|prior| window[event_index].underlying_price - prior);
            let momentum_continuation = momentum.is_some_and(|momentum| match side {
                ValidationSide::Call => momentum >= MOMENTUM_OVERRIDE_POINTS,
                ValidationSide::Put => momentum <= -MOMENTUM_OVERRIDE_POINTS,
            });
            let short_gamma = structural_short_gamma || momentum_continuation;
            let reaction_end = event_us + REACTION_US;
            let mut outcome = None;
            for snapshot in window[event_index..]
                .iter()
                .take_while(|snapshot| snapshot.as_of_us <= reaction_end)
            {
                let price = snapshot.underlying_price;
                let (effective, failed) = match (short_gamma, side) {
                    (false, ValidationSide::Call) => (
                        price <= level - REACTION_MOVE,
                        price >= level + FAILURE_MOVE,
                    ),
                    (false, ValidationSide::Put) => (
                        price >= level + REACTION_MOVE,
                        price <= level - FAILURE_MOVE,
                    ),
                    (true, ValidationSide::Call) => (
                        price >= level + REACTION_MOVE,
                        price <= level - FAILURE_MOVE,
                    ),
                    (true, ValidationSide::Put) => (
                        price <= level - REACTION_MOVE,
                        price >= level + FAILURE_MOVE,
                    ),
                };
                if effective {
                    outcome = Some(true);
                    break;
                }
                if failed {
                    outcome = Some(false);
                    break;
                }
            }
            match outcome {
                Some(true) => {
                    result.effective += 1;
                    if !superseded_before_touch {
                        result.actionable_effective += 1;
                    }
                }
                Some(false) => {
                    result.failed += 1;
                    if !superseded_before_touch {
                        result.actionable_failed += 1;
                    }
                }
                None => {
                    result.pending_reactions += 1;
                    if !superseded_before_touch {
                        result.actionable_pending_reactions += 1;
                    }
                }
            }
        }
    }
    for result in levels.values_mut() {
        if result.completed_signals > 0 {
            result.touch_rate_pct =
                Some(result.touches as f64 * 100.0 / result.completed_signals as f64);
        }
        let decided = result.effective + result.failed;
        if decided > 0 {
            result.effective_rate_pct = Some(result.effective as f64 * 100.0 / decided as f64);
        }
        if result.actionable_signals > 0 {
            result.actionable_touch_rate_pct =
                Some(result.actionable_touches as f64 * 100.0 / result.actionable_signals as f64);
        }
        let actionable_decided = result.actionable_effective + result.actionable_failed;
        if actionable_decided > 0 {
            result.actionable_effective_rate_pct =
                Some(result.actionable_effective as f64 * 100.0 / actionable_decided as f64);
        }
    }
    TouchValidationReport {
        session_started_us,
        through_us,
        rules: ValidationRules {
            sample_minutes: 60,
            horizon_minutes: 60,
            touch_tolerance_points: TOLERANCE,
            reaction_minutes: 15,
            continuation_or_reversion_points: REACTION_MOVE,
            failure_points: FAILURE_MOVE,
            momentum_lookback_minutes: 5,
            momentum_override_points: MOMENTUM_OVERRIDE_POINTS,
        },
        levels,
    }
}

struct Engine {
    config: OptionsConfig,
    contracts: HashMap<u32, OptionContract>,
    states: HashMap<u32, ContractState>,
    underlying_price: Option<f64>,
    session_started_us: i64,
    underlying_id: u32,
}

impl Engine {
    fn snapshot(&self) -> AnalyticsSnapshot {
        let now = now_us();
        let Some(spot) = self.underlying_price.filter(|v| *v > 0.0) else {
            let mut value = AnalyticsSnapshot::empty(&self.config);
            value.contracts = self.contracts.len();
            value.session_started_us = self.session_started_us;
            return value;
        };
        let mut rows: BTreeMap<(String, i64), StrikeAnalytics> = BTreeMap::new();
        let mut total_gex = 0.0;
        let mut total_flow_gex = 0.0;
        let mut priced = 0usize;
        for (id, contract) in &self.contracts {
            let state = self.states.get(id).cloned().unwrap_or_default();
            let row = rows
                .entry((contract.expiration.clone(), strike_key(contract.strike)))
                .or_insert(StrikeAnalytics {
                    expiration: contract.expiration.clone(),
                    strike: contract.strike,
                    call_oi: 0,
                    put_oi: 0,
                    call_volume: 0.0,
                    put_volume: 0.0,
                    call_buy_volume: 0.0,
                    call_sell_volume: 0.0,
                    call_unknown_volume: 0.0,
                    put_buy_volume: 0.0,
                    put_sell_volume: 0.0,
                    put_unknown_volume: 0.0,
                    call_net_flow: 0.0,
                    put_net_flow: 0.0,
                    call_iv: None,
                    put_iv: None,
                    call_gex: None,
                    put_gex: None,
                    net_gex: None,
                    call_flow_gex: None,
                    put_flow_gex: None,
                    flow_gex: None,
                    confidence: None,
                });
            let volume = state.buy_volume + state.sell_volume + state.unknown_volume;
            let flow = state.buy_volume - state.sell_volume;
            match contract.option_type {
                OptionType::Call => {
                    row.call_oi += state.oi;
                    row.call_volume += volume;
                    row.call_buy_volume += state.buy_volume;
                    row.call_sell_volume += state.sell_volume;
                    row.call_unknown_volume += state.unknown_volume;
                    row.call_net_flow += flow;
                }
                OptionType::Put => {
                    row.put_oi += state.oi;
                    row.put_volume += volume;
                    row.put_buy_volume += state.buy_volume;
                    row.put_sell_volume += state.sell_volume;
                    row.put_unknown_volume += state.unknown_volume;
                    row.put_net_flow += flow;
                }
            }
            let Some(mid) = midpoint(&state) else {
                continue;
            };
            let Some(t) = years_to_expiry(&contract.expiration, now) else {
                continue;
            };
            let Some(iv) = implied_volatility(
                contract.option_type,
                spot,
                contract.strike,
                t,
                self.config.risk_free_rate,
                mid,
            ) else {
                continue;
            };
            let gamma = black76_gamma(spot, contract.strike, t, self.config.risk_free_rate, iv);
            let unit_gex = gamma * contract.multiplier * spot * spot * 0.01;
            let magnitude = unit_gex * state.oi as f64;
            let signed = if contract.option_type == OptionType::Call {
                magnitude
            } else {
                -magnitude
            };
            total_gex += signed;
            // Aggressor buys are treated as customer buys (dealer sells), while
            // aggressor sells are treated as customer sells (dealer buys).
            let flow_gex = unit_gex * (state.sell_volume - state.buy_volume);
            total_flow_gex += flow_gex;
            row.net_gex = Some(row.net_gex.unwrap_or(0.0) + signed);
            row.flow_gex = Some(row.flow_gex.unwrap_or(0.0) + flow_gex);
            match contract.option_type {
                OptionType::Call => {
                    row.call_iv = Some(iv);
                    row.call_gex = Some(row.call_gex.unwrap_or(0.0) + magnitude);
                    row.call_flow_gex = Some(row.call_flow_gex.unwrap_or(0.0) + flow_gex);
                }
                OptionType::Put => {
                    row.put_iv = Some(iv);
                    row.put_gex = Some(row.put_gex.unwrap_or(0.0) - magnitude);
                    row.put_flow_gex = Some(row.put_flow_gex.unwrap_or(0.0) + flow_gex);
                }
            }
            priced += 1;
        }
        let mut strikes: Vec<_> = rows.into_values().collect();
        for row in &mut strikes {
            let quoted_sides = f64::from(row.call_gex.is_some()) + f64::from(row.put_gex.is_some());
            let quote_score = quoted_sides / 2.0;
            let total_volume = row.call_volume + row.put_volume;
            let classified_volume = row.call_buy_volume
                + row.call_sell_volume
                + row.put_buy_volume
                + row.put_sell_volume;
            let flow_score = if total_volume > 0.0 {
                (classified_volume / total_volume).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let oi_score = f64::from(row.call_oi + row.put_oi > 0);
            row.confidence =
                Some((quote_score * 0.45 + flow_score * 0.35 + oi_score * 0.20) * 100.0);
        }
        let call_wall = directional_wall_level(
            strikes.iter().filter_map(|row| {
                wall_strength(row.call_gex, row.call_oi as f64)
                    .map(|strength| (row.strike, strength))
            }),
            spot,
            true,
        );
        let put_wall = directional_wall_level(
            strikes.iter().filter_map(|row| {
                wall_strength(row.put_gex, row.put_oi as f64).map(|strength| (row.strike, strength))
            }),
            spot,
            false,
        );
        let hvl = strikes
            .iter()
            .filter_map(|r| r.net_gex.map(|g| (r.strike, g.abs())))
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .map(|v| v.0);
        let call_trigger = distance_weighted_level(
            strikes.iter().filter_map(|row| {
                row.call_gex
                    .map(|gex| (row.strike, gex.abs(), row.confidence.unwrap_or(0.0)))
            }),
            spot,
            true,
        );
        let put_trigger = distance_weighted_level(
            strikes.iter().filter_map(|row| {
                row.put_gex
                    .map(|gex| (row.strike, gex.abs(), row.confidence.unwrap_or(0.0)))
            }),
            spot,
            false,
        );
        let flow_call_wall = directional_wall_level(
            strikes.iter().filter_map(|row| {
                wall_strength(row.call_flow_gex, row.call_net_flow)
                    .map(|strength| (row.strike, strength))
            }),
            spot,
            true,
        );
        let flow_put_wall = directional_wall_level(
            strikes.iter().filter_map(|row| {
                wall_strength(row.put_flow_gex, row.put_net_flow)
                    .map(|strength| (row.strike, strength))
            }),
            spot,
            false,
        );
        let flow_hvl = strikes
            .iter()
            .filter_map(|row| row.flow_gex.map(|gex| (row.strike, gex.abs())))
            .filter(|(_, gex)| *gex > f64::EPSILON)
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .map(|value| value.0);
        let confidence = (!strikes.is_empty()).then(|| {
            strikes.iter().filter_map(|row| row.confidence).sum::<f64>() / strikes.len() as f64
        });
        let zero_gamma = self.zero_gamma(spot, now, false);
        let flow_zero_gamma = self.zero_gamma(spot, now, true);
        let regime = if priced == 0 {
            "awaiting-option-quotes"
        } else if total_gex >= 0.0 {
            "long-gamma"
        } else {
            "short-gamma"
        };
        AnalyticsSnapshot {
            as_of_us: now,
            session_started_us: self.session_started_us,
            underlying: self.config.underlying_symbol.clone(),
            underlying_price: Some(spot),
            contracts: self.contracts.len(),
            net_gex: (priced > 0).then_some(total_gex),
            flow_gex: (priced > 0).then_some(total_flow_gex),
            confidence,
            zero_gamma,
            call_wall,
            put_wall,
            hvl,
            call_trigger,
            put_trigger,
            flow_zero_gamma,
            flow_call_wall,
            flow_put_wall,
            flow_hvl,
            regime: regime.to_owned(),
            momentum_5m: None,
            playbook: if regime == "long-gamma" {
                "mean-reversion"
            } else if regime == "short-gamma" {
                "continuation"
            } else {
                "awaiting-momentum"
            }
            .to_owned(),
            model:
                "Black-76; calls dealer-long / puts dealer-short proxy; front1-strength25-sticky60-near-v5-momentum5-walls2"
                    .to_owned(),
            strikes,
        }
    }

    fn zero_gamma(&self, spot: f64, now: i64, flow_adjusted: bool) -> Option<f64> {
        let mut profile = Vec::with_capacity(81);
        for n in 0..81 {
            let test_spot = spot * (0.8 + n as f64 * 0.005);
            let mut sum = 0.0;
            let mut count = 0;
            for (id, contract) in &self.contracts {
                let Some(state) = self.states.get(id) else {
                    continue;
                };
                let Some(mid) = midpoint(state) else {
                    continue;
                };
                let Some(t) = years_to_expiry(&contract.expiration, now) else {
                    continue;
                };
                let Some(iv) = implied_volatility(
                    contract.option_type,
                    spot,
                    contract.strike,
                    t,
                    self.config.risk_free_rate,
                    mid,
                ) else {
                    continue;
                };
                let gamma = black76_gamma(
                    test_spot,
                    contract.strike,
                    t,
                    self.config.risk_free_rate,
                    iv,
                );
                let position = if flow_adjusted {
                    state.sell_volume - state.buy_volume
                } else if contract.option_type == OptionType::Call {
                    state.oi as f64
                } else {
                    -(state.oi as f64)
                };
                if position.abs() > f64::EPSILON {
                    sum += position * gamma * contract.multiplier * test_spot * test_spot * 0.01;
                    count += 1;
                }
            }
            if count == 0 {
                return None;
            }
            profile.push((test_spot, sum));
        }
        nearest_zero_crossing(&profile, spot)
    }
}

fn distance_weighted_level<I>(candidates: I, spot: f64, upper: bool) -> Option<f64>
where
    I: IntoIterator<Item = (f64, f64, f64)>,
{
    // About 0.15% of spot (11.4 ES points near 7,600) favors a reachable
    // secondary wall after the 25% structural-strength floor has already been
    // applied. The one-point exclusion prevents a level already being touched
    // from being advertised as a fresh trigger.
    let distance_scale = (spot * 0.0015).clamp(8.0, 30.0);
    let candidates: Vec<_> = candidates
        .into_iter()
        .filter(|(strike, magnitude, confidence)| {
            strike.is_finite()
                && magnitude.is_finite()
                && *magnitude > 0.0
                && *confidence >= 50.0
                && if upper {
                    *strike > spot + 1.0
                } else {
                    *strike < spot - 1.0
                }
        })
        .collect();
    let strongest = candidates
        .iter()
        .map(|(_, magnitude, _)| *magnitude)
        .filter(|magnitude| magnitude.is_finite())
        .max_by(f64::total_cmp)?;
    candidates
        .into_iter()
        .filter(|(_, magnitude, _)| *magnitude >= strongest * 0.25)
        .map(|(strike, magnitude, confidence)| {
            let distance = (strike - spot).abs();
            let confidence_weight = (confidence / 100.0).clamp(0.5, 1.0);
            let score = magnitude * (-distance / distance_scale).exp() * confidence_weight;
            (strike, score)
        })
        .max_by(|left, right| left.1.total_cmp(&right.1))
        .map(|(strike, _)| strike)
}

fn wall_strength(gex: Option<f64>, position: f64) -> Option<f64> {
    let gex = gex?.abs();
    let position = position.abs();
    (gex.is_finite() && position.is_finite() && gex > f64::EPSILON && position > f64::EPSILON)
        .then(|| (gex * position).sqrt())
}

fn directional_wall_level<I>(candidates: I, spot: f64, upper: bool) -> Option<f64>
where
    I: IntoIterator<Item = (f64, f64)>,
{
    candidates
        .into_iter()
        .filter(|(strike, strength)| {
            strike.is_finite()
                && strength.is_finite()
                && *strength > f64::EPSILON
                && if upper {
                    *strike > spot + WALL_DIRECTION_BUFFER_POINTS
                } else {
                    *strike < spot - WALL_DIRECTION_BUFFER_POINTS
                }
        })
        .max_by(|left, right| left.1.total_cmp(&right.1))
        .map(|(strike, _)| strike)
}

fn nearest_zero_crossing(profile: &[(f64, f64)], spot: f64) -> Option<f64> {
    let mut crossings = Vec::new();
    for pair in profile.windows(2) {
        let (left_spot, left_gex) = pair[0];
        let (right_spot, right_gex) = pair[1];
        if left_gex == 0.0 {
            crossings.push(left_spot);
        }
        if right_gex == 0.0 {
            crossings.push(right_spot);
        } else if left_gex.signum() != right_gex.signum() {
            let weight = left_gex.abs() / (left_gex.abs() + right_gex.abs()).max(f64::EPSILON);
            crossings.push(left_spot + (right_spot - left_spot) * weight);
        }
    }
    crossings
        .into_iter()
        .filter(|value| value.is_finite())
        .min_by(|left, right| (left - spot).abs().total_cmp(&(right - spot).abs()))
}

struct MarketView {
    snapshot: Arc<RwLock<AnalyticsSnapshot>>,
    history: Arc<RwLock<VecDeque<AnalyticsSnapshot>>>,
    heatmap_history: Arc<RwLock<VecDeque<HeatmapPoint>>>,
    validation_history: Arc<RwLock<VecDeque<ValidationPoint>>>,
    bars: Arc<RwLock<Vec<PriceBar>>>,
}

/// HTTP-facing options state can be prepared before Rithmic is available. This
/// keeps the authenticated dashboard reachable during scheduled maintenance;
/// cached snapshots remain visible until the live collector can attach.
pub struct OptionsService {
    listen_addr: String,
    views: Arc<BTreeMap<String, Arc<MarketView>>>,
    access_token: Option<Arc<String>>,
    feed_connected: Arc<AtomicBool>,
}

impl OptionsService {
    pub fn prepare(configs: &[OptionsConfig]) -> Result<Self, String> {
        if configs.is_empty() {
            return Err("no option markets configured".to_owned());
        }
        let listen_addr = configs[0].listen_addr.clone();
        let mut views = BTreeMap::new();
        for config in configs {
            let snapshot_path = config.data_dir.join("snapshots.jsonl");
            let restored = load_snapshot_history(
                &snapshot_path,
                &config.underlying_symbol,
                REPLAY_HISTORY_LIMIT,
            );
            let validation_history = load_validation_history(
                &snapshot_path,
                &config.underlying_symbol,
                VALIDATION_HISTORY_LIMIT,
            );
            let heatmap_history = load_heatmap_history(
                &snapshot_path,
                &config.underlying_symbol,
                HEATMAP_HISTORY_LIMIT,
            );
            let initial_snapshot = restored
                .back()
                .cloned()
                .unwrap_or_else(|| AnalyticsSnapshot::empty(config));
            let cached_bars = load_price_bar_cache(&config.data_dir.join("price-bars.json"));
            views.insert(
                config.root.clone(),
                Arc::new(MarketView {
                    snapshot: Arc::new(RwLock::new(initial_snapshot)),
                    history: Arc::new(RwLock::new(restored)),
                    heatmap_history: Arc::new(RwLock::new(heatmap_history)),
                    validation_history: Arc::new(RwLock::new(validation_history)),
                    bars: Arc::new(RwLock::new(cached_bars)),
                }),
            );
        }
        Ok(Self {
            listen_addr,
            views: Arc::new(views),
            access_token: load_access_token()?,
            feed_connected: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn serve(&self) -> Result<(), String> {
        println!(
            "Options analytics hub listening on http://{} ({})",
            self.listen_addr,
            self.views.keys().cloned().collect::<Vec<_>>().join(", ")
        );
        serve_http(
            &self.listen_addr,
            Arc::clone(&self.views),
            self.access_token.clone(),
            Arc::clone(&self.feed_connected),
        )
        .await
    }

    pub fn start_collector(
        &self,
        client: MarketDataClient,
        markets: Vec<(HistoryDataClient, OptionsConfig)>,
    ) {
        let views = Arc::clone(&self.views);
        let feed_connected = Arc::clone(&self.feed_connected);
        feed_connected.store(true, Ordering::Relaxed);
        tokio::spawn(async move {
            if let Err(error) =
                collect_markets(client, markets, views, Arc::clone(&feed_connected)).await
            {
                eprintln!("[Options] multi-market collector stopped: {error}");
            }
            feed_connected.store(false, Ordering::Relaxed);
        });
    }
}

pub async fn start(
    client: MarketDataClient,
    markets: Vec<(HistoryDataClient, OptionsConfig)>,
) -> Result<(), String> {
    let configs = markets
        .iter()
        .map(|(_, config)| config.clone())
        .collect::<Vec<_>>();
    let service = OptionsService::prepare(&configs)?;
    service.start_collector(client, markets);
    service.serve().await
}

struct RunningMarket {
    engine: Engine,
    view: Arc<MarketView>,
    writer: EventWriter,
    call_trigger: StickyTrigger,
    put_trigger: StickyTrigger,
    walls: StickyWalls,
    recent_prices: VecDeque<(i64, f64)>,
}

#[derive(Debug, Default)]
struct StickyWall {
    level: Option<f64>,
    challenger: Option<f64>,
    challenger_since_us: i64,
    release_after_publish: bool,
}

impl StickyWall {
    fn new(level: Option<f64>) -> Self {
        Self {
            level,
            ..Self::default()
        }
    }

    fn update<I>(
        &mut self,
        candidate: Option<f64>,
        strengths: I,
        spot: f64,
        upper: bool,
        as_of_us: i64,
    ) -> Option<f64>
    where
        I: IntoIterator<Item = (f64, f64)>,
    {
        if self.release_after_publish {
            self.level = candidate;
            self.challenger = None;
            self.release_after_publish = false;
            return self.level;
        }
        let Some(level) = self.level else {
            self.level = candidate;
            return self.level;
        };
        let directionally_ahead = if upper {
            level > spot + WALL_DIRECTION_BUFFER_POINTS
        } else {
            level < spot - WALL_DIRECTION_BUFFER_POINTS
        };
        if !directionally_ahead {
            // Publish the touched/crossed wall once, then rotate to the next
            // directionally valid inventory wall on the following snapshot.
            self.release_after_publish = true;
            self.challenger = None;
            return self.level;
        }
        let values: Vec<_> = strengths
            .into_iter()
            .filter(|(strike, strength)| {
                strength.is_finite()
                    && *strength > f64::EPSILON
                    && if upper {
                        *strike > spot + WALL_DIRECTION_BUFFER_POINTS
                    } else {
                        *strike < spot - WALL_DIRECTION_BUFFER_POINTS
                    }
            })
            .collect();
        let strongest = values
            .iter()
            .max_by(|left, right| left.1.total_cmp(&right.1))
            .copied();
        let current_strength = values
            .iter()
            .find(|(strike, _)| (*strike - level).abs() < 1e-9)
            .map(|(_, strength)| *strength);
        let Some((strongest_level, strongest_strength)) = strongest else {
            return self.level;
        };
        if (strongest_level - level).abs() < 1e-9
            || current_strength.is_some_and(|current| {
                current >= strongest_strength * WALL_RETAIN_STRENGTH_FRACTION
            })
        {
            self.challenger = None;
            return self.level;
        }
        if self
            .challenger
            .is_none_or(|challenger| (challenger - strongest_level).abs() >= 1e-9)
        {
            self.challenger = Some(strongest_level);
            self.challenger_since_us = as_of_us;
            return self.level;
        }
        if as_of_us.saturating_sub(self.challenger_since_us) >= WALL_CHALLENGER_CONFIRM_US {
            self.level = Some(strongest_level);
            self.challenger = None;
        }
        self.level
    }
}

#[derive(Debug, Default)]
struct StickyWalls {
    session_started_us: Option<i64>,
    call: StickyWall,
    put: StickyWall,
    flow_call: StickyWall,
    flow_put: StickyWall,
}

impl StickyWalls {
    fn from_snapshot(snapshot: &AnalyticsSnapshot) -> Self {
        Self {
            session_started_us: Some(snapshot.session_started_us),
            call: StickyWall::new(snapshot.call_wall),
            put: StickyWall::new(snapshot.put_wall),
            flow_call: StickyWall::new(snapshot.flow_call_wall),
            flow_put: StickyWall::new(snapshot.flow_put_wall),
        }
    }

    fn update(&mut self, snapshot: &mut AnalyticsSnapshot) {
        let Some(spot) = snapshot.underlying_price else {
            return;
        };
        let call_strengths: Vec<_> = snapshot
            .strikes
            .iter()
            .filter_map(|row| {
                wall_strength(row.call_gex, row.call_oi as f64)
                    .map(|strength| (row.strike, strength))
            })
            .collect();
        let put_strengths: Vec<_> = snapshot
            .strikes
            .iter()
            .filter_map(|row| {
                wall_strength(row.put_gex, row.put_oi as f64).map(|strength| (row.strike, strength))
            })
            .collect();
        let flow_call_strengths: Vec<_> = snapshot
            .strikes
            .iter()
            .filter_map(|row| {
                wall_strength(row.call_flow_gex, row.call_net_flow)
                    .map(|strength| (row.strike, strength))
            })
            .collect();
        let flow_put_strengths: Vec<_> = snapshot
            .strikes
            .iter()
            .filter_map(|row| {
                wall_strength(row.put_flow_gex, row.put_net_flow)
                    .map(|strength| (row.strike, strength))
            })
            .collect();
        let call_candidate = directional_wall_level(call_strengths.iter().copied(), spot, true);
        let put_candidate = directional_wall_level(put_strengths.iter().copied(), spot, false);
        let flow_call_candidate =
            directional_wall_level(flow_call_strengths.iter().copied(), spot, true);
        let flow_put_candidate =
            directional_wall_level(flow_put_strengths.iter().copied(), spot, false);
        if self.session_started_us != Some(snapshot.session_started_us) {
            self.session_started_us = Some(snapshot.session_started_us);
            self.call = StickyWall::new(call_candidate);
            self.put = StickyWall::new(put_candidate);
            self.flow_call = StickyWall::new(flow_call_candidate);
            self.flow_put = StickyWall::new(flow_put_candidate);
            snapshot.call_wall = call_candidate;
            snapshot.put_wall = put_candidate;
            snapshot.flow_call_wall = flow_call_candidate;
            snapshot.flow_put_wall = flow_put_candidate;
            return;
        }
        snapshot.call_wall = self.call.update(
            call_candidate,
            call_strengths,
            spot,
            true,
            snapshot.as_of_us,
        );
        snapshot.put_wall =
            self.put
                .update(put_candidate, put_strengths, spot, false, snapshot.as_of_us);
        snapshot.flow_call_wall = self.flow_call.update(
            flow_call_candidate,
            flow_call_strengths,
            spot,
            true,
            snapshot.as_of_us,
        );
        snapshot.flow_put_wall = self.flow_put.update(
            flow_put_candidate,
            flow_put_strengths,
            spot,
            false,
            snapshot.as_of_us,
        );
    }
}

#[derive(Debug, Default)]
struct StickyTrigger {
    level: Option<f64>,
    release_after_publish: bool,
    selected_at_us: i64,
}

impl StickyTrigger {
    fn new(level: Option<f64>, selected_at_us: i64) -> Self {
        Self {
            level,
            release_after_publish: false,
            selected_at_us,
        }
    }

    fn update<I>(
        &mut self,
        candidate: Option<f64>,
        strengths: I,
        spot: f64,
        upper: bool,
        as_of_us: i64,
    ) -> Option<f64>
    where
        I: IntoIterator<Item = (f64, f64, f64)>,
    {
        if self.release_after_publish {
            self.level = candidate;
            self.release_after_publish = false;
            self.selected_at_us = as_of_us;
            return self.level;
        }
        let Some(level) = self.level else {
            self.level = candidate;
            self.selected_at_us = as_of_us;
            return self.level;
        };
        if as_of_us.saturating_sub(self.selected_at_us) >= TRIGGER_HOLD_US {
            self.level = candidate;
            self.selected_at_us = as_of_us;
            return self.level;
        }
        let values: Vec<_> = strengths
            .into_iter()
            .filter(|(strike, magnitude, confidence)| {
                *confidence >= 50.0
                    && *magnitude > 0.0
                    && if upper {
                        *strike > spot + 1.0
                    } else {
                        *strike < spot - 1.0
                    }
            })
            .collect();
        let directionally_ahead = if upper {
            level > spot + 1.0
        } else {
            level < spot - 1.0
        };
        if !directionally_ahead {
            // Keep the old level for the touch/cross snapshot, then advance on
            // the next publication so validation and traders can observe it.
            self.release_after_publish = true;
            return self.level;
        }
        let strongest = values
            .iter()
            .map(|(_, magnitude, _)| *magnitude)
            .max_by(f64::total_cmp);
        let current = values
            .iter()
            .find(|(strike, _, _)| (*strike - level).abs() < 1e-9)
            .map(|(_, magnitude, _)| *magnitude);
        if strongest
            .zip(current)
            .is_some_and(|(strongest, current)| current >= strongest * 0.25)
        {
            return self.level;
        }
        self.level = candidate;
        self.selected_at_us = as_of_us;
        self.level
    }
}

async fn setup_market(
    client: &MarketDataClient,
    config: OptionsConfig,
    view: Arc<MarketView>,
    market_index: usize,
) -> Result<RunningMarket, String> {
    fs::create_dir_all(&config.data_dir)
        .map_err(|e| format!("create options data directory: {e}"))?;
    let underlying_id = UNDERLYING_ID + (market_index as u32) * 0x1_0000;
    let first_option_id = underlying_id + 1;
    let underlying_snapshot = client
        .subscribe_raw(underlying_id, &config.underlying_symbol, &config.exchange)
        .await?;
    let mut contracts = client
        .discover_options(&config.underlying_symbol, &config.exchange, None)
        .await?;
    if contracts.is_empty() && config.root != config.underlying_symbol {
        contracts = client
            .discover_options(&config.root, &config.exchange, None)
            .await?;
    }
    // CME's option-board lookup is not uniform across product groups.  Equity
    // index options are keyed by the futures symbol (ESU6/NQU6), while COMEX
    // Gold and CBOT E-mini Dow can be keyed by their option-board product code.
    // Try those official Globex product codes before declaring the chain empty.
    let option_board_aliases: &[&str] = match config.root.as_str() {
        "GC" => &["OG", "LO"],
        "YM" => &["OYM"],
        _ => &[],
    };
    for alias in option_board_aliases {
        if !contracts.is_empty() {
            break;
        }
        println!(
            "[Options:{}] trying option-board product {}.{}",
            config.root, alias, config.exchange
        );
        contracts = client
            .discover_options(alias, &config.exchange, None)
            .await?;
    }
    println!(
        "[Options] discovered {} contracts for {} (fallback root {})",
        contracts.len(),
        config.underlying_symbol,
        config.root
    );
    let center = config
        .center_price
        .or(underlying_snapshot.last)
        .or_else(|| contracts.get(contracts.len() / 2).map(|c| c.strike))
        .ok_or_else(|| "Rithmic returned no futures-option contracts".to_owned())?;
    contracts = select_contracts(contracts, center, &config);
    if contracts.is_empty() {
        return Err("no option contracts survived the expiry/strike filters".to_owned());
    }

    let mut by_id = HashMap::new();
    let mut states = HashMap::new();
    for (index, contract) in contracts.into_iter().enumerate() {
        let id = first_option_id + index as u32;
        let snapshot = match client
            .subscribe_raw(id, &contract.symbol, &contract.exchange)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                eprintln!(
                    "[Options] skipping {}.{}: {error}",
                    contract.symbol, contract.exchange
                );
                continue;
            }
        };
        states.insert(
            id,
            ContractState {
                bid: snapshot.bid,
                ask: snapshot.ask,
                oi: u64::from(snapshot.open_interest.unwrap_or(0)),
                ..ContractState::default()
            },
        );
        by_id.insert(id, contract);
    }
    if by_id.is_empty() {
        return Err("Rithmic rejected every selected option subscription; verify CME option entitlements and symbols".to_owned());
    }

    let session_started_us = now_us();
    let engine = Engine {
        config: config.clone(),
        contracts: by_id,
        states,
        underlying_price: config.center_price.or(underlying_snapshot.last),
        session_started_us,
        underlying_id,
    };
    let initial_snapshot = engine.snapshot();
    *view.snapshot.write().await = initial_snapshot.clone();
    let snapshot_path = config.data_dir.join("snapshots.jsonl");
    let restored = load_snapshot_history(
        &snapshot_path,
        &config.underlying_symbol,
        REPLAY_HISTORY_LIMIT,
    );
    let validation_history = load_validation_history(
        &snapshot_path,
        &config.underlying_symbol,
        VALIDATION_HISTORY_LIMIT,
    );
    let heatmap_history = load_heatmap_history(
        &snapshot_path,
        &config.underlying_symbol,
        HEATMAP_HISTORY_LIMIT,
    );
    if !restored.is_empty() {
        println!("[Options] restored {} historical snapshots", restored.len());
    }
    if !restored.is_empty() {
        *view.history.write().await = restored;
    }
    if !validation_history.is_empty() {
        *view.validation_history.write().await = validation_history;
    }
    if !heatmap_history.is_empty() {
        println!(
            "[Options] restored {} one-minute heatmap frames",
            heatmap_history.len()
        );
        *view.heatmap_history.write().await = heatmap_history;
    }
    let writer = EventWriter::new(snapshot_path)?;
    println!(
        "Options analytics listening on http://{} ({} contracts)",
        config.listen_addr,
        engine.contracts.len()
    );

    Ok(RunningMarket {
        engine,
        view,
        writer,
        call_trigger: StickyTrigger::new(initial_snapshot.call_trigger, initial_snapshot.as_of_us),
        put_trigger: StickyTrigger::new(initial_snapshot.put_trigger, initial_snapshot.as_of_us),
        walls: StickyWalls::from_snapshot(&initial_snapshot),
        recent_prices: VecDeque::new(),
    })
}

async fn collect_markets(
    mut client: MarketDataClient,
    markets: Vec<(HistoryDataClient, OptionsConfig)>,
    views: Arc<BTreeMap<String, Arc<MarketView>>>,
    feed_connected: Arc<AtomicBool>,
) -> Result<(), String> {
    let snapshot_secs = markets
        .iter()
        .map(|(_, config)| config.snapshot_secs)
        .min()
        .unwrap_or(3);
    let mut setups = Vec::new();
    let mut history_jobs = Vec::new();
    for (index, (history, config)) in markets.into_iter().enumerate() {
        let label = config.root.clone();
        let Some(view) = views.get(&label).cloned() else {
            continue;
        };
        // Markets without cached bars must be repaired first. Existing markets
        // remain visible from cache while their refresh waits its turn.
        let has_cached_bars = !view.bars.read().await.is_empty();
        history_jobs.push((has_cached_bars, history, config.clone(), Arc::clone(&view)));
        setups.push((index, label, config, view));
    }
    // Backfill is independent of option discovery/subscription. A slow or stuck
    // option board must never prevent later markets from receiving price bars.
    // Keep the jobs sequential so tick replay cannot multiply peak memory.
    tokio::spawn(async move {
        history_jobs.sort_by_key(|(has_cached_bars, ..)| *has_cached_bars);
        for (_, history, config, view) in history_jobs {
            if let Err(error) = refresh_price_bars(history, config.clone(), view).await {
                eprintln!(
                    "[Options:{}] historical bars unavailable: {error}",
                    config.root
                );
            }
        }
    });

    let mut running = Vec::new();
    for (index, label, config, view) in setups {
        match setup_market(&client, config, view, index).await {
            Ok(market) => running.push(market),
            Err(error) => eprintln!("[Options:{label}] setup failed: {error}"),
        }
    }
    if running.is_empty() {
        return Err("every configured options market failed to initialize".to_owned());
    }
    let mut interval = time::interval(Duration::from_secs(snapshot_secs));
    loop {
        tokio::select! {
            event = client.next_raw_event() => {
                let Some(event) = event else { return Err("Rithmic market event channel closed".to_owned()); };
                match &event {
                    MarketEvent::FeedStatus { available } => {
                        feed_connected.store(*available, Ordering::Relaxed);
                    }
                    MarketEvent::FeedError(_) => {
                        feed_connected.store(false, Ordering::Relaxed);
                    }
                    _ => {}
                }
                if let Some(symbol_id) = market_event_symbol_id(&event) {
                    if let Some(market) = running.iter_mut().find(|market| {
                        market.engine.underlying_id == symbol_id || market.engine.contracts.contains_key(&symbol_id)
                    }) {
                        apply_event(&mut market.engine, event);
                    }
                }
            }
            _ = interval.tick() => {
                for market in &mut running {
                    let mut snapshot = market.engine.snapshot();
                    if let Some(spot) = snapshot.underlying_price {
                        market.recent_prices.push_back((snapshot.as_of_us, spot));
                        while market.recent_prices.front().is_some_and(|(as_of_us, _)| {
                            *as_of_us < snapshot.as_of_us - 2 * MOMENTUM_LOOKBACK_US
                        }) {
                            market.recent_prices.pop_front();
                        }
                        snapshot.momentum_5m = market
                            .recent_prices
                            .iter()
                            .rev()
                            .find(|(as_of_us, _)| {
                                *as_of_us <= snapshot.as_of_us - MOMENTUM_LOOKBACK_US
                            })
                            .map(|(_, prior)| spot - prior);
                        if let Some(momentum) = snapshot.momentum_5m {
                            if momentum >= MOMENTUM_OVERRIDE_POINTS {
                                snapshot.playbook = "upside-continuation".to_owned();
                            } else if momentum <= -MOMENTUM_OVERRIDE_POINTS {
                                snapshot.playbook = "downside-continuation".to_owned();
                            }
                        }
                        market.walls.update(&mut snapshot);
                        let candidate = snapshot.call_trigger;
                        snapshot.call_trigger = market.call_trigger.update(
                            candidate,
                            snapshot.strikes.iter().filter_map(|row| {
                                row.call_gex.map(|gex| {
                                    (row.strike, gex.abs(), row.confidence.unwrap_or(0.0))
                                })
                            }),
                            spot,
                            true,
                            snapshot.as_of_us,
                        );
                        let candidate = snapshot.put_trigger;
                        snapshot.put_trigger = market.put_trigger.update(
                            candidate,
                            snapshot.strikes.iter().filter_map(|row| {
                                row.put_gex.map(|gex| {
                                    (row.strike, gex.abs(), row.confidence.unwrap_or(0.0))
                                })
                            }),
                            spot,
                            false,
                            snapshot.as_of_us,
                        );
                    }
                    if let Ok(line) = serde_json::to_string(&snapshot) { market.writer.write(line); }
                    let mut replay = market.view.history.write().await;
                    if replay.len() == REPLAY_HISTORY_LIMIT { replay.pop_front(); }
                    replay.push_back(snapshot.clone());
                    drop(replay);
                    {
                        let mut heatmap = market.view.heatmap_history.write().await;
                        push_heatmap_point(&mut heatmap, &snapshot, HEATMAP_HISTORY_LIMIT);
                    }
                    if let Ok(point) = ValidationPoint::try_from(&snapshot) {
                        let mut validation = market.view.validation_history.write().await;
                        if validation.len() == VALIDATION_HISTORY_LIMIT {
                            validation.pop_front();
                        }
                        validation.push_back(point);
                    }
                    *market.view.snapshot.write().await = snapshot;
                }
            }
        }
    }
}

fn market_event_symbol_id(event: &MarketEvent) -> Option<u32> {
    match event {
        MarketEvent::Snapshot { symbol_id, .. }
        | MarketEvent::SessionVolume { symbol_id, .. }
        | MarketEvent::LastTrade { symbol_id, .. }
        | MarketEvent::BestBidAsk { symbol_id, .. }
        | MarketEvent::DepthUpdate { symbol_id, .. }
        | MarketEvent::DepthSnapshotLevel { symbol_id, .. } => Some(*symbol_id),
        MarketEvent::FeedStatus { .. } | MarketEvent::FeedError(_) => None,
    }
}

async fn refresh_price_bars(
    history: HistoryDataClient,
    config: OptionsConfig,
    view: Arc<MarketView>,
) -> Result<(), String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs() as i64;
    // Load backwards in bounded tick chunks and publish/cache each chunk. Liquid
    // futures can produce hundreds of thousands of tick responses, so production
    // hosts may use smaller chunks to keep the transient replay allocation low.
    let mut chunk_end = latest_history_end(now);
    let mut all_bars = BTreeMap::<i64, PriceBar>::new();
    let history_chunks = env_usize("OPTIONS_HISTORY_CHUNKS", 36).clamp(1, 288);
    let chunk_minutes = env_usize("OPTIONS_HISTORY_CHUNK_MINUTES", 120).clamp(5, 120);
    let max_rss_mb = env_usize("OPTIONS_HISTORY_MAX_RSS_MB", 0);
    for chunk in 0..history_chunks {
        let chunk_start = chunk_end - (chunk_minutes as i64) * 60 + 1;
        let records = history
            .load(HistoricalRequest {
                request_id: 9_001 + chunk as i32,
                symbol: config.underlying_symbol.clone(),
                exchange: config.exchange.clone(),
                // Rithmic Paper's time-bar replay can omit its completion marker and
                // hang indefinitely. Tick replay completes reliably; aggregate those
                // server ticks into one-minute OHLCV bars locally.
                record_interval: 0,
                start_time: chunk_start,
                end_time: chunk_end,
                max_days: 1,
            })
            .await?;
        for bar in aggregate_minute_bars(records) {
            all_bars.insert(bar.start_datetime_us, bar);
        }
        let bars: Vec<_> = all_bars.values().cloned().collect();
        *view.bars.write().await = bars.clone();
        save_price_bar_cache(&config.data_dir.join("price-bars.json"), &bars)?;
        println!(
            "[Options:{}] loaded {} historical one-minute bars",
            config.root,
            bars.len()
        );
        trim_process_heap();
        if let Some(rss_mb) = resident_set_mb() {
            println!(
                "[Options:{}] history RSS after trim: {rss_mb} MiB",
                config.root
            );
            if max_rss_mb > 0 && rss_mb >= max_rss_mb {
                eprintln!(
                    "[Options:{}] stopping historical backfill at {rss_mb} MiB (limit {max_rss_mb} MiB)",
                    config.root
                );
                break;
            }
        }
        // Jump directly over the closed weekend instead of issuing dozens of
        // empty half-hour replay requests.
        chunk_end = latest_history_end(chunk_start - 1);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn trim_process_heap() {
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    // SAFETY: glibc's malloc_trim only releases currently unused allocator pages.
    unsafe {
        malloc_trim(0);
    }
}

#[cfg(not(target_os = "linux"))]
fn trim_process_heap() {}

#[cfg(target_os = "linux")]
fn resident_set_mb() -> Option<usize> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse::<usize>()
        .ok()
        .map(|kb| kb.div_ceil(1024))
}

#[cfg(not(target_os = "linux"))]
fn resident_set_mb() -> Option<usize> {
    None
}

fn load_price_bar_cache(path: &PathBuf) -> Vec<PriceBar> {
    fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn save_price_bar_cache(path: &PathBuf, bars: &[PriceBar]) -> Result<(), String> {
    let bytes = serde_json::to_vec(bars).map_err(|e| format!("encode price-bar cache: {e}"))?;
    fs::write(path, bytes).map_err(|e| format!("write price-bar cache: {e}"))
}

fn aggregate_minute_bars(records: Vec<HistoricalRecord>) -> Vec<PriceBar> {
    const MINUTE_US: i64 = 60_000_000;
    let mut bars = BTreeMap::<i64, PriceBar>::new();
    for record in records {
        let HistoricalRecord::Tick {
            datetime_us,
            price,
            volume,
            ..
        } = record
        else {
            continue;
        };
        if !price.is_finite() || !volume.is_finite() {
            continue;
        }
        let start_datetime_us = datetime_us.div_euclid(MINUTE_US) * MINUTE_US;
        bars.entry(start_datetime_us)
            .and_modify(|bar| {
                bar.high = bar.high.max(price);
                bar.low = bar.low.min(price);
                bar.close = price;
                bar.volume += volume;
            })
            .or_insert(PriceBar {
                start_datetime_us,
                open: price,
                high: price,
                low: price,
                close: price,
                volume,
            });
    }
    bars.into_values().collect()
}

fn latest_history_end(now: i64) -> i64 {
    const DAY: i64 = 86_400;
    const FRIDAY_CLOSE_UTC: i64 = 20 * 3_600 + 59 * 60;
    let days = now.div_euclid(DAY);
    let seconds = now.rem_euclid(DAY);
    // Unix epoch was a Thursday: 0=Sunday, 5=Friday, 6=Saturday.
    let weekday = (days + 4).rem_euclid(7);
    match weekday {
        6 => (days - 1) * DAY + FRIDAY_CLOSE_UTC,
        0 if seconds < 22 * 3_600 => (days - 2) * DAY + FRIDAY_CLOSE_UTC,
        _ => now.saturating_sub(60),
    }
}

fn load_snapshot_history(
    path: &PathBuf,
    underlying: &str,
    limit: usize,
) -> VecDeque<AnalyticsSnapshot> {
    let Ok(file) = fs::File::open(path) else {
        return VecDeque::with_capacity(limit);
    };
    let mut snapshots = VecDeque::with_capacity(limit);
    let mut walls = StickyWalls::default();
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(mut snapshot) = serde_json::from_str::<AnalyticsSnapshot>(&line) else {
            continue;
        };
        if snapshot.underlying != underlying || snapshot.underlying_price.is_none() {
            continue;
        }
        walls.update(&mut snapshot);
        if snapshots.len() == limit {
            snapshots.pop_front();
        }
        snapshots.push_back(snapshot);
    }
    snapshots
}

fn load_validation_history(
    path: &PathBuf,
    underlying: &str,
    limit: usize,
) -> VecDeque<ValidationPoint> {
    let Ok(file) = fs::File::open(path) else {
        return VecDeque::with_capacity(limit);
    };
    let mut points = VecDeque::with_capacity(limit);
    let mut walls = StickyWalls::default();
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(mut snapshot) = serde_json::from_str::<AnalyticsSnapshot>(&line) else {
            continue;
        };
        if snapshot.underlying != underlying {
            continue;
        }
        walls.update(&mut snapshot);
        let Ok(point) = ValidationPoint::try_from(&snapshot) else {
            continue;
        };
        if points.len() == limit {
            points.pop_front();
        }
        points.push_back(point);
    }
    points
}

fn load_heatmap_history(path: &PathBuf, underlying: &str, limit: usize) -> VecDeque<HeatmapPoint> {
    let Ok(file) = fs::File::open(path) else {
        return VecDeque::with_capacity(limit);
    };
    let mut points = VecDeque::with_capacity(limit);
    let mut walls = StickyWalls::default();
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(mut snapshot) = serde_json::from_str::<AnalyticsSnapshot>(&line) else {
            continue;
        };
        if snapshot.underlying != underlying {
            continue;
        }
        walls.update(&mut snapshot);
        push_heatmap_point(&mut points, &snapshot, limit);
    }
    points
}

fn select_contracts(
    contracts: Vec<OptionContract>,
    center: f64,
    config: &OptionsConfig,
) -> Vec<OptionContract> {
    select_contracts_at(contracts, center, config, now_us())
}

fn select_contracts_at(
    mut contracts: Vec<OptionContract>,
    center: f64,
    config: &OptionsConfig,
    as_of_us: i64,
) -> Vec<OptionContract> {
    contracts.retain(|contract| {
        market_data::date_to_unix(&contract.expiration)
            .is_some_and(|expiry| i64::from(expiry) + 21 * 3_600 > as_of_us / 1_000_000)
    });
    contracts.sort_by(|a, b| {
        a.expiration
            .cmp(&b.expiration)
            .then_with(|| a.strike.total_cmp(&b.strike))
    });
    let mut expirations: Vec<_> = contracts.iter().map(|c| c.expiration.clone()).collect();
    expirations.sort();
    expirations.dedup();
    expirations.truncate(config.max_expirations);
    contracts.retain(|c| expirations.contains(&c.expiration));
    let mut strikes: Vec<_> = contracts.iter().map(|c| c.strike).collect();
    strikes.sort_by(|a, b| (a - center).abs().total_cmp(&(b - center).abs()));
    strikes.dedup_by(|a, b| (*a - *b).abs() < 1e-9);
    strikes.truncate(config.strikes_each_side.saturating_mul(2).saturating_add(1));
    contracts.retain(|c| strikes.iter().any(|v| (*v - c.strike).abs() < 1e-9));
    contracts.sort_by(|a, b| {
        a.expiration
            .cmp(&b.expiration)
            .then_with(|| a.strike.total_cmp(&b.strike))
            .then_with(|| (a.option_type as u8).cmp(&(b.option_type as u8)))
    });
    contracts.truncate(config.max_contracts);
    contracts
}

fn apply_event(engine: &mut Engine, event: MarketEvent) {
    match event {
        MarketEvent::LastTrade {
            symbol_id,
            price,
            is_snapshot: false,
            ..
        } if symbol_id == engine.underlying_id => engine.underlying_price = Some(price),
        MarketEvent::LastTrade {
            symbol_id,
            volume,
            at_bid_or_ask,
            is_snapshot: false,
            ..
        } => {
            if let Some(state) = engine.states.get_mut(&symbol_id) {
                match at_bid_or_ask {
                    2 => state.buy_volume += volume,
                    1 => state.sell_volume += volume,
                    _ => state.unknown_volume += volume,
                }
            }
        }
        MarketEvent::BestBidAsk {
            symbol_id,
            bid_price,
            ask_price,
            ..
        } if symbol_id == engine.underlying_id => {
            engine.underlying_price = Some((bid_price + ask_price) / 2.0)
        }
        MarketEvent::BestBidAsk {
            symbol_id,
            bid_price,
            ask_price,
            ..
        } => {
            if let Some(state) = engine.states.get_mut(&symbol_id) {
                state.bid = Some(bid_price);
                state.ask = Some(ask_price);
            }
        }
        MarketEvent::Snapshot {
            symbol_id,
            snapshot,
        } => {
            if symbol_id == engine.underlying_id {
                engine.underlying_price = snapshot.last.or(engine.underlying_price);
            } else if let Some(state) = engine.states.get_mut(&symbol_id) {
                if let Some(oi) = snapshot.open_interest {
                    state.oi = u64::from(oi);
                }
            }
        }
        _ => {}
    }
}

struct EventWriter(std_mpsc::Sender<String>);

impl EventWriter {
    fn new(path: PathBuf) -> Result<Self, String> {
        let (sender, receiver) = std_mpsc::channel::<String>();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open options snapshot log: {e}"))?;
        std::thread::Builder::new()
            .name("options-snapshot-writer".to_owned())
            .spawn(move || {
                while let Ok(line) = receiver.recv() {
                    let _ = writeln!(file, "{line}");
                    let _ = file.flush();
                }
            })
            .map_err(|e| format!("start options snapshot writer: {e}"))?;
        Ok(Self(sender))
    }
    fn write(&self, line: String) {
        let _ = self.0.send(line);
    }
}

async fn serve_http(
    address: &str,
    markets: Arc<BTreeMap<String, Arc<MarketView>>>,
    access_token: Option<Arc<String>>,
    feed_connected: Arc<AtomicBool>,
) -> Result<(), String> {
    let listener = TcpListener::bind(address)
        .await
        .map_err(|e| e.to_string())?;
    loop {
        let (stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let views = Arc::clone(&markets);
        let access_token = access_token.clone();
        let feed_connected = Arc::clone(&feed_connected);
        tokio::spawn(async move {
            let _ = respond(stream, views, access_token, feed_connected).await;
        });
    }
}

async fn respond(
    mut stream: TcpStream,
    markets: Arc<BTreeMap<String, Arc<MarketView>>>,
    access_token: Option<Arc<String>>,
    feed_connected: Arc<AtomicBool>,
) -> RespondResult {
    let request = read_http_request(&mut stream).await?;
    let request_text = String::from_utf8_lossy(&request);
    let request_line = request_text.lines().next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or("GET");
    let path = request_parts.next().unwrap_or("/");
    let (route, query) = path.split_once('?').unwrap_or((path, ""));
    if let Some(expected) = access_token.as_deref() {
        if route == "/login" {
            if method == "POST" {
                let supplied = request_text
                    .split_once("\r\n\r\n")
                    .and_then(|(_, body)| body.strip_prefix("token="))
                    .unwrap_or_default();
                if constant_time_eq(supplied.as_bytes(), expected.as_bytes()) {
                    return write_http_response(
                        &mut stream,
                        "303 See Other",
                        "text/plain; charset=utf-8",
                        "登录成功",
                        &format!(
                            "Location: /\r\nSet-Cookie: options_session={expected}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=86400\r\n"
                        ),
                    )
                    .await;
                }
                return write_http_response(
                    &mut stream,
                    "401 Unauthorized",
                    "text/html; charset=utf-8",
                    LOGIN_PAGE_INVALID,
                    "",
                )
                .await;
            }
            return write_http_response(
                &mut stream,
                "200 OK",
                "text/html; charset=utf-8",
                LOGIN_PAGE,
                "",
            )
            .await;
        }
        if route == "/logout" {
            return write_http_response(
                &mut stream,
                "303 See Other",
                "text/plain; charset=utf-8",
                "已退出",
                "Location: /login\r\nSet-Cookie: options_session=; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=0\r\n",
            )
            .await;
        }
        if !request_is_authenticated(&request_text, expected) {
            let is_api = route.starts_with("/api/");
            return write_http_response(
                &mut stream,
                if is_api {
                    "401 Unauthorized"
                } else {
                    "303 See Other"
                },
                if is_api {
                    "application/json"
                } else {
                    "text/plain; charset=utf-8"
                },
                if is_api {
                    "{\"error\":\"authentication required\"}"
                } else {
                    "需要登录"
                },
                if is_api { "" } else { "Location: /login\r\n" },
            )
            .await;
        }
    }
    let requested = query
        .split('&')
        .find_map(|part| part.strip_prefix("symbol="))
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| "ES".to_owned());
    let view = markets.get(&requested).or_else(|| markets.values().next());
    let (content_type, body, status) = match route {
        "/" | "/index.html" => ("text/html; charset=utf-8", DASHBOARD.to_owned(), "200 OK"),
        "/api/v1/health" => {
            let now = now_us();
            let mut ages = BTreeMap::new();
            for (symbol, view) in markets.iter() {
                let snapshot = view.snapshot.read().await;
                ages.insert(symbol.clone(), (now - snapshot.as_of_us).max(0) / 1_000_000);
            }
            let connected = feed_connected.load(Ordering::Relaxed);
            let fresh = !ages.is_empty() && ages.values().all(|age| *age <= 30);
            (
                "application/json",
                format!(
                    "{{\"status\":\"{}\",\"rithmic_connected\":{},\"snapshots_fresh\":{},\"snapshot_age_seconds\":{}}}",
                    if connected && fresh { "ok" } else { "degraded" },
                    connected,
                    fresh,
                    serde_json::to_string(&ages)?
                ),
                "200 OK",
            )
        }
        "/api/v1/markets" => (
            "application/json",
            serde_json::to_string(&markets.keys().cloned().collect::<Vec<_>>())?,
            "200 OK",
        ),
        "/api/v1/analytics" | "/api/v1/options" => (
            "application/json",
            serde_json::to_string(&*view.expect("at least one market").snapshot.read().await)?,
            "200 OK",
        ),
        "/api/v1/replay" => (
            "application/json",
            serde_json::to_string(&*view.expect("at least one market").history.read().await)?,
            "200 OK",
        ),
        "/api/v1/heatmap" => (
            "application/json",
            serde_json::to_string(
                &*view
                    .expect("at least one market")
                    .heatmap_history
                    .read()
                    .await,
            )?,
            "200 OK",
        ),
        "/api/v1/bars" => (
            "application/json",
            serde_json::to_string(&*view.expect("at least one market").bars.read().await)?,
            "200 OK",
        ),
        "/api/v1/validation" => (
            "application/json",
            serde_json::to_string(&touch_validation(
                &*view
                    .expect("at least one market")
                    .validation_history
                    .read()
                    .await,
            ))?,
            "200 OK",
        ),
        _ => (
            "application/json",
            "{\"error\":\"not found\"}".to_owned(),
            "404 Not Found",
        ),
    };
    write_http_response(&mut stream, status, content_type, &body, "").await
}

async fn read_http_request(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    const MAX_REQUEST_SIZE: usize = 16 * 1024;
    let mut request = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 4096];
    loop {
        let size = stream.read(&mut chunk).await?;
        if size == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..size]);
        if request.len() > MAX_REQUEST_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP request exceeds 16 KiB",
            ));
        }
        let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if request.len() >= header_end + 4 + content_length {
            break;
        }
    }
    Ok(request)
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
    extra_headers: &str,
) -> RespondResult {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{extra_headers}Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    Ok(())
}

fn request_is_authenticated(request: &str, expected: &str) -> bool {
    request.lines().any(|line| {
        let line = line.trim_end_matches('\r');
        line.strip_prefix("Authorization: Bearer ")
            .or_else(|| line.strip_prefix("authorization: Bearer "))
            .is_some_and(|token| constant_time_eq(token.as_bytes(), expected.as_bytes()))
            || line
                .strip_prefix("Cookie: ")
                .or_else(|| line.strip_prefix("cookie: "))
                .is_some_and(|cookies| {
                    cookies.split(';').any(|cookie| {
                        cookie
                            .trim()
                            .strip_prefix("options_session=")
                            .is_some_and(|token| {
                                constant_time_eq(token.as_bytes(), expected.as_bytes())
                            })
                    })
                })
    })
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

fn load_access_token() -> Result<Option<Arc<String>>, String> {
    let token = match env::var("OPTIONS_ACCESS_TOKEN") {
        Ok(token) => Some(token),
        Err(_) => match env::var("OPTIONS_ACCESS_TOKEN_FILE") {
            Ok(path) => Some(
                fs::read_to_string(&path)
                    .map_err(|error| format!("read OPTIONS_ACCESS_TOKEN_FILE {path}: {error}"))?,
            ),
            Err(_) => None,
        },
    };
    token
        .map(|token| {
            let token = token.trim().to_owned();
            if token.len() < 32 {
                Err("OPTIONS_ACCESS_TOKEN must contain at least 32 characters".to_owned())
            } else {
                Ok(Arc::new(token))
            }
        })
        .transpose()
}

type RespondResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn midpoint(state: &ContractState) -> Option<f64> {
    match (state.bid, state.ask) {
        (Some(b), Some(a)) if b >= 0.0 && a >= b => Some((a + b) / 2.0),
        _ => None,
    }
}

fn years_to_expiry(expiration: &str, now_us: i64) -> Option<f64> {
    let end = i64::from(market_data::date_to_unix(expiration)?) + 21 * 3600;
    Some(
        ((end as f64 - now_us as f64 / 1_000_000.0) / (365.25 * 86400.0))
            .max(1.0 / (365.25 * 24.0)),
    )
}

fn strike_key(strike: f64) -> i64 {
    (strike * 1000.0).round() as i64
}
fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}
fn norm_pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt()
}
fn norm_cdf(x: f64) -> f64 {
    let k = 1.0 / (1.0 + 0.2316419 * x.abs());
    let p = 1.0
        - norm_pdf(x)
            * k
            * (0.319381530
                + k * (-0.356563782 + k * (1.781477937 + k * (-1.821255978 + k * 1.330274429))));
    if x >= 0.0 { p } else { 1.0 - p }
}
fn black76_price(kind: OptionType, future: f64, strike: f64, t: f64, rate: f64, vol: f64) -> f64 {
    let root_t = t.sqrt();
    let d1 = ((future / strike).ln() + 0.5 * vol * vol * t) / (vol * root_t);
    let d2 = d1 - vol * root_t;
    let discount = (-rate * t).exp();
    match kind {
        OptionType::Call => discount * (future * norm_cdf(d1) - strike * norm_cdf(d2)),
        OptionType::Put => discount * (strike * norm_cdf(-d2) - future * norm_cdf(-d1)),
    }
}
fn black76_gamma(future: f64, strike: f64, t: f64, rate: f64, vol: f64) -> f64 {
    let d1 = ((future / strike).ln() + 0.5 * vol * vol * t) / (vol * t.sqrt());
    (-rate * t).exp() * norm_pdf(d1) / (future * vol * t.sqrt())
}
fn implied_volatility(
    kind: OptionType,
    future: f64,
    strike: f64,
    t: f64,
    rate: f64,
    price: f64,
) -> Option<f64> {
    if !(future > 0.0 && strike > 0.0 && price > 0.0 && t > 0.0) {
        return None;
    }
    let mut lo = 0.0001;
    let mut hi = 5.0;
    if black76_price(kind, future, strike, t, rate, hi) < price {
        return None;
    }
    for _ in 0..80 {
        let mid = (lo + hi) / 2.0;
        if black76_price(kind, future, strike, t, rate, mid) < price {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Some((lo + hi) / 2.0)
}

const LOGIN_PAGE: &str = r#"<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Rithmic Flow 登录</title><style>html,body{height:100%}body{margin:0;display:grid;place-items:center;background:#06080d;color:#edf4ff;font-family:system-ui,sans-serif}.box{width:min(390px,calc(100% - 32px));padding:30px;border:1px solid #202937;border-radius:12px;background:#0a0e15;box-shadow:0 24px 80px #0008}h1{font:700 20px ui-monospace,monospace;margin:0 0 8px;color:#26d9e8}p{color:#8491a5;font-size:13px;margin:0 0 22px}input,button{box-sizing:border-box;width:100%;height:42px;border-radius:7px;font:14px ui-monospace,monospace}input{border:1px solid #2a374a;background:#070a0f;color:#fff;padding:0 12px;margin-bottom:10px}button{border:1px solid #168897;background:#0b3037;color:#62edf5;cursor:pointer}</style></head><body><form class="box" method="post" action="/login"><h1>λ RITHMIC FLOW</h1><p>请输入访问 Token</p><input name="token" type="password" minlength="32" required autofocus autocomplete="current-password"><button type="submit">登录查看</button></form></body></html>"#;
const LOGIN_PAGE_INVALID: &str = r#"<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>登录失败</title><style>body{margin:0;height:100vh;display:grid;place-items:center;background:#06080d;color:#edf4ff;font-family:system-ui}.box{padding:28px;border:1px solid #743044;border-radius:10px;background:#0a0e15}a{color:#26d9e8}</style></head><body><div class="box"><p>Token 不正确。</p><a href="/login">返回重新登录</a></div></body></html>"#;
const DASHBOARD: &str = include_str!("options_dashboard.html");

#[cfg(test)]
mod tests {
    use super::*;

    fn validation_snapshot(as_of_us: i64, spot: f64, regime: &str) -> AnalyticsSnapshot {
        let config = OptionsConfig {
            root: "ES".into(),
            exchange: "CME".into(),
            underlying_symbol: "ESU6".into(),
            listen_addr: "x".into(),
            max_expirations: 1,
            strikes_each_side: 1,
            max_contracts: 2,
            center_price: Some(spot),
            risk_free_rate: 0.05,
            snapshot_secs: 3,
            data_dir: "x".into(),
        };
        let mut snapshot = AnalyticsSnapshot::empty(&config);
        snapshot.as_of_us = as_of_us;
        snapshot.session_started_us = 1;
        snapshot.underlying_price = Some(spot);
        snapshot.put_trigger = Some(95.0);
        snapshot.regime = regime.to_owned();
        snapshot
    }

    fn validation_report(history: &VecDeque<AnalyticsSnapshot>) -> TouchValidationReport {
        let points = history
            .iter()
            .filter_map(|snapshot| ValidationPoint::try_from(snapshot).ok())
            .collect();
        touch_validation(&points)
    }
    #[test]
    fn black76_iv_round_trip() {
        let price = black76_price(OptionType::Call, 6000.0, 6050.0, 7.0 / 365.0, 0.04, 0.22);
        let iv =
            implied_volatility(OptionType::Call, 6000.0, 6050.0, 7.0 / 365.0, 0.04, price).unwrap();
        assert!((iv - 0.22).abs() < 1e-6);
    }

    #[test]
    fn zero_gamma_prefers_the_crossing_nearest_spot() {
        let profile = [
            (80.0, -2.0),
            (82.0, 2.0),
            (98.0, 4.0),
            (102.0, -4.0),
            (118.0, -2.0),
            (120.0, 2.0),
        ];
        let crossing = nearest_zero_crossing(&profile, 100.0).unwrap();
        assert!((crossing - 100.0).abs() < 1e-9);
    }

    #[test]
    fn actionable_trigger_balances_strength_distance_and_direction() {
        let upper = distance_weighted_level(
            [
                (101.0, 55.0, 100.0),
                (102.0, 5.0, 100.0),
                (105.0, 80.0, 100.0),
                (140.0, 120.0, 100.0),
                (95.0, 500.0, 100.0),
            ],
            100.0,
            true,
        );
        let lower = distance_weighted_level(
            [
                (99.0, 45.0, 100.0),
                (98.0, 5.0, 100.0),
                (95.0, 70.0, 100.0),
                (60.0, 140.0, 100.0),
                (105.0, 500.0, 100.0),
            ],
            100.0,
            false,
        );
        assert_eq!(upper, Some(105.0));
        assert_eq!(lower, Some(95.0));
    }

    #[test]
    fn walls_stay_on_their_directional_side_and_reduce_atm_bias() {
        let upper = directional_wall_level(
            [
                (99.0, 1_000.0),
                (101.0, 900.0),
                (105.0, 100.0),
                (110.0, 200.0),
            ],
            100.0,
            true,
        );
        let lower = directional_wall_level(
            [
                (101.0, 1_000.0),
                (99.0, 900.0),
                (95.0, 100.0),
                (90.0, 200.0),
            ],
            100.0,
            false,
        );
        assert_eq!(upper, Some(110.0));
        assert_eq!(lower, Some(90.0));
        assert_eq!(wall_strength(Some(1_000.0), 1.0), Some(1_000.0_f64.sqrt()));
        assert_eq!(wall_strength(Some(400.0), 100.0), Some(200.0));
    }

    #[test]
    fn sticky_walls_recompute_legacy_snapshot_levels_from_strike_inventory() {
        let mut snapshot = validation_snapshot(1, 100.0, "long-gamma");
        snapshot.call_wall = Some(101.0);
        snapshot.put_wall = Some(101.0);
        snapshot.flow_call_wall = Some(99.0);
        snapshot.flow_put_wall = Some(101.0);
        snapshot.strikes = serde_json::from_value(serde_json::json!([
            {
                "expiration": "2026-09-18",
                "strike": 105.0,
                "call_oi": 1,
                "call_gex": 1000.0,
                "call_net_flow": 1.0,
                "call_flow_gex": 1000.0
            },
            {
                "expiration": "2026-09-18",
                "strike": 110.0,
                "call_oi": 100,
                "call_gex": 400.0,
                "call_net_flow": 100.0,
                "call_flow_gex": 400.0
            },
            {
                "expiration": "2026-09-18",
                "strike": 95.0,
                "put_oi": 1,
                "put_gex": -1000.0,
                "put_net_flow": -1.0,
                "put_flow_gex": 1000.0
            },
            {
                "expiration": "2026-09-18",
                "strike": 90.0,
                "put_oi": 100,
                "put_gex": -400.0,
                "put_net_flow": -100.0,
                "put_flow_gex": 400.0
            }
        ]))
        .unwrap();
        let mut walls = StickyWalls::default();
        walls.update(&mut snapshot);
        assert_eq!(snapshot.call_wall, Some(110.0));
        assert_eq!(snapshot.put_wall, Some(90.0));
        assert_eq!(snapshot.flow_call_wall, Some(110.0));
        assert_eq!(snapshot.flow_put_wall, Some(90.0));
    }

    #[test]
    fn sticky_wall_requires_a_persistent_material_challenger() {
        let mut wall = StickyWall::new(Some(105.0));
        let strengths = [(105.0, 59.0), (110.0, 100.0)];
        assert_eq!(
            wall.update(Some(110.0), strengths, 100.0, true, 0),
            Some(105.0)
        );
        assert_eq!(
            wall.update(
                Some(110.0),
                strengths,
                100.0,
                true,
                WALL_CHALLENGER_CONFIRM_US - 1,
            ),
            Some(105.0)
        );
        assert_eq!(
            wall.update(
                Some(110.0),
                strengths,
                100.0,
                true,
                WALL_CHALLENGER_CONFIRM_US,
            ),
            Some(110.0)
        );
    }

    #[test]
    fn sticky_wall_publishes_touch_before_rotating_forward() {
        let mut wall = StickyWall::new(Some(105.0));
        let strengths = [(105.0, 100.0), (110.0, 120.0)];
        assert_eq!(
            wall.update(Some(110.0), strengths, 104.0, true, 0),
            Some(105.0)
        );
        assert_eq!(
            wall.update(Some(110.0), strengths, 104.0, true, 1),
            Some(110.0)
        );
    }

    #[test]
    fn sticky_trigger_holds_qualified_level_until_touch_then_advances() {
        let mut trigger = StickyTrigger::new(Some(105.0), 0);
        let strengths = [(105.0, 30.0, 100.0), (110.0, 100.0, 100.0)];
        assert_eq!(
            trigger.update(Some(110.0), strengths, 100.0, true, 0),
            Some(105.0)
        );
        assert_eq!(
            trigger.update(Some(110.0), strengths, 104.0, true, 1),
            Some(105.0)
        );
        assert_eq!(
            trigger.update(Some(110.0), strengths, 104.0, true, 2),
            Some(110.0)
        );
    }

    #[test]
    fn sticky_trigger_replaces_level_that_falls_below_strength_floor() {
        let mut trigger = StickyTrigger::new(Some(95.0), 0);
        let strengths = [(90.0, 100.0, 100.0), (95.0, 20.0, 100.0)];
        assert_eq!(
            trigger.update(Some(90.0), strengths, 100.0, false, 0),
            Some(90.0)
        );
    }

    #[test]
    fn sticky_trigger_refreshes_to_near_candidate_after_one_hour() {
        let mut trigger = StickyTrigger::new(Some(95.0), 0);
        let strengths = [(95.0, 100.0, 100.0), (99.0, 35.0, 100.0)];
        assert_eq!(
            trigger.update(Some(99.0), strengths, 100.0, false, TRIGGER_HOLD_US - 1),
            Some(95.0)
        );
        assert_eq!(
            trigger.update(Some(99.0), strengths, 100.0, false, TRIGGER_HOLD_US),
            Some(99.0)
        );
    }

    #[test]
    fn validation_scores_short_gamma_put_continuation_without_lookahead() {
        let minute = 60 * 1_000_000;
        let history = VecDeque::from([
            validation_snapshot(0, 100.0, "short-gamma"),
            validation_snapshot(10 * minute, 95.0, "short-gamma"),
            validation_snapshot(20 * minute, 90.0, "short-gamma"),
            validation_snapshot(60 * minute, 89.0, "short-gamma"),
        ]);
        let report = validation_report(&history);
        let put = report.levels.get("put_trigger").unwrap();
        assert_eq!(put.completed_signals, 1);
        assert_eq!(put.touches, 1);
        assert_eq!(put.effective, 1);
        assert_eq!(put.failed, 0);
        assert_eq!(put.touch_rate_pct, Some(100.0));
        assert_eq!(put.effective_rate_pct, Some(100.0));
        assert_eq!(put.superseded_before_touch, 0);
        assert_eq!(put.actionable_signals, 1);
        assert_eq!(put.actionable_touches, 1);
        assert_eq!(put.actionable_effective, 1);
        assert_eq!(put.actionable_touch_rate_pct, Some(100.0));
        assert_eq!(put.actionable_effective_rate_pct, Some(100.0));
    }

    #[test]
    fn validation_keeps_raw_failure_but_excludes_level_replaced_before_touch() {
        let minute = 60 * 1_000_000;
        let first = validation_snapshot(0, 100.0, "long-gamma");
        let mut replaced = validation_snapshot(10 * minute, 96.0, "long-gamma");
        replaced.put_trigger = Some(90.0);
        let mut old_level_touch = validation_snapshot(20 * minute, 95.0, "long-gamma");
        old_level_touch.put_trigger = Some(90.0);
        let mut old_level_failure = validation_snapshot(21 * minute, 92.0, "long-gamma");
        old_level_failure.put_trigger = Some(90.0);
        let mut complete = validation_snapshot(60 * minute, 94.0, "long-gamma");
        complete.put_trigger = Some(90.0);
        let history = VecDeque::from([
            first,
            replaced,
            old_level_touch,
            old_level_failure,
            complete,
        ]);

        let report = validation_report(&history);
        let put = report.levels.get("put_trigger").unwrap();
        assert_eq!(put.completed_signals, 1);
        assert_eq!(put.touches, 1);
        assert_eq!(put.failed, 1);
        assert_eq!(put.superseded_before_touch, 1);
        assert_eq!(put.actionable_signals, 0);
        assert_eq!(put.actionable_touches, 0);
        assert_eq!(put.actionable_failed, 0);
        assert_eq!(put.actionable_touch_rate_pct, None);
        assert_eq!(put.actionable_effective_rate_pct, None);
    }

    #[test]
    fn validation_excludes_incomplete_future_windows() {
        let minute = 60 * 1_000_000;
        let history = VecDeque::from([
            validation_snapshot(0, 100.0, "short-gamma"),
            validation_snapshot(30 * minute, 95.0, "short-gamma"),
        ]);
        let report = validation_report(&history);
        assert!(report.levels.is_empty());
    }

    #[test]
    fn validation_uses_five_point_momentum_as_continuation_override() {
        let minute = 60 * 1_000_000;
        let mut signal = validation_snapshot(0, 100.0, "long-gamma");
        signal.call_trigger = Some(105.0);
        let mut touch = validation_snapshot(5 * minute, 106.0, "long-gamma");
        touch.call_trigger = Some(105.0);
        let mut continuation = validation_snapshot(10 * minute, 110.0, "long-gamma");
        continuation.call_trigger = Some(105.0);
        let mut complete = validation_snapshot(60 * minute, 109.0, "long-gamma");
        complete.call_trigger = Some(105.0);
        let history = VecDeque::from([signal, touch, continuation, complete]);
        let report = validation_report(&history);
        let call = &report.levels["call_trigger"];
        assert_eq!(call.touches, 1);
        assert_eq!(call.effective, 1);
        assert_eq!(call.failed, 0);
    }

    #[test]
    fn validation_keeps_sub_five_point_approach_as_mean_reversion() {
        let minute = 60 * 1_000_000;
        let mut signal = validation_snapshot(0, 100.0, "long-gamma");
        signal.call_trigger = Some(105.0);
        let mut touch = validation_snapshot(5 * minute, 104.0, "long-gamma");
        touch.call_trigger = Some(105.0);
        let mut breakout = validation_snapshot(10 * minute, 110.0, "long-gamma");
        breakout.call_trigger = Some(105.0);
        let mut complete = validation_snapshot(60 * minute, 109.0, "long-gamma");
        complete.call_trigger = Some(105.0);
        let history = VecDeque::from([signal, touch, breakout, complete]);
        let report = validation_report(&history);
        let call = &report.levels["call_trigger"];
        assert_eq!(call.touches, 1);
        assert_eq!(call.effective, 0);
        assert_eq!(call.failed, 1);
    }
    #[test]
    fn selects_nearby_strikes_and_expiries() {
        let mut values = Vec::new();
        for expiry in ["2026-09-18", "2026-09-25"] {
            for strike in [5900.0, 5950.0, 6000.0, 6050.0, 6100.0] {
                for option_type in [OptionType::Call, OptionType::Put] {
                    values.push(OptionContract {
                        symbol: format!("x{strike}{option_type:?}"),
                        exchange: "CME".into(),
                        underlying: "ES".into(),
                        expiration: expiry.into(),
                        strike,
                        option_type,
                        multiplier: 50.0,
                        tick_size: Some(0.25),
                    });
                }
            }
        }
        let mut config = OptionsConfig {
            root: "ES".into(),
            exchange: "CME".into(),
            underlying_symbol: "ESZ6".into(),
            listen_addr: "x".into(),
            max_expirations: 1,
            strikes_each_side: 1,
            max_contracts: 20,
            center_price: Some(6000.0),
            risk_free_rate: 0.05,
            snapshot_secs: 3,
            data_dir: "x".into(),
        };
        let as_of_us = i64::from(market_data::date_to_unix("2026-09-17").unwrap()) * 1_000_000;
        let selected = select_contracts_at(values, 6000.0, &config, as_of_us);
        assert_eq!(selected.len(), 6);
        assert!(selected.iter().all(|c| c.expiration == "2026-09-18"));
        config.max_expirations = 2;
    }

    #[test]
    fn expired_contracts_are_excluded_before_expiry_selection() {
        let contract = |expiration: &str| OptionContract {
            symbol: expiration.to_owned(),
            exchange: "CME".into(),
            underlying: "ESU6".into(),
            expiration: expiration.to_owned(),
            strike: 6000.0,
            option_type: OptionType::Call,
            multiplier: 50.0,
            tick_size: Some(0.25),
        };
        let config = OptionsConfig {
            root: "ES".into(),
            exchange: "CME".into(),
            underlying_symbol: "ESU6".into(),
            listen_addr: "x".into(),
            max_expirations: 2,
            strikes_each_side: 1,
            max_contracts: 20,
            center_price: Some(6000.0),
            risk_free_rate: 0.05,
            snapshot_secs: 3,
            data_dir: "x".into(),
        };
        let after_first_expiry =
            (i64::from(market_data::date_to_unix("2026-09-18").unwrap()) + 22 * 3_600) * 1_000_000;
        let selected = select_contracts_at(
            vec![contract("2026-09-18"), contract("2026-09-25")],
            6000.0,
            &config,
            after_first_expiry,
        );
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].expiration, "2026-09-25");
    }

    #[test]
    fn history_window_skips_closed_weekend() {
        // 1970-01-02 through 1970-01-04 were Friday through Sunday.
        let friday = 86_400 + 12 * 3_600;
        let saturday = 2 * 86_400 + 12 * 3_600;
        let sunday_before_open = 3 * 86_400 + 12 * 3_600;
        let sunday_after_open = 3 * 86_400 + 23 * 3_600;
        assert_eq!(latest_history_end(friday), friday - 60);
        assert_eq!(latest_history_end(saturday), 86_400 + 20 * 3_600 + 59 * 60);
        assert_eq!(
            latest_history_end(sunday_before_open),
            86_400 + 20 * 3_600 + 59 * 60
        );
        assert_eq!(
            latest_history_end(sunday_after_open),
            sunday_after_open - 60
        );
    }

    #[test]
    fn aggregates_history_ticks_into_minute_ohlcv() {
        let ticks = vec![
            HistoricalRecord::Tick {
                datetime_us: 60_000_000,
                price: 100.0,
                volume: 2.0,
                at_bid_or_ask: 1,
            },
            HistoricalRecord::Tick {
                datetime_us: 75_000_000,
                price: 102.0,
                volume: 3.0,
                at_bid_or_ask: 2,
            },
            HistoricalRecord::Tick {
                datetime_us: 90_000_000,
                price: 99.0,
                volume: 5.0,
                at_bid_or_ask: 1,
            },
            HistoricalRecord::Tick {
                datetime_us: 120_000_000,
                price: 101.0,
                volume: 7.0,
                at_bid_or_ask: 2,
            },
        ];
        let bars = aggregate_minute_bars(ticks);
        assert_eq!(bars.len(), 2);
        assert_eq!(
            (
                bars[0].open,
                bars[0].high,
                bars[0].low,
                bars[0].close,
                bars[0].volume
            ),
            (100.0, 102.0, 99.0, 99.0, 10.0)
        );
        assert_eq!(
            (bars[1].open, bars[1].close, bars[1].volume),
            (101.0, 101.0, 7.0)
        );
    }

    #[test]
    fn dashboard_auth_accepts_bearer_or_exact_cookie_only() {
        let token = "0123456789abcdef0123456789abcdef";
        assert!(request_is_authenticated(
            &format!("GET / HTTP/1.1\r\nAuthorization: Bearer {token}\r\n\r\n"),
            token
        ));
        assert!(request_is_authenticated(
            &format!("GET / HTTP/1.1\r\nCookie: x=1; options_session={token}\r\n\r\n"),
            token
        ));
        assert!(!request_is_authenticated(
            "GET / HTTP/1.1\r\nCookie: options_session=wrong\r\n\r\n",
            token
        ));
        assert!(!constant_time_eq(token.as_bytes(), b"short"));
    }

    #[test]
    fn heatmap_history_keeps_latest_frame_per_minute_and_honors_limit() {
        let mut history = VecDeque::new();
        let mut snapshot = validation_snapshot(61_000_000, 100.0, "long-gamma");
        snapshot.call_wall = Some(105.0);
        snapshot.put_wall = Some(95.0);
        snapshot.flow_call_wall = Some(110.0);
        snapshot.flow_put_wall = Some(90.0);
        snapshot.strikes = vec![
            serde_json::from_value(serde_json::json!({
                "expiration": "20260917",
                "strike": 100.0,
                "net_gex": 1.0
            }))
            .unwrap(),
        ];
        push_heatmap_point(&mut history, &snapshot, 2);

        snapshot.as_of_us = 119_000_000;
        snapshot.strikes[0].net_gex = Some(2.0);
        push_heatmap_point(&mut history, &snapshot, 2);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].as_of_us, 119_000_000);
        assert_eq!(history[0].call_wall, Some(105.0));
        assert_eq!(history[0].put_wall, Some(95.0));
        assert_eq!(history[0].flow_call_wall, Some(110.0));
        assert_eq!(history[0].flow_put_wall, Some(90.0));
        assert_eq!(history[0].expirations[0].strikes[0].net_gex, Some(2.0));

        snapshot.as_of_us = 121_000_000;
        push_heatmap_point(&mut history, &snapshot, 2);
        snapshot.as_of_us = 181_000_000;
        push_heatmap_point(&mut history, &snapshot, 2);
        assert_eq!(history.len(), 2);
        assert_eq!(history.front().unwrap().as_of_us, 121_000_000);
        assert_eq!(history.back().unwrap().as_of_us, 181_000_000);
    }
}
