use std::{
    env,
    error::Error,
    fmt,
    time::{Duration, SystemTime},
};

use rithmic_rs::{
    ConnectStrategy, LoginConfig, RithmicConfig, RithmicError, RithmicHistoryPlant,
    RithmicHistoryPlantHandle, TimeBarType, rti::messages::RithmicMessage,
};
use tokio::sync::mpsc;

use crate::{
    connection::SharedConnection,
    identity::synthetic_mac,
    maintenance_retry::MaintenanceBackoff,
    market_gateway::{HistoricalRecord, HistoricalRequest, HistoricalResponse, HistoryDataClient},
};

#[derive(Debug)]
pub struct HistoryError(String);

impl fmt::Display for HistoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for HistoryError {}

pub struct RithmicHistoryFeed {
    commands: mpsc::Sender<HistoryCommand>,
}

struct HistorySession {
    plant: RithmicHistoryPlant,
    handle: RithmicHistoryPlantHandle,
}

#[derive(Debug)]
pub(crate) struct LoadHistoryError {
    pub(crate) message: String,
    pub(crate) connection_issue: bool,
}

impl LoadHistoryError {
    fn local(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            connection_issue: false,
        }
    }

    fn rithmic(error: RithmicError) -> Self {
        let connection_issue = error.is_connection_issue();
        Self {
            message: error.to_string(),
            connection_issue,
        }
    }
}

enum HistoryCommand {
    Load {
        request: HistoricalRequest,
        response: mpsc::Sender<Result<HistoricalResponse, String>>,
    },
}

/// How long one history request waits for the History Plant session before it
/// reports a local failure. The account allows a limited number of concurrent
/// sessions, so a slot that a recently stopped process still holds is a normal,
/// temporary condition: the request must fail with a clear reason (and let the
/// caller fall back to its cache) instead of hanging for minutes.
const HISTORY_SESSION_BUDGET: Duration = Duration::from_secs(20);

impl RithmicHistoryFeed {
    pub async fn connect_from_env() -> Result<Self, HistoryError> {
        Self::connect_with(SharedConnection::from_env()).await
    }

    pub async fn connect(config: RithmicConfig) -> Result<Self, HistoryError> {
        Self::connect_with(SharedConnection::from_config(config)).await
    }

    pub async fn connect_with(connection: SharedConnection) -> Result<Self, HistoryError> {
        let config = history_config(&connection)?;
        let initial_session = establish_history_session_with_backoff(&config).await?;
        Ok(Self::spawn(connection, Some(initial_session)))
    }

    /// Builds a history feed without waiting for the History Plant session.
    ///
    /// The market feed does not depend on history, so a startup path that must
    /// not stall uses this constructor: the command loop establishes the session
    /// on the first history request and after every connection loss, each time
    /// inside [`HISTORY_SESSION_BUDGET`] so the request can fail fast.
    pub async fn pending(config: RithmicConfig) -> Result<Self, HistoryError> {
        Self::pending_with(SharedConnection::from_config(config)).await
    }

    pub async fn pending_from_env() -> Result<Self, HistoryError> {
        Self::pending_with(SharedConnection::from_env()).await
    }

    /// Like [`pending_from_env`](Self::pending_from_env) but every session is
    /// established with the current settings of `connection`; after a settings
    /// change the next request drops the old session and logs in again.
    pub async fn pending_with(connection: SharedConnection) -> Result<Self, HistoryError> {
        history_config(&connection)?;
        Ok(Self::spawn(connection, None))
    }

    fn spawn(connection: SharedConnection, session: Option<HistorySession>) -> Self {
        let (commands, mut receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut session = session;
            let mut session_generation = connection.generation();
            while let Some(command) = receiver.recv().await {
                match command {
                    HistoryCommand::Load { request, response } => {
                        if session.is_some() && session_generation != connection.generation() {
                            eprintln!(
                                "[History] Connection settings changed; logging in to the History Plant again"
                            );
                            if let Some(stale) = session.take() {
                                let _ = stale.handle.disconnect().await;
                                let _ = stale.plant.await_shutdown().await;
                            }
                        }
                        let config = match history_config(&connection) {
                            Ok(config) => config,
                            Err(error) => {
                                let _ = response.send(Err(error.to_string())).await;
                                continue;
                            }
                        };
                        let chunks = match split_history_request(&request) {
                            Ok(chunks) => chunks,
                            Err(error) => {
                                let _ = response.send(Err(error.message)).await;
                                continue;
                            }
                        };
                        let total = chunks.len();
                        // Each chunk is one bounded request. A chunk that loses the
                        // connection (the History Plant closes idle sessions and
                        // drops very large replays) reconnects and is retried, so a
                        // long tick download survives a dropped socket instead of
                        // failing as a whole. Exactly one response is sent per
                        // chunk so the caller can track progress.
                        for (index, chunk) in chunks.into_iter().enumerate() {
                            let mut attempts = 0u8;
                            loop {
                                if session.is_none() {
                                    session_generation = connection.generation();
                                    session = match establish_history_session_within(
                                        &config,
                                        HISTORY_SESSION_BUDGET,
                                    )
                                    .await
                                    {
                                        Ok(established) => Some(established),
                                        Err(error) => {
                                            eprintln!(
                                                "[History] History Plant session unavailable: {error}"
                                            );
                                            None
                                        }
                                    };
                                }
                                let result = match session.as_ref() {
                                    Some(active) => {
                                        load_history_with_timeout(&active.handle, chunk.clone())
                                            .await
                                    }
                                    None => Err(LoadHistoryError::local(
                                        "Rithmic History Plant is unavailable; using cached or public history",
                                    )),
                                };
                                match result {
                                    Ok(mut loaded) => {
                                        loaded.is_final = index + 1 == total;
                                        if response.send(Ok(loaded)).await.is_err() {
                                            break;
                                        }
                                        break;
                                    }
                                    Err(error)
                                        if error.connection_issue
                                            && attempts < HISTORY_CHUNK_RETRIES =>
                                    {
                                        attempts += 1;
                                        eprintln!(
                                            "[History] Connection lost during chunk {}/{} ({}); reconnecting and retrying (attempt {attempts})",
                                            index + 1,
                                            total,
                                            error.message
                                        );
                                        if let Some(stale) = session.take() {
                                            stale.handle.abort();
                                            let _ = stale.plant.await_shutdown().await;
                                        }
                                        tokio::time::sleep(Duration::from_millis(500)).await;
                                    }
                                    Err(error) => {
                                        if error.connection_issue {
                                            if let Some(stale) = session.take() {
                                                stale.handle.abort();
                                                let _ = stale.plant.await_shutdown().await;
                                            }
                                        }
                                        let _ = response.send(Err(error.message)).await;
                                        break;
                                    }
                                }
                            }
                            if response.is_closed() {
                                break;
                            }
                        }
                    }
                }
            }
            if let Some(active) = session {
                let _ = active.handle.disconnect().await;
                let _ = active.plant.await_shutdown().await;
            }
        });
        Self { commands }
    }

    pub fn client(&self) -> HistoryDataClient {
        let commands = self.commands.clone();
        let (client_tx, mut client_rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Some((request, response)) = client_rx.recv().await {
                if commands
                    .send(HistoryCommand::Load { request, response })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        HistoryDataClient::new(client_tx)
    }
}

/// Retries per chunk after a lost History Plant connection.
const HISTORY_CHUNK_RETRIES: u8 = 2;

/// Split one request into the bounded requests the worker actually sends.
///
/// Time bars are cheap and travel as a single request. Tick-level replays
/// (raw ticks and N-trade bars) are split into short windows: the History
/// Plant drops the connection on very large replays, and a bounded window also
/// keeps memory flat and lets the caller show progress per chunk. The result is
/// deterministic for a given request, so a caller can compute the chunk count
/// up front with the same function.
pub(crate) fn split_history_request(
    request: &HistoricalRequest,
) -> Result<Vec<HistoricalRequest>, LoadHistoryError> {
    let (start, end) = historical_range(request)?;
    // Time bars: the History Plant closes the connection on replies much past
    // 10,000 records, so bound each request to a budget of bars. Ticks: a
    // fixed short window, since their density is unknown up front.
    let chunk_seconds = if request.record_interval > 0 {
        i64::from(request.record_interval).saturating_mul(history_bar_chunk_records())
    } else {
        history_tick_chunk_seconds()
    };
    let mut chunks = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let chunk_end = cursor
            .saturating_add(chunk_seconds.saturating_sub(1))
            .min(end);
        let mut chunk = request.clone();
        chunk.start_time = cursor;
        chunk.end_time = chunk_end;
        chunk.max_days = 0;
        chunks.push(chunk);
        if chunk_end == end {
            break;
        }
        cursor = chunk_end.saturating_add(1);
    }
    Ok(chunks)
}

fn historical_range(request: &HistoricalRequest) -> Result<(i64, i64), LoadHistoryError> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|error| LoadHistoryError::local(error.to_string()))?
        .as_secs()
        .min(i32::MAX as u64) as i64;
    let end = if request.end_time == 0 {
        now
    } else {
        request.end_time.min(i32::MAX as i64)
    };
    let mut start = if request.start_time > 0 {
        request.start_time
    } else if request.max_days > 0 {
        end.saturating_sub(i64::from(request.max_days) * 86_400)
            .max(1)
    } else {
        1
    };
    if request.max_days > 0 {
        start = start.max(end.saturating_sub(i64::from(request.max_days) * 86_400));
    }
    if start <= 0 || end <= 0 || start > end {
        return Err(LoadHistoryError::local("invalid historical time range"));
    }
    Ok((start, end))
}

/// Bars per time-bar replay request. 1-second bars for an hour is 3,600
/// records and returns in a few seconds; six hours in one request is closed
/// by the server.
fn history_bar_chunk_records() -> i64 {
    env::var("RITHMIC_HISTORY_BAR_CHUNK_RECORDS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|records| *records > 0)
        .unwrap_or(4_000)
}

/// Window of one tick-level replay request. Ten minutes of a liquid contract
/// is well under the 10,000-record cap and returns in a few seconds; larger
/// windows are where the History Plant starts closing the connection.
fn history_tick_chunk_seconds() -> i64 {
    if let Some(minutes) = env::var("RITHMIC_HISTORY_TICK_CHUNK_MINUTES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|minutes| *minutes > 0)
    {
        return i64::from(minutes).saturating_mul(60);
    }
    if let Some(hours) = env::var("RITHMIC_HISTORY_TICK_CHUNK_HOURS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|hours| *hours > 0)
    {
        return i64::from(hours).saturating_mul(3_600);
    }
    600
}

async fn establish_history_session(config: &RithmicConfig) -> Result<HistorySession, HistoryError> {
    let plant = RithmicHistoryPlant::connect(config, ConnectStrategy::Simple)
        .await
        .map_err(|error| {
            HistoryError(format!("Rithmic History Plant connection failed: {error}"))
        })?;
    let handle = plant.get_handle();
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    if let Err(error) = handle.login_with_config(login).await {
        handle.abort();
        let _ = plant.await_shutdown().await;
        return Err(HistoryError(format!(
            "Rithmic History Plant login failed: {error}"
        )));
    }
    Ok(HistorySession { plant, handle })
}

async fn establish_history_session_with_backoff(
    config: &RithmicConfig,
) -> Result<HistorySession, HistoryError> {
    let mut backoff = MaintenanceBackoff::from_env();
    loop {
        match establish_history_session(config).await {
            Ok(session) => return Ok(session),
            Err(error) if MaintenanceBackoff::is_retryable(&error.to_string()) => {
                backoff.wait("History", &error.to_string()).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Bounded variant of [`establish_history_session_with_backoff`] for callers
/// that must answer a request instead of waiting for the account's session slot.
async fn establish_history_session_within(
    config: &RithmicConfig,
    budget: Duration,
) -> Result<HistorySession, HistoryError> {
    match tokio::time::timeout(budget, establish_history_session_with_backoff(config)).await {
        Ok(result) => result,
        Err(_) => Err(HistoryError(format!(
            "Rithmic History Plant session was unavailable within {}s",
            budget.as_secs()
        ))),
    }
}

fn history_config(connection: &SharedConnection) -> Result<RithmicConfig, HistoryError> {
    connection
        .config()
        .map_err(|error| HistoryError(format!("Rithmic history configuration failed: {error}")))
}

async fn load_history_with_timeout(
    handle: &RithmicHistoryPlantHandle,
    request: HistoricalRequest,
) -> Result<HistoricalResponse, LoadHistoryError> {
    let Some(timeout) = history_request_timeout() else {
        return load_history(handle, request).await;
    };
    match tokio::time::timeout(timeout, load_history(handle, request)).await {
        Ok(result) => result,
        Err(_) => Err(LoadHistoryError {
            message: format!(
                "Rithmic historical replay timed out after {} seconds",
                timeout.as_secs()
            ),
            // The request future can be dropped while rithmic-rs still has it registered.
            // Retire the whole plant before retrying so a late response cannot corrupt the
            // following request's lifecycle.
            connection_issue: true,
        }),
    }
}

fn history_request_timeout() -> Option<Duration> {
    env::var("RITHMIC_HISTORY_REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
}

async fn load_history(
    handle: &RithmicHistoryPlantHandle,
    request: HistoricalRequest,
) -> Result<HistoricalResponse, LoadHistoryError> {
    let (start, end) = historical_range(&request)?;
    let start = i32::try_from(start)
        .map_err(|_| LoadHistoryError::local("historical start is outside Rithmic's i32 range"))?;
    let end = i32::try_from(end)
        .map_err(|_| LoadHistoryError::local("historical end is outside Rithmic's i32 range"))?;

    let mut responses = if request.tick_bar_length > 1 {
        handle
            .load_tick_bars_all(
                request.symbol.clone(),
                request.exchange.clone(),
                request.tick_bar_length,
                start,
                end,
            )
            .await
            .map_err(LoadHistoryError::rithmic)?
    } else if request.record_interval == 0 {
        handle
            .load_ticks_all(request.symbol.clone(), request.exchange.clone(), start, end)
            .await
            .map_err(LoadHistoryError::rithmic)?
    } else {
        let (bar_type, period) =
            time_bar_request(request.record_interval).map_err(LoadHistoryError::local)?;
        handle
            .load_time_bars_all(
                request.symbol.clone(),
                request.exchange.clone(),
                bar_type,
                period,
                start,
                end,
            )
            .await
            .map_err(LoadHistoryError::rithmic)?
    };

    if request.record_interval >= 86_400
        && responses.iter().all(|response| response.error.is_none())
        && !responses.iter().any(|response| {
            matches!(
                &response.message,
                RithmicMessage::ResponseTimeBarReplay(bar) if bar.marker.is_some()
            )
        })
    {
        // Some Rithmic Paper systems accept DAILY_BAR but return only the empty end marker.
        // A 1440-minute bar has the same fixed DTC interval and preserves server-provided
        // OHLCV, so retry without synthesizing bars from trades in this bridge.
        let period = request.record_interval / 60;
        eprintln!("[History] Rithmic returned no day bars; retrying as {period}-minute bars");
        responses = handle
            .load_time_bars_all(
                request.symbol,
                request.exchange,
                TimeBarType::MinuteBar,
                period,
                start,
                end,
            )
            .await
            .map_err(LoadHistoryError::rithmic)?;
    }

    let mut records = Vec::new();
    for response in responses {
        if let Some(error) = response.error {
            return Err(LoadHistoryError::rithmic(error));
        }
        match response.message {
            RithmicMessage::ResponseTickBarReplay(bar) if request.tick_bar_length > 1 => {
                let (Some(open), Some(high), Some(low), Some(close)) = (
                    bar.open_price,
                    bar.high_price,
                    bar.low_price,
                    bar.close_price,
                ) else {
                    continue;
                };
                let Some(seconds) = bar.data_bar_ssboe.first().copied() else {
                    continue;
                };
                let micros = bar.data_bar_usecs.first().copied().unwrap_or_default();
                records.push(HistoricalRecord::Bar {
                    start_datetime_us: i64::from(seconds) * 1_000_000
                        + i64::from(micros.clamp(0, 999_999)),
                    open,
                    high,
                    low,
                    close,
                    volume: bar.volume.unwrap_or_default() as f64,
                    num_trades: bar.num_trades.unwrap_or_default().min(u32::MAX as u64) as u32,
                    bid_volume: bar.bid_volume.unwrap_or_default() as f64,
                    ask_volume: bar.ask_volume.unwrap_or_default() as f64,
                });
            }
            RithmicMessage::ResponseTickBarReplay(tick) if request.record_interval == 0 => {
                let Some(price) = tick.close_price else {
                    continue;
                };
                let Some(volume) = tick.volume else { continue };
                let seconds = tick
                    .data_bar_ssboe
                    .get(1)
                    .or_else(|| tick.data_bar_ssboe.first())
                    .copied();
                let micros = tick
                    .data_bar_usecs
                    .get(1)
                    .or_else(|| tick.data_bar_usecs.first())
                    .copied();
                let Some(seconds) = seconds else { continue };
                let bid_volume = tick.bid_volume.unwrap_or_default() as f64;
                let ask_volume = tick.ask_volume.unwrap_or_default() as f64;
                let at_bid_or_ask = match (bid_volume > 0.0, ask_volume > 0.0) {
                    (true, false) => 1,
                    (false, true) => 2,
                    _ => 0,
                };
                records.push(HistoricalRecord::Tick {
                    datetime_us: i64::from(seconds) * 1_000_000
                        + i64::from(micros.unwrap_or_default().clamp(0, 999_999)),
                    price,
                    volume: volume as f64,
                    at_bid_or_ask,
                });
            }
            RithmicMessage::ResponseTimeBarReplay(bar) if request.record_interval > 0 => {
                let (Some(marker), Some(open), Some(high), Some(low), Some(close)) = (
                    bar.marker,
                    bar.open_price,
                    bar.high_price,
                    bar.low_price,
                    bar.close_price,
                ) else {
                    continue;
                };
                records.push(HistoricalRecord::Bar {
                    start_datetime_us: (i64::from(marker) - i64::from(request.record_interval))
                        * 1_000_000,
                    open,
                    high,
                    low,
                    close,
                    volume: bar.volume.unwrap_or_default() as f64,
                    num_trades: bar.num_trades.unwrap_or_default().min(u32::MAX as u64) as u32,
                    bid_volume: bar.bid_volume.unwrap_or_default() as f64,
                    ask_volume: bar.ask_volume.unwrap_or_default() as f64,
                });
            }
            _ => {}
        }
    }
    records.sort_by_key(HistoricalRecord::datetime_us);
    Ok(HistoricalResponse {
        request_id: request.request_id,
        record_interval: request.record_interval,
        records,
        is_final: false,
    })
}

fn time_bar_request(interval: i32) -> Result<(TimeBarType, i32), String> {
    match interval {
        1..=59 => Ok((TimeBarType::SecondBar, interval)),
        value if value > 0 && value % 604_800 == 0 => Ok((TimeBarType::WeeklyBar, value / 604_800)),
        value if value > 0 && value % 86_400 == 0 => Ok((TimeBarType::DailyBar, value / 86_400)),
        value if value > 0 && value % 60 == 0 => Ok((TimeBarType::MinuteBar, value / 60)),
        _ => Err(format!("unsupported historical record interval {interval}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_days_caps_explicit_start_without_expanding_a_narrower_range() {
        let end = 1_800_000_000;
        let mut request = HistoricalRequest {
            request_id: 1,
            symbol: "ESU6".into(),
            exchange: "CME".into(),
            record_interval: 0,
            start_time: end - 30 * 86_400,
            end_time: end,
            max_days: 2,
            tick_bar_length: 0,
        };
        assert_eq!(historical_range(&request).unwrap(), (end - 2 * 86_400, end));
        request.start_time = end - 3600;
        assert_eq!(historical_range(&request).unwrap(), (end - 3600, end));
        request.max_days = 0;
        request.start_time = end - 30 * 86_400;
        assert_eq!(
            historical_range(&request).unwrap(),
            (request.start_time, end)
        );
        request.start_time = 0;
        request.max_days = u32::MAX;
        assert_eq!(historical_range(&request).unwrap(), (1, end));
    }

    #[test]
    fn retries_history_only_for_transport_failures() {
        assert!(LoadHistoryError::rithmic(RithmicError::ConnectionClosed).connection_issue);
        assert!(LoadHistoryError::rithmic(RithmicError::HeartbeatTimeout).connection_issue);
        assert!(LoadHistoryError::rithmic(RithmicError::SendFailed).connection_issue);
        assert!(
            !LoadHistoryError::rithmic(RithmicError::InvalidArgument("bad interval".to_owned()))
                .connection_issue
        );
        assert!(!LoadHistoryError::local("invalid range").connection_issue);
    }

    #[test]
    fn history_interval_mapping_is_explicit_and_rejects_ambiguous_values() {
        assert_eq!(time_bar_request(30).unwrap(), (TimeBarType::SecondBar, 30));
        assert_eq!(time_bar_request(60).unwrap(), (TimeBarType::MinuteBar, 1));
        assert_eq!(
            time_bar_request(86_400).unwrap(),
            (TimeBarType::DailyBar, 1)
        );
        assert_eq!(
            time_bar_request(604_800).unwrap(),
            (TimeBarType::WeeklyBar, 1)
        );
        assert!(time_bar_request(0).is_err());
        assert!(time_bar_request(61).is_err());
    }
}

/// Live diagnostic against the History Plant. Ignored by default; run with
/// `cargo test --lib history_probe -- --ignored --nocapture` and the
/// `RITHMIC_<ENV>_USER` / `RITHMIC_<ENV>_PW` variables of the account to check.
#[cfg(test)]
mod history_probe {
    use super::*;
    use crate::market_gateway::HistoricalRequest;

    #[tokio::test]
    #[ignore]
    async fn probe_tick_history_entitlement() {
        let _ = dotenvy::from_path(".env");
        let connection = SharedConnection::from_env();
        println!("probe user: {}", connection.settings().user);
        let feed = RithmicHistoryFeed::pending_with(connection)
            .await
            .expect("history config");
        let client = feed.client();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let symbol = std::env::var("RITHMIC_PROBE_SYMBOL").unwrap_or_else(|_| "NQZ6".into());
        let exchange = std::env::var("RITHMIC_PROBE_EXCHANGE").unwrap_or_else(|_| "CME".into());
        let cases = [
            ("1s bars, last 1h", 1, 0u32, now - 3_600),
            ("1s bars, last 24h", 1, 0, now - 86_400),
        ];
        for (label, interval, tick_len, start) in cases {
            let request = HistoricalRequest {
                request_id: 9,
                symbol: symbol.clone(),
                exchange: exchange.clone(),
                record_interval: interval,
                start_time: start,
                end_time: now,
                max_days: 0,
                tick_bar_length: tick_len,
            };
            let began = std::time::Instant::now();
            match client.load(request).await {
                Ok(records) => {
                    let first = records
                        .first()
                        .map(HistoricalRecord::datetime_us)
                        .unwrap_or(0)
                        / 1_000_000;
                    let last = records
                        .last()
                        .map(HistoricalRecord::datetime_us)
                        .unwrap_or(0)
                        / 1_000_000;
                    println!(
                        "[{label}] OK {} records in {:.1}s, span {} -> {} (now {now})",
                        records.len(),
                        began.elapsed().as_secs_f64(),
                        first,
                        last
                    );
                }
                Err(error) => println!(
                    "[{label}] ERROR after {:.1}s: {error}",
                    began.elapsed().as_secs_f64()
                ),
            }
        }
    }
}

#[cfg(test)]
mod history_probe_variants {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn probe_tick_request_variants() {
        let _ = dotenvy::from_path(".env");
        let connection = SharedConnection::from_env();
        let config = history_config(&connection).expect("config");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i32;
        let symbol = std::env::var("RITHMIC_PROBE_SYMBOL").unwrap_or_else(|_| "NQZ6".into());
        let exchange = std::env::var("RITHMIC_PROBE_EXCHANGE").unwrap_or_else(|_| "CME".into());
        let cases: Vec<(&str, u32, bool, i32, i32)> = vec![
            ("uncapped 2-tick, 15m window", 2, true, now - 900, now),
            ("uncapped 2-tick, 30m window", 2, true, now - 1_800, now),
            ("uncapped raw ticks, 5m window", 1, true, now - 300, now),
            ("uncapped raw ticks, 10m window", 1, true, now - 600, now),
            ("uncapped 2-tick, 60m window", 2, true, now - 3_600, now),
        ];
        for (label, len, all, start, end) in cases {
            let session =
                match establish_history_session_within(&config, Duration::from_secs(30)).await {
                    Ok(session) => session,
                    Err(error) => {
                        println!("[{label}] login failed: {error}");
                        continue;
                    }
                };
            let began = std::time::Instant::now();
            let result = tokio::time::timeout(Duration::from_secs(60), async {
                if all {
                    session
                        .handle
                        .load_tick_bars_all(symbol.clone(), exchange.clone(), len, start, end)
                        .await
                } else {
                    session
                        .handle
                        .load_tick_bars(symbol.clone(), exchange.clone(), len, start, end)
                        .await
                }
            })
            .await;
            match result {
                Ok(Ok(responses)) => {
                    let errors = responses
                        .iter()
                        .filter_map(|r| r.error.as_ref().map(|e| e.to_string()))
                        .collect::<Vec<_>>();
                    println!(
                        "[{label}] OK {} responses in {:.1}s, errors: {:?}",
                        responses.len(),
                        began.elapsed().as_secs_f64(),
                        errors
                    );
                    if let Some(first) = responses.first() {
                        println!("    first: {:?}", first.message);
                    }
                }
                Ok(Err(error)) => println!(
                    "[{label}] ERROR after {:.1}s: {error}",
                    began.elapsed().as_secs_f64()
                ),
                Err(_) => println!("[{label}] TIMEOUT after 60s"),
            }
            session.handle.abort();
            let _ = session.plant.await_shutdown().await;
        }
    }
}
