use std::{
    collections::{BTreeMap, HashMap},
    env, fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
};

use axum::{
    Json, Router,
    extract::{
        Query, Request, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use rithmic_dtc_bridge::{
    connection::{ConnectionSettings, KNOWN_SYSTEMS, KNOWN_URLS, SharedConnection},
    market_data::MarketSnapshot,
    market_gateway::{
        AccountBalance, CancelOrderRequest, HistoricalRecord, HistoryDataClient, MarketControl,
        MarketDataClient, MarketEvent, NewOrderRequest, TradeAccount, TradingControl,
        TradingDataClient, TradingEvent, TradingOrder, TradingPosition,
    },
    order_book::{DepthLevel, LevelUpdateType, Side},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{RwLock, broadcast, mpsc},
};

const INDEX_HTML: &str = include_str!("terminal/index.html");
const APP_JS: &str = include_str!("terminal/app.js");
const APP_CSS: &str = include_str!("terminal/app.css");
const LIGHTWEIGHT_CHARTS_JS: &str =
    include_str!("terminal/vendor/lightweight-charts.standalone.production.mjs");
const KLINECHART_JS: &str = include_str!("terminal/vendor/klinecharts.min.js");
pub async fn serve(
    market: MarketDataClient,
    history: HistoryDataClient,
    trading: Option<TradingDataClient>,
    connection: SharedConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    let (market, mut market_events) = market.split();
    let trading_enabled = env::var("RITHMIC_ENABLE_TRADING").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    });
    let trading_connected = trading.is_some();
    let (trading, trading_events) = trading.map(TradingDataClient::split).unzip();
    let (events, _) = broadcast::channel(4096);
    let feed_available = Arc::new(AtomicBool::new(false));
    let membership = MembershipAuth::from_env();
    if membership.accounts.is_empty() {
        eprintln!(
            "[Membership] No member accounts configured; protected terminal APIs will reject access"
        );
    } else {
        println!(
            "[Membership] {} active/configured terminal account(s) loaded",
            membership.accounts.len()
        );
    }
    let state = Arc::new(TerminalState {
        market,
        history,
        trading: RwLock::new(trading),
        trading_enabled,
        trading_connected: AtomicBool::new(trading_connected),
        events: events.clone(),
        feed_available: Arc::clone(&feed_available),
        ids: RwLock::new(HashMap::new()),
        next_id: AtomicU32::new(100),
        order_connection: connection,
        last_feed_error: std::sync::Mutex::new(None),
        membership,
    });
    let event_tx = events.clone();
    let recorder = Arc::clone(&state);
    tokio::spawn(async move {
        while let Some(event) = market_events.recv().await {
            match &event {
                MarketEvent::FeedStatus { available } => {
                    feed_available.store(*available, Ordering::Release);
                    if *available {
                        if let Ok(mut last) = recorder.last_feed_error.lock() {
                            *last = None;
                        }
                    }
                }
                MarketEvent::FeedError(error)
                    if !recorder.feed_available.load(Ordering::Acquire) =>
                {
                    // Only login/connection failures matter here; warnings from
                    // a live session (book rebuilds, lag) are not account errors.
                    if let Ok(mut last) = recorder.last_feed_error.lock() {
                        *last = Some(error.clone());
                    }
                }
                _ => {}
            }
            let _ = event_tx.send(market_event_json(event));
        }
    });
    if let Some(receiver) = trading_events {
        forward_trading_events(events.clone(), receiver);
    }
    let protected = Router::new()
        .route("/ws", get(websocket))
        .route("/api/history", get(history_api))
        .route("/api/account", get(account_api))
        .route(
            "/api/connection",
            get(connection_api).post(connection_update_api),
        )
        .route("/api/connection/test", post(connection_test_api))
        .route("/api/connection/reset", post(connection_reset_api))
        .route("/api/menthorq/levels", post(menthorq_levels_api))
        .route("/api/order", post(order_api))
        .route("/api/cancel", post(cancel_api))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_member,
        ));
    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/app.css", get(app_css))
        .route("/vendor/lightweight-charts.mjs", get(lightweight_charts_js))
        .route("/vendor/klinecharts.min.js", get(klinecharts_js))
        .route("/api/auth/session", get(member_session_api))
        .route("/api/auth/login", post(member_login_api))
        .route("/api/auth/logout", post(member_logout_api))
        .merge(protected)
        .with_state(state);
    let address =
        env::var("TERMINAL_HTTP_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:11200".to_owned());
    let listener = TcpListener::bind(&address).await?;
    println!("Trading terminal listening on http://{address}");
    axum::serve(listener, app).await?;
    Ok(())
}

struct TerminalState {
    market: MarketControl,
    history: HistoryDataClient,
    trading: RwLock<Option<TradingControl>>,
    trading_enabled: bool,
    trading_connected: AtomicBool,
    events: broadcast::Sender<String>,
    feed_available: Arc<AtomicBool>,
    ids: RwLock<HashMap<String, u32>>,
    next_id: AtomicU32,
    /// User-owned order/PnL connection. Market and history use a separate,
    /// server-owned connection that is never exposed through this state.
    order_connection: SharedConnection,
    /// Most recent feed error, cleared when the feed reports available.
    last_feed_error: std::sync::Mutex<Option<String>>,
    membership: MembershipAuth,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MemberAccount {
    username: String,
    password: String,
    #[serde(default = "default_true")]
    active: bool,
    /// Optional Unix timestamp after which the membership is invalid.
    #[serde(default)]
    expires_at: Option<i64>,
}

#[derive(Clone)]
struct MemberSession {
    username: String,
    expires_at: i64,
}

struct MembershipAuth {
    accounts: Vec<MemberAccount>,
    sessions: std::sync::RwLock<HashMap<String, MemberSession>>,
    session_seconds: i64,
    secure_cookie: bool,
}

impl MembershipAuth {
    fn from_env() -> Self {
        let accounts = env::var("TERMINAL_MEMBERS_JSON")
            .ok()
            .and_then(|value| serde_json::from_str::<Vec<MemberAccount>>(&value).ok())
            .unwrap_or_else(|| {
                let username = env::var("TERMINAL_MEMBER_USER").unwrap_or_default();
                let password = env::var("TERMINAL_MEMBER_PASSWORD").unwrap_or_default();
                if username.trim().is_empty() || password.is_empty() {
                    Vec::new()
                } else {
                    vec![MemberAccount {
                        username: username.trim().to_owned(),
                        password,
                        active: true,
                        expires_at: env::var("TERMINAL_MEMBER_EXPIRES_AT")
                            .ok()
                            .and_then(|value| value.parse().ok()),
                    }]
                }
            });
        let session_seconds = env::var("TERMINAL_MEMBER_SESSION_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value: &i64| *value >= 300 && *value <= 2_592_000)
            .unwrap_or(43_200);
        let secure_cookie = env::var("TERMINAL_COOKIE_SECURE").is_ok_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        });
        Self {
            accounts,
            sessions: std::sync::RwLock::new(HashMap::new()),
            session_seconds,
            secure_cookie,
        }
    }

    fn authenticate(&self, username: &str, password: &str) -> bool {
        let now = chrono_like_now();
        self.accounts.iter().any(|account| {
            account.active
                && account.expires_at.is_none_or(|expires| expires > now)
                && constant_time_eq(account.username.as_bytes(), username.trim().as_bytes())
                && constant_time_eq(account.password.as_bytes(), password.as_bytes())
        })
    }

    fn create_session(&self, username: &str) -> String {
        let bytes = rand::random::<[u8; 32]>();
        let token = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let session = MemberSession {
            username: username.trim().to_owned(),
            expires_at: chrono_like_now() + self.session_seconds,
        };
        self.sessions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(token.clone(), session);
        token
    }

    fn session(&self, headers: &HeaderMap) -> Option<MemberSession> {
        let token = cookie_value(headers, "odt_member")?;
        let now = chrono_like_now();
        let mut sessions = self
            .sessions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.retain(|_, session| session.expires_at > now);
        sessions.get(token).cloned()
    }

    fn remove_session(&self, headers: &HeaderMap) {
        if let Some(token) = cookie_value(headers, "odt_member") {
            self.sessions
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(token);
        }
    }

    fn cookie(&self, token: &str) -> String {
        format!(
            "odt_member={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{}",
            self.session_seconds,
            if self.secure_cookie { "; Secure" } else { "" }
        )
    }
}

fn constant_time_eq(expected: &[u8], supplied: &[u8]) -> bool {
    let mut different = expected.len() ^ supplied.len();
    let length = expected.len().max(supplied.len());
    for index in 0..length {
        different |= usize::from(
            expected.get(index).copied().unwrap_or_default()
                ^ supplied.get(index).copied().unwrap_or_default(),
        );
    }
    different == 0
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(key, value)| (key == name).then_some(value))
}

async fn require_member(
    State(state): State<Arc<TerminalState>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    if state.membership.session(&headers).is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "active membership required"})),
        )
            .into_response();
    }
    next.run(request).await
}

#[derive(Deserialize)]
struct MemberLogin {
    username: String,
    password: String,
}

async fn member_session_api(
    State(state): State<Arc<TerminalState>>,
    headers: HeaderMap,
) -> Response {
    match state.membership.session(&headers) {
        Some(session) => Json(json!({
            "authenticated": true,
            "username": session.username,
            "expiresAt": session.expires_at
        }))
        .into_response(),
        None => Json(json!({
            "authenticated": false,
            "configured": !state.membership.accounts.is_empty()
        }))
        .into_response(),
    }
}

async fn member_login_api(
    State(state): State<Arc<TerminalState>>,
    Json(login): Json<MemberLogin>,
) -> Response {
    if state.membership.accounts.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "membership accounts are not configured on the server"})),
        )
            .into_response();
    }
    if !state
        .membership
        .authenticate(&login.username, &login.password)
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid or inactive membership"})),
        )
            .into_response();
    }
    let token = state.membership.create_session(&login.username);
    (
        [(header::SET_COOKIE, state.membership.cookie(&token))],
        Json(json!({"authenticated": true, "username": login.username.trim()})),
    )
        .into_response()
}

async fn member_logout_api(
    State(state): State<Arc<TerminalState>>,
    headers: HeaderMap,
) -> Response {
    state.membership.remove_session(&headers);
    (
        [(
            header::SET_COOKIE,
            "odt_member=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0",
        )],
        Json(json!({"ok": true})),
    )
        .into_response()
}

fn forward_trading_events(
    events: broadcast::Sender<String>,
    mut receiver: mpsc::Receiver<TradingEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let _ = events.send(trading_event_json(event));
        }
    });
}

async fn prepare_trading_replacement(
    state: &TerminalState,
    settings: &ConnectionSettings,
) -> Result<Option<(TradingControl, mpsc::Receiver<TradingEvent>)>, String> {
    if !state.trading_enabled {
        return Ok(None);
    }
    crate::dtc_client::configure_gateway_trading(settings).await?;
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    Ok(Some(
        crate::dtc_client::trading_client(crate::dtc_client::address_from_env()).split(),
    ))
}

async fn switch_connection(
    state: &TerminalState,
    mut settings: ConnectionSettings,
    persist: bool,
) -> Result<ConnectionSettings, String> {
    if settings.password.trim().is_empty() {
        settings.password = state.order_connection.settings().password;
    }
    let settings = settings.normalized()?;
    let replacement = prepare_trading_replacement(state, &settings).await?;
    if let Some((control, receiver)) = replacement {
        let mut active = state.trading.write().await;
        let applied = state.order_connection.apply(settings, persist)?;
        *active = Some(control);
        state.trading_connected.store(true, Ordering::Release);
        drop(active);
        forward_trading_events(state.events.clone(), receiver);
        Ok(applied)
    } else {
        state.order_connection.apply(settings, persist)
    }
}

async fn reset_connection(state: &TerminalState) -> Result<ConnectionSettings, String> {
    crate::dtc_client::disable_gateway_trading().await?;
    *state.trading.write().await = None;
    state.trading_connected.store(false, Ordering::Release);
    Ok(state.order_connection.clear_trading_settings())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}
async fn app_js() -> Response {
    (
        [
            (
                (axum::http::header::CONTENT_TYPE),
                "text/javascript; charset=utf-8",
            ),
            (
                axum::http::header::CACHE_CONTROL,
                "no-store, no-cache, must-revalidate",
            ),
            (axum::http::header::PRAGMA, "no-cache"),
            (axum::http::header::EXPIRES, "0"),
        ],
        APP_JS,
    )
        .into_response()
}
async fn app_css() -> Response {
    (
        [
            (
                (axum::http::header::CONTENT_TYPE),
                "text/css; charset=utf-8",
            ),
            (
                axum::http::header::CACHE_CONTROL,
                "no-store, no-cache, must-revalidate",
            ),
            (axum::http::header::PRAGMA, "no-cache"),
            (axum::http::header::EXPIRES, "0"),
        ],
        APP_CSS,
    )
        .into_response()
}

async fn lightweight_charts_js() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/javascript; charset=utf-8",
        )],
        LIGHTWEIGHT_CHARTS_JS,
    )
        .into_response()
}
async fn klinecharts_js() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/javascript; charset=utf-8",
        )],
        KLINECHART_JS,
    )
        .into_response()
}
#[derive(Deserialize)]
struct HistoryQuery {
    symbol: String,
    #[serde(default = "default_exchange")]
    exchange: String,
    #[serde(default = "default_interval")]
    interval: i32,
    #[serde(default = "default_days")]
    days: u32,
    /// "time" (default), "tick" or "range".
    #[serde(default = "default_bar_type", rename = "barType")]
    bar_type: String,
    /// Trades per bar for "tick"; bar height in ticks for "range".
    #[serde(default, rename = "barSize")]
    bar_size: u32,
    /// Non-zero bypasses the fresh-cache short circuit and re-downloads the range.
    #[serde(default)]
    refresh: u8,
    /// Non-zero requests raw Rithmic trades aggregated into price-by-price
    /// Bid x Ask levels for the browser footprint renderer.
    #[serde(default)]
    footprint: u8,
}
const MAX_HISTORY_DAYS: u32 = 3_650;
fn default_bar_type() -> String {
    "time".to_owned()
}
fn default_exchange() -> String {
    "CME".to_owned()
}
fn default_interval() -> i32 {
    60
}
fn default_days() -> u32 {
    2
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MenthorqLevelsRequest {
    api_key: String,
    api_url: String,
    platform: String,
    ticker: String,
    user_id: String,
    level_types: Vec<String>,
}

async fn menthorq_levels_api(Json(request): Json<MenthorqLevelsRequest>) -> Response {
    let api_key = request.api_key.trim();
    let ticker = request.ticker.trim();
    if api_key.is_empty() || ticker.is_empty() || request.level_types.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "API key, ticker and level types are required"})),
        )
            .into_response();
    }
    if api_key.len() > 512
        || ticker.len() > 32
        || request.platform.len() > 64
        || request.user_id.len() > 128
        || request.level_types.len() > 32
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "MenthorQ settings exceed the allowed size"})),
        )
            .into_response();
    }

    let Ok(api_url) = reqwest::Url::parse(request.api_url.trim()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Invalid MenthorQ API URL"})),
        )
            .into_response();
    };
    if api_url.scheme() != "https" || api_url.host_str().is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "MenthorQ API URL must use HTTPS"})),
        )
            .into_response();
    }

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(25))
        .build()
    {
        Ok(client) => client,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Unable to initialize the MenthorQ client"})),
            )
                .into_response();
        }
    };
    let level_types = request.level_types.join(",");
    let response = client
        .get(api_url)
        .header("X-API-Key", api_key)
        .query(&[
            ("platform", request.platform.trim()),
            ("ticker", ticker),
            ("level_type", level_types.as_str()),
            ("user_id", request.user_id.trim()),
        ])
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "MenthorQ request failed"})),
            )
                .into_response();
        }
    };
    let upstream_status = response.status();
    if !upstream_status.is_success() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": format!("MenthorQ returned HTTP {}", upstream_status.as_u16())
            })),
        )
            .into_response();
    }
    match response.json::<Value>().await {
        Ok(payload) => Json(payload).into_response(),
        Err(_) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "MenthorQ returned invalid JSON"})),
        )
            .into_response(),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum BarKind {
    Time,
    Tick,
    Range,
}

fn bar_kind(query: &HistoryQuery) -> Result<BarKind, String> {
    match query.bar_type.trim().to_ascii_lowercase().as_str() {
        "" | "time" => {
            if query.interval <= 0 {
                return Err("interval must be a positive number of seconds".to_owned());
            }
            Ok(BarKind::Time)
        }
        "tick" => {
            if query.bar_size < 2 || query.bar_size > 1_000_000 {
                return Err("tick bar size must be between 2 and 1000000 trades".to_owned());
            }
            Ok(BarKind::Tick)
        }
        "range" => {
            if query.bar_size < 1 || query.bar_size > 100_000 {
                return Err("range bar size must be between 1 and 100000 ticks".to_owned());
            }
            Ok(BarKind::Range)
        }
        other => Err(format!("unsupported bar type {other}")),
    }
}

/// Broadcast a history download progress step to every terminal page.
/// `percent` is -1 while the size of the step is unknown (indeterminate bar).
fn report_history_progress(
    state: &TerminalState,
    symbol: &str,
    exchange: &str,
    label: &str,
    stage: &str,
    percent: i32,
    detail: impl Into<String>,
    done: bool,
) {
    let detail = detail.into();
    eprintln!("[History][{stage}] {symbol}.{exchange} [{label}] {percent}% {detail}");
    let _ = state.events.send(
        json!({
            "type": "historyProgress",
            "symbol": symbol,
            "exchange": exchange,
            "period": label,
            "stage": stage,
            "percent": percent,
            "detail": detail,
            "done": done,
            "at": chrono_like_now(),
        })
        .to_string(),
    );
}

fn history_period_label(query: &HistoryQuery) -> String {
    match bar_kind(query) {
        Ok(BarKind::Tick) => format!("{} tick", query.bar_size),
        Ok(BarKind::Range) => format!("{} range", query.bar_size),
        _ => format!("{}s", query.interval),
    }
}

/// Calendar bar sizes the terminal offers above one day. They are built from
/// daily bars and aligned to the calendar (Monday, 1st of the month, 1 Jan)
/// instead of fixed multiples of seconds, so they match how charts label them.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CalendarUnit {
    Week,
    Month,
    Year,
}

const WEEK_SECONDS: i32 = 604_800;
const MONTH_SECONDS: i32 = 2_592_000;
const YEAR_SECONDS: i32 = 31_536_000;

fn calendar_unit(interval: i32) -> Option<CalendarUnit> {
    match interval {
        WEEK_SECONDS => Some(CalendarUnit::Week),
        MONTH_SECONDS => Some(CalendarUnit::Month),
        YEAR_SECONDS => Some(CalendarUnit::Year),
        _ => None,
    }
}

/// Civil date (year, month, day) from days since the Unix epoch.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Days since the Unix epoch for a civil date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Start (Unix seconds, UTC midnight) of the calendar bucket holding `seconds`.
/// Futures sessions open the evening before their trading date (17:00 Chicago
/// is 22:00–23:00 UTC), so the timestamp is shifted forward eight hours before
/// the date is taken; that books each session to its trading date.
fn calendar_bucket_start(seconds: i64, unit: CalendarUnit) -> i64 {
    let days = (seconds + 8 * 3_600).div_euclid(86_400);
    let start_day = match unit {
        // 1970-01-01 was a Thursday, so (days + 3) % 7 is the offset from Monday.
        CalendarUnit::Week => days - (days + 3).rem_euclid(7),
        CalendarUnit::Month => {
            let (y, m, _) = civil_from_days(days);
            days_from_civil(y, m, 1)
        }
        CalendarUnit::Year => {
            let (y, _, _) = civil_from_days(days);
            days_from_civil(y, 1, 1)
        }
    };
    start_day * 86_400
}

/// Roll daily bars up into calendar weeks, months or years.
fn aggregate_calendar(bars: Vec<Value>, unit: CalendarUnit) -> Vec<Value> {
    struct Agg {
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
    }
    let mut groups = BTreeMap::<i64, Agg>::new();
    let mut sorted = bars;
    sorted.sort_by_key(|bar| bar_time_us(bar).unwrap_or(0));
    for bar in sorted {
        let Some(time_us) = bar_time_us(&bar) else {
            continue;
        };
        let field = |name: &str| bar.get(name).and_then(Value::as_f64).unwrap_or(f64::NAN);
        let (open, high, low, close) = (field("open"), field("high"), field("low"), field("close"));
        if !(open.is_finite() && high.is_finite() && low.is_finite() && close.is_finite()) {
            continue;
        }
        let volume = bar.get("volume").and_then(Value::as_f64).unwrap_or(0.0);
        let key = calendar_bucket_start(time_us / 1_000_000, unit);
        match groups.get_mut(&key) {
            Some(group) => {
                group.high = group.high.max(high);
                group.low = group.low.min(low);
                group.close = close;
                group.volume += volume;
            }
            None => {
                groups.insert(
                    key,
                    Agg {
                        open,
                        high,
                        low,
                        close,
                        volume,
                    },
                );
            }
        }
    }
    groups
        .into_iter()
        .map(|(time, group)| {
            json!({
                "time": time as f64,
                "open": group.open, "high": group.high, "low": group.low, "close": group.close,
                "volume": group.volume
            })
        })
        .collect()
}

/// Bars inside the requested window, rolled up to the calendar unit if the
/// caller asked for weeks, months or years.
fn finish_history(bars: Vec<Value>, start: i64, calendar: Option<CalendarUnit>) -> Vec<Value> {
    let windowed = history_window(bars, start);
    match calendar {
        Some(unit) => aggregate_calendar(windowed, unit),
        None => windowed,
    }
}

fn format_clock(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let rem = seconds.rem_euclid(86_400);
    // Civil date from days since epoch (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}",
        rem / 3_600,
        (rem % 3_600) / 60
    )
}

async fn history_api(
    State(state): State<Arc<TerminalState>>,
    Query(query): Query<HistoryQuery>,
) -> Response {
    let kind = match bar_kind(&query) {
        Ok(kind) => kind,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": error}))).into_response();
        }
    };
    if query.days == 0 || query.days > MAX_HISTORY_DAYS {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("days must be between 1 and {MAX_HISTORY_DAYS}")})),
        )
            .into_response();
    }
    let now = chrono_like_now();
    let range_start = now - i64::from(query.days) * 86_400;
    // Weeks, months and years are served from the daily cache and rolled up
    // on the way out, so every calendar size shares one download.
    let calendar = if kind == BarKind::Time {
        calendar_unit(query.interval)
    } else {
        None
    };
    let query = match calendar {
        Some(_) => HistoryQuery {
            interval: 86_400,
            ..query
        },
        None => query,
    };
    let label = history_period_label(&query);
    let progress = |stage: &str, percent: i32, detail: String, done: bool| {
        report_history_progress(
            &state,
            &query.symbol,
            &query.exchange,
            &label,
            stage,
            percent,
            detail,
            done,
        )
    };
    progress(
        "cache",
        2,
        format!("读取本地缓存 {}", history_cache_path(&query).display()),
        false,
    );
    let cached = read_history_cache(&query);
    progress(
        "cache",
        5,
        match cached.as_deref() {
            Some(bars) => format!("本地缓存 {} 根K线", bars.len()),
            None => "本地缓存为空".to_owned(),
        },
        false,
    );
    let force_rithmic = env::var("RITHMIC_FORCE_HISTORY_REFRESH").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    });
    let force_refresh = force_rithmic || query.refresh != 0;
    let step = if kind == BarKind::Time {
        i64::from(query.interval.max(1))
    } else {
        60
    };
    // Fresh enough to skip the network: three bars, but never more than six
    // hours so daily and larger bars still pick up the current session.
    let stale_window = (step * 3).clamp(300, 6 * 3_600);
    let cached_times = cached
        .as_deref()
        .map(|bars| {
            bars.iter()
                .filter_map(bar_time_us)
                .map(|us| us / 1_000_000)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let cached_first = cached_times.iter().min().copied();
    let cached_last = cached_times.iter().max().copied();
    let cached_stale = cached_last.map_or(true, |last| last < now - stale_window);
    // Cache covers the start of the requested window (allowing one bar of
    // slack) when the oldest stored bar is at or before it.
    let cached_covers_start = cached_first.is_some_and(|first| first <= range_start + step);
    if !force_refresh && !cached_stale && cached_covers_start {
        if let Some(cached_bars) = cached.clone() {
            progress("done", 100, "缓存已是最新，无需下载".to_owned(), true);
            return Json(json!({"bars": finish_history(cached_bars, range_start, calendar), "source": "local-cache"}))
                .into_response();
        }
    }
    // Only the ranges the cache lacks are fetched: the newer tail since the
    // last stored bar and, when more days are requested than stored, the
    // older head. A forced refresh re-downloads the whole window. Bars are
    // merged into one shared file per symbol/period, so the next open of any
    // day count reuses everything downloaded so far.
    let ranges = if force_refresh {
        vec![(range_start, now)]
    } else {
        missing_history_ranges(cached.as_deref(), range_start, now, step)
    };
    if ranges.is_empty() {
        if let Some(cached_bars) = cached.clone() {
            progress("done", 100, "缓存已覆盖请求范围".to_owned(), true);
            return Json(json!({"bars": finish_history(cached_bars, range_start, calendar), "source": "local-cache"}))
                .into_response();
        }
    }
    for (index, (start, end)) in ranges.iter().enumerate() {
        progress(
            "plan",
            8,
            format!(
                "缺失区间 {}/{}: {} → {}",
                index + 1,
                ranges.len(),
                format_clock(*start),
                format_clock(*end)
            ),
            false,
        );
    }
    let total_steps = ranges.len() as i32 + 1;
    let step_percent = |step: i32| 10 + (step * 85) / total_steps.max(1);
    let mut merged = cached.clone();
    let mut fetched_any = false;
    for (index, (start, end)) in ranges.iter().enumerate() {
        progress(
            "rithmic",
            step_percent(index as i32),
            format!(
                "Rithmic 历史 {}/{}: {} → {}",
                index + 1,
                ranges.len(),
                format_clock(*start),
                format_clock(*end)
            ),
            false,
        );
        // Footprints need raw trades even for a time period; ordinary charts
        // continue to request native Rithmic time bars.
        let request_kind = if query.footprint != 0 {
            BarKind::Range
        } else {
            kind
        };
        match state
            .history
            .load(rithmic_history_request(&query, request_kind, *start, *end))
            .await
        {
            Ok(records) => {
                progress(
                    "rithmic",
                    step_percent(index as i32 + 1),
                    format!("Rithmic 返回 {} 条记录", records.len()),
                    false,
                );
                let tick_size = tick_size_for(&query.symbol.to_ascii_uppercase());
                let bars = if query.footprint != 0 {
                    match kind {
                        BarKind::Time => {
                            footprint_bars_from_ticks(records, query.interval, tick_size)
                        }
                        BarKind::Tick => {
                            orderflow_tick_bars_from_ticks(records, query.bar_size, tick_size)
                        }
                        BarKind::Range => range_bars_from_ticks(
                            records,
                            f64::from(query.bar_size) * tick_size,
                            tick_size,
                        ),
                    }
                } else {
                    match kind {
                        BarKind::Range => range_bars_from_ticks(
                            records,
                            f64::from(query.bar_size) * tick_size,
                            tick_size,
                        ),
                        _ => records.into_iter().filter_map(bar_json).collect::<Vec<_>>(),
                    }
                };
                if !bars.is_empty() {
                    fetched_any = true;
                    merged = Some(merge_history_bars(merged.as_deref(), bars));
                }
            }
            Err(rithmic_error) => {
                progress(
                    "error",
                    -1,
                    format!("Rithmic 下载失败: {rithmic_error}"),
                    true,
                );
                return stale_history_response(
                    cached.map(|bars| finish_history(bars, range_start, calendar)),
                    rithmic_error,
                );
            }
        }
    }
    match merged {
        Some(bars) if fetched_any => {
            progress(
                "save",
                97,
                format!("保存 {} 根K线到本地缓存", bars.len()),
                false,
            );
            write_history_cache(&query, &bars);
            progress("done", 100, format!("完成，共 {} 根K线", bars.len()), true);
            Json(json!({"bars": finish_history(bars, range_start, calendar), "source": "rithmic"}))
                .into_response()
        }
        _ => {
            progress("error", -1, "Rithmic 未返回任何K线".to_owned(), true);
            stale_history_response(
                cached.map(|bars| finish_history(bars, range_start, calendar)),
                "Rithmic returned no historical bars".to_owned(),
            )
        }
    }
}

fn rithmic_history_request(
    query: &HistoryQuery,
    kind: BarKind,
    start_time: i64,
    end_time: i64,
) -> rithmic_dtc_bridge::market_gateway::HistoricalRequest {
    rithmic_dtc_bridge::market_gateway::HistoricalRequest {
        request_id: 1,
        symbol: query.symbol.clone(),
        exchange: query.exchange.clone(),
        record_interval: if kind == BarKind::Time {
            query.interval
        } else {
            0
        },
        start_time,
        end_time,
        max_days: query.days,
        tick_bar_length: if kind == BarKind::Tick {
            query.bar_size
        } else {
            0
        },
    }
}

/// Bar start time in microseconds. Time bars carry whole seconds; tick and
/// range bars carry fractional seconds because several can share one second.
fn bar_time_us(bar: &Value) -> Option<i64> {
    bar.get("time")
        .and_then(Value::as_f64)
        .filter(|time| time.is_finite())
        .map(|time| (time * 1_000_000.0).round() as i64)
}

fn merge_history_bars(cached: Option<&[Value]>, fresh: Vec<Value>) -> Vec<Value> {
    let mut by_time = BTreeMap::<i64, Value>::new();
    if let Some(cached) = cached {
        for bar in cached {
            if let Some(time) = bar_time_us(bar) {
                by_time.insert(time, bar.clone());
            }
        }
    }
    for bar in fresh {
        if let Some(time) = bar_time_us(&bar) {
            by_time.insert(time, bar);
        }
    }
    by_time.into_values().collect()
}

/// Build range bars from a trade stream. A bar spans at most `range` in price;
/// the first trade that would push high-low past the range closes the bar
/// (clamped to the range) and opens a new one at its own price.
type FlowLevel = [f64; 6];

fn update_flow_level(
    levels: &mut BTreeMap<i64, FlowLevel>,
    price: f64,
    volume: f64,
    at_bid_or_ask: u16,
    tick_size: f64,
) {
    let level = levels
        .entry((price / tick_size).round() as i64)
        .or_default();
    let offset = match at_bid_or_ask {
        1 => 0,
        2 => 1,
        _ => 2,
    };
    level[offset] += volume;
    level[offset + 3] = level[offset + 3].max(volume);
}

fn flow_levels_json(levels: BTreeMap<i64, FlowLevel>, tick_size: f64) -> Vec<Value> {
    levels
        .into_iter()
        .map(|(price_tick, level)| {
            json!({
                "price": price_tick as f64 * tick_size,
                "bid": level[0], "ask": level[1], "neutral": level[2],
                "maxBid": level[3], "maxAsk": level[4], "maxNeutral": level[5]
            })
        })
        .collect()
}

fn range_bars_from_ticks(records: Vec<HistoricalRecord>, range: f64, tick_size: f64) -> Vec<Value> {
    struct Bar {
        time_us: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        trades: u32,
        levels: BTreeMap<i64, FlowLevel>,
    }
    if !(range > 0.0) {
        return Vec::new();
    }
    let mut bars = Vec::<Value>::new();
    let mut current: Option<Bar> = None;
    let push = |bar: Bar, bars: &mut Vec<Value>| {
        bars.push(json!({
            "time": bar.time_us as f64 / 1_000_000.0,
            "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
            "volume": bar.volume, "trades": bar.trades,
            "levels": flow_levels_json(bar.levels, tick_size)
        }));
    };
    for record in records {
        let HistoricalRecord::Tick {
            datetime_us,
            price,
            volume,
            at_bid_or_ask,
        } = record
        else {
            continue;
        };
        if !price.is_finite() {
            continue;
        }
        match current.as_mut() {
            Some(bar) if price.max(bar.high) - price.min(bar.low) <= range + 1e-9 => {
                bar.high = bar.high.max(price);
                bar.low = bar.low.min(price);
                bar.close = price;
                bar.volume += volume;
                bar.trades += 1;
                update_flow_level(&mut bar.levels, price, volume, at_bid_or_ask, tick_size);
            }
            Some(bar) => {
                bar.close = price.clamp(bar.high - range, bar.low + range);
                bar.high = bar.high.max(bar.close);
                bar.low = bar.low.min(bar.close);
                let completed = current.take().expect("range bar exists");
                push(completed, &mut bars);
                let mut levels = BTreeMap::new();
                update_flow_level(&mut levels, price, volume, at_bid_or_ask, tick_size);
                current = Some(Bar {
                    time_us: datetime_us,
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume,
                    trades: 1,
                    levels,
                });
            }
            None => {
                let mut levels = BTreeMap::new();
                update_flow_level(&mut levels, price, volume, at_bid_or_ask, tick_size);
                current = Some(Bar {
                    time_us: datetime_us,
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume,
                    trades: 1,
                    levels,
                });
            }
        }
    }
    if let Some(bar) = current {
        push(bar, &mut bars);
    }
    bars
}

fn orderflow_tick_bars_from_ticks(
    records: Vec<HistoricalRecord>,
    size: u32,
    tick_size: f64,
) -> Vec<Value> {
    if size == 0 {
        return Vec::new();
    }
    let mut bars = Vec::new();
    let mut chunk = Vec::with_capacity(size as usize);
    let push = |chunk: &mut Vec<HistoricalRecord>, bars: &mut Vec<Value>| {
        if chunk.is_empty() {
            return;
        }
        let mut time = 0_i64;
        let mut open = 0.0;
        let mut high = f64::NEG_INFINITY;
        let mut low = f64::INFINITY;
        let mut close = 0.0;
        let mut volume = 0.0;
        let mut levels = BTreeMap::new();
        for (index, record) in chunk.drain(..).enumerate() {
            if let HistoricalRecord::Tick {
                datetime_us,
                price,
                volume: trade_volume,
                at_bid_or_ask,
            } = record
            {
                if index == 0 {
                    time = datetime_us;
                    open = price;
                }
                high = high.max(price);
                low = low.min(price);
                close = price;
                volume += trade_volume;
                update_flow_level(&mut levels, price, trade_volume, at_bid_or_ask, tick_size);
            }
        }
        bars.push(json!({"time": time as f64 / 1_000_000.0, "open": open, "high": high, "low": low, "close": close, "volume": volume, "trades": size.min(u32::MAX), "levels": flow_levels_json(levels, tick_size)}));
    };
    for record in records {
        if matches!(record, HistoricalRecord::Tick { .. }) {
            chunk.push(record);
            if chunk.len() >= size as usize {
                push(&mut chunk, &mut bars);
            }
        }
    }
    if !chunk.is_empty() {
        let remaining = chunk.len() as u32;
        push(&mut chunk, &mut bars);
        if let Some(last) = bars.last_mut() {
            last["trades"] = json!(remaining);
        }
    }
    bars
}

/// Aggregate raw Rithmic trades into time bars with price-level Bid x Ask
/// volume. Unclassified volume is retained separately instead of guessing its
/// aggressor side.
fn footprint_bars_from_ticks(
    records: Vec<HistoricalRecord>,
    interval: i32,
    tick_size: f64,
) -> Vec<Value> {
    struct FootprintBar {
        time: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        trades: u32,
        levels: BTreeMap<i64, FlowLevel>,
    }
    if interval <= 0 || !(tick_size > 0.0) {
        return Vec::new();
    }
    let mut grouped = BTreeMap::<i64, FootprintBar>::new();
    for record in records {
        let HistoricalRecord::Tick {
            datetime_us,
            price,
            volume,
            at_bid_or_ask,
        } = record
        else {
            continue;
        };
        if !price.is_finite() || !volume.is_finite() {
            continue;
        }
        let seconds = datetime_us.div_euclid(1_000_000);
        let bucket = seconds.div_euclid(i64::from(interval)) * i64::from(interval);
        let price_tick = (price / tick_size).round() as i64;
        let bar = grouped.entry(bucket).or_insert_with(|| FootprintBar {
            time: bucket,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 0.0,
            trades: 0,
            levels: BTreeMap::new(),
        });
        bar.high = bar.high.max(price);
        bar.low = bar.low.min(price);
        bar.close = price;
        bar.volume += volume;
        bar.trades = bar.trades.saturating_add(1);
        let _ = price_tick;
        update_flow_level(&mut bar.levels, price, volume, at_bid_or_ask, tick_size);
    }
    grouped
        .into_values()
        .map(|bar| {
            let levels = flow_levels_json(bar.levels, tick_size);
            json!({
                "time": bar.time,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "trades": bar.trades,
                "levels": levels
            })
        })
        .collect()
}

fn stale_history_response(cached: Option<Vec<Value>>, error: String) -> Response {
    match cached {
        Some(bars) => Json(json!({
            "bars": bars,
            "source": "local-cache-stale",
            "warning": format!("History refresh failed: {error}")
        }))
        .into_response(),
        None => (StatusCode::BAD_GATEWAY, Json(json!({"error": error}))).into_response(),
    }
}

fn history_cache_dir() -> PathBuf {
    env::var_os("HISTORY_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data").join("history"))
}

fn history_cache_path(query: &HistoryQuery) -> PathBuf {
    let safe_symbol: String = query
        .symbol
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() {
                value
            } else {
                '_'
            }
        })
        .collect();
    let safe_exchange: String = query
        .exchange
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() {
                value
            } else {
                '_'
            }
        })
        .collect();
    let period = match bar_kind(query) {
        Ok(BarKind::Tick) => format!("t{}", query.bar_size),
        Ok(BarKind::Range) => format!("r{}", query.bar_size),
        _ => format!("{}m", query.interval),
    };
    let detail = if query.footprint != 0 {
        "_footprint-v2"
    } else {
        ""
    };
    // Deliberately separate these files from caches written by older builds,
    // because those could contain Yahoo bars mixed with Rithmic history.
    history_cache_dir().join(format!(
        "{}_{}_{}_rithmic-v1{}.json",
        safe_symbol, safe_exchange, period, detail
    ))
}

/// Cache files written before the day count was dropped from the file name.
fn legacy_history_cache_paths(query: &HistoryQuery) -> Vec<PathBuf> {
    let path = history_cache_path(query);
    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return Vec::new();
    };
    let prefix = format!("{stem}_");
    fs::read_dir(history_cache_dir())
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|candidate| {
                    candidate
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with(&prefix) && name.ends_with("d.json"))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn read_history_cache_file(path: &std::path::Path) -> Option<Vec<Value>> {
    let text = fs::read_to_string(path).ok()?;
    let payload = serde_json::from_str::<Value>(&text).ok()?;
    payload
        .get("bars")
        .and_then(Value::as_array)
        .filter(|bars| !bars.is_empty())
        .cloned()
}

/// Everything stored for this symbol/period, regardless of how many days the
/// caller asks for now: the shared file plus any legacy per-day files, merged.
fn read_history_cache(query: &HistoryQuery) -> Option<Vec<Value>> {
    let mut merged: Option<Vec<Value>> = None;
    for path in legacy_history_cache_paths(query)
        .into_iter()
        .chain(std::iter::once(history_cache_path(query)))
    {
        if let Some(bars) = read_history_cache_file(&path) {
            merged = Some(merge_history_bars(merged.as_deref(), bars));
        }
    }
    merged.filter(|bars| !bars.is_empty())
}

/// Bars inside the window the caller asked for.
fn history_window(bars: Vec<Value>, start: i64) -> Vec<Value> {
    let start_us = start * 1_000_000;
    bars.into_iter()
        .filter(|bar| bar_time_us(bar).is_some_and(|time| time >= start_us))
        .collect()
}

/// Ranges still missing from the cache for a request covering `[start, now]`:
/// an older head (cache does not reach back far enough) and a newer tail
/// (cache ends before now). The tail overlaps the last cached bar so a
/// partial bar is replaced rather than duplicated.
fn missing_history_ranges(
    cached: Option<&[Value]>,
    start: i64,
    now: i64,
    step: i64,
) -> Vec<(i64, i64)> {
    let times = cached
        .map(|bars| bars.iter().filter_map(bar_time_us).map(|us| us / 1_000_000))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let (Some(first), Some(last)) = (times.iter().min().copied(), times.iter().max().copied())
    else {
        return vec![(start, now)];
    };
    let mut ranges = Vec::new();
    if first > start + step {
        ranges.push((start, first));
    }
    if last < now {
        ranges.push(((last - step).max(start), now));
    }
    ranges
}

fn write_history_cache(query: &HistoryQuery, bars: &[Value]) {
    if bars.is_empty() {
        return;
    }
    let path = history_cache_path(query);
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(text) = serde_json::to_string(&json!({"bars": bars})) {
        let temp = path.with_extension("json.tmp");
        if fs::write(&temp, text).is_ok() {
            let _ = fs::rename(temp, path);
        }
    }
}

fn chrono_like_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|v| v.as_secs() as i64)
        .unwrap_or_default()
}

fn connection_status_json(state: &TerminalState) -> Value {
    let settings = state.order_connection.settings();
    let mut payload = settings.public_json();
    payload["source"] = json!(state.order_connection.source().as_str());
    payload["feedAvailable"] = json!(state.feed_available.load(Ordering::Acquire));
    payload["tradingConnected"] = json!(state.trading_connected.load(Ordering::Acquire));
    payload["dataProvider"] = json!("server");
    payload["lastError"] = json!(
        state
            .last_feed_error
            .lock()
            .ok()
            .and_then(|value| value.clone())
    );
    payload["knownUrls"] = json!(KNOWN_URLS);
    payload["knownSystems"] = json!(KNOWN_SYSTEMS);
    payload["savedFile"] = json!(
        rithmic_dtc_bridge::connection::saved_settings_path()
            .display()
            .to_string()
    );
    payload["tradingEnabled"] = json!(state.trading_enabled);
    payload
}

async fn connection_api(State(state): State<Arc<TerminalState>>) -> Response {
    Json(connection_status_json(&state)).into_response()
}

#[derive(Deserialize)]
struct ConnectionUpdate {
    #[serde(flatten)]
    settings: ConnectionSettings,
    /// Persist to the order-only connection file so the choice survives restarts.
    #[serde(default = "default_true")]
    persist: bool,
}
fn default_true() -> bool {
    true
}

async fn connection_update_api(
    State(state): State<Arc<TerminalState>>,
    Json(update): Json<ConnectionUpdate>,
) -> Response {
    match switch_connection(&state, update.settings, update.persist).await {
        Ok(applied) => {
            println!(
                "Rithmic order connection switched to user {} on {} ({})",
                applied.user, applied.system_name, applied.url
            );
            let mut payload = connection_status_json(&state);
            payload["ok"] = json!(true);
            Json(payload).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, Json(json!({"error": error}))).into_response(),
    }
}

async fn connection_reset_api(State(state): State<Arc<TerminalState>>) -> Response {
    match reset_connection(&state).await {
        Ok(_) => {
            let mut payload = connection_status_json(&state);
            payload["ok"] = json!(true);
            Json(payload).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, Json(json!({"error": error}))).into_response(),
    }
}

/// Logs in to the Ticker Plant with candidate settings and logs out again,
/// without touching the running feeds. Reports the Rithmic systems the
/// gateway lists so the user can pick a valid system name.
async fn connection_test_api(
    State(state): State<Arc<TerminalState>>,
    Json(mut settings): Json<ConnectionSettings>,
) -> Response {
    if settings.password.trim().is_empty() {
        settings.password = state.order_connection.settings().password;
    }
    match settings.normalized() {
        Ok(settings) => Json(json!({
            "ok": true,
            "elapsedMs": 0,
            "systems": KNOWN_SYSTEMS,
            "message": format!("配置格式有效；保存时将由 Gateway 验证 {} / {}", settings.user, settings.system_name)
        })).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, Json(json!({"ok": false, "error": error}))).into_response(),
    }
}

async fn account_api(State(state): State<Arc<TerminalState>>) -> Response {
    let Some(trading) = state.trading.read().await.clone() else {
        return Json(json!({"enabled": false})).into_response();
    };
    let accounts = trading.accounts().await;
    let orders = trading.open_orders().await;
    let positions = trading.positions().await;
    let balance = trading.balance().await;
    Json(json!({"enabled": true, "accounts": accounts.ok().map(|v| v.into_iter().map(account_json).collect::<Vec<_>>()), "orders": orders.ok().map(|v| v.into_iter().map(order_json).collect::<Vec<_>>()), "positions": positions.ok().map(|v| v.into_iter().map(position_json).collect::<Vec<_>>()), "balance": balance.ok().map(balance_json)})).into_response()
}

async fn order_api(
    State(state): State<Arc<TerminalState>>,
    Json(request): Json<NewOrderRequest>,
) -> Response {
    let Some(trading) = state.trading.read().await.clone() else {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Paper trading is disabled"})),
        )
            .into_response();
    };
    match trading.submit(request).await {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, Json(json!({"error": error}))).into_response(),
    }
}

async fn cancel_api(
    State(state): State<Arc<TerminalState>>,
    Json(request): Json<CancelOrderRequest>,
) -> Response {
    let Some(trading) = state.trading.read().await.clone() else {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Paper trading is disabled"})),
        )
            .into_response();
    };
    match trading.cancel(request).await {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, Json(json!({"error": error}))).into_response(),
    }
}

async fn websocket(
    upgrade: WebSocketUpgrade,
    State(state): State<Arc<TerminalState>>,
) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| websocket_session(socket, state))
}

async fn websocket_session(mut socket: WebSocket, state: Arc<TerminalState>) {
    let mut events = state.events.subscribe();
    // Symbols and depth are requested upstream, which can take seconds while the
    // session is already streaming depth. The work runs in its own task and
    // reports back through this channel, so the loop keeps forwarding market data
    // for every symbol that is already streaming instead of pausing the browser.
    let (outbound, mut outbound_events) = mpsc::channel::<String>(256);
    let _ = socket
        .send(Message::Text(
            json!({
                "type":"ready",
                "tradingEnabled":state.trading_enabled,
                "tradingConnected":state.trading_connected.load(Ordering::Acquire),
                "feedAvailable":state.feed_available.load(Ordering::Acquire)
            })
            .to_string()
            .into(),
        ))
        .await;
    loop {
        tokio::select! {
            incoming = socket.next() => {
                let Some(Ok(Message::Text(text))) = incoming else { break; };
                let Ok(command) = serde_json::from_str::<Value>(&text) else { continue };
                if command.get("type").and_then(Value::as_str) != Some("subscribe") { continue }
                let symbol = command.get("symbol").and_then(Value::as_str).unwrap_or("ESZ6").to_ascii_uppercase();
                let exchange = command.get("exchange").and_then(Value::as_str).unwrap_or("CME").to_ascii_uppercase();
                let key = format!("{symbol}.{exchange}");
                let id = { let mut ids = state.ids.write().await; *ids.entry(key).or_insert_with(|| state.next_id.fetch_add(1, Ordering::Relaxed)) };
                let tick = tick_size_for(&symbol);
                if socket.send(Message::Text(json!({"type":"subscribed","symbol":symbol,"exchange":exchange,"symbolId":id,"tickSize":tick}).to_string().into())).await.is_err() { break }
                let market = state.market.clone();
                let outbound = outbound.clone();
                tokio::spawn(async move {
                    let snapshot = match market.subscribe(id, &symbol, &exchange).await {
                        Ok(snapshot) => snapshot,
                        Err(error) if error.contains("already subscribed") => {
                            // A browser refresh reuses the process-wide subscription:
                            // serve its current state from memory.
                            match market.snapshot(&symbol, &exchange).await {
                                Ok(snapshot) => snapshot,
                                Err(_) => return,
                            }
                        }
                        Err(error) => {
                            let _ = outbound.send(json!({"type":"error","message":error}).to_string()).await;
                            return;
                        }
                    };
                    let _ = outbound.send(json!({"type":"snapshot","symbol":symbol,"exchange":exchange,"symbolId":id,"data":snapshot_json(&snapshot)}).to_string()).await;
                    let levels = match market.subscribe_depth(id, &symbol, &exchange, tick, 20).await {
                        Ok(levels) => levels.into_iter().map(depth_json).collect::<Vec<_>>(),
                        Err(error) if error.contains("already has a depth subscription") => snapshot_bbo_levels(&snapshot),
                        Err(error) => {
                            let _ = outbound.send(json!({"type":"warning","symbol":symbol,"exchange":exchange,"symbolId":id,"message":format!("Depth unavailable: {error}")}).to_string()).await;
                            snapshot_bbo_levels(&snapshot)
                        }
                    };
                    if !levels.is_empty() {
                        let _ = outbound.send(json!({"type":"depth","symbol":symbol,"exchange":exchange,"symbolId":id,"levels":levels}).to_string()).await;
                    }
                });
            }
            message = outbound_events.recv() => {
                let Some(message) = message else { break };
                if socket.send(Message::Text(message.into())).await.is_err() { break }
            }
            event = events.recv() => {
                match event { Ok(message) => { if socket.send(Message::Text(message.into())).await.is_err() { break; } }, Err(broadcast::error::RecvError::Lagged(_)) => continue, Err(_) => break }
            }
        }
    }
}

fn tick_size_for(symbol: &str) -> f64 {
    if symbol.starts_with("GC") { 0.1 } else { 0.25 }
}

fn market_event_json(event: MarketEvent) -> String {
    match event {
        MarketEvent::LastTrade { symbol_id, price, volume, datetime_us, at_bid_or_ask, .. } => json!({"type":"trade","symbolId":symbol_id,"price":price,"volume":volume,"time":datetime_us / 1_000_000,"timeUs":datetime_us,"atBidOrAsk":at_bid_or_ask}).to_string(),
        MarketEvent::BestBidAsk { symbol_id, bid_price, bid_quantity, ask_price, ask_quantity, datetime_us } => json!({"type":"quote","symbolId":symbol_id,"bid":bid_price,"bidSize":bid_quantity,"ask":ask_price,"askSize":ask_quantity,"time":datetime_us / 1_000_000}).to_string(),
        MarketEvent::Snapshot { symbol_id, snapshot } => json!({"type":"snapshotEvent","symbolId":symbol_id,"data":snapshot_json(&snapshot)}).to_string(),
        MarketEvent::SessionVolume { symbol_id, volume } => json!({"type":"volume","symbolId":symbol_id,"volume":volume}).to_string(),
        MarketEvent::DepthUpdate { symbol_id, update, datetime_us, .. } => json!({"type":"depthUpdate","symbolId":symbol_id,"time":datetime_us / 1_000_000,"level":depth_update_json(&update)}).to_string(),
        MarketEvent::DepthSnapshotLevel { symbol_id, level, datetime_us, .. } => json!({"type":"depthLevel","symbolId":symbol_id,"time":datetime_us / 1_000_000,"level":depth_json(level)}).to_string(),
        MarketEvent::FeedStatus { available } => json!({"type":"feed","available":available}).to_string(),
        MarketEvent::FeedError(error) => json!({"type":"error","message":error}).to_string(),
    }
}

fn snapshot_json(snapshot: &MarketSnapshot) -> Value {
    json!({"last":snapshot.last,"lastSize":snapshot.last_size,"bid":snapshot.bid,"bidSize":snapshot.bid_size,"ask":snapshot.ask,"askSize":snapshot.ask_size,"open":snapshot.open,"high":snapshot.high,"low":snapshot.low,"volume":snapshot.volume,"openInterest":snapshot.open_interest,"settlement":snapshot.settlement,"time":snapshot.last_time_us / 1_000_000})
}
fn snapshot_bbo_levels(snapshot: &MarketSnapshot) -> Vec<Value> {
    let mut levels = Vec::with_capacity(2);
    if let (Some(price), Some(quantity)) = (snapshot.bid, snapshot.bid_size) {
        levels.push(json!({"side":"bid","price":price,"quantity":quantity,"orders":0,"level":1}));
    }
    if let (Some(price), Some(quantity)) = (snapshot.ask, snapshot.ask_size) {
        levels.push(json!({"side":"ask","price":price,"quantity":quantity,"orders":0,"level":1}));
    }
    levels
}
fn depth_json(level: DepthLevel) -> Value {
    json!({"side":match level.side { Side::Bid => "bid", Side::Ask => "ask" },"price":level.price,"quantity":level.quantity,"orders":level.num_orders,"level":level.level})
}
fn depth_update_json(update: &rithmic_dtc_bridge::order_book::LevelUpdate) -> Value {
    json!({"side":match update.side { Side::Bid => "bid", Side::Ask => "ask" },"price":update.price,"quantity":update.quantity,"orders":update.num_orders,"level":update.level,"action":match update.update_type { LevelUpdateType::Delete => "delete", LevelUpdateType::Insert => "insert", LevelUpdateType::Update => "update" }})
}
fn bar_json(record: HistoricalRecord) -> Option<Value> {
    match record {
        HistoricalRecord::Bar {
            start_datetime_us,
            open,
            high,
            low,
            close,
            volume,
            num_trades,
            ..
        } => {
            let time = if start_datetime_us % 1_000_000 == 0 {
                json!(start_datetime_us / 1_000_000)
            } else {
                json!(start_datetime_us as f64 / 1_000_000.0)
            };
            Some(
                json!({"time":time,"open":open,"high":high,"low":low,"close":close,"volume":volume,"trades":num_trades}),
            )
        }
        HistoricalRecord::Tick { .. } => None,
    }
}

fn account_json(value: TradeAccount) -> Value {
    json!({"accountId":value.account_id,"currency":value.currency,"tradingDisabled":value.trading_disabled})
}
fn order_json(value: TradingOrder) -> Value {
    json!({"serverOrderId":value.server_order_id,"clientOrderId":value.client_order_id,"symbol":value.symbol,"exchange":value.exchange,"accountId":value.account_id,"status":value.order_status,"side":value.buy_sell,"type":value.order_type,"price1":value.price1,"price2":value.price2,"quantity":value.quantity,"filled":value.filled_quantity,"remaining":value.remaining_quantity,"avgFill":value.average_fill_price,"info":value.info_text})
}
fn position_json(value: TradingPosition) -> Value {
    json!({"symbol":value.symbol,"exchange":value.exchange,"accountId":value.account_id,"quantity":value.quantity,"averagePrice":value.average_price,"openPnl":value.open_profit_loss})
}
fn balance_json(value: AccountBalance) -> Value {
    json!({"accountId":value.account_id,"currency":value.currency,"cash":value.cash_balance,"available":value.available_funds,"openPnl":value.open_profit_loss,"dailyPnl":value.daily_profit_loss,"tradingDisabled":value.trading_disabled})
}
fn trading_event_json(event: TradingEvent) -> String {
    match event {
        TradingEvent::Order(order) => json!({"type":"order","data":order_json(order)}).to_string(),
        TradingEvent::Position(position) => {
            json!({"type":"position","data":position_json(position)}).to_string()
        }
        TradingEvent::Balance(balance) => {
            json!({"type":"balance","data":balance_json(balance)}).to_string()
        }
        TradingEvent::Error(error) => json!({"type":"error","message":error}).to_string(),
    }
}

#[cfg(test)]
mod membership_tests {
    use super::*;

    #[test]
    fn credential_comparison_rejects_length_and_content_changes() {
        assert!(constant_time_eq(
            b"member@example.com",
            b"member@example.com"
        ));
        assert!(!constant_time_eq(
            b"member@example.com",
            b"member@example.co"
        ));
        assert!(!constant_time_eq(b"secret-one", b"secret-two"));
    }

    #[test]
    fn member_cookie_is_found_among_other_cookies() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            "theme=dark; odt_member=abc123; locale=zh-CN"
                .parse()
                .unwrap(),
        );
        assert_eq!(cookie_value(&headers, "odt_member"), Some("abc123"));
        assert_eq!(cookie_value(&headers, "missing"), None);
    }
}

#[cfg(test)]
mod footprint_indicator_tests {
    use super::*;

    #[test]
    fn footprint_keeps_largest_individual_trade_per_side() {
        let records = vec![
            HistoricalRecord::Tick {
                datetime_us: 1_700_000_000_000_000,
                price: 5000.0,
                volume: 30.0,
                at_bid_or_ask: 2,
            },
            HistoricalRecord::Tick {
                datetime_us: 1_700_000_001_000_000,
                price: 5000.0,
                volume: 20.0,
                at_bid_or_ask: 2,
            },
            HistoricalRecord::Tick {
                datetime_us: 1_700_000_002_000_000,
                price: 5000.0,
                volume: 12.0,
                at_bid_or_ask: 1,
            },
        ];
        let bars = footprint_bars_from_ticks(records, 60, 0.25);
        let level = &bars[0]["levels"][0];
        assert_eq!(level["ask"].as_f64(), Some(50.0));
        assert_eq!(level["maxAsk"].as_f64(), Some(30.0));
        assert_eq!(level["bid"].as_f64(), Some(12.0));
        assert_eq!(level["maxBid"].as_f64(), Some(12.0));
    }
}

#[cfg(test)]
mod history_cache_tests {
    use super::*;

    fn bar(time: i64) -> Value {
        json!({"time": time as f64, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 0.0})
    }

    #[test]
    fn empty_cache_fetches_the_whole_window() {
        assert_eq!(
            missing_history_ranges(None, 100, 1_000, 60),
            vec![(100, 1_000)]
        );
    }

    #[test]
    fn covered_cache_only_fetches_the_new_tail() {
        let cached = vec![bar(100), bar(160), bar(220)];
        assert_eq!(
            missing_history_ranges(Some(&cached), 100, 1_000, 60),
            vec![(160, 1_000)]
        );
    }

    #[test]
    fn asking_for_more_days_fetches_the_older_head_too() {
        let cached = vec![bar(500), bar(560), bar(1_000)];
        assert_eq!(
            missing_history_ranges(Some(&cached), 100, 1_000, 60),
            vec![(100, 500)]
        );
    }

    #[test]
    fn window_drops_bars_older_than_the_request() {
        let bars = history_window(vec![bar(50), bar(100), bar(200)], 100);
        assert_eq!(bars.len(), 2);
    }
}

#[cfg(test)]
mod calendar_bar_tests {
    use super::*;

    fn bar(time: i64, open: f64, high: f64, low: f64, close: f64, volume: f64) -> Value {
        json!({"time": time as f64, "open": open, "high": high, "low": low, "close": close, "volume": volume})
    }

    #[test]
    fn civil_round_trip() {
        for days in [-1, 0, 4, 19_000, 20_700] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn weeks_start_on_monday_and_months_on_the_first() {
        // 2026-09-23 (Wednesday) 00:00 UTC
        let wednesday = days_from_civil(2026, 9, 23) * 86_400;
        assert_eq!(
            calendar_bucket_start(wednesday, CalendarUnit::Week),
            days_from_civil(2026, 9, 21) * 86_400
        );
        assert_eq!(
            calendar_bucket_start(wednesday, CalendarUnit::Month),
            days_from_civil(2026, 9, 1) * 86_400
        );
        assert_eq!(
            calendar_bucket_start(wednesday, CalendarUnit::Year),
            days_from_civil(2026, 1, 1) * 86_400
        );
        // A session opening 22:00 UTC the evening before belongs to the next trading date.
        let sunday_evening = days_from_civil(2026, 9, 20) * 86_400 + 22 * 3_600;
        assert_eq!(
            calendar_bucket_start(sunday_evening, CalendarUnit::Week),
            days_from_civil(2026, 9, 21) * 86_400
        );
    }

    #[test]
    fn daily_bars_roll_up_into_months() {
        let sep1 = days_from_civil(2026, 9, 1) * 86_400;
        let sep2 = sep1 + 86_400;
        let oct1 = days_from_civil(2026, 10, 1) * 86_400;
        let bars = vec![
            bar(sep2, 11.0, 15.0, 10.0, 12.0, 5.0),
            bar(sep1, 10.0, 12.0, 9.0, 11.0, 3.0),
            bar(oct1, 12.0, 13.0, 11.0, 12.5, 1.0),
        ];
        let months = aggregate_calendar(bars, CalendarUnit::Month);
        assert_eq!(months.len(), 2);
        assert_eq!(months[0]["time"].as_f64().unwrap() as i64, sep1);
        assert_eq!(months[0]["open"].as_f64().unwrap(), 10.0);
        assert_eq!(months[0]["high"].as_f64().unwrap(), 15.0);
        assert_eq!(months[0]["low"].as_f64().unwrap(), 9.0);
        assert_eq!(months[0]["close"].as_f64().unwrap(), 12.0);
        assert_eq!(months[0]["volume"].as_f64().unwrap(), 8.0);
    }
}
