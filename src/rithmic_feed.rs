use std::{
    collections::{HashMap, HashSet},
    env,
    error::Error,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rithmic_rs::{
    ConnectStrategy, InstrumentInfo, LoginConfig, RithmicConfig, RithmicEnv, RithmicTickerPlant,
    RithmicTickerPlantHandle,
    api::RithmicResponse,
    error::RithmicError,
    rti::{messages::RithmicMessage, request_search_symbols},
};
use tokio::sync::{broadcast, mpsc};

use crate::{
    identity::synthetic_mac,
    maintenance_retry::MaintenanceBackoff,
    market_data::{self, MarketSnapshot},
    market_gateway::{
        Instrument as MarketInstrument, MarketCommand, MarketDataClient, MarketEvent,
        OptionContract, OptionType,
    },
    order_book::{BookError, DepthLevel, OrderBook, OrderUpdate, Side, UpdateAction, diff_levels},
};

const MAX_PUBLISHED_CATALOG_PRODUCTS: usize = 32;
const PRIORITY_CATALOG_PRODUCTS: &[(&str, &str)] = &[
    ("ES", "CME"),
    ("NQ", "CME"),
    ("MES", "CME"),
    ("MNQ", "CME"),
    ("RTY", "CME"),
    ("YM", "CBOT"),
    ("CL", "NYMEX"),
    ("MCL", "NYMEX"),
    ("GC", "COMEX"),
    ("MGC", "COMEX"),
    ("SI", "COMEX"),
    ("HG", "COMEX"),
    ("6E", "CME"),
    ("6B", "CME"),
    ("6J", "CME"),
    ("ZB", "CBOT"),
    ("ZN", "CBOT"),
    ("ZF", "CBOT"),
    ("ZC", "CBOT"),
    ("ZS", "CBOT"),
    ("ZW", "CBOT"),
];

#[derive(Debug)]
pub struct FeedError(String);

impl fmt::Display for FeedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for FeedError {}

#[derive(Clone)]
pub struct RithmicFeed {
    config: Arc<RithmicConfig>,
    initial_plant: Arc<Mutex<Option<RithmicTickerPlant>>>,
    client_active: Arc<AtomicBool>,
    catalog_cache: Arc<Mutex<Option<Vec<MarketInstrument>>>>,
}

impl RithmicFeed {
    pub async fn connect_from_env() -> Result<Self, FeedError> {
        let environment =
            parse_environment(&env::var("RITHMIC_ENV").unwrap_or_else(|_| "demo".to_owned()))?;
        let config = RithmicConfig::from_env(environment)
            .map_err(|error| FeedError(format!("Rithmic configuration failed: {error}")))?;
        let mut backoff = MaintenanceBackoff::from_env();
        let plant = loop {
            match connect_and_login(&config, ConnectStrategy::Simple).await {
                Ok(plant) => break plant,
                Err(error) if MaintenanceBackoff::is_retryable(&error) => {
                    backoff.wait("Ticker", &error).await;
                }
                Err(error) => return Err(FeedError(error)),
            }
        };
        Ok(Self {
            config: Arc::new(config),
            initial_plant: Arc::new(Mutex::new(Some(plant))),
            client_active: Arc::new(AtomicBool::new(false)),
            catalog_cache: Arc::new(Mutex::new(None)),
        })
    }

    pub fn client(&self) -> MarketDataClient {
        let (commands_tx, commands_rx) = mpsc::channel(32);
        let (events_tx, events_rx) = mpsc::channel(4096);
        tokio::spawn(run_market_supervisor(
            Arc::clone(&self.config),
            Arc::clone(&self.initial_plant),
            Arc::clone(&self.client_active),
            Arc::clone(&self.catalog_cache),
            commands_rx,
            events_tx,
        ));
        let publish_catalog_at_logon =
            env::var("DTC_PUBLISH_CATALOG_AT_LOGON").map_or(true, |value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "no"
                )
            });
        if publish_catalog_at_logon {
            MarketDataClient::with_logon_catalog(commands_tx, events_rx)
        } else {
            MarketDataClient::new(commands_tx, events_rx)
        }
    }
}

async fn connect_and_login(
    config: &RithmicConfig,
    strategy: ConnectStrategy,
) -> Result<RithmicTickerPlant, String> {
    let plant = RithmicTickerPlant::connect(config, strategy)
        .await
        .map_err(|error| format!("Rithmic connection failed: {error}"))?;
    let handle = plant.get_handle();
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    if let Err(error) = handle.login_with_config(login).await {
        handle.abort();
        let _ = plant.await_shutdown().await;
        return Err(format!("Rithmic login failed: {error}"));
    }
    Ok(plant)
}

enum SessionExit {
    ClientClosed,
    ConnectionLost(String),
}

struct ActiveClientGuard(Arc<AtomicBool>);

impl Drop for ActiveClientGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

async fn run_market_supervisor(
    config: Arc<RithmicConfig>,
    initial_plant: Arc<Mutex<Option<RithmicTickerPlant>>>,
    client_active: Arc<AtomicBool>,
    catalog_cache: Arc<Mutex<Option<Vec<MarketInstrument>>>>,
    mut commands: mpsc::Receiver<MarketCommand>,
    events: mpsc::Sender<MarketEvent>,
) {
    while client_active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        if commands.is_closed() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _active_guard = ActiveClientGuard(client_active);
    let mut plant = initial_plant
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    let mut subscriptions: HashMap<u32, (String, String)> = HashMap::new();
    let mut depth: HashMap<u32, DepthSubscription> = HashMap::new();
    let mut reconnecting = false;
    let mut backoff = Duration::from_millis(500);

    loop {
        let current_plant = match plant.take() {
            Some(plant) => plant,
            None => match connect_and_login(&config, ConnectStrategy::Simple).await {
                Ok(plant) => plant,
                Err(error) => {
                    if commands.is_closed() {
                        return;
                    }
                    let _ = events.send(MarketEvent::FeedError(error)).await;
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                    continue;
                }
            },
        };
        let mut handle = current_plant.get_handle();

        if reconnecting {
            match restore_subscriptions(&handle, &subscriptions, &mut depth).await {
                Ok(()) => {
                    let _ = events
                        .send(MarketEvent::FeedStatus { available: true })
                        .await;
                    for subscription in depth.values() {
                        emit_depth_snapshot(subscription, &events, 0).await;
                    }
                    backoff = Duration::from_millis(500);
                }
                Err(error) => {
                    let _ = events
                        .send(MarketEvent::FeedError(format!(
                            "Rithmic subscription restore failed: {error}"
                        )))
                        .await;
                    handle.abort();
                    let _ = current_plant.await_shutdown().await;
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                    continue;
                }
            }
        }

        match run_connected_session(
            &mut handle,
            &config.user,
            &catalog_cache,
            &mut commands,
            &events,
            &mut subscriptions,
            &mut depth,
        )
        .await
        {
            SessionExit::ClientClosed => {
                let _ = handle.disconnect().await;
                let _ = current_plant.await_shutdown().await;
                return;
            }
            SessionExit::ConnectionLost(reason) => {
                let _ = events
                    .send(MarketEvent::FeedStatus { available: false })
                    .await;
                let _ = events.send(MarketEvent::FeedError(reason)).await;
                handle.abort();
                let _ = current_plant.await_shutdown().await;
                reconnecting = true;
                if commands.is_closed() {
                    return;
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
}

async fn run_connected_session(
    handle: &mut RithmicTickerPlantHandle,
    user: &str,
    catalog_cache: &Arc<Mutex<Option<Vec<MarketInstrument>>>>,
    commands: &mut mpsc::Receiver<MarketCommand>,
    events: &mpsc::Sender<MarketEvent>,
    subscriptions: &mut HashMap<u32, (String, String)>,
    depth: &mut HashMap<u32, DepthSubscription>,
) -> SessionExit {
    // Cleared on every upstream connection; never serve a stale pre-reconnect quote.
    let mut snapshots: HashMap<(String, String), MarketSnapshot> = HashMap::new();
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { return SessionExit::ClientClosed };
                match command {
                    MarketCommand::DiscoverOptions { underlying, exchange, expiration, response } => {
                        let _ = response.send(discover_options(handle, &underlying, &exchange, expiration.as_deref()).await);
                    }
                    MarketCommand::LoadCatalog {
                        preferred_underlying,
                        response,
                    } => {
                        let cached = catalog_cache
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .clone();
                        let result = match cached {
                            Some(catalog) => Ok(catalog),
                            None => load_catalog(handle, &preferred_underlying).await.map(|catalog| {
                                *catalog_cache
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                    Some(catalog.clone());
                                catalog
                            }),
                        };
                        let _ = response.send(result);
                    }
                    MarketCommand::ListCatalogExchanges { response } => {
                        let _ = response.send(list_catalog_exchanges(handle, user).await);
                    }
                    MarketCommand::SearchCatalog {
                        search_text,
                        exchange,
                        search_type,
                        response,
                    } => {
                        let _ = response.send(
                            search_catalog(handle, user, &search_text, &exchange, search_type).await,
                        );
                    }
                    MarketCommand::EnumerateCatalog { exchange, underlying, roots_only, response } => {
                        let _ = response.send(enumerate_catalog(handle, user, &exchange, &underlying, roots_only).await);
                    }
                    MarketCommand::ResolveCatalogInstrument {
                        symbol,
                        exchange,
                        response,
                    } => {
                        let _ = response.send(
                            resolve_catalog_instrument(handle, &symbol, &exchange).await,
                        );
                    }
                    MarketCommand::Subscribe { symbol_id, symbol, exchange, response } => {
                        let result = if let Err(error) = validate_subscription(subscriptions, symbol_id, &symbol, &exchange) {
                            Err(error)
                        } else {
                            capture_market_snapshot(handle, &symbol, &exchange, false).await.map(|snapshot| {
                                snapshots.insert((symbol.clone(), exchange.clone()), snapshot.clone());
                                subscriptions.insert(symbol_id, (symbol, exchange));
                                snapshot
                            })
                        };
                        let _ = response.send(result);
                    }
                    MarketCommand::Snapshot { symbol, exchange, response } => {
                        let active = subscriptions.values().any(|(s, e)| s == &symbol && e == &exchange);
                        let result = if active {
                            if let Some(snapshot) = snapshots.get(&(symbol.clone(), exchange.clone())) {
                                Ok(snapshot.clone())
                            } else { Ok(MarketSnapshot::default()) }
                        } else { capture_market_snapshot(handle, &symbol, &exchange, true).await };
                        let _ = response.send(result);
                    }
                    MarketCommand::DepthSnapshot { symbol, exchange, tick_size, max_levels, response } => {
                        // DBO snapshot is a request, not a streaming subscription.
                        let result = create_depth_subscription(handle, 0, symbol, exchange, tick_size, max_levels, false)
                            .await.map(|(_, levels)| levels);
                        let _ = response.send(result);
                    }
                    MarketCommand::Unsubscribe { symbol_id, response } => {
                        let result = match subscriptions.remove(&symbol_id) {
                            Some((symbol, exchange)) => {
                                snapshots.remove(&(symbol.clone(), exchange.clone()));
                                unsubscribe(&handle, &symbol, &exchange).await
                            }
                            None => Err(format!("SymbolID {symbol_id} is not subscribed")),
                        };
                        let _ = response.send(result);
                    }
                    MarketCommand::SubscribeDepth {
                        symbol_id,
                        symbol,
                        exchange,
                        tick_size,
                        max_levels,
                        response,
                    } => {
                        let result = if depth.contains_key(&symbol_id) {
                            Err(format!("SymbolID {symbol_id} already has a depth subscription"))
                        } else {
                            let needs_upstream_subscription = !depth.values().any(|subscription| {
                                subscription.symbol == symbol && subscription.exchange == exchange
                            });
                            create_depth_subscription(
                                &handle,
                                symbol_id,
                                symbol,
                                exchange,
                                tick_size,
                                max_levels,
                                needs_upstream_subscription,
                            )
                            .await
                            .map(|(subscription, levels)| {
                                depth.insert(symbol_id, subscription);
                                levels
                            })
                        };
                        let _ = response.send(result);
                    }
                    MarketCommand::UnsubscribeDepth { symbol_id, response } => {
                        let result = match depth.remove(&symbol_id) {
                            Some(subscription) => {
                                let still_used = depth.values().any(|candidate| {
                                    candidate.symbol == subscription.symbol
                                        && candidate.exchange == subscription.exchange
                                });
                                if still_used {
                                    Ok(())
                                } else {
                                    unsubscribe_depth(
                                        &handle,
                                        &subscription.symbol,
                                        &subscription.exchange,
                                    )
                                    .await
                                }
                            }
                            None => Err(format!("SymbolID {symbol_id} has no depth subscription")),
                        };
                        let _ = response.send(result);
                    }
                }
            }
            response = handle.subscription_receiver.recv() => {
                match response {
                    Ok(response) => {
                        if let Some(reason) = connection_loss_reason(&response) {
                            return SessionExit::ConnectionLost(reason);
                        }
                        if let Some((symbol, exchange)) = market_data::key(&response.message).filter(|_| response.error.is_none()) {
                            if subscriptions.values().any(|(s,e)| s == symbol && e == exchange) {
                                let snapshot = snapshots.entry((symbol.to_owned(), exchange.to_owned())).or_default();
                                snapshot.apply(&response.message);
                                for (&symbol_id, _) in subscriptions.iter().filter(|(_, (s,e))| s == symbol && e == exchange) {
                                    match &response.message {
                                        RithmicMessage::TradeStatistics(_) | RithmicMessage::OpenInterest(_) | RithmicMessage::EndOfDayPrices(_) => {
                                            let _ = events.send(MarketEvent::Snapshot {symbol_id, snapshot: snapshot.clone()}).await;
                                        }
                                        RithmicMessage::LastTrade(v) if v.volume.is_some() || v.clear_bits.unwrap_or(0) & 8 != 0 => {
                                            let _ = events.send(MarketEvent::SessionVolume {symbol_id, volume: snapshot.volume.unwrap_or(f64::MAX)}).await;
                                        }
                                        RithmicMessage::BestBidOffer(v) if v.bid_price.is_none() || v.ask_price.is_none() || v.clear_bits.unwrap_or(0) != 0 => {
                                            let _ = events.send(MarketEvent::Snapshot {symbol_id, snapshot: snapshot.clone()}).await;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        forward_response(&handle, subscriptions, depth, response, events).await
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        return SessionExit::ConnectionLost(format!(
                            "Rithmic subscription stream lagged by {skipped} messages; reconnecting"
                        ));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return SessionExit::ConnectionLost(
                            "Rithmic subscription stream closed; reconnecting".to_owned(),
                        );
                    }
                }
            }
        }
    }
}

async fn discover_options(
    handle: &RithmicTickerPlantHandle,
    underlying: &str,
    exchange: &str,
    expiration: Option<&str>,
) -> Result<Vec<OptionContract>, String> {
    let responses = handle
        .get_instrument_by_underlying(underlying, exchange, expiration)
        .await
        .map_err(|error| error.to_string())?;
    let response_count = responses.len();
    let mut contracts = Vec::new();
    let mut expirations = Vec::new();
    let mut samples = Vec::new();
    for response in responses {
        if let Some(error) = response.error {
            return Err(error.to_string());
        }
        let item = match response.message {
            RithmicMessage::ResponseGetInstrumentByUnderlyingKeys(keys) => {
                expirations.extend(keys.expiration_date);
                continue;
            }
            RithmicMessage::ResponseGetInstrumentByUnderlying(item) => item,
            _ => continue,
        };
        if samples.len() < 4 {
            samples.push(format!(
                "symbol={:?} type={:?} underlying={:?} expiry={:?} pc={:?} strike={:?}",
                item.symbol,
                item.instrument_type,
                item.underlying_symbol,
                item.expiration_date,
                item.put_call_indicator,
                item.strike_price
            ));
        }
        if let Some(value) = item
            .expiration_date
            .clone()
            .filter(|value| !value.is_empty())
        {
            expirations.push(value);
        }
        if let Some(contract) = option_contract_from_underlying(item, underlying, exchange) {
            contracts.push(contract);
        }
    }
    if contracts.is_empty() && expiration.is_none() {
        println!(
            "[Options] reference query {underlying}.{exchange}: {response_count} responses, {} expirations, samples: {}",
            expirations.len(),
            samples.join(" | ")
        );
        expirations.sort();
        expirations.dedup();
        let mut expiry_samples = Vec::new();
        for expiry in expirations.into_iter().take(16) {
            let responses = handle
                .get_instrument_by_underlying(underlying, exchange, Some(&expiry))
                .await
                .map_err(|error| error.to_string())?;
            for response in responses {
                if let Some(error) = response.error {
                    return Err(error.to_string());
                }
                let RithmicMessage::ResponseGetInstrumentByUnderlying(item) = response.message
                else {
                    continue;
                };
                if expiry_samples.len() < 8 {
                    expiry_samples.push(format!(
                        "query={expiry} symbol={:?} type={:?} underlying={:?} expiry={:?} pc={:?} strike={:?} exchange={:?}",
                        item.symbol,
                        item.instrument_type,
                        item.underlying_symbol,
                        item.expiration_date,
                        item.put_call_indicator,
                        item.strike_price,
                        item.exchange
                    ));
                }
                if let Some(contract) = option_contract_from_underlying(item, underlying, exchange)
                {
                    contracts.push(contract);
                }
            }
        }
        if contracts.is_empty() && !expiry_samples.is_empty() {
            println!(
                "[Options] rejected instrument samples for {underlying}.{exchange}: {}",
                expiry_samples.join(" | ")
            );
        }
    }
    contracts.sort_by(|a, b| {
        a.expiration
            .cmp(&b.expiration)
            .then_with(|| a.strike.total_cmp(&b.strike))
            .then_with(|| (a.option_type as u8).cmp(&(b.option_type as u8)))
    });
    contracts.dedup_by(|a, b| a.symbol == b.symbol && a.exchange == b.exchange);
    Ok(contracts)
}

fn option_contract_from_underlying(
    item: rithmic_rs::rti::ResponseGetInstrumentByUnderlying,
    underlying: &str,
    exchange: &str,
) -> Option<OptionContract> {
    if !item
        .instrument_type
        .as_deref()
        .is_some_and(|kind| kind.to_ascii_uppercase().contains("OPTION"))
    {
        return None;
    }
    let option_type = match item.put_call_indicator.as_deref().map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("C") || value.eq_ignore_ascii_case("CALL") => {
            OptionType::Call
        }
        Some(value) if value.eq_ignore_ascii_case("P") || value.eq_ignore_ascii_case("PUT") => {
            OptionType::Put
        }
        _ => return None,
    };
    let (Some(symbol), Some(strike), Some(expiration)) =
        (item.symbol, item.strike_price, item.expiration_date)
    else {
        return None;
    };
    if symbol.is_empty() || !strike.is_finite() || strike <= 0.0 {
        return None;
    }
    Some(OptionContract {
        symbol,
        exchange: item.exchange.unwrap_or_else(|| exchange.to_owned()),
        underlying: item
            .underlying_symbol
            .unwrap_or_else(|| underlying.to_owned()),
        expiration,
        strike,
        option_type,
        multiplier: item
            .single_point_value
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(1.0),
        tick_size: item
            .min_qprice_change
            .filter(|value| value.is_finite() && *value > 0.0),
    })
}

fn connection_loss_reason(response: &RithmicResponse) -> Option<String> {
    if is_connection_loss(&response.message, response.error.as_ref()) {
        Some(format!(
            "Rithmic connection lost ({:?}); reconnecting",
            response.message
        ))
    } else {
        None
    }
}

fn is_connection_loss(message: &RithmicMessage, error: Option<&RithmicError>) -> bool {
    if matches!(
        (message, error),
        (
            RithmicMessage::HeartbeatTimeout,
            Some(RithmicError::RequestRejected(_))
        )
    ) {
        return false;
    }
    matches!(
        message,
        RithmicMessage::HeartbeatTimeout
            | RithmicMessage::ForcedLogout(_)
            | RithmicMessage::ConnectionError
    ) || error.is_some_and(RithmicError::is_connection_issue)
}

async fn list_catalog_exchanges(
    handle: &RithmicTickerPlantHandle,
    user: &str,
) -> Result<Vec<String>, String> {
    let responses = handle
        .list_exchange_permissions(user)
        .await
        .map_err(|error| error.to_string())?;
    let mut exchanges = Vec::new();
    for response in responses {
        if let Some(error) = response.error {
            return Err(error.to_string());
        }
        if let RithmicMessage::ResponseListExchangePermissions(permission) = response.message {
            if let Some(exchange) = permission.exchange.filter(|value| !value.trim().is_empty()) {
                exchanges.push(exchange);
            }
        }
    }
    exchanges.sort_unstable();
    exchanges.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    Ok(exchanges)
}

async fn load_catalog(
    handle: &RithmicTickerPlantHandle,
    preferred_underlying: &str,
) -> Result<Vec<MarketInstrument>, String> {
    let responses = handle
        .get_product_codes(None, Some(true))
        .await
        .map_err(|error| error.to_string())?;
    let mut products = Vec::new();
    for response in responses {
        if let Some(error) = response.error {
            return Err(error.to_string());
        }
        if let RithmicMessage::ResponseProductCodes(product) = response.message {
            if let (Some(product_code), Some(exchange)) = (product.product_code, product.exchange) {
                if !product_code.trim().is_empty() && !exchange.trim().is_empty() {
                    products.push((product_code, exchange));
                }
            }
        }
    }
    // The TOI list is intentionally selective and can omit active micros such
    // as MES/MNQ. Probe the common roots explicitly; products unavailable to
    // this account are skipped when the front-month request is rejected.
    products.extend(
        PRIORITY_CATALOG_PRODUCTS
            .iter()
            .map(|(product, exchange)| ((*product).to_owned(), (*exchange).to_owned())),
    );
    products.sort_by(|left, right| {
        catalog_product_priority(&left.0, preferred_underlying)
            .cmp(&catalog_product_priority(&right.0, preferred_underlying))
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.0.cmp(&right.0))
    });
    products.dedup_by(|left, right| {
        left.0.eq_ignore_ascii_case(&right.0) && left.1.eq_ignore_ascii_case(&right.1)
    });
    products.truncate(MAX_PUBLISHED_CATALOG_PRODUCTS);

    let mut instruments = Vec::with_capacity(products.len());
    for (product, exchange) in products {
        let front = match handle
            .get_front_month_contract(&product, &exchange, false)
            .await
        {
            Ok(response) if response.error.is_none() => response,
            _ => continue,
        };
        let RithmicMessage::ResponseFrontMonthContract(front) = front.message else {
            continue;
        };
        let symbol = front.trading_symbol.or(front.symbol).unwrap_or_default();
        let trading_exchange = front
            .trading_exchange
            .or(front.exchange)
            .unwrap_or(exchange);
        if symbol.is_empty() || trading_exchange.is_empty() {
            continue;
        }
        if let Ok(instrument) = resolve_catalog_instrument(handle, &symbol, &trading_exchange).await
        {
            instruments.push(instrument);
        }
    }
    instruments.sort_by(|left, right| {
        left.exchange
            .cmp(&right.exchange)
            .then_with(|| left.underlying_symbol.cmp(&right.underlying_symbol))
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    instruments.dedup_by(|left, right| {
        left.symbol.eq_ignore_ascii_case(&right.symbol)
            && left.exchange.eq_ignore_ascii_case(&right.exchange)
    });
    if instruments.is_empty() {
        Err("Rithmic returned no publishable front-month futures definitions".to_owned())
    } else {
        Ok(instruments)
    }
}

fn catalog_product_priority(product: &str, preferred_underlying: &str) -> usize {
    if product.eq_ignore_ascii_case(preferred_underlying) {
        return 0;
    }
    PRIORITY_CATALOG_PRODUCTS
        .iter()
        .position(|(candidate, _)| product.eq_ignore_ascii_case(candidate))
        .map_or(usize::MAX, |index| index + 1)
}

async fn search_catalog(
    handle: &RithmicTickerPlantHandle,
    user: &str,
    search_text: &str,
    exchange: &str,
    search_type: i32,
) -> Result<Vec<MarketInstrument>, String> {
    // Upstream search has no Description selector. Enumerate when searching
    // descriptions (or both fields), then let DTC apply its exact filter.
    if search_type != 1 {
        return enumerate_catalog(handle, user, exchange, "", false).await;
    }
    let responses = handle
        .search_symbols(
            search_text,
            (!exchange.is_empty()).then_some(exchange),
            None,
            Some(request_search_symbols::InstrumentType::Future),
            Some(request_search_symbols::Pattern::Contains),
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut keys = Vec::new();
    for response in responses {
        if let Some(error) = response.error {
            return Err(error.to_string());
        }
        if let RithmicMessage::ResponseSearchSymbols(found) = response.message {
            if let (Some(symbol), Some(exchange)) = (found.symbol, found.exchange) {
                if !symbol.is_empty() && !exchange.is_empty() {
                    keys.push((symbol, exchange));
                }
            }
        }
    }
    keys.sort_unstable();
    keys.dedup();

    let mut instruments = Vec::with_capacity(keys.len());
    for (symbol, exchange) in keys {
        instruments.push(resolve_catalog_instrument(handle, &symbol, &exchange).await?);
    }
    Ok(instruments)
}

async fn enumerate_catalog(
    handle: &RithmicTickerPlantHandle,
    user: &str,
    exchange: &str,
    underlying: &str,
    roots_only: bool,
) -> Result<Vec<MarketInstrument>, String> {
    let exchanges = if exchange.is_empty() {
        list_catalog_exchanges(handle, user).await?
    } else {
        vec![exchange.to_owned()]
    };
    let mut products = Vec::new();
    for exchange in exchanges {
        if !underlying.is_empty() {
            products.push((underlying.to_owned(), exchange, String::new()));
            continue;
        }
        for response in handle
            .get_product_codes(Some(&exchange), Some(false))
            .await
            .map_err(|e| e.to_string())?
        {
            if let Some(error) = response.error {
                return Err(error.to_string());
            }
            if let RithmicMessage::ResponseProductCodes(item) = response.message {
                if let Some(product) = item.product_code.filter(|p| !p.is_empty()) {
                    products.push((
                        product,
                        item.exchange.unwrap_or_else(|| exchange.clone()),
                        item.symbol_name.unwrap_or_default(),
                    ));
                }
            }
        }
    }
    products.sort();
    products.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
    let mut instruments = Vec::new();
    let mut keys = HashSet::new();
    for (product, exchange, description) in products {
        if roots_only {
            instruments.push(MarketInstrument {
                symbol: String::new(),
                exchange,
                underlying_symbol: product,
                description,
                min_price_increment: 0.0,
                price_display_format: -1,
                currency_value_per_increment: 0.0,
                contract_size: 0.0,
                currency: String::new(),
                expiration_date: 0,
                exchange_symbol: String::new(),
            });
            continue;
        }
        for response in handle
            .get_instrument_by_underlying(&product, &exchange, None)
            .await
            .map_err(|e| e.to_string())?
        {
            if let Some(error) = response.error {
                return Err(error.to_string());
            }
            if let RithmicMessage::ResponseGetInstrumentByUnderlying(item) = response.message {
                if item
                    .instrument_type
                    .as_deref()
                    .is_some_and(|kind| !kind.eq_ignore_ascii_case("FUTURE"))
                {
                    continue;
                }
                if let Some(symbol) = item.symbol.filter(|s| !s.is_empty()) {
                    let exchange = item.exchange.unwrap_or_else(|| exchange.clone());
                    if keys.insert((symbol.clone(), exchange.clone())) {
                        instruments
                            .push(resolve_catalog_instrument(handle, &symbol, &exchange).await?);
                    }
                }
            }
        }
    }
    instruments.sort_by(|a, b| {
        (&a.exchange, &a.underlying_symbol, &a.symbol).cmp(&(
            &b.exchange,
            &b.underlying_symbol,
            &b.symbol,
        ))
    });
    Ok(instruments)
}

async fn resolve_catalog_instrument(
    handle: &RithmicTickerPlantHandle,
    symbol: &str,
    exchange: &str,
) -> Result<MarketInstrument, String> {
    if symbol.trim().is_empty() || exchange.trim().is_empty() {
        return Err("Both symbol and exchange are required for Rithmic reference data".to_owned());
    }
    let response = handle
        .get_reference_data(symbol, exchange)
        .await
        .map_err(|error| error.to_string())?;
    if let Some(error) = response.error {
        return Err(error.to_string());
    }
    let RithmicMessage::ResponseReferenceData(reference) = response.message else {
        return Err("Rithmic returned no reference data".to_owned());
    };
    let info = InstrumentInfo::try_from(&reference).map_err(|error| error.to_string())?;
    instrument_from_rithmic(info)
}

fn instrument_from_rithmic(info: InstrumentInfo) -> Result<MarketInstrument, String> {
    if info
        .instrument_type
        .as_deref()
        .is_some_and(|value| !value.eq_ignore_ascii_case("FUTURE"))
    {
        return Err(format!(
            "{}.{}, type {:?}, is not a futures contract",
            info.symbol, info.exchange, info.instrument_type
        ));
    }
    let tick_size = info
        .tick_size
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or_else(|| format!("{}.{} has no valid tick size", info.symbol, info.exchange))?;
    let point_value = info
        .point_value
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(0.0);
    let price_display_format = i32::from(info.price_precision());
    let underlying_symbol = info
        .product_code
        .clone()
        .or_else(|| info.underlying.clone())
        .unwrap_or_else(|| info.symbol.clone());
    Ok(MarketInstrument {
        symbol: info.symbol,
        exchange: info.exchange,
        underlying_symbol,
        description: info.name.unwrap_or_else(|| "Futures contract".to_owned()),
        min_price_increment: tick_size as f32,
        price_display_format,
        currency_value_per_increment: (tick_size * point_value) as f32,
        contract_size: point_value as f32,
        currency: info.currency.unwrap_or_default(),
        expiration_date: info
            .expiration_date
            .as_deref()
            .and_then(market_data::date_to_unix)
            .unwrap_or(0),
        exchange_symbol: info.exchange_symbol.unwrap_or_default(),
    })
}

async fn restore_subscriptions(
    handle: &RithmicTickerPlantHandle,
    subscriptions: &HashMap<u32, (String, String)>,
    depth: &mut HashMap<u32, DepthSubscription>,
) -> Result<(), String> {
    let unique: HashSet<_> = subscriptions.values().cloned().collect();
    for (symbol, exchange) in unique {
        subscribe(handle, &symbol, &exchange).await?;
    }
    let unique_depth: HashSet<_> = depth
        .values()
        .map(|subscription| (subscription.symbol.clone(), subscription.exchange.clone()))
        .collect();
    for (symbol, exchange) in unique_depth {
        let response = handle
            .subscribe_depth_by_order_update(&symbol, &exchange)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(error) = response.error {
            return Err(error.to_string());
        }
    }
    for subscription in depth.values_mut() {
        refresh_depth_snapshot(handle, subscription).await?;
        sync_published_depth(subscription);
    }
    Ok(())
}

struct DepthSubscription {
    symbol_id: u32,
    symbol: String,
    exchange: String,
    max_levels: usize,
    book: OrderBook,
    published_bids: Vec<DepthLevel>,
    published_asks: Vec<DepthLevel>,
    crossed_updates: u16,
}

async fn create_depth_subscription(
    handle: &RithmicTickerPlantHandle,
    symbol_id: u32,
    symbol: String,
    exchange: String,
    tick_size: f64,
    max_levels: usize,
    needs_upstream_subscription: bool,
) -> Result<(DepthSubscription, Vec<DepthLevel>), String> {
    if needs_upstream_subscription {
        let response = handle
            .subscribe_depth_by_order_update(&symbol, &exchange)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(error) = response.error {
            return Err(error.to_string());
        }
    }
    let mut subscription = DepthSubscription {
        symbol_id,
        symbol,
        exchange,
        max_levels,
        book: OrderBook::new(tick_size).map_err(|error| error.to_string())?,
        published_bids: Vec::new(),
        published_asks: Vec::new(),
        crossed_updates: 0,
    };
    if let Err(error) = refresh_depth_snapshot(handle, &mut subscription).await {
        if needs_upstream_subscription {
            let _ = handle
                .unsubscribe_depth_by_order_update(&subscription.symbol, &subscription.exchange)
                .await;
        }
        return Err(error);
    }
    sync_published_depth(&mut subscription);
    let levels = depth_levels(&subscription);
    Ok((subscription, levels))
}

async fn unsubscribe_depth(
    handle: &RithmicTickerPlantHandle,
    symbol: &str,
    exchange: &str,
) -> Result<(), String> {
    let response = handle
        .unsubscribe_depth_by_order_update(symbol, exchange)
        .await
        .map_err(|error| error.to_string())?;
    response
        .error
        .map_or(Ok(()), |error| Err(error.to_string()))
}

async fn refresh_depth_snapshot(
    handle: &RithmicTickerPlantHandle,
    subscription: &mut DepthSubscription,
) -> Result<(), String> {
    let responses = handle
        .get_depth_by_order_snapshot(&subscription.symbol, &subscription.exchange)
        .await
        .map_err(|error| error.to_string())?;
    let mut orders = Vec::new();
    let mut sequence = None;
    for response in responses {
        if let Some(error) = response.error {
            return Err(error.to_string());
        }
        match response.message {
            RithmicMessage::ResponseDepthByOrderSnapshot(snapshot) => {
                sequence = max_sequence(sequence, snapshot.sequence_number);
                let Some(side) = snapshot.depth_side.and_then(rithmic_side) else {
                    continue;
                };
                let Some(price) = snapshot.depth_price else {
                    continue;
                };
                let count = snapshot
                    .exchange_order_id
                    .len()
                    .min(snapshot.depth_size.len());
                for index in 0..count {
                    orders.push(OrderUpdate {
                        action: UpdateAction::New,
                        order_id: snapshot.exchange_order_id[index].clone(),
                        side,
                        price,
                        quantity: snapshot.depth_size[index],
                        priority: snapshot
                            .depth_order_priority
                            .get(index)
                            .copied()
                            .unwrap_or_default(),
                    });
                }
            }
            RithmicMessage::DepthByOrderEndEvent(end) => {
                sequence = max_sequence(sequence, end.sequence_number);
            }
            _ => {}
        }
    }
    if orders.is_empty() {
        return Err("Rithmic returned an empty Depth-by-Order snapshot".to_owned());
    }
    subscription
        .book
        .reset_snapshot(sequence, orders)
        .map_err(|error| error.to_string())
}

fn depth_levels(subscription: &DepthSubscription) -> Vec<DepthLevel> {
    let mut levels = subscription.book.levels(Side::Bid, subscription.max_levels);
    levels.extend(subscription.book.levels(Side::Ask, subscription.max_levels));
    levels
}

fn sync_published_depth(subscription: &mut DepthSubscription) {
    subscription.published_bids = subscription.book.levels(Side::Bid, subscription.max_levels);
    subscription.published_asks = subscription.book.levels(Side::Ask, subscription.max_levels);
    subscription.crossed_updates = 0;
}

fn publishable_depth_changes(
    subscription: &mut DepthSubscription,
) -> Option<Vec<crate::order_book::LevelUpdate>> {
    if subscription.book.is_crossed() {
        subscription.crossed_updates = subscription.crossed_updates.saturating_add(1);
        return None;
    }

    let bids = subscription.book.levels(Side::Bid, subscription.max_levels);
    let asks = subscription.book.levels(Side::Ask, subscription.max_levels);
    let mut changes = diff_levels(&subscription.published_bids, &bids);
    changes.extend(diff_levels(&subscription.published_asks, &asks));
    subscription.published_bids = bids;
    subscription.published_asks = asks;
    subscription.crossed_updates = 0;
    Some(changes)
}

async fn subscribe(
    handle: &RithmicTickerPlantHandle,
    symbol: &str,
    exchange: &str,
) -> Result<(), String> {
    let response = tokio::time::timeout(Duration::from_secs(5), handle.subscribe(symbol, exchange))
        .await
        .map_err(|_| format!("Timed out subscribing to {symbol}.{exchange}"))?
        .map_err(|error| error.to_string())?;
    if let Some(error) = response.error {
        return Err(error.to_string());
    }
    // Statistics permissions can differ from Last/BBO permissions. Preserve
    // the usable feed and leave unavailable statistics explicitly unset.
    for result in [
        tokio::time::timeout(
            Duration::from_secs(3),
            handle.subscribe_session_prices(symbol, exchange),
        )
        .await
        .map_err(|_| "session-price subscription timed out".to_owned())
        .and_then(|value| value.map_err(|error| error.to_string())),
        tokio::time::timeout(
            Duration::from_secs(3),
            handle.subscribe_open_interest(symbol, exchange),
        )
        .await
        .map_err(|_| "open-interest subscription timed out".to_owned())
        .and_then(|value| value.map_err(|error| error.to_string())),
        tokio::time::timeout(
            Duration::from_secs(3),
            handle.subscribe_end_of_day_prices(symbol, exchange),
        )
        .await
        .map_err(|_| "end-of-day subscription timed out".to_owned())
        .and_then(|value| value.map_err(|error| error.to_string())),
    ] {
        match result {
            Ok(response) if response.error.is_none() => {}
            result => eprintln!(
                "[Feed] Optional session statistics unavailable for {symbol}.{exchange}: {result:?}"
            ),
        }
    }
    Ok(())
}

async fn unsubscribe(
    handle: &RithmicTickerPlantHandle,
    symbol: &str,
    exchange: &str,
) -> Result<(), String> {
    let mut first_error = None;
    for result in [
        tokio::time::timeout(Duration::from_secs(3), handle.unsubscribe(symbol, exchange))
            .await
            .map_err(|_| "market-data unsubscribe timed out".to_owned())
            .and_then(|value| value.map_err(|error| error.to_string())),
        tokio::time::timeout(
            Duration::from_secs(3),
            handle.unsubscribe_session_prices(symbol, exchange),
        )
        .await
        .map_err(|_| "session-price unsubscribe timed out".to_owned())
        .and_then(|value| value.map_err(|error| error.to_string())),
        tokio::time::timeout(
            Duration::from_secs(3),
            handle.unsubscribe_open_interest(symbol, exchange),
        )
        .await
        .map_err(|_| "open-interest unsubscribe timed out".to_owned())
        .and_then(|value| value.map_err(|error| error.to_string())),
        tokio::time::timeout(
            Duration::from_secs(3),
            handle.unsubscribe_end_of_day_prices(symbol, exchange),
        )
        .await
        .map_err(|_| "end-of-day unsubscribe timed out".to_owned())
        .and_then(|value| value.map_err(|error| error.to_string())),
    ] {
        let result = result.and_then(|r| r.error.map_or(Ok(()), |e| Err(e.to_string())));
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn validate_subscription(
    subscriptions: &HashMap<u32, (String, String)>,
    symbol_id: u32,
    symbol: &str,
    exchange: &str,
) -> Result<(), String> {
    if subscriptions.contains_key(&symbol_id) {
        return Err(format!("SymbolID {symbol_id} is already subscribed"));
    }
    if subscriptions
        .values()
        .any(|(s, e)| s.eq_ignore_ascii_case(symbol) && e.eq_ignore_ascii_case(exchange))
    {
        return Err("Symbol/Exchange already has a different SymbolID".to_owned());
    }
    Ok(())
}

async fn capture_market_snapshot(
    handle: &RithmicTickerPlantHandle,
    symbol: &str,
    exchange: &str,
    temporary: bool,
) -> Result<MarketSnapshot, String> {
    let mut receiver = handle.subscription_receiver.resubscribe();
    let result = async {
        subscribe(handle, symbol, exchange).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut quiet = deadline;
        let mut seen = false;
        let mut groups = 0_u8;
        let mut snapshot = MarketSnapshot::default();
        loop {
            match tokio::time::timeout_at(quiet.min(deadline), receiver.recv()).await {
                Ok(Ok(response)) => {
                    if let Some(reason) = connection_loss_reason(&response) {
                        return Err(reason);
                    }
                    if response.error.is_none()
                        && market_data::key(&response.message) == Some((symbol, exchange))
                    {
                        snapshot.apply(&response.message);
                        seen = true;
                        let group = match &response.message {
                            RithmicMessage::LastTrade(_) => 1,
                            RithmicMessage::BestBidOffer(_) => 2,
                            RithmicMessage::TradeStatistics(_) => 4,
                            RithmicMessage::OpenInterest(_) => 8,
                            RithmicMessage::EndOfDayPrices(_) => 16,
                            _ => 0,
                        };
                        if groups & group == 0 {
                            groups |= group;
                            quiet = tokio::time::Instant::now() + Duration::from_millis(150);
                        }
                    }
                }
                Ok(Err(error)) => return Err(format!("Snapshot stream failed: {error}")),
                Err(_) if seen => return Ok(snapshot),
                Err(_) => return Err("No initial market snapshot received from Rithmic".to_owned()),
            }
            if tokio::time::Instant::now() >= deadline {
                return if seen {
                    Ok(snapshot)
                } else {
                    Err("No initial market snapshot received".to_owned())
                };
            }
        }
    }
    .await;
    if temporary || result.is_err() {
        let cleanup = unsubscribe(handle, symbol, exchange).await;
        if result.is_ok() {
            cleanup?;
        }
    }
    result
}

async fn forward_response(
    handle: &RithmicTickerPlantHandle,
    subscriptions: &HashMap<u32, (String, String)>,
    depth: &mut HashMap<u32, DepthSubscription>,
    response: RithmicResponse,
    events: &mpsc::Sender<MarketEvent>,
) {
    if let Some(error) = response.error {
        let error = error.to_string();
        if !error.trim().is_empty() {
            let _ = events.send(MarketEvent::FeedError(error)).await;
        }
        return;
    }
    match response.message {
        RithmicMessage::LastTrade(trade) => {
            let (Some(price), Some(size)) = (trade.trade_price, trade.trade_size) else {
                return;
            };
            if size <= 0 {
                return;
            }
            let symbol = trade.symbol.as_deref().unwrap_or_default();
            let exchange = trade.exchange.as_deref().unwrap_or_default();
            let datetime_us = timestamp_us(
                trade.source_ssboe.or(trade.ssboe),
                trade.source_usecs.or(trade.usecs),
                trade.source_nsecs,
            );
            let at_bid_or_ask = match trade.aggressor {
                Some(1) => 2, // Rithmic buy aggressor traded at the ask
                Some(2) => 1, // Rithmic sell aggressor traded at the bid
                _ => 0,
            };
            for (&symbol_id, _) in subscriptions
                .iter()
                .filter(|(_, (sub_symbol, sub_exchange))| {
                    sub_symbol == symbol && sub_exchange == exchange
                })
            {
                let _ = events
                    .send(MarketEvent::LastTrade {
                        symbol_id,
                        price,
                        volume: size as f64,
                        datetime_us,
                        at_bid_or_ask,
                        is_snapshot: trade.is_snapshot.unwrap_or(false),
                    })
                    .await;
            }
        }
        RithmicMessage::BestBidOffer(quote) => {
            let (Some(bid_price), Some(bid_size), Some(ask_price), Some(ask_size)) = (
                quote.bid_price,
                quote.bid_size,
                quote.ask_price,
                quote.ask_size,
            ) else {
                return;
            };
            let symbol = quote.symbol.as_deref().unwrap_or_default();
            let exchange = quote.exchange.as_deref().unwrap_or_default();
            let datetime_us = timestamp_us(quote.ssboe, quote.usecs, None);
            for (&symbol_id, _) in subscriptions
                .iter()
                .filter(|(_, (sub_symbol, sub_exchange))| {
                    sub_symbol == symbol && sub_exchange == exchange
                })
            {
                let _ = events
                    .send(MarketEvent::BestBidAsk {
                        symbol_id,
                        bid_price,
                        bid_quantity: bid_size.max(0) as f64,
                        ask_price,
                        ask_quantity: ask_size.max(0) as f64,
                        datetime_us,
                    })
                    .await;
            }
        }
        RithmicMessage::DepthByOrder(update) => {
            let symbol = update.symbol.as_deref().unwrap_or_default();
            let exchange = update.exchange.as_deref().unwrap_or_default();
            let matching_ids: Vec<_> = depth
                .iter()
                .filter(|(_, subscription)| {
                    subscription.symbol == symbol && subscription.exchange == exchange
                })
                .map(|(&symbol_id, _)| symbol_id)
                .collect();
            let datetime_us = timestamp_us(
                update.source_ssboe.or(update.ssboe),
                update.source_usecs.or(update.usecs),
                update.source_nsecs,
            );
            for symbol_id in matching_ids {
                let Some(subscription) = depth.get_mut(&symbol_id) else {
                    continue;
                };
                let applied = parse_depth_updates(&update).and_then(|updates| {
                    subscription.book.apply_batch(
                        update.sequence_number,
                        updates,
                        subscription.max_levels,
                    )
                });
                match applied {
                    Ok(_) => {
                        let Some(changes) = publishable_depth_changes(subscription) else {
                            if subscription.crossed_updates >= 128 {
                                let reason = format!(
                                    "DBO book remained crossed for {} updates; rebuilding one snapshot",
                                    subscription.crossed_updates
                                );
                                let _ = events.send(MarketEvent::FeedError(reason)).await;
                                match refresh_depth_snapshot(handle, subscription).await {
                                    Ok(()) => {
                                        sync_published_depth(subscription);
                                        emit_depth_snapshot(subscription, events, datetime_us)
                                            .await;
                                    }
                                    Err(snapshot_error) => {
                                        let _ = events
                                            .send(MarketEvent::FeedError(format!(
                                                "DBO snapshot rebuild failed: {snapshot_error}"
                                            )))
                                            .await;
                                    }
                                }
                            }
                            continue;
                        };
                        let count = changes.len();
                        for (index, change) in changes.into_iter().enumerate() {
                            let _ = events
                                .send(MarketEvent::DepthUpdate {
                                    symbol_id: subscription.symbol_id,
                                    update: change,
                                    datetime_us,
                                    is_final: index + 1 == count,
                                })
                                .await;
                        }
                    }
                    Err(error) => {
                        let reason = format!(
                            "DBO book desynchronized ({error}); update sequence={:?}, book sequence={:?}, actions={:?}, ids={}, prices={}, sizes={}; rebuilding snapshot",
                            update.sequence_number,
                            subscription.book.last_sequence(),
                            update.update_type,
                            update.exchange_order_id.len(),
                            update.depth_price.len(),
                            update.depth_size.len(),
                        );
                        let _ = events.send(MarketEvent::FeedError(reason)).await;
                        match refresh_depth_snapshot(handle, subscription).await {
                            Ok(()) => {
                                sync_published_depth(subscription);
                                emit_depth_snapshot(subscription, events, datetime_us).await;
                            }
                            Err(snapshot_error) => {
                                let _ = events
                                    .send(MarketEvent::FeedError(format!(
                                        "DBO snapshot rebuild failed: {snapshot_error}"
                                    )))
                                    .await;
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

async fn emit_depth_snapshot(
    subscription: &DepthSubscription,
    events: &mpsc::Sender<MarketEvent>,
    datetime_us: i64,
) {
    let levels = depth_levels(subscription);
    let count = levels.len();
    for (index, level) in levels.into_iter().enumerate() {
        let _ = events
            .send(MarketEvent::DepthSnapshotLevel {
                symbol_id: subscription.symbol_id,
                level,
                datetime_us,
                is_first: index == 0,
                is_last: index + 1 == count,
            })
            .await;
    }
}

fn parse_depth_updates(
    update: &rithmic_rs::rti::DepthByOrder,
) -> Result<Vec<OrderUpdate>, BookError> {
    let mut updates = Vec::with_capacity(update.update_type.len());
    for index in 0..update.update_type.len() {
        let action = match update.update_type[index] {
            1 => UpdateAction::New,
            2 => UpdateAction::Change,
            3 => UpdateAction::Delete,
            _ => continue,
        };
        let order_id = update
            .exchange_order_id
            .get(index)
            .cloned()
            .ok_or(BookError::EmptyOrderId)?;
        let side = update
            .transaction_type
            .get(index)
            .copied()
            .and_then(rithmic_side)
            .unwrap_or(Side::Bid);
        updates.push(OrderUpdate {
            action,
            order_id,
            side,
            price: update.depth_price.get(index).copied().unwrap_or_default(),
            quantity: update.depth_size.get(index).copied().unwrap_or_default(),
            priority: update
                .depth_order_priority
                .get(index)
                .copied()
                .unwrap_or_default(),
        });
    }
    Ok(updates)
}

fn rithmic_side(value: i32) -> Option<Side> {
    match value {
        1 => Some(Side::Bid),
        2 => Some(Side::Ask),
        _ => None,
    }
}

fn max_sequence(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn timestamp_us(seconds: Option<i32>, microseconds: Option<i32>, nanoseconds: Option<i32>) -> i64 {
    let seconds = i64::from(seconds.unwrap_or_default());
    let fraction = nanoseconds
        .map(|value| i64::from(value.clamp(0, 999_999_999)) / 1_000)
        .unwrap_or_else(|| i64::from(microseconds.unwrap_or_default().clamp(0, 999_999)));
    seconds.saturating_mul(1_000_000).saturating_add(fraction)
}

fn parse_environment(value: &str) -> Result<RithmicEnv, FeedError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "demo" => Ok(RithmicEnv::Demo),
        "live" => Ok(RithmicEnv::Live),
        "test" => Ok(RithmicEnv::Test),
        _ => Err(FeedError(
            "RITHMIC_ENV must be demo, live, or test".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_metadata_preserves_expiry_and_does_not_invent_currency_or_value() {
        let mut info = InstrumentInfo::default();
        info.symbol = "ESU6".into();
        info.exchange = "CME".into();
        info.tick_size = Some(0.25);
        info.expiration_date = Some("2026-09-18".into());
        info.exchange_symbol = Some("EXCHANGE-ESU6".into());
        let item = instrument_from_rithmic(info).unwrap();
        assert_eq!(
            item.expiration_date,
            market_data::date_to_unix("20260918").unwrap()
        );
        assert_eq!(item.exchange_symbol, "EXCHANGE-ESU6");
        assert!(item.currency.is_empty());
        assert_eq!(item.contract_size, 0.0);
        assert_eq!(item.currency_value_per_increment, 0.0);
    }

    #[test]
    fn symbol_subscription_cannot_change_ids_until_unsubscribed() {
        let mut subscriptions = HashMap::from([(7, ("ESU6".to_owned(), "CME".to_owned()))]);
        assert!(validate_subscription(&subscriptions, 8, "ESU6", "CME").is_err());
        assert!(validate_subscription(&subscriptions, 7, "NQU6", "CME").is_err());
        assert!(validate_subscription(&subscriptions, 8, "NQU6", "CME").is_ok());
        subscriptions.remove(&7);
        assert!(validate_subscription(&subscriptions, 8, "ESU6", "CME").is_ok());
    }

    #[test]
    fn converts_rithmic_epoch_parts_to_dtc_microseconds() {
        assert_eq!(timestamp_us(Some(10), Some(123_456), None), 10_123_456);
        assert_eq!(timestamp_us(Some(10), None, Some(123_456_789)), 10_123_456);
    }

    #[test]
    fn generated_mac_is_not_a_hardware_mac() {
        let mac = synthetic_mac();
        let first = u8::from_str_radix(&mac[..2], 16).unwrap();
        assert_eq!(first & 0x01, 0);
        assert_eq!(first & 0x02, 0x02);
    }

    #[test]
    fn micro_futures_are_explicit_priority_catalog_products() {
        assert!(PRIORITY_CATALOG_PRODUCTS.contains(&("MES", "CME")));
        assert!(PRIORITY_CATALOG_PRODUCTS.contains(&("MNQ", "CME")));
        assert!(catalog_product_priority("MES", "ES") < catalog_product_priority("6A", "ES"));
    }

    #[test]
    fn recognizes_rithmic_connection_health_signals() {
        assert!(is_connection_loss(&RithmicMessage::ConnectionError, None));
        assert!(is_connection_loss(&RithmicMessage::HeartbeatTimeout, None));
        assert!(is_connection_loss(
            &RithmicMessage::HeartbeatTimeout,
            Some(&RithmicError::HeartbeatTimeout)
        ));
    }
}
