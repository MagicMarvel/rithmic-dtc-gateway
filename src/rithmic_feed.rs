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
    dtc::{Instrument as DtcInstrument, MarketCommand, MarketDataClient, MarketEvent},
    identity::synthetic_mac,
    order_book::{BookError, DepthLevel, OrderBook, OrderUpdate, Side, UpdateAction, diff_levels},
};

const MAX_CATALOG_SEARCH_RESULTS: usize = 32;
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
    catalog_cache: Arc<Mutex<Option<Vec<DtcInstrument>>>>,
}

impl RithmicFeed {
    pub async fn connect_from_env() -> Result<Self, FeedError> {
        let environment =
            parse_environment(&env::var("RITHMIC_ENV").unwrap_or_else(|_| "demo".to_owned()))?;
        let config = RithmicConfig::from_env(environment)
            .map_err(|error| FeedError(format!("Rithmic configuration failed: {error}")))?;
        let plant = connect_and_login(&config, ConnectStrategy::Simple)
            .await
            .map_err(FeedError)?;
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
    catalog_cache: Arc<Mutex<Option<Vec<DtcInstrument>>>>,
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
    catalog_cache: &Arc<Mutex<Option<Vec<DtcInstrument>>>>,
    commands: &mut mpsc::Receiver<MarketCommand>,
    events: &mpsc::Sender<MarketEvent>,
    subscriptions: &mut HashMap<u32, (String, String)>,
    depth: &mut HashMap<u32, DepthSubscription>,
) -> SessionExit {
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { return SessionExit::ClientClosed };
                match command {
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
                        response,
                    } => {
                        let _ = response.send(
                            search_catalog(handle, &search_text, &exchange).await,
                        );
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
                        let result = if subscriptions.contains_key(&symbol_id) {
                            Err(format!("SymbolID {symbol_id} is already subscribed"))
                        } else {
                            subscribe(&handle, &symbol, &exchange).await.map(|()| {
                                subscriptions.insert(symbol_id, (symbol, exchange));
                            })
                        };
                        let _ = response.send(result);
                    }
                    MarketCommand::Unsubscribe { symbol_id, response } => {
                        let result = match subscriptions.remove(&symbol_id) {
                            Some((symbol, exchange)) => unsubscribe(&handle, &symbol, &exchange).await,
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
) -> Result<Vec<DtcInstrument>, String> {
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
    search_text: &str,
    exchange: &str,
) -> Result<Vec<DtcInstrument>, String> {
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
    keys.truncate(MAX_CATALOG_SEARCH_RESULTS);

    let mut instruments = Vec::with_capacity(keys.len());
    for (symbol, exchange) in keys {
        if let Ok(instrument) = resolve_catalog_instrument(handle, &symbol, &exchange).await {
            instruments.push(instrument);
        }
    }
    Ok(instruments)
}

async fn resolve_catalog_instrument(
    handle: &RithmicTickerPlantHandle,
    symbol: &str,
    exchange: &str,
) -> Result<DtcInstrument, String> {
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
    dtc_instrument_from_rithmic(info)
}

fn dtc_instrument_from_rithmic(info: InstrumentInfo) -> Result<DtcInstrument, String> {
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
        .unwrap_or(1.0);
    let price_display_format = i32::from(info.price_precision());
    let underlying_symbol = info
        .product_code
        .clone()
        .or_else(|| info.underlying.clone())
        .unwrap_or_else(|| info.symbol.clone());
    Ok(DtcInstrument {
        symbol: info.symbol,
        exchange: info.exchange,
        underlying_symbol,
        description: info.name.unwrap_or_else(|| "Futures contract".to_owned()),
        min_price_increment: tick_size as f32,
        price_display_format,
        currency_value_per_increment: (tick_size * point_value) as f32,
        contract_size: point_value as f32,
        currency: info.currency.unwrap_or_else(|| "USD".to_owned()),
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
    let response = handle
        .subscribe(symbol, exchange)
        .await
        .map_err(|error| error.to_string())?;
    response
        .error
        .map_or(Ok(()), |error| Err(error.to_string()))
}

async fn unsubscribe(
    handle: &RithmicTickerPlantHandle,
    symbol: &str,
    exchange: &str,
) -> Result<(), String> {
    let response = handle
        .unsubscribe(symbol, exchange)
        .await
        .map_err(|error| error.to_string())?;
    response
        .error
        .map_or(Ok(()), |error| Err(error.to_string()))
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
