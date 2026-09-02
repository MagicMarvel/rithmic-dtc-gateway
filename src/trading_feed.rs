use std::{
    collections::{HashMap, HashSet},
    env,
    error::Error,
    fmt,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rithmic_rs::{
    ConnectStrategy, LoginConfig, ManualOrAutoEntry, OrderSide, OrderStatus, OrderType,
    RithmicAccount, RithmicCancelOrder, RithmicConfig, RithmicEnv, RithmicModifyOrder,
    RithmicOrder, RithmicOrderPlant, RithmicPnlPlant, TimeInForce,
    rti::{ExchangeOrderNotification, RithmicOrderNotification, messages::RithmicMessage},
};
use tokio::sync::{RwLock, broadcast, mpsc};

use crate::{
    dtc::{
        AccountBalance, CancelOrderRequest, ModifyOrderRequest, NewOrderRequest, TradeAccount,
        TradingCommand, TradingDataClient, TradingEvent, TradingOrder, TradingPosition,
    },
    identity::synthetic_mac,
};

#[derive(Debug)]
pub struct TradingError(String);

impl fmt::Display for TradingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for TradingError {}

pub struct RithmicTradingFeed {
    commands: mpsc::Sender<TradingCommand>,
    events: broadcast::Sender<TradingEvent>,
    order_ready: Arc<AtomicBool>,
    pnl_ready: Arc<AtomicBool>,
}

impl RithmicTradingFeed {
    pub async fn connect_from_env() -> Result<Self, TradingError> {
        require_paper_trading_enabled()?;
        let config = RithmicConfig::from_env(RithmicEnv::Demo).map_err(|error| {
            TradingError(format!("Rithmic trading configuration failed: {error}"))
        })?;
        let account = RithmicAccount::from_env(RithmicEnv::Demo).map_err(|error| {
            TradingError(format!(
                "Rithmic Paper account configuration failed: {error}"
            ))
        })?;
        let order_session = establish_order_session(&config, &account).await?;
        let pnl_session = match establish_pnl_session(&config, &account).await {
            Ok(session) => session,
            Err(error) => {
                let _ = order_session.handle.disconnect().await;
                let _ = order_session.plant.await_shutdown().await;
                return Err(error);
            }
        };
        let OrderSession {
            plant,
            handle,
            updates,
        } = order_session;
        let PnlSession {
            plant: pnl_plant,
            handle: pnl_handle,
            updates: pnl_updates,
        } = pnl_session;

        let max_quantity = env::var("RITHMIC_MAX_ORDER_QUANTITY")
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1);
        let allow_market_orders = env_flag("RITHMIC_ALLOW_MARKET_ORDERS");
        let safety_cancel_secs = env::var("RITHMIC_ORDER_SAFETY_CANCEL_SECS")
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|value| *value > 0);
        let (commands, mut command_rx) = mpsc::channel(64);
        let (events, _) = broadcast::channel(4096);
        let order_ready = Arc::new(AtomicBool::new(true));
        let pnl_ready = Arc::new(AtomicBool::new(true));
        let event_tx = events.clone();
        let positions = Arc::new(RwLock::new(HashMap::<String, TradingPosition>::new()));
        let balance = Arc::new(RwLock::new(None::<AccountBalance>));
        let positions_for_orders = Arc::clone(&positions);
        let balance_for_orders = Arc::clone(&balance);
        let account_for_task = account.clone();
        let order_ready_for_task = Arc::clone(&order_ready);
        let config = Arc::new(config);
        let order_config = Arc::clone(&config);
        tokio::spawn(async move {
            let mut orders = HashMap::<String, TradingOrder>::new();
            let mut used_client_order_ids = HashSet::<String>::new();
            let mut session = Some(OrderSession {
                plant,
                handle,
                updates,
            });
            let mut backoff = Duration::from_millis(500);
            loop {
                let OrderSession {
                    plant,
                    handle,
                    mut updates,
                } = session.take().expect("order session must be connected");
                order_ready_for_task.store(true, Ordering::Release);
                let disconnect_reason = loop {
                    tokio::select! {
                        command = command_rx.recv() => {
                            let Some(command) = command else {
                                let _ = handle.disconnect().await;
                                let _ = plant.await_shutdown().await;
                                order_ready_for_task.store(false, Ordering::Release);
                                return;
                            };
                            handle_command(
                                command,
                                &handle,
                                &account_for_task,
                                &mut orders,
                                &mut used_client_order_ids,
                                &positions_for_orders,
                                &balance_for_orders,
                                max_quantity,
                                allow_market_orders,
                                safety_cancel_secs,
                            ).await;
                        }
                        update = updates.recv() => {
                            match update {
                                Ok(response) => {
                                    if let Some(reason) = apply_order_response(
                                        response,
                                        &mut orders,
                                        &mut used_client_order_ids,
                                        &event_tx,
                                    ) {
                                        break reason;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(count)) => {
                                    break format!("Rithmic Order Plant stream lagged by {count} messages");
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    break "Rithmic Order Plant stream closed".to_owned();
                                }
                            }
                        }
                    }
                };
                order_ready_for_task.store(false, Ordering::Release);
                let _ = event_tx.send(TradingEvent::Error(format!(
                    "{disconnect_reason}; rejecting order requests while reconnecting"
                )));
                handle.abort();
                let _ = plant.await_shutdown().await;
                orders.clear();
                while let Ok(command) = command_rx.try_recv() {
                    reject_command(command, "Rithmic Order Plant is reconnecting");
                }
                loop {
                    match establish_order_session(&order_config, &account_for_task).await {
                        Ok(connected) => {
                            session = Some(connected);
                            backoff = Duration::from_millis(500);
                            eprintln!("[Trading] Rithmic Order Plant reconnected");
                            break;
                        }
                        Err(error) => {
                            let _ = event_tx.send(TradingEvent::Error(format!(
                                "Rithmic Order Plant reconnect failed: {error}"
                            )));
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(60));
                        }
                    }
                }
            }
        });
        let pnl_event_tx = events.clone();
        let pnl_account = account.clone();
        let pnl_ready_for_task = Arc::clone(&pnl_ready);
        let pnl_config = Arc::clone(&config);
        tokio::spawn(async move {
            let mut session = Some(PnlSession {
                plant: pnl_plant,
                handle: pnl_handle,
                updates: pnl_updates,
            });
            let mut backoff = Duration::from_millis(500);
            loop {
                let PnlSession {
                    plant,
                    handle,
                    mut updates,
                } = session.take().expect("PnL session must be connected");
                pnl_ready_for_task.store(true, Ordering::Release);
                let disconnect_reason = loop {
                    match updates.recv().await {
                        Ok(response) => {
                            if let Some(reason) = apply_pnl_response(
                                response,
                                &pnl_account,
                                &positions,
                                &balance,
                                &pnl_event_tx,
                            )
                            .await
                            {
                                break reason;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(count)) => {
                            break format!("Rithmic PnL Plant stream lagged by {count} messages");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break "Rithmic PnL Plant stream closed".to_owned();
                        }
                    }
                };
                pnl_ready_for_task.store(false, Ordering::Release);
                let _ = pnl_event_tx.send(TradingEvent::Error(format!(
                    "{disconnect_reason}; PnL snapshots unavailable while reconnecting"
                )));
                handle.abort();
                let _ = plant.await_shutdown().await;
                positions.write().await.clear();
                *balance.write().await = None;
                loop {
                    match establish_pnl_session(&pnl_config, &pnl_account).await {
                        Ok(connected) => {
                            session = Some(connected);
                            backoff = Duration::from_millis(500);
                            eprintln!("[Trading] Rithmic PnL Plant reconnected");
                            break;
                        }
                        Err(error) => {
                            let _ = pnl_event_tx.send(TradingEvent::Error(format!(
                                "Rithmic PnL Plant reconnect failed: {error}"
                            )));
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(60));
                        }
                    }
                }
            }
        });
        Ok(Self {
            commands,
            events,
            order_ready,
            pnl_ready,
        })
    }

    pub fn client(&self) -> TradingDataClient {
        let upstream_commands = self.commands.clone();
        let order_ready = Arc::clone(&self.order_ready);
        let pnl_ready = Arc::clone(&self.pnl_ready);
        let (commands, mut command_rx) = mpsc::channel(32);
        tokio::spawn(async move {
            while let Some(command) = command_rx.recv().await {
                let ready = match &command {
                    TradingCommand::Positions(_) | TradingCommand::Balance(_) => {
                        pnl_ready.load(Ordering::Acquire)
                    }
                    _ => order_ready.load(Ordering::Acquire),
                };
                if !ready {
                    reject_command(command, "Rithmic trading service is reconnecting");
                    continue;
                }
                if let Err(error) = upstream_commands.send(command).await {
                    reject_command(error.0, "Rithmic trading worker stopped");
                    break;
                }
            }
        });
        let mut subscription = self.events.subscribe();
        let (events, event_rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            loop {
                match subscription.recv().await {
                    Ok(event) => {
                        if events.send(event).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        if events
                            .send(TradingEvent::Error(format!(
                                "missed {count} trading events"
                            )))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        TradingDataClient::new(commands, event_rx)
    }
}

async fn connect_order_plant(config: &RithmicConfig) -> Result<RithmicOrderPlant, TradingError> {
    let mut delay = Duration::from_millis(500);
    let mut last_error = None;
    for _ in 0..5 {
        match RithmicOrderPlant::connect(config, ConnectStrategy::Simple).await {
            Ok(plant) => return Ok(plant),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(4));
    }
    Err(TradingError(format!(
        "Rithmic Order Plant connection failed after 5 attempts: {}",
        last_error.expect("at least one connection attempt")
    )))
}

async fn connect_pnl_plant(config: &RithmicConfig) -> Result<RithmicPnlPlant, TradingError> {
    let mut delay = Duration::from_millis(500);
    let mut last_error = None;
    for _ in 0..5 {
        match RithmicPnlPlant::connect(config, ConnectStrategy::Simple).await {
            Ok(plant) => return Ok(plant),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(4));
    }
    Err(TradingError(format!(
        "Rithmic PnL Plant connection failed after 5 attempts: {}",
        last_error.expect("at least one connection attempt")
    )))
}

struct OrderSession {
    plant: RithmicOrderPlant,
    handle: rithmic_rs::RithmicOrderPlantHandle,
    updates: rithmic_rs::SubscriptionFilter,
}

struct PnlSession {
    plant: RithmicPnlPlant,
    handle: rithmic_rs::RithmicPnlPlantHandle,
    updates: rithmic_rs::SubscriptionFilter,
}

async fn establish_order_session(
    config: &RithmicConfig,
    account: &RithmicAccount,
) -> Result<OrderSession, TradingError> {
    let plant = connect_order_plant(config).await?;
    let handle = plant.get_handle(account);
    let setup =
        async {
            let mut login = LoginConfig::default();
            login.mac_addr = Some(vec![synthetic_mac()]);
            handle.login_with_config(login).await.map_err(|error| {
                TradingError(format!("Rithmic Order Plant login failed: {error}"))
            })?;
            let discovered = handle.get_account_list().await.map_err(|error| {
                TradingError(format!("Rithmic account discovery failed: {error}"))
            })?;
            let account_found = discovered.iter().any(|response| {
                response.error.is_none()
                    && matches!(
                        &response.message,
                        RithmicMessage::ResponseAccountList(found)
                            if found.account_id.as_deref() == Some(&account.account_id)
                                && found.fcm_id.as_deref() == Some(&account.fcm_id)
                                && found.ib_id.as_deref() == Some(&account.ib_id)
                    )
            });
            if !account_found {
                return Err(TradingError(format!(
                    "configured Paper account {} / {} / {} is not available to this login",
                    account.account_id, account.fcm_id, account.ib_id
                )));
            }
            handle
                .trade_route_for("CME")
                .await
                .map_err(|error| TradingError(format!("CME trade route unavailable: {error}")))?;
            let updates = handle.subscription_receiver.resubscribe();
            checked_response(
                handle.subscribe_order_updates().await,
                "order update subscription",
            )?;
            checked_response(handle.show_orders().await, "open-order snapshot")?;
            Ok(updates)
        }
        .await;
    match setup {
        Ok(updates) => Ok(OrderSession {
            plant,
            handle,
            updates,
        }),
        Err(error) => {
            let _ = handle.disconnect().await;
            let _ = plant.await_shutdown().await;
            Err(error)
        }
    }
}

async fn establish_pnl_session(
    config: &RithmicConfig,
    account: &RithmicAccount,
) -> Result<PnlSession, TradingError> {
    let plant = connect_pnl_plant(config).await?;
    let handle = plant.get_handle(account);
    let setup = async {
        let mut login = LoginConfig::default();
        login.mac_addr = Some(vec![synthetic_mac()]);
        handle
            .login_with_config(login)
            .await
            .map_err(|error| TradingError(format!("Rithmic PnL Plant login failed: {error}")))?;
        let updates = handle.subscription_receiver.resubscribe();
        checked_response(
            handle.subscribe_pnl_updates().await,
            "PnL update subscription",
        )?;
        checked_response(
            handle.get_pnl_position_snapshot().await,
            "PnL position snapshot",
        )?;
        Ok(updates)
    }
    .await;
    match setup {
        Ok(updates) => Ok(PnlSession {
            plant,
            handle,
            updates,
        }),
        Err(error) => {
            let _ = handle.disconnect().await;
            let _ = plant.await_shutdown().await;
            Err(error)
        }
    }
}

fn trading_connection_loss(response: &rithmic_rs::RithmicResponse) -> Option<String> {
    if is_trading_connection_loss(&response.message, response.error.as_ref()) {
        Some(format!("Rithmic connection lost ({:?})", response.message))
    } else {
        None
    }
}

fn is_trading_connection_loss(
    message: &RithmicMessage,
    error: Option<&rithmic_rs::RithmicError>,
) -> bool {
    let message_is_loss = matches!(
        message,
        RithmicMessage::HeartbeatTimeout
            | RithmicMessage::ForcedLogout(_)
            | RithmicMessage::ConnectionError
    );
    let request_heartbeat_rejection = matches!(
        (message, error),
        (
            RithmicMessage::HeartbeatTimeout,
            Some(rithmic_rs::RithmicError::RequestRejected(_))
        )
    );
    (message_is_loss && !request_heartbeat_rejection)
        || error.is_some_and(rithmic_rs::RithmicError::is_connection_issue)
}

fn apply_order_response(
    response: rithmic_rs::RithmicResponse,
    orders: &mut HashMap<String, TradingOrder>,
    used_client_order_ids: &mut HashSet<String>,
    events: &broadcast::Sender<TradingEvent>,
) -> Option<String> {
    if let Some(reason) = trading_connection_loss(&response) {
        return Some(reason);
    }
    if let Some(error) = response.error {
        let _ = events.send(TradingEvent::Error(error.to_string()));
        return None;
    }
    let order = match response.message {
        RithmicMessage::RithmicOrderNotification(notification) => {
            eprintln!(
                "[Trading] Rithmic order basket={:?} notify={:?} status={:?} price={:?} filled={:?} remaining={:?} completion={:?}",
                notification.basket_id,
                notification.notify_type,
                notification.status,
                notification.price,
                notification.total_fill_size,
                notification.total_unfilled_size,
                notification.completion_reason,
            );
            map_rithmic_order(notification)
        }
        RithmicMessage::ExchangeOrderNotification(notification) => {
            eprintln!(
                "[Trading] Exchange order basket={:?} notify={:?} status={:?} price={:?} fill={:?} total_fill={:?} remaining={:?}",
                notification.basket_id,
                notification.notify_type,
                notification.status,
                notification.price,
                notification.fill_size,
                notification.total_fill_size,
                notification.total_unfilled_size,
            );
            map_exchange_order(notification)
        }
        _ => None,
    };
    if let Some(order) = order {
        apply_mapped_order(order, orders, used_client_order_ids, events);
    }
    None
}

fn apply_mapped_order(
    mut order: TradingOrder,
    orders: &mut HashMap<String, TradingOrder>,
    used_client_order_ids: &mut HashSet<String>,
    events: &broadcast::Sender<TradingEvent>,
) {
    if !order.client_order_id.is_empty() {
        used_client_order_ids.insert(order.client_order_id.clone());
    }
    if !order.server_order_id.is_empty() {
        if let Some(existing) = orders.get(&order.server_order_id) {
            merge_order(existing, &mut order);
        }
        if is_terminal(order.order_status) {
            orders.remove(&order.server_order_id);
        } else {
            orders.insert(order.server_order_id.clone(), order.clone());
        }
    }
    if order.is_snapshot {
        if is_terminal(order.order_status) {
            return;
        }
        order.update_reason = 3;
    }
    let _ = events.send(TradingEvent::Order(order));
}

async fn apply_pnl_response(
    response: rithmic_rs::RithmicResponse,
    account: &RithmicAccount,
    positions: &Arc<RwLock<HashMap<String, TradingPosition>>>,
    balance: &Arc<RwLock<Option<AccountBalance>>>,
    events: &broadcast::Sender<TradingEvent>,
) -> Option<String> {
    if let Some(reason) = trading_connection_loss(&response) {
        return Some(reason);
    }
    if let Some(error) = response.error {
        let _ = events.send(TradingEvent::Error(error.to_string()));
        return None;
    }
    match response.message {
        RithmicMessage::InstrumentPnLPositionUpdate(update) => {
            if let Some(position) = map_position(update) {
                let key = format!("{}.{}", position.symbol, position.exchange);
                if position.quantity == 0.0 {
                    positions.write().await.remove(&key);
                } else {
                    positions.write().await.insert(key, position.clone());
                }
                let _ = events.send(TradingEvent::Position(position));
            }
        }
        RithmicMessage::AccountPnLPositionUpdate(update) => {
            let value = map_balance(update, account);
            *balance.write().await = Some(value.clone());
            let _ = events.send(TradingEvent::Balance(value));
        }
        _ => {}
    }
    None
}

fn require_paper_trading_enabled() -> Result<(), TradingError> {
    let environment = env::var("RITHMIC_ENV").unwrap_or_else(|_| "demo".to_owned());
    if !environment.eq_ignore_ascii_case("demo") {
        return Err(TradingError(
            "trading is hard-locked to RITHMIC_ENV=demo".to_owned(),
        ));
    }
    if !env_flag("RITHMIC_ENABLE_TRADING") {
        return Err(TradingError(
            "trading is disabled; set RITHMIC_ENABLE_TRADING=true only for Paper Trading"
                .to_owned(),
        ));
    }
    Ok(())
}

fn env_flag(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    })
}

fn checked_response<T>(
    response: Result<T, rithmic_rs::RithmicError>,
    action: &str,
) -> Result<T, TradingError>
where
    T: ResponseError,
{
    let response =
        response.map_err(|error| TradingError(format!("Rithmic {action} failed: {error}")))?;
    if let Some(error) = response.response_error() {
        return Err(TradingError(format!("Rithmic {action} rejected: {error}")));
    }
    Ok(response)
}

trait ResponseError {
    fn response_error(&self) -> Option<&rithmic_rs::RithmicError>;
}

impl ResponseError for rithmic_rs::RithmicResponse {
    fn response_error(&self) -> Option<&rithmic_rs::RithmicError> {
        self.error.as_ref()
    }
}

fn reject_command(command: TradingCommand, reason: &str) {
    let reason = reason.to_owned();
    match command {
        TradingCommand::Accounts(response) => {
            let _ = response.send(Err(reason));
        }
        TradingCommand::OpenOrders(response) => {
            let _ = response.send(Err(reason));
        }
        TradingCommand::Positions(response) => {
            let _ = response.send(Err(reason));
        }
        TradingCommand::Balance(response) => {
            let _ = response.send(Err(reason));
        }
        TradingCommand::Submit(_, response)
        | TradingCommand::Modify(_, response)
        | TradingCommand::Cancel(_, response) => {
            let _ = response.send(Err(reason));
        }
    }
}

async fn handle_command(
    command: TradingCommand,
    handle: &rithmic_rs::RithmicOrderPlantHandle,
    account: &RithmicAccount,
    orders: &mut HashMap<String, TradingOrder>,
    used_client_order_ids: &mut HashSet<String>,
    positions: &Arc<RwLock<HashMap<String, TradingPosition>>>,
    balance: &Arc<RwLock<Option<AccountBalance>>>,
    max_quantity: i32,
    allow_market_orders: bool,
    safety_cancel_secs: Option<i32>,
) {
    match command {
        TradingCommand::Accounts(response) => {
            let _ = response.send(Ok(vec![TradeAccount {
                account_id: account.account_id.clone(),
                currency: "USD".to_owned(),
                trading_disabled: false,
            }]));
        }
        TradingCommand::OpenOrders(response) => {
            let _ = response.send(Ok(orders.values().cloned().collect()));
        }
        TradingCommand::Positions(response) => {
            let _ = response.send(Ok(positions.read().await.values().cloned().collect()));
        }
        TradingCommand::Balance(response) => {
            let result = balance
                .read()
                .await
                .clone()
                .ok_or_else(|| "Rithmic has not published an account balance yet".to_owned());
            let _ = response.send(result);
        }
        TradingCommand::Submit(request, response) => {
            let result = submit_order(
                handle,
                account,
                request,
                max_quantity,
                allow_market_orders,
                safety_cancel_secs,
                used_client_order_ids,
            )
            .await;
            let _ = response.send(result);
        }
        TradingCommand::Modify(request, response) => {
            let result = modify_order(handle, account, orders, request, max_quantity).await;
            let _ = response.send(result);
        }
        TradingCommand::Cancel(request, response) => {
            let result = cancel_order(handle, account, orders, request).await;
            let _ = response.send(result);
        }
    }
}

async fn submit_order(
    handle: &rithmic_rs::RithmicOrderPlantHandle,
    account: &RithmicAccount,
    request: NewOrderRequest,
    max_quantity: i32,
    allow_market_orders: bool,
    safety_cancel_secs: Option<i32>,
    used_client_order_ids: &mut HashSet<String>,
) -> Result<(), String> {
    validate_account(account, &request.account_id)?;
    if request.client_order_id.trim().is_empty() {
        return Err("ClientOrderID is required".to_owned());
    }
    if used_client_order_ids.contains(&request.client_order_id) {
        return Err("ClientOrderID was already used in this trading session".to_owned());
    }
    let client_order_id = request.client_order_id.clone();
    let mut order = build_new_order(request, max_quantity, allow_market_orders)?;
    if let Some(seconds) = safety_cancel_secs {
        order = order.cancel_after_secs(seconds);
    }
    let order = order.build().map_err(|error| error.to_string())?;
    let responses = handle
        .place_order(order)
        .await
        .map_err(|error| error.to_string())?;
    check_responses(&responses)?;
    used_client_order_ids.insert(client_order_id);
    Ok(())
}

fn build_new_order(
    request: NewOrderRequest,
    max_quantity: i32,
    allow_market_orders: bool,
) -> Result<RithmicOrder, String> {
    let quantity = validate_quantity(request.quantity, max_quantity)?;
    let order_type = dtc_order_type(request.order_type)?;
    if order_type == OrderType::Market && !allow_market_orders {
        return Err(
            "market orders are disabled; set RITHMIC_ALLOW_MARKET_ORDERS=true explicitly"
                .to_owned(),
        );
    }
    let mut order = RithmicOrder::new()
        .symbol(request.symbol)
        .exchange(request.exchange)
        .quantity(quantity)
        .transaction_type(dtc_side(request.buy_sell)?)
        .price_type(order_type)
        .duration(dtc_tif(request.time_in_force)?)
        .manual_or_auto(if request.is_automated {
            ManualOrAutoEntry::Auto
        } else {
            ManualOrAutoEntry::Manual
        })
        .user_tag(request.client_order_id)
        .window_name("Sierra Chart DTC");
    match order_type {
        OrderType::Market => {}
        OrderType::Limit => order = order.price(validate_es_price(request.price1, "Price1")?),
        OrderType::StopMarket => {
            order = order.trigger_price(validate_es_price(request.price1, "Price1")?)
        }
        OrderType::StopLimit => {
            order = order
                .trigger_price(validate_es_price(request.price1, "Price1")?)
                .price(validate_es_price(request.price2, "Price2")?);
        }
        _ => return Err("unsupported DTC order type".to_owned()),
    }
    order.build().map_err(|error| error.to_string())
}

async fn modify_order(
    handle: &rithmic_rs::RithmicOrderPlantHandle,
    account: &RithmicAccount,
    orders: &mut HashMap<String, TradingOrder>,
    request: ModifyOrderRequest,
    max_quantity: i32,
) -> Result<(), String> {
    validate_account(account, &request.account_id)?;
    let existing = orders
        .get(&request.server_order_id)
        .ok_or_else(|| "unknown or non-working ServerOrderID".to_owned())?;
    if request.client_order_id != existing.client_order_id {
        return Err("ClientOrderID does not match the working order".to_owned());
    }
    let requested_quantity = if request.quantity == 0.0 {
        existing.quantity
    } else {
        request.quantity
    };
    let quantity = validate_quantity(requested_quantity, max_quantity)?;
    if f64::from(quantity) < existing.filled_quantity {
        return Err("new quantity cannot be less than the already-filled quantity".to_owned());
    }
    if request.time_in_force != 0 && request.time_in_force != existing.time_in_force {
        return Err("changing TimeInForce is not supported".to_owned());
    }
    let order_type = dtc_order_type(existing.order_type)?;
    let server_order_id = request.server_order_id.clone();
    let requested_price1 = request.price1;
    let requested_price2 = request.price2;
    let mut modification = RithmicModifyOrder::new()
        .id(&server_order_id)
        .symbol(existing.symbol.clone())
        .exchange(existing.exchange.clone())
        .quantity(quantity)
        .price_type(order_type)
        .manual_or_auto(ManualOrAutoEntry::Manual)
        .window_name("Sierra Chart DTC");
    match order_type {
        OrderType::Market => {
            if request.price1.is_some() || request.price2.is_some() {
                return Err("market orders do not have modifiable prices".to_owned());
            }
        }
        OrderType::Limit => {
            let price = request.price1.unwrap_or(existing.price1);
            modification = modification.price(validate_es_price(price, "Price1")?);
        }
        OrderType::StopMarket => {
            let trigger = request.price1.unwrap_or(existing.price1);
            modification = modification.trigger_price(validate_es_price(trigger, "Price1")?);
        }
        OrderType::StopLimit => {
            let trigger = request.price1.unwrap_or(existing.price1);
            let limit = request.price2.unwrap_or(existing.price2);
            modification = modification
                .trigger_price(validate_es_price(trigger, "Price1")?)
                .price(validate_es_price(limit, "Price2")?);
        }
        _ => return Err("unsupported DTC order type".to_owned()),
    }
    let responses = handle
        .modify_order(modification.build().map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    check_responses(&responses)?;
    if let Some(order) = orders.get_mut(&server_order_id) {
        order.quantity = f64::from(quantity);
        order.remaining_quantity = (order.quantity - order.filled_quantity).max(0.0);
        if let Some(price) = requested_price1 {
            order.price1 = price;
        }
        if let Some(price) = requested_price2 {
            order.price2 = price;
        }
        if request.time_in_force != 0 {
            order.time_in_force = request.time_in_force;
        }
        eprintln!(
            "[Trading] Updated local order {} after modify: price1={} quantity={}",
            server_order_id, order.price1, order.quantity
        );
    } else {
        eprintln!(
            "[Trading] Modify accepted but local order {} was absent from cache",
            server_order_id
        );
    }
    Ok(())
}

async fn cancel_order(
    handle: &rithmic_rs::RithmicOrderPlantHandle,
    account: &RithmicAccount,
    orders: &HashMap<String, TradingOrder>,
    request: CancelOrderRequest,
) -> Result<(), String> {
    validate_account(account, &request.account_id)?;
    if request.server_order_id.is_empty() {
        return Err("ServerOrderID is required for cancel".to_owned());
    }
    let existing = orders
        .get(&request.server_order_id)
        .ok_or_else(|| "unknown or non-working ServerOrderID".to_owned())?;
    if request.client_order_id != existing.client_order_id {
        return Err("ClientOrderID does not match the working order".to_owned());
    }
    let cancel = RithmicCancelOrder::new()
        .id(request.server_order_id)
        .manual_or_auto(ManualOrAutoEntry::Manual)
        .window_name("Sierra Chart DTC")
        .build()
        .map_err(|error| error.to_string())?;
    let responses = handle
        .cancel_order(cancel)
        .await
        .map_err(|error| error.to_string())?;
    check_responses(&responses)
}

fn check_responses(responses: &[rithmic_rs::RithmicResponse]) -> Result<(), String> {
    responses
        .iter()
        .find_map(|response| response.error.as_ref())
        .map_or(Ok(()), |error| Err(error.to_string()))
}

fn validate_account(account: &RithmicAccount, requested: &str) -> Result<(), String> {
    if requested == account.account_id {
        Ok(())
    } else {
        Err(format!("unknown Paper trade account {requested}"))
    }
}

fn validate_quantity(quantity: f64, max: i32) -> Result<i32, String> {
    if !quantity.is_finite()
        || quantity.fract() != 0.0
        || quantity < 1.0
        || quantity > f64::from(max)
    {
        Err(format!(
            "quantity must be a whole number from 1 through {max}"
        ))
    } else {
        Ok(quantity as i32)
    }
}

fn validate_es_price(price: f64, field: &str) -> Result<f64, String> {
    if !price.is_finite() || price <= 0.0 {
        return Err(format!("{field} must be a finite positive ES price"));
    }
    let ticks = price / 0.25;
    if (ticks - ticks.round()).abs() > 1e-8 {
        return Err(format!("{field} must be aligned to the ES 0.25 tick size"));
    }
    Ok(price)
}

fn dtc_side(value: i32) -> Result<OrderSide, String> {
    match value {
        1 => Ok(OrderSide::Buy),
        2 => Ok(OrderSide::Sell),
        _ => Err("unsupported BuySell value".to_owned()),
    }
}

fn dtc_order_type(value: i32) -> Result<OrderType, String> {
    match value {
        1 => Ok(OrderType::Market),
        2 => Ok(OrderType::Limit),
        3 => Ok(OrderType::StopMarket),
        4 => Ok(OrderType::StopLimit),
        5 | 6 => Err("DTC if-touched orders are not supported by this bridge".to_owned()),
        _ => Err("unsupported DTC OrderType".to_owned()),
    }
}

fn dtc_tif(value: i32) -> Result<TimeInForce, String> {
    match value {
        0 | 1 => Ok(TimeInForce::Day),
        2 => Ok(TimeInForce::Gtc),
        4 => Ok(TimeInForce::Ioc),
        6 => Ok(TimeInForce::Fok),
        _ => Err("unsupported TimeInForce".to_owned()),
    }
}

fn map_status(status: Option<&str>) -> i32 {
    match OrderStatus::from_str(status.unwrap_or("unknown")).unwrap() {
        OrderStatus::Open => 4,
        OrderStatus::Complete => 7,
        OrderStatus::Cancelled | OrderStatus::Expired => 8,
        OrderStatus::Pending => 2,
        OrderStatus::Rejected => 9,
        OrderStatus::Partial => 10,
        _ => 0,
    }
}

fn map_duration(duration: Option<i32>) -> i32 {
    match duration {
        Some(1) => 1,
        Some(2) => 2,
        Some(3) => 4,
        Some(4) => 6,
        _ => 0,
    }
}

fn is_terminal(status: i32) -> bool {
    matches!(status, 7..=9)
}

fn merge_order(existing: &TradingOrder, update: &mut TradingOrder) {
    if update.order_status == 0 {
        update.order_status = existing.order_status;
    }
    if update.symbol.is_empty() {
        update.symbol.clone_from(&existing.symbol);
    }
    if update.exchange.is_empty() {
        update.exchange.clone_from(&existing.exchange);
    }
    if update.account_id.is_empty() {
        update.account_id.clone_from(&existing.account_id);
    }
    if update.client_order_id.is_empty() {
        update.client_order_id.clone_from(&existing.client_order_id);
    }
    if update.exchange_order_id.is_empty() {
        update
            .exchange_order_id
            .clone_from(&existing.exchange_order_id);
    }
    if update.order_type == 0 {
        update.order_type = existing.order_type;
    }
    if update.buy_sell == 0 {
        update.buy_sell = existing.buy_sell;
    }
    if update.price1 == f64::MAX {
        update.price1 = existing.price1;
    }
    if update.price2 == f64::MAX {
        update.price2 = existing.price2;
    }
    if update.quantity == 0.0 {
        update.quantity = existing.quantity;
    }
    if update.filled_quantity == 0.0 {
        update.filled_quantity = existing.filled_quantity;
    }
    if update.remaining_quantity == 0.0 && update.filled_quantity < update.quantity {
        update.remaining_quantity = existing.remaining_quantity;
    }
    if update.average_fill_price == f64::MAX {
        update.average_fill_price = existing.average_fill_price;
    }
    if update.time_in_force == 0 {
        update.time_in_force = existing.time_in_force;
    }
}

fn map_rithmic_order(order: RithmicOrderNotification) -> Option<TradingOrder> {
    let server_order_id = order.basket_id.clone()?;
    let (order_status, update_reason) = rithmic_status_reason(&order);
    let order_type = notification_price_type_to_dtc(order.price_type);
    let (price1, price2) = notification_prices_to_dtc(order_type, order.price, order.trigger_price);
    Some(TradingOrder {
        request_id: 0,
        symbol: order.symbol.unwrap_or_default(),
        exchange: order.exchange.unwrap_or_default(),
        account_id: order.account_id.unwrap_or_default(),
        client_order_id: order.user_tag.unwrap_or_default(),
        server_order_id,
        exchange_order_id: order.exchange_order_id.unwrap_or_default(),
        order_status,
        update_reason,
        order_type,
        buy_sell: order.transaction_type.unwrap_or_default(),
        price1,
        price2,
        quantity: f64::from(order.quantity.unwrap_or_default()),
        filled_quantity: f64::from(order.total_fill_size.unwrap_or_default()),
        remaining_quantity: f64::from(order.total_unfilled_size.unwrap_or_default()),
        average_fill_price: order.avg_fill_price.unwrap_or(f64::MAX),
        last_fill_price: f64::MAX,
        last_fill_quantity: f64::MAX,
        last_fill_datetime_ms: 0,
        last_fill_execution_id: String::new(),
        info_text: order.report_text.or(order.text).unwrap_or_default(),
        time_in_force: map_duration(order.duration),
        is_snapshot: order.is_snapshot.unwrap_or(false),
    })
}

fn map_exchange_order(order: ExchangeOrderNotification) -> Option<TradingOrder> {
    let server_order_id = order.basket_id.clone()?;
    let filled = order
        .total_fill_size
        .or(order.fill_size)
        .unwrap_or_default();
    let remaining = order
        .total_unfilled_size
        .unwrap_or_else(|| order.quantity.unwrap_or_default().saturating_sub(filled));
    let (order_status, update_reason) = exchange_status_reason(&order, filled, remaining);
    let order_type = notification_price_type_to_dtc(order.price_type);
    let (price1, price2) = notification_prices_to_dtc(order_type, order.price, order.trigger_price);
    Some(TradingOrder {
        request_id: 0,
        symbol: order.symbol.unwrap_or_default(),
        exchange: order.exchange.unwrap_or_default(),
        account_id: order.account_id.unwrap_or_default(),
        client_order_id: order.user_tag.unwrap_or_default(),
        server_order_id,
        exchange_order_id: order.exchange_order_id.unwrap_or_default(),
        order_status,
        update_reason,
        order_type,
        buy_sell: order.transaction_type.unwrap_or_default(),
        price1,
        price2,
        quantity: f64::from(order.quantity.unwrap_or_default()),
        filled_quantity: f64::from(filled),
        remaining_quantity: f64::from(remaining),
        average_fill_price: order.avg_fill_price.unwrap_or(f64::MAX),
        last_fill_price: order.fill_price.unwrap_or(f64::MAX),
        last_fill_quantity: f64::from(order.fill_size.unwrap_or_default()),
        last_fill_datetime_ms: i64::from(order.ssboe.unwrap_or_default()) * 1000
            + i64::from(order.usecs.unwrap_or_default()) / 1000,
        last_fill_execution_id: order.fill_id.unwrap_or_default(),
        info_text: order.report_text.or(order.text).unwrap_or_default(),
        time_in_force: map_duration(order.duration),
        is_snapshot: order.is_snapshot.unwrap_or(false),
    })
}

fn rithmic_status_reason(order: &RithmicOrderNotification) -> (i32, i32) {
    match order.notify_type {
        Some(1 | 4 | 7 | 10) => (2, 3),
        Some(2 | 5 | 8 | 11) => (5, 3),
        Some(3 | 6 | 9 | 12) => (6, 3),
        Some(13) => (4, 2),
        Some(14) => (4, 7),
        // A stop accepted by Rithmic remains "trigger pending" until its trigger trades.
        // It is already a live working order at that point, not a submission still pending.
        Some(18) => (4, 2),
        Some(15) => {
            let quantity = order.quantity.unwrap_or_default();
            let filled = order.total_fill_size.unwrap_or_default();
            if quantity > 0 && filled >= quantity {
                // The exchange notification carries the actual fill execution and is the
                // only update that may use ORDER_FILLED. Rithmic's later completion report
                // repeats the cumulative quantity without a new execution; marking it as a
                // second fill makes Sierra report an overfill.
                (7, 3)
            } else {
                (8, 6)
            }
        }
        Some(16) => (9, 10),
        Some(17) => (9, 9),
        _ => (map_status(order.status.as_deref()), 3),
    }
}

fn exchange_status_reason(
    order: &ExchangeOrderNotification,
    filled: i32,
    remaining: i32,
) -> (i32, i32) {
    match order.notify_type {
        Some(2) => (4, 7),
        Some(3) => (8, 6),
        Some(5) if remaining > 0 => (10, 5),
        Some(5) => (7, 4),
        Some(6) => (9, 8),
        Some(7) => (9, 10),
        Some(8) => (9, 9),
        _ if filled > 0 && remaining == 0 => (7, 4),
        _ => (map_status(order.status.as_deref()), 3),
    }
}

// Rithmic order notifications use LIMIT=1, MARKET=2, STOP_LIMIT=3,
// STOP_MARKET=4. DTC uses MARKET=1, LIMIT=2, STOP_MARKET=3,
// STOP_LIMIT=4. Never cache the Rithmic integer as a DTC OrderType: doing so
// would turn a later cancel/replace of a limit order into a market order.
fn notification_price_type_to_dtc(value: Option<i32>) -> i32 {
    match value {
        Some(1) => 2,
        Some(2) => 1,
        Some(3) => 4,
        Some(4) => 3,
        _ => 0,
    }
}

fn notification_prices_to_dtc(
    dtc_order_type: i32,
    rithmic_price: Option<f64>,
    rithmic_trigger_price: Option<f64>,
) -> (f64, f64) {
    match dtc_order_type {
        3 => (rithmic_trigger_price.unwrap_or(f64::MAX), f64::MAX),
        4 => (
            rithmic_trigger_price.unwrap_or(f64::MAX),
            rithmic_price.unwrap_or(f64::MAX),
        ),
        _ => (rithmic_price.unwrap_or(f64::MAX), f64::MAX),
    }
}

fn map_position(update: rithmic_rs::rti::InstrumentPnLPositionUpdate) -> Option<TradingPosition> {
    let symbol = update.symbol?;
    let exchange = update.exchange?;
    Some(TradingPosition {
        symbol,
        exchange,
        account_id: update.account_id.unwrap_or_default(),
        quantity: f64::from(update.net_quantity.unwrap_or_default()),
        average_price: update.avg_open_fill_price.unwrap_or_default(),
        open_profit_loss: parse_number(update.open_position_pnl.as_deref()),
    })
}

fn map_balance(
    update: rithmic_rs::rti::AccountPnLPositionUpdate,
    account: &RithmicAccount,
) -> AccountBalance {
    AccountBalance {
        account_id: update
            .account_id
            .unwrap_or_else(|| account.account_id.clone()),
        currency: "USD".to_owned(),
        cash_balance: parse_number(
            update
                .cash_on_hand
                .as_deref()
                .or(update.account_balance.as_deref()),
        ),
        available_funds: first_number(&[
            update.available_buying_power.as_deref(),
            update.excess_buy_margin.as_deref(),
        ]),
        open_profit_loss: parse_number(update.open_position_pnl.as_deref()),
        daily_profit_loss: parse_number(update.day_pnl.as_deref()),
        trading_disabled: false,
    }
}

fn parse_number(value: Option<&str>) -> f64 {
    value
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn first_number(values: &[Option<&str>]) -> f64 {
    values
        .iter()
        .flatten()
        .find_map(|value| value.trim().parse().ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_request(order_type: i32, price1: f64, price2: f64) -> NewOrderRequest {
        NewOrderRequest {
            symbol: "ESU6".to_owned(),
            exchange: "CME".to_owned(),
            account_id: "paper-account".to_owned(),
            client_order_id: "client-1".to_owned(),
            order_type,
            buy_sell: 1,
            price1,
            price2,
            quantity: 1.0,
            time_in_force: 1,
            is_automated: false,
        }
    }

    #[test]
    fn safety_rejects_fractional_zero_and_oversized_quantity() {
        assert!(validate_quantity(0.0, 1).is_err());
        assert!(validate_quantity(0.5, 1).is_err());
        assert!(validate_quantity(2.0, 1).is_err());
        assert_eq!(validate_quantity(1.0, 1).unwrap(), 1);
    }

    #[test]
    fn rithmic_notification_price_types_are_converted_to_dtc() {
        assert_eq!(notification_price_type_to_dtc(Some(1)), 2); // Limit
        assert_eq!(notification_price_type_to_dtc(Some(2)), 1); // Market
        assert_eq!(notification_price_type_to_dtc(Some(3)), 4); // Stop limit
        assert_eq!(notification_price_type_to_dtc(Some(4)), 3); // Stop market
        assert_eq!(notification_price_type_to_dtc(None), 0);
    }

    #[test]
    fn cached_rithmic_limit_order_stays_a_dtc_limit_order() {
        let mut notification = RithmicOrderNotification::default();
        notification.basket_id = Some("basket-limit".to_owned());
        notification.price_type = Some(1); // Rithmic LIMIT
        notification.price = Some(7600.25);
        notification.quantity = Some(1);
        notification.total_unfilled_size = Some(1);
        let mapped = map_rithmic_order(notification).unwrap();
        assert_eq!(mapped.order_type, 2); // DTC LIMIT
        assert_eq!(dtc_order_type(mapped.order_type).unwrap(), OrderType::Limit);
    }

    #[test]
    fn cached_exchange_limit_order_stays_a_dtc_limit_order() {
        let mut notification = ExchangeOrderNotification::default();
        notification.basket_id = Some("exchange-limit".to_owned());
        notification.price_type = Some(1); // Rithmic LIMIT
        notification.price = Some(7600.25);
        notification.quantity = Some(1);
        notification.total_unfilled_size = Some(1);
        let mapped = map_exchange_order(notification).unwrap();
        assert_eq!(mapped.order_type, 2); // DTC LIMIT
        assert_eq!(dtc_order_type(mapped.order_type).unwrap(), OrderType::Limit);
    }

    #[test]
    fn rithmic_completion_does_not_replay_an_exchange_fill() {
        let mut completion = RithmicOrderNotification::default();
        completion.basket_id = Some("filled-order".to_owned());
        completion.notify_type = Some(15);
        completion.quantity = Some(1);
        completion.total_fill_size = Some(1);
        completion.total_unfilled_size = Some(0);
        let mapped = map_rithmic_order(completion).unwrap();
        assert_eq!(mapped.order_status, 7);
        assert_eq!(mapped.update_reason, 3, "completion is not a second fill");
        assert_eq!(mapped.last_fill_quantity, f64::MAX);

        let mut exchange_fill = ExchangeOrderNotification::default();
        exchange_fill.basket_id = Some("filled-order".to_owned());
        exchange_fill.notify_type = Some(5);
        exchange_fill.quantity = Some(1);
        exchange_fill.fill_size = Some(1);
        exchange_fill.total_fill_size = Some(1);
        exchange_fill.total_unfilled_size = Some(0);
        let mapped = map_exchange_order(exchange_fill).unwrap();
        assert_eq!(mapped.order_status, 7);
        assert_eq!(mapped.update_reason, 4);
        assert_eq!(mapped.last_fill_quantity, 1.0);
    }

    #[test]
    fn trigger_pending_stop_is_a_working_open_order_in_dtc() {
        let mut notification = RithmicOrderNotification::default();
        notification.basket_id = Some("working-stop".to_owned());
        notification.notify_type = Some(18);
        notification.status = Some("trigger pending".to_owned());
        notification.quantity = Some(1);
        notification.total_fill_size = Some(0);
        notification.total_unfilled_size = Some(1);
        let mapped = map_rithmic_order(notification).unwrap();
        assert_eq!(mapped.order_status, 4);
        assert_eq!(mapped.update_reason, 2);
        assert_eq!(mapped.remaining_quantity, 1.0);
    }

    #[test]
    fn available_funds_falls_back_when_buying_power_is_empty() {
        assert_eq!(first_number(&[Some(""), Some("100000.00")]), 100_000.0);
        assert_eq!(first_number(&[Some("250.50"), Some("100000")]), 250.5);
    }

    #[test]
    fn dtc_limit_and_stop_prices_map_to_rithmic_fields() {
        let limit = build_new_order(new_request(2, 7600.25, f64::MAX), 1, false).unwrap();
        assert_eq!(limit.price_type, OrderType::Limit);
        assert_eq!(limit.price, Some(7600.25));
        assert_eq!(limit.trigger_price, None);

        let stop = build_new_order(new_request(3, 7601.0, f64::MAX), 1, false).unwrap();
        assert_eq!(stop.price_type, OrderType::StopMarket);
        assert_eq!(stop.price, None);
        assert_eq!(stop.trigger_price, Some(7601.0));

        let stop_limit = build_new_order(new_request(4, 7601.0, 7600.75), 1, false).unwrap();
        assert_eq!(stop_limit.price_type, OrderType::StopLimit);
        assert_eq!(stop_limit.trigger_price, Some(7601.0));
        assert_eq!(stop_limit.price, Some(7600.75));
    }

    #[test]
    fn ambiguous_if_touched_and_off_tick_prices_are_rejected_locally() {
        assert!(build_new_order(new_request(5, 7600.25, f64::MAX), 1, false).is_err());
        assert!(build_new_order(new_request(6, 7600.25, f64::MAX), 1, false).is_err());
        assert!(build_new_order(new_request(2, 7600.10, f64::MAX), 1, false).is_err());
    }

    #[test]
    fn rithmic_stop_limit_prices_map_back_to_dtc_fields() {
        assert_eq!(
            notification_prices_to_dtc(4, Some(7600.75), Some(7601.0)),
            (7601.0, 7600.75)
        );
        assert_eq!(
            notification_prices_to_dtc(3, None, Some(7601.0)),
            (7601.0, f64::MAX)
        );
    }

    #[test]
    fn recognizes_order_and_pnl_connection_loss_signals() {
        assert!(is_trading_connection_loss(
            &RithmicMessage::ConnectionError,
            None
        ));
        assert!(is_trading_connection_loss(
            &RithmicMessage::HeartbeatTimeout,
            None
        ));
        assert!(is_trading_connection_loss(
            &RithmicMessage::RequestHeartbeat(Default::default()),
            Some(&rithmic_rs::RithmicError::ConnectionClosed)
        ));
        assert!(!is_trading_connection_loss(
            &RithmicMessage::RequestHeartbeat(Default::default()),
            Some(&rithmic_rs::RithmicError::InvalidArgument(
                "ordinary request error".to_owned()
            ))
        ));
    }

    #[test]
    fn terminal_order_snapshots_rebuild_state_without_replaying_fills() {
        let (events, _) = broadcast::channel(8);
        let mut receiver = events.subscribe();
        let mut orders = HashMap::new();
        let mut used_ids = HashSet::new();
        let order = TradingOrder {
            request_id: 0,
            symbol: "ESU6".to_owned(),
            exchange: "CME".to_owned(),
            account_id: "paper-account".to_owned(),
            server_order_id: "filled-basket".to_owned(),
            client_order_id: "old-client-id".to_owned(),
            exchange_order_id: String::new(),
            order_status: 7,
            update_reason: 0,
            order_type: 2,
            buy_sell: 1,
            price1: 7600.0,
            price2: f64::MAX,
            quantity: 1.0,
            filled_quantity: 1.0,
            remaining_quantity: 0.0,
            average_fill_price: 7600.0,
            last_fill_price: 7600.0,
            last_fill_quantity: 1.0,
            last_fill_datetime_ms: 0,
            last_fill_execution_id: "old-fill".to_owned(),
            info_text: String::new(),
            time_in_force: 1,
            is_snapshot: true,
        };
        apply_mapped_order(order, &mut orders, &mut used_ids, &events);
        assert!(orders.is_empty());
        assert!(used_ids.contains("old-client-id"));
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn unavailable_trading_proxy_rejects_instead_of_queueing() {
        let (upstream, mut upstream_rx) = mpsc::channel(1);
        let (events, _) = broadcast::channel(8);
        let feed = RithmicTradingFeed {
            commands: upstream,
            events,
            order_ready: Arc::new(AtomicBool::new(false)),
            pnl_ready: Arc::new(AtomicBool::new(false)),
        };
        let client = feed.client();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        client
            .commands
            .send(TradingCommand::Accounts(result_tx))
            .await
            .unwrap();
        let error = result_rx.await.unwrap().unwrap_err();
        assert!(error.contains("reconnecting"));
        assert!(matches!(
            upstream_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}
