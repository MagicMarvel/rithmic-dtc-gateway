//! DTC client adapters used by the standalone Web terminal.
//!
//! The browser service talks only to the gateway's DTC listener. Rithmic
//! credentials and SDK objects stay in the gateway process.

use std::{collections::HashMap, env, io, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{RwLock, mpsc},
    time,
};

use rithmic_dtc_bridge::{
    connection::ConnectionSettings,
    dtc,
    dtc_accounts::DtcAccounts,
    market_data::MarketSnapshot,
    market_gateway::{
        AccountBalance, CancelOrderRequest, HistoricalRecord, HistoricalRequest,
        HistoricalResponse, HistoryDataClient, MarketCommand, MarketDataClient, MarketEvent,
        NewOrderRequest, TradeAccount, TradingCommand, TradingDataClient, TradingEvent,
        TradingOrder, TradingPosition,
    },
    order_book::{DepthLevel, LevelUpdate, LevelUpdateType, Side},
};

const LOGON_SIZE: usize = 284;

#[derive(Debug)]
struct Frame {
    kind: u16,
    bytes: Vec<u8>,
}

pub fn address_from_env() -> String {
    env::var("DTC_GATEWAY_ADDR").unwrap_or_else(|_| "gateway:11099".to_owned())
}

/// Create provider-neutral clients backed entirely by the DTC wire protocol.
pub fn clients(address: impl Into<String>) -> (MarketDataClient, HistoryDataClient) {
    let address = address.into();
    (market_client(address.clone()), history_client(address))
}

pub fn trading_client(address: impl Into<String>) -> TradingDataClient {
    let address = address.into();
    let (commands_tx, mut commands_rx) = mpsc::channel(64);
    let (events_tx, events_rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        while let Some(command) = commands_rx.recv().await {
            let address = address.clone();
            let events = events_tx.clone();
            tokio::spawn(async move {
                match command {
                    TradingCommand::Accounts(response) => {
                        let _ = response.send(load_accounts(&address).await);
                    }
                    TradingCommand::OpenOrders(response) => {
                        let _ = response.send(load_orders(&address).await);
                    }
                    TradingCommand::Positions(response) => {
                        let _ = response.send(load_positions(&address).await);
                    }
                    TradingCommand::Balance(response) => {
                        let _ = response.send(load_balance(&address).await);
                    }
                    TradingCommand::Submit(request, response) => {
                        let result = submit_order(&address, &request).await;
                        if let Ok(order) = &result {
                            let _ = events.send(TradingEvent::Order(order.clone())).await;
                        }
                        let _ = response.send(result.map(|_| ()));
                    }
                    TradingCommand::Cancel(request, response) => {
                        let result = cancel_order(&address, &request).await;
                        if let Ok(order) = &result {
                            let _ = events.send(TradingEvent::Order(order.clone())).await;
                        }
                        let _ = response.send(result.map(|_| ()));
                    }
                    TradingCommand::OrderState(_, response) => {
                        let _ = response.send(Ok(None));
                    }
                    TradingCommand::Modify(_, response) => {
                        let _ = response.send(Err(
                            "DTC Web adapter does not expose order modification yet".to_owned(),
                        ));
                    }
                }
            });
        }
    });
    TradingDataClient::new(commands_tx, events_rx)
}

pub async fn configure_gateway_trading(settings: &ConnectionSettings) -> Result<(), String> {
    let base =
        env::var("DTC_GATEWAY_ADMIN_URL").map_err(|_| "DTC_GATEWAY_ADMIN_URL is not configured")?;
    let token = env::var("DTC_ADMIN_TOKEN")
        .map_err(|_| "DTC_ADMIN_TOKEN is not configured in the Web container")?;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/config", base.trim_end_matches('/')))
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "Gateway account config returned HTTP {}",
            response.status()
        ));
    }
    let mut accounts: DtcAccounts = response.json().await.map_err(|e| e.to_string())?;
    accounts.trading_enabled = true;
    accounts.trading.login.environment = settings.environment.clone();
    accounts.trading.login.user = settings.user.clone();
    accounts.trading.login.password = settings.password.clone();
    accounts.trading.login.url = settings.url.clone();
    accounts.trading.login.beta_url = settings.alt_url.clone();
    accounts.trading.login.system_name = settings.system_name.clone();
    accounts.trading.login.app_name = settings.app_name.clone();
    accounts.trading.login.app_version = settings.app_version.clone();
    accounts.trading.account_id = settings.account_id.clone();
    accounts.trading.fcm_id = settings.fcm_id.clone();
    accounts.trading.ib_id = settings.ib_id.clone();
    let response = client
        .post(format!("{}/api/config", base.trim_end_matches('/')))
        .bearer_auth(&token)
        .json(&accounts)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(response
            .text()
            .await
            .unwrap_or_else(|_| "Gateway rejected trading settings".to_owned()))
    }
}

pub async fn disable_gateway_trading() -> Result<(), String> {
    let base =
        env::var("DTC_GATEWAY_ADMIN_URL").map_err(|_| "DTC_GATEWAY_ADMIN_URL is not configured")?;
    let token = env::var("DTC_ADMIN_TOKEN")
        .map_err(|_| "DTC_ADMIN_TOKEN is not configured in the Web container")?;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/config", base.trim_end_matches('/')))
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let mut accounts: DtcAccounts = response.json().await.map_err(|e| e.to_string())?;
    accounts.trading_enabled = false;
    let response = client
        .post(format!("{}/api/config", base.trim_end_matches('/')))
        .bearer_auth(&token)
        .json(&accounts)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(response
            .text()
            .await
            .unwrap_or_else(|_| "Gateway rejected trading reset".to_owned()))
    }
}

fn market_client(address: String) -> MarketDataClient {
    let (commands_tx, mut commands_rx) = mpsc::channel(128);
    let (events_tx, events_rx) = mpsc::channel(4096);
    let snapshots = Arc::new(RwLock::new(
        HashMap::<(String, String), MarketSnapshot>::new(),
    ));
    tokio::spawn(async move {
        while let Some(command) = commands_rx.recv().await {
            match command {
                MarketCommand::Subscribe {
                    symbol_id,
                    symbol,
                    exchange,
                    response,
                } => {
                    let address = address.clone();
                    let events = events_tx.clone();
                    let snapshots = snapshots.clone();
                    tokio::spawn(async move {
                        let result = subscribe_market(
                            &address, symbol_id, &symbol, &exchange, events, snapshots,
                        )
                        .await;
                        let _ = response.send(result);
                    });
                }
                MarketCommand::Snapshot {
                    symbol,
                    exchange,
                    response,
                } => {
                    let value = snapshots
                        .read()
                        .await
                        .get(&(symbol, exchange))
                        .cloned()
                        .ok_or_else(|| {
                            "DTC snapshot is not available before subscription".to_owned()
                        });
                    let _ = response.send(value);
                }
                MarketCommand::SubscribeDepth {
                    symbol_id,
                    symbol,
                    exchange,
                    max_levels,
                    response,
                    ..
                } => {
                    let address = address.clone();
                    let events = events_tx.clone();
                    tokio::spawn(async move {
                        let result = subscribe_depth(
                            &address, symbol_id, &symbol, &exchange, max_levels, events,
                        )
                        .await;
                        let _ = response.send(result);
                    });
                }
                MarketCommand::DepthSnapshot {
                    symbol,
                    exchange,
                    max_levels,
                    response,
                    ..
                } => {
                    let address = address.clone();
                    tokio::spawn(async move {
                        let (events, _) = mpsc::channel(1);
                        let result =
                            subscribe_depth(&address, 1, &symbol, &exchange, max_levels, events)
                                .await;
                        let _ = response.send(result);
                    });
                }
                MarketCommand::Unsubscribe { response, .. }
                | MarketCommand::UnsubscribeDepth { response, .. } => {
                    let _ = response.send(Ok(()));
                }
                MarketCommand::ResolveCatalogInstrument { response, .. } => {
                    let _ = response.send(Err(
                        "DTC Web adapter does not expose catalog discovery".to_owned()
                    ));
                }
                MarketCommand::LoadCatalog { response, .. }
                | MarketCommand::SearchCatalog { response, .. }
                | MarketCommand::EnumerateCatalog { response, .. } => {
                    let _ = response.send(Ok(Vec::new()));
                }
                MarketCommand::ListCatalogExchanges { response } => {
                    let _ = response.send(Ok(Vec::new()));
                }
            }
        }
    });
    MarketDataClient::new(commands_tx, events_rx)
}

fn history_client(address: String) -> HistoryDataClient {
    let (commands_tx, mut commands_rx) = mpsc::channel::<(
        HistoricalRequest,
        mpsc::Sender<Result<HistoricalResponse, String>>,
    )>(32);
    tokio::spawn(async move {
        while let Some((request, output)) = commands_rx.recv().await {
            let address = address.clone();
            tokio::spawn(async move {
                let request_id = request.request_id;
                let interval = request.record_interval;
                let result =
                    load_history(&address, request)
                        .await
                        .map(|records| HistoricalResponse {
                            request_id,
                            record_interval: interval,
                            records,
                            is_final: true,
                        });
                let _ = output.send(result).await;
            });
        }
    });
    HistoryDataClient::new(commands_tx)
}

async fn open_session(address: &str) -> Result<TcpStream, String> {
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|e| format!("DTC connect {address}: {e}"))?;
    stream.set_nodelay(true).map_err(|e| e.to_string())?;
    let mut encoding = [0_u8; 16];
    put_u16(&mut encoding, 0, 16);
    put_u16(&mut encoding, 2, dtc::ENCODING_REQUEST);
    put_i32(&mut encoding, 4, dtc::CURRENT_VERSION);
    put_i32(&mut encoding, 8, dtc::BINARY_ENCODING);
    encoding[12..15].copy_from_slice(b"DTC");
    stream
        .write_all(&encoding)
        .await
        .map_err(|e| e.to_string())?;
    let response = read_frame(&mut stream)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("DTC closed during encoding negotiation")?;
    if response.kind != dtc::ENCODING_RESPONSE {
        return Err(format!(
            "expected DTC encoding response, got {}",
            response.kind
        ));
    }
    let mut logon = [0_u8; LOGON_SIZE];
    put_u16(&mut logon, 0, LOGON_SIZE as u16);
    put_u16(&mut logon, 2, dtc::LOGON_REQUEST);
    put_i32(&mut logon, 4, dtc::CURRENT_VERSION);
    put_i32(&mut logon, 144, 15);
    put_string(&mut logon[148..180], "rithmic-web-terminal");
    stream.write_all(&logon).await.map_err(|e| e.to_string())?;
    loop {
        let response = read_frame(&mut stream)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("DTC closed during logon")?;
        if response.kind == dtc::LOGON_RESPONSE {
            if i32_at(&response.bytes, 8) != 1 {
                return Err(format!(
                    "DTC logon rejected: {}",
                    string_at(&response.bytes, 12, 108)
                ));
            }
            return Ok(stream);
        }
    }
}

async fn subscribe_market(
    address: &str,
    symbol_id: u32,
    symbol: &str,
    exchange: &str,
    events: mpsc::Sender<MarketEvent>,
    snapshots: Arc<RwLock<HashMap<(String, String), MarketSnapshot>>>,
) -> Result<MarketSnapshot, String> {
    let mut stream = open_session(address).await?;
    let request = market_request(dtc::MARKET_DATA_REQUEST, symbol_id, symbol, exchange, 0);
    stream
        .write_all(&request)
        .await
        .map_err(|e| e.to_string())?;
    let snapshot = loop {
        let frame = read_frame(&mut stream)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("DTC market connection closed")?;
        if frame.kind == dtc::MARKET_DATA_REJECT {
            return Err(string_at(&frame.bytes, 8, frame.bytes.len()));
        }
        if frame.kind == dtc::MARKET_DATA_SNAPSHOT && u32_at(&frame.bytes, 4) == symbol_id {
            break parse_snapshot(&frame.bytes);
        }
    };
    snapshots
        .write()
        .await
        .insert((symbol.to_owned(), exchange.to_owned()), snapshot.clone());
    let _ = events
        .send(MarketEvent::FeedStatus { available: true })
        .await;
    let _ = events
        .send(MarketEvent::Snapshot {
            symbol_id,
            snapshot: snapshot.clone(),
        })
        .await;
    tokio::spawn(stream_market(
        stream,
        events,
        snapshots,
        symbol.to_owned(),
        exchange.to_owned(),
    ));
    Ok(snapshot)
}

async fn stream_market(
    mut stream: TcpStream,
    events: mpsc::Sender<MarketEvent>,
    snapshots: Arc<RwLock<HashMap<(String, String), MarketSnapshot>>>,
    symbol: String,
    exchange: String,
) {
    let mut heartbeat = time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => { if stream.write_all(&heartbeat_frame()).await.is_err() { break; } }
            frame = read_frame(&mut stream) => match frame {
                Ok(Some(frame)) => {
                    if let Some(event) = parse_market_event(&frame) {
                        if let MarketEvent::BestBidAsk { bid_price, bid_quantity, ask_price, ask_quantity, datetime_us, .. } = &event {
                            if let Some(value) = snapshots.write().await.get_mut(&(symbol.clone(), exchange.clone())) {
                                value.bid = Some(*bid_price); value.bid_size = Some(*bid_quantity); value.ask = Some(*ask_price); value.ask_size = Some(*ask_quantity); value.quote_time_us = *datetime_us;
                            }
                        }
                        if events.send(event).await.is_err() { return; }
                    }
                }
                _ => break,
            }
        }
    }
    let _ = events
        .send(MarketEvent::FeedStatus { available: false })
        .await;
    let _ = events
        .send(MarketEvent::FeedError(
            "DTC market connection ended".to_owned(),
        ))
        .await;
}

async fn subscribe_depth(
    address: &str,
    symbol_id: u32,
    symbol: &str,
    exchange: &str,
    max_levels: usize,
    events: mpsc::Sender<MarketEvent>,
) -> Result<Vec<DepthLevel>, String> {
    let mut stream = open_session(address).await?;
    let request = market_request(
        dtc::MARKET_DEPTH_REQUEST,
        symbol_id,
        symbol,
        exchange,
        max_levels.min(i32::MAX as usize) as i32,
    );
    stream
        .write_all(&request)
        .await
        .map_err(|e| e.to_string())?;
    let mut levels = Vec::new();
    loop {
        let frame = read_frame(&mut stream)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("DTC depth connection closed")?;
        if frame.kind == dtc::MARKET_DEPTH_REJECT {
            return Err(string_at(&frame.bytes, 8, frame.bytes.len()));
        }
        if frame.kind == dtc::MARKET_DEPTH_SNAPSHOT_LEVEL && u32_at(&frame.bytes, 4) == symbol_id {
            if f64_at(&frame.bytes, 24) > 0.0 {
                levels.push(parse_depth_level(&frame.bytes));
            }
            if frame.bytes.get(35).copied() == Some(1) {
                break;
            }
        }
    }
    let initial = levels.clone();
    tokio::spawn(async move {
        let mut heartbeat = time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = heartbeat.tick() => { if stream.write_all(&heartbeat_frame()).await.is_err() { return; } }
                frame = read_frame(&mut stream) => match frame {
                    Ok(Some(frame)) if frame.kind == dtc::MARKET_DEPTH_UPDATE_LEVEL_V2 => {
                        let _ = events.send(parse_depth_update(&frame.bytes)).await;
                    }
                    Ok(Some(frame)) if frame.kind == dtc::MARKET_DEPTH_SNAPSHOT_LEVEL => {
                        let level = parse_depth_level(&frame.bytes);
                        let _ = events.send(MarketEvent::DepthSnapshotLevel { symbol_id: u32_at(&frame.bytes, 4), level, datetime_us: (f64_at(&frame.bytes, 40) * 1_000_000.0) as i64, is_first: frame.bytes[34] != 0, is_last: frame.bytes[35] != 0 }).await;
                    }
                    Ok(Some(_)) => {}
                    _ => return,
                }
            }
        }
    });
    Ok(initial)
}

async fn load_history(
    address: &str,
    request: rithmic_dtc_bridge::market_gateway::HistoricalRequest,
) -> Result<Vec<HistoricalRecord>, String> {
    let mut stream = open_session(address).await?;
    let mut wire = vec![0_u8; 128];
    put_u16(&mut wire, 0, 128);
    put_u16(&mut wire, 2, dtc::HISTORICAL_PRICE_DATA_REQUEST);
    put_i32(&mut wire, 4, request.request_id);
    put_string(&mut wire[8..72], &request.symbol);
    put_string(&mut wire[72..88], &request.exchange);
    put_i32(&mut wire, 88, request.record_interval);
    put_i64(&mut wire, 96, request.start_time);
    put_i64(&mut wire, 104, request.end_time);
    put_u32(&mut wire, 112, request.max_days);
    put_u32(&mut wire, 116, request.tick_bar_length);
    stream.write_all(&wire).await.map_err(|e| e.to_string())?;
    let mut records = Vec::new();
    let mut heartbeat = time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => stream.write_all(&heartbeat_frame()).await.map_err(|e| e.to_string())?,
            frame = read_frame(&mut stream) => {
                let frame = frame.map_err(|e| e.to_string())?.ok_or("DTC history connection closed")?;
                match frame.kind {
                    dtc::HISTORICAL_PRICE_DATA_REJECT => return Err(string_at(&frame.bytes, 8, 104)),
                    dtc::HISTORICAL_PRICE_DATA_RESPONSE_HEADER if frame.bytes.get(13).copied() == Some(1) => return Ok(records),
                    dtc::HISTORICAL_PRICE_DATA_RECORD_RESPONSE => {
                        records.push(HistoricalRecord::Bar { start_datetime_us: i64_at(&frame.bytes, 8), open: f64_at(&frame.bytes, 16), high: f64_at(&frame.bytes, 24), low: f64_at(&frame.bytes, 32), close: f64_at(&frame.bytes, 40), volume: f64_at(&frame.bytes, 48), num_trades: u32_at(&frame.bytes, 56), bid_volume: f64_at(&frame.bytes, 64), ask_volume: f64_at(&frame.bytes, 72) });
                        if frame.bytes[80] != 0 { return Ok(records); }
                    }
                    dtc::HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE => {
                        records.push(HistoricalRecord::Tick { datetime_us: (f64_at(&frame.bytes, 8) * 1_000_000.0) as i64, at_bid_or_ask: u16_at(&frame.bytes, 16), price: f64_at(&frame.bytes, 24), volume: f64_at(&frame.bytes, 32) });
                        if frame.bytes[40] != 0 { return Ok(records); }
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn request_frames(
    address: &str,
    request: &[u8],
    final_message: impl Fn(&Frame) -> bool,
) -> Result<Vec<Frame>, String> {
    let mut stream = open_session(address).await?;
    stream.write_all(request).await.map_err(|e| e.to_string())?;
    let mut frames = Vec::new();
    let mut heartbeat = time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => stream.write_all(&heartbeat_frame()).await.map_err(|e| e.to_string())?,
            frame = read_frame(&mut stream) => {
                let frame = frame.map_err(|e| e.to_string())?.ok_or("DTC trading connection closed")?;
                if matches!(frame.kind, dtc::OPEN_ORDERS_REJECT | dtc::CURRENT_POSITIONS_REJECT | dtc::ACCOUNT_BALANCE_REJECT) {
                    return Err(string_at(&frame.bytes, 8, 104));
                }
                let done = final_message(&frame);
                frames.push(frame);
                if done { return Ok(frames); }
            }
        }
    }
}

async fn load_accounts(address: &str) -> Result<Vec<TradeAccount>, String> {
    let request_id = 401;
    let mut request = vec![0_u8; 8];
    put_u16(&mut request, 0, 8);
    put_u16(&mut request, 2, dtc::TRADE_ACCOUNTS_REQUEST);
    put_i32(&mut request, 4, request_id);
    let frames = request_frames(address, &request, |f| {
        f.kind == dtc::TRADE_ACCOUNT_RESPONSE && i32_at(&f.bytes, 8) >= i32_at(&f.bytes, 4)
    })
    .await?;
    Ok(frames
        .into_iter()
        .filter(|f| f.kind == dtc::TRADE_ACCOUNT_RESPONSE)
        .filter_map(|f| {
            let account_id = string_at(&f.bytes, 12, 44);
            (!account_id.is_empty()).then(|| TradeAccount {
                account_id,
                currency: "USD".to_owned(),
                trading_disabled: i32_at(&f.bytes, 48) != 0,
            })
        })
        .collect())
}

async fn load_orders(address: &str) -> Result<Vec<TradingOrder>, String> {
    let request_id = 301;
    let mut request = vec![0_u8; 76];
    put_u16(&mut request, 0, 76);
    put_u16(&mut request, 2, dtc::OPEN_ORDERS_REQUEST);
    put_i32(&mut request, 4, request_id);
    put_i32(&mut request, 8, 1);
    let frames = request_frames(address, &request, |f| {
        f.kind == dtc::ORDER_UPDATE
            && (f.bytes[520] != 0 || i32_at(&f.bytes, 12) >= i32_at(&f.bytes, 8))
    })
    .await?;
    Ok(frames
        .into_iter()
        .filter(|f| f.kind == dtc::ORDER_UPDATE && f.bytes[520] == 0)
        .map(|f| parse_order(&f.bytes))
        .collect())
}

async fn load_positions(address: &str) -> Result<Vec<TradingPosition>, String> {
    let request_id = 306;
    let mut request = vec![0_u8; 40];
    put_u16(&mut request, 0, 40);
    put_u16(&mut request, 2, dtc::CURRENT_POSITIONS_REQUEST);
    put_i32(&mut request, 4, request_id);
    let frames = request_frames(address, &request, |f| {
        f.kind == dtc::POSITION_UPDATE && i32_at(&f.bytes, 12) >= i32_at(&f.bytes, 8)
    })
    .await?;
    Ok(frames
        .into_iter()
        .filter(|f| f.kind == dtc::POSITION_UPDATE && f.bytes[176] == 0)
        .map(|f| TradingPosition {
            symbol: string_at(&f.bytes, 16, 80),
            exchange: string_at(&f.bytes, 80, 96),
            quantity: f64_at(&f.bytes, 96),
            average_price: f64_at(&f.bytes, 104),
            account_id: string_at(&f.bytes, 144, 176),
            open_profit_loss: f64_at(&f.bytes, 200),
        })
        .collect())
}

async fn load_balance(address: &str) -> Result<AccountBalance, String> {
    let request_id = 601;
    let mut request = vec![0_u8; 40];
    put_u16(&mut request, 0, 40);
    put_u16(&mut request, 2, dtc::ACCOUNT_BALANCE_REQUEST);
    put_i32(&mut request, 4, request_id);
    let frames =
        request_frames(address, &request, |f| f.kind == dtc::ACCOUNT_BALANCE_UPDATE).await?;
    let frame = frames
        .into_iter()
        .find(|f| f.kind == dtc::ACCOUNT_BALANCE_UPDATE)
        .ok_or("DTC balance response missing")?;
    Ok(AccountBalance {
        cash_balance: f64_at(&frame.bytes, 8),
        available_funds: f64_at(&frame.bytes, 16),
        currency: string_at(&frame.bytes, 24, 32),
        account_id: string_at(&frame.bytes, 32, 64),
        open_profit_loss: f64_at(&frame.bytes, 96),
        daily_profit_loss: f64_at(&frame.bytes, 104),
        trading_disabled: frame.bytes[235] != 0,
    })
}

async fn submit_order(address: &str, order: &NewOrderRequest) -> Result<TradingOrder, String> {
    let mut request = vec![0_u8; 304];
    put_u16(&mut request, 0, 304);
    put_u16(&mut request, 2, dtc::SUBMIT_NEW_SINGLE_ORDER);
    put_string(&mut request[4..68], &order.symbol);
    put_string(&mut request[68..84], &order.exchange);
    put_string(&mut request[84..116], &order.account_id);
    put_string(&mut request[116..148], &order.client_order_id);
    put_i32(&mut request, 148, order.order_type);
    put_i32(&mut request, 152, order.buy_sell);
    put_f64(&mut request, 160, order.price1);
    put_f64(&mut request, 168, order.price2);
    put_f64(&mut request, 176, order.quantity);
    put_i32(&mut request, 184, order.time_in_force);
    request[200] = u8::from(order.is_automated);
    order_action(address, &request).await
}

async fn cancel_order(address: &str, order: &CancelOrderRequest) -> Result<TradingOrder, String> {
    let mut request = vec![0_u8; 100];
    put_u16(&mut request, 0, 100);
    put_u16(&mut request, 2, dtc::CANCEL_ORDER);
    put_string(&mut request[4..36], &order.server_order_id);
    put_string(&mut request[36..68], &order.client_order_id);
    put_string(&mut request[68..100], &order.account_id);
    order_action(address, &request).await
}

async fn order_action(address: &str, request: &[u8]) -> Result<TradingOrder, String> {
    let frames = request_frames(address, request, |f| f.kind == dtc::ORDER_UPDATE).await?;
    let frame = frames
        .into_iter()
        .find(|f| f.kind == dtc::ORDER_UPDATE)
        .ok_or("DTC order response missing")?;
    let order = parse_order(&frame.bytes);
    if order.order_status == 9 || order.order_status == 0 {
        return Err(if order.info_text.is_empty() {
            "DTC gateway rejected the order".to_owned()
        } else {
            order.info_text
        });
    }
    Ok(order)
}

fn parse_order(b: &[u8]) -> TradingOrder {
    TradingOrder {
        request_id: i32_at(b, 4),
        symbol: string_at(b, 16, 80),
        exchange: string_at(b, 80, 96),
        server_order_id: string_at(b, 128, 160),
        client_order_id: string_at(b, 160, 192),
        exchange_order_id: string_at(b, 192, 224),
        order_status: i32_at(b, 224),
        update_reason: i32_at(b, 228),
        order_type: i32_at(b, 232),
        buy_sell: i32_at(b, 236),
        price1: f64_at(b, 240),
        price2: f64_at(b, 248),
        time_in_force: i32_at(b, 256),
        quantity: f64_at(b, 272),
        filled_quantity: f64_at(b, 280),
        remaining_quantity: f64_at(b, 288),
        average_fill_price: f64_at(b, 296),
        last_fill_price: f64_at(b, 304),
        last_fill_datetime_ms: i64_at(b, 312),
        last_fill_quantity: f64_at(b, 320),
        last_fill_execution_id: string_at(b, 328, 392),
        account_id: string_at(b, 392, 424),
        info_text: string_at(b, 424, 520),
        is_snapshot: true,
    }
}

fn parse_market_event(frame: &Frame) -> Option<MarketEvent> {
    let b = &frame.bytes;
    match frame.kind {
        dtc::MARKET_DATA_FEED_STATUS => Some(MarketEvent::FeedStatus {
            available: i32_at(b, 4) == 2,
        }),
        dtc::MARKET_DATA_UPDATE_SESSION_VOLUME => Some(MarketEvent::SessionVolume {
            symbol_id: u32_at(b, 4),
            volume: f64_at(b, 8),
        }),
        dtc::MARKET_DATA_UPDATE_LAST_TRADE_SNAPSHOT => Some(MarketEvent::LastTrade {
            symbol_id: u32_at(b, 4),
            price: f64_at(b, 8),
            volume: f64_at(b, 16),
            datetime_us: (f64_at(b, 24) * 1_000_000.0) as i64,
            at_bid_or_ask: 0,
            is_snapshot: true,
        }),
        dtc::MARKET_DATA_UPDATE_TRADE_V2 => Some(MarketEvent::LastTrade {
            symbol_id: u32_at(b, 4),
            price: f64_at(b, 8),
            volume: f64_at(b, 16),
            datetime_us: i64_at(b, 24),
            at_bid_or_ask: b[32],
            is_snapshot: false,
        }),
        dtc::MARKET_DATA_UPDATE_BID_ASK_V2 => Some(MarketEvent::BestBidAsk {
            symbol_id: u32_at(b, 4),
            bid_price: f64_at(b, 8),
            bid_quantity: f64_at(b, 16),
            ask_price: f64_at(b, 24),
            ask_quantity: f64_at(b, 32),
            datetime_us: i64_at(b, 40),
        }),
        _ => None,
    }
}

fn parse_snapshot(b: &[u8]) -> MarketSnapshot {
    let optional = |offset| {
        let value = f64_at(b, offset);
        (value != f64::MAX).then_some(value)
    };
    MarketSnapshot {
        settlement: optional(8),
        open: optional(16),
        high: optional(24),
        low: optional(32),
        volume: optional(40),
        open_interest: (u32_at(b, 52) != u32::MAX).then(|| u32_at(b, 52)),
        bid: optional(56),
        ask: optional(64),
        ask_size: optional(72),
        bid_size: optional(80),
        last: optional(88),
        last_size: optional(96),
        last_time_us: (f64_at(b, 104) * 1_000_000.0) as i64,
        quote_time_us: (f64_at(b, 112) * 1_000_000.0) as i64,
        settlement_date: u32_at(b, 120),
    }
}

fn parse_depth_level(b: &[u8]) -> DepthLevel {
    DepthLevel {
        side: side(u16_at(b, 8) as u8),
        price: f64_at(b, 16),
        quantity: f64_at(b, 24),
        level: u16_at(b, 32),
        num_orders: u32_at(b, 48),
    }
}
fn parse_depth_update(b: &[u8]) -> MarketEvent {
    MarketEvent::DepthUpdate {
        symbol_id: u32_at(b, 4),
        datetime_us: i64_at(b, 8) * 1_000,
        update: LevelUpdate {
            price: f64_at(b, 16),
            quantity: f64_at(b, 24),
            num_orders: u16_at(b, 32),
            level: u16_at(b, 34),
            side: side(b[36]),
            update_type: match b[37] {
                2 => LevelUpdateType::Delete,
                3 => LevelUpdateType::Insert,
                _ => LevelUpdateType::Update,
            },
        },
        is_final: b[38] == 1,
    }
}
fn side(value: u8) -> Side {
    if value == 2 { Side::Ask } else { Side::Bid }
}

fn market_request(kind: u16, id: u32, symbol: &str, exchange: &str, levels: i32) -> Vec<u8> {
    let mut b = vec![0_u8; 96];
    put_u16(&mut b, 0, 96);
    put_u16(&mut b, 2, kind);
    put_i32(&mut b, 4, 1);
    put_u32(&mut b, 8, id);
    put_string(&mut b[12..76], symbol);
    put_string(&mut b[76..92], exchange);
    if kind == dtc::MARKET_DEPTH_REQUEST {
        put_i32(&mut b, 92, levels);
    }
    b
}
fn heartbeat_frame() -> [u8; 16] {
    let mut b = [0_u8; 16];
    put_u16(&mut b, 0, 16);
    put_u16(&mut b, 2, dtc::HEARTBEAT);
    b
}
async fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Frame>> {
    let mut h = [0_u8; 4];
    match stream.read_exact(&mut h).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };
    let size = u16::from_le_bytes([h[0], h[1]]) as usize;
    if size < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid DTC frame size",
        ));
    }
    let mut bytes = vec![0_u8; size];
    bytes[..4].copy_from_slice(&h);
    stream.read_exact(&mut bytes[4..]).await?;
    Ok(Some(Frame {
        kind: u16_at(&bytes, 2),
        bytes,
    }))
}
fn put_string(target: &mut [u8], value: &str) {
    let bytes = value.as_bytes();
    let n = bytes.len().min(target.len().saturating_sub(1));
    target[..n].copy_from_slice(&bytes[..n]);
}
fn string_at(b: &[u8], start: usize, end: usize) -> String {
    String::from_utf8_lossy(&b[start..end.min(b.len())])
        .trim_end_matches('\0')
        .to_owned()
}
fn put_u16(b: &mut [u8], o: usize, v: u16) {
    b[o..o + 2].copy_from_slice(&v.to_le_bytes());
}
fn put_u32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}
fn put_i32(b: &mut [u8], o: usize, v: i32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}
fn put_i64(b: &mut [u8], o: usize, v: i64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes());
}
fn put_f64(b: &mut [u8], o: usize, v: f64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes());
}
fn u16_at(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}
fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn i32_at(b: &[u8], o: usize) -> i32 {
    i32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn i64_at(b: &[u8], o: usize) -> i64 {
    i64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}
fn f64_at(b: &[u8], o: usize) -> f64 {
    f64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}
