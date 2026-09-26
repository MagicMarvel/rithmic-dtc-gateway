//! Provider-neutral market service contracts.
//!
//! Rithmic implements these contracts. DTC and options consume them without
//! depending on one another.

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::{
    market_data::MarketSnapshot,
    order_book::{DepthLevel, LevelUpdate},
};

#[derive(Debug, Clone, PartialEq)]
pub struct Instrument {
    pub symbol: String,
    pub exchange: String,
    pub underlying_symbol: String,
    pub description: String,
    pub min_price_increment: f32,
    pub price_display_format: i32,
    pub currency_value_per_increment: f32,
    pub contract_size: f32,
    pub currency: String,
    pub expiration_date: u32,
    pub exchange_symbol: String,
}

impl Instrument {
    pub fn es(symbol: impl Into<String>, exchange: impl Into<String>) -> Result<Self, String> {
        let symbol = symbol.into();
        let exchange = exchange.into();
        if !symbol.starts_with("ES") || symbol.len() < 4 {
            return Err(format!("{symbol} is not an ES futures contract symbol"));
        }
        if exchange.trim().is_empty() {
            return Err("ES exchange must not be empty".to_owned());
        }
        Ok(Self {
            symbol,
            exchange,
            underlying_symbol: "ES".to_owned(),
            description: "E-mini S&P 500 Futures".to_owned(),
            min_price_increment: 0.25,
            price_display_format: 2,
            currency_value_per_increment: 12.5,
            contract_size: 50.0,
            currency: "USD".to_owned(),
            expiration_date: 0,
            exchange_symbol: String::new(),
        })
    }

    pub(crate) fn matches(&self, symbol: &str, exchange: &str) -> bool {
        (symbol.eq_ignore_ascii_case(&self.symbol)
            && (exchange.is_empty() || exchange.eq_ignore_ascii_case(&self.exchange)))
            || (exchange.is_empty()
                && [
                    format!("{}-{}", self.symbol, self.exchange),
                    format!("{}.{}", self.symbol, self.exchange),
                ]
                .iter()
                .any(|combined| symbol.eq_ignore_ascii_case(combined)))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MarketEvent {
    Snapshot {
        symbol_id: u32,
        snapshot: MarketSnapshot,
    },
    SessionVolume {
        symbol_id: u32,
        volume: f64,
    },
    FeedStatus {
        available: bool,
    },
    LastTrade {
        symbol_id: u32,
        price: f64,
        volume: f64,
        datetime_us: i64,
        at_bid_or_ask: u8,
        is_snapshot: bool,
    },
    BestBidAsk {
        symbol_id: u32,
        bid_price: f64,
        bid_quantity: f64,
        ask_price: f64,
        ask_quantity: f64,
        datetime_us: i64,
    },
    DepthUpdate {
        symbol_id: u32,
        update: LevelUpdate,
        datetime_us: i64,
        is_final: bool,
    },
    DepthSnapshotLevel {
        symbol_id: u32,
        level: DepthLevel,
        datetime_us: i64,
        is_first: bool,
        is_last: bool,
    },
    FeedError(String),
}

#[derive(Debug)]
pub(crate) enum MarketCommand {
    LoadCatalog {
        preferred_underlying: String,
        response: oneshot::Sender<Result<Vec<Instrument>, String>>,
    },
    ListCatalogExchanges {
        response: oneshot::Sender<Result<Vec<String>, String>>,
    },
    SearchCatalog {
        search_text: String,
        exchange: String,
        search_type: i32,
        response: oneshot::Sender<Result<Vec<Instrument>, String>>,
    },
    EnumerateCatalog {
        exchange: String,
        underlying: String,
        roots_only: bool,
        response: oneshot::Sender<Result<Vec<Instrument>, String>>,
    },
    ResolveCatalogInstrument {
        symbol: String,
        exchange: String,
        response: oneshot::Sender<Result<Instrument, String>>,
    },
    Subscribe {
        symbol_id: u32,
        symbol: String,
        exchange: String,
        response: oneshot::Sender<Result<MarketSnapshot, String>>,
    },
    Snapshot {
        symbol: String,
        exchange: String,
        response: oneshot::Sender<Result<MarketSnapshot, String>>,
    },
    DepthSnapshot {
        symbol: String,
        exchange: String,
        tick_size: f64,
        max_levels: usize,
        response: oneshot::Sender<Result<Vec<DepthLevel>, String>>,
    },
    Unsubscribe {
        symbol_id: u32,
        response: oneshot::Sender<Result<(), String>>,
    },
    SubscribeDepth {
        symbol_id: u32,
        symbol: String,
        exchange: String,
        tick_size: f64,
        max_levels: usize,
        response: oneshot::Sender<Result<Vec<DepthLevel>, String>>,
    },
    UnsubscribeDepth {
        symbol_id: u32,
        response: oneshot::Sender<Result<(), String>>,
    },
}

pub struct MarketDataClient {
    pub(crate) commands: mpsc::Sender<MarketCommand>,
    pub(crate) events: mpsc::Receiver<MarketEvent>,
    pub(crate) publish_catalog_at_logon: bool,
}

/// Commands the terminal and the DTC server issue without owning the event
/// stream. Cloning only clones the command channel.
#[derive(Clone)]
pub(crate) struct MarketControl {
    commands: mpsc::Sender<MarketCommand>,
}

impl MarketDataClient {
    pub(crate) fn new(
        commands: mpsc::Sender<MarketCommand>,
        events: mpsc::Receiver<MarketEvent>,
    ) -> Self {
        Self {
            commands,
            events,
            publish_catalog_at_logon: false,
        }
    }

    pub(crate) fn with_logon_catalog(
        commands: mpsc::Sender<MarketCommand>,
        events: mpsc::Receiver<MarketEvent>,
    ) -> Self {
        Self {
            commands,
            events,
            publish_catalog_at_logon: true,
        }
    }

    pub(crate) fn split(self) -> (MarketControl, mpsc::Receiver<MarketEvent>) {
        (
            MarketControl {
                commands: self.commands,
            },
            self.events,
        )
    }
}

impl MarketControl {
    pub(crate) async fn subscribe(
        &self,
        symbol_id: u32,
        symbol: &str,
        exchange: &str,
    ) -> Result<MarketSnapshot, String> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(MarketCommand::Subscribe {
                symbol_id,
                symbol: symbol.to_owned(),
                exchange: exchange.to_owned(),
                response,
            })
            .await
            .map_err(|_| "Rithmic market gateway stopped".to_owned())?;
        receiver
            .await
            .map_err(|_| "subscription response dropped".to_owned())?
    }

    /// One-shot snapshot of an already subscribed symbol: served from the
    /// in-memory state without an upstream round trip, which is what a browser
    /// reload or a second tab needs.
    pub(crate) async fn snapshot(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<MarketSnapshot, String> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(MarketCommand::Snapshot {
                symbol: symbol.to_owned(),
                exchange: exchange.to_owned(),
                response,
            })
            .await
            .map_err(|_| "Rithmic market gateway stopped".to_owned())?;
        receiver
            .await
            .map_err(|_| "snapshot response dropped".to_owned())?
    }

    pub(crate) async fn subscribe_depth(
        &self,
        symbol_id: u32,
        symbol: &str,
        exchange: &str,
        tick_size: f64,
        max_levels: usize,
    ) -> Result<Vec<DepthLevel>, String> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(MarketCommand::SubscribeDepth {
                symbol_id,
                symbol: symbol.to_owned(),
                exchange: exchange.to_owned(),
                tick_size,
                max_levels,
                response,
            })
            .await
            .map_err(|_| "Rithmic depth gateway stopped".to_owned())?;
        receiver
            .await
            .map_err(|_| "depth subscription response dropped".to_owned())?
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistoricalRequest {
    pub request_id: i32,
    pub symbol: String,
    pub exchange: String,
    pub record_interval: i32,
    pub start_time: i64,
    pub end_time: i64,
    pub max_days: u32,
    /// When greater than 1, request Rithmic tick bars aggregating this many
    /// trades per bar instead of time bars or raw ticks.
    pub tick_bar_length: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HistoricalRecord {
    Tick {
        datetime_us: i64,
        price: f64,
        volume: f64,
        at_bid_or_ask: u16,
    },
    Bar {
        start_datetime_us: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        num_trades: u32,
        bid_volume: f64,
        ask_volume: f64,
    },
}

impl HistoricalRecord {
    pub(crate) fn datetime_us(&self) -> i64 {
        match self {
            Self::Tick { datetime_us, .. } => *datetime_us,
            Self::Bar {
                start_datetime_us, ..
            } => *start_datetime_us,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistoricalResponse {
    pub request_id: i32,
    pub record_interval: i32,
    pub records: Vec<HistoricalRecord>,
    pub is_final: bool,
}

#[derive(Clone)]
pub struct HistoryDataClient {
    pub(crate) commands: mpsc::Sender<(
        HistoricalRequest,
        mpsc::Sender<Result<HistoricalResponse, String>>,
    )>,
}

impl HistoryDataClient {
    pub(crate) fn new(
        commands: mpsc::Sender<(
            HistoricalRequest,
            mpsc::Sender<Result<HistoricalResponse, String>>,
        )>,
    ) -> Self {
        Self { commands }
    }

    pub(crate) async fn load(
        &self,
        request: HistoricalRequest,
    ) -> Result<Vec<HistoricalRecord>, String> {
        self.load_with_progress(request, |_, _| {}).await
    }

    /// Like [`load`](Self::load) but calls `on_batch(batches_received,
    /// records_so_far)` after every batch the worker sends, which is one per
    /// request chunk (see `history_feed::split_history_request`).
    pub(crate) async fn load_with_progress(
        &self,
        request: HistoricalRequest,
        mut on_batch: impl FnMut(usize, usize),
    ) -> Result<Vec<HistoricalRecord>, String> {
        let (response_tx, mut response_rx) = mpsc::channel(8);
        self.commands
            .send((request, response_tx))
            .await
            .map_err(|_| "Rithmic history worker stopped".to_owned())?;
        let mut records = Vec::new();
        let mut batches = 0usize;
        while let Some(response) = response_rx.recv().await {
            let response = response?;
            records.extend(response.records);
            batches += 1;
            on_batch(batches, records.len());
            if response.is_final {
                return Ok(records);
            }
        }
        Err("Rithmic history response ended before the final batch".to_owned())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TradeAccount {
    pub account_id: String,
    pub currency: String,
    pub trading_disabled: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TradingOrder {
    pub request_id: i32,
    pub symbol: String,
    pub exchange: String,
    pub account_id: String,
    pub client_order_id: String,
    pub server_order_id: String,
    pub exchange_order_id: String,
    pub order_status: i32,
    pub update_reason: i32,
    pub order_type: i32,
    pub buy_sell: i32,
    pub price1: f64,
    pub price2: f64,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub average_fill_price: f64,
    pub last_fill_price: f64,
    pub last_fill_quantity: f64,
    pub last_fill_datetime_ms: i64,
    pub last_fill_execution_id: String,
    pub info_text: String,
    pub time_in_force: i32,
    pub is_snapshot: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TradingPosition {
    pub symbol: String,
    pub exchange: String,
    pub account_id: String,
    pub quantity: f64,
    pub average_price: f64,
    pub open_profit_loss: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccountBalance {
    pub account_id: String,
    pub currency: String,
    pub cash_balance: f64,
    pub available_funds: f64,
    pub open_profit_loss: f64,
    pub daily_profit_loss: f64,
    pub trading_disabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewOrderRequest {
    pub symbol: String,
    pub exchange: String,
    pub account_id: String,
    pub client_order_id: String,
    pub order_type: i32,
    pub buy_sell: i32,
    pub price1: f64,
    pub price2: f64,
    pub quantity: f64,
    pub time_in_force: i32,
    pub is_automated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModifyOrderRequest {
    pub server_order_id: String,
    pub client_order_id: String,
    pub account_id: String,
    pub price1: Option<f64>,
    pub price2: Option<f64>,
    pub quantity: f64,
    pub time_in_force: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelOrderRequest {
    pub server_order_id: String,
    pub client_order_id: String,
    pub account_id: String,
}

#[derive(Debug)]
pub(crate) enum TradingCommand {
    Accounts(oneshot::Sender<Result<Vec<TradeAccount>, String>>),
    OpenOrders(oneshot::Sender<Result<Vec<TradingOrder>, String>>),
    OrderState(
        String,
        oneshot::Sender<Result<Option<TradingOrder>, String>>,
    ),
    Positions(oneshot::Sender<Result<Vec<TradingPosition>, String>>),
    Balance(oneshot::Sender<Result<AccountBalance, String>>),
    Submit(NewOrderRequest, oneshot::Sender<Result<(), String>>),
    Modify(ModifyOrderRequest, oneshot::Sender<Result<(), String>>),
    Cancel(CancelOrderRequest, oneshot::Sender<Result<(), String>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TradingEvent {
    Order(TradingOrder),
    Position(TradingPosition),
    Balance(AccountBalance),
    Error(String),
}

pub struct TradingDataClient {
    pub(crate) commands: mpsc::Sender<TradingCommand>,
    pub(crate) events: mpsc::Receiver<TradingEvent>,
}

#[derive(Clone)]
pub(crate) struct TradingControl {
    commands: mpsc::Sender<TradingCommand>,
}

impl TradingDataClient {
    pub(crate) fn new(
        commands: mpsc::Sender<TradingCommand>,
        events: mpsc::Receiver<TradingEvent>,
    ) -> Self {
        Self { commands, events }
    }

    pub(crate) fn split(self) -> (TradingControl, mpsc::Receiver<TradingEvent>) {
        (
            TradingControl {
                commands: self.commands,
            },
            self.events,
        )
    }
}

impl TradingControl {
    pub(crate) async fn accounts(&self) -> Result<Vec<TradeAccount>, String> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(TradingCommand::Accounts(tx))
            .await
            .map_err(|_| "trading service stopped".to_owned())?;
        rx.await
            .map_err(|_| "account response dropped".to_owned())?
    }
    pub(crate) async fn open_orders(&self) -> Result<Vec<TradingOrder>, String> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(TradingCommand::OpenOrders(tx))
            .await
            .map_err(|_| "trading service stopped".to_owned())?;
        rx.await.map_err(|_| "orders response dropped".to_owned())?
    }
    pub(crate) async fn positions(&self) -> Result<Vec<TradingPosition>, String> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(TradingCommand::Positions(tx))
            .await
            .map_err(|_| "trading service stopped".to_owned())?;
        rx.await
            .map_err(|_| "positions response dropped".to_owned())?
    }
    pub(crate) async fn balance(&self) -> Result<AccountBalance, String> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(TradingCommand::Balance(tx))
            .await
            .map_err(|_| "trading service stopped".to_owned())?;
        rx.await
            .map_err(|_| "balance response dropped".to_owned())?
    }
    pub(crate) async fn submit(&self, request: NewOrderRequest) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(TradingCommand::Submit(request, tx))
            .await
            .map_err(|_| "trading service stopped".to_owned())?;
        rx.await.map_err(|_| "submit response dropped".to_owned())?
    }
    pub(crate) async fn cancel(&self, request: CancelOrderRequest) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(TradingCommand::Cancel(request, tx))
            .await
            .map_err(|_| "trading service stopped".to_owned())?;
        rx.await.map_err(|_| "cancel response dropped".to_owned())?
    }
}

pub type MarketClientFactory = std::sync::Arc<dyn Fn() -> MarketDataClient + Send + Sync>;
pub type HistoryClientFactory = std::sync::Arc<dyn Fn() -> HistoryDataClient + Send + Sync>;
pub type TradingClientFactory = std::sync::Arc<dyn Fn() -> TradingDataClient + Send + Sync>;
