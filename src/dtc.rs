//! Minimal DTC fixed-length binary session support.
//!
//! The wire layouts in this module follow Sierra Chart's public-domain
//! `DTCProtocol.h`, protocol version 8. Market data is deliberately not
//! advertised until the corresponding messages are implemented.

#[cfg(test)]
#[path = "dtc_audit_tests.rs"]
mod audit_tests;

use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    fmt, io,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    time,
};

use crate::market_data::MarketSnapshot;
use crate::order_book::{DepthLevel, LevelUpdate};

pub const CURRENT_VERSION: i32 = 8;
pub const BINARY_ENCODING: i32 = 0;

pub const LOGON_REQUEST: u16 = 1;
pub const LOGON_RESPONSE: u16 = 2;
pub const HEARTBEAT: u16 = 3;
pub const LOGOFF: u16 = 5;
pub const ENCODING_REQUEST: u16 = 6;
pub const ENCODING_RESPONSE: u16 = 7;
pub const MARKET_DATA_FEED_STATUS: u16 = 100;
pub const MARKET_DATA_REQUEST: u16 = 101;
pub const MARKET_DATA_REJECT: u16 = 103;
pub const MARKET_DATA_SNAPSHOT: u16 = 104;
pub const MARKET_DATA_UPDATE_SESSION_VOLUME: u16 = 113;
pub const MARKET_DATA_UPDATE_LAST_TRADE_SNAPSHOT: u16 = 134;
pub const MARKET_DATA_UPDATE_TRADE_V2: u16 = 147;
pub const MARKET_DATA_UPDATE_BID_ASK_V2: u16 = 148;
pub const MARKET_DEPTH_REQUEST: u16 = 102;
pub const MARKET_DEPTH_REJECT: u16 = 121;
pub const MARKET_DEPTH_SNAPSHOT_LEVEL: u16 = 122;
pub const MARKET_DEPTH_UPDATE_LEVEL_V2: u16 = 109;
pub const EXCHANGE_LIST_REQUEST: u16 = 500;
pub const EXCHANGE_LIST_RESPONSE: u16 = 501;
pub const SYMBOLS_FOR_EXCHANGE_REQUEST: u16 = 502;
pub const UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST: u16 = 503;
pub const SYMBOLS_FOR_UNDERLYING_REQUEST: u16 = 504;
pub const SECURITY_DEFINITION_FOR_SYMBOL_REQUEST: u16 = 506;
pub const SECURITY_DEFINITION_RESPONSE: u16 = 507;
pub const SYMBOL_SEARCH_REQUEST: u16 = 508;
pub const SECURITY_DEFINITION_REJECT: u16 = 509;
pub const HISTORICAL_PRICE_DATA_REQUEST: u16 = 800;
pub const HISTORICAL_PRICE_DATA_RESPONSE_HEADER: u16 = 801;
pub const HISTORICAL_PRICE_DATA_REJECT: u16 = 802;
pub const HISTORICAL_PRICE_DATA_RECORD_RESPONSE: u16 = 803;
pub const HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE: u16 = 804;
pub const SUBMIT_NEW_SINGLE_ORDER: u16 = 208;
pub const CANCEL_ORDER: u16 = 203;
pub const CANCEL_REPLACE_ORDER: u16 = 204;
pub const OPEN_ORDERS_REQUEST: u16 = 300;
pub const ORDER_UPDATE: u16 = 301;
pub const OPEN_ORDERS_REJECT: u16 = 302;
pub const CURRENT_POSITIONS_REQUEST: u16 = 305;
pub const POSITION_UPDATE: u16 = 306;
pub const CURRENT_POSITIONS_REJECT: u16 = 307;
pub const TRADE_ACCOUNTS_REQUEST: u16 = 400;
pub const TRADE_ACCOUNT_RESPONSE: u16 = 401;
pub const ACCOUNT_BALANCE_UPDATE: u16 = 600;
pub const ACCOUNT_BALANCE_REQUEST: u16 = 601;
pub const ACCOUNT_BALANCE_REJECT: u16 = 602;

const ENCODING_MESSAGE_SIZE: usize = 16;
const MIN_LOGON_REQUEST_SIZE: usize = 148;
const LOGON_RESPONSE_SIZE: usize = 256;
const HEARTBEAT_SIZE: usize = 16;
const LOGOFF_SIZE: usize = 102;
const MARKET_DATA_FEED_STATUS_SIZE: usize = 8;
const MARKET_DATA_REQUEST_SIZE: usize = 96;
const MARKET_DATA_REJECT_SIZE: usize = 104;
const MARKET_DATA_SNAPSHOT_SIZE: usize = 144;
const EXCHANGE_LIST_REQUEST_SIZE: usize = 8;
const EXCHANGE_LIST_RESPONSE_SIZE: usize = 76;
const SYMBOLS_FOR_EXCHANGE_REQUEST_SIZE: usize = 96;
const UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST_SIZE: usize = 28;
const SYMBOLS_FOR_UNDERLYING_REQUEST_SIZE: usize = 60;
const SECURITY_DEFINITION_REQUEST_SIZE: usize = 88;
const SECURITY_DEFINITION_RESPONSE_SIZE: usize = 432;
const SECURITY_DEFINITION_REJECT_SIZE: usize = 104;
const SYMBOL_SEARCH_REQUEST_SIZE: usize = 96;
const MARKET_DEPTH_REQUEST_SIZE: usize = 96;
const MARKET_DEPTH_REJECT_SIZE: usize = 104;
const MARKET_DEPTH_SNAPSHOT_LEVEL_SIZE: usize = 56;
const MARKET_DEPTH_UPDATE_LEVEL_V2_SIZE: usize = 39;
const HISTORICAL_PRICE_DATA_REQUEST_SIZE: usize = 128;
const HISTORICAL_PRICE_DATA_RESPONSE_HEADER_SIZE: usize = 24;
const HISTORICAL_PRICE_DATA_REJECT_SIZE: usize = 108;
const HISTORICAL_PRICE_DATA_RECORD_RESPONSE_SIZE: usize = 88;
const HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE_SIZE: usize = 48;
const SUBMIT_NEW_SINGLE_ORDER_SIZE: usize = 304;
const CANCEL_REPLACE_ORDER_SIZE: usize = 192;
const CANCEL_ORDER_SIZE: usize = 100;
const OPEN_ORDERS_REQUEST_SIZE: usize = 76;
const ORDER_UPDATE_SIZE: usize = 720;
const TRADE_ACCOUNTS_REQUEST_SIZE: usize = 8;
const TRADE_ACCOUNT_RESPONSE_SIZE: usize = 52;
const CURRENT_POSITIONS_REQUEST_SIZE: usize = 40;
const POSITION_UPDATE_SIZE: usize = 240;
const ACCOUNT_BALANCE_REQUEST_SIZE: usize = 40;
const ACCOUNT_BALANCE_UPDATE_SIZE: usize = 416;
const TRADING_REJECT_SIZE: usize = 104;
const MAX_DEPTH_LEVELS: usize = u16::MAX as usize;
const LOGON_RESULT_TEXT: &str = "Logon successful";

const SUBSCRIBE: i32 = 1;
const UNSUBSCRIBE: i32 = 2;
const SNAPSHOT: i32 = 3;

pub use crate::market_gateway::{
    AccountBalance, CancelOrderRequest, HistoricalRecord, HistoricalRequest, HistoricalResponse,
    HistoryClientFactory, HistoryDataClient, Instrument, MarketClientFactory, MarketDataClient,
    MarketEvent, ModifyOrderRequest, NewOrderRequest, TradeAccount, TradingClientFactory,
    TradingDataClient, TradingEvent, TradingOrder, TradingPosition,
};
pub(crate) use crate::market_gateway::{MarketCommand, TradingCommand};

#[derive(Debug)]
pub enum SessionError {
    Io(io::Error),
    Protocol(String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Protocol(message) => write!(f, "DTC protocol error: {message}"),
        }
    }
}

impl Error for SessionError {}

impl From<io::Error> for SessionError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug)]
struct Frame {
    message_type: u16,
    bytes: Vec<u8>,
}

pub async fn serve(
    listener: TcpListener,
    instrument: Instrument,
    market_factory: MarketClientFactory,
    history_factory: HistoryClientFactory,
) -> io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true)?;
        println!("[DTC] Client connected: {peer}");
        let instrument = instrument.clone();
        let market = market_factory();
        let history = history_factory();
        tokio::spawn(async move {
            if let Err(error) = handle_connection_with_all_services(
                stream,
                instrument,
                Some(market),
                Some(history),
                None,
            )
            .await
            {
                eprintln!("[DTC] Session {peer} ended: {error}");
            } else {
                println!("[DTC] Client disconnected: {peer}");
            }
        });
    }
}

pub async fn serve_with_trading(
    listener: TcpListener,
    instrument: Instrument,
    market_factory: MarketClientFactory,
    history_factory: HistoryClientFactory,
    trading_factory: TradingClientFactory,
) -> io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true)?;
        println!("[DTC] Client connected: {peer}");
        let instrument = instrument.clone();
        let market = market_factory();
        let history = history_factory();
        let trading = trading_factory();
        tokio::spawn(async move {
            if let Err(error) = handle_connection_with_all_services(
                stream,
                instrument,
                Some(market),
                Some(history),
                Some(trading),
            )
            .await
            {
                eprintln!("[DTC] Session {peer} ended: {error}");
            } else {
                println!("[DTC] Client disconnected: {peer}");
            }
        });
    }
}

pub async fn handle_connection(stream: TcpStream) -> Result<(), SessionError> {
    let instrument = Instrument::es("ESU6", "CME").expect("static instrument is valid");
    handle_connection_with_all_services(stream, instrument, None, None, None).await
}

pub async fn handle_connection_with_market(
    stream: TcpStream,
    instrument: Instrument,
    market: Option<MarketDataClient>,
) -> Result<(), SessionError> {
    handle_connection_with_all_services(stream, instrument, market, None, None).await
}

pub async fn handle_connection_with_services(
    stream: TcpStream,
    instrument: Instrument,
    market: Option<MarketDataClient>,
    history: Option<HistoryDataClient>,
) -> Result<(), SessionError> {
    handle_connection_with_all_services(stream, instrument, market, history, None).await
}

pub async fn handle_connection_with_all_services(
    mut stream: TcpStream,
    instrument: Instrument,
    market: Option<MarketDataClient>,
    history: Option<HistoryDataClient>,
    trading: Option<TradingDataClient>,
) -> Result<(), SessionError> {
    let market_is_supported = market.is_some();
    let history_is_supported = history.is_some();
    let trading_is_supported = trading.is_some();
    let heartbeat_interval = loop {
        let frame = read_frame(&mut stream)
            .await?
            .ok_or_else(|| SessionError::Protocol("connection closed before logon".to_owned()))?;

        match frame.message_type {
            ENCODING_REQUEST => {
                validate_encoding_request(&frame.bytes)?;
                stream.write_all(&encoding_response()).await?;
            }
            LOGON_REQUEST => {
                let heartbeat_interval = match parse_heartbeat_interval(&frame.bytes) {
                    Ok(interval) => interval,
                    Err(error) => {
                        let mut response = logon_response(false, false, false);
                        put_i32(&mut response, 8, 3); // LOGON_ERROR_NO_RECONNECT
                        put_fixed_string(&mut response[12..108], &error.to_string());
                        stream.write_all(&response).await?;
                        return Ok(());
                    }
                };
                stream
                    .write_all(&logon_response(
                        market_is_supported,
                        history_is_supported,
                        trading_is_supported,
                    ))
                    .await?;
                break heartbeat_interval;
            }
            other => {
                return Err(SessionError::Protocol(format!(
                    "expected ENCODING_REQUEST or LOGON_REQUEST, received type {other}"
                )));
            }
        }
    };

    let (market_commands, mut market_events, publish_catalog_at_logon) = match market {
        Some(market) => (
            Some(market.commands),
            Some(market.events),
            market.publish_catalog_at_logon,
        ),
        None => (None, None, false),
    };
    let history_commands = history.map(|history| history.commands);
    let (trading_commands, mut trading_events) = match trading {
        Some(trading) => (Some(trading.commands), Some(trading.events)),
        None => (None, None),
    };
    let (history_wire_tx, mut history_wire_rx) = mpsc::channel::<HistoryWireMessage>(64);
    let mut history_request_in_progress = false;
    let mut heartbeat = time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let peer_timeout = heartbeat_timeout(heartbeat_interval);
    let peer_silence = time::sleep(peer_timeout);
    tokio::pin!(peer_silence);

    let (mut reader, mut writer) = stream.into_split();
    let mut frames = FrameReader::default();
    let mut pending_frames = VecDeque::new();
    let mut has_streaming_requests = false;
    let mut historical_connection = false;
    let mut published_catalog = Vec::new();
    let mut published_catalog_keys = HashSet::new();
    if publish_catalog_at_logon {
        let mut catalog = pump_request(
            catalog_load(market_commands.as_ref(), &instrument.underlying_symbol),
            &mut reader,
            &mut frames,
            &mut pending_frames,
            &mut writer,
            &mut heartbeat,
            &mut peer_silence,
            peer_timeout,
        )
        .await?
        .unwrap_or_else(|error| {
            eprintln!("[DTC] Rithmic catalog preload failed: {error}");
            Vec::new()
        });
        if !catalog
            .iter()
            .any(|entry| instrument.matches(&entry.symbol, &entry.exchange))
        {
            catalog.push(instrument.clone());
        }
        catalog.sort_by(|left, right| {
            left.exchange
                .cmp(&right.exchange)
                .then_with(|| left.underlying_symbol.cmp(&right.underlying_symbol))
                .then_with(|| left.symbol.cmp(&right.symbol))
        });
        catalog.dedup_by(|left, right| {
            left.symbol.eq_ignore_ascii_case(&right.symbol)
                && left.exchange.eq_ignore_ascii_case(&right.exchange)
        });
        eprintln!(
            "[DTC] Publishing {} Rithmic futures definitions to Sierra Symbol Settings",
            catalog.len()
        );
        published_catalog_keys.extend(catalog.iter().map(|entry| {
            (
                entry.symbol.to_ascii_lowercase(),
                entry.exchange.to_ascii_lowercase(),
            )
        }));
        published_catalog = security_definition_responses(0, &catalog);
        for response in &published_catalog {
            writer.write_all(&response).await?;
        }
        eprintln!("[DTC] Finished publishing Symbol Settings catalog");
    }

    loop {
        tokio::select! {
            frame = next_frame(&mut reader, &mut frames, &mut pending_frames) => {
                match frame? {
                    Some(frame) => {
                        // The DTC specification treats any received message as proof that the
                        // peer is alive, not only an explicit HEARTBEAT.
                        peer_silence.as_mut().reset(time::Instant::now() + peer_timeout);
                        if frame.message_type == LOGOFF {
                            return Ok(());
                        }
                        if matches!(frame.message_type, MARKET_DATA_REQUEST | MARKET_DEPTH_REQUEST) || is_trading_message(frame.message_type) {
                            has_streaming_requests = true;
                            historical_connection = false;
                        }
                        if is_symbol_discovery_request(frame.message_type) {
                            let request_id = if frame.bytes.len() >= 8 {
                                read_i32(&frame.bytes, 4)
                            } else {
                                0
                            };
                            eprintln!(
                                "[DTC] Symbol discovery request: {} ({}), RequestID={request_id}",
                                symbol_discovery_message_name(frame.message_type),
                                frame.message_type,
                            );
                            let responses = pump_request(handle_symbol_discovery_request(
                                frame.message_type,
                                &frame.bytes,
                                &instrument,
                                market_commands.as_ref(),
                            ), &mut reader, &mut frames, &mut pending_frames, &mut writer, &mut heartbeat, &mut peer_silence, peer_timeout).await??;
                            eprintln!(
                                "[DTC] Symbol discovery response: RequestID={request_id}, messages={}",
                                responses.len(),
                            );
                            let catalog_changed = frame.message_type
                                == SECURITY_DEFINITION_FOR_SYMBOL_REQUEST
                                && publish_catalog_at_logon
                                && responses.first().is_some_and(|response| {
                                    add_security_definition_to_catalog(
                                        response,
                                        &mut published_catalog,
                                        &mut published_catalog_keys,
                                    )
                                });
                            for response in responses {
                                writer.write_all(&response).await?;
                            }
                            if catalog_changed {
                                eprintln!(
                                    "[DTC] Republishing {} cumulative futures definitions",
                                    published_catalog.len()
                                );
                                for response in &published_catalog {
                                    writer.write_all(response).await?;
                                }
                            }
                        } else if frame.message_type == MARKET_DATA_REQUEST {
                            let response = pump_request(handle_market_data_request(
                                &frame.bytes,
                                &instrument,
                                market_commands.as_ref(),
                            ), &mut reader, &mut frames, &mut pending_frames, &mut writer, &mut heartbeat, &mut peer_silence, peer_timeout).await??;
                            if let Some(response) = response {
                                writer.write_all(&response).await?;
                            }
                        } else if frame.message_type == MARKET_DEPTH_REQUEST {
                            let responses = pump_request(handle_market_depth_request(
                                &frame.bytes,
                                &instrument,
                                market_commands.as_ref(),
                            ), &mut reader, &mut frames, &mut pending_frames, &mut writer, &mut heartbeat, &mut peer_silence, peer_timeout).await??;
                            for response in responses {
                                writer.write_all(&response).await?;
                            }
                        } else if frame.message_type == HISTORICAL_PRICE_DATA_REQUEST {
                            historical_connection = !has_streaming_requests;
                            if history_request_in_progress {
                                let request_id = if frame.bytes.len() >= 8 {
                                    read_i32(&frame.bytes, 4)
                                } else {
                                    0
                                };
                                writer
                                    .write_all(&historical_reject(
                                        request_id,
                                        "Only one historical request can be active at a time",
                                    ))
                                    .await?;
                            } else {
                                history_request_in_progress = true;
                                pump_request(start_historical_request(
                                    &frame.bytes,
                                    history_commands.as_ref(),
                                    history_wire_tx.clone(),
                                ), &mut reader, &mut frames, &mut pending_frames, &mut writer, &mut heartbeat, &mut peer_silence, peer_timeout).await??;
                            }
                        } else if is_trading_message(frame.message_type) {
                            eprintln!(
                                "[DTC] Trading request: type={}, bytes={}",
                                frame.message_type,
                                frame.bytes.len(),
                            );
                            let responses = pump_request(handle_trading_request(
                                frame.message_type,
                                &frame.bytes,
                                &instrument,
                                market_commands.as_ref(),
                                trading_commands.as_ref(),
                            ), &mut reader, &mut frames, &mut pending_frames, &mut writer, &mut heartbeat, &mut peer_silence, peer_timeout).await??;
                            eprintln!(
                                "[DTC] Trading response: request_type={}, messages={}",
                                frame.message_type,
                                responses.len(),
                            );
                            for response in responses {
                                writer.write_all(&response).await?;
                                if u16::from_le_bytes(response[2..4].try_into().unwrap()) == LOGOFF {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    None => return Ok(()),
                }
            }
            event = receive_market_event(&mut market_events), if market_events.is_some() => {
                match event {
                    Some(MarketEvent::FeedError(error)) => {
                        eprintln!("[DTC] Rithmic feed warning: {error}");
                    }
                    Some(event) => writer.write_all(&encode_market_event(event)).await?,
                    None => market_events = None,
                }
            }
            event = receive_trading_event(&mut trading_events), if trading_events.is_some() => {
                match event {
                    Some(TradingEvent::Error(error)) => {
                        eprintln!("[DTC] Rithmic trading warning: {error}");
                    }
                    Some(event) => writer.write_all(&encode_trading_event(event)).await?,
                    None => trading_events = None,
                }
            }
            _ = heartbeat.tick() => {
                writer.write_all(&heartbeat_message()).await?;
            }
            history_message = history_wire_rx.recv(), if history_request_in_progress => {
                if let Some(history_message) = history_message {
                    writer.write_all(&history_message.bytes).await?;
                    if historical_connection {
                        peer_silence.as_mut().reset(time::Instant::now() + peer_timeout.max(Duration::from_secs(30)));
                    }
                    if history_message.is_final {
                        // IsFinalRecord (or NoRecordsToReturn in the response header) is the
                        // protocol completion signal. Sierra 2945 treats a server LOGOFF as a
                        // connection error and a server EOF as a canceled download, so keep the
                        // socket alive and accept its next sequential history request.
                        history_request_in_progress = false;
                    }
                }
            }
            _ = &mut peer_silence, if !(historical_connection && history_request_in_progress) => {
                writer
                    .write_all(&logoff_message("Client heartbeat timeout", false))
                    .await?;
                writer.shutdown().await?;
                return Ok(());
            }
        }
    }
}

fn heartbeat_timeout(interval: Duration) -> Duration {
    interval.saturating_mul(2)
}

async fn next_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    frames: &mut FrameReader,
    pending: &mut VecDeque<Frame>,
) -> Result<Option<Frame>, SessionError> {
    if let Some(frame) = pending.pop_front() {
        return Ok(Some(frame));
    }
    frames.read(reader).await
}

/// Maintain transport liveness while a sequential business request is pending.
/// Do not reorder new business requests, or retry a possibly submitted order.
#[allow(clippy::too_many_arguments)]
async fn pump_request<F: std::future::Future>(
    request: F,
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    frames: &mut FrameReader,
    pending: &mut VecDeque<Frame>,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    heartbeat: &mut time::Interval,
    peer_silence: &mut std::pin::Pin<&mut time::Sleep>,
    peer_timeout: Duration,
) -> Result<F::Output, SessionError> {
    tokio::pin!(request);
    loop {
        tokio::select! {
            result = &mut request => return Ok(result),
            _ = heartbeat.tick() => writer.write_all(&heartbeat_message()).await?,
            frame = frames.read(reader) => {
                let Some(frame) = frame? else { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Client disconnected").into()); };
                peer_silence.as_mut().reset(time::Instant::now() + peer_timeout);
                if frame.message_type == LOGOFF {
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "Client logged off").into());
                }
                if frame.message_type != HEARTBEAT {
                    if pending.len() >= 64 { return Err(SessionError::Protocol("Too many queued requests".to_owned())); }
                    pending.push_back(frame);
                }
            }
            _ = peer_silence.as_mut() => {
                writer.write_all(&logoff_message("Client heartbeat timeout", false)).await?;
                return Err(io::Error::new(io::ErrorKind::TimedOut, "Client heartbeat timeout").into());
            }
        }
    }
}

struct HistoryWireMessage {
    bytes: Vec<u8>,
    is_final: bool,
}

async fn start_historical_request(
    bytes: &[u8],
    commands: Option<
        &mpsc::Sender<(
            HistoricalRequest,
            mpsc::Sender<Result<HistoricalResponse, String>>,
        )>,
    >,
    output: mpsc::Sender<HistoryWireMessage>,
) -> Result<(), SessionError> {
    if bytes.len() < HISTORICAL_PRICE_DATA_REQUEST_SIZE {
        return Err(SessionError::Protocol(format!(
            "HISTORICAL_PRICE_DATA_REQUEST is {} bytes; expected at least {HISTORICAL_PRICE_DATA_REQUEST_SIZE}",
            bytes.len()
        )));
    }
    let request_id = read_i32(bytes, 4);
    let requested_symbol = read_fixed_string(&bytes[8..72])?;
    let requested_exchange = read_fixed_string(&bytes[72..88])?;
    let (symbol, exchange) = normalize_symbol_exchange(&requested_symbol, &requested_exchange);
    let request = HistoricalRequest {
        request_id,
        symbol: symbol.clone(),
        exchange: exchange.clone(),
        record_interval: read_i32(bytes, 88),
        start_time: read_i64(bytes, 96),
        end_time: read_i64(bytes, 104),
        max_days: read_u32(bytes, 112),
        tick_bar_length: 0,
    };
    eprintln!(
        "[DTC] Historical request: RequestID={}, {}.{}, interval={}s, start={}, end={}, max_days={}",
        request.request_id,
        request.symbol,
        request.exchange,
        request.record_interval,
        request.start_time,
        request.end_time,
        request.max_days
    );

    if symbol.is_empty() || exchange.is_empty() {
        output
            .send(HistoryWireMessage {
                bytes: historical_reject(
                    request_id,
                    "Both symbol and exchange are required for historical data",
                ),
                is_final: true,
            })
            .await
            .ok();
        return Ok(());
    }
    let Some(commands) = commands else {
        output
            .send(HistoryWireMessage {
                bytes: historical_reject(request_id, "Rithmic historical data is unavailable"),
                is_final: true,
            })
            .await
            .ok();
        return Ok(());
    };

    let (response_tx, mut response_rx) = mpsc::channel(2);
    commands
        .send((request, response_tx))
        .await
        .map_err(|_| SessionError::Protocol("Rithmic history worker stopped".to_owned()))?;
    tokio::spawn(async move {
        let mut send_header = true;
        let mut pending_record = None;
        while let Some(result) = response_rx.recv().await {
            match result {
                Ok(mut response) => {
                    let is_final = response.is_final;
                    // Retain one real record so an empty final batch can mark
                    // that record final without inventing a trade or second header.
                    if let Some(record) = pending_record.take() {
                        response.records.insert(0, record);
                    }
                    if !is_final {
                        pending_record = response.records.pop();
                        if response.records.is_empty() {
                            continue;
                        }
                    }
                    eprintln!(
                        "[DTC] Historical response chunk: RequestID={}, interval={}s, records={}, final={}",
                        response.request_id,
                        response.record_interval,
                        response.records.len(),
                        is_final
                    );
                    stream_historical_response(response, &output, send_header).await;
                    send_header = false;
                    if is_final {
                        return;
                    }
                }
                Err(error) => {
                    eprintln!(
                        "[DTC] Historical request rejected: RequestID={request_id}, reason={error}"
                    );
                    let _ = output
                        .send(HistoryWireMessage {
                            bytes: historical_reject(request_id, &error),
                            is_final: true,
                        })
                        .await;
                    return;
                }
            }
        }
        let _ = output
            .send(HistoryWireMessage {
                bytes: historical_reject(
                    request_id,
                    "History worker ended before the final response",
                ),
                is_final: true,
            })
            .await;
    });
    Ok(())
}

fn normalize_symbol_exchange(symbol: &str, exchange: &str) -> (String, String) {
    if !exchange.is_empty() {
        return (symbol.to_owned(), exchange.to_owned());
    }
    symbol
        .rsplit_once(['-', '.'])
        .map(|(symbol, exchange)| (symbol.to_owned(), exchange.to_owned()))
        .unwrap_or_else(|| (symbol.to_owned(), exchange.to_owned()))
}

async fn stream_historical_response(
    response: HistoricalResponse,
    output: &mpsc::Sender<HistoryWireMessage>,
    send_header: bool,
) {
    let no_records = response.records.is_empty() && response.is_final;
    if send_header
        && output
            .send(HistoryWireMessage {
                bytes: historical_response_header(
                    response.request_id,
                    response.record_interval,
                    no_records,
                ),
                is_final: no_records,
            })
            .await
            .is_err()
    {
        return;
    }
    if response.records.is_empty() {
        return;
    }
    let count = response.records.len();
    for (index, record) in response.records.into_iter().enumerate() {
        let is_final = response.is_final && index + 1 == count;
        let bytes = match record {
            HistoricalRecord::Tick {
                datetime_us,
                price,
                volume,
                at_bid_or_ask,
            } => historical_tick_record(
                response.request_id,
                datetime_us,
                price,
                volume,
                at_bid_or_ask,
                is_final,
            ),
            HistoricalRecord::Bar {
                start_datetime_us,
                open,
                high,
                low,
                close,
                volume,
                num_trades,
                bid_volume,
                ask_volume,
            } => historical_bar_record(
                response.request_id,
                start_datetime_us,
                open,
                high,
                low,
                close,
                volume,
                num_trades,
                bid_volume,
                ask_volume,
                is_final,
            ),
        };
        if output
            .send(HistoryWireMessage { bytes, is_final })
            .await
            .is_err()
        {
            return;
        }
    }
}

fn historical_response_header(request_id: i32, interval: i32, no_records: bool) -> Vec<u8> {
    let mut message = vec![0_u8; HISTORICAL_PRICE_DATA_RESPONSE_HEADER_SIZE];
    put_u16(
        &mut message,
        0,
        HISTORICAL_PRICE_DATA_RESPONSE_HEADER_SIZE as u16,
    );
    put_u16(&mut message, 2, HISTORICAL_PRICE_DATA_RESPONSE_HEADER);
    put_i32(&mut message, 4, request_id);
    put_i32(&mut message, 8, interval);
    message[12] = 0;
    message[13] = u8::from(no_records);
    put_f32(&mut message, 16, 0.0);
    message
}

fn historical_reject(request_id: i32, reason: &str) -> Vec<u8> {
    let mut message = vec![0_u8; HISTORICAL_PRICE_DATA_REJECT_SIZE];
    put_u16(&mut message, 0, HISTORICAL_PRICE_DATA_REJECT_SIZE as u16);
    put_u16(&mut message, 2, HISTORICAL_PRICE_DATA_REJECT);
    put_i32(&mut message, 4, request_id);
    put_fixed_string(&mut message[8..104], reason);
    put_i16(&mut message, 104, 4); // HPDR_GENERAL_REJECT_ERROR
    message
}

#[allow(clippy::too_many_arguments)]
fn historical_bar_record(
    request_id: i32,
    datetime_us: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    num_trades: u32,
    bid_volume: f64,
    ask_volume: f64,
    is_final: bool,
) -> Vec<u8> {
    let mut message = vec![0_u8; HISTORICAL_PRICE_DATA_RECORD_RESPONSE_SIZE];
    put_u16(
        &mut message,
        0,
        HISTORICAL_PRICE_DATA_RECORD_RESPONSE_SIZE as u16,
    );
    put_u16(&mut message, 2, HISTORICAL_PRICE_DATA_RECORD_RESPONSE);
    put_i32(&mut message, 4, request_id);
    put_i64(&mut message, 8, datetime_us);
    put_f64(&mut message, 16, open);
    put_f64(&mut message, 24, high);
    put_f64(&mut message, 32, low);
    put_f64(&mut message, 40, close);
    put_f64(&mut message, 48, volume);
    put_u32(&mut message, 56, num_trades);
    put_f64(&mut message, 64, bid_volume);
    put_f64(&mut message, 72, ask_volume);
    message[80] = u8::from(is_final);
    message
}

fn historical_tick_record(
    request_id: i32,
    datetime_us: i64,
    price: f64,
    volume: f64,
    at_bid_or_ask: u16,
    is_final: bool,
) -> Vec<u8> {
    let mut message = vec![0_u8; HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE_SIZE];
    put_u16(
        &mut message,
        0,
        HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE_SIZE as u16,
    );
    put_u16(&mut message, 2, HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE);
    put_i32(&mut message, 4, request_id);
    put_f64(&mut message, 8, datetime_us as f64 / 1_000_000.0);
    put_u16(&mut message, 16, at_bid_or_ask);
    put_f64(&mut message, 24, price);
    put_f64(&mut message, 32, volume);
    message[40] = u8::from(is_final);
    message
}

fn encode_trading_event(event: TradingEvent) -> Vec<u8> {
    match event {
        TradingEvent::Order(order) => encode_order_update(&order, 0, 1, false),
        TradingEvent::Position(position) => position_update(0, &position, 0, 1, true),
        TradingEvent::Balance(balance) => account_balance_update(0, &balance, true),
        TradingEvent::Error(error) => trading_reject(OPEN_ORDERS_REJECT, 0, &error),
    }
}

fn trade_account_response(
    request_id: i32,
    account: &TradeAccount,
    index: usize,
    count: usize,
) -> Vec<u8> {
    let mut message = vec![0_u8; TRADE_ACCOUNT_RESPONSE_SIZE];
    put_u16(&mut message, 0, TRADE_ACCOUNT_RESPONSE_SIZE as u16);
    put_u16(&mut message, 2, TRADE_ACCOUNT_RESPONSE);
    put_i32(&mut message, 4, count as i32);
    put_i32(&mut message, 8, index as i32 + 1);
    put_fixed_string(&mut message[12..44], &account.account_id);
    put_i32(&mut message, 44, request_id);
    put_i32(&mut message, 48, i32::from(account.trading_disabled));
    message
}

fn empty_trade_accounts(request_id: i32) -> Vec<u8> {
    trade_account_response(
        request_id,
        &TradeAccount {
            account_id: String::new(),
            currency: String::new(),
            trading_disabled: true,
        },
        0,
        1,
    )
}

fn encode_order_update(
    order: &TradingOrder,
    index: usize,
    count: usize,
    no_orders: bool,
) -> Vec<u8> {
    let mut message = vec![0_u8; ORDER_UPDATE_SIZE];
    put_u16(&mut message, 0, ORDER_UPDATE_SIZE as u16);
    put_u16(&mut message, 2, ORDER_UPDATE);
    put_i32(&mut message, 4, order.request_id);
    put_i32(&mut message, 8, count as i32);
    put_i32(&mut message, 12, index as i32 + 1);
    put_fixed_string(&mut message[16..80], &order.symbol);
    put_fixed_string(&mut message[80..96], &order.exchange);
    put_fixed_string(&mut message[128..160], &order.server_order_id);
    put_fixed_string(&mut message[160..192], &order.client_order_id);
    put_fixed_string(&mut message[192..224], &order.exchange_order_id);
    put_i32(&mut message, 224, order.order_status);
    put_i32(&mut message, 228, order.update_reason);
    put_i32(&mut message, 232, order.order_type);
    put_i32(&mut message, 236, order.buy_sell);
    put_f64(&mut message, 240, order.price1);
    put_f64(&mut message, 248, order.price2);
    put_i32(&mut message, 256, order.time_in_force);
    put_f64(&mut message, 272, order.quantity);
    put_f64(&mut message, 280, order.filled_quantity);
    put_f64(&mut message, 288, order.remaining_quantity);
    put_f64(&mut message, 296, order.average_fill_price);
    put_f64(&mut message, 304, order.last_fill_price);
    put_i64(&mut message, 312, order.last_fill_datetime_ms);
    put_f64(&mut message, 320, order.last_fill_quantity);
    put_fixed_string(&mut message[328..392], &order.last_fill_execution_id);
    put_fixed_string(&mut message[392..424], &order.account_id);
    put_fixed_string(&mut message[424..520], &order.info_text);
    message[520] = u8::from(no_orders);
    message
}

fn no_orders_update(request_id: i32) -> Vec<u8> {
    let order = TradingOrder {
        request_id,
        symbol: String::new(),
        exchange: String::new(),
        account_id: String::new(),
        client_order_id: String::new(),
        server_order_id: String::new(),
        exchange_order_id: String::new(),
        order_status: 0,
        update_reason: 1,
        order_type: 0,
        buy_sell: 0,
        price1: f64::MAX,
        price2: f64::MAX,
        quantity: f64::MAX,
        filled_quantity: f64::MAX,
        remaining_quantity: f64::MAX,
        average_fill_price: f64::MAX,
        last_fill_price: f64::MAX,
        last_fill_quantity: f64::MAX,
        last_fill_datetime_ms: 0,
        last_fill_execution_id: String::new(),
        info_text: String::new(),
        time_in_force: 0,
        is_snapshot: true,
    };
    encode_order_update(&order, 0, 1, true)
}

fn order_rejection(client_id: &str, account_id: &str, reason: &str) -> Vec<u8> {
    order_action_rejection("", client_id, account_id, 8, reason)
}

fn new_order_rejection(request: &NewOrderRequest, reason: &str) -> Vec<u8> {
    let mut message = order_rejection(&request.client_order_id, &request.account_id, reason);
    put_fixed_string(&mut message[16..80], &request.symbol);
    put_fixed_string(&mut message[80..96], &request.exchange);
    message
}

async fn reject_order_action(
    commands: Option<&mpsc::Sender<TradingCommand>>,
    server_id: &str,
    client_id: &str,
    account_id: &str,
    reason: i32,
    text: &str,
) -> Vec<u8> {
    let state = if let Some(commands) = commands {
        let (tx, rx) = oneshot::channel();
        if commands
            .send(TradingCommand::OrderState(server_id.to_owned(), tx))
            .await
            .is_ok()
        {
            rx.await.ok().and_then(Result::ok)
        } else {
            None
        }
    } else {
        None
    };
    let mut message = order_action_rejection(server_id, client_id, account_id, reason, text);
    match state {
        Some(Some(order)) if account_id.is_empty() || order.account_id == account_id => {
            message = encode_order_update(&order, 0, 1, false);
            put_i32(&mut message, 4, 0);
            put_i32(&mut message, 228, reason);
            put_fixed_string(&mut message[160..192], client_id);
            put_fixed_string(&mut message[424..520], text);
            // A rejected operation is not a new execution.
            put_f64(&mut message, 304, f64::MAX);
            put_i64(&mut message, 312, 0);
            put_f64(&mut message, 320, f64::MAX);
            message[328..392].fill(0);
        }
        Some(None) => {
            message[128..160].fill(0);
        }
        _ => {
            put_i32(&mut message, 224, 0);
        } // state unavailable, not a rejected order
    }
    message
}

fn order_action_rejection(
    server_id: &str,
    client_id: &str,
    account_id: &str,
    reason: i32,
    text: &str,
) -> Vec<u8> {
    let mut order = TradingOrder {
        request_id: 0,
        symbol: String::new(),
        exchange: String::new(),
        account_id: account_id.to_owned(),
        client_order_id: client_id.to_owned(),
        server_order_id: server_id.to_owned(),
        exchange_order_id: String::new(),
        order_status: 9,
        update_reason: reason,
        order_type: 0,
        buy_sell: 0,
        price1: f64::MAX,
        price2: f64::MAX,
        quantity: f64::MAX,
        filled_quantity: f64::MAX,
        remaining_quantity: f64::MAX,
        average_fill_price: f64::MAX,
        last_fill_price: f64::MAX,
        last_fill_quantity: f64::MAX,
        last_fill_datetime_ms: 0,
        last_fill_execution_id: String::new(),
        info_text: text.to_owned(),
        time_in_force: 0,
        is_snapshot: false,
    };
    encode_order_update(&mut order, 0, 1, false)
}

fn position_update(
    request_id: i32,
    position: &TradingPosition,
    index: usize,
    count: usize,
    unsolicited: bool,
) -> Vec<u8> {
    let mut message = vec![0_u8; POSITION_UPDATE_SIZE];
    put_u16(&mut message, 0, POSITION_UPDATE_SIZE as u16);
    put_u16(&mut message, 2, POSITION_UPDATE);
    put_i32(&mut message, 4, request_id);
    put_i32(&mut message, 8, count as i32);
    put_i32(&mut message, 12, index as i32 + 1);
    put_fixed_string(&mut message[16..80], &position.symbol);
    put_fixed_string(&mut message[80..96], &position.exchange);
    put_f64(&mut message, 96, position.quantity);
    put_f64(&mut message, 104, position.average_price);
    put_fixed_string(
        &mut message[112..144],
        &format!("{}.{}", position.symbol, position.exchange),
    );
    put_fixed_string(&mut message[144..176], &position.account_id);
    message[177] = u8::from(unsolicited);
    put_f64(&mut message, 200, position.open_profit_loss);
    message
}

fn no_positions_update(request_id: i32) -> Vec<u8> {
    let mut message = vec![0_u8; POSITION_UPDATE_SIZE];
    put_u16(&mut message, 0, POSITION_UPDATE_SIZE as u16);
    put_u16(&mut message, 2, POSITION_UPDATE);
    put_i32(&mut message, 4, request_id);
    put_i32(&mut message, 8, 1);
    put_i32(&mut message, 12, 1);
    message[176] = 1;
    message
}

fn account_balance_update(request_id: i32, balance: &AccountBalance, unsolicited: bool) -> Vec<u8> {
    let mut message = vec![0_u8; ACCOUNT_BALANCE_UPDATE_SIZE];
    put_u16(&mut message, 0, ACCOUNT_BALANCE_UPDATE_SIZE as u16);
    put_u16(&mut message, 2, ACCOUNT_BALANCE_UPDATE);
    put_i32(&mut message, 4, request_id);
    put_f64(&mut message, 8, balance.cash_balance);
    put_f64(&mut message, 16, balance.available_funds);
    put_fixed_string(&mut message[24..32], &balance.currency);
    put_fixed_string(&mut message[32..64], &balance.account_id);
    put_i32(&mut message, 80, 1);
    put_i32(&mut message, 84, 1);
    message[89] = u8::from(unsolicited);
    put_f64(&mut message, 96, balance.open_profit_loss);
    put_f64(&mut message, 104, balance.daily_profit_loss);
    message[235] = u8::from(balance.trading_disabled);
    message
}

fn trading_reject(message_type: u16, request_id: i32, reason: &str) -> Vec<u8> {
    if message_type == ORDER_UPDATE {
        return order_rejection("", "", reason);
    }
    let mut message = vec![0_u8; TRADING_REJECT_SIZE];
    put_u16(&mut message, 0, TRADING_REJECT_SIZE as u16);
    put_u16(&mut message, 2, message_type);
    put_i32(&mut message, 4, request_id);
    put_fixed_string(&mut message[8..104], reason);
    message
}

async fn read_frame<R>(reader: &mut R) -> Result<Option<Frame>, SessionError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0_u8; 4];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }

    let size = u16::from_le_bytes([header[0], header[1]]) as usize;
    let message_type = u16::from_le_bytes([header[2], header[3]]);
    if size < header.len() {
        return Err(SessionError::Protocol(format!(
            "message type {message_type} declared invalid size {size}"
        )));
    }

    let mut bytes = vec![0_u8; size];
    bytes[..4].copy_from_slice(&header);
    reader.read_exact(&mut bytes[4..]).await?;
    Ok(Some(decode_frame(message_type, bytes)?))
}

/// A read may be canceled by select! after consuming bytes. Keep all consumed
/// bytes outside the future so the next poll resumes the same frame.
#[derive(Default)]
struct FrameReader {
    bytes: Vec<u8>,
}

impl FrameReader {
    async fn read<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<Frame>, SessionError> {
        loop {
            let target = if self.bytes.len() < 4 {
                4
            } else {
                let size = u16::from_le_bytes([self.bytes[0], self.bytes[1]]) as usize;
                if size < 4 {
                    return Err(SessionError::Protocol("invalid DTC frame size".to_owned()));
                }
                size
            };
            if self.bytes.len() == target && target >= 4 {
                let kind = u16::from_le_bytes([self.bytes[2], self.bytes[3]]);
                let size = u16::from_le_bytes([self.bytes[0], self.bytes[1]]) as usize;
                if size == target {
                    return decode_frame(kind, std::mem::take(&mut self.bytes)).map(Some);
                }
                continue;
            }
            let mut chunk = [0_u8; 4096];
            let count = (target - self.bytes.len()).min(chunk.len());
            let count = reader.read(&mut chunk[..count]).await?;
            if count == 0 {
                if self.bytes.is_empty() {
                    return Ok(None);
                }
                return Err(
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated DTC frame").into(),
                );
            }
            self.bytes.extend_from_slice(&chunk[..count]);
        }
    }
}

fn decode_frame(message_type: u16, mut bytes: Vec<u8>) -> Result<Frame, SessionError> {
    // Only absent trailing fields are defaulted. Required prefixes must be
    // present, and the wire Size is retained for diagnostics/versioning.
    let layout = match message_type {
        MARKET_DATA_REQUEST => Some((92, MARKET_DATA_REQUEST_SIZE)),
        MARKET_DEPTH_REQUEST => Some((92, MARKET_DEPTH_REQUEST_SIZE)),
        HISTORICAL_PRICE_DATA_REQUEST => Some((112, HISTORICAL_PRICE_DATA_REQUEST_SIZE)),
        SUBMIT_NEW_SINGLE_ORDER => Some((188, SUBMIT_NEW_SINGLE_ORDER_SIZE)),
        CANCEL_REPLACE_ORDER => Some((98, CANCEL_REPLACE_ORDER_SIZE)),
        CANCEL_ORDER => Some((68, CANCEL_ORDER_SIZE)),
        OPEN_ORDERS_REQUEST => Some((44, OPEN_ORDERS_REQUEST_SIZE)),
        CURRENT_POSITIONS_REQUEST => Some((8, CURRENT_POSITIONS_REQUEST_SIZE)),
        ACCOUNT_BALANCE_REQUEST => Some((8, ACCOUNT_BALANCE_REQUEST_SIZE)),
        SYMBOLS_FOR_EXCHANGE_REQUEST => Some((28, SYMBOLS_FOR_EXCHANGE_REQUEST_SIZE)),
        _ => None,
    };
    if let Some((minimum, current)) = layout {
        if bytes.len() < minimum {
            return Err(SessionError::Protocol(format!(
                "message {message_type} requires {minimum} bytes, received {}",
                bytes.len()
            )));
        }
        let received = bytes.len();
        bytes.resize(current.max(received), 0);
        // A partially present field is absent, not a little-endian integer
        // whose missing high bytes happen to be zero.
        let tail_fields: &[(usize, usize)] = match message_type {
            MARKET_DATA_REQUEST | MARKET_DEPTH_REQUEST => &[(92, 4)],
            HISTORICAL_PRICE_DATA_REQUEST => &[(112, 4), (118, 2)],
            SUBMIT_NEW_SINGLE_ORDER => &[
                (192, 8),
                (202, 48),
                (252, 4),
                (256, 8),
                (264, 16),
                (280, 16),
                (296, 8),
            ],
            CANCEL_REPLACE_ORDER => &[
                (100, 4),
                (104, 4),
                (112, 8),
                (121, 32),
                (153, 16),
                (169, 16),
            ],
            CANCEL_ORDER => &[(68, 32)],
            OPEN_ORDERS_REQUEST => &[(44, 32)],
            CURRENT_POSITIONS_REQUEST | ACCOUNT_BALANCE_REQUEST => &[(8, 32)],
            SYMBOLS_FOR_EXCHANGE_REQUEST => &[(28, 4), (32, 64)],
            _ => &[],
        };
        for &(offset, size) in tail_fields {
            if received < offset + size {
                bytes[offset..offset + size].fill(0);
            }
        }
        if message_type == SYMBOLS_FOR_EXCHANGE_REQUEST && received < 32 {
            put_i32(&mut bytes, 28, SUBSCRIBE);
        }
    }
    Ok(Frame {
        message_type,
        bytes,
    })
}

fn validate_encoding_request(bytes: &[u8]) -> Result<(), SessionError> {
    if bytes.len() < ENCODING_MESSAGE_SIZE {
        return Err(SessionError::Protocol(format!(
            "ENCODING_REQUEST is {} bytes; expected at least {ENCODING_MESSAGE_SIZE}",
            bytes.len()
        )));
    }
    if &bytes[12..15] != b"DTC" {
        return Err(SessionError::Protocol(
            "ENCODING_REQUEST ProtocolType is not DTC".to_owned(),
        ));
    }
    Ok(())
}

fn parse_heartbeat_interval(bytes: &[u8]) -> Result<Duration, SessionError> {
    if bytes.len() < MIN_LOGON_REQUEST_SIZE {
        return Err(SessionError::Protocol(format!(
            "LOGON_REQUEST is {} bytes; HeartbeatIntervalInSeconds requires at least {MIN_LOGON_REQUEST_SIZE}",
            bytes.len()
        )));
    }
    let seconds = i32::from_le_bytes(bytes[144..148].try_into().expect("four-byte slice"));
    if !(5..=60).contains(&seconds) {
        return Err(SessionError::Protocol(
            "LOGON_REQUEST HeartbeatIntervalInSeconds must be between 5 and 60".to_owned(),
        ));
    }
    Ok(Duration::from_secs(seconds as u64))
}

fn encoding_response() -> [u8; ENCODING_MESSAGE_SIZE] {
    let mut message = [0_u8; ENCODING_MESSAGE_SIZE];
    put_u16(&mut message, 0, ENCODING_MESSAGE_SIZE as u16);
    put_u16(&mut message, 2, ENCODING_RESPONSE);
    put_i32(&mut message, 4, CURRENT_VERSION);
    put_i32(&mut message, 8, BINARY_ENCODING);
    message[12..15].copy_from_slice(b"DTC");
    message
}

fn logon_response(
    market_data_supported: bool,
    historical_data_supported: bool,
    trading_supported: bool,
) -> [u8; LOGON_RESPONSE_SIZE] {
    let mut message = [0_u8; LOGON_RESPONSE_SIZE];
    put_u16(&mut message, 0, LOGON_RESPONSE_SIZE as u16);
    put_u16(&mut message, 2, LOGON_RESPONSE);
    put_i32(&mut message, 4, CURRENT_VERSION);
    put_i32(&mut message, 8, 1); // LOGON_SUCCESS
    put_fixed_string(&mut message[12..108], LOGON_RESULT_TEXT);
    put_fixed_string(&mut message[176..236], "rithmic-dtc-bridge");
    put_fixed_string(&mut message[240..244], "-");

    message[244] = 1; // SecurityDefinitionsSupported
    message[245] = u8::from(historical_data_supported); // HistoricalPriceDataSupported
    message[247] = u8::from(market_data_supported); // MarketDepthIsSupported
    message[248] = 0; // Multiple sequential historical requests are supported per connection.
    message[252] = u8::from(market_data_supported); // MarketDataSupported
    message[237] = u8::from(trading_supported); // TradingIsSupported
    message[239] = u8::from(trading_supported); // OrderCancelReplaceSupported
    message
}

async fn receive_market_event(
    receiver: &mut Option<mpsc::Receiver<MarketEvent>>,
) -> Option<MarketEvent> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => None,
    }
}

async fn receive_trading_event(
    receiver: &mut Option<mpsc::Receiver<TradingEvent>>,
) -> Option<TradingEvent> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => None,
    }
}

fn is_trading_message(message_type: u16) -> bool {
    matches!(
        message_type,
        SUBMIT_NEW_SINGLE_ORDER
            | CANCEL_ORDER
            | CANCEL_REPLACE_ORDER
            | OPEN_ORDERS_REQUEST
            | CURRENT_POSITIONS_REQUEST
            | TRADE_ACCOUNTS_REQUEST
            | ACCOUNT_BALANCE_REQUEST
    )
}

async fn handle_trading_request(
    message_type: u16,
    bytes: &[u8],
    instrument: &Instrument,
    catalog: Option<&mpsc::Sender<MarketCommand>>,
    commands: Option<&mpsc::Sender<TradingCommand>>,
) -> Result<Vec<Vec<u8>>, SessionError> {
    let Some(commands) = commands else {
        let reason = "Paper trading service is disabled";
        if message_type == TRADE_ACCOUNTS_REQUEST {
            require_size(bytes, TRADE_ACCOUNTS_REQUEST_SIZE, "TRADE_ACCOUNTS_REQUEST")?;
            return Ok(vec![empty_trade_accounts(read_i32(bytes, 4))]);
        }
        if message_type == SUBMIT_NEW_SINGLE_ORDER {
            require_size(
                bytes,
                SUBMIT_NEW_SINGLE_ORDER_SIZE,
                "SUBMIT_NEW_SINGLE_ORDER",
            )?;
            return Ok(vec![new_order_rejection(&parse_new_order(bytes)?, reason)]);
        }
        if matches!(message_type, CANCEL_ORDER | CANCEL_REPLACE_ORDER) {
            require_size(
                bytes,
                if message_type == CANCEL_ORDER {
                    CANCEL_ORDER_SIZE
                } else {
                    CANCEL_REPLACE_ORDER_SIZE
                },
                "order action",
            )?;
            let account = if message_type == CANCEL_ORDER {
                &bytes[68..100]
            } else {
                &bytes[121..153]
            };
            return Ok(vec![
                reject_order_action(
                    None,
                    &read_fixed_string(&bytes[4..36])?,
                    &read_fixed_string(&bytes[36..68])?,
                    &read_fixed_string(account)?,
                    if message_type == CANCEL_ORDER { 9 } else { 10 },
                    reason,
                )
                .await,
            ]);
        }
        let request_id = if bytes.len() >= 8 {
            read_i32(bytes, 4)
        } else {
            0
        };
        return Ok(vec![trading_reject(
            rejection_type(message_type),
            request_id,
            "Paper trading service is disabled",
        )]);
    };
    match message_type {
        TRADE_ACCOUNTS_REQUEST => {
            require_size(bytes, TRADE_ACCOUNTS_REQUEST_SIZE, "TRADE_ACCOUNTS_REQUEST")?;
            let request_id = read_i32(bytes, 4);
            let (tx, rx) = oneshot::channel();
            commands
                .send(TradingCommand::Accounts(tx))
                .await
                .map_err(|_| trading_stopped())?;
            match rx.await.map_err(|_| trading_stopped())? {
                Ok(accounts) if accounts.is_empty() => Ok(vec![empty_trade_accounts(request_id)]),
                Ok(accounts) => Ok(accounts
                    .iter()
                    .enumerate()
                    .map(|(index, account)| {
                        trade_account_response(request_id, account, index, accounts.len())
                    })
                    .collect()),
                Err(error) => Ok(vec![
                    logoff_message(&format!("Trade account discovery failed: {error}"), false)
                        .to_vec(),
                ]),
            }
        }
        OPEN_ORDERS_REQUEST => {
            require_size(bytes, OPEN_ORDERS_REQUEST_SIZE, "OPEN_ORDERS_REQUEST")?;
            let request_id = read_i32(bytes, 4);
            let request_all = read_i32(bytes, 8) != 0;
            let requested_server_order_id = read_fixed_string(&bytes[12..44])?;
            let requested_account = read_fixed_string(&bytes[44..76])?;
            let (tx, rx) = oneshot::channel();
            commands
                .send(TradingCommand::OpenOrders(tx))
                .await
                .map_err(|_| trading_stopped())?;
            match rx.await.map_err(|_| trading_stopped())? {
                Ok(orders) => {
                    let orders: Vec<_> = orders
                        .into_iter()
                        .filter(|order| {
                            (requested_account.is_empty() || order.account_id == requested_account)
                                && (request_all
                                    || (!requested_server_order_id.is_empty()
                                        && order.server_order_id == requested_server_order_id))
                        })
                        .collect();
                    if orders.is_empty() {
                        return Ok(vec![no_orders_update(request_id)]);
                    }
                    let count = orders.len();
                    Ok(orders
                        .into_iter()
                        .enumerate()
                        .map(|(index, mut order)| {
                            order.request_id = request_id;
                            order.update_reason = 1;
                            encode_order_update(&order, index, count, false)
                        })
                        .collect())
                }
                Err(error) => Ok(vec![trading_reject(OPEN_ORDERS_REJECT, request_id, &error)]),
            }
        }
        CURRENT_POSITIONS_REQUEST => {
            require_size(
                bytes,
                CURRENT_POSITIONS_REQUEST_SIZE,
                "CURRENT_POSITIONS_REQUEST",
            )?;
            let request_id = read_i32(bytes, 4);
            let requested_account = read_fixed_string(&bytes[8..40])?;
            let (tx, rx) = oneshot::channel();
            commands
                .send(TradingCommand::Positions(tx))
                .await
                .map_err(|_| trading_stopped())?;
            match rx.await.map_err(|_| trading_stopped())? {
                Ok(positions) => {
                    let positions: Vec<_> = positions
                        .into_iter()
                        .filter(|position| {
                            requested_account.is_empty() || position.account_id == requested_account
                        })
                        .collect();
                    if positions.is_empty() {
                        return Ok(vec![no_positions_update(request_id)]);
                    }
                    let count = positions.len();
                    Ok(positions
                        .iter()
                        .enumerate()
                        .map(|(index, position)| {
                            position_update(request_id, position, index, count, false)
                        })
                        .collect())
                }
                Err(error) => Ok(vec![trading_reject(
                    CURRENT_POSITIONS_REJECT,
                    request_id,
                    &error,
                )]),
            }
        }
        ACCOUNT_BALANCE_REQUEST => {
            require_size(
                bytes,
                ACCOUNT_BALANCE_REQUEST_SIZE,
                "ACCOUNT_BALANCE_REQUEST",
            )?;
            let request_id = read_i32(bytes, 4);
            let requested_account = read_fixed_string(&bytes[8..40])?;
            let (tx, rx) = oneshot::channel();
            commands
                .send(TradingCommand::Balance(tx))
                .await
                .map_err(|_| trading_stopped())?;
            match rx.await.map_err(|_| trading_stopped())? {
                Ok(balance)
                    if requested_account.is_empty() || requested_account == balance.account_id =>
                {
                    Ok(vec![account_balance_update(request_id, &balance, false)])
                }
                Ok(_) => Ok(vec![trading_reject(
                    ACCOUNT_BALANCE_REJECT,
                    request_id,
                    "Unknown Paper trade account",
                )]),
                Err(error) => Ok(vec![trading_reject(
                    ACCOUNT_BALANCE_REJECT,
                    request_id,
                    &error,
                )]),
            }
        }
        SUBMIT_NEW_SINGLE_ORDER => {
            require_size(
                bytes,
                SUBMIT_NEW_SINGLE_ORDER_SIZE,
                "SUBMIT_NEW_SINGLE_ORDER",
            )?;
            let mut request = parse_new_order(bytes)?;
            if bytes[201] != 0 {
                return Ok(vec![new_order_rejection(
                    &request,
                    "Bracket parent orders are not supported; submit a standalone order",
                )]);
            }
            let resolved = if instrument.matches(&request.symbol, &request.exchange) {
                Ok(instrument.clone())
            } else if let Some(catalog) = catalog {
                resolve_requested_instrument(
                    instrument,
                    catalog,
                    &request.symbol,
                    &request.exchange,
                )
                .await
            } else {
                Err("Rithmic catalog is unavailable".to_owned())
            };
            let resolved = match resolved {
                Ok(resolved) => resolved,
                Err(error) => {
                    return Ok(vec![new_order_rejection(
                        &request,
                        &format!("Unsupported symbol: {error}"),
                    )]);
                }
            };
            if !resolved.min_price_increment.is_finite()
                || resolved.min_price_increment <= 0.0
                || (resolved.min_price_increment - 0.25).abs() > f32::EPSILON
                    && (resolved.min_price_increment - 0.1).abs() > f32::EPSILON
            {
                return Ok(vec![new_order_rejection(
                    &request,
                    "Paper trading is limited to verified 0.25 (ES/NQ) or 0.1 (GC) tick contracts",
                )]);
            }
            // Sierra can send the combined display symbol (for example NQU6-CME) with
            // an empty Exchange. Always pass Rithmic the canonical pair from reference data.
            request.symbol = resolved.symbol;
            request.exchange = resolved.exchange;
            let rejected_request = request.clone();
            let (tx, rx) = oneshot::channel();
            commands
                .send(TradingCommand::Submit(request, tx))
                .await
                .map_err(|_| trading_stopped())?;
            match rx.await.map_err(|_| trading_stopped())? {
                Ok(()) => Ok(Vec::new()),
                Err(error) => Ok(vec![new_order_rejection(&rejected_request, &error)]),
            }
        }
        CANCEL_REPLACE_ORDER => {
            require_size(bytes, CANCEL_REPLACE_ORDER_SIZE, "CANCEL_REPLACE_ORDER")?;
            let request = parse_modify_order(bytes)?;
            let client_id = request.client_order_id.clone();
            let account_id = request.account_id.clone();
            let server_id = request.server_order_id.clone();
            let (tx, rx) = oneshot::channel();
            commands
                .send(TradingCommand::Modify(request, tx))
                .await
                .map_err(|_| trading_stopped())?;
            match rx.await.map_err(|_| trading_stopped())? {
                Ok(()) => Ok(Vec::new()),
                Err(error) => Ok(vec![
                    reject_order_action(
                        Some(commands),
                        &server_id,
                        &client_id,
                        &account_id,
                        10,
                        &error,
                    )
                    .await,
                ]),
            }
        }
        CANCEL_ORDER => {
            require_size(bytes, CANCEL_ORDER_SIZE, "CANCEL_ORDER")?;
            let request = CancelOrderRequest {
                server_order_id: read_fixed_string(&bytes[4..36])?,
                client_order_id: read_fixed_string(&bytes[36..68])?,
                account_id: read_fixed_string(&bytes[68..100])?,
            };
            let client_id = request.client_order_id.clone();
            let account_id = request.account_id.clone();
            let server_id = request.server_order_id.clone();
            let (tx, rx) = oneshot::channel();
            commands
                .send(TradingCommand::Cancel(request, tx))
                .await
                .map_err(|_| trading_stopped())?;
            match rx.await.map_err(|_| trading_stopped())? {
                Ok(()) => Ok(Vec::new()),
                Err(error) => Ok(vec![
                    reject_order_action(
                        Some(commands),
                        &server_id,
                        &client_id,
                        &account_id,
                        9,
                        &error,
                    )
                    .await,
                ]),
            }
        }
        _ => Ok(Vec::new()),
    }
}

fn require_size(bytes: &[u8], expected: usize, name: &str) -> Result<(), SessionError> {
    if bytes.len() < expected {
        Err(SessionError::Protocol(format!(
            "{name} is {} bytes; expected at least {expected}",
            bytes.len()
        )))
    } else {
        Ok(())
    }
}

fn trading_stopped() -> SessionError {
    SessionError::Protocol("Rithmic trading worker stopped".to_owned())
}

fn rejection_type(message_type: u16) -> u16 {
    match message_type {
        OPEN_ORDERS_REQUEST => OPEN_ORDERS_REJECT,
        CURRENT_POSITIONS_REQUEST => CURRENT_POSITIONS_REJECT,
        ACCOUNT_BALANCE_REQUEST | TRADE_ACCOUNTS_REQUEST => ACCOUNT_BALANCE_REJECT,
        _ => ORDER_UPDATE,
    }
}

fn parse_new_order(bytes: &[u8]) -> Result<NewOrderRequest, SessionError> {
    Ok(NewOrderRequest {
        symbol: read_fixed_string(&bytes[4..68])?,
        exchange: read_fixed_string(&bytes[68..84])?,
        account_id: read_fixed_string(&bytes[84..116])?,
        client_order_id: read_fixed_string(&bytes[116..148])?,
        order_type: read_i32(bytes, 148),
        buy_sell: read_i32(bytes, 152),
        price1: read_f64(bytes, 160),
        price2: read_f64(bytes, 168),
        quantity: read_f64(bytes, 176),
        time_in_force: read_i32(bytes, 184),
        is_automated: bytes[200] != 0,
    })
}

fn parse_modify_order(bytes: &[u8]) -> Result<ModifyOrderRequest, SessionError> {
    Ok(ModifyOrderRequest {
        server_order_id: read_fixed_string(&bytes[4..36])?,
        client_order_id: read_fixed_string(&bytes[36..68])?,
        price1: (bytes[96] != 0).then(|| read_f64(bytes, 72)),
        price2: (bytes[97] != 0).then(|| read_f64(bytes, 80)),
        quantity: read_f64(bytes, 88),
        time_in_force: read_i32(bytes, 104),
        account_id: read_fixed_string(&bytes[121..153])?,
    })
}

async fn handle_market_data_request(
    bytes: &[u8],
    instrument: &Instrument,
    commands: Option<&mpsc::Sender<MarketCommand>>,
) -> Result<Option<Vec<u8>>, SessionError> {
    require_size(bytes, MARKET_DATA_REQUEST_SIZE, "MARKET_DATA_REQUEST")?;
    let action = read_i32(bytes, 4);
    let symbol_id = read_u32(bytes, 8);
    let Some(commands) = commands else {
        return Ok(Some(market_data_reject(
            symbol_id,
            "Rithmic market data is unavailable",
        )));
    };
    if action == UNSUBSCRIBE {
        let (response, result) = oneshot::channel();
        commands
            .send(MarketCommand::Unsubscribe {
                symbol_id,
                response,
            })
            .await
            .map_err(|_| SessionError::Protocol("Market worker stopped".to_owned()))?;
        return match result.await {
            Ok(Ok(())) => Ok(None),
            Ok(Err(error)) => Ok(Some(market_data_reject(symbol_id, &error))),
            Err(_) => Err(SessionError::Protocol("Market response dropped".to_owned())),
        };
    }
    if !matches!(action, SUBSCRIBE | SNAPSHOT) {
        return Ok(Some(market_data_reject(symbol_id, "Unknown RequestAction")));
    }
    let symbol = read_fixed_string(&bytes[12..76])?;
    let exchange = read_fixed_string(&bytes[76..92])?;
    let resolved =
        match resolve_requested_instrument(instrument, commands, &symbol, &exchange).await {
            Ok(item) => item,
            Err(error) => return Ok(Some(market_data_reject(symbol_id, &error))),
        };
    let (response, result) = oneshot::channel();
    let command = if action == SNAPSHOT {
        MarketCommand::Snapshot {
            symbol: resolved.symbol,
            exchange: resolved.exchange,
            response,
        }
    } else {
        MarketCommand::Subscribe {
            symbol_id,
            symbol: resolved.symbol,
            exchange: resolved.exchange,
            response,
        }
    };
    commands
        .send(command)
        .await
        .map_err(|_| SessionError::Protocol("Market worker stopped".to_owned()))?;
    match result.await {
        Ok(Ok(snapshot)) => Ok(Some(market_data_snapshot(symbol_id, &snapshot))),
        Ok(Err(error)) => Ok(Some(market_data_reject(symbol_id, &error))),
        Err(_) => Err(SessionError::Protocol("Market response dropped".to_owned())),
    }
}

async fn handle_market_depth_request(
    bytes: &[u8],
    instrument: &Instrument,
    commands: Option<&mpsc::Sender<MarketCommand>>,
) -> Result<Vec<Vec<u8>>, SessionError> {
    if bytes.len() < MARKET_DEPTH_REQUEST_SIZE {
        return Err(SessionError::Protocol(format!(
            "MARKET_DEPTH_REQUEST is {} bytes; expected at least {MARKET_DEPTH_REQUEST_SIZE}",
            bytes.len()
        )));
    }
    let action = read_i32(bytes, 4);
    let symbol_id = read_u32(bytes, 8);
    let symbol = read_fixed_string(&bytes[12..76])?;
    let exchange = read_fixed_string(&bytes[76..92])?;
    let requested_levels = read_i32(bytes, 92);
    let max_levels = if requested_levels <= 0 {
        MAX_DEPTH_LEVELS
    } else {
        (requested_levels as usize).min(MAX_DEPTH_LEVELS)
    };
    println!(
        "[DTC] Market depth request: action={action}, SymbolID={symbol_id}, {symbol}.{exchange}, NumLevels={requested_levels}, effective_max={max_levels}"
    );

    let Some(commands) = commands else {
        return Ok(vec![market_depth_reject(
            symbol_id,
            "Rithmic market depth is unavailable",
        )]);
    };

    match action {
        SUBSCRIBE | SNAPSHOT => {
            let resolved = match resolve_requested_instrument(
                instrument, commands, &symbol, &exchange,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(error) => return Ok(vec![market_depth_reject(symbol_id, &error)]),
            };
            let (response, result) = oneshot::channel();
            let command = if action == SNAPSHOT {
                MarketCommand::DepthSnapshot {
                    symbol: resolved.symbol,
                    exchange: resolved.exchange,
                    tick_size: f64::from(resolved.min_price_increment),
                    max_levels,
                    response,
                }
            } else {
                MarketCommand::SubscribeDepth {
                    symbol_id,
                    symbol: resolved.symbol,
                    exchange: resolved.exchange,
                    tick_size: f64::from(resolved.min_price_increment),
                    max_levels,
                    response,
                }
            };
            commands.send(command).await.map_err(|_| {
                SessionError::Protocol("Rithmic market-depth worker stopped".to_owned())
            })?;
            match result.await {
                Ok(Ok(levels)) => {
                    println!(
                        "[DTC] Market depth snapshot: SymbolID={symbol_id}, levels={}",
                        levels.len()
                    );
                    Ok(encode_depth_snapshot(
                        symbol_id,
                        &levels,
                        now_microseconds(),
                    ))
                }
                Ok(Err(error)) => Ok(vec![market_depth_reject(symbol_id, &error)]),
                Err(_) => Err(SessionError::Protocol(
                    "Rithmic market-depth worker dropped its response".to_owned(),
                )),
            }
        }
        UNSUBSCRIBE => {
            let (response, result) = oneshot::channel();
            commands
                .send(MarketCommand::UnsubscribeDepth {
                    symbol_id,
                    response,
                })
                .await
                .map_err(|_| {
                    SessionError::Protocol("Rithmic market-depth worker stopped".to_owned())
                })?;
            match result.await {
                Ok(Ok(())) => Ok(Vec::new()),
                Ok(Err(error)) => Ok(vec![market_depth_reject(symbol_id, &error)]),
                Err(_) => Err(SessionError::Protocol(
                    "Rithmic market-depth worker dropped its response".to_owned(),
                )),
            }
        }
        _ => Ok(vec![market_depth_reject(
            symbol_id,
            "Unknown RequestAction",
        )]),
    }
}

async fn resolve_requested_instrument(
    configured: &Instrument,
    catalog: &mpsc::Sender<MarketCommand>,
    symbol: &str,
    exchange: &str,
) -> Result<Instrument, String> {
    if configured.matches(symbol, exchange) {
        Ok(configured.clone())
    } else {
        catalog_resolve(Some(catalog), symbol, exchange).await
    }
}

fn is_symbol_discovery_request(message_type: u16) -> bool {
    matches!(
        message_type,
        EXCHANGE_LIST_REQUEST
            | SYMBOLS_FOR_EXCHANGE_REQUEST
            | UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST
            | SYMBOLS_FOR_UNDERLYING_REQUEST
            | SECURITY_DEFINITION_FOR_SYMBOL_REQUEST
            | SYMBOL_SEARCH_REQUEST
    )
}

fn symbol_discovery_message_name(message_type: u16) -> &'static str {
    match message_type {
        EXCHANGE_LIST_REQUEST => "EXCHANGE_LIST_REQUEST",
        SYMBOLS_FOR_EXCHANGE_REQUEST => "SYMBOLS_FOR_EXCHANGE_REQUEST",
        UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST => "UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST",
        SYMBOLS_FOR_UNDERLYING_REQUEST => "SYMBOLS_FOR_UNDERLYING_REQUEST",
        SECURITY_DEFINITION_FOR_SYMBOL_REQUEST => "SECURITY_DEFINITION_FOR_SYMBOL_REQUEST",
        SYMBOL_SEARCH_REQUEST => "SYMBOL_SEARCH_REQUEST",
        _ => "UNKNOWN_SYMBOL_DISCOVERY_REQUEST",
    }
}

async fn handle_symbol_discovery_request(
    message_type: u16,
    bytes: &[u8],
    instrument: &Instrument,
    catalog: Option<&mpsc::Sender<MarketCommand>>,
) -> Result<Vec<Vec<u8>>, SessionError> {
    let require_size = |expected: usize, name: &str| {
        if bytes.len() < expected {
            Err(SessionError::Protocol(format!(
                "{name} is {} bytes; expected at least {expected}",
                bytes.len()
            )))
        } else {
            Ok(())
        }
    };

    match message_type {
        EXCHANGE_LIST_REQUEST => {
            require_size(EXCHANGE_LIST_REQUEST_SIZE, "EXCHANGE_LIST_REQUEST")?;
            let request_id = read_i32(bytes, 4);
            let exchanges = catalog_list_exchanges(catalog)
                .await
                .unwrap_or_else(|error| {
                    eprintln!("[DTC] Rithmic catalog exchange lookup failed: {error}");
                    vec![instrument.exchange.clone()]
                });
            Ok(exchange_list_responses(request_id, &exchanges))
        }
        SYMBOLS_FOR_EXCHANGE_REQUEST => {
            require_size(
                SYMBOLS_FOR_EXCHANGE_REQUEST_SIZE,
                "SYMBOLS_FOR_EXCHANGE_REQUEST",
            )?;
            let request_id = read_i32(bytes, 4);
            let exchange = read_fixed_string(&bytes[8..24])?;
            let security_type = read_i32(bytes, 24);
            let request_action = read_i32(bytes, 28);
            let symbol = read_fixed_string(&bytes[32..96])?;
            if request_action != SUBSCRIBE && request_action != UNSUBSCRIBE {
                return Ok(vec![security_definition_reject(
                    request_id,
                    "Only SUBSCRIBE and UNSUBSCRIBE are supported",
                )]);
            }
            if request_action == UNSUBSCRIBE || !futures_type_matches(security_type) {
                return Ok(vec![empty_security_definition_response(request_id)]);
            }
            let instruments = if !symbol.is_empty() {
                if instrument.matches(&symbol, &exchange) {
                    Ok(vec![instrument.clone()])
                } else {
                    catalog_resolve(catalog, &symbol, &exchange)
                        .await
                        .map(|item| vec![item])
                }
            } else {
                catalog_enumerate(catalog, instrument, &exchange, "", false).await
            };
            Ok(match instruments {
                Ok(items) => security_definition_responses(request_id, &items),
                Err(error) => vec![security_definition_reject(request_id, &error)],
            })
        }
        UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST => {
            require_size(
                UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST_SIZE,
                "UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST",
            )?;
            let request_id = read_i32(bytes, 4);
            let exchange = read_fixed_string(&bytes[8..24])?;
            let security_type = read_i32(bytes, 24);
            if !futures_type_matches(security_type) {
                return Ok(vec![empty_security_definition_response(request_id)]);
            }
            Ok(
                match catalog_enumerate(catalog, instrument, &exchange, "", true).await {
                    Ok(mut items) => {
                        for item in &mut items {
                            item.symbol.clear();
                            item.exchange_symbol.clear();
                            item.expiration_date = 0;
                        }
                        security_definition_responses(request_id, &items)
                    }
                    Err(error) => vec![security_definition_reject(request_id, &error)],
                },
            )
        }
        SYMBOLS_FOR_UNDERLYING_REQUEST => {
            require_size(
                SYMBOLS_FOR_UNDERLYING_REQUEST_SIZE,
                "SYMBOLS_FOR_UNDERLYING_REQUEST",
            )?;
            let request_id = read_i32(bytes, 4);
            let underlying = read_fixed_string(&bytes[8..40])?;
            let exchange = read_fixed_string(&bytes[40..56])?;
            let security_type = read_i32(bytes, 56);
            if !futures_type_matches(security_type) {
                return Ok(vec![empty_security_definition_response(request_id)]);
            }
            Ok(
                match catalog_enumerate(catalog, instrument, &exchange, &underlying, false).await {
                    Ok(items) => security_definition_responses(request_id, &items),
                    Err(error) => vec![security_definition_reject(request_id, &error)],
                },
            )
        }
        SECURITY_DEFINITION_FOR_SYMBOL_REQUEST => {
            require_size(
                SECURITY_DEFINITION_REQUEST_SIZE,
                "SECURITY_DEFINITION_FOR_SYMBOL_REQUEST",
            )?;
            let request_id = read_i32(bytes, 4);
            let symbol = read_fixed_string(&bytes[8..72])?;
            let exchange = read_fixed_string(&bytes[72..88])?;
            if instrument.matches(&symbol, &exchange) {
                Ok(vec![security_definition_response(request_id, instrument)])
            } else if let Ok(resolved) = catalog_resolve(catalog, &symbol, &exchange).await {
                Ok(vec![security_definition_response(request_id, &resolved)])
            } else {
                Ok(vec![security_definition_reject(
                    request_id,
                    &format!("Unsupported symbol {symbol}.{exchange}"),
                )])
            }
        }
        SYMBOL_SEARCH_REQUEST => {
            require_size(SYMBOL_SEARCH_REQUEST_SIZE, "SYMBOL_SEARCH_REQUEST")?;
            let request_id = read_i32(bytes, 4);
            let search_text = read_fixed_string(&bytes[8..72])?;
            let exchange = read_fixed_string(&bytes[72..88])?;
            let security_type = read_i32(bytes, 88);
            let search_type = read_i32(bytes, 92);
            if search_text.is_empty() {
                return Ok(vec![security_definition_reject(
                    request_id,
                    "SearchText must not be empty",
                )]);
            }
            if !futures_type_matches(security_type) {
                return Ok(vec![empty_security_definition_response(request_id)]);
            }
            if !(0..=2).contains(&search_type) {
                return Ok(vec![security_definition_reject(
                    request_id,
                    "Unknown SearchType",
                )]);
            }
            let mut instruments = if catalog.is_some() {
                match catalog_search(catalog, &search_text, &exchange, search_type).await {
                    Ok(items) => items,
                    Err(error) => return Ok(vec![security_definition_reject(request_id, &error)]),
                }
            } else {
                Vec::new()
            };
            instruments.retain(|item| {
                exchange_matches(item, &exchange) && search_matches(item, &search_text, search_type)
            });
            if instruments.is_empty()
                && exchange_matches(instrument, &exchange)
                && search_matches(instrument, &search_text, search_type)
            {
                instruments.push(instrument.clone());
            }
            Ok(security_definition_responses(request_id, &instruments))
        }
        _ => unreachable!("caller filters symbol-discovery message types"),
    }
}

fn exchange_matches(instrument: &Instrument, exchange: &str) -> bool {
    exchange.is_empty() || exchange.eq_ignore_ascii_case(&instrument.exchange)
}

fn futures_type_matches(security_type: i32) -> bool {
    security_type == 0 || security_type == 1
}

fn search_matches(instrument: &Instrument, search_text: &str, search_type: i32) -> bool {
    if search_text.is_empty() {
        return true;
    }
    let needle = search_text.to_ascii_lowercase();
    let symbol = instrument.symbol.to_ascii_lowercase();
    let combined = format!("{}-{}", instrument.symbol, instrument.exchange).to_ascii_lowercase();
    let description = instrument.description.to_ascii_lowercase();
    match search_type {
        1 => symbol.contains(&needle) || combined.contains(&needle),
        2 => description.contains(&needle),
        0 => {
            symbol.contains(&needle) || combined.contains(&needle) || description.contains(&needle)
        }
        _ => false,
    }
}

async fn catalog_list_exchanges(
    catalog: Option<&mpsc::Sender<MarketCommand>>,
) -> Result<Vec<String>, String> {
    let Some(catalog) = catalog else {
        return Err("Rithmic catalog is unavailable".to_owned());
    };
    let (response, result) = oneshot::channel();
    catalog
        .send(MarketCommand::ListCatalogExchanges { response })
        .await
        .map_err(|_| "Rithmic catalog worker stopped".to_owned())?;
    result
        .await
        .map_err(|_| "Rithmic catalog response was dropped".to_owned())?
}

async fn catalog_load(
    catalog: Option<&mpsc::Sender<MarketCommand>>,
    preferred_underlying: &str,
) -> Result<Vec<Instrument>, String> {
    let Some(catalog) = catalog else {
        return Err("Rithmic catalog is unavailable".to_owned());
    };
    let (response, result) = oneshot::channel();
    catalog
        .send(MarketCommand::LoadCatalog {
            preferred_underlying: preferred_underlying.to_owned(),
            response,
        })
        .await
        .map_err(|_| "Rithmic catalog worker stopped".to_owned())?;
    result
        .await
        .map_err(|_| "Rithmic catalog response was dropped".to_owned())?
}

async fn catalog_search(
    catalog: Option<&mpsc::Sender<MarketCommand>>,
    search_text: &str,
    exchange: &str,
    search_type: i32,
) -> Result<Vec<Instrument>, String> {
    let Some(catalog) = catalog else {
        return Err("Rithmic catalog is unavailable".to_owned());
    };
    let (response, result) = oneshot::channel();
    catalog
        .send(MarketCommand::SearchCatalog {
            search_text: search_text.to_owned(),
            exchange: exchange.to_owned(),
            search_type,
            response,
        })
        .await
        .map_err(|_| "Rithmic catalog worker stopped".to_owned())?;
    result
        .await
        .map_err(|_| "Rithmic catalog response was dropped".to_owned())?
}

async fn catalog_resolve(
    catalog: Option<&mpsc::Sender<MarketCommand>>,
    symbol: &str,
    exchange: &str,
) -> Result<Instrument, String> {
    let Some(catalog) = catalog else {
        return Err("Rithmic catalog is unavailable".to_owned());
    };
    let (resolved_symbol, resolved_exchange) = if exchange.is_empty() {
        symbol
            .rsplit_once(['-', '.'])
            .map(|(symbol, exchange)| (symbol, exchange))
            .unwrap_or((symbol, exchange))
    } else {
        (symbol, exchange)
    };
    let (response, result) = oneshot::channel();
    catalog
        .send(MarketCommand::ResolveCatalogInstrument {
            symbol: resolved_symbol.to_owned(),
            exchange: resolved_exchange.to_owned(),
            response,
        })
        .await
        .map_err(|_| "Rithmic catalog worker stopped".to_owned())?;
    result
        .await
        .map_err(|_| "Rithmic catalog response was dropped".to_owned())?
}

fn exchange_list_responses(request_id: i32, exchanges: &[String]) -> Vec<Vec<u8>> {
    if exchanges.is_empty() {
        let mut message = vec![0_u8; EXCHANGE_LIST_RESPONSE_SIZE];
        put_u16(&mut message, 0, EXCHANGE_LIST_RESPONSE_SIZE as u16);
        put_u16(&mut message, 2, EXCHANGE_LIST_RESPONSE);
        put_i32(&mut message, 4, request_id);
        message[24] = 1;
        return vec![message];
    }
    exchanges
        .iter()
        .enumerate()
        .map(|(index, exchange)| {
            exchange_list_response(request_id, exchange, index + 1 == exchanges.len())
        })
        .collect()
}

fn exchange_list_response(request_id: i32, exchange: &str, is_final: bool) -> Vec<u8> {
    let mut message = vec![0_u8; EXCHANGE_LIST_RESPONSE_SIZE];
    put_u16(&mut message, 0, EXCHANGE_LIST_RESPONSE_SIZE as u16);
    put_u16(&mut message, 2, EXCHANGE_LIST_RESPONSE);
    put_i32(&mut message, 4, request_id);
    put_fixed_string(&mut message[8..24], exchange);
    message[24] = u8::from(is_final);
    let description = if exchange.eq_ignore_ascii_case("CME") {
        "Chicago Mercantile Exchange"
    } else {
        exchange
    };
    put_fixed_string(&mut message[25..73], description);
    message
}

fn security_definition_responses(request_id: i32, instruments: &[Instrument]) -> Vec<Vec<u8>> {
    if instruments.is_empty() {
        return vec![empty_security_definition_response(request_id)];
    }
    instruments
        .iter()
        .enumerate()
        .map(|(index, instrument)| {
            let mut response = security_definition_response(request_id, instrument);
            response[168] = u8::from(index + 1 == instruments.len());
            response
        })
        .collect()
}

fn add_security_definition_to_catalog(
    response: &[u8],
    catalog: &mut Vec<Vec<u8>>,
    keys: &mut HashSet<(String, String)>,
) -> bool {
    if response.len() < SECURITY_DEFINITION_RESPONSE_SIZE
        || u16::from_le_bytes(response[2..4].try_into().expect("validated response size"))
            != SECURITY_DEFINITION_RESPONSE
    {
        return false;
    }
    let Ok(symbol) = read_fixed_string(&response[8..72]) else {
        return false;
    };
    let Ok(exchange) = read_fixed_string(&response[72..88]) else {
        return false;
    };
    if symbol.is_empty()
        || !keys.insert((symbol.to_ascii_lowercase(), exchange.to_ascii_lowercase()))
    {
        return false;
    }
    let mut registration = response.to_vec();
    put_i32(&mut registration, 4, 0);
    catalog.push(registration);
    let final_index = catalog.len() - 1;
    for (index, definition) in catalog.iter_mut().enumerate() {
        definition[168] = u8::from(index == final_index);
    }
    true
}

fn empty_security_definition_response(request_id: i32) -> Vec<u8> {
    let mut message = vec![0_u8; SECURITY_DEFINITION_RESPONSE_SIZE];
    put_u16(&mut message, 0, SECURITY_DEFINITION_RESPONSE_SIZE as u16);
    put_u16(&mut message, 2, SECURITY_DEFINITION_RESPONSE);
    put_i32(&mut message, 4, request_id);
    put_i32(&mut message, 160, -1);
    for offset in [172, 176, 256] {
        put_f32(&mut message, offset, 1.0);
    }
    message[252] = 1; // official default, not a declaration for a specific symbol
    message[168] = 1; // IsFinalMessage
    message
}

async fn catalog_enumerate(
    catalog: Option<&mpsc::Sender<MarketCommand>>,
    fallback: &Instrument,
    exchange: &str,
    underlying: &str,
    roots_only: bool,
) -> Result<Vec<Instrument>, String> {
    let Some(catalog) = catalog else {
        return Ok(
            if exchange_matches(fallback, exchange)
                && (underlying.is_empty()
                    || underlying.eq_ignore_ascii_case(&fallback.underlying_symbol))
            {
                let mut item = fallback.clone();
                if roots_only {
                    item.symbol.clear();
                    item.exchange_symbol.clear();
                    item.expiration_date = 0;
                    item.min_price_increment = 0.0;
                    item.price_display_format = -1;
                    item.currency_value_per_increment = 0.0;
                    item.contract_size = 0.0;
                    item.currency.clear();
                }
                vec![item]
            } else {
                Vec::new()
            },
        );
    };
    let (response, result) = oneshot::channel();
    catalog
        .send(MarketCommand::EnumerateCatalog {
            exchange: exchange.to_owned(),
            underlying: underlying.to_owned(),
            roots_only,
            response,
        })
        .await
        .map_err(|_| "Catalog worker stopped".to_owned())?;
    result
        .await
        .map_err(|_| "Catalog response dropped".to_owned())?
}

fn security_definition_response(request_id: i32, instrument: &Instrument) -> Vec<u8> {
    let mut message = vec![0_u8; SECURITY_DEFINITION_RESPONSE_SIZE];
    put_u16(&mut message, 0, SECURITY_DEFINITION_RESPONSE_SIZE as u16);
    put_u16(&mut message, 2, SECURITY_DEFINITION_RESPONSE);
    put_i32(&mut message, 4, request_id);
    put_fixed_string(&mut message[8..72], &instrument.symbol);
    put_fixed_string(&mut message[72..88], &instrument.exchange);
    put_i32(&mut message, 88, 1); // SECURITY_TYPE_FUTURES
    put_fixed_string(&mut message[92..156], &instrument.description);
    put_f32(&mut message, 156, instrument.min_price_increment);
    put_i32(&mut message, 160, instrument.price_display_format);
    put_f32(&mut message, 164, instrument.currency_value_per_increment);
    message[168] = 1; // IsFinalMessage
    put_f32(&mut message, 172, 1.0); // FloatToIntPriceMultiplier
    put_f32(&mut message, 176, 1.0); // IntToFloatPriceDivisor
    put_fixed_string(&mut message[180..212], &instrument.underlying_symbol);
    put_u32(&mut message, 228, instrument.expiration_date);
    put_f32(&mut message, 248, 1.0); // IntToFloatQuantityDivisor
    message[252] = 1; // HasMarketDepthData
    put_f32(&mut message, 256, 1.0); // DisplayPriceMultiplier
    put_fixed_string(
        &mut message[260..324],
        if instrument.exchange_symbol.is_empty() {
            &instrument.symbol
        } else {
            &instrument.exchange_symbol
        },
    );
    put_fixed_string(&mut message[332..340], &instrument.currency);
    put_f32(&mut message, 340, instrument.contract_size);
    put_fixed_string(&mut message[368..432], &instrument.underlying_symbol);
    message
}

fn security_definition_reject(request_id: i32, reason: &str) -> Vec<u8> {
    let mut message = vec![0_u8; SECURITY_DEFINITION_REJECT_SIZE];
    put_u16(&mut message, 0, SECURITY_DEFINITION_REJECT_SIZE as u16);
    put_u16(&mut message, 2, SECURITY_DEFINITION_REJECT);
    put_i32(&mut message, 4, request_id);
    put_fixed_string(&mut message[8..104], reason);
    message
}

fn market_data_reject(symbol_id: u32, reason: &str) -> Vec<u8> {
    let mut message = vec![0_u8; MARKET_DATA_REJECT_SIZE];
    put_u16(&mut message, 0, MARKET_DATA_REJECT_SIZE as u16);
    put_u16(&mut message, 2, MARKET_DATA_REJECT);
    put_u32(&mut message, 4, symbol_id);
    put_fixed_string(&mut message[8..104], reason);
    message
}

fn market_depth_reject(symbol_id: u32, reason: &str) -> Vec<u8> {
    let mut message = vec![0_u8; MARKET_DEPTH_REJECT_SIZE];
    put_u16(&mut message, 0, MARKET_DEPTH_REJECT_SIZE as u16);
    put_u16(&mut message, 2, MARKET_DEPTH_REJECT);
    put_u32(&mut message, 4, symbol_id);
    put_fixed_string(&mut message[8..104], reason);
    message
}

fn encode_depth_snapshot(symbol_id: u32, levels: &[DepthLevel], datetime_us: i64) -> Vec<Vec<u8>> {
    if levels.is_empty() {
        let mut message = vec![0_u8; MARKET_DEPTH_SNAPSHOT_LEVEL_SIZE];
        put_u16(&mut message, 0, MARKET_DEPTH_SNAPSHOT_LEVEL_SIZE as u16);
        put_u16(&mut message, 2, MARKET_DEPTH_SNAPSHOT_LEVEL);
        put_u32(&mut message, 4, symbol_id);
        message[34] = 1;
        message[35] = 1;
        return vec![message];
    }
    levels
        .iter()
        .enumerate()
        .map(|(index, level)| {
            let mut message = vec![0_u8; MARKET_DEPTH_SNAPSHOT_LEVEL_SIZE];
            put_u16(&mut message, 0, MARKET_DEPTH_SNAPSHOT_LEVEL_SIZE as u16);
            put_u16(&mut message, 2, MARKET_DEPTH_SNAPSHOT_LEVEL);
            put_u32(&mut message, 4, symbol_id);
            put_u16(&mut message, 8, u16::from(level.side.dtc_value()));
            put_f64(&mut message, 16, level.price);
            put_f64(&mut message, 24, level.quantity);
            put_u16(&mut message, 32, level.level);
            message[34] = u8::from(index == 0);
            message[35] = u8::from(index + 1 == levels.len());
            put_f64(&mut message, 40, datetime_us as f64 / 1_000_000.0);
            put_u32(&mut message, 48, level.num_orders);
            message
        })
        .collect()
}

fn encode_depth_update(
    symbol_id: u32,
    update: &LevelUpdate,
    datetime_us: i64,
    is_final: bool,
) -> Vec<u8> {
    let mut message = vec![0_u8; MARKET_DEPTH_UPDATE_LEVEL_V2_SIZE];
    put_u16(&mut message, 0, MARKET_DEPTH_UPDATE_LEVEL_V2_SIZE as u16);
    put_u16(&mut message, 2, MARKET_DEPTH_UPDATE_LEVEL_V2);
    put_u32(&mut message, 4, symbol_id);
    put_i64(&mut message, 8, datetime_us / 1_000);
    put_f64(&mut message, 16, update.price);
    put_f64(&mut message, 24, update.quantity);
    put_u16(&mut message, 32, update.num_orders);
    put_u16(&mut message, 34, update.level);
    message[36] = update.side.dtc_value();
    message[37] = update.update_type as u8;
    message[38] = if is_final { 1 } else { 2 };
    message
}

fn empty_market_data_snapshot(symbol_id: u32) -> Vec<u8> {
    let mut message = vec![0_u8; MARKET_DATA_SNAPSHOT_SIZE];
    put_u16(&mut message, 0, MARKET_DATA_SNAPSHOT_SIZE as u16);
    put_u16(&mut message, 2, MARKET_DATA_SNAPSHOT);
    put_u32(&mut message, 4, symbol_id);
    for offset in [8, 16, 24, 32, 40, 56, 64, 72, 80, 88, 96] {
        put_f64(&mut message, offset, f64::MAX);
    }
    put_u32(&mut message, 48, u32::MAX);
    put_u32(&mut message, 52, u32::MAX);
    message
}

fn market_data_snapshot(symbol_id: u32, snapshot: &MarketSnapshot) -> Vec<u8> {
    let mut message = empty_market_data_snapshot(symbol_id);
    for (offset, value) in [
        (8, snapshot.settlement),
        (16, snapshot.open),
        (24, snapshot.high),
        (32, snapshot.low),
        (40, snapshot.volume),
        (56, snapshot.bid),
        (64, snapshot.ask),
        (72, snapshot.ask_size),
        (80, snapshot.bid_size),
        (88, snapshot.last),
        (96, snapshot.last_size),
    ] {
        put_f64(&mut message, offset, value.unwrap_or(f64::MAX));
    }
    put_u32(&mut message, 52, snapshot.open_interest.unwrap_or(u32::MAX));
    put_f64(
        &mut message,
        104,
        snapshot.last_time_us as f64 / 1_000_000.0,
    );
    put_f64(
        &mut message,
        112,
        snapshot.quote_time_us as f64 / 1_000_000.0,
    );
    put_u32(&mut message, 120, snapshot.settlement_date);
    message
}

fn encode_market_event(event: MarketEvent) -> Vec<u8> {
    match event {
        MarketEvent::Snapshot {
            symbol_id,
            snapshot,
        } => market_data_snapshot(symbol_id, &snapshot),
        MarketEvent::SessionVolume { symbol_id, volume } => {
            let mut message = vec![0; 24];
            put_u16(&mut message, 0, 24);
            put_u16(&mut message, 2, MARKET_DATA_UPDATE_SESSION_VOLUME);
            put_u32(&mut message, 4, symbol_id);
            put_f64(&mut message, 8, volume);
            message
        }
        MarketEvent::FeedStatus { available } => {
            let mut message = vec![0_u8; MARKET_DATA_FEED_STATUS_SIZE];
            put_u16(&mut message, 0, MARKET_DATA_FEED_STATUS_SIZE as u16);
            put_u16(&mut message, 2, MARKET_DATA_FEED_STATUS);
            put_i32(&mut message, 4, if available { 2 } else { 1 });
            message
        }
        MarketEvent::LastTrade {
            symbol_id,
            price,
            volume,
            datetime_us,
            at_bid_or_ask: _,
            is_snapshot: true,
        } => {
            let mut message = vec![0_u8; 32];
            put_u16(&mut message, 0, 32);
            put_u16(&mut message, 2, MARKET_DATA_UPDATE_LAST_TRADE_SNAPSHOT);
            put_u32(&mut message, 4, symbol_id);
            put_f64(&mut message, 8, price);
            put_f64(&mut message, 16, volume);
            put_f64(&mut message, 24, datetime_us as f64 / 1_000_000.0);
            message
        }
        MarketEvent::LastTrade {
            symbol_id,
            price,
            volume,
            datetime_us,
            at_bid_or_ask,
            is_snapshot: false,
        } => {
            let mut message = vec![0_u8; 40];
            put_u16(&mut message, 0, 40);
            put_u16(&mut message, 2, MARKET_DATA_UPDATE_TRADE_V2);
            put_u32(&mut message, 4, symbol_id);
            put_f64(&mut message, 8, price);
            put_f64(&mut message, 16, volume);
            put_i64(&mut message, 24, datetime_us);
            message[32] = at_bid_or_ask;
            message
        }
        MarketEvent::BestBidAsk {
            symbol_id,
            bid_price,
            bid_quantity,
            ask_price,
            ask_quantity,
            datetime_us,
        } => {
            let mut message = vec![0_u8; 48];
            put_u16(&mut message, 0, 48);
            put_u16(&mut message, 2, MARKET_DATA_UPDATE_BID_ASK_V2);
            put_u32(&mut message, 4, symbol_id);
            put_f64(&mut message, 8, bid_price);
            put_f64(&mut message, 16, bid_quantity);
            put_f64(&mut message, 24, ask_price);
            put_f64(&mut message, 32, ask_quantity);
            put_i64(&mut message, 40, datetime_us);
            message
        }
        MarketEvent::DepthUpdate {
            symbol_id,
            update,
            datetime_us,
            is_final,
        } => encode_depth_update(symbol_id, &update, datetime_us, is_final),
        MarketEvent::DepthSnapshotLevel {
            symbol_id,
            level,
            datetime_us,
            is_first,
            is_last,
        } => encode_depth_snapshot_level(symbol_id, &level, datetime_us, is_first, is_last),
        MarketEvent::FeedError(_) => Vec::new(),
    }
}

fn encode_depth_snapshot_level(
    symbol_id: u32,
    level: &DepthLevel,
    datetime_us: i64,
    is_first: bool,
    is_last: bool,
) -> Vec<u8> {
    let mut message = vec![0_u8; MARKET_DEPTH_SNAPSHOT_LEVEL_SIZE];
    put_u16(&mut message, 0, MARKET_DEPTH_SNAPSHOT_LEVEL_SIZE as u16);
    put_u16(&mut message, 2, MARKET_DEPTH_SNAPSHOT_LEVEL);
    put_u32(&mut message, 4, symbol_id);
    put_u16(&mut message, 8, u16::from(level.side.dtc_value()));
    put_f64(&mut message, 16, level.price);
    put_f64(&mut message, 24, level.quantity);
    put_u16(&mut message, 32, level.level);
    message[34] = u8::from(is_first);
    message[35] = u8::from(is_last);
    put_f64(&mut message, 40, datetime_us as f64 / 1_000_000.0);
    put_u32(&mut message, 48, level.num_orders);
    message
}

fn heartbeat_message() -> [u8; HEARTBEAT_SIZE] {
    let mut message = [0_u8; HEARTBEAT_SIZE];
    put_u16(&mut message, 0, HEARTBEAT_SIZE as u16);
    put_u16(&mut message, 2, HEARTBEAT);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64;
    message[8..16].copy_from_slice(&now.to_le_bytes());
    message
}

fn logoff_message(reason: &str, do_not_reconnect: bool) -> [u8; LOGOFF_SIZE] {
    let mut message = [0_u8; LOGOFF_SIZE];
    put_u16(&mut message, 0, LOGOFF_SIZE as u16);
    put_u16(&mut message, 2, LOGOFF);
    put_fixed_string(&mut message[4..100], reason);
    message[100] = u8::from(do_not_reconnect);
    message
}

fn put_u16(target: &mut [u8], offset: usize, value: u16) {
    target[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_i16(target: &mut [u8], offset: usize, value: i16) {
    target[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_i32(target: &mut [u8], offset: usize, value: i32) {
    target[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(target: &mut [u8], offset: usize, value: u32) {
    target[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_i64(target: &mut [u8], offset: usize, value: i64) {
    target[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn put_f32(target: &mut [u8], offset: usize, value: f32) {
    target[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_f64(target: &mut [u8], offset: usize, value: f64) {
    target[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_i32(source: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(
        source[offset..offset + 4]
            .try_into()
            .expect("four-byte slice"),
    )
}

fn read_i64(source: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(
        source[offset..offset + 8]
            .try_into()
            .expect("eight-byte slice"),
    )
}

fn read_f64(source: &[u8], offset: usize) -> f64 {
    f64::from_le_bytes(
        source[offset..offset + 8]
            .try_into()
            .expect("eight-byte slice"),
    )
}

fn read_u32(source: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        source[offset..offset + 4]
            .try_into()
            .expect("four-byte slice"),
    )
}

fn read_fixed_string(source: &[u8]) -> Result<String, SessionError> {
    let end = source
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(source.len());
    std::str::from_utf8(&source[..end])
        .map(str::to_owned)
        .map_err(|_| SessionError::Protocol("fixed string is not valid UTF-8/ASCII".to_owned()))
}

fn put_fixed_string(target: &mut [u8], value: &str) {
    let bytes = value.as_bytes();
    let length = bytes.len().min(target.len().saturating_sub(1));
    target[..length].copy_from_slice(&bytes[..length]);
}

fn now_microseconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    fn encoding_request(encoding: i32) -> [u8; ENCODING_MESSAGE_SIZE] {
        let mut message = [0_u8; ENCODING_MESSAGE_SIZE];
        put_u16(&mut message, 0, ENCODING_MESSAGE_SIZE as u16);
        put_u16(&mut message, 2, ENCODING_REQUEST);
        put_i32(&mut message, 4, CURRENT_VERSION);
        put_i32(&mut message, 8, encoding);
        message[12..15].copy_from_slice(b"DTC");
        message
    }

    fn logon_request(heartbeat_seconds: i32) -> [u8; 284] {
        let mut message = [0_u8; 284];
        put_u16(&mut message, 0, 284);
        put_u16(&mut message, 2, LOGON_REQUEST);
        put_i32(&mut message, 4, CURRENT_VERSION);
        put_i32(&mut message, 144, heartbeat_seconds);
        message
    }

    fn security_definition_request(request_id: i32) -> [u8; SECURITY_DEFINITION_REQUEST_SIZE] {
        let mut message = [0_u8; SECURITY_DEFINITION_REQUEST_SIZE];
        put_u16(&mut message, 0, SECURITY_DEFINITION_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, SECURITY_DEFINITION_FOR_SYMBOL_REQUEST);
        put_i32(&mut message, 4, request_id);
        put_fixed_string(&mut message[8..72], "ESU6");
        put_fixed_string(&mut message[72..88], "CME");
        message
    }

    fn exchange_list_request(request_id: i32) -> [u8; EXCHANGE_LIST_REQUEST_SIZE] {
        let mut message = [0_u8; EXCHANGE_LIST_REQUEST_SIZE];
        put_u16(&mut message, 0, EXCHANGE_LIST_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, EXCHANGE_LIST_REQUEST);
        put_i32(&mut message, 4, request_id);
        message
    }

    fn symbols_for_exchange_request(
        request_id: i32,
        exchange: &str,
    ) -> [u8; SYMBOLS_FOR_EXCHANGE_REQUEST_SIZE] {
        let mut message = [0_u8; SYMBOLS_FOR_EXCHANGE_REQUEST_SIZE];
        put_u16(&mut message, 0, SYMBOLS_FOR_EXCHANGE_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, SYMBOLS_FOR_EXCHANGE_REQUEST);
        put_i32(&mut message, 4, request_id);
        put_fixed_string(&mut message[8..24], exchange);
        put_i32(&mut message, 24, 1); // SECURITY_TYPE_FUTURES
        put_i32(&mut message, 28, SUBSCRIBE);
        message
    }

    fn symbol_search_request(
        request_id: i32,
        search_text: &str,
        search_type: i32,
    ) -> [u8; SYMBOL_SEARCH_REQUEST_SIZE] {
        let mut message = [0_u8; SYMBOL_SEARCH_REQUEST_SIZE];
        put_u16(&mut message, 0, SYMBOL_SEARCH_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, SYMBOL_SEARCH_REQUEST);
        put_i32(&mut message, 4, request_id);
        put_fixed_string(&mut message[8..72], search_text);
        put_i32(&mut message, 88, 1); // SECURITY_TYPE_FUTURES
        put_i32(&mut message, 92, search_type);
        message
    }

    fn market_data_request(symbol_id: u32) -> [u8; MARKET_DATA_REQUEST_SIZE] {
        let mut message = [0_u8; MARKET_DATA_REQUEST_SIZE];
        put_u16(&mut message, 0, MARKET_DATA_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, MARKET_DATA_REQUEST);
        put_i32(&mut message, 4, SUBSCRIBE);
        put_u32(&mut message, 8, symbol_id);
        put_fixed_string(&mut message[12..76], "ESU6");
        put_fixed_string(&mut message[76..92], "CME");
        message
    }

    fn market_depth_request(symbol_id: u32, levels: i32) -> [u8; MARKET_DEPTH_REQUEST_SIZE] {
        let mut message = [0_u8; MARKET_DEPTH_REQUEST_SIZE];
        put_u16(&mut message, 0, MARKET_DEPTH_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, MARKET_DEPTH_REQUEST);
        put_i32(&mut message, 4, SUBSCRIBE);
        put_u32(&mut message, 8, symbol_id);
        put_fixed_string(&mut message[12..76], "ESU6");
        put_fixed_string(&mut message[76..92], "CME");
        put_i32(&mut message, 92, levels);
        message
    }

    fn historical_request(
        request_id: i32,
        interval: i32,
    ) -> [u8; HISTORICAL_PRICE_DATA_REQUEST_SIZE] {
        let mut message = [0_u8; HISTORICAL_PRICE_DATA_REQUEST_SIZE];
        put_u16(&mut message, 0, HISTORICAL_PRICE_DATA_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, HISTORICAL_PRICE_DATA_REQUEST);
        put_i32(&mut message, 4, request_id);
        put_fixed_string(&mut message[8..72], "ESU6");
        put_fixed_string(&mut message[72..88], "CME");
        put_i32(&mut message, 88, interval);
        put_i64(&mut message, 96, 1_800_000_000);
        put_i64(&mut message, 104, 1_800_000_060);
        message
    }

    fn open_orders_request(
        request_id: i32,
        request_all: bool,
        server_order_id: &str,
        account_id: &str,
    ) -> [u8; OPEN_ORDERS_REQUEST_SIZE] {
        let mut message = [0_u8; OPEN_ORDERS_REQUEST_SIZE];
        put_u16(&mut message, 0, OPEN_ORDERS_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, OPEN_ORDERS_REQUEST);
        put_i32(&mut message, 4, request_id);
        put_i32(&mut message, 8, i32::from(request_all));
        put_fixed_string(&mut message[12..44], server_order_id);
        put_fixed_string(&mut message[44..76], account_id);
        message
    }

    fn current_positions_request(
        request_id: i32,
        account_id: &str,
    ) -> [u8; CURRENT_POSITIONS_REQUEST_SIZE] {
        let mut message = [0_u8; CURRENT_POSITIONS_REQUEST_SIZE];
        put_u16(&mut message, 0, CURRENT_POSITIONS_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, CURRENT_POSITIONS_REQUEST);
        put_i32(&mut message, 4, request_id);
        put_fixed_string(&mut message[8..40], account_id);
        message
    }

    fn account_balance_request(
        request_id: i32,
        account_id: &str,
    ) -> [u8; ACCOUNT_BALANCE_REQUEST_SIZE] {
        let mut message = [0_u8; ACCOUNT_BALANCE_REQUEST_SIZE];
        put_u16(&mut message, 0, ACCOUNT_BALANCE_REQUEST_SIZE as u16);
        put_u16(&mut message, 2, ACCOUNT_BALANCE_REQUEST);
        put_i32(&mut message, 4, request_id);
        put_fixed_string(&mut message[8..40], account_id);
        message
    }

    fn test_order(server_order_id: &str, account_id: &str) -> TradingOrder {
        TradingOrder {
            request_id: 0,
            symbol: "ESU6".to_owned(),
            exchange: "CME".to_owned(),
            account_id: account_id.to_owned(),
            client_order_id: format!("client-{server_order_id}"),
            server_order_id: server_order_id.to_owned(),
            exchange_order_id: String::new(),
            order_status: 2,
            update_reason: 3,
            order_type: 2,
            buy_sell: 1,
            price1: 5000.0,
            price2: f64::MAX,
            quantity: 1.0,
            filled_quantity: 0.0,
            remaining_quantity: 1.0,
            average_fill_price: f64::MAX,
            last_fill_price: f64::MAX,
            last_fill_quantity: 0.0,
            last_fill_datetime_ms: 0,
            last_fill_execution_id: String::new(),
            info_text: String::new(),
            time_in_force: 1,
            is_snapshot: true,
        }
    }

    async fn read_wire_message(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0_u8; 4];
        stream.read_exact(&mut header).await.unwrap();
        let size = u16::from_le_bytes(header[..2].try_into().unwrap()) as usize;
        let mut message = vec![0_u8; size];
        message[..4].copy_from_slice(&header);
        stream.read_exact(&mut message[4..]).await.unwrap();
        message
    }

    #[tokio::test]
    async fn tcp_session_supports_sierra_find_symbol_discovery() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream).await.unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&encoding_request(0)).await.unwrap();
        read_wire_message(&mut client).await;
        client.write_all(&logon_request(30)).await.unwrap();
        let logon = read_wire_message(&mut client).await;
        assert_eq!(logon[244], 1, "security definitions must be advertised");

        client
            .write_all(&exchange_list_request(5010))
            .await
            .unwrap();
        let exchange = read_wire_message(&mut client).await;
        assert_eq!(exchange.len(), EXCHANGE_LIST_RESPONSE_SIZE);
        assert_eq!(
            u16::from_le_bytes(exchange[2..4].try_into().unwrap()),
            EXCHANGE_LIST_RESPONSE
        );
        assert_eq!(read_i32(&exchange, 4), 5010);
        assert_eq!(read_fixed_string(&exchange[8..24]).unwrap(), "CME");
        assert_eq!(exchange[24], 1);

        client
            .write_all(&symbols_for_exchange_request(5020, "CME"))
            .await
            .unwrap();
        let definition = read_wire_message(&mut client).await;
        assert_eq!(read_i32(&definition, 4), 5020);
        assert_eq!(read_fixed_string(&definition[8..72]).unwrap(), "ESU6");
        assert_eq!(read_fixed_string(&definition[72..88]).unwrap(), "CME");
        assert_eq!(definition[168], 1);

        client
            .write_all(&symbol_search_request(5080, "S&P", 2))
            .await
            .unwrap();
        let search_match = read_wire_message(&mut client).await;
        assert_eq!(read_i32(&search_match, 4), 5080);
        assert_eq!(read_fixed_string(&search_match[8..72]).unwrap(), "ESU6");

        client
            .write_all(&symbol_search_request(5081, "NQ", 1))
            .await
            .unwrap();
        let no_match = read_wire_message(&mut client).await;
        assert_eq!(read_i32(&no_match, 4), 5081);
        assert_eq!(read_fixed_string(&no_match[8..72]).unwrap(), "");
        assert_eq!(no_match[168], 1);

        client.shutdown().await.unwrap();
        server.await.unwrap();
    }

    #[test]
    fn official_v8_response_layouts_have_expected_headers_and_sizes() {
        let encoding = encoding_response();
        assert_eq!(&encoding[..4], &[16, 0, 7, 0]);
        assert_eq!(&encoding[4..8], &CURRENT_VERSION.to_le_bytes());
        assert_eq!(&encoding[8..12], &BINARY_ENCODING.to_le_bytes());
        assert_eq!(&encoding[12..16], b"DTC\0");

        let logon = logon_response(false, false, false);
        assert_eq!(&logon[..4], &[0, 1, 2, 0]);
        assert_eq!(&logon[8..12], &1_i32.to_le_bytes());
        assert_eq!(
            read_fixed_string(&logon[12..108]).unwrap(),
            LOGON_RESULT_TEXT
        );
        assert_eq!(&logon[176..194], b"rithmic-dtc-bridge");
        assert_eq!(logon[194], 0);
        assert_eq!(&logon[236..240], &[0; 4]);
        assert_eq!(&logon[240..244], b"-\0\0\0");
        assert_eq!(logon[244], 1);
        assert_eq!(&logon[245..253], &[0; 8]);
    }

    #[test]
    fn official_v8_trading_layouts_and_capabilities_are_exact() {
        let logon = logon_response(true, true, true);
        assert_eq!(logon[237], 1);
        assert_eq!(logon[238], 0, "OCO is not implemented");
        assert_eq!(logon[239], 1);

        let account = trade_account_response(
            31,
            &TradeAccount {
                account_id: "paper-account".to_owned(),
                currency: "USD".to_owned(),
                trading_disabled: false,
            },
            0,
            1,
        );
        assert_eq!(account.len(), 52);
        assert_eq!(u16::from_le_bytes(account[2..4].try_into().unwrap()), 401);
        assert_eq!(read_i32(&account, 4), 1);
        assert_eq!(read_i32(&account, 8), 1);
        assert_eq!(
            read_fixed_string(&account[12..44]).unwrap(),
            "paper-account"
        );
        assert_eq!(read_i32(&account, 44), 31);

        let position = position_update(
            32,
            &TradingPosition {
                symbol: "ESU6".to_owned(),
                exchange: "CME".to_owned(),
                account_id: "paper-account".to_owned(),
                quantity: -2.0,
                average_price: 6500.25,
                open_profit_loss: 125.0,
            },
            0,
            1,
            false,
        );
        assert_eq!(position.len(), 240);
        assert_eq!(read_f64(&position, 96), -2.0);
        assert_eq!(read_f64(&position, 104), 6500.25);
        assert_eq!(read_f64(&position, 200), 125.0);

        let balance = account_balance_update(
            33,
            &AccountBalance {
                account_id: "paper-account".to_owned(),
                currency: "USD".to_owned(),
                cash_balance: 100_000.0,
                available_funds: 90_000.0,
                open_profit_loss: 25.0,
                daily_profit_loss: 50.0,
                trading_disabled: false,
            },
            false,
        );
        assert_eq!(balance.len(), 416);
        assert_eq!(read_f64(&balance, 8), 100_000.0);
        assert_eq!(read_f64(&balance, 16), 90_000.0);
        assert_eq!(read_fixed_string(&balance[24..32]).unwrap(), "USD");
        assert_eq!(balance[235], 0);
    }

    #[test]
    fn parses_official_single_order_modify_and_cancel_layouts() {
        let mut submit = vec![0_u8; SUBMIT_NEW_SINGLE_ORDER_SIZE];
        put_u16(&mut submit, 0, SUBMIT_NEW_SINGLE_ORDER_SIZE as u16);
        put_u16(&mut submit, 2, SUBMIT_NEW_SINGLE_ORDER);
        put_fixed_string(&mut submit[4..68], "ESU6");
        put_fixed_string(&mut submit[68..84], "CME");
        put_fixed_string(&mut submit[84..116], "paper-account");
        put_fixed_string(&mut submit[116..148], "sc-1");
        put_i32(&mut submit, 148, 2);
        put_i32(&mut submit, 152, 1);
        put_f64(&mut submit, 160, 5000.0);
        put_f64(&mut submit, 176, 1.0);
        put_i32(&mut submit, 184, 1);
        let parsed = parse_new_order(&submit).unwrap();
        assert_eq!(parsed.symbol, "ESU6");
        assert_eq!(parsed.order_type, 2);
        assert_eq!(parsed.price1, 5000.0);
        assert_eq!(parsed.quantity, 1.0);

        let mut modify = vec![0_u8; CANCEL_REPLACE_ORDER_SIZE];
        put_fixed_string(&mut modify[4..36], "basket-1");
        put_fixed_string(&mut modify[36..68], "sc-1");
        put_f64(&mut modify, 72, 5000.25);
        put_f64(&mut modify, 88, 1.0);
        modify[96] = 1;
        put_i32(&mut modify, 104, 1);
        put_fixed_string(&mut modify[121..153], "paper-account");
        let parsed = parse_modify_order(&modify).unwrap();
        assert_eq!(parsed.server_order_id, "basket-1");
        assert_eq!(parsed.price1, Some(5000.25));
        assert_eq!(parsed.price2, None);
    }

    #[tokio::test]
    async fn trading_resolves_and_canonicalizes_a_discovered_nq_contract() {
        let configured = Instrument::es("ESU6", "CME").unwrap();
        let (catalog_tx, mut catalog_rx) = mpsc::channel(1);
        let catalog_task = tokio::spawn(async move {
            let Some(MarketCommand::ResolveCatalogInstrument {
                symbol,
                exchange,
                response,
            }) = catalog_rx.recv().await
            else {
                panic!("expected catalog resolution");
            };
            assert_eq!(symbol, "NQU6");
            assert_eq!(exchange, "CME");
            response
                .send(Ok(Instrument {
                    symbol: "NQU6".to_owned(),
                    exchange: "CME".to_owned(),
                    underlying_symbol: "NQ".to_owned(),
                    description: "E-mini Nasdaq-100 Futures".to_owned(),
                    min_price_increment: 0.25,
                    price_display_format: 2,
                    currency_value_per_increment: 5.0,
                    contract_size: 20.0,
                    currency: "USD".to_owned(),
                    expiration_date: 0,
                    exchange_symbol: String::new(),
                }))
                .unwrap();
        });
        let (trading_tx, mut trading_rx) = mpsc::channel(1);
        let trading_task = tokio::spawn(async move {
            let Some(TradingCommand::Submit(request, response)) = trading_rx.recv().await else {
                panic!("expected order submission");
            };
            assert_eq!(request.symbol, "NQU6");
            assert_eq!(request.exchange, "CME");
            response.send(Ok(())).unwrap();
        });

        let mut submit = vec![0_u8; SUBMIT_NEW_SINGLE_ORDER_SIZE];
        put_u16(&mut submit, 0, SUBMIT_NEW_SINGLE_ORDER_SIZE as u16);
        put_u16(&mut submit, 2, SUBMIT_NEW_SINGLE_ORDER);
        put_fixed_string(&mut submit[4..68], "NQU6-CME");
        put_fixed_string(&mut submit[84..116], "paper-account");
        put_fixed_string(&mut submit[116..148], "sc-nq-1");
        put_i32(&mut submit, 148, 1);
        put_i32(&mut submit, 152, 1);
        put_f64(&mut submit, 176, 1.0);
        put_i32(&mut submit, 184, 1);

        let responses = handle_trading_request(
            SUBMIT_NEW_SINGLE_ORDER,
            &submit,
            &configured,
            Some(&catalog_tx),
            Some(&trading_tx),
        )
        .await
        .unwrap();
        assert!(responses.is_empty());
        catalog_task.await.unwrap();
        trading_task.await.unwrap();
    }

    #[test]
    fn enforces_official_heartbeat_interval_range() {
        for seconds in [0, 4, 61] {
            let error = parse_heartbeat_interval(&logon_request(seconds)).unwrap_err();
            assert!(error.to_string().contains("between 5 and 60"));
        }
        assert_eq!(
            parse_heartbeat_interval(&logon_request(5)).unwrap(),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_heartbeat_interval(&logon_request(60)).unwrap(),
            Duration::from_secs(60)
        );
        assert_eq!(
            heartbeat_timeout(Duration::from_secs(30)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn accepts_sierra_combined_symbol_delimiters() {
        let instrument = Instrument::es("ESU6", "CME").unwrap();
        assert!(instrument.matches("ESU6", "CME"));
        assert!(instrument.matches("ESU6-CME", ""));
        assert!(instrument.matches("ESU6.CME", ""));
        assert!(!instrument.matches("NQU6-CME", ""));
    }

    #[test]
    fn empty_depth_snapshot_uses_official_batch_sentinel() {
        let messages = encode_depth_snapshot(17, &[], 123);
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        assert_eq!(message.len(), MARKET_DEPTH_SNAPSHOT_LEVEL_SIZE);
        assert_eq!(read_u32(message, 4), 17);
        assert_eq!(message[34], 1);
        assert_eq!(message[35], 1);
        assert!(message[8..34].iter().all(|byte| *byte == 0));
        assert!(message[36..].iter().all(|byte| *byte == 0));
    }

    #[tokio::test]
    async fn tcp_session_negotiates_binary_and_logs_on() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream).await.unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&encoding_request(2)).await.unwrap();
        let mut encoding = [0_u8; ENCODING_MESSAGE_SIZE];
        client.read_exact(&mut encoding).await.unwrap();
        assert_eq!(
            i32::from_le_bytes(encoding[8..12].try_into().unwrap()),
            BINARY_ENCODING
        );

        client.write_all(&logon_request(30)).await.unwrap();
        let mut logon = [0_u8; LOGON_RESPONSE_SIZE];
        client.read_exact(&mut logon).await.unwrap();
        assert_eq!(
            u16::from_le_bytes(logon[2..4].try_into().unwrap()),
            LOGON_RESPONSE
        );
        assert_eq!(i32::from_le_bytes(logon[8..12].try_into().unwrap()), 1);
        assert_eq!(logon[237], 0, "trading must not be advertised");
        assert_eq!(logon[247], 0, "market depth is not implemented yet");
        assert_eq!(logon[252], 0, "market data is not implemented yet");

        client.shutdown().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn silent_client_receives_logoff_after_two_heartbeat_intervals() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream).await.unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&encoding_request(0)).await.unwrap();
        assert_eq!(
            u16::from_le_bytes(
                read_wire_message(&mut client).await[2..4]
                    .try_into()
                    .unwrap()
            ),
            ENCODING_RESPONSE
        );
        client.write_all(&logon_request(5)).await.unwrap();
        assert_eq!(
            u16::from_le_bytes(
                read_wire_message(&mut client).await[2..4]
                    .try_into()
                    .unwrap()
            ),
            LOGON_RESPONSE
        );

        let logoff = loop {
            let message = read_wire_message(&mut client).await;
            if u16::from_le_bytes(message[2..4].try_into().unwrap()) == LOGOFF {
                break message;
            }
        };
        assert_eq!(logoff.len(), LOGOFF_SIZE);
        assert_eq!(
            read_fixed_string(&logoff[4..100]).unwrap(),
            "Client heartbeat timeout"
        );
        assert_eq!(logoff[100], 0, "the client may reconnect");

        let mut trailing = [0_u8; 1];
        assert_eq!(client.read(&mut trailing).await.unwrap(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_session_serves_multiple_sequential_history_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (commands_tx, mut commands_rx) = mpsc::channel(1);
        let history = HistoryDataClient::new(commands_tx);
        let instrument = Instrument::es("ESU6", "CME").unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection_with_services(stream, instrument, None, Some(history))
                .await
                .unwrap();
        });
        let worker = tokio::spawn(async move {
            let (request, response) = commands_rx.recv().await.unwrap();
            assert_eq!(request.request_id, 71);
            assert_eq!(request.record_interval, 0);
            assert_eq!(request.start_time, 1_800_000_000);
            response
                .send(Ok(HistoricalResponse {
                    request_id: request.request_id,
                    record_interval: 0,
                    is_final: false,
                    records: vec![HistoricalRecord::Tick {
                        datetime_us: 1_800_000_001_123_456,
                        price: 6500.25,
                        volume: 2.0,
                        at_bid_or_ask: 2,
                    }],
                }))
                .await
                .unwrap();
            response
                .send(Ok(HistoricalResponse {
                    request_id: request.request_id,
                    record_interval: 0,
                    is_final: true,
                    records: vec![HistoricalRecord::Tick {
                        datetime_us: 1_800_000_002_000_000,
                        price: 6500.0,
                        volume: 1.0,
                        at_bid_or_ask: 1,
                    }],
                }))
                .await
                .unwrap();

            let (request, response) = commands_rx.recv().await.unwrap();
            assert_eq!(request.request_id, 72);
            assert_eq!(request.record_interval, 60);
            response
                .send(Ok(HistoricalResponse {
                    request_id: request.request_id,
                    record_interval: 60,
                    is_final: true,
                    records: vec![HistoricalRecord::Bar {
                        start_datetime_us: 1_800_000_000_000_000,
                        open: 6500.0,
                        high: 6501.0,
                        low: 6499.75,
                        close: 6500.25,
                        volume: 123.0,
                        num_trades: 45,
                        bid_volume: 50.0,
                        ask_volume: 73.0,
                    }],
                }))
                .await
                .unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&encoding_request(0)).await.unwrap();
        read_wire_message(&mut client).await;
        client.write_all(&logon_request(30)).await.unwrap();
        let logon = read_wire_message(&mut client).await;
        assert_eq!(logon[245], 1, "history must be advertised");
        assert_eq!(logon[248], 0, "sequential history requests are supported");
        assert_eq!(logon[252], 0, "market data is not present in this test");
        client.write_all(&historical_request(71, 0)).await.unwrap();
        let header = read_wire_message(&mut client).await;
        assert_eq!(header.len(), HISTORICAL_PRICE_DATA_RESPONSE_HEADER_SIZE);
        assert_eq!(read_i32(&header, 4), 71);
        assert_eq!(read_i32(&header, 8), 0);
        assert_eq!(header[13], 0);

        let ask_trade = read_wire_message(&mut client).await;
        assert_eq!(
            ask_trade.len(),
            HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE_SIZE
        );
        assert_eq!(read_i32(&ask_trade, 4), 71);
        assert_eq!(u16::from_le_bytes(ask_trade[16..18].try_into().unwrap()), 2);
        assert_eq!(ask_trade[40], 0);
        let bid_trade = read_wire_message(&mut client).await;
        assert_eq!(u16::from_le_bytes(bid_trade[16..18].try_into().unwrap()), 1);
        assert_eq!(bid_trade[40], 1);

        client.write_all(&historical_request(72, 60)).await.unwrap();
        let second_header = read_wire_message(&mut client).await;
        assert_eq!(
            u16::from_le_bytes(second_header[2..4].try_into().unwrap()),
            HISTORICAL_PRICE_DATA_RESPONSE_HEADER
        );
        assert_eq!(read_i32(&second_header, 4), 72);
        assert_eq!(read_i32(&second_header, 8), 60);
        let bar = read_wire_message(&mut client).await;
        assert_eq!(
            u16::from_le_bytes(bar[2..4].try_into().unwrap()),
            HISTORICAL_PRICE_DATA_RECORD_RESPONSE
        );
        assert_eq!(read_i32(&bar, 4), 72);
        assert_eq!(bar[80], 1);

        client.shutdown().await.unwrap();

        worker.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_trading_snapshots_honor_official_request_filters() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (commands_tx, mut commands_rx) = mpsc::channel(8);
        let (_events_tx, events_rx) = mpsc::channel(8);
        let trading = TradingDataClient::new(commands_tx, events_rx);
        let instrument = Instrument::es("ESU6", "CME").unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection_with_all_services(stream, instrument, None, None, Some(trading))
                .await
                .unwrap();
        });
        let worker = tokio::spawn(async move {
            let orders = vec![
                test_order("basket-paper", "paper-account"),
                test_order("basket-other", "other-account"),
            ];
            match commands_rx.recv().await.unwrap() {
                TradingCommand::OpenOrders(response) => response.send(Ok(orders.clone())).unwrap(),
                other => panic!("unexpected command: {other:?}"),
            }
            match commands_rx.recv().await.unwrap() {
                TradingCommand::OpenOrders(response) => response.send(Ok(orders)).unwrap(),
                other => panic!("unexpected command: {other:?}"),
            }
            match commands_rx.recv().await.unwrap() {
                TradingCommand::Positions(response) => response
                    .send(Ok(vec![
                        TradingPosition {
                            symbol: "ESU6".to_owned(),
                            exchange: "CME".to_owned(),
                            account_id: "paper-account".to_owned(),
                            quantity: 0.0,
                            average_price: 0.0,
                            open_profit_loss: 0.0,
                        },
                        TradingPosition {
                            symbol: "ESU6".to_owned(),
                            exchange: "CME".to_owned(),
                            account_id: "other-account".to_owned(),
                            quantity: 1.0,
                            average_price: 6500.0,
                            open_profit_loss: 10.0,
                        },
                    ]))
                    .unwrap(),
                other => panic!("unexpected command: {other:?}"),
            }
            match commands_rx.recv().await.unwrap() {
                TradingCommand::Balance(response) => response
                    .send(Ok(AccountBalance {
                        account_id: "paper-account".to_owned(),
                        currency: "USD".to_owned(),
                        cash_balance: 100_000.0,
                        available_funds: 99_000.0,
                        open_profit_loss: 0.0,
                        daily_profit_loss: 0.0,
                        trading_disabled: false,
                    }))
                    .unwrap(),
                other => panic!("unexpected command: {other:?}"),
            }
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&encoding_request(0)).await.unwrap();
        read_wire_message(&mut client).await;
        client.write_all(&logon_request(30)).await.unwrap();
        let logon = read_wire_message(&mut client).await;
        assert_eq!(logon[237], 1, "trading must be advertised");

        client
            .write_all(&open_orders_request(91, true, "", "paper-account"))
            .await
            .unwrap();
        let account_order = read_wire_message(&mut client).await;
        assert_eq!(
            u16::from_le_bytes(account_order[2..4].try_into().unwrap()),
            ORDER_UPDATE
        );
        assert_eq!(read_i32(&account_order, 4), 91);
        assert_eq!(read_i32(&account_order, 8), 1);
        assert_eq!(read_i32(&account_order, 12), 1);
        assert_eq!(read_i32(&account_order, 228), 1, "snapshot reason");
        assert_eq!(
            read_fixed_string(&account_order[128..160]).unwrap(),
            "basket-paper"
        );

        client
            .write_all(&open_orders_request(
                92,
                false,
                "basket-other",
                "other-account",
            ))
            .await
            .unwrap();
        let targeted_order = read_wire_message(&mut client).await;
        assert_eq!(read_i32(&targeted_order, 4), 92);
        assert_eq!(
            read_fixed_string(&targeted_order[128..160]).unwrap(),
            "basket-other"
        );

        client
            .write_all(&current_positions_request(93, "paper-account"))
            .await
            .unwrap();
        let position = read_wire_message(&mut client).await;
        assert_eq!(
            u16::from_le_bytes(position[2..4].try_into().unwrap()),
            POSITION_UPDATE
        );
        assert_eq!(read_i32(&position, 4), 93);
        assert_eq!(
            read_fixed_string(&position[144..176]).unwrap(),
            "paper-account"
        );

        client
            .write_all(&account_balance_request(94, "unknown-account"))
            .await
            .unwrap();
        let balance_reject = read_wire_message(&mut client).await;
        assert_eq!(
            u16::from_le_bytes(balance_reject[2..4].try_into().unwrap()),
            ACCOUNT_BALANCE_REJECT
        );
        assert_eq!(read_i32(&balance_reject, 4), 94);
        assert!(
            read_fixed_string(&balance_reject[8..104])
                .unwrap()
                .contains("Unknown Paper trade account")
        );

        client.shutdown().await.unwrap();
        worker.await.unwrap();
        server.await.unwrap();
    }

    #[test]
    fn historical_bar_layout_carries_footprint_volumes() {
        let message = historical_bar_record(
            9,
            1_800_000_000_000_000,
            6500.0,
            6501.0,
            6499.75,
            6500.25,
            123.0,
            45,
            50.0,
            73.0,
            true,
        );
        assert_eq!(message.len(), HISTORICAL_PRICE_DATA_RECORD_RESPONSE_SIZE);
        assert_eq!(u16::from_le_bytes(message[2..4].try_into().unwrap()), 803);
        assert_eq!(read_i32(&message, 4), 9);
        assert_eq!(read_i64(&message, 8), 1_800_000_000_000_000);
        assert_eq!(read_u32(&message, 56), 45);
        assert_eq!(
            f64::from_le_bytes(message[64..72].try_into().unwrap()),
            50.0
        );
        assert_eq!(
            f64::from_le_bytes(message[72..80].try_into().unwrap()),
            73.0
        );
        assert_eq!(message[80], 1);
    }

    #[tokio::test]
    async fn tcp_session_serves_security_definition_trade_and_bbo() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (commands_tx, mut commands_rx) = mpsc::channel(4);
        let (events_tx, events_rx) = mpsc::channel(4);
        let market = MarketDataClient::with_logon_catalog(commands_tx, events_rx);
        let instrument = Instrument::es("ESU6", "CME").unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection_with_market(stream, instrument, Some(market))
                .await
                .unwrap();
        });
        let worker = tokio::spawn(async move {
            match commands_rx.recv().await.unwrap() {
                MarketCommand::LoadCatalog {
                    preferred_underlying,
                    response,
                } => {
                    assert_eq!(preferred_underlying, "ES");
                    let mut nq = Instrument::es("ESU6", "CME").unwrap();
                    nq.symbol = "NQU6".to_owned();
                    nq.underlying_symbol = "NQ".to_owned();
                    nq.description = "E-mini Nasdaq-100 Futures".to_owned();
                    nq.currency_value_per_increment = 5.0;
                    nq.contract_size = 20.0;
                    response.send(Ok(vec![nq])).unwrap();
                }
                other => panic!("expected catalog preload, received {other:?}"),
            }
            match commands_rx.recv().await.unwrap() {
                MarketCommand::Subscribe {
                    symbol_id,
                    symbol,
                    exchange,
                    response,
                } => {
                    assert_eq!(
                        (symbol_id, symbol.as_str(), exchange.as_str()),
                        (9, "ESU6", "CME")
                    );
                    response.send(Ok(MarketSnapshot::default())).unwrap();
                    events_tx
                        .send(MarketEvent::LastTrade {
                            symbol_id,
                            price: 6500.25,
                            volume: 2.0,
                            datetime_us: 1_800_000_000_000_001,
                            at_bid_or_ask: 2,
                            is_snapshot: false,
                        })
                        .await
                        .unwrap();
                    events_tx
                        .send(MarketEvent::BestBidAsk {
                            symbol_id,
                            bid_price: 6500.0,
                            bid_quantity: 10.0,
                            ask_price: 6500.25,
                            ask_quantity: 11.0,
                            datetime_us: 1_800_000_000_000_002,
                        })
                        .await
                        .unwrap();
                    events_tx
                        .send(MarketEvent::FeedStatus { available: false })
                        .await
                        .unwrap();
                    events_tx
                        .send(MarketEvent::FeedStatus { available: true })
                        .await
                        .unwrap();
                }
                MarketCommand::LoadCatalog { .. }
                | MarketCommand::Unsubscribe { .. }
                | MarketCommand::SubscribeDepth { .. }
                | MarketCommand::UnsubscribeDepth { .. }
                | MarketCommand::ListCatalogExchanges { .. }
                | MarketCommand::EnumerateCatalog { .. }
                | MarketCommand::Snapshot { .. }
                | MarketCommand::DepthSnapshot { .. }
                | MarketCommand::SearchCatalog { .. }
                | MarketCommand::ResolveCatalogInstrument { .. } => panic!("unexpected command"),
            }
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&encoding_request(0)).await.unwrap();
        assert_eq!(read_wire_message(&mut client).await.len(), 16);
        client.write_all(&logon_request(30)).await.unwrap();
        let logon = read_wire_message(&mut client).await;
        assert_eq!(logon[244], 1);
        assert_eq!(logon[252], 1);
        let mut catalog_symbols = Vec::new();
        for index in 0..2 {
            let definition = read_wire_message(&mut client).await;
            assert_eq!(read_i32(&definition, 4), 0);
            assert_eq!(definition[168], u8::from(index == 1));
            catalog_symbols.push(read_fixed_string(&definition[8..72]).unwrap());
        }
        assert_eq!(catalog_symbols, ["ESU6", "NQU6"]);
        client
            .write_all(&security_definition_request(77))
            .await
            .unwrap();
        let definition = read_wire_message(&mut client).await;
        assert_eq!(definition.len(), 432);
        assert_eq!(
            u16::from_le_bytes(definition[2..4].try_into().unwrap()),
            507
        );
        assert_eq!(read_i32(&definition, 4), 77);

        client.write_all(&market_data_request(9)).await.unwrap();
        let snapshot = read_wire_message(&mut client).await;
        assert_eq!(u16::from_le_bytes(snapshot[2..4].try_into().unwrap()), 104);
        assert_eq!(read_u32(&snapshot, 4), 9);

        let updates = [
            read_wire_message(&mut client).await,
            read_wire_message(&mut client).await,
            read_wire_message(&mut client).await,
            read_wire_message(&mut client).await,
        ];
        let types: Vec<_> = updates
            .iter()
            .map(|message| u16::from_le_bytes(message[2..4].try_into().unwrap()))
            .collect();
        assert!(types.contains(&147));
        assert!(types.contains(&148));
        let statuses: Vec<_> = updates
            .iter()
            .filter(|message| {
                u16::from_le_bytes(message[2..4].try_into().unwrap()) == MARKET_DATA_FEED_STATUS
            })
            .map(|message| read_i32(message, 4))
            .collect();
        assert_eq!(statuses, [1, 2]);

        client.shutdown().await.unwrap();
        worker.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_session_serves_aggregated_depth_snapshot_and_update() {
        use crate::order_book::{LevelUpdateType, Side};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (commands_tx, mut commands_rx) = mpsc::channel(4);
        let (events_tx, events_rx) = mpsc::channel(4);
        let market = MarketDataClient::new(commands_tx, events_rx);
        let instrument = Instrument::es("ESU6", "CME").unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection_with_market(stream, instrument, Some(market))
                .await
                .unwrap();
        });
        let worker = tokio::spawn(async move {
            match commands_rx.recv().await.unwrap() {
                MarketCommand::SubscribeDepth {
                    symbol_id,
                    max_levels,
                    response,
                    ..
                } => {
                    assert_eq!((symbol_id, max_levels), (11, 1400));
                    response
                        .send(Ok(vec![
                            DepthLevel {
                                side: Side::Bid,
                                price: 6500.0,
                                quantity: 12.0,
                                num_orders: 3,
                                level: 1,
                            },
                            DepthLevel {
                                side: Side::Ask,
                                price: 6500.25,
                                quantity: 9.0,
                                num_orders: 2,
                                level: 1,
                            },
                        ]))
                        .unwrap();
                    events_tx
                        .send(MarketEvent::DepthUpdate {
                            symbol_id,
                            update: LevelUpdate {
                                side: Side::Bid,
                                price: 6500.0,
                                quantity: 14.0,
                                num_orders: 4,
                                level: 1,
                                update_type: LevelUpdateType::Update,
                            },
                            datetime_us: 1_800_000_000_123_000,
                            is_final: true,
                        })
                        .await
                        .unwrap();
                }
                _ => panic!("unexpected command"),
            }
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&encoding_request(0)).await.unwrap();
        read_wire_message(&mut client).await;
        client.write_all(&logon_request(30)).await.unwrap();
        let logon = read_wire_message(&mut client).await;
        assert_eq!(logon[247], 1, "market depth must be advertised");
        client
            .write_all(&market_depth_request(11, 1400))
            .await
            .unwrap();
        let bid = read_wire_message(&mut client).await;
        let ask = read_wire_message(&mut client).await;
        assert_eq!(bid.len(), 56);
        assert_eq!(u16::from_le_bytes(bid[2..4].try_into().unwrap()), 122);
        assert_eq!(u16::from_le_bytes(bid[8..10].try_into().unwrap()), 1);
        assert_eq!(bid[34], 1);
        assert_eq!(bid[35], 0);
        assert_eq!(u32::from_le_bytes(bid[48..52].try_into().unwrap()), 3);
        assert_eq!(u16::from_le_bytes(ask[8..10].try_into().unwrap()), 2);
        assert_eq!(ask[35], 1);

        let update = read_wire_message(&mut client).await;
        assert_eq!(update.len(), 39);
        assert_eq!(u16::from_le_bytes(update[2..4].try_into().unwrap()), 109);
        assert_eq!(update[36], 1);
        assert_eq!(update[37], 4);
        assert_eq!(update[38], 1);

        client.shutdown().await.unwrap();
        worker.await.unwrap();
        server.await.unwrap();
    }

    #[test]
    fn es_security_definition_uses_official_fixed_binary_offsets() {
        let instrument = Instrument::es("ESU6", "CME").unwrap();
        let response = security_definition_response(42, &instrument);
        assert_eq!(response.len(), 432);
        assert_eq!(u16::from_le_bytes(response[2..4].try_into().unwrap()), 507);
        assert_eq!(read_i32(&response, 4), 42);
        assert_eq!(read_fixed_string(&response[8..72]).unwrap(), "ESU6");
        assert_eq!(read_fixed_string(&response[72..88]).unwrap(), "CME");
        assert_eq!(read_fixed_string(&response[180..212]).unwrap(), "ES");
        assert_eq!(read_fixed_string(&response[368..432]).unwrap(), "ES");
        assert_eq!(read_i32(&response, 88), 1);
        assert_eq!(
            f32::from_le_bytes(response[156..160].try_into().unwrap()),
            0.25
        );
        assert_eq!(
            f32::from_le_bytes(response[164..168].try_into().unwrap()),
            12.5
        );
        assert_eq!(response[168], 1);
        assert_eq!(response[252], 1);
        assert_eq!(
            f32::from_le_bytes(response[340..344].try_into().unwrap()),
            50.0
        );
    }

    #[test]
    fn unsolicited_security_definition_uses_zero_request_id_for_symbol_registration() {
        let instrument = Instrument::es("ESU6", "CME").unwrap();
        let response = security_definition_response(0, &instrument);
        assert_eq!(read_i32(&response, 4), 0);
        assert_eq!(read_fixed_string(&response[8..72]).unwrap(), "ESU6");
        assert_eq!(read_fixed_string(&response[72..88]).unwrap(), "CME");
        assert_eq!(response[168], 1);
    }

    #[test]
    fn realtime_trade_and_bbo_use_current_v2_messages() {
        let trade = encode_market_event(MarketEvent::LastTrade {
            symbol_id: 7,
            price: 6500.25,
            volume: 3.0,
            datetime_us: 1_800_000_000_123_456,
            at_bid_or_ask: 2,
            is_snapshot: false,
        });
        assert_eq!(trade.len(), 40);
        assert_eq!(u16::from_le_bytes(trade[2..4].try_into().unwrap()), 147);
        assert_eq!(read_u32(&trade, 4), 7);
        assert_eq!(trade[32], 2);

        let bbo = encode_market_event(MarketEvent::BestBidAsk {
            symbol_id: 7,
            bid_price: 6500.0,
            bid_quantity: 10.0,
            ask_price: 6500.25,
            ask_quantity: 12.0,
            datetime_us: 1_800_000_000_123_456,
        });
        assert_eq!(bbo.len(), 48);
        assert_eq!(u16::from_le_bytes(bbo[2..4].try_into().unwrap()), 148);
    }

    #[test]
    fn feed_availability_uses_official_status_message() {
        let unavailable = encode_market_event(MarketEvent::FeedStatus { available: false });
        assert_eq!(unavailable, [8, 0, 100, 0, 1, 0, 0, 0]);

        let available = encode_market_event(MarketEvent::FeedStatus { available: true });
        assert_eq!(available, [8, 0, 100, 0, 2, 0, 0, 0]);
    }
}
