use std::{
    env,
    error::Error,
    fmt,
    time::{Duration, SystemTime},
};

use rithmic_rs::{
    ConnectStrategy, LoginConfig, RithmicConfig, RithmicEnv, RithmicError, RithmicHistoryPlant,
    RithmicHistoryPlantHandle, TimeBarType, rti::messages::RithmicMessage,
};
use tokio::sync::mpsc;

use crate::{
    dtc::{HistoricalRecord, HistoricalRequest, HistoricalResponse, HistoryDataClient},
    identity::synthetic_mac,
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
struct LoadHistoryError {
    message: String,
    connection_issue: bool,
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

impl RithmicHistoryFeed {
    pub async fn connect_from_env() -> Result<Self, HistoryError> {
        let environment =
            parse_environment(&env::var("RITHMIC_ENV").unwrap_or_else(|_| "demo".to_owned()))?;
        let config = RithmicConfig::from_env(environment).map_err(|error| {
            HistoryError(format!("Rithmic history configuration failed: {error}"))
        })?;
        let initial_session = establish_history_session(&config).await?;

        let (commands, mut receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut session = Some(initial_session);
            while let Some(command) = receiver.recv().await {
                match command {
                    HistoryCommand::Load { request, response } => {
                        if session.is_none() {
                            session = establish_history_session(&config).await.ok();
                        }
                        let result = match session.as_ref() {
                            Some(active) => {
                                stream_history(&active.handle, request, &response).await
                            }
                            None => Err(LoadHistoryError::local(
                                "Rithmic History Plant is unavailable after reconnect",
                            )),
                        };
                        if result.as_ref().is_err_and(|error| error.connection_issue) {
                            eprintln!(
                                "[History] Connection lost; reconnecting the History Plant for the next request"
                            );
                            if let Some(stale) = session.take() {
                                stale.handle.abort();
                                let _ = stale.plant.await_shutdown().await;
                            }
                            match establish_history_session(&config).await {
                                Ok(reconnected) => {
                                    session = Some(reconnected);
                                }
                                Err(error) => {
                                    eprintln!("[History] History Plant reconnect failed: {error}");
                                }
                            }
                        }
                        if let Err(error) = result {
                            let _ = response.send(Err(error.message)).await;
                        }
                    }
                }
            }
            if let Some(active) = session {
                let _ = active.handle.disconnect().await;
                let _ = active.plant.await_shutdown().await;
            }
        });
        Ok(Self { commands })
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

async fn stream_history(
    handle: &RithmicHistoryPlantHandle,
    request: HistoricalRequest,
    output: &mpsc::Sender<Result<HistoricalResponse, String>>,
) -> Result<(), LoadHistoryError> {
    let (start, end) = historical_range(&request)?;
    if request.record_interval != 0 {
        let mut response = load_history_with_timeout(handle, request).await?;
        response.is_final = true;
        output
            .send(Ok(response))
            .await
            .map_err(|_| LoadHistoryError::local("historical client disconnected"))?;
        return Ok(());
    }

    let chunk_seconds = history_tick_chunk_seconds();
    let mut cursor = start;
    let mut pending: Option<HistoricalResponse> = None;
    while cursor <= end {
        let chunk_end = cursor
            .saturating_add(chunk_seconds.saturating_sub(1))
            .min(end);
        let mut chunk_request = request.clone();
        chunk_request.start_time = cursor;
        chunk_request.end_time = chunk_end;
        chunk_request.max_days = 0;
        let response = load_history_with_timeout(handle, chunk_request).await?;
        if !response.records.is_empty() {
            if let Some(previous) = pending.replace(response) {
                output
                    .send(Ok(previous))
                    .await
                    .map_err(|_| LoadHistoryError::local("historical client disconnected"))?;
            }
        }
        if chunk_end == end {
            break;
        }
        cursor = chunk_end.saturating_add(1);
    }

    let mut final_response = pending.unwrap_or(HistoricalResponse {
        request_id: request.request_id,
        record_interval: request.record_interval,
        records: Vec::new(),
        is_final: false,
    });
    final_response.is_final = true;
    output
        .send(Ok(final_response))
        .await
        .map_err(|_| LoadHistoryError::local("historical client disconnected"))
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
    let start = if request.start_time > 0 {
        request.start_time
    } else if request.max_days > 0 {
        end.saturating_sub(i64::from(request.max_days) * 86_400)
    } else {
        1
    };
    if start <= 0 || end <= 0 || start > end {
        return Err(LoadHistoryError::local("invalid historical time range"));
    }
    Ok((start, end))
}

fn history_tick_chunk_seconds() -> i64 {
    let hours = env::var("RITHMIC_HISTORY_TICK_CHUNK_HOURS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|hours| *hours > 0)
        .unwrap_or(6);
    i64::from(hours).saturating_mul(3_600)
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

    let mut responses = if request.record_interval == 0 {
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

fn parse_environment(value: &str) -> Result<RithmicEnv, HistoryError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "demo" => Ok(RithmicEnv::Demo),
        "live" => Ok(RithmicEnv::Live),
        "test" => Ok(RithmicEnv::Test),
        _ => Err(HistoryError(
            "RITHMIC_ENV must be demo, live, or test".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
