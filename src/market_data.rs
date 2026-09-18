//! Merge partial upstream quotes/statistics without inventing missing values.
use rithmic_rs::rti::messages::RithmicMessage;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MarketSnapshot {
    pub(crate) settlement: Option<f64>,
    pub(crate) open: Option<f64>,
    pub(crate) high: Option<f64>,
    pub(crate) low: Option<f64>,
    pub(crate) volume: Option<f64>,
    pub(crate) open_interest: Option<u32>,
    pub(crate) bid: Option<f64>,
    pub(crate) ask: Option<f64>,
    pub(crate) bid_size: Option<f64>,
    pub(crate) ask_size: Option<f64>,
    pub(crate) last: Option<f64>,
    pub(crate) last_size: Option<f64>,
    pub(crate) last_time_us: i64,
    pub(crate) quote_time_us: i64,
    pub(crate) settlement_date: u32,
}

pub(crate) fn key(message: &RithmicMessage) -> Option<(&str, &str)> {
    let (symbol, exchange) = match message {
        RithmicMessage::LastTrade(v) => (&v.symbol, &v.exchange),
        RithmicMessage::BestBidOffer(v) => (&v.symbol, &v.exchange),
        RithmicMessage::TradeStatistics(v) => (&v.symbol, &v.exchange),
        RithmicMessage::OpenInterest(v) => (&v.symbol, &v.exchange),
        RithmicMessage::EndOfDayPrices(v) => (&v.symbol, &v.exchange),
        _ => return None,
    };
    Some((symbol.as_deref()?, exchange.as_deref()?))
}

fn update(target: &mut Option<f64>, value: Option<f64>, clear: bool) {
    if clear {
        *target = None;
    } else if let Some(value) = value.filter(|v| v.is_finite()) {
        *target = Some(value);
    }
}

fn timestamp(seconds: Option<i32>, micros: Option<i32>) -> i64 {
    i64::from(seconds.unwrap_or(0)) * 1_000_000 + i64::from(micros.unwrap_or(0).clamp(0, 999_999))
}

impl MarketSnapshot {
    pub(crate) fn apply(&mut self, message: &RithmicMessage) {
        match message {
            RithmicMessage::LastTrade(v) => {
                let clear = v.clear_bits.unwrap_or(0);
                update(&mut self.last, v.trade_price, clear & 1 != 0);
                update(
                    &mut self.last_size,
                    v.trade_size.map(f64::from),
                    clear & 1 != 0,
                );
                update(&mut self.volume, v.volume.map(|n| n as f64), clear & 8 != 0);
                if clear & 1 != 0 {
                    self.last_time_us = 0;
                } else if v.trade_price.is_some() {
                    self.last_time_us =
                        timestamp(v.source_ssboe.or(v.ssboe), v.source_usecs.or(v.usecs));
                }
            }
            RithmicMessage::BestBidOffer(v) => {
                let clear = v.clear_bits.unwrap_or(0);
                update(&mut self.bid, v.bid_price, clear & 1 != 0);
                update(
                    &mut self.bid_size,
                    v.bid_size.map(|n| n.max(0) as f64),
                    clear & 1 != 0,
                );
                update(&mut self.ask, v.ask_price, clear & 2 != 0);
                update(
                    &mut self.ask_size,
                    v.ask_size.map(|n| n.max(0) as f64),
                    clear & 2 != 0,
                );
                self.quote_time_us = timestamp(v.ssboe, v.usecs);
            }
            RithmicMessage::TradeStatistics(v) => {
                let clear = v.clear_bits.unwrap_or(0);
                update(&mut self.open, v.open_price, clear & 1 != 0);
                update(&mut self.high, v.high_price, clear & 2 != 0);
                update(&mut self.low, v.low_price, clear & 4 != 0);
            }
            RithmicMessage::OpenInterest(v) => {
                if v.should_clear == Some(true) {
                    self.open_interest = None;
                } else if let Some(value) = v.open_interest {
                    self.open_interest = Some(value.min(u64::from(u32::MAX - 1)) as u32);
                }
            }
            RithmicMessage::EndOfDayPrices(v) => {
                let clear = v.clear_bits.unwrap_or(0) & 2 != 0;
                update(&mut self.settlement, v.settlement_price, clear);
                if clear {
                    self.settlement_date = 0;
                } else if let Some(date) = &v.settlement_date {
                    self.settlement_date = date_to_unix(date).unwrap_or(0);
                }
            }
            _ => {}
        }
    }
}

/// Rithmic calendar dates (YYYYMMDD or YYYY-MM-DD) to DTC UTC midnight.
pub(crate) fn date_to_unix(value: &str) -> Option<u32> {
    let value = value.trim();
    let digits = if value.len() == 8 && value.bytes().all(|c| c.is_ascii_digit()) {
        value.to_owned()
    } else if value.len() == 10 && value.as_bytes()[4] == b'-' && value.as_bytes()[7] == b'-' {
        value.chars().filter(|c| *c != '-').collect()
    } else {
        return None;
    };
    if digits.len() != 8 || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let year: i64 = digits[..4].parse().ok()?;
    let month: i64 = digits[4..6].parse().ok()?;
    let day: i64 = digits[6..8].parse().ok()?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&month) || day < 1 || day > days[(month - 1) as usize] {
        return None;
    }
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = month + if month > 2 { -3 } else { 9 };
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + (153 * m + 2) / 5 + day - 1;
    u32::try_from((era * 146097 + doe - 719468) * 86400).ok()
}
