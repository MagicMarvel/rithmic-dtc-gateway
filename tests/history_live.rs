use std::time::{SystemTime, UNIX_EPOCH};

use rithmic_dtc_bridge::identity::synthetic_mac;
use rithmic_rs::{
    ConnectStrategy, LoginConfig, RithmicConfig, RithmicEnv, RithmicHistoryPlant,
    rti::messages::RithmicMessage,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires configured Rithmic Paper Trading credentials and ES history permission"]
async fn paper_trading_history_plant_returns_classified_es_ticks() {
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("RITHMIC_ENV")
            .unwrap_or_else(|_| "demo".to_owned())
            .to_ascii_lowercase(),
        "demo",
        "live history tests are restricted to Paper Trading"
    );
    let symbol = std::env::var("RITHMIC_DTC_SYMBOL")
        .or_else(|_| std::env::var("RITHMIC_PROBE_SYMBOL"))
        .unwrap();
    let exchange = std::env::var("RITHMIC_DTC_EXCHANGE")
        .or_else(|_| std::env::var("RITHMIC_PROBE_EXCHANGE"))
        .unwrap();
    let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();
    let plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Simple)
        .await
        .unwrap();
    let handle = plant.get_handle();
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    handle.login_with_config(login).await.unwrap();

    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i32
        - 10;
    let responses = handle
        .load_ticks_all(symbol, exchange, end - 120, end)
        .await
        .unwrap();
    let ticks: Vec<_> = responses
        .iter()
        .filter_map(|response| match &response.message {
            RithmicMessage::ResponseTickBarReplay(tick) if tick.close_price.is_some() => Some(tick),
            _ => None,
        })
        .collect();
    for response in &responses {
        assert!(
            response.error.is_none(),
            "History response rejected: {}",
            response.error.as_ref().unwrap()
        );
    }
    assert!(
        !ticks.is_empty(),
        "recent active ES window must contain ticks"
    );
    assert!(ticks.windows(2).all(|pair| {
        let left = pair[0]
            .data_bar_ssboe
            .get(1)
            .zip(pair[0].data_bar_usecs.get(1));
        let right = pair[1]
            .data_bar_ssboe
            .get(1)
            .zip(pair[1].data_bar_usecs.get(1));
        left <= right
    }));
    assert!(ticks.iter().any(|tick| {
        tick.bid_volume.unwrap_or_default() > 0 || tick.ask_volume.unwrap_or_default() > 0
    }));

    let _ = handle.disconnect().await;
    let _ = plant.await_shutdown().await;
}

/// Read-only entitlement comparison: same windows across two fresh logins.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Paper credentials; read-only historical permission diagnostic"]
async fn paper_history_permission_matrix() {
    use rithmic_rs::TimeBarType;
    use std::time::Duration;
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("RITHMIC_ENV")
            .unwrap_or_else(|_| "demo".into())
            .to_ascii_lowercase(),
        "demo"
    );
    let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();
    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i32
        - 60;
    let mut rejected = 0;
    let mut records_total = 0;
    for session in 1..=2 {
        let plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Simple)
            .await
            .unwrap();
        let handle = plant.get_handle();
        let mut login = LoginConfig::default();
        login.mac_addr = Some(vec![synthetic_mac()]);
        handle.login_with_config(login).await.unwrap();
        for (symbol, exchange, kind) in [
            ("ESU6", "CME", 0),
            ("ESU6", "CME", 60),
            ("NQU6", "CME", 86400),
            ("GCZ6", "COMEX", 0),
        ] {
            let start = end - if kind == 86400 { 7 * 86400 } else { 600 };
            let result = tokio::time::timeout(Duration::from_secs(30), async {
                if kind == 0 {
                    handle
                        .load_ticks_all(symbol.to_owned(), exchange.to_owned(), start, end)
                        .await
                } else {
                    handle
                        .load_time_bars_all(
                            symbol.to_owned(),
                            exchange.to_owned(),
                            if kind == 86400 {
                                TimeBarType::DailyBar
                            } else {
                                TimeBarType::MinuteBar
                            },
                            1,
                            start,
                            end,
                        )
                        .await
                }
            })
            .await;
            match result {
                Ok(Ok(responses)) => {
                    let mut count = 0;
                    for response in responses {
                        if let Some(error) = response.error {
                            rejected += 1;
                            eprintln!(
                                "session={session} {symbol}.{exchange} interval={kind} start={start} end={end} ERROR {error}"
                            );
                        } else {
                            count += usize::from(match response.message {
                                RithmicMessage::ResponseTickBarReplay(v) => v.close_price.is_some(),
                                RithmicMessage::ResponseTimeBarReplay(v) => v.close_price.is_some(),
                                _ => false,
                            });
                        }
                    }
                    records_total += count;
                    eprintln!(
                        "session={session} {symbol}.{exchange} interval={kind} records={count}"
                    );
                }
                Ok(Err(error)) => {
                    rejected += 1;
                    eprintln!(
                        "session={session} {symbol}.{exchange} interval={kind} ERROR {error}"
                    );
                }
                Err(_) => {
                    rejected += 1;
                    eprintln!("session={session} {symbol}.{exchange} interval={kind} TIMEOUT");
                }
            }
        }
        let _ = handle.disconnect().await;
        let _ = plant.await_shutdown().await;
    }
    assert_eq!(
        rejected, 0,
        "historical requests rejected across two fresh sessions"
    );
    assert!(records_total > 0, "no historical records returned");
}
