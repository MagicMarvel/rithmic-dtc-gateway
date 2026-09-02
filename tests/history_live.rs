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
