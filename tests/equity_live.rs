use std::time::Duration;

use rithmic_dtc_bridge::identity::synthetic_mac;
use rithmic_rs::{
    ConnectStrategy, LoginConfig, RithmicConfig, RithmicEnv, RithmicTickerPlant,
    rti::{messages::RithmicMessage, request_search_symbols},
};
use tokio::time::{Instant, timeout};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "read-only probe for Rithmic Paper US equity search and real-time permission"]
async fn paper_trading_us_equity_search_and_realtime_probe() {
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("RITHMIC_ENV")
            .unwrap_or_else(|_| "demo".to_owned())
            .to_ascii_lowercase(),
        "demo",
        "the equity permission probe is restricted to Paper Trading"
    );
    let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();
    let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple)
        .await
        .unwrap();
    let mut handle = plant.get_handle();
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    handle.login_with_config(login).await.unwrap();
    let requested_symbol =
        std::env::var("RITHMIC_EQUITY_TEST_SYMBOL").unwrap_or_else(|_| "AAPL".to_owned());

    let responses = handle
        .search_symbols(
            &requested_symbol,
            None,
            None,
            Some(request_search_symbols::InstrumentType::Equity),
            Some(request_search_symbols::Pattern::Equals),
        )
        .await
        .unwrap();
    let mut matches = Vec::new();
    for response in responses {
        if let Some(error) = response.error {
            panic!("Rithmic equity search rejected: {error}");
        }
        if let RithmicMessage::ResponseSearchSymbols(found) = response.message {
            if let (Some(symbol), Some(exchange)) = (found.symbol, found.exchange) {
                matches.push((symbol, exchange));
            }
        }
    }
    assert!(
        !matches.is_empty(),
        "Rithmic Paper returned no EQUITY match for {requested_symbol}"
    );
    eprintln!("Rithmic Paper {requested_symbol} equity matches: {matches:?}");

    let (symbol, exchange) = matches[0].clone();
    let subscription = handle.subscribe(&symbol, &exchange).await.unwrap();
    assert!(
        subscription.error.is_none(),
        "Rithmic {requested_symbol} real-time subscription rejected: {:?}",
        subscription.error
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut saw_trade = false;
    let mut saw_bbo = false;
    while !(saw_trade && saw_bbo) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "{requested_symbol} subscription was accepted but no complete Last Trade/BBO evidence arrived"
        );
        let response = timeout(remaining, handle.subscription_receiver.recv())
            .await
            .expect("timed out waiting for US equity real-time data")
            .expect("Rithmic US equity subscription channel closed");
        if let Some(error) = response.error {
            panic!("Rithmic {requested_symbol} stream failed: {error}");
        }
        match response.message {
            RithmicMessage::LastTrade(_) => saw_trade = true,
            RithmicMessage::BestBidOffer(_) => saw_bbo = true,
            _ => {}
        }
    }

    let _ = handle.disconnect().await;
    let _ = plant.await_shutdown().await;
}
