use std::{env, error::Error, sync::Arc};

use rithmic_dtc_bridge::{
    dtc,
    history_feed::RithmicHistoryFeed,
    identity::synthetic_mac,
    options::{OptionsConfig, OptionsService},
    rithmic_feed::RithmicFeed,
    trading_feed::RithmicTradingFeed,
};
use tokio::net::TcpListener;

const DEFAULT_LISTEN_ADDRESS: &str = "127.0.0.1:11099";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    println!("Rithmic synthetic machine identity: {}", synthetic_mac());
    let symbol = env::var("RITHMIC_DTC_SYMBOL").or_else(|_| env::var("RITHMIC_PROBE_SYMBOL"))?;
    let exchange =
        env::var("RITHMIC_DTC_EXCHANGE").or_else(|_| env::var("RITHMIC_PROBE_EXCHANGE"))?;
    let instrument = dtc::Instrument::es(symbol, exchange)?;
    let options_enabled = env::var("RITHMIC_ENABLE_OPTIONS").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    });
    let options = if options_enabled {
        let configs = OptionsConfig::markets_from_env()?;
        let service = Arc::new(OptionsService::prepare(&configs)?);
        let http_service = Arc::clone(&service);
        tokio::spawn(async move {
            if let Err(error) = http_service.serve().await {
                eprintln!("[Options] HTTP service stopped: {error}");
            }
        });
        Some((service, configs))
    } else {
        None
    };
    let feed = RithmicFeed::connect_from_env().await?;
    let history_feed = RithmicHistoryFeed::connect_from_env().await?;
    println!(
        "Rithmic login accepted; serving {}.{}",
        instrument.symbol, instrument.exchange
    );

    if let Some((service, configs)) = options {
        let options_feed = RithmicFeed::connect_from_env().await?;
        let options_client = options_feed.client();
        let option_markets = configs
            .into_iter()
            .map(|config| (history_feed.client(), config))
            .collect();
        service.start_collector(options_client, option_markets);
    }

    let address = env::var("DTC_LISTEN_ADDR").unwrap_or_else(|_| DEFAULT_LISTEN_ADDRESS.to_owned());
    let listener = TcpListener::bind(&address).await?;
    let actual_address = listener.local_addr()?;
    println!("DTC binary market-data server listening on {actual_address}");
    let market_factory = std::sync::Arc::new(move || feed.client());
    let history_factory = std::sync::Arc::new(move || history_feed.client());
    let trading_enabled = env::var("RITHMIC_ENABLE_TRADING").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    });
    if trading_enabled {
        let trading_feed = RithmicTradingFeed::connect_from_env().await?;
        println!(
            "Aggregated L2, historical data, and Paper-only single-order trading are enabled."
        );
        let trading_factory = std::sync::Arc::new(move || trading_feed.client());
        dtc::serve_with_trading(
            listener,
            instrument,
            market_factory,
            history_factory,
            trading_factory,
        )
        .await?;
    } else {
        println!("Aggregated L2 and historical price data are enabled; trading remains disabled.");
        dtc::serve(listener, instrument, market_factory, history_factory).await?;
    }
    Ok(())
}
