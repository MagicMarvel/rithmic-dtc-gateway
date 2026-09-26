use rithmic_dtc_bridge::{
    connection::SharedConnection, history_feed::RithmicHistoryFeed, rithmic_feed::RithmicFeed,
    terminal, trading_feed::RithmicTradingFeed,
};
use std::{env, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    // Market data and history are server-owned and always use the server's
    // environment configuration. Browser account changes never mutate this
    // connection or reveal its credentials.
    let data_connection = SharedConnection::from_env();
    let feed = RithmicFeed::pending_with(data_connection.clone()).await?;
    let history = RithmicHistoryFeed::pending_with(data_connection).await?;

    // Orders use a separate, browser-configurable Rithmic connection. With no
    // saved user account the terminal remains read-only while server data keeps
    // streaming normally.
    let order_connection = SharedConnection::from_saved_or_trading_defaults();
    let trading_allowed = env::var("RITHMIC_ENABLE_TRADING")
        .is_ok_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
    let trading = if trading_allowed && !order_connection.settings().user.trim().is_empty() {
        let settings = order_connection.settings();
        let config = settings.to_config().map_err(std::io::Error::other)?;
        let account = settings.to_account().map_err(std::io::Error::other)?;
        match RithmicTradingFeed::connect(config, account).await {
            Ok(feed) => Some(feed),
            Err(error) => {
                eprintln!("[Trading] {error}; continuing in read-only mode");
                None
            }
        }
    } else {
        None
    };
    terminal::serve(
        feed.client(),
        history.client(),
        trading.map(|v| v.client()),
        order_connection,
    )
    .await
}
