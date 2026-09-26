use std::error::Error;

use rithmic_dtc_bridge::{connection::SharedConnection, dtc_client, terminal};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let address = dtc_client::address_from_env();
    let (market, history) = dtc_client::clients(address.clone());
    let trading = dtc_client::trading_client(address.clone());
    println!("Web terminal using DTC gateway at {address}");

    // The settings object is retained for the existing account UI while the
    // actual market/history transport is exclusively DTC.
    let order_connection = SharedConnection::from_saved_or_trading_defaults();
    terminal::serve(market, history, Some(trading), order_connection).await
}
