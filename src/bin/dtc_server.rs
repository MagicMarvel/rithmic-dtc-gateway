use std::{env, error::Error, sync::Arc};

use rithmic_dtc_bridge::{
    dtc,
    dtc_accounts::{DtcAccountAdmin, DtcAccounts},
    history_feed::RithmicHistoryFeed,
    identity::synthetic_mac,
    rithmic_feed::RithmicFeed,
    trading_feed::RithmicTradingFeed,
};
use rithmic_rs::RithmicEnv;
use tokio::net::TcpListener;

const DEFAULT_LISTEN_ADDRESS: &str = "127.0.0.1:11099";

#[derive(Clone)]
struct DtcServices {
    market: Arc<RithmicFeed>,
    history: Arc<RithmicHistoryFeed>,
    trading: Option<Arc<RithmicTradingFeed>>,
}

const HISTORY_STARTUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(20);

async fn connect_dtc_services(accounts: &DtcAccounts) -> Result<DtcServices, String> {
    let market_config = accounts.market.config()?;
    let market = Arc::new(
        RithmicFeed::connect(market_config.clone())
            .await
            .map_err(|e| e.to_string())?,
    );
    let history = match tokio::time::timeout(
        HISTORY_STARTUP_BUDGET,
        RithmicHistoryFeed::connect(market_config.clone()),
    )
    .await
    {
        Ok(Ok(feed)) => feed,
        Ok(Err(error)) => {
            eprintln!("[History] {error}; using lazy History Plant connection");
            RithmicHistoryFeed::pending(market_config)
                .await
                .map_err(|e| e.to_string())?
        }
        Err(_) => {
            eprintln!(
                "[History] unavailable within {}s; using lazy History Plant connection",
                HISTORY_STARTUP_BUDGET.as_secs()
            );
            RithmicHistoryFeed::pending(market_config)
                .await
                .map_err(|e| e.to_string())?
        }
    };
    let trading = if accounts.trading_enabled {
        let config = accounts.trading.login.config()?;
        if config.env != RithmicEnv::Demo {
            return Err("交易账号只允许 demo/Paper 环境".to_owned());
        }
        Some(Arc::new(
            RithmicTradingFeed::connect(config, accounts.trading.account()?)
                .await
                .map_err(|e| e.to_string())?,
        ))
    } else {
        None
    };
    Ok(DtcServices {
        market,
        history: Arc::new(history),
        trading,
    })
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    println!("Rithmic synthetic machine identity: {}", synthetic_mac());
    let symbol = env::var("RITHMIC_DTC_SYMBOL").or_else(|_| env::var("RITHMIC_PROBE_SYMBOL"))?;
    let exchange =
        env::var("RITHMIC_DTC_EXCHANGE").or_else(|_| env::var("RITHMIC_PROBE_EXCHANGE"))?;
    let instrument = dtc::Instrument::es(symbol, exchange)?;
    let admin = DtcAccountAdmin::load()?;
    let mut account_updates = admin.subscribe();
    let admin_server = admin.clone();
    tokio::spawn(async move {
        if let Err(error) = admin_server.serve().await {
            eprintln!("[DTC Admin] stopped: {error}");
        }
    });
    let mut active_config = admin.config();
    let mut services = match connect_dtc_services(&active_config).await {
        Ok(value) => {
            admin.set_status("账号已连接");
            Some(value)
        }
        Err(error) => {
            admin.set_status(format!("连接失败：{error}"));
            eprintln!("[DTC] initial accounts unavailable: {error}");
            None
        }
    };
    let address = env::var("DTC_LISTEN_ADDR").unwrap_or_else(|_| DEFAULT_LISTEN_ADDRESS.to_owned());
    let listener = TcpListener::bind(&address).await?;
    let actual_address = listener.local_addr()?;
    println!("DTC binary market-data server listening on {actual_address}");
    let (generation, _) = tokio::sync::watch::channel(0_u64);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                stream.set_nodelay(true)?;
                let active = services.clone();
                let instrument = instrument.clone();
                let mut changed = generation.subscribe();
                tokio::spawn(async move {
                    println!("[DTC] Client connected: {peer}");
                    let session = dtc::handle_connection_with_all_services(
                        stream,
                        instrument,
                        active.as_ref().map(|v| v.market.client()),
                        active.as_ref().map(|v| v.history.client()),
                        active.as_ref().and_then(|v| v.trading.as_ref().map(|t| t.client())),
                    );
                    tokio::select! {
                        result = session => {
                            if let Err(error) = result {
                                eprintln!("[DTC] Session {peer} ended: {error}");
                            }
                        }
                        _ = changed.changed() => {
                            println!("[DTC] Account route changed; reconnecting {peer}");
                        }
                    }
                });
            }
            changed = account_updates.changed() => {
                if changed.is_err() {
                    continue;
                }
                let next = account_updates.borrow_and_update().clone();
                let market_changed = next.market != active_config.market;
                match connect_dtc_services(&next).await {
                    Ok(value) => {
                        services = Some(value);
                        active_config = next;
                        if market_changed {
                            admin.set_status("行情账号切换成功，DTC 客户端正在重连");
                            generation.send_replace(*generation.borrow() + 1);
                        } else {
                            admin.set_status("下单账号切换成功，行情连接保持不变");
                        }
                    }
                    Err(error) => {
                        admin.set_status(format!("切换失败，继续使用原账号：{error}"));
                    }
                }
            }
        }
    }
}
