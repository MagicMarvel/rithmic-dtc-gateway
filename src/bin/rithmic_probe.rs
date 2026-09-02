use std::{env, error::Error, fmt, path::Path, process::ExitCode, time::Duration};

use rithmic_dtc_bridge::identity::synthetic_mac;
use rithmic_rs::{
    ConnectStrategy, LoginConfig, RithmicConfig, RithmicEnv, RithmicTickerPlant,
    api::RithmicResponse, rti::messages::RithmicMessage,
};
use tokio::time::{Instant, timeout};

const DEFAULT_TIMEOUT_SECS: u64 = 45;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProbeSettings {
    environment: RithmicEnv,
    symbol: String,
    exchange: String,
    timeout: Duration,
}

impl ProbeSettings {
    fn from_env() -> Result<Self, ProbeFailure> {
        let environment =
            parse_environment(&env::var("RITHMIC_ENV").unwrap_or_else(|_| "demo".to_owned()))?;
        let symbol = required_non_secret("RITHMIC_PROBE_SYMBOL")?;
        let exchange = required_non_secret("RITHMIC_PROBE_EXCHANGE")?;
        let timeout_secs = match env::var("RITHMIC_PROBE_TIMEOUT_SECS") {
            Ok(value) => value.parse::<u64>().map_err(|_| {
                ProbeFailure::Configuration(
                    "RITHMIC_PROBE_TIMEOUT_SECS must be a positive integer".to_owned(),
                )
            })?,
            Err(_) => DEFAULT_TIMEOUT_SECS,
        };
        if timeout_secs == 0 {
            return Err(ProbeFailure::Configuration(
                "RITHMIC_PROBE_TIMEOUT_SECS must be greater than zero".to_owned(),
            ));
        }

        Ok(Self {
            environment,
            symbol,
            exchange,
            timeout: Duration::from_secs(timeout_secs),
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Evidence {
    last_trade: bool,
    best_bid_ask: bool,
    order_book: bool,
    depth_by_order: bool,
}

impl Evidence {
    fn observe(&mut self, response: &RithmicResponse) {
        match &response.message {
            RithmicMessage::LastTrade(message) if !self.last_trade => {
                self.last_trade = true;
                println!("[PASS] Last Trade: {message:?}");
            }
            RithmicMessage::BestBidOffer(message) if !self.best_bid_ask => {
                self.best_bid_ask = true;
                println!("[PASS] Best Bid/Ask: {message:?}");
            }
            RithmicMessage::OrderBook(message) if !self.order_book => {
                self.order_book = true;
                println!("[PASS] Order Book: {message:?}");
            }
            RithmicMessage::DepthByOrder(message) if !self.depth_by_order => {
                self.depth_by_order = true;
                println!("[PASS] Depth By Order: {message:?}");
            }
            RithmicMessage::DepthByOrderEndEvent(message) => {
                println!("[INFO] Depth By Order snapshot complete: {message:?}");
            }
            _ => {}
        }
    }

    fn complete(self) -> bool {
        self.last_trade && self.best_bid_ask && self.order_book && self.depth_by_order
    }

    fn missing(self) -> Vec<&'static str> {
        [
            (self.last_trade, "Last Trade"),
            (self.best_bid_ask, "Best Bid/Ask"),
            (self.order_book, "Order Book"),
            (self.depth_by_order, "Depth By Order"),
        ]
        .into_iter()
        .filter_map(|(seen, name)| (!seen).then_some(name))
        .collect()
    }
}

#[derive(Debug)]
enum ProbeFailure {
    Configuration(String),
    Connect(String),
    Login(String),
    Subscription(String),
    Stream(String),
    MissingData(Vec<&'static str>),
}

impl fmt::Display for ProbeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => write!(f, "configuration error: {message}"),
            Self::Connect(message) => write!(f, "connection failed: {message}"),
            Self::Login(message) => write!(f, "login failed: {message}"),
            Self::Subscription(message) => write!(f, "market-data request failed: {message}"),
            Self::Stream(message) => write!(f, "market-data stream failed: {message}"),
            Self::MissingData(items) => write!(
                f,
                "validation timed out without receiving: {}",
                items.join(", ")
            ),
        }
    }
}

impl Error for ProbeFailure {}

#[tokio::main]
async fn main() -> ExitCode {
    let result = match load_environment_file() {
        Ok(()) => run().await,
        Err(error) => Err(error),
    };

    match result {
        Ok(()) => {
            println!(
                "\nPROBE PASSED: all four required real-time market-data classes were observed."
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("\nPROBE FAILED: {error}");
            print_diagnostics(&error);
            ExitCode::FAILURE
        }
    }
}

fn load_environment_file() -> Result<(), ProbeFailure> {
    let path = Path::new(".env");
    if !path.exists() {
        return Ok(());
    }

    dotenvy::from_path(path).map_err(|error| {
        ProbeFailure::Configuration(format!("failed to load {}: {error}", path.display()))
    })
}

async fn run() -> Result<(), ProbeFailure> {
    let settings = ProbeSettings::from_env()?;
    println!(
        "Rithmic probe: environment={:?}, instrument={}.{}, timeout={}s",
        settings.environment,
        settings.symbol,
        settings.exchange,
        settings.timeout.as_secs()
    );

    let config = RithmicConfig::from_env(settings.environment).map_err(|error| {
        ProbeFailure::Configuration(format!(
            "{error}. Check the RITHMIC_APP_* and environment-specific RITHMIC_* variables in .env.example"
        ))
    })?;

    let plant = timeout(
        settings.timeout,
        RithmicTickerPlant::connect(&config, ConnectStrategy::Simple),
    )
    .await
    .map_err(|_| ProbeFailure::Connect("connection attempt timed out".to_owned()))?
    .map_err(|error| ProbeFailure::Connect(error.to_string()))?;
    let mut handle = plant.get_handle();

    let synthetic_mac = synthetic_mac();
    println!("[INFO] Sending generated non-hardware MAC: {synthetic_mac}");
    let mut login_config = LoginConfig::default();
    login_config.mac_addr = Some(vec![synthetic_mac]);

    let probe_result: Result<Evidence, ProbeFailure> = async {
        timeout(settings.timeout, handle.login_with_config(login_config))
            .await
            .map_err(|_| ProbeFailure::Login("login request timed out".to_owned()))?
            .map_err(|error| ProbeFailure::Login(error.to_string()))?;
        println!("[PASS] Login accepted");

        let standard = timeout(
            settings.timeout,
            handle.subscribe(&settings.symbol, &settings.exchange),
        )
        .await
        .map_err(|_| ProbeFailure::Subscription("standard subscription timed out".to_owned()))?
        .map_err(|error| ProbeFailure::Subscription(error.to_string()))?;
        ensure_accepted("standard market data", &standard)?;
        println!("[PASS] Standard market-data subscription accepted");

        let order_book = timeout(
            settings.timeout,
            handle.subscribe_order_book_summary(&settings.symbol, &settings.exchange),
        )
        .await
        .map_err(|_| ProbeFailure::Subscription("Order Book subscription timed out".to_owned()))?
        .map_err(|error| ProbeFailure::Subscription(error.to_string()))?;
        ensure_accepted("Order Book", &order_book)?;
        println!("[PASS] Order Book subscription accepted");

        let dbo_updates = timeout(
            settings.timeout,
            handle.subscribe_depth_by_order_update(&settings.symbol, &settings.exchange),
        )
        .await
        .map_err(|_| {
            ProbeFailure::Subscription("Depth By Order subscription timed out".to_owned())
        })?
        .map_err(|error| ProbeFailure::Subscription(error.to_string()))?;
        ensure_accepted("Depth By Order updates", &dbo_updates)?;
        println!("[PASS] Depth By Order subscription accepted");

        let snapshot = timeout(
            settings.timeout,
            handle.get_depth_by_order_snapshot(&settings.symbol, &settings.exchange),
        )
        .await
        .map_err(|_| ProbeFailure::Subscription("Depth By Order snapshot timed out".to_owned()))?
        .map_err(|error| ProbeFailure::Subscription(error.to_string()))?;

        let mut evidence = Evidence::default();
        for response in &snapshot {
            ensure_accepted("Depth By Order snapshot", response)?;
            evidence.observe(response);
        }

        let deadline = Instant::now() + settings.timeout;
        while !evidence.complete() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, handle.subscription_receiver.recv()).await {
                Ok(Ok(response)) => {
                    if let Some(error) = &response.error {
                        return Err(ProbeFailure::Stream(error.to_string()));
                    }
                    evidence.observe(&response);
                }
                Ok(Err(error)) => return Err(ProbeFailure::Stream(error.to_string())),
                Err(_) => break,
            }
        }

        Ok(evidence)
    }
    .await;

    if let Err(error) = handle.disconnect().await {
        eprintln!("[WARN] Graceful disconnect failed: {error}");
        handle.abort();
    }
    if let Err(error) = plant.await_shutdown().await {
        eprintln!("[WARN] Ticker Plant shutdown task failed: {error}");
    }

    let evidence = probe_result?;

    if evidence.complete() {
        Ok(())
    } else {
        Err(ProbeFailure::MissingData(evidence.missing()))
    }
}

fn ensure_accepted(label: &str, response: &RithmicResponse) -> Result<(), ProbeFailure> {
    match &response.error {
        Some(error) => Err(ProbeFailure::Subscription(format!(
            "{label} was rejected: {error}"
        ))),
        None => Ok(()),
    }
}

fn required_non_secret(name: &str) -> Result<String, ProbeFailure> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ProbeFailure::Configuration(format!("{name} is missing or empty")))
}

fn parse_environment(value: &str) -> Result<RithmicEnv, ProbeFailure> {
    match value.trim().to_ascii_lowercase().as_str() {
        "demo" => Ok(RithmicEnv::Demo),
        "live" => Ok(RithmicEnv::Live),
        "test" => Ok(RithmicEnv::Test),
        _ => Err(ProbeFailure::Configuration(
            "RITHMIC_ENV must be demo, live, or test".to_owned(),
        )),
    }
}

fn print_diagnostics(error: &ProbeFailure) {
    eprintln!("No DTC work has been started. Resolve this probe failure first.");
    match error {
        ProbeFailure::Configuration(_) => eprintln!(
            "Copy .env.example to .env and fill only values issued by Rithmic/your broker. Secrets are never printed."
        ),
        ProbeFailure::Connect(_) => eprintln!(
            "Check the selected URL/ALT_URL, DNS, firewall/TLS access, and whether the endpoint belongs to the selected environment."
        ),
        ProbeFailure::Login(_) => eprintln!(
            "Check user/password, APP_NAME/APP_VERSION, SYSTEM_NAME, concurrent-session limits, and whether R | Protocol API access is enabled."
        ),
        ProbeFailure::Subscription(_) | ProbeFailure::Stream(_) => eprintln!(
            "Login worked, but the server rejected or ended a market-data request. Check symbol/exchange spelling and real-time plus Depth-by-Order entitlements."
        ),
        ProbeFailure::MissingData(items) => eprintln!(
            "Accepted requests are not sufficient proof. Missing [{}] may indicate a closed/inactive market, a stale contract, or missing quote/depth entitlements. Retry with an actively trading contract.",
            items.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_environment_names_case_insensitively() {
        assert_eq!(parse_environment("DEMO").unwrap(), RithmicEnv::Demo);
        assert_eq!(parse_environment(" live ").unwrap(), RithmicEnv::Live);
        assert_eq!(parse_environment("Test").unwrap(), RithmicEnv::Test);
    }

    #[test]
    fn rejects_unknown_environment() {
        assert!(parse_environment("paper").is_err());
    }

    #[test]
    fn evidence_reports_only_missing_classes() {
        let evidence = Evidence {
            last_trade: true,
            best_bid_ask: true,
            order_book: false,
            depth_by_order: false,
        };
        assert_eq!(evidence.missing(), vec!["Order Book", "Depth By Order"]);
        assert!(!evidence.complete());
    }

    #[test]
    fn generated_mac_is_local_unicast_and_well_formed() {
        let mac = synthetic_mac();
        let octets = mac
            .split(':')
            .map(|part| u8::from_str_radix(part, 16).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(octets.len(), 6);
        assert_eq!(octets[0] & 0x01, 0, "must be unicast");
        assert_eq!(octets[0] & 0x02, 0x02, "must be locally administered");
    }
}
