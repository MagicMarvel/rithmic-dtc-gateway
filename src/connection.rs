//! Rithmic connection settings used by the server data feeds and by the
//! browser-configurable order connection. Each caller owns a separate
//! `SharedConnection`, so changing an order login cannot mutate market data or
//! history credentials.

use std::{
    env, fs,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use rithmic_rs::{RithmicAccount, RithmicConfig, RithmicConfigBuilder, RithmicEnv};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;

/// Rithmic login and endpoint settings as edited in the terminal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSettings {
    /// "demo" (Paper Trading), "live", or "test".
    #[serde(default = "default_environment")]
    pub environment: String,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: String,
    /// Paper-trading account routing identifiers. These are not secrets, but
    /// they must match the selected login before an order connection can start.
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub fcm_id: String,
    #[serde(default)]
    pub ib_id: String,
    /// Primary WebSocket endpoint (wss://...).
    #[serde(default)]
    pub url: String,
    /// Alternative WebSocket endpoint; defaults to the primary when empty.
    #[serde(default)]
    pub alt_url: String,
    /// Rithmic system name, e.g. "Rithmic Paper Trading" or "Rithmic 01".
    #[serde(default)]
    pub system_name: String,
    #[serde(default)]
    pub app_name: String,
    #[serde(default)]
    pub app_version: String,
}

fn default_environment() -> String {
    "demo".to_owned()
}

/// Well-known Rithmic endpoints and system names offered by the terminal UI.
pub const KNOWN_URLS: &[&str] = &[
    "wss://rprotocol.rithmic.com:443",
    "wss://rituz00100.rithmic.com:443",
    "wss://rprotocol-de.rithmic.com:443",
    "wss://rprotocol-sg.rithmic.com:443",
    "wss://rprotocol-au.rithmic.com:443",
];

pub const KNOWN_SYSTEMS: &[&str] = &[
    "Rithmic Paper Trading",
    "Rithmic 01",
    "Rithmic Test",
    "TopstepTrader",
    "Apex",
    "MES Capital",
    "TheTradingPit",
    "Bulenox",
    "PropShopTrader",
    "4PropTrader",
    "FastTrackTrading",
    "SpeedUp",
    "Earn2Trade",
    "DayTraders.com",
    "10XFutures",
    "LucidTrading",
    "ThriveTrading",
    "LegendsTrading",
];

impl ConnectionSettings {
    /// Reads the same environment variables the feeds always used
    /// (`RITHMIC_ENV`, `RITHMIC_<ENV>_USER`, ...). Missing values become empty
    /// strings so the terminal can show and complete them; `to_config` is
    /// where the strict validation happens.
    pub fn from_env() -> Self {
        let environment = env::var("RITHMIC_ENV")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| matches!(value.as_str(), "demo" | "live" | "test"))
            .unwrap_or_else(default_environment);
        let prefix = format!("RITHMIC_{}", environment.to_ascii_uppercase());
        let read = |suffix: &str| {
            env::var(format!("{prefix}_{suffix}"))
                .unwrap_or_default()
                .trim()
                .to_owned()
        };
        let system_name = {
            let value = read("SYSTEM_NAME");
            if value.is_empty() {
                default_system_name(&environment).to_owned()
            } else {
                value
            }
        };
        Self {
            environment,
            user: read("USER"),
            password: read("PW"),
            account_id: read("ACCOUNT_ID"),
            fcm_id: read("FCM_ID"),
            ib_id: read("IB_ID"),
            url: read("URL"),
            alt_url: read("ALT_URL"),
            system_name,
            app_name: env::var("RITHMIC_APP_NAME")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "rithmic_dtc_bridge".to_owned()),
            app_version: env::var("RITHMIC_APP_VERSION")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "1".to_owned()),
        }
    }

    /// Safe initial state for a browser-configured trading connection. Server
    /// market-data credentials are intentionally not copied into this value or
    /// exposed through the terminal API.
    pub fn trading_defaults() -> Self {
        Self {
            environment: "demo".to_owned(),
            user: String::new(),
            password: String::new(),
            account_id: String::new(),
            fcm_id: String::new(),
            ib_id: String::new(),
            url: "wss://rprotocol.rithmic.com:443".to_owned(),
            alt_url: String::new(),
            system_name: "Rithmic Paper Trading".to_owned(),
            app_name: "rithmic_dtc_bridge".to_owned(),
            app_version: "1".to_owned(),
        }
    }

    pub fn rithmic_env(&self) -> Result<RithmicEnv, String> {
        match self.environment.trim().to_ascii_lowercase().as_str() {
            "demo" => Ok(RithmicEnv::Demo),
            "live" => Ok(RithmicEnv::Live),
            "test" => Ok(RithmicEnv::Test),
            other => Err(format!(
                "environment must be demo, live, or test (got {other:?})"
            )),
        }
    }

    /// Trims every field, fills the optional ones, and rejects missing values.
    pub fn normalized(&self) -> Result<Self, String> {
        let environment = self.environment.trim().to_ascii_lowercase();
        let mut settings = Self {
            environment: environment.clone(),
            user: self.user.trim().to_owned(),
            password: self.password.clone(),
            account_id: self.account_id.trim().to_owned(),
            fcm_id: self.fcm_id.trim().to_owned(),
            ib_id: self.ib_id.trim().to_owned(),
            url: self.url.trim().to_owned(),
            alt_url: self.alt_url.trim().to_owned(),
            system_name: self.system_name.trim().to_owned(),
            app_name: self.app_name.trim().to_owned(),
            app_version: self.app_version.trim().to_owned(),
        };
        settings.rithmic_env()?;
        if settings.user.is_empty() {
            return Err("Rithmic user is required".to_owned());
        }
        if settings.password.is_empty() {
            return Err("Rithmic password is required".to_owned());
        }
        if settings.url.is_empty() {
            return Err("primary WebSocket URL is required".to_owned());
        }
        if !(settings.url.starts_with("wss://") || settings.url.starts_with("ws://")) {
            return Err("primary WebSocket URL must start with wss://".to_owned());
        }
        if settings.alt_url.is_empty() {
            settings.alt_url = settings.url.clone();
        }
        if settings.system_name.is_empty() {
            settings.system_name = default_system_name(&environment).to_owned();
        }
        if settings.app_name.is_empty() {
            settings.app_name = "rithmic_dtc_bridge".to_owned();
        }
        if settings.app_version.is_empty() {
            settings.app_version = "1".to_owned();
        }
        Ok(settings)
    }

    pub fn from_config(config: RithmicConfig) -> Self {
        let environment = config.env.to_string().to_ascii_lowercase();
        Self {
            environment,
            user: config.user,
            password: config.password,
            account_id: String::new(),
            fcm_id: String::new(),
            ib_id: String::new(),
            url: config.url,
            alt_url: config.beta_url,
            system_name: config.system_name,
            app_name: config.app_name,
            app_version: config.app_version,
        }
    }

    pub fn to_config(&self) -> Result<RithmicConfig, String> {
        let settings = self.normalized()?;
        RithmicConfigBuilder::new(settings.rithmic_env()?)
            .url(settings.url)
            .beta_url(settings.alt_url)
            .user(settings.user)
            .password(settings.password)
            .system_name(settings.system_name)
            .app_name(settings.app_name)
            .app_version(settings.app_version)
            .build()
            .map_err(|error| format!("Rithmic configuration failed: {error}"))
    }

    pub fn to_account(&self) -> Result<RithmicAccount, String> {
        if self.rithmic_env()? != RithmicEnv::Demo {
            return Err("Paper trading requires the demo environment".to_owned());
        }
        for (name, value) in [
            ("account ID", self.account_id.trim()),
            ("FCM ID", self.fcm_id.trim()),
            ("IB ID", self.ib_id.trim()),
        ] {
            if value.is_empty() {
                return Err(format!("{name} is required when Paper trading is enabled"));
            }
        }
        Ok(RithmicAccount::new(
            self.fcm_id.trim(),
            self.ib_id.trim(),
            self.account_id.trim(),
        ))
    }

    /// JSON for the browser: everything except the password itself.
    pub fn public_json(&self) -> Value {
        json!({
            "environment": self.environment,
            "user": self.user,
            "hasPassword": !self.password.is_empty(),
            "accountId": self.account_id,
            "fcmId": self.fcm_id,
            "ibId": self.ib_id,
            "url": self.url,
            "altUrl": self.alt_url,
            "systemName": self.system_name,
            "appName": self.app_name,
            "appVersion": self.app_version,
        })
    }
}

pub fn default_system_name(environment: &str) -> &'static str {
    match environment {
        "live" => "Rithmic 01",
        "test" => "Rithmic Test",
        _ => "Rithmic Paper Trading",
    }
}

/// Where the terminal persists the user's order-only Rithmic settings.
/// `RITHMIC_TRADING_CONNECTION_FILE` is preferred; the old variable remains a
/// compatibility fallback.
pub fn saved_settings_path() -> PathBuf {
    env::var_os("RITHMIC_TRADING_CONNECTION_FILE")
        .or_else(|| env::var_os("RITHMIC_CONNECTION_FILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data").join("rithmic-trading-connection.json"))
}

fn legacy_saved_settings_path() -> PathBuf {
    PathBuf::from("data").join("rithmic-connection.json")
}

fn read_saved_settings() -> Option<ConnectionSettings> {
    [saved_settings_path(), legacy_saved_settings_path()]
        .into_iter()
        .find_map(|path| {
            let text = fs::read_to_string(path).ok()?;
            serde_json::from_str::<ConnectionSettings>(&text).ok()
        })
}

/// Origin of the active settings, reported to the browser.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettingsSource {
    Environment,
    SavedFile,
    Session,
}

impl SettingsSource {
    pub fn as_str(self) -> &'static str {
        match self {
            SettingsSource::Environment => "env",
            SettingsSource::SavedFile => "saved",
            SettingsSource::Session => "session",
        }
    }
}

struct Inner {
    settings: ConnectionSettings,
    source: SettingsSource,
}

/// Runtime-switchable settings. Clones share the same state and `apply` bumps
/// the generation for supervisors using this particular connection instance.
#[derive(Clone)]
pub struct SharedConnection {
    inner: Arc<RwLock<Inner>>,
    generation: Arc<watch::Sender<u64>>,
}

impl SharedConnection {
    pub fn new(settings: ConnectionSettings, source: SettingsSource) -> Self {
        let (generation, _) = watch::channel(0);
        Self {
            inner: Arc::new(RwLock::new(Inner { settings, source })),
            generation: Arc::new(generation),
        }
    }

    /// Environment variables only, as the DTC server and probes always used.
    pub fn from_env() -> Self {
        Self::new(ConnectionSettings::from_env(), SettingsSource::Environment)
    }

    pub fn from_config(config: RithmicConfig) -> Self {
        Self::new(
            ConnectionSettings::from_config(config),
            SettingsSource::Session,
        )
    }

    /// Legacy constructor retained for non-terminal callers.
    pub fn from_env_or_saved() -> Self {
        match read_saved_settings() {
            Some(saved) => {
                println!(
                    "Rithmic connection settings loaded from {} (user {})",
                    saved_settings_path().display(),
                    saved.user
                );
                Self::new(saved, SettingsSource::SavedFile)
            }
            None => Self::from_env(),
        }
    }

    /// Browser-editable trading settings never fall back to the server's data
    /// account. This prevents server market-data credentials from being sent
    /// to the browser through `/api/connection`.
    pub fn from_saved_or_trading_defaults() -> Self {
        match read_saved_settings() {
            Some(saved) => {
                println!(
                    "Rithmic order connection settings loaded for user {}",
                    saved.user
                );
                Self::new(saved, SettingsSource::SavedFile)
            }
            None => Self::new(
                ConnectionSettings::trading_defaults(),
                SettingsSource::Session,
            ),
        }
    }

    pub fn settings(&self) -> ConnectionSettings {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .settings
            .clone()
    }

    pub fn source(&self) -> SettingsSource {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .source
    }

    pub fn config(&self) -> Result<RithmicConfig, String> {
        self.settings().to_config()
    }

    pub fn generation(&self) -> u64 {
        *self.generation.borrow()
    }

    /// Receiver that resolves whenever this connection's settings change.
    pub fn watch(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }

    /// Validates, activates, optionally persists, and asks every feed to
    /// reconnect. An empty password keeps the currently active one so the
    /// browser never has to echo it back.
    pub fn apply(
        &self,
        mut settings: ConnectionSettings,
        persist: bool,
    ) -> Result<ConnectionSettings, String> {
        if settings.password.trim().is_empty() {
            settings.password = self.settings().password;
        }
        let settings = settings.normalized()?;
        settings.to_config()?;
        if persist {
            let path = saved_settings_path();
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
            }
            let text =
                serde_json::to_string_pretty(&settings).map_err(|error| error.to_string())?;
            fs::write(&path, text)
                .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
        } else {
            let _ = fs::remove_file(saved_settings_path());
        }
        {
            let mut inner = self
                .inner
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.settings = settings.clone();
            inner.source = if persist {
                SettingsSource::SavedFile
            } else {
                SettingsSource::Session
            };
        }
        self.generation.send_modify(|generation| *generation += 1);
        Ok(settings)
    }

    /// Discards the saved file and returns to the environment variables.
    pub fn reset_to_env(&self) -> Result<ConnectionSettings, String> {
        let _ = fs::remove_file(saved_settings_path());
        let settings = ConnectionSettings::from_env();
        settings.to_config()?;
        {
            let mut inner = self
                .inner
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.settings = settings.clone();
            inner.source = SettingsSource::Environment;
        }
        self.generation.send_modify(|generation| *generation += 1);
        Ok(settings)
    }

    /// Removes browser-configured order credentials without touching the
    /// server-owned market/history connection.
    pub fn clear_trading_settings(&self) -> ConnectionSettings {
        let _ = fs::remove_file(saved_settings_path());
        let _ = fs::remove_file(legacy_saved_settings_path());
        let settings = ConnectionSettings::trading_defaults();
        {
            let mut inner = self
                .inner
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.settings = settings.clone();
            inner.source = SettingsSource::Session;
        }
        self.generation.send_modify(|generation| *generation += 1);
        settings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ConnectionSettings {
        ConnectionSettings {
            environment: "demo".to_owned(),
            user: " PP-000 ".to_owned(),
            password: "secret".to_owned(),
            account_id: "paper-account".to_owned(),
            fcm_id: "fcm".to_owned(),
            ib_id: "ib".to_owned(),
            url: "wss://rprotocol.rithmic.com:443".to_owned(),
            alt_url: String::new(),
            system_name: String::new(),
            app_name: String::new(),
            app_version: String::new(),
        }
    }

    #[test]
    fn normalized_fills_defaults_and_trims() {
        let settings = sample().normalized().unwrap();
        assert_eq!(settings.user, "PP-000");
        assert_eq!(settings.alt_url, settings.url);
        assert_eq!(settings.system_name, "Rithmic Paper Trading");
        assert_eq!(settings.app_name, "rithmic_dtc_bridge");
        assert!(settings.to_config().is_ok());
    }

    #[test]
    fn normalized_rejects_missing_or_invalid_values() {
        let mut settings = sample();
        settings.user.clear();
        assert!(settings.normalized().is_err());
        let mut settings = sample();
        settings.url = "https://example.com".to_owned();
        assert!(settings.normalized().is_err());
        let mut settings = sample();
        settings.environment = "paper".to_owned();
        assert!(settings.normalized().is_err());
    }

    #[test]
    fn apply_bumps_generation_and_keeps_password_when_blank() {
        let shared =
            SharedConnection::new(sample().normalized().unwrap(), SettingsSource::Environment);
        let watcher = shared.watch();
        let mut next = sample();
        next.user = "PP-111".to_owned();
        next.password.clear();
        let applied = shared.apply(next, false).unwrap();
        assert_eq!(applied.password, "secret");
        assert_eq!(shared.generation(), 1);
        assert!(watcher.has_changed().unwrap());
        assert_eq!(shared.settings().user, "PP-111");
        assert_eq!(shared.source(), SettingsSource::Session);
    }

    #[test]
    fn public_json_never_contains_the_password() {
        let text = sample().public_json().to_string();
        assert!(!text.contains("secret"));
        assert!(text.contains("\"hasPassword\":true"));
    }
}
