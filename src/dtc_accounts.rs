//! DTC-only account routing and loopback administration.

use std::{
    env, fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, RwLock},
};

use rithmic_rs::{RithmicAccount, RithmicConfig, RithmicEnv};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
};

const ADMIN_HTML: &str = include_str!("dtc_accounts.html");

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoginProfile {
    pub label: String,
    pub environment: String,
    pub user: String,
    #[serde(default)]
    pub password: String,
    pub url: String,
    pub beta_url: String,
    pub system_name: String,
    pub app_name: String,
    pub app_version: String,
}

impl LoginProfile {
    fn from_config(label: &str, config: RithmicConfig) -> Self {
        Self {
            label: label.to_owned(),
            environment: config.env.to_string(),
            user: config.user,
            password: config.password,
            url: config.url,
            beta_url: config.beta_url,
            system_name: config.system_name,
            app_name: config.app_name,
            app_version: config.app_version,
        }
    }

    pub fn config(&self) -> Result<RithmicConfig, String> {
        let environment = RithmicEnv::from_str(self.environment.trim())
            .map_err(|error| format!("{} environment: {error}", self.label))?;
        RithmicConfig::builder(environment)
            .user(self.user.trim())
            .password(&self.password)
            .url(self.url.trim())
            .beta_url(self.beta_url.trim())
            .system_name(self.system_name.trim())
            .app_name(self.app_name.trim())
            .app_version(self.app_version.trim())
            .build()
            .map_err(|error| format!("{} configuration: {error}", self.label))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TradingProfile {
    #[serde(flatten)]
    pub login: LoginProfile,
    pub account_id: String,
    pub fcm_id: String,
    pub ib_id: String,
}

impl TradingProfile {
    pub fn account(&self) -> Result<RithmicAccount, String> {
        for (name, value) in [
            ("account_id", &self.account_id),
            ("fcm_id", &self.fcm_id),
            ("ib_id", &self.ib_id),
        ] {
            if value.trim().is_empty() {
                return Err(format!("trading {name} must not be empty"));
            }
        }
        Ok(RithmicAccount::new(
            self.fcm_id.trim(),
            self.ib_id.trim(),
            self.account_id.trim(),
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DtcAccounts {
    pub market: LoginProfile,
    pub trading_enabled: bool,
    pub trading: TradingProfile,
}

impl DtcAccounts {
    pub fn from_env() -> Result<Self, String> {
        let market_env = env::var("RITHMIC_ENV").unwrap_or_else(|_| "demo".to_owned());
        let market_env = RithmicEnv::from_str(market_env.trim()).map_err(|e| e.to_string())?;
        let market = RithmicConfig::from_env(market_env).map_err(|e| e.to_string())?;
        let trading = RithmicConfig::from_env(RithmicEnv::Demo).unwrap_or_else(|_| {
            let mut fallback = market.clone();
            fallback.env = RithmicEnv::Demo;
            fallback.system_name = "Rithmic Paper Trading".to_owned();
            fallback
        });
        let account = RithmicAccount::from_env(RithmicEnv::Demo)
            .unwrap_or_else(|_| RithmicAccount::new("", "", ""));
        Ok(Self {
            market: LoginProfile::from_config("Market data", market),
            trading_enabled: env_flag("RITHMIC_ENABLE_TRADING"),
            trading: TradingProfile {
                login: LoginProfile::from_config("Trading", trading),
                account_id: account.account_id,
                fcm_id: account.fcm_id,
                ib_id: account.ib_id,
            },
        })
    }

    fn validate(&self) -> Result<(), String> {
        self.market.config()?;
        if self.trading_enabled {
            let trading = self.trading.login.config()?;
            if trading.env != RithmicEnv::Demo {
                return Err("trading must remain in the demo environment".to_owned());
            }
            self.trading.account()?;
        }
        Ok(())
    }

    fn redacted(&self) -> Self {
        let mut value = self.clone();
        value.market.password.clear();
        value.trading.login.password.clear();
        value
    }

    fn merge_passwords(&mut self, previous: &Self) {
        if self.market.password.is_empty() {
            self.market.password.clone_from(&previous.market.password);
        }
        if self.trading.login.password.is_empty() {
            self.trading
                .login
                .password
                .clone_from(&previous.trading.login.password);
        }
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    })
}

#[derive(Clone)]
pub struct DtcAccountAdmin {
    config: Arc<RwLock<DtcAccounts>>,
    status: Arc<RwLock<String>>,
    path: Arc<PathBuf>,
    token: Arc<String>,
    token_was_generated: bool,
    updates: watch::Sender<DtcAccounts>,
}

impl DtcAccountAdmin {
    pub fn load() -> Result<Self, String> {
        let path = PathBuf::from(
            env::var("DTC_ACCOUNTS_FILE").unwrap_or_else(|_| "data/dtc/accounts.json".to_owned()),
        );
        let config = if path.exists() {
            serde_json::from_slice(&fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| format!("parse {}: {e}", path.display()))?
        } else {
            DtcAccounts::from_env()?
        };
        config.validate()?;
        let supplied_token = env::var("DTC_ADMIN_TOKEN").ok().filter(|v| v.len() >= 32);
        let token_was_generated = supplied_token.is_none();
        let token = supplied_token.unwrap_or_else(|| {
            rand::random::<[u8; 32]>()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        });
        let (updates, _) = watch::channel(config.clone());
        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            status: Arc::new(RwLock::new("等待连接".to_owned())),
            path: Arc::new(path),
            token: Arc::new(token),
            token_was_generated,
            updates,
        })
    }

    pub fn config(&self) -> DtcAccounts {
        self.config
            .read()
            .expect("DTC account lock poisoned")
            .clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<DtcAccounts> {
        self.updates.subscribe()
    }

    pub fn set_status(&self, status: impl Into<String>) {
        *self.status.write().expect("DTC status lock poisoned") = status.into();
    }

    pub async fn serve(self) -> Result<(), String> {
        let address =
            env::var("DTC_ADMIN_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:11101".to_owned());
        let listener = TcpListener::bind(&address)
            .await
            .map_err(|e| format!("bind DTC admin {address}: {e}"))?;
        println!("DTC account admin: http://{address}/");
        if self.token_was_generated {
            println!("DTC account admin token: {}", self.token);
        } else {
            println!("DTC account admin token loaded from DTC_ADMIN_TOKEN");
        }
        loop {
            let (stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let admin = self.clone();
            tokio::spawn(async move {
                if let Err(error) = admin.handle(stream).await {
                    eprintln!("[DTC Admin] {error}");
                }
            });
        }
    }

    async fn handle(&self, mut stream: TcpStream) -> Result<(), String> {
        let mut bytes = vec![0_u8; 64 * 1024];
        let mut used = 0;
        let header_end = loop {
            let read = stream
                .read(&mut bytes[used..])
                .await
                .map_err(|e| e.to_string())?;
            if read == 0 {
                return Ok(());
            }
            used += read;
            if let Some(end) = bytes[..used].windows(4).position(|v| v == b"\r\n\r\n") {
                break end + 4;
            }
            if used == bytes.len() {
                return write_response(&mut stream, 413, "text/plain", b"request too large").await;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
        let mut lines = headers.lines();
        let request = lines.next().unwrap_or_default();
        let mut parts = request.split_whitespace();
        let method = parts.next().unwrap_or_default();
        let path = parts.next().unwrap_or_default();
        let content_length = lines
            .clone()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
            })
            .unwrap_or(0);
        while used < header_end + content_length {
            let read = stream
                .read(&mut bytes[used..])
                .await
                .map_err(|e| e.to_string())?;
            if read == 0 {
                break;
            }
            used += read;
        }
        if method == "GET" && path == "/" {
            return write_response(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                ADMIN_HTML.as_bytes(),
            )
            .await;
        }
        let authorized = lines.any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("authorization")
                    && value.trim().strip_prefix("Bearer ") == Some(self.token.as_str())
            })
        });
        if !authorized {
            return write_response(&mut stream, 401, "text/plain", b"unauthorized").await;
        }
        match (method, path) {
            ("GET", "/api/config") => {
                let json =
                    serde_json::to_vec(&self.config().redacted()).map_err(|e| e.to_string())?;
                write_response(&mut stream, 200, "application/json", &json).await
            }
            ("GET", "/api/status") => {
                let status = self
                    .status
                    .read()
                    .expect("DTC status lock poisoned")
                    .clone();
                let json = serde_json::to_vec(&serde_json::json!({ "status": status }))
                    .map_err(|e| e.to_string())?;
                write_response(&mut stream, 200, "application/json", &json).await
            }
            ("POST", "/api/config") => {
                let body_end = (header_end + content_length).min(used);
                let mut next: DtcAccounts =
                    match serde_json::from_slice(&bytes[header_end..body_end]) {
                        Ok(value) => value,
                        Err(error) => {
                            let message = format!("invalid account JSON: {error}");
                            return write_response(
                                &mut stream,
                                400,
                                "text/plain",
                                message.as_bytes(),
                            )
                            .await;
                        }
                    };
                let previous = self.config();
                next.merge_passwords(&previous);
                if let Err(error) = next.validate() {
                    return write_response(&mut stream, 400, "text/plain", error.as_bytes()).await;
                }
                persist(&self.path, &next)?;
                *self.config.write().expect("DTC account lock poisoned") = next.clone();
                self.set_status("正在连接新账号…");
                self.updates.send_replace(next);
                write_response(&mut stream, 202, "application/json", br#"{"ok":true}"#).await
            }
            _ => write_response(&mut stream, 404, "text/plain", b"not found").await,
        }
    }
}

fn persist(path: &Path, config: &DtcAccounts) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let temporary = path.with_extension("json.tmp");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(config).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("write {}: {e}", temporary.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("protect {}: {e}", temporary.display()))?;
    }
    fs::rename(&temporary, path).map_err(|e| format!("replace {}: {e}", path.display()))
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<(), String> {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        413 => "Payload Too Large",
        _ => "Error",
    };
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(headers.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    stream.write_all(body).await.map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blank_password_preserves_existing_secret() {
        let login = LoginProfile {
            label: "test".to_owned(),
            environment: "demo".to_owned(),
            user: "user".to_owned(),
            password: "secret".to_owned(),
            url: "wss://example.test".to_owned(),
            beta_url: "wss://beta.example.test".to_owned(),
            system_name: "Rithmic Paper Trading".to_owned(),
            app_name: "app".to_owned(),
            app_version: "1".to_owned(),
        };
        let mut next = DtcAccounts {
            market: login.clone(),
            trading_enabled: true,
            trading: TradingProfile {
                login,
                account_id: "account".to_owned(),
                fcm_id: "fcm".to_owned(),
                ib_id: "ib".to_owned(),
            },
        };
        let previous = next.clone();
        next.market.password.clear();
        next.trading.login.password.clear();
        next.merge_passwords(&previous);
        assert_eq!(next.market.password, previous.market.password);
        assert_eq!(next.trading.login.password, previous.trading.login.password);
    }
}
