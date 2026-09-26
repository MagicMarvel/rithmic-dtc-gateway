use std::{env, time::Duration};

/// A rejected login is usually the account's concurrent-session limit: the slot
/// of a process that was stopped a moment ago is still held server-side and is
/// released after a few minutes. A short first retry recovers from that quickly,
/// and the low cap keeps a longer outage from turning into quarter-hour waits.
const DEFAULT_INITIAL_SECS: u64 = 10;
const DEFAULT_MAX_SECS: u64 = 120;

pub(crate) struct MaintenanceBackoff {
    delay: Duration,
    maximum: Duration,
    attempts: u32,
}

impl MaintenanceBackoff {
    pub(crate) fn from_env() -> Self {
        let initial = env_seconds(
            "RITHMIC_MAINTENANCE_RETRY_INITIAL_SECS",
            DEFAULT_INITIAL_SECS,
        );
        let maximum =
            env_seconds("RITHMIC_MAINTENANCE_RETRY_MAX_SECS", DEFAULT_MAX_SECS).max(initial);
        Self {
            delay: Duration::from_secs(initial),
            maximum: Duration::from_secs(maximum),
            attempts: 0,
        }
    }

    pub(crate) fn is_retryable(message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        [
            "permission denied",
            "connection failed",
            "connection attempt timed out",
            "tls error",
            "unexpected eof",
            "connection reset",
        ]
        .iter()
        .any(|needle| message.contains(needle))
    }

    pub(crate) async fn wait(&mut self, subsystem: &str, error: &str) {
        self.attempts = self.attempts.saturating_add(1);
        eprintln!(
            "[{subsystem}] Rithmic unavailable ({error}); maintenance retry {} in {}s",
            self.attempts,
            self.delay.as_secs()
        );
        if error.to_ascii_lowercase().contains("permission denied") {
            eprintln!(
                "[{subsystem}] Hint: this account allows only a limited number of concurrent Rithmic sessions. Close other Rithmic software, or wait for the session of a recently stopped process to expire; the retry continues automatically."
            );
        }
        tokio::time::sleep(self.delay).await;
        self.delay = self.delay.saturating_mul(2).min(self.maximum);
    }
}

fn env_seconds(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_login_and_transport_failures_are_retryable() {
        assert!(MaintenanceBackoff::is_retryable(
            "request rejected: [13] permission denied"
        ));
        assert!(MaintenanceBackoff::is_retryable(
            "Rithmic connection failed: TLS error: unexpected EOF"
        ));
        assert!(!MaintenanceBackoff::is_retryable("invalid RITHMIC_ENV"));
    }

    #[test]
    fn delay_doubles_and_stops_at_the_cap() {
        let mut backoff = MaintenanceBackoff {
            delay: Duration::from_secs(30),
            maximum: Duration::from_secs(90),
            attempts: 0,
        };
        backoff.delay = backoff.delay.saturating_mul(2).min(backoff.maximum);
        assert_eq!(backoff.delay, Duration::from_secs(60));
        backoff.delay = backoff.delay.saturating_mul(2).min(backoff.maximum);
        assert_eq!(backoff.delay, Duration::from_secs(90));
    }
}
