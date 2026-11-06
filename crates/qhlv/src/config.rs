//! Space-agnostic core configuration.
//!
//! These are the knobs that apply to *every* qhlv node regardless of which
//! `Space` it runs. Space-specific settings (e.g. the geo noise sigma) live on
//! the `Space` implementation itself, not here.

use std::env;

/// Core configuration loaded from environment variables at startup.
/// Every field has a default so a node runs with no env setup.
#[derive(Clone, Debug)]
pub struct Config {
    /// How long an object stays alive without activity (seconds).
    pub inactivity_ttl_secs: i64,
    /// Maximum object lifetime from creation, regardless of activity (seconds).
    pub hard_cap_secs: i64,
    /// Safety cap on the serialized size of an app payload (bytes). The app's
    /// own validator enforces structure; this is just a backstop against abuse.
    pub max_payload_bytes: usize,
    /// When true, the first activity extends an object's life straight to the hard
    /// cap instead of resetting the inactivity window. Default false (the original
    /// reset-the-window behaviour).
    pub activity_extends_to_cap: bool,
}

impl Config {
    /// Read config from the environment, falling back to defaults for missing keys.
    pub fn from_env() -> Self {
        Self {
            inactivity_ttl_secs: parse_env("INACTIVITY_TTL_SECS", 1800),
            hard_cap_secs: parse_env("HARD_CAP_SECS", 3600),
            max_payload_bytes: parse_env("MAX_PAYLOAD_BYTES", 4096),
            activity_extends_to_cap: parse_env("ACTIVITY_EXTENDS_TO_CAP", false),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::from_env()
    }
}

/// Parse a typed value from an env var, returning `default` if the var is
/// absent or unparseable — so a bad value never silently becomes 0.
pub(crate) fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
