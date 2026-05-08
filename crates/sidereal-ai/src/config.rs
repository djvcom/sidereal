//! Configuration loading for sidereal-ai.
//!
//! Configuration is loaded from `~/.config/sidereal-ai/config.toml` and
//! overridden by environment variables.
//!
//! Supported environment variables:
//! - `SIDEREAL_AI_OIDC_ISSUER`
//! - `SIDEREAL_AI_OIDC_CLIENT_ID`
//! - `SIDEREAL_URL`
//! - `SIDEREAL_LISTEN_ADDRESS`

use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::Deserialize;
use std::path::PathBuf;

/// Top-level configuration.
#[derive(Debug, Deserialize)]
pub struct Config {
    /// OIDC authentication configuration.
    pub oidc: OidcConfig,
    /// Sidereal service configuration.
    pub sidereal: SiderealConfig,
    /// Address for the future HTTP API to listen on.
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
}

/// OIDC provider configuration.
#[derive(Debug, Deserialize)]
pub struct OidcConfig {
    /// OIDC issuer URL, used for discovery.
    pub issuer: String,
    /// `OAuth2` client identifier.
    pub client_id: String,
}

/// Sidereal service connection configuration.
#[derive(Debug, Deserialize)]
pub struct SiderealConfig {
    /// Base URL of the Sidereal query API.
    pub url: String,
}

fn default_listen_address() -> String {
    "127.0.0.1:3200".to_owned()
}

/// Error returned when configuration cannot be loaded or parsed.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ConfigError(Box<figment::Error>);

impl From<figment::Error> for ConfigError {
    fn from(e: figment::Error) -> Self {
        Self(Box::new(e))
    }
}

/// Load configuration from disk and environment variables.
///
/// Reads `~/.config/sidereal-ai/config.toml` as the base, then applies
/// environment variable overrides.
pub fn load() -> Result<Config, ConfigError> {
    let config_path = config_file_path();
    Figment::new()
        .merge(Toml::file(config_path))
        .merge(Env::prefixed("SIDEREAL_AI_").map(|k| match k.as_str() {
            "oidc_issuer" => "oidc.issuer".into(),
            "oidc_client_id" => "oidc.client_id".into(),
            other => other.into(),
        }))
        .merge(Env::raw().map(|k| match k.as_str() {
            "SIDEREAL_URL" => "sidereal.url".into(),
            "SIDEREAL_LISTEN_ADDRESS" => "listen_address".into(),
            other => other.into(),
        }))
        .extract()
        .map_err(ConfigError::from)
}

/// Path to the configuration file.
pub fn config_file_path() -> PathBuf {
    xdg_config_dir().join("sidereal-ai").join("config.toml")
}

/// Path to the token cache file.
pub fn token_cache_path() -> PathBuf {
    xdg_config_dir().join("sidereal-ai").join("tokens.json")
}

fn xdg_config_dir() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        return PathBuf::from(xdg);
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config")
}
