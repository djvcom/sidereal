//! Configuration loading for sidereal-ai.
//!
//! Configuration is loaded from `~/.config/sidereal-ai/config.toml` and
//! overridden by environment variables.
//!
//! Supported environment variables:
//! - `SIDEREAL_AI_OIDC_ISSUER`
//! - `SIDEREAL_AI_OIDC_CLIENT_ID`
//! - `SIDEREAL_AI_MODEL_PROVIDER`
//! - `SIDEREAL_AI_MODEL_NAME`
//! - `SIDEREAL_AI_MODEL_API_KEY`
//! - `SIDEREAL_AI_MODEL_BASE_URL`
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
    /// Model provider configuration.
    #[serde(default)]
    pub model: ModelConfig,
    /// Address for the HTTP API to listen on.
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

/// Model provider configuration.
#[derive(Debug, Default, Deserialize)]
pub struct ModelConfig {
    /// Which provider to use.
    #[serde(default)]
    pub provider: ModelProvider,
    /// Model name, e.g. `claude-opus-4-8`. Defaults per provider.
    pub name: Option<String>,
    /// API key. Falls back to the provider's conventional environment
    /// variable (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`) when unset.
    pub api_key: Option<String>,
    /// Base URL override, for self-hosted or OpenAI-compatible endpoints.
    pub base_url: Option<String>,
}

/// Supported model providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelProvider {
    /// The Anthropic API (default).
    #[default]
    Anthropic,
    /// A self-hosted Ollama instance.
    Ollama,
    /// An `OpenAI`-compatible endpoint (`OpenAI` itself, vLLM, and similar).
    Openai,
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
            "model_provider" => "model.provider".into(),
            "model_name" => "model.name".into(),
            "model_api_key" => "model.api_key".into(),
            "model_base_url" => "model.base_url".into(),
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn model_config_defaults_to_anthropic() {
        let config: ModelConfig = toml::from_str("").expect("empty model config should parse");
        assert_eq!(config.provider, ModelProvider::Anthropic);
        assert!(config.name.is_none());
        assert!(config.api_key.is_none());
        assert!(config.base_url.is_none());
    }

    #[test]
    fn model_provider_parses_lowercase_names() {
        let config: ModelConfig = toml::from_str(
            r#"
            provider = "ollama"
            name = "qwen2.5:14b"
            base_url = "http://localhost:11434"
            "#,
        )
        .expect("ollama model config should parse");
        assert_eq!(config.provider, ModelProvider::Ollama);
        assert_eq!(config.name.as_deref(), Some("qwen2.5:14b"));
    }

    #[test]
    fn model_provider_rejects_unknown_names() {
        let result: Result<ModelConfig, _> = toml::from_str(r#"provider = "mystery""#);
        assert!(result.is_err());
    }
}
