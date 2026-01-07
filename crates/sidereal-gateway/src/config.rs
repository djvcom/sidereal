//! Gateway configuration with layered loading and environment variable interpolation.

use figment::{
    providers::{Format, Toml},
    value::{Dict, Map, Value},
    Error as FigmentError, Figment, Metadata, Profile, Provider,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Configuration error: {0}")]
    Figment(Box<FigmentError>),

    #[error("Configuration file not found: {0}")]
    FileNotFound(String),

    #[error("Invalid configuration: {0}")]
    Invalid(String),
}

impl From<FigmentError> for ConfigError {
    fn from(err: FigmentError) -> Self {
        ConfigError::Figment(Box::new(err))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    #[serde(default)]
    pub server: ServerConfig,

    pub routing: RoutingConfig,

    #[serde(default)]
    pub middleware: MiddlewareConfig,

    #[serde(default)]
    pub limits: LimitsConfig,

    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_bind_address")]
    pub bind_address: SocketAddr,

    #[serde(default = "default_metrics_path")]
    pub path: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            bind_address: default_metrics_bind_address(),
            path: default_metrics_path(),
        }
    }
}

fn default_metrics_bind_address() -> SocketAddr {
    "127.0.0.1:9090".parse().unwrap()
}

fn default_metrics_path() -> String {
    "/metrics".to_string()
}

impl GatewayConfig {
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from("gateway.toml")
    }

    pub fn load_from(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(ConfigError::FileNotFound(path.display().to_string()));
        }

        let figment = Figment::new().merge(InterpolatingToml::file(path)?).merge(
            figment::providers::Env::prefixed("GATEWAY_")
                .split("__")
                .lowercase(false),
        );

        figment.extract::<Self>().map_err(ConfigError::from)
    }

    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let figment = Figment::new().merge(InterpolatingToml::string(content));
        figment.extract::<Self>().map_err(ConfigError::from)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,

    #[serde(
        default = "default_shutdown_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub shutdown_timeout: Duration,

    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            shutdown_timeout: default_shutdown_timeout(),
            tls: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

fn default_bind_address() -> SocketAddr {
    "127.0.0.1:8080".parse().unwrap()
}

fn default_shutdown_timeout() -> Duration {
    Duration::from_secs(30)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum RoutingConfig {
    Static {
        #[serde(default)]
        functions: HashMap<String, FunctionBackendConfig>,

        /// Default load balancing strategy for all functions.
        #[serde(default)]
        load_balance: LoadBalanceStrategyConfig,
    },
    Discovery {
        endpoint: String,
    },
}

/// Configuration for a function's backends.
#[derive(Debug, Clone, Deserialize)]
pub struct FunctionBackendConfig {
    /// Backend addresses for this function.
    pub addresses: Vec<BackendAddress>,

    /// Load balancing strategy (overrides the default).
    #[serde(default)]
    pub load_balance: Option<LoadBalanceStrategyConfig>,
}

/// Load balancing strategy configuration.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalanceStrategyConfig {
    /// Round-robin selection.
    #[default]
    RoundRobin,
    /// Random selection.
    Random,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackendAddress {
    Http { url: String },
    Vsock { uds_path: PathBuf, port: u32 },
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MiddlewareConfig {
    #[serde(default)]
    pub tracing: TracingConfig,

    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,

    #[serde(default)]
    pub circuit_breaker: Option<CircuitBreakerConfig>,

    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub secret: String,

    #[serde(default = "default_auth_algorithm")]
    pub algorithm: AuthAlgorithm,

    #[serde(default)]
    pub issuer: Option<String>,

    #[serde(default)]
    pub audience: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum AuthAlgorithm {
    #[default]
    HS256,
    HS384,
    HS512,
}

fn default_auth_algorithm() -> AuthAlgorithm {
    AuthAlgorithm::HS256
}

#[derive(Debug, Clone, Deserialize)]
pub struct TracingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    pub requests_per_second: u32,
    pub burst_size: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,

    /// Number of successful requests in half-open state before closing.
    #[serde(default = "default_success_threshold")]
    pub success_threshold: u32,

    /// Time in milliseconds before attempting recovery (open -> half-open).
    #[serde(default = "default_reset_timeout_ms")]
    pub reset_timeout_ms: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: default_failure_threshold(),
            success_threshold: default_success_threshold(),
            reset_timeout_ms: default_reset_timeout_ms(),
        }
    }
}

fn default_failure_threshold() -> u32 {
    5
}

fn default_success_threshold() -> u32 {
    3
}

fn default_reset_timeout_ms() -> u32 {
    30_000 // 30 seconds
}

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    #[serde(default = "default_max_body_size")]
    pub max_body_size: usize,

    #[serde(default = "default_max_header_size")]
    pub max_header_size: usize,

    #[serde(default = "default_max_uri_length")]
    pub max_uri_length: usize,

    #[serde(
        default = "default_request_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub request_timeout: Duration,

    #[serde(
        default = "default_connect_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub connect_timeout: Duration,

    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    #[serde(default = "default_max_connections_per_ip")]
    pub max_connections_per_ip: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_body_size: default_max_body_size(),
            max_header_size: default_max_header_size(),
            max_uri_length: default_max_uri_length(),
            request_timeout: default_request_timeout(),
            connect_timeout: default_connect_timeout(),
            max_connections: default_max_connections(),
            max_connections_per_ip: default_max_connections_per_ip(),
        }
    }
}

fn default_max_body_size() -> usize {
    10 * 1024 * 1024 // 10MB
}

fn default_max_header_size() -> usize {
    16 * 1024 // 16KB
}

fn default_max_uri_length() -> usize {
    8 * 1024 // 8KB
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_max_connections() -> usize {
    10_000
}

fn default_max_connections_per_ip() -> usize {
    100
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_duration(&s).map_err(serde::de::Error::custom)
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if let Some(stripped) = s.strip_suffix("ms") {
        let ms: u64 = stripped
            .trim()
            .parse()
            .map_err(|_| format!("Invalid duration: {}", s))?;
        Ok(Duration::from_millis(ms))
    } else if let Some(stripped) = s.strip_suffix('s') {
        let secs: u64 = stripped
            .trim()
            .parse()
            .map_err(|_| format!("Invalid duration: {}", s))?;
        Ok(Duration::from_secs(secs))
    } else if let Some(stripped) = s.strip_suffix('m') {
        let mins: u64 = stripped
            .trim()
            .parse()
            .map_err(|_| format!("Invalid duration: {}", s))?;
        Ok(Duration::from_secs(mins * 60))
    } else {
        let secs: u64 = s.parse().map_err(|_| format!("Invalid duration: {}", s))?;
        Ok(Duration::from_secs(secs))
    }
}

struct EnvVarInterpolator;

impl EnvVarInterpolator {
    fn interpolate_value(value: Value) -> Value {
        match value {
            Value::String(_, s) => {
                let interpolated = Self::interpolate_string(&s);
                Value::from(interpolated)
            }
            Value::Dict(tag, dict) => {
                let new_dict: Dict = dict
                    .into_iter()
                    .map(|(k, v)| (k, Self::interpolate_value(v)))
                    .collect();
                Value::Dict(tag, new_dict)
            }
            Value::Array(tag, arr) => {
                let new_arr: Vec<Value> = arr.into_iter().map(Self::interpolate_value).collect();
                Value::Array(tag, new_arr)
            }
            other => other,
        }
    }

    fn interpolate_string(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '$' && chars.peek() == Some(&'{') {
                chars.next();
                let mut var_name = String::new();

                for ch in chars.by_ref() {
                    if ch == '}' {
                        break;
                    }
                    var_name.push(ch);
                }

                if let Ok(val) = std::env::var(&var_name) {
                    result.push_str(&val);
                }
            } else {
                result.push(c);
            }
        }

        result
    }
}

pub struct InterpolatingToml {
    content: String,
}

impl InterpolatingToml {
    pub fn file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|_| ConfigError::FileNotFound(path.as_ref().display().to_string()))?;
        Ok(Self { content })
    }

    pub fn string(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
        }
    }
}

impl Provider for InterpolatingToml {
    fn metadata(&self) -> Metadata {
        Metadata::named("Interpolating TOML")
    }

    fn data(&self) -> Result<Map<Profile, Dict>, FigmentError> {
        let base = Toml::string(&self.content);
        let base_data = base.data()?;

        let interpolated: Map<Profile, Dict> = base_data
            .into_iter()
            .map(|(profile, dict)| {
                let new_dict: Dict = dict
                    .into_iter()
                    .map(|(k, v)| (k, EnvVarInterpolator::interpolate_value(v)))
                    .collect();
                (profile, new_dict)
            })
            .collect();

        Ok(interpolated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
    }

    #[test]
    fn parse_duration_milliseconds() {
        assert_eq!(parse_duration("100ms").unwrap(), Duration::from_millis(100));
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn parse_duration_bare_number() {
        assert_eq!(parse_duration("60").unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn config_from_string() {
        let config_str = r#"
            [server]
            bind_address = "0.0.0.0:9000"
            shutdown_timeout = "60s"

            [routing]
            mode = "static"

            [routing.functions.hello]
            addresses = [{ type = "http", url = "http://127.0.0.1:7850" }]

            [limits]
            max_body_size = 5242880
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();

        assert_eq!(config.server.bind_address, "0.0.0.0:9000".parse().unwrap());
        assert_eq!(config.server.shutdown_timeout, Duration::from_secs(60));
        assert_eq!(config.limits.max_body_size, 5 * 1024 * 1024);

        match config.routing {
            RoutingConfig::Static { functions, .. } => {
                assert!(functions.contains_key("hello"));
            }
            _ => panic!("Expected static routing"),
        }
    }

    #[test]
    fn config_defaults() {
        let config_str = r#"
            [routing]
            mode = "static"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();

        assert_eq!(
            config.server.bind_address,
            "127.0.0.1:8080".parse().unwrap()
        );
        assert_eq!(config.limits.max_body_size, 10 * 1024 * 1024);
        assert_eq!(config.limits.request_timeout, Duration::from_secs(30));
    }

    #[test]
    fn config_rate_limit() {
        let config_str = r#"
            [routing]
            mode = "static"

            [middleware.rate_limit]
            requests_per_second = 100
            burst_size = 50
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();

        let rate_limit = config.middleware.rate_limit.expect("Rate limit should be configured");
        assert_eq!(rate_limit.requests_per_second, 100);
        assert_eq!(rate_limit.burst_size, 50);
    }

    #[test]
    fn config_rate_limit_disabled_by_default() {
        let config_str = r#"
            [routing]
            mode = "static"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        assert!(config.middleware.rate_limit.is_none());
    }

    #[test]
    fn config_circuit_breaker() {
        let config_str = r#"
            [routing]
            mode = "static"

            [middleware.circuit_breaker]
            failure_threshold = 10
            success_threshold = 5
            reset_timeout_ms = 60000
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();

        let cb = config.middleware.circuit_breaker.expect("Circuit breaker should be configured");
        assert_eq!(cb.failure_threshold, 10);
        assert_eq!(cb.success_threshold, 5);
        assert_eq!(cb.reset_timeout_ms, 60000);
    }

    #[test]
    fn config_circuit_breaker_defaults() {
        let config_str = r#"
            [routing]
            mode = "static"

            [middleware.circuit_breaker]
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();

        let cb = config.middleware.circuit_breaker.expect("Circuit breaker should be configured");
        assert_eq!(cb.failure_threshold, 5);
        assert_eq!(cb.success_threshold, 3);
        assert_eq!(cb.reset_timeout_ms, 30000);
    }

    #[test]
    fn config_circuit_breaker_disabled_by_default() {
        let config_str = r#"
            [routing]
            mode = "static"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        assert!(config.middleware.circuit_breaker.is_none());
    }

    #[test]
    fn config_load_balance_default() {
        let config_str = r#"
            [routing]
            mode = "static"

            [routing.functions.hello]
            addresses = [{ type = "http", url = "http://127.0.0.1:7850" }]
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        match config.routing {
            RoutingConfig::Static { load_balance, .. } => {
                assert_eq!(load_balance, LoadBalanceStrategyConfig::RoundRobin);
            }
            _ => panic!("Expected static routing"),
        }
    }

    #[test]
    fn config_load_balance_random() {
        let config_str = r#"
            [routing]
            mode = "static"
            load_balance = "random"

            [routing.functions.hello]
            addresses = [{ type = "http", url = "http://127.0.0.1:7850" }]
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        match config.routing {
            RoutingConfig::Static { load_balance, .. } => {
                assert_eq!(load_balance, LoadBalanceStrategyConfig::Random);
            }
            _ => panic!("Expected static routing"),
        }
    }

    #[test]
    fn config_multiple_backends() {
        let config_str = r#"
            [routing]
            mode = "static"

            [routing.functions.hello]
            addresses = [
                { type = "http", url = "http://backend1:8080" },
                { type = "http", url = "http://backend2:8080" },
                { type = "http", url = "http://backend3:8080" }
            ]
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        match config.routing {
            RoutingConfig::Static { functions, .. } => {
                let func = functions.get("hello").expect("Function 'hello' should exist");
                assert_eq!(func.addresses.len(), 3);
            }
            _ => panic!("Expected static routing"),
        }
    }

    #[test]
    fn config_per_function_load_balance() {
        let config_str = r#"
            [routing]
            mode = "static"
            load_balance = "round_robin"

            [routing.functions.hello]
            addresses = [{ type = "http", url = "http://127.0.0.1:7850" }]
            load_balance = "random"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        match config.routing {
            RoutingConfig::Static { functions, load_balance } => {
                assert_eq!(load_balance, LoadBalanceStrategyConfig::RoundRobin);
                let func = functions.get("hello").expect("Function 'hello' should exist");
                assert_eq!(func.load_balance, Some(LoadBalanceStrategyConfig::Random));
            }
            _ => panic!("Expected static routing"),
        }
    }

    #[test]
    fn config_tls() {
        let config_str = r#"
            [server]
            bind_address = "0.0.0.0:8443"

            [server.tls]
            cert_path = "/etc/ssl/certs/server.crt"
            key_path = "/etc/ssl/private/server.key"

            [routing]
            mode = "static"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        let tls = config.server.tls.expect("TLS should be configured");
        assert_eq!(tls.cert_path.to_str().unwrap(), "/etc/ssl/certs/server.crt");
        assert_eq!(tls.key_path.to_str().unwrap(), "/etc/ssl/private/server.key");
    }

    #[test]
    fn config_tls_disabled_by_default() {
        let config_str = r#"
            [routing]
            mode = "static"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        assert!(config.server.tls.is_none());
    }

    #[test]
    fn config_auth() {
        let config_str = r#"
            [routing]
            mode = "static"

            [middleware.auth]
            secret = "my-secret-key"
            algorithm = "HS384"
            issuer = "my-issuer"
            audience = "my-audience"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        let auth = config.middleware.auth.expect("Auth should be configured");
        assert_eq!(auth.secret, "my-secret-key");
        assert_eq!(auth.algorithm, AuthAlgorithm::HS384);
        assert_eq!(auth.issuer.as_deref(), Some("my-issuer"));
        assert_eq!(auth.audience.as_deref(), Some("my-audience"));
    }

    #[test]
    fn config_auth_defaults() {
        let config_str = r#"
            [routing]
            mode = "static"

            [middleware.auth]
            secret = "test-secret"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        let auth = config.middleware.auth.expect("Auth should be configured");
        assert_eq!(auth.algorithm, AuthAlgorithm::HS256);
        assert!(auth.issuer.is_none());
        assert!(auth.audience.is_none());
    }

    #[test]
    fn config_auth_disabled_by_default() {
        let config_str = r#"
            [routing]
            mode = "static"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        assert!(config.middleware.auth.is_none());
    }

    #[test]
    fn config_metrics() {
        let config_str = r#"
            [routing]
            mode = "static"

            [metrics]
            bind_address = "0.0.0.0:9090"
            path = "/prom/metrics"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        let metrics = config.metrics.expect("Metrics should be configured");
        assert_eq!(metrics.bind_address, "0.0.0.0:9090".parse().unwrap());
        assert_eq!(metrics.path, "/prom/metrics");
    }

    #[test]
    fn config_metrics_defaults() {
        let config_str = r#"
            [routing]
            mode = "static"

            [metrics]
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        let metrics = config.metrics.expect("Metrics should be configured");
        assert_eq!(metrics.bind_address, "127.0.0.1:9090".parse().unwrap());
        assert_eq!(metrics.path, "/metrics");
    }

    #[test]
    fn config_metrics_disabled_by_default() {
        let config_str = r#"
            [routing]
            mode = "static"
        "#;

        let config = GatewayConfig::parse(config_str).unwrap();
        assert!(config.metrics.is_none());
    }
}
