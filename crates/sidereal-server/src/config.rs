//! Unified server configuration.
//!
//! Provides configuration for running all Sidereal services in a single process.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::Deserialize;
use thiserror::Error;

/// Configuration errors.
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Failed to parse configuration.
    #[error("Configuration error: {0}")]
    Parse(String),

    /// Invalid configuration value.
    #[error("Invalid configuration: {0}")]
    Invalid(String),
}

impl From<figment::Error> for ConfigError {
    fn from(err: figment::Error) -> Self {
        Self::Parse(err.to_string())
    }
}

/// Unified server configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Server mode and socket configuration.
    #[serde(default)]
    pub server: ServerSettings,

    /// Gateway configuration.
    #[serde(default)]
    pub gateway: GatewaySettings,

    /// Scheduler configuration.
    #[serde(default)]
    pub scheduler: SchedulerSettings,

    /// Control plane configuration.
    #[serde(default)]
    pub control: ControlSettings,

    /// Build service configuration.
    #[serde(default)]
    pub build: BuildSettings,

    /// Database configuration (shared).
    #[serde(default)]
    pub database: DatabaseSettings,

    /// Valkey configuration (shared).
    #[serde(default)]
    pub valkey: ValkeySettings,

    /// Object storage configuration (shared).
    #[serde(default)]
    pub storage: StorageSettings,
}

impl ServerConfig {
    /// Load configuration from file and environment.
    ///
    /// Configuration is loaded in the following order (later sources override earlier):
    /// 1. Default values
    /// 2. `sidereal.toml` in the current directory (if present)
    /// 3. Specified config file path (if provided)
    /// 4. Environment variables with `SIDEREAL_` prefix
    pub fn load(path: Option<&str>) -> Result<Self, ConfigError> {
        let mut figment = Figment::new().merge(Toml::file("sidereal.toml"));

        if let Some(p) = path {
            figment = figment.merge(Toml::file(p));
        }

        figment
            .merge(Env::prefixed("SIDEREAL_").split("__"))
            .extract()
            .map_err(ConfigError::from)
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server: ServerSettings::default(),
            gateway: GatewaySettings::default(),
            scheduler: SchedulerSettings::default(),
            control: ControlSettings::default(),
            build: BuildSettings::default(),
            database: DatabaseSettings::default(),
            valkey: ValkeySettings::default(),
            storage: StorageSettings::default(),
        }
    }
}

/// Server mode and socket configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerSettings {
    /// Deployment mode.
    #[serde(default)]
    pub mode: DeploymentMode,

    /// Directory for Unix sockets in single-node mode.
    #[serde(default = "default_socket_dir")]
    pub socket_dir: PathBuf,

    /// Data directory for persistent state.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            mode: DeploymentMode::default(),
            socket_dir: default_socket_dir(),
            data_dir: default_data_dir(),
        }
    }
}

fn default_socket_dir() -> PathBuf {
    PathBuf::from("/run/sidereal")
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/sidereal")
}

/// Deployment mode.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum DeploymentMode {
    /// Single-node deployment with Unix socket communication.
    #[default]
    SingleNode,

    /// Distributed deployment with TCP communication.
    Distributed,
}

/// Gateway service settings.
#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySettings {
    /// Whether to enable the gateway.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// TCP address for external traffic.
    #[serde(default = "default_gateway_listen")]
    pub listen: SocketAddr,
}

impl Default for GatewaySettings {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: default_gateway_listen(),
        }
    }
}

const fn default_gateway_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8422)
}

/// Scheduler service settings.
#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerSettings {
    /// Whether to enable the scheduler.
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for SchedulerSettings {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// Control plane service settings.
#[derive(Debug, Clone, Deserialize)]
pub struct ControlSettings {
    /// Whether to enable the control plane.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Provisioner type.
    #[serde(default)]
    pub provisioner: ProvisionerType,
}

impl Default for ControlSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            provisioner: ProvisionerType::default(),
        }
    }
}

/// Worker provisioner type.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProvisionerType {
    /// Mock provisioner for testing.
    #[default]
    Mock,

    /// Firecracker microVM provisioner.
    Firecracker,
}

/// Build service settings.
#[derive(Debug, Clone, Deserialize)]
pub struct BuildSettings {
    /// Whether to enable the build service.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Number of concurrent build workers.
    #[serde(default = "default_worker_count")]
    pub workers: usize,

    /// Path configuration.
    #[serde(default)]
    pub paths: sidereal_build::service::PathsConfig,

    /// VM configuration.
    #[serde(default)]
    pub vm: sidereal_build::service::VmConfig,

    /// Git forge authentication configuration.
    #[serde(default)]
    pub forge_auth: sidereal_build::ForgeAuthConfig,
}

impl Default for BuildSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            workers: default_worker_count(),
            paths: sidereal_build::service::PathsConfig::default(),
            vm: sidereal_build::service::VmConfig::default(),
            forge_auth: sidereal_build::ForgeAuthConfig::default(),
        }
    }
}

const fn default_worker_count() -> usize {
    2
}

/// Database configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseSettings {
    /// PostgreSQL connection URL.
    #[serde(default = "default_database_url")]
    pub url: String,

    /// Maximum connection pool size.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            url: default_database_url(),
            max_connections: default_max_connections(),
        }
    }
}

fn default_database_url() -> String {
    "postgres://localhost/sidereal".to_owned()
}

const fn default_max_connections() -> u32 {
    10
}

/// Valkey (Redis) configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ValkeySettings {
    /// Connection URL.
    #[serde(default = "default_valkey_url")]
    pub url: String,
}

impl Default for ValkeySettings {
    fn default() -> Self {
        Self {
            url: default_valkey_url(),
        }
    }
}

fn default_valkey_url() -> String {
    "redis://localhost:6379".to_owned()
}

/// Object storage configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct StorageSettings {
    /// Storage backend type.
    #[serde(default)]
    pub backend: StorageBackend,

    /// S3-compatible endpoint URL.
    pub endpoint: Option<String>,

    /// S3 region (use "garage" for Garage).
    pub region: Option<String>,

    /// Bucket name.
    #[serde(default = "default_bucket")]
    pub bucket: String,

    /// Access key ID (or use environment/IAM).
    pub access_key_id: Option<String>,

    /// Secret access key (or use environment/IAM).
    pub secret_access_key: Option<String>,
}

impl Default for StorageSettings {
    fn default() -> Self {
        Self {
            backend: StorageBackend::default(),
            endpoint: None,
            region: None,
            bucket: default_bucket(),
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

fn default_bucket() -> String {
    "sidereal".to_owned()
}

/// Object storage backend type.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// Local filesystem storage.
    #[default]
    Filesystem,

    /// S3-compatible object storage.
    S3,
}

const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = ServerConfig::default();
        assert_eq!(config.server.mode, DeploymentMode::SingleNode);
        assert_eq!(config.gateway.listen.port(), 8422);
        assert!(config.gateway.enabled);
        assert!(config.scheduler.enabled);
        assert!(config.control.enabled);
        assert!(config.build.enabled);
    }

    #[test]
    fn default_paths() {
        let config = ServerConfig::default();
        assert_eq!(config.server.socket_dir, PathBuf::from("/run/sidereal"));
        assert_eq!(config.server.data_dir, PathBuf::from("/var/lib/sidereal"));
    }
}
