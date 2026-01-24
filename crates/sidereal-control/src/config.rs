//! Configuration for sidereal-control.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::Deserialize;
use sidereal_core::Transport;

use crate::error::{ControlError, ControlResult};
use crate::strategy::DeploymentStrategy;

/// Top-level configuration for the control service.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ControlConfig {
    /// HTTP server configuration.
    #[serde(default)]
    pub server: ServerConfig,

    /// Database configuration.
    #[serde(default)]
    pub database: DatabaseConfig,

    /// Scheduler client configuration.
    #[serde(default)]
    pub scheduler: SchedulerConfig,

    /// Worker provisioner configuration.
    #[serde(default)]
    pub provisioner: ProvisionerConfig,

    /// Artifact storage configuration.
    #[serde(default)]
    pub artifacts: ArtifactConfig,

    /// Deployment behaviour configuration.
    #[serde(default)]
    pub deployment: DeploymentConfig,
}

impl ControlConfig {
    /// Load configuration from the default sources.
    ///
    /// Configuration is loaded in the following order (later sources override earlier):
    /// 1. Default values
    /// 2. `control.toml` in the current directory (if present)
    /// 3. Environment variables with `SIDEREAL_CONTROL_` prefix
    pub fn load() -> ControlResult<Self> {
        Figment::new()
            .merge(Toml::file("control.toml"))
            .merge(Env::prefixed("SIDEREAL_CONTROL_").split("__"))
            .extract()
            .map_err(|e| ControlError::Config(e.to_string()))
    }

    /// Load configuration from a specific TOML file.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> ControlResult<Self> {
        Figment::new()
            .merge(Toml::file(path.as_ref()))
            .merge(Env::prefixed("SIDEREAL_CONTROL_").split("__"))
            .extract()
            .map_err(|e| ControlError::Config(e.to_string()))
    }
}

/// HTTP server configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Transport to listen on (TCP or Unix socket).
    #[serde(default = "default_listen")]
    pub listen: Transport,

    /// Request timeout in seconds.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

fn default_listen() -> Transport {
    Transport::tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8083))
}

const fn default_request_timeout_secs() -> u64 {
    30
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            request_timeout_secs: default_request_timeout_secs(),
        }
    }
}

/// Database configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    /// PostgreSQL connection URL.
    #[serde(default = "default_database_url")]
    pub url: String,

    /// Maximum number of connections in the pool.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Minimum number of connections in the pool.
    #[serde(default = "default_min_connections")]
    pub min_connections: u32,

    /// Connection timeout in seconds.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
}

fn default_database_url() -> String {
    "postgres://localhost/sidereal".to_owned()
}

const fn default_max_connections() -> u32 {
    10
}

const fn default_min_connections() -> u32 {
    1
}

const fn default_connect_timeout_secs() -> u64 {
    5
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: default_database_url(),
            max_connections: default_max_connections(),
            min_connections: default_min_connections(),
            connect_timeout_secs: default_connect_timeout_secs(),
        }
    }
}

/// Scheduler client configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    /// Base URL for the scheduler HTTP API.
    #[serde(default = "default_scheduler_url")]
    pub url: String,

    /// Request timeout in seconds.
    #[serde(default = "default_scheduler_timeout_secs")]
    pub timeout_secs: u64,

    /// How often to poll for scaling decisions (seconds).
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
}

fn default_scheduler_url() -> String {
    "http://localhost:8082".to_owned()
}

const fn default_scheduler_timeout_secs() -> u64 {
    10
}

const fn default_poll_interval_secs() -> u64 {
    5
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            url: default_scheduler_url(),
            timeout_secs: default_scheduler_timeout_secs(),
            poll_interval_secs: default_poll_interval_secs(),
        }
    }
}

/// Worker provisioner configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ProvisionerConfig {
    /// Type of provisioner to use.
    #[serde(default)]
    pub provisioner_type: ProvisionerType,

    /// Path to the Firecracker kernel image.
    #[serde(default = "default_kernel_path")]
    pub kernel_path: PathBuf,

    /// Working directory for VM files.
    #[serde(default = "default_work_dir")]
    pub work_dir: PathBuf,
}

fn default_kernel_path() -> PathBuf {
    PathBuf::from("/opt/sidereal/vmlinux")
}

fn default_work_dir() -> PathBuf {
    PathBuf::from("/var/lib/sidereal/vms")
}

impl Default for ProvisionerConfig {
    fn default() -> Self {
        Self {
            provisioner_type: ProvisionerType::default(),
            kernel_path: default_kernel_path(),
            work_dir: default_work_dir(),
        }
    }
}

/// Type of worker provisioner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionerType {
    /// Firecracker microVM provisioner.
    #[default]
    Firecracker,

    /// Mock provisioner for testing.
    Mock,
}

/// Artifact storage configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ArtifactConfig {
    /// Object store URL (e.g., "s3://bucket" or "file:///path").
    #[serde(default = "default_store_url")]
    pub store_url: String,

    /// Local cache directory for downloaded artifacts.
    #[serde(default = "default_cache_dir")]
    pub cache_dir: PathBuf,

    /// S3 endpoint URL (for S3-compatible stores like Garage).
    pub endpoint: Option<String>,

    /// S3 region (use "garage" for Garage).
    pub region: Option<String>,

    /// S3 access key ID.
    pub access_key_id: Option<String>,

    /// S3 secret access key.
    pub secret_access_key: Option<String>,
}

fn default_store_url() -> String {
    "file:///var/lib/sidereal/artifacts".to_owned()
}

fn default_cache_dir() -> PathBuf {
    PathBuf::from("/var/cache/sidereal/artifacts")
}

impl Default for ArtifactConfig {
    fn default() -> Self {
        Self {
            store_url: default_store_url(),
            cache_dir: default_cache_dir(),
            endpoint: None,
            region: None,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

/// Deployment behaviour configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DeploymentConfig {
    /// Default deployment strategy.
    #[serde(default)]
    pub strategy: DeploymentStrategy,

    /// Timeout for deployment operations in seconds.
    #[serde(default = "default_deployment_timeout_secs")]
    pub timeout_secs: u64,

    /// Maximum concurrent deployments.
    #[serde(default = "default_max_concurrent_deployments")]
    pub max_concurrent: usize,
}

const fn default_deployment_timeout_secs() -> u64 {
    300 // 5 minutes
}

const fn default_max_concurrent_deployments() -> usize {
    10
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            strategy: DeploymentStrategy::default(),
            timeout_secs: default_deployment_timeout_secs(),
            max_concurrent: default_max_concurrent_deployments(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = ControlConfig::default();
        match &config.server.listen {
            Transport::Tcp { addr } => assert_eq!(addr.port(), 8083),
            Transport::Unix { .. } => panic!("expected TCP transport"),
        }
        assert_eq!(config.database.max_connections, 10);
        assert_eq!(config.scheduler.url, "http://localhost:8082");
        assert_eq!(config.deployment.strategy, DeploymentStrategy::Simple);
    }

    #[test]
    fn config_from_toml() {
        let toml = r#"
            [server.listen]
            type = "tcp"
            addr = "127.0.0.1:9000"

            [database]
            url = "postgres://user:pass@db:5432/mydb"
            max_connections = 20

            [deployment]
            strategy = "intent_log"
        "#;

        let config: ControlConfig = toml::from_str(toml).unwrap();
        match &config.server.listen {
            Transport::Tcp { addr } => assert_eq!(addr.port(), 9000),
            Transport::Unix { .. } => panic!("expected TCP transport"),
        }
        assert_eq!(config.database.url, "postgres://user:pass@db:5432/mydb");
        assert_eq!(config.database.max_connections, 20);
        assert_eq!(config.deployment.strategy, DeploymentStrategy::IntentLog);
    }
}
