//! Build service components.
//!
//! Provides the build worker and service configuration.

mod worker;

pub use worker::BuildWorker;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_ADDR: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 7860);

use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::Deserialize;

use crate::artifact::StorageConfig;
use crate::error::{BuildError, BuildResult};

/// Build service configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ServiceConfig {
    /// Server configuration.
    #[serde(default)]
    pub server: ServerConfig,

    /// Path configuration.
    #[serde(default)]
    pub paths: PathsConfig,

    /// Build limits.
    #[serde(default)]
    pub limits: LimitsConfig,

    /// Worker configuration.
    #[serde(default)]
    pub worker: WorkerConfig,

    /// Storage configuration.
    #[serde(default)]
    pub storage: StorageConfig,
}

impl ServiceConfig {
    /// Load configuration from file and environment.
    pub fn load() -> BuildResult<Self> {
        Figment::new()
            .merge(Toml::file("build.toml"))
            .merge(Env::prefixed("BUILD_").split("_"))
            .extract()
            .map_err(|e| BuildError::ConfigParse(e.to_string()))
    }

    /// Load configuration from a specific path.
    pub fn load_from(path: &str) -> BuildResult<Self> {
        Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("BUILD_").split("_"))
            .extract()
            .map_err(|e| BuildError::ConfigParse(e.to_string()))
    }
}

/// Server configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Listen address for the HTTP API.
    #[serde(default = "default_listen_addr")]
    pub listen_addr: SocketAddr,

    /// Maximum concurrent builds in queue.
    #[serde(default = "default_max_queue_size")]
    pub max_queue_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            max_queue_size: default_max_queue_size(),
        }
    }
}

const fn default_listen_addr() -> SocketAddr {
    SocketAddr::V4(DEFAULT_ADDR)
}

const fn default_max_queue_size() -> usize {
    100
}

/// Path configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct PathsConfig {
    /// Directory for source checkouts.
    #[serde(default = "default_checkouts_dir")]
    pub checkouts: PathBuf,

    /// Directory for build caches.
    #[serde(default = "default_caches_dir")]
    pub caches: PathBuf,

    /// Directory for build artifacts.
    #[serde(default = "default_artifacts_dir")]
    pub artifacts: PathBuf,

    /// Path to the sidereal runtime binary.
    #[serde(default = "default_runtime_path")]
    pub runtime: PathBuf,
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            checkouts: default_checkouts_dir(),
            caches: default_caches_dir(),
            artifacts: default_artifacts_dir(),
            runtime: default_runtime_path(),
        }
    }
}

fn default_checkouts_dir() -> PathBuf {
    PathBuf::from("/var/lib/sidereal/checkouts")
}

fn default_caches_dir() -> PathBuf {
    PathBuf::from("/var/lib/sidereal/caches")
}

fn default_artifacts_dir() -> PathBuf {
    PathBuf::from("/var/lib/sidereal/artifacts")
}

fn default_runtime_path() -> PathBuf {
    PathBuf::from("/usr/lib/sidereal/runtime")
}

/// Build limits configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    /// Build timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Memory limit in megabytes.
    #[serde(default = "default_memory_limit")]
    pub memory_limit_mb: u32,

    /// Maximum artifact size in megabytes.
    #[serde(default = "default_max_artifact_size")]
    pub max_artifact_size_mb: u32,
}

impl LimitsConfig {
    /// Get the timeout as a Duration.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            timeout_secs: default_timeout_secs(),
            memory_limit_mb: default_memory_limit(),
            max_artifact_size_mb: default_max_artifact_size(),
        }
    }
}

const fn default_timeout_secs() -> u64 {
    600
}

const fn default_memory_limit() -> u32 {
    4096
}

const fn default_max_artifact_size() -> u32 {
    100
}

/// Worker configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    /// Number of concurrent build workers.
    #[serde(default = "default_worker_count")]
    pub count: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            count: default_worker_count(),
        }
    }
}

const fn default_worker_count() -> usize {
    2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = ServiceConfig::default();
        assert_eq!(config.server.listen_addr.port(), 7860);
        assert_eq!(config.worker.count, 2);
        assert_eq!(config.limits.timeout(), Duration::from_secs(600));
    }
}
