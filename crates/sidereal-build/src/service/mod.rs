//! Build service components.
//!
//! Provides the build worker and service configuration.

mod worker;

pub use worker::BuildWorker;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use sidereal_core::Transport;

use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::Deserialize;

use crate::artifact::StorageConfig;
use crate::cache::CacheConfig;
use crate::error::{BuildError, BuildResult};
use crate::forge_auth::ForgeAuthConfig;

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

    /// Build cache configuration.
    #[serde(default)]
    pub cache: CacheConfig,

    /// Git forge authentication.
    #[serde(default)]
    pub forge_auth: ForgeAuthConfig,

    /// VM-based compilation configuration.
    #[serde(default)]
    pub vm: VmConfig,
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
    /// Transport to listen on (TCP or Unix socket).
    #[serde(default = "default_listen")]
    pub listen: Transport,

    /// Maximum concurrent builds in queue.
    #[serde(default = "default_max_queue_size")]
    pub max_queue_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            max_queue_size: default_max_queue_size(),
        }
    }
}

fn default_listen() -> Transport {
    Transport::tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7860))
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
    1800
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

/// VM-based compilation configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct VmConfig {
    /// Path to the Linux kernel for builder VMs.
    #[serde(default = "default_kernel_path")]
    pub kernel_path: PathBuf,

    /// Path to the builder rootfs image.
    #[serde(default = "default_builder_rootfs")]
    pub builder_rootfs: PathBuf,

    /// Number of vCPUs for builder VMs.
    #[serde(default = "default_vcpu_count")]
    pub vcpu_count: u8,

    /// Memory size in MB for builder VMs.
    #[serde(default = "default_vm_memory")]
    pub mem_size_mib: u32,

    /// Rust target triple.
    #[serde(default = "default_target")]
    pub target: String,

    /// S3 key for the runtime binary.
    #[serde(default = "default_runtime_s3_key")]
    pub runtime_s3_key: String,
}

impl Default for VmConfig {
    fn default() -> Self {
        Self {
            kernel_path: default_kernel_path(),
            builder_rootfs: default_builder_rootfs(),
            vcpu_count: default_vcpu_count(),
            mem_size_mib: default_vm_memory(),
            target: default_target(),
            runtime_s3_key: default_runtime_s3_key(),
        }
    }
}

fn default_runtime_s3_key() -> String {
    "runtime/sidereal-runtime".to_owned()
}

fn default_kernel_path() -> PathBuf {
    PathBuf::from("/var/lib/sidereal/kernel/vmlinux")
}

fn default_builder_rootfs() -> PathBuf {
    PathBuf::from("/var/lib/sidereal/rootfs/builder.ext4")
}

const fn default_vcpu_count() -> u8 {
    8
}

const fn default_vm_memory() -> u32 {
    16384
}

fn default_target() -> String {
    "x86_64-unknown-linux-musl".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = ServiceConfig::default();
        match &config.server.listen {
            Transport::Tcp { addr } => assert_eq!(addr.port(), 7860),
            Transport::Unix { .. } => panic!("expected TCP transport"),
        }
        assert_eq!(config.worker.count, 2);
        assert_eq!(config.limits.timeout(), Duration::from_secs(1800));
    }
}
