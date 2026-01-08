//! Configuration types for the scheduler.

use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

/// Scheduler configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SchedulerConfig {
    /// HTTP API configuration.
    pub api: ApiConfig,
    /// vsock configuration.
    pub vsock: VsockConfig,
    /// Valkey configuration.
    pub valkey: ValkeyConfig,
    /// Health check configuration.
    pub health: HealthConfig,
    /// Placement configuration.
    pub placement: PlacementConfig,
    /// Scaling configuration.
    pub scaling: ScalingConfig,
}

/// HTTP API configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    /// Address to listen on.
    pub listen_addr: SocketAddr,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8081),
        }
    }
}

/// vsock configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct VsockConfig {
    /// Port for scheduler registration.
    pub port: u32,
}

impl Default for VsockConfig {
    fn default() -> Self {
        Self {
            port: sidereal_proto::ports::SCHEDULER,
        }
    }
}

/// Valkey configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ValkeyConfig {
    /// Connection URL.
    pub url: String,
    /// TTL for placement entries in seconds.
    pub placement_ttl_secs: u64,
    /// Pub/sub channel for placement updates.
    pub pub_sub_channel: String,
    /// Maximum pool connections.
    pub max_connections: usize,
}

impl Default for ValkeyConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_owned(),
            placement_ttl_secs: 30,
            pub_sub_channel: "sidereal:placements".to_owned(),
            max_connections: 10,
        }
    }
}

/// Health check configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    /// Expected heartbeat interval from workers.
    #[serde(with = "serde_duration_secs")]
    pub heartbeat_interval: Duration,
    /// Timeout before considering a worker unhealthy.
    #[serde(with = "serde_duration_secs")]
    pub heartbeat_timeout: Duration,
    /// Number of missed heartbeats before marking unhealthy.
    pub unhealthy_threshold: u32,
    /// Number of successful heartbeats to mark healthy again.
    pub healthy_threshold: u32,
    /// Interval for active ping checks.
    #[serde(with = "serde_duration_secs")]
    pub ping_interval: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(15),
            unhealthy_threshold: 3,
            healthy_threshold: 2,
            ping_interval: Duration::from_secs(10),
        }
    }
}

/// Placement algorithm configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PlacementConfig {
    /// Placement algorithm to use.
    pub algorithm: PlacementAlgorithmType,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            algorithm: PlacementAlgorithmType::PowerOfTwo,
        }
    }
}

/// Placement algorithm types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum PlacementAlgorithmType {
    /// Simple round-robin.
    RoundRobin,
    /// Power-of-two-choices.
    PowerOfTwo,
    /// Least-loaded worker.
    LeastLoaded,
}

/// Scaling configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ScalingConfig {
    /// Minimum workers per function.
    pub min_workers: u32,
    /// Maximum workers per function.
    pub max_workers: u32,
    /// Target utilisation (0.0-1.0).
    pub target_utilisation: f64,
    /// Utilisation above which to scale up.
    pub scale_up_threshold: f64,
    /// Utilisation below which to scale down.
    pub scale_down_threshold: f64,
    /// Cooldown after scaling up.
    #[serde(with = "serde_duration_secs")]
    pub scale_up_cooldown: Duration,
    /// Cooldown after scaling down.
    #[serde(with = "serde_duration_secs")]
    pub scale_down_cooldown: Duration,
    /// Workers to add per scale-up.
    pub scale_up_step: u32,
    /// Workers to remove per scale-down.
    pub scale_down_step: u32,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            min_workers: 1,
            max_workers: 100,
            target_utilisation: 0.7,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
            scale_up_cooldown: Duration::from_secs(60),
            scale_down_cooldown: Duration::from_secs(300),
            scale_up_step: 2,
            scale_down_step: 1,
        }
    }
}

/// Serde helper for Duration as seconds.
mod serde_duration_secs {
    use serde::{Deserialize, Deserializer};
    use std::time::Duration;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = SchedulerConfig::default();
        assert_eq!(config.api.listen_addr.port(), 8081);
        assert_eq!(config.vsock.port, 1027);
        assert_eq!(config.health.unhealthy_threshold, 3);
    }

    #[test]
    fn scaling_defaults() {
        let config = ScalingConfig::default();
        assert_eq!(config.min_workers, 1);
        assert_eq!(config.max_workers, 100);
        assert!((config.target_utilisation - 0.7).abs() < f64::EPSILON);
    }
}
