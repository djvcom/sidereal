//! Scheduler client for interacting with the scheduler HTTP API.
//!
//! This module provides a client for the scheduler service, allowing the
//! control plane to query worker status, manage placements, and drain workers.

mod client;

pub use client::SchedulerClient;

use serde::{Deserialize, Serialize};

/// Worker information returned by the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    /// Unique worker identifier.
    pub id: String,
    /// Network address.
    pub address: String,
    /// vsock CID if in Firecracker VM.
    pub vsock_cid: Option<u32>,
    /// Functions this worker can handle.
    pub functions: Vec<String>,
    /// Current status.
    pub status: String,
    /// Worker capacity.
    pub capacity: WorkerCapacity,
    /// Seconds since registration.
    pub registered_at_secs_ago: u64,
    /// Seconds since last heartbeat.
    pub last_heartbeat_secs_ago: u64,
}

impl WorkerInfo {
    /// Returns true if the worker is healthy.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.status == "Healthy"
    }

    /// Returns true if the worker is draining.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.status == "Draining"
    }

    /// Returns true if the worker can accept requests.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.status == "Healthy" || self.status == "Degraded"
    }
}

/// Worker capacity information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCapacity {
    /// Maximum concurrent invocations.
    pub max_concurrent: u32,
    /// Current load.
    pub current_load: u32,
    /// Total memory in MB.
    pub memory_mb: u32,
    /// Used memory in MB.
    pub memory_used_mb: u32,
}

impl WorkerCapacity {
    /// Returns the utilisation as a fraction (0.0-1.0).
    #[must_use]
    pub fn utilisation(&self) -> f64 {
        if self.max_concurrent == 0 {
            return 0.0;
        }
        f64::from(self.current_load) / f64::from(self.max_concurrent)
    }

    /// Returns true if the worker has available capacity.
    #[must_use]
    pub const fn has_capacity(&self) -> bool {
        self.current_load < self.max_concurrent
    }
}

/// Placement information for a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementInfo {
    /// Function name.
    pub function: String,
    /// Placement status.
    pub status: PlacementStatus,
    /// Workers that can handle this function.
    pub workers: Vec<PlacementWorker>,
}

/// Placement status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementStatus {
    /// Healthy workers available.
    Available,
    /// Workers are starting up.
    Provisioning,
    /// Workers exist but are unhealthy.
    AllUnhealthy,
    /// Function not found in placement data.
    NotFound,
}

impl PlacementStatus {
    /// Parse from string status.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "available" => Self::Available,
            "provisioning" => Self::Provisioning,
            "all_unhealthy" => Self::AllUnhealthy,
            _ => Self::NotFound,
        }
    }
}

/// Worker in a placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementWorker {
    /// Worker identifier.
    pub worker_id: String,
    /// Network address.
    pub address: String,
    /// Worker status.
    pub status: String,
}

/// Health check response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Health status.
    pub status: String,
}

/// Readiness check response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResponse {
    /// Whether the scheduler is ready.
    pub ready: bool,
    /// Number of registered workers.
    pub workers: usize,
}
