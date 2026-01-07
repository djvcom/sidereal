//! Sidereal scheduler - worker registration, health tracking, and placement.
//!
//! The scheduler is responsible for:
//!
//! - **Worker registration**: Tracking available workers and their capabilities
//! - **Health monitoring**: Detecting worker failures via heartbeats and active pings
//! - **Function placement**: Deciding which workers can handle each function
//! - **Scaling decisions**: Emitting scale-up/down recommendations based on load
//!
//! # Architecture
//!
//! The scheduler uses a push-with-cache model:
//! - Workers send heartbeats to the scheduler via vsock
//! - Scheduler pushes placement decisions to Valkey
//! - Gateway reads placement data from Valkey (not from scheduler)
//!
//! This keeps the scheduler out of the request path for low latency.
//!
//! # Example
//!
//! ```ignore
//! use sidereal_scheduler::{SchedulerService, SchedulerConfig};
//!
//! let config = SchedulerConfig::default();
//! let scheduler = SchedulerService::new(config).await?;
//! scheduler.run().await?;
//! ```

pub mod api;
pub mod config;
pub mod error;
pub mod health;
pub mod placement;
pub mod registry;
pub mod scaling;
pub mod store;

// Re-export main types
pub use config::{HealthConfig, PlacementAlgorithmType, PlacementConfig, ScalingConfig, SchedulerConfig};
pub use error::{Result, SchedulerError};
pub use health::HealthTracker;
pub use placement::{LeastLoaded, PlacementAlgorithm, PowerOfTwoChoices, RoundRobin};
pub use registry::{WorkerCapacity, WorkerId, WorkerInfo, WorkerRegistry, WorkerStatus};
pub use scaling::{ClusterMetrics, ScalingDecision, ScalingPolicy};
pub use store::{PlacementChange, PlacementStore, ValkeyPlacementStore, WorkerAvailability, WorkerEndpoint};
