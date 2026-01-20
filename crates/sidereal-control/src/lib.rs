//! Sidereal Control Plane
//!
//! This crate provides the central orchestration layer for Sidereal deployments.
//! It coordinates between the build system, scheduler, and worker agents to
//! deploy and manage function workloads.
//!
//! # Architecture
//!
//! The control plane is responsible for:
//!
//! - **Deployment orchestration**: Receiving build completion notifications and
//!   coordinating the deployment of artifacts to workers
//! - **State management**: Tracking which deployments are active for each
//!   project/environment combination
//! - **Worker provisioning**: Acting on scaling decisions from the scheduler
//!   to boot or terminate worker VMs
//! - **API surface**: Providing HTTP endpoints for manual deployment triggers
//!   and status queries
//!
//! # State Machine
//!
//! Deployments follow a strict state machine enforced at compile time using
//! the typestate pattern:
//!
//! ```text
//! Pending ──▶ Registering ──▶ Active ──▶ Superseded
//!                 │              │
//!                 ▼              ▼
//!               Failed      Terminated
//! ```
//!
//! Invalid state transitions are caught at compile time, not runtime.
//!
//! # Example
//!
//! ```ignore
//! use sidereal_control::{
//!     Deployment, Pending,
//!     types::{DeploymentData, ProjectId},
//! };
//!
//! // Create a new deployment
//! let data = DeploymentData::new(
//!     ProjectId::new("my-project"),
//!     "production".to_owned(),
//!     "abc123".to_owned(),
//!     "s3://artifacts/rootfs.ext4".to_owned(),
//!     vec![],
//! );
//!
//! let pending = Deployment::<Pending>::create(data);
//!
//! // State transitions are type-safe
//! let registering = pending.start_registering();
//! let active = registering.activate();
//!
//! // This would not compile:
//! // let invalid = active.start_registering(); // Error!
//! ```

#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

pub mod config;
pub mod error;
pub mod scheduler;
pub mod state;
pub mod store;
pub mod strategy;
pub mod types;

// Re-export commonly used types at the crate root
pub use config::ControlConfig;
pub use error::{ControlError, ControlResult};
pub use scheduler::SchedulerClient;
pub use state::{
    Active, AnyDeployment, Deployment, DeploymentState, Failed, Pending, Registering, Superseded,
    Terminated,
};
pub use store::{DeploymentFilter, DeploymentStore, MemoryStore, PostgresStore};
pub use strategy::DeploymentStrategy;
pub use types::{
    DeploymentData, DeploymentId, DeploymentRecord, FunctionMetadata, FunctionTrigger,
    PersistedState, ProjectId,
};
