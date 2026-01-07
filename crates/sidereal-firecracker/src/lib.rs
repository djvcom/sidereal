//! Firecracker VM management for Sidereal.
//!
//! This crate provides functionality for:
//! - Managing Firecracker microVM lifecycle
//! - Communication with guest via vsock
//! - Rootfs preparation and management
//! - State proxying for guest state operations

pub mod config;
pub mod error;
pub mod rootfs;
pub mod vm;
pub mod vsock;

#[cfg(feature = "state-server")]
pub mod state_server;

pub use config::VmConfig;
pub use error::FirecrackerError;
pub use vm::{VmInstance, VmManager};
pub use vsock::VsockClient;

#[cfg(feature = "state-server")]
pub use state_server::{StateBackends, StateRequestHandler};

// Re-export protocol types from sidereal-proto for convenience
pub use sidereal_proto::{
    ControlMessage, Envelope, FunctionMessage, InvokeRequest, InvokeResponse,
};
