//! Firecracker VM management for Sidereal.
//!
//! This crate provides functionality for:
//! - Managing Firecracker microVM lifecycle
//! - Communication with guest via vsock
//! - Rootfs preparation and management

pub mod config;
pub mod error;
pub mod protocol;
pub mod rootfs;
pub mod vm;
pub mod vsock;

pub use config::VmConfig;
pub use error::FirecrackerError;
pub use protocol::{GuestRequest, GuestResponse};
pub use vm::{VmInstance, VmManager};
pub use vsock::VsockClient;
