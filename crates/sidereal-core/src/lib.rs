//! Core infrastructure for Sidereal.
//!
//! This crate provides shared infrastructure components used across Sidereal services:
//!
//! - **Transport**: Abstraction over Unix sockets and TCP for service communication
//! - **Registry**: Service discovery for single-node and distributed deployments

pub mod registry;
pub mod transport;

pub use registry::{LocalRegistry, RegistryError, ServiceRegistry};
pub use transport::{Connection, Listener, Transport, TransportError};
