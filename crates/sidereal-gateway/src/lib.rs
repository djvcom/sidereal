//! HTTP ingress gateway for Sidereal.
//!
//! The gateway routes incoming HTTP requests to backend workers,
//! supporting both HTTP (dev mode) and vsock (Firecracker) backends.

pub mod backend;
pub mod config;
pub mod error;
pub mod middleware;
pub mod resolver;
pub mod server;

pub use config::GatewayConfig;
pub use error::GatewayError;
pub use server::run;
