//! Sidereal unified server.
//!
//! This crate provides a unified binary for running all Sidereal services
//! in a single process, suitable for single-node deployments.
//!
//! # Architecture
//!
//! In single-node mode, services communicate via Unix sockets:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │  sidereal-server                                                │
//! │                                                                 │
//! │  Gateway ──► /run/sidereal/gateway.sock (+ TCP :8422)          │
//! │  Scheduler ► /run/sidereal/scheduler.sock                      │
//! │  Control ──► /run/sidereal/control.sock                        │
//! │  Build ────► /run/sidereal/build.sock                          │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Configuration
//!
//! Configuration is loaded from `sidereal.toml` in the current directory,
//! with environment variable overrides using the `SIDEREAL_` prefix.
//!
//! ```toml
//! [server]
//! mode = "single-node"
//! socket_dir = "/run/sidereal"
//!
//! [gateway]
//! listen = "127.0.0.1:8422"
//!
//! [database]
//! url = "postgres://localhost/sidereal"
//!
//! [valkey]
//! url = "redis://localhost:6379"
//! ```

pub mod config;
pub mod services;

pub use config::ServerConfig;
pub use services::Services;
