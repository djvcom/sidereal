//! Sidereal Telemetry - Self-hosted observability backend.
//!
//! This crate provides a full-featured observability backend that:
//! - Receives standard OTLP (gRPC + HTTP) for traces, metrics, and logs
//! - Converts to Apache Arrow format for efficient storage and querying
//! - Stores in tiered architecture: memory buffer → object storage (Parquet)
//! - Queries via DataFusion with automatic partition pruning
//!
//! ## Architecture
//!
//! ```text
//! OTLP gRPC/HTTP → Arrow Buffer → Parquet → Object Storage
//!                                              ↓
//!                               DataFusion (ListingTable)
//!                                              ↓
//!                                    HTTP Query API
//! ```

pub mod buffer;
pub mod config;
pub mod deployments;
pub mod error;
pub mod errors;
pub mod ingest;
pub mod query;
pub mod redact;
pub mod schema;
pub mod storage;

#[cfg(test)]
pub mod test_fixtures;

pub use config::TelemetryConfig;
pub use error::TelemetryError;
pub use storage::Signal;
