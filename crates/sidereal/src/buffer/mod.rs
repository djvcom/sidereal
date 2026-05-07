//! In-memory buffering and flush to object storage.

pub mod ingester;

pub use ingester::{start_background_flush, FlushHandle, Ingester};
