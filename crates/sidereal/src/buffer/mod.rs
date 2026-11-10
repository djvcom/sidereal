//! In-memory buffering and flush to object storage.

pub mod ingester;
pub mod layout;

pub use ingester::{start_background_flush, FlushHandle, Ingester};
pub use layout::FileLayout;
