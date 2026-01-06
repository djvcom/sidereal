//! Prelude module for convenient imports.
//!
//! # Usage
//!
//! ```no_run
//! use sidereal_sdk::prelude::*;
//! # fn main() {}
//! ```

// Re-export the proc macros
pub use crate::{function, service};

// Re-export trigger types
pub use crate::triggers::{ErrorResponse, HttpRequest, HttpResponse, QueueMessage};

// Re-export extractors
pub use crate::extractors::{
    AppState, Config, InvocationMeta, Kv, KvClient, Lock, LockClient, Queue, QueueClient, Secrets,
};

// Re-export state types
pub use sidereal_state::LockGuard;

// Re-export config types
pub use crate::config::{ConfigError, ConfigManager};

// Re-export service types
pub use crate::service_registry::{ServiceError, ServiceKind};

// Re-export CancellationToken for background services
pub use tokio_util::sync::CancellationToken;

// Re-export Router and State for router services
pub use axum::{extract::State, Router};

// Re-export serde derives for user types
pub use serde::{Deserialize, Serialize};
