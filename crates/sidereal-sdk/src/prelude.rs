//! Prelude module for convenient imports.
//!
//! # Usage
//!
//! ```ignore
//! use sidereal_sdk::prelude::*;
//! ```

// Re-export the function macro
pub use crate::function;

// Re-export trigger types
pub use crate::triggers::{ErrorResponse, HttpRequest, HttpResponse, QueueMessage};

// Re-export context
pub use crate::context::Context;

// Re-export serde derives for user types
pub use serde::{Deserialize, Serialize};
