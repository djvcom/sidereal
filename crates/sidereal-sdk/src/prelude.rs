//! Prelude module for convenient imports.
//!
//! # Usage
//!
//! ```ignore
//! use sidereal_sdk::prelude::*;
//! ```

// Re-export the function macro
pub use crate::function;

// Re-export serde derives for user types
pub use serde::{Deserialize, Serialize};
