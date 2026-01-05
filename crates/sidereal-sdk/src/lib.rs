//! Sidereal SDK for building serverless functions.
//!
//! This crate provides the user-facing API for writing Sidereal functions.
//!
//! # Example
//!
//! ```ignore
//! use sidereal_sdk::prelude::*;
//!
//! #[derive(Serialize, Deserialize)]
//! pub struct GreetRequest {
//!     pub name: String,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! pub struct GreetResponse {
//!     pub message: String,
//! }
//!
//! #[sidereal::function]
//! async fn greet(request: GreetRequest) -> GreetResponse {
//!     GreetResponse {
//!         message: format!("Hello, {}!", request.name),
//!     }
//! }
//! ```

pub mod prelude;
pub mod runtime;

// Re-export the proc macro
pub use sidereal_macros::function;
