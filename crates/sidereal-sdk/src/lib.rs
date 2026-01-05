//! Sidereal SDK for building serverless functions and services.
//!
//! This crate provides the user-facing API for writing Sidereal applications.
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
//! #[sidereal_sdk::function]
//! async fn greet(
//!     req: HttpRequest<GreetRequest>,
//!     ctx: Context,
//! ) -> HttpResponse<GreetResponse> {
//!     HttpResponse::ok(GreetResponse {
//!         message: format!("Hello, {}!", req.body.name),
//!     })
//! }
//! ```

pub mod config;
pub mod context;
pub mod prelude;
pub mod registry;
pub mod server;
pub mod triggers;

// Re-export the proc macro
pub use sidereal_macros::function;

#[doc(hidden)]
pub mod __internal {
    pub use inventory;
    pub use serde_json;
}

// Re-export key types at the crate root
pub use context::Context;
pub use registry::{FunctionMetadata, FunctionResult};
pub use server::{run, ServerConfig};
pub use triggers::{HttpRequest, HttpResponse, QueueMessage, TriggerKind};
