//! Sidereal SDK for building serverless functions and services.
//!
//! This crate provides the user-facing API for writing Sidereal applications.
//!
//! # Functions Example
//!
//! ```no_run
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
//! ) -> HttpResponse<GreetResponse> {
//!     HttpResponse::ok(GreetResponse {
//!         message: format!("Hello, {}!", req.body.name),
//!     })
//! }
//! ```
//!
//! # Functions with Extractors
//!
//! ```no_run
//! use sidereal_sdk::prelude::*;
//!
//! #[derive(Deserialize)]
//! struct StripeConfig {
//!     api_key: String,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct CreateOrderPayload {
//!     item_id: String,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct Order {
//!     id: String,
//! }
//!
//! #[sidereal_sdk::function]
//! async fn create_order(
//!     req: HttpRequest<CreateOrderPayload>,
//!     Config(stripe): Config<StripeConfig>,
//!     secrets: Secrets,
//! ) -> HttpResponse<Order> {
//!     let _api_key = secrets.get("STRIPE_API_KEY");
//!     HttpResponse::ok(Order { id: "123".into() })
//! }
//! ```
//!
//! # Services Example
//!
//! ```no_run
//! use sidereal_sdk::prelude::*;
//! use axum::routing::get;
//! use std::time::Duration;
//!
//! // Background service - runs continuously
//! #[sidereal_sdk::service]
//! async fn background_worker(cancel: CancellationToken) -> Result<(), ServiceError> {
//!     loop {
//!         tokio::select! {
//!             _ = cancel.cancelled() => break,
//!             _ = tokio::time::sleep(Duration::from_secs(60)) => {
//!                 // Do work...
//!             }
//!         }
//!     }
//!     Ok(())
//! }
//!
//! // Router service - mounted at a path prefix
//! #[sidereal_sdk::service(path = "/api")]
//! fn api_service() -> Router {
//!     Router::new()
//!         .route("/health", get(|| async { "OK" }))
//! }
//! ```

pub mod config;
pub mod context;
pub mod extractors;
pub mod prelude;
pub mod registry;
pub mod server;
pub mod service_registry;
pub mod tracing_layer;
pub mod triggers;

// Re-export the proc macros
pub use sidereal_macros::{function, service};

#[doc(hidden)]
pub mod __internal {
    pub use axum;
    pub use inventory;
    pub use serde_json;
    pub use tokio_util;

    use crate::ServiceError;

    /// Helper to convert various return types to Result<(), ServiceError>.
    pub fn convert_service_result<T: IntoServiceResult>(result: T) -> Result<(), ServiceError> {
        result.into_service_result()
    }

    pub trait IntoServiceResult {
        fn into_service_result(self) -> Result<(), ServiceError>;
    }

    impl IntoServiceResult for () {
        fn into_service_result(self) -> Result<(), ServiceError> {
            Ok(())
        }
    }

    impl<E: std::error::Error + Send + Sync + 'static> IntoServiceResult for Result<(), E> {
        fn into_service_result(self) -> Result<(), ServiceError> {
            self.map_err(|e| ServiceError::Custom(Box::new(e)))
        }
    }
}

// Re-export key types at the crate root
pub use config::{ConfigError, ConfigManager};
pub use extractors::{
    AppState, Config, ConfigRejection, InvocationMeta, InvocationMetaRejection, Kv, KvClient,
    KvError, KvRejection, SecretError, Secrets, SecretsRejection,
};
pub use registry::{FunctionMetadata, FunctionResult};
pub use server::{run, ServerConfig};
pub use service_registry::{
    get_background_services, get_router_services, get_services, ServiceError, ServiceFactory,
    ServiceKind, ServiceMetadata,
};
pub use tracing_layer::{OtelTraceLayer, OtelTraceService};
pub use triggers::{HttpRequest, HttpResponse, QueueMessage, TriggerKind};
