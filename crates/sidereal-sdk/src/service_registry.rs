//! Service registry for discovering registered services at runtime.
//!
//! Services annotated with `#[sidereal_sdk::service]` are automatically
//! registered using the `inventory` crate's distributed slice pattern.

use crate::extractors::AppState;
use axum::Router;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// The kind of service based on return type detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceKind {
    /// Background service: async fn(Arc<AppState>, CancellationToken) -> Result<(), E>
    /// Spawned as a tokio task, runs continuously until cancelled.
    Background,
    /// Router service: fn() -> Router
    /// Mounted at a path prefix on the main HTTP server. Route handlers use axum extractors.
    Router,
}

/// Errors from background services.
#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("service failed: {0}")]
    Failed(String),

    #[error("service cancelled")]
    Cancelled,

    #[error("{0}")]
    Custom(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Type alias for background service startup.
/// Takes AppState and CancellationToken, returns a future that runs until cancelled.
pub type BackgroundServiceFn =
    fn(
        state: Arc<AppState>,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + 'static>>;

/// Type alias for router service factory.
/// Returns an axum Router to be mounted. Route handlers use extractors for state access.
pub type RouterServiceFn = fn() -> Router;

/// Union type for service factories.
pub enum ServiceFactory {
    Background(BackgroundServiceFn),
    Router(RouterServiceFn),
}

/// Metadata about a registered service.
pub struct ServiceMetadata {
    /// The service name (derived from the Rust function name).
    pub name: &'static str,

    /// The kind of service.
    pub kind: ServiceKind,

    /// Optional path prefix for router services (e.g., "/api/v1").
    /// Derived from #[service(path = "/api/v1")] attribute or defaults to "/{name}".
    pub path_prefix: Option<&'static str>,

    /// The service factory/starter function.
    pub factory: ServiceFactory,
}

// Register the inventory collection for service metadata
inventory::collect!(ServiceMetadata);

/// Get all registered services.
pub fn get_services() -> impl Iterator<Item = &'static ServiceMetadata> {
    inventory::iter::<ServiceMetadata>()
}

/// Get all background services.
pub fn get_background_services() -> impl Iterator<Item = &'static ServiceMetadata> {
    get_services().filter(|s| s.kind == ServiceKind::Background)
}

/// Get all router services.
pub fn get_router_services() -> impl Iterator<Item = &'static ServiceMetadata> {
    get_services().filter(|s| s.kind == ServiceKind::Router)
}

/// Find a service by name.
pub fn find_service(name: &str) -> Option<&'static ServiceMetadata> {
    get_services().find(|s| s.name == name)
}
