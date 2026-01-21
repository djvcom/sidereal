//! Service registry for dynamic service discovery.
//!
//! Provides a unified interface for services to register themselves and
//! discover other services, supporting both single-node and distributed deployments.

mod local;

use async_trait::async_trait;
use thiserror::Error;

pub use local::LocalRegistry;

use crate::Transport;

/// Errors that can occur during registry operations.
#[derive(Error, Debug)]
pub enum RegistryError {
    /// The requested service was not found.
    #[error("Service not found: {0}")]
    NotFound(String),

    /// A service with this name is already registered.
    #[error("Service already registered: {0}")]
    AlreadyRegistered(String),

    /// The registry is shutting down.
    #[error("Registry is shutting down")]
    ShuttingDown,
}

/// Result type for registry operations.
pub type Result<T> = std::result::Result<T, RegistryError>;

/// Service registry for discovering service endpoints.
///
/// Services register themselves on startup and deregister on shutdown.
/// Clients use the registry to look up service endpoints by name.
#[async_trait]
pub trait ServiceRegistry: Send + Sync {
    /// Registers a service with the given name and endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if a service with this name is already registered.
    async fn register(&self, name: &str, endpoint: Transport) -> Result<()>;

    /// Deregisters a service by name.
    ///
    /// Returns silently if the service was not registered.
    async fn deregister(&self, name: &str) -> Result<()>;

    /// Looks up a service by name.
    ///
    /// Returns `None` if the service is not registered.
    async fn lookup(&self, name: &str) -> Result<Option<Transport>>;

    /// Lists all registered services.
    async fn list(&self) -> Result<Vec<(String, Transport)>>;
}
