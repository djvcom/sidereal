//! Worker backend abstractions for dispatching requests.

mod http;

pub use self::http::HttpBackend;

use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::GatewayError;

/// Address of a worker backend.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerAddress {
    /// HTTP endpoint (dev mode).
    Http { url: String },
    /// vsock endpoint (Firecracker).
    Vsock { uds_path: PathBuf, port: u32 },
}

impl std::fmt::Display for WorkerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerAddress::Http { url } => write!(f, "{}", url),
            WorkerAddress::Vsock { uds_path, port } => {
                write!(f, "vsock://{}:{}", uds_path.display(), port)
            }
        }
    }
}

/// Request to be dispatched to a worker.
pub struct DispatchRequest {
    pub function_name: String,
    pub payload: Vec<u8>,
    pub trace_id: String,
    pub headers: ::http::HeaderMap,
}

/// Response from a worker.
pub struct DispatchResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: ::http::HeaderMap,
}

/// Backend for dispatching requests to workers.
#[async_trait]
pub trait WorkerBackend: Send + Sync + std::fmt::Debug {
    /// Dispatch a request to a worker.
    async fn dispatch(&self, req: DispatchRequest) -> Result<DispatchResponse, GatewayError>;

    /// Check if the backend is healthy.
    async fn health_check(&self) -> Result<(), GatewayError>;
}

/// Registry for managing backend instances.
#[derive(Debug, Default)]
pub struct BackendRegistry {
    http_backends: dashmap::DashMap<String, Arc<dyn WorkerBackend>>,
}

impl BackendRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create an HTTP backend for the given URL.
    pub fn get_or_create_http(&self, url: &str) -> Arc<dyn WorkerBackend> {
        if let Some(backend) = self.http_backends.get(url) {
            return backend.clone();
        }

        let backend = Arc::new(HttpBackend::new(url.to_string()));
        self.http_backends.insert(url.to_string(), backend.clone());
        backend
    }

    /// Get a backend for the given address.
    pub fn get_backend(
        &self,
        address: &WorkerAddress,
    ) -> Result<Arc<dyn WorkerBackend>, GatewayError> {
        match address {
            WorkerAddress::Http { url } => Ok(self.get_or_create_http(url)),
            WorkerAddress::Vsock { .. } => Err(GatewayError::BackendError(
                "vsock backend not yet implemented".into(),
            )),
        }
    }
}
