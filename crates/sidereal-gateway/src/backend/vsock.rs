//! vsock backend for proxying requests to Firecracker VMs.

use async_trait::async_trait;
use sidereal_firecracker::{FirecrackerError, GuestResponse, VsockClient};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::timeout;

use super::{DispatchRequest, DispatchResponse, WorkerBackend};
use crate::error::GatewayError;

/// Backend that proxies requests to a Firecracker VM via vsock.
pub struct VsockBackend {
    uds_path: PathBuf,
    port: u32,
    timeout: Duration,
}

impl std::fmt::Debug for VsockBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VsockBackend")
            .field("uds_path", &self.uds_path)
            .field("port", &self.port)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl VsockBackend {
    /// Create a new vsock backend with the given UDS path and port.
    pub fn new(uds_path: PathBuf, port: u32) -> Self {
        Self {
            uds_path,
            port,
            timeout: Duration::from_secs(30),
        }
    }

    /// Set the request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn client(&self) -> VsockClient {
        VsockClient::new(self.uds_path.clone(), self.port)
    }
}

#[async_trait]
impl WorkerBackend for VsockBackend {
    async fn dispatch(&self, req: DispatchRequest) -> Result<DispatchResponse, GatewayError> {
        let client = self.client();
        let response = timeout(
            self.timeout,
            client.invoke(&req.function_name, req.payload, &req.trace_id),
        )
        .await
        .map_err(|_| GatewayError::Timeout)?
        .map_err(firecracker_to_gateway_error)?;

        match response {
            GuestResponse::Result {
                status,
                body,
                trace_id: _,
            } => Ok(DispatchResponse {
                status,
                body,
                headers: http::HeaderMap::new(),
            }),
            GuestResponse::Error { message, .. } => Err(GatewayError::BackendError(format!(
                "Guest error: {}",
                message
            ))),
            other => Err(GatewayError::BackendError(format!(
                "Unexpected response type: {:?}",
                other
            ))),
        }
    }

    async fn health_check(&self) -> Result<(), GatewayError> {
        let client = self.client();
        timeout(Duration::from_secs(5), client.ping())
            .await
            .map_err(|_| GatewayError::Timeout)?
            .map_err(firecracker_to_gateway_error)
    }
}

fn firecracker_to_gateway_error(err: FirecrackerError) -> GatewayError {
    match err {
        FirecrackerError::VsockConnectionFailed(msg) => GatewayError::ConnectionFailed(msg),
        FirecrackerError::VsockError(msg) => GatewayError::VsockError(msg),
        FirecrackerError::ProtocolError(msg) => GatewayError::BackendError(msg),
        other => GatewayError::BackendError(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_backend_with_defaults() {
        let backend = VsockBackend::new("/tmp/test.sock".into(), 1024);
        assert_eq!(backend.timeout, Duration::from_secs(30));
    }

    #[test]
    fn creates_backend_with_custom_timeout() {
        let backend =
            VsockBackend::new("/tmp/test.sock".into(), 1024).with_timeout(Duration::from_secs(60));
        assert_eq!(backend.timeout, Duration::from_secs(60));
    }
}
