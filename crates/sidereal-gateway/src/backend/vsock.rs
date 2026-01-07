//! vsock backend for proxying requests to Firecracker VMs.

use async_trait::async_trait;
use sidereal_firecracker::{FirecrackerError, VsockClient};
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

        // Build metadata from headers and trace ID
        let mut metadata = Vec::new();
        if !req.trace_id.is_empty() {
            metadata.push(("x-trace-id".to_string(), req.trace_id.clone()));
        }

        let response = timeout(
            self.timeout,
            client.invoke(&req.function_name, req.payload, metadata),
        )
        .await
        .map_err(|_| GatewayError::Timeout)?
        .map_err(firecracker_to_gateway_error)?;

        // Check if the response indicates an error
        if response.status >= 400 {
            // Still return as a response, let the gateway handle error status codes
        }

        Ok(DispatchResponse {
            status: response.status,
            body: response.body,
            headers: convert_headers(&response.headers),
        })
    }

    async fn health_check(&self) -> Result<(), GatewayError> {
        let client = self.client();
        timeout(Duration::from_secs(5), client.ping())
            .await
            .map_err(|_| GatewayError::Timeout)?
            .map_err(firecracker_to_gateway_error)
    }
}

fn convert_headers(headers: &[(String, Vec<u8>)]) -> http::HeaderMap {
    let mut map = http::HeaderMap::new();
    for (name, value) in headers {
        if let (Ok(header_name), Ok(header_value)) = (
            http::header::HeaderName::from_bytes(name.as_bytes()),
            http::header::HeaderValue::from_bytes(value),
        ) {
            map.insert(header_name, header_value);
        }
    }
    map
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

    #[test]
    fn converts_headers() {
        let headers = vec![
            ("content-type".to_string(), b"application/json".to_vec()),
            ("x-custom".to_string(), b"value".to_vec()),
        ];
        let result = convert_headers(&headers);
        assert_eq!(result.get("content-type").unwrap(), "application/json");
        assert_eq!(result.get("x-custom").unwrap(), "value");
    }
}
