//! HTTP backend for proxying requests to dev servers.

use async_trait::async_trait;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::client::conn::http1;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use std::time::Duration;
use tokio::net::TcpStream;

use super::{DispatchRequest, DispatchResponse, WorkerBackend};
use crate::error::GatewayError;

/// HTTP backend that proxies requests to an HTTP server.
#[derive(Debug)]
#[must_use]
pub struct HttpBackend {
    base_url: String,
    timeout: Duration,
}

impl HttpBackend {
    /// Create a new HTTP backend with the given base URL.
    pub const fn new(base_url: String) -> Self {
        Self {
            base_url,
            timeout: Duration::from_secs(30),
        }
    }

    /// Set the request timeout.
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn parse_host_port(&self) -> Result<(String, u16), GatewayError> {
        let uri: hyper::Uri = self
            .base_url
            .parse()
            .map_err(|e| GatewayError::InvalidBackendUrl(format!("{e}")))?;

        let host = uri
            .host()
            .ok_or_else(|| GatewayError::InvalidBackendUrl("missing host".into()))?
            .to_owned();

        let port = uri.port_u16().unwrap_or_else(|| match uri.scheme_str() {
            Some("https") => 443,
            _ => 80,
        });

        Ok((host, port))
    }
}

#[async_trait]
impl WorkerBackend for HttpBackend {
    async fn dispatch(&self, req: DispatchRequest) -> Result<DispatchResponse, GatewayError> {
        let url = format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),
            req.function_name
        );

        // Build the request
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(&url)
            .header("content-type", "application/json");

        // Propagate trace headers
        if let Some(traceparent) = req.headers.get("traceparent") {
            builder = builder.header("traceparent", traceparent);
        }
        if let Some(tracestate) = req.headers.get("tracestate") {
            builder = builder.header("tracestate", tracestate);
        }

        // Add request ID
        builder = builder.header("x-request-id", &req.trace_id);

        let request = builder
            .body(Full::new(Bytes::from(req.payload)))
            .map_err(|e| GatewayError::RequestBuildFailed(e.to_string()))?;

        // Connect to backend
        let (host, port) = self.parse_host_port()?;
        let addr = format!("{host}:{port}");

        let stream = tokio::time::timeout(self.timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| GatewayError::Timeout)?
            .map_err(|e| GatewayError::ConnectionFailed(e.to_string()))?;

        let io = TokioIo::new(stream);
        let (mut sender, conn) = http1::handshake(io)
            .await
            .map_err(|e| GatewayError::ConnectionFailed(e.to_string()))?;

        // Spawn connection driver
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!(error = %e, "HTTP connection error");
            }
        });

        // Send request with timeout
        let response = tokio::time::timeout(self.timeout, sender.send_request(request))
            .await
            .map_err(|_| GatewayError::Timeout)?
            .map_err(|e| GatewayError::BackendError(e.to_string()))?;

        let status = response.status().as_u16();
        let headers = response.headers().clone();

        let body = response
            .collect()
            .await
            .map_err(|e| GatewayError::BackendError(e.to_string()))?
            .to_bytes()
            .to_vec();

        Ok(DispatchResponse {
            status,
            body,
            headers,
        })
    }

    async fn health_check(&self) -> Result<(), GatewayError> {
        let (host, port) = self.parse_host_port()?;
        let addr = format!("{host}:{port}");

        tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&addr))
            .await
            .map_err(|_| GatewayError::Timeout)?
            .map_err(|e| GatewayError::ConnectionFailed(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_host_port_http() {
        let backend = HttpBackend::new("http://localhost:7850".into());
        let (host, port) = backend.parse_host_port().unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 7850);
    }

    #[test]
    fn parse_host_port_default() {
        let backend = HttpBackend::new("http://example.com".into());
        let (host, port) = backend.parse_host_port().unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 80);
    }

    #[test]
    fn parse_host_port_https() {
        let backend = HttpBackend::new("https://example.com".into());
        let (host, port) = backend.parse_host_port().unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 443);
    }
}
