//! HTTP proxy for cargo dependency fetching.
//!
//! This module provides a domain-allowlisted HTTP proxy that runs on the host
//! and accepts connections from builder VMs via vsock. This approach avoids
//! giving VMs direct network access, eliminating entire classes of attacks
//! (DNS rebinding, IPv6 bypass, etc.).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────┐     vsock      ┌─────────────────────┐
//! │     Builder VM      │ ─────────────▶ │    CargoProxy       │
//! │  cargo fetch        │                │  (host)             │
//! │  HTTP_PROXY=vsock   │                │                     │
//! └─────────────────────┘                │  ┌───────────────┐  │
//!                                        │  │  Allowlist    │  │
//!                                        │  │  - crates.io  │  │
//!                                        │  │  - github.com │  │
//!                                        │  └───────────────┘  │
//!                                        │         │           │
//!                                        │         ▼           │
//!                                        │  ┌───────────────┐  │
//!                                        │  │  HTTP Client  │──┼──▶ Internet
//!                                        │  └───────────────┘  │
//!                                        └─────────────────────┘
//! ```

use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::Arc;

use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Default allowed domains for cargo operations.
pub const DEFAULT_ALLOWED_DOMAINS: &[&str] = &[
    // Crates.io registry
    "crates.io",
    "static.crates.io",
    "index.crates.io",
    // GitHub (for git dependencies)
    "github.com",
    "raw.githubusercontent.com",
    "codeload.github.com",
    // GitLab (for git dependencies)
    "gitlab.com",
    // Common CDNs used by crates.io
    "cloudfront.net",
    "fastly.net",
];

/// Proxy port on vsock.
pub const PROXY_PORT: u32 = 1080;

/// Errors that can occur during proxy operations.
#[derive(Debug, Error)]
pub enum ProxyError {
    /// Domain is not in the allowlist.
    #[error("domain not allowed: {0}")]
    DomainBlocked(String),

    /// Invalid request format.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Connection to upstream failed.
    #[error("upstream connection failed: {0}")]
    UpstreamConnection(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Result type for proxy operations.
pub type ProxyResult<T> = Result<T, ProxyError>;

/// Configuration for the cargo proxy.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// Allowed domain suffixes.
    allowed_domains: HashSet<String>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            allowed_domains: DEFAULT_ALLOWED_DOMAINS
                .iter()
                .map(|&s| s.to_owned())
                .collect(),
        }
    }
}

impl ProxyConfig {
    /// Create a new proxy configuration with custom allowed domains.
    pub fn with_domains(domains: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            allowed_domains: domains.into_iter().map(Into::into).collect(),
        }
    }

    /// Check if a domain is allowed.
    ///
    /// Returns true if the domain exactly matches or is a subdomain of
    /// any allowed domain.
    pub fn is_allowed(&self, domain: &str) -> bool {
        let domain = domain.to_lowercase();

        for allowed in &self.allowed_domains {
            if domain == *allowed || domain.ends_with(&format!(".{allowed}")) {
                return true;
            }
        }

        false
    }

    /// Add a domain to the allowlist.
    pub fn allow_domain(&mut self, domain: impl Into<String>) {
        self.allowed_domains.insert(domain.into());
    }
}

/// HTTP proxy for cargo dependency fetching.
///
/// Handles HTTP CONNECT requests (for HTTPS tunneling) and validates
/// that the target domain is in the allowlist.
pub struct CargoProxy {
    config: Arc<ProxyConfig>,
}

impl CargoProxy {
    /// Create a new cargo proxy with the given configuration.
    pub fn new(config: ProxyConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Create a new cargo proxy with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(ProxyConfig::default())
    }

    /// Handle a single proxy connection.
    ///
    /// This method reads the HTTP CONNECT request, validates the domain,
    /// establishes an upstream connection, and then relays data bidirectionally.
    pub async fn handle_connection<S>(&self, mut stream: S) -> ProxyResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        // Read the HTTP request line
        let mut reader = BufReader::new(&mut stream);
        let mut request_line = String::new();
        reader.read_line(&mut request_line).await?;

        let request_line = request_line.trim();
        debug!(request = %request_line, "Received proxy request");

        // Parse CONNECT request: "CONNECT host:port HTTP/1.1"
        let (host, port) = parse_connect_request(request_line)?;

        // Extract domain (strip port if present)
        let domain = host.split(':').next().unwrap_or(&host);

        // Validate domain against allowlist
        if !self.config.is_allowed(domain) {
            warn!(domain = %domain, "Blocked request to non-allowed domain");

            // Send 403 Forbidden response
            let response = "HTTP/1.1 403 Forbidden\r\n\r\nDomain not in allowlist\r\n";
            stream.write_all(response.as_bytes()).await?;

            return Err(ProxyError::DomainBlocked(domain.to_owned()));
        }

        // Consume remaining headers (until empty line)
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).await?;
            if header.trim().is_empty() {
                break;
            }
        }

        // Connect to upstream
        let upstream_addr = format!("{host}:{port}");
        let upstream = tokio::net::TcpStream::connect(&upstream_addr)
            .await
            .map_err(|e| ProxyError::UpstreamConnection(format!("{upstream_addr}: {e}")))?;

        info!(domain = %domain, port = %port, "Connected to upstream");

        // Send 200 Connection Established
        let response = "HTTP/1.1 200 Connection Established\r\n\r\n";

        // Get the inner stream back from the BufReader
        let stream = reader.into_inner();
        stream.write_all(response.as_bytes()).await?;

        // Relay data bidirectionally
        let (mut client_read, mut client_write) = tokio::io::split(stream);
        let (mut upstream_read, mut upstream_write) = tokio::io::split(upstream);

        let client_to_upstream = tokio::io::copy(&mut client_read, &mut upstream_write);
        let upstream_to_client = tokio::io::copy(&mut upstream_read, &mut client_write);

        // Wait for either direction to complete
        tokio::select! {
            result = client_to_upstream => {
                if let Err(e) = result {
                    debug!(error = %e, "Client to upstream copy ended");
                }
            }
            result = upstream_to_client => {
                if let Err(e) = result {
                    debug!(error = %e, "Upstream to client copy ended");
                }
            }
        }

        debug!(domain = %domain, "Connection closed");
        Ok(())
    }

    /// Get the proxy configuration.
    pub fn config(&self) -> &ProxyConfig {
        &self.config
    }
}

/// Server that accepts proxy connections over vsock (via Unix socket).
///
/// Firecracker exposes vsock as a Unix socket on the host. The server
/// listens on this socket and handles incoming CONNECT requests.
pub struct ProxyServer {
    proxy: Arc<CargoProxy>,
}

impl ProxyServer {
    /// Create a new proxy server with the given proxy.
    pub fn new(proxy: CargoProxy) -> Self {
        Self {
            proxy: Arc::new(proxy),
        }
    }

    /// Create a new proxy server with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(CargoProxy::with_defaults())
    }

    /// Run the proxy server, listening on the given vsock Unix socket path.
    ///
    /// The vsock path format for Firecracker is: `<vsock_uds_path>_<port>`
    /// For example: `/tmp/firecracker/vsock.sock_1080`
    ///
    /// This method runs until the cancellation token is triggered.
    pub async fn run(&self, vsock_path: &Path, cancel: CancellationToken) -> ProxyResult<()> {
        // Remove existing socket if present
        if vsock_path.exists() {
            tokio::fs::remove_file(vsock_path).await?;
        }

        // Create parent directory if needed
        if let Some(parent) = vsock_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let listener = UnixListener::bind(vsock_path)?;
        info!(path = %vsock_path.display(), "Proxy server listening");

        loop {
            tokio::select! {
                biased;

                () = cancel.cancelled() => {
                    info!("Proxy server shutting down");
                    break;
                }

                result = listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            let proxy = Arc::clone(&self.proxy);
                            tokio::spawn(async move {
                                if let Err(e) = proxy.handle_connection(stream).await {
                                    match &e {
                                        ProxyError::DomainBlocked(domain) => {
                                            debug!(domain = %domain, "Blocked proxy request");
                                        }
                                        _ => {
                                            error!(error = %e, "Proxy connection error");
                                        }
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to accept connection");
                        }
                    }
                }
            }
        }

        // Clean up socket file
        let _ = tokio::fs::remove_file(vsock_path).await;

        Ok(())
    }

    /// Run the proxy server on a TCP socket (for testing without vsock).
    ///
    /// This is useful for integration testing where vsock isn't available.
    pub async fn run_tcp(
        &self,
        addr: std::net::SocketAddr,
        cancel: CancellationToken,
    ) -> ProxyResult<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        info!(addr = %addr, "Proxy server listening (TCP mode)");

        loop {
            tokio::select! {
                biased;

                () = cancel.cancelled() => {
                    info!("Proxy server shutting down");
                    break;
                }

                result = listener.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            debug!(peer = %peer, "Accepted TCP connection");
                            let proxy = Arc::clone(&self.proxy);
                            tokio::spawn(async move {
                                if let Err(e) = proxy.handle_connection(stream).await {
                                    match &e {
                                        ProxyError::DomainBlocked(domain) => {
                                            debug!(domain = %domain, "Blocked proxy request");
                                        }
                                        _ => {
                                            error!(error = %e, "Proxy connection error");
                                        }
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to accept TCP connection");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Parse an HTTP CONNECT request line.
///
/// Format: "CONNECT host:port HTTP/1.x"
fn parse_connect_request(line: &str) -> ProxyResult<(String, u16)> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() < 3 {
        return Err(ProxyError::InvalidRequest(format!(
            "malformed request line: {line}"
        )));
    }

    if parts[0] != "CONNECT" {
        return Err(ProxyError::InvalidRequest(format!(
            "expected CONNECT method, got: {}",
            parts[0]
        )));
    }

    let host_port = parts[1];
    let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
        let host = &host_port[..colon_pos];
        let port_str = &host_port[colon_pos + 1..];
        let port = port_str
            .parse::<u16>()
            .map_err(|_| ProxyError::InvalidRequest(format!("invalid port number: {port_str}")))?;
        (host.to_owned(), port)
    } else {
        // Default to HTTPS port
        (host_port.to_owned(), 443)
    };

    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn test_default_allowed_domains() {
        let config = ProxyConfig::default();

        // Exact matches
        assert!(config.is_allowed("crates.io"));
        assert!(config.is_allowed("github.com"));
        assert!(config.is_allowed("static.crates.io"));

        // Subdomains
        assert!(config.is_allowed("index.crates.io"));
        assert!(config.is_allowed("api.github.com"));
        assert!(config.is_allowed("raw.githubusercontent.com"));

        // Not allowed
        assert!(!config.is_allowed("evil.com"));
        assert!(!config.is_allowed("crates.io.evil.com"));
        assert!(!config.is_allowed("notcrates.io"));
    }

    #[test]
    fn test_custom_allowed_domains() {
        let config = ProxyConfig::with_domains(["example.com", "test.org"]);

        assert!(config.is_allowed("example.com"));
        assert!(config.is_allowed("sub.example.com"));
        assert!(config.is_allowed("test.org"));

        assert!(!config.is_allowed("crates.io"));
        assert!(!config.is_allowed("github.com"));
    }

    #[test]
    fn test_case_insensitive() {
        let config = ProxyConfig::default();

        assert!(config.is_allowed("CRATES.IO"));
        assert!(config.is_allowed("GitHub.com"));
        assert!(config.is_allowed("Static.Crates.IO"));
    }

    #[test]
    fn test_parse_connect_request() {
        // Standard CONNECT request
        let (host, port) = parse_connect_request("CONNECT crates.io:443 HTTP/1.1").unwrap();
        assert_eq!(host, "crates.io");
        assert_eq!(port, 443);

        // Non-standard port
        let (host, port) = parse_connect_request("CONNECT example.com:8080 HTTP/1.1").unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 8080);

        // IPv6 address
        let (host, port) = parse_connect_request("CONNECT [::1]:443 HTTP/1.1").unwrap();
        assert_eq!(host, "[::1]");
        assert_eq!(port, 443);
    }

    #[test]
    fn test_parse_connect_request_errors() {
        // Wrong method
        assert!(parse_connect_request("GET / HTTP/1.1").is_err());

        // Missing parts
        assert!(parse_connect_request("CONNECT").is_err());

        // Invalid port
        assert!(parse_connect_request("CONNECT example.com:abc HTTP/1.1").is_err());
    }

    #[tokio::test]
    async fn test_blocked_domain() {
        use tokio::io::duplex;

        let proxy = CargoProxy::with_defaults();
        let (mut client, server) = duplex(1024);

        // Send CONNECT request to blocked domain
        let request = "CONNECT evil.com:443 HTTP/1.1\r\nHost: evil.com\r\n\r\n";
        client.write_all(request.as_bytes()).await.unwrap();

        // Handle connection (should fail)
        let result = proxy.handle_connection(server).await;
        assert!(matches!(result, Err(ProxyError::DomainBlocked(_))));

        // Read response
        let mut response = vec![0u8; 1024];
        let n = client.read(&mut response).await.unwrap();
        let response = String::from_utf8_lossy(&response[..n]);

        assert!(response.contains("403 Forbidden"));
        assert!(response.contains("not in allowlist"));
    }

    #[tokio::test]
    async fn test_allowed_domain_connects() {
        use tokio::io::duplex;
        use tokio::net::TcpListener;

        // Start a mock upstream server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Use a config that allows localhost for testing
        let config = ProxyConfig::with_domains(["127.0.0.1"]);
        let proxy = CargoProxy::new(config);

        let (mut client, server) = duplex(4096);

        // Spawn upstream acceptor
        let upstream_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Echo back whatever we receive
            let mut buf = [0u8; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            socket.write_all(&buf[..n]).await.unwrap();
        });

        // Send CONNECT request
        let request = format!(
            "CONNECT 127.0.0.1:{} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            addr.port()
        );

        // Run proxy handler in background
        let proxy_handle = tokio::spawn(async move { proxy.handle_connection(server).await });

        // Write request
        client.write_all(request.as_bytes()).await.unwrap();

        // Read 200 response
        let mut response = vec![0u8; 1024];
        let n = client.read(&mut response).await.unwrap();
        let response_str = String::from_utf8_lossy(&response[..n]);
        assert!(
            response_str.contains("200 Connection Established"),
            "Expected 200, got: {response_str}"
        );

        // Send data through tunnel
        client.write_all(b"hello").await.unwrap();

        // Read echoed data
        let mut echo = vec![0u8; 1024];
        let n = client.read(&mut echo).await.unwrap();
        assert_eq!(&echo[..n], b"hello");

        // Cleanup
        drop(client);
        let _ = proxy_handle.await;
        let _ = upstream_handle.await;
    }

    #[test]
    fn test_add_domain_to_allowlist() {
        let mut config = ProxyConfig::default();

        assert!(!config.is_allowed("custom.example.com"));

        config.allow_domain("example.com");

        assert!(config.is_allowed("example.com"));
        assert!(config.is_allowed("custom.example.com"));
    }

    #[tokio::test]
    async fn test_proxy_server_tcp() {
        use std::time::Duration;
        use tokio::net::TcpListener;

        // Start mock upstream server
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();

        // Configure proxy to allow localhost
        let config = ProxyConfig::with_domains(["127.0.0.1"]);
        let server = ProxyServer::new(CargoProxy::new(config));

        // Find free port for proxy
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        drop(proxy_listener);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Spawn proxy server
        let server_handle =
            tokio::spawn(async move { server.run_tcp(proxy_addr, cancel_clone).await });

        // Spawn upstream echo server
        let upstream_handle = tokio::spawn(async move {
            if let Ok((mut socket, _)) = upstream.accept().await {
                let mut buf = [0u8; 1024];
                if let Ok(n) = socket.read(&mut buf).await {
                    let _ = socket.write_all(&buf[..n]).await;
                }
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect to proxy
        let mut client = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();

        // Send CONNECT request
        let connect_req = format!(
            "CONNECT 127.0.0.1:{} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            upstream_addr.port()
        );
        client.write_all(connect_req.as_bytes()).await.unwrap();

        // Read response
        let mut response = vec![0u8; 1024];
        let n = client.read(&mut response).await.unwrap();
        let response_str = String::from_utf8_lossy(&response[..n]);
        assert!(
            response_str.contains("200 Connection Established"),
            "Expected 200, got: {response_str}"
        );

        // Send data through tunnel
        client.write_all(b"test data").await.unwrap();

        // Read echoed data
        let mut echo = vec![0u8; 1024];
        let n = client.read(&mut echo).await.unwrap();
        assert_eq!(&echo[..n], b"test data");

        // Cleanup
        cancel.cancel();
        let _ = server_handle.await;
        let _ = upstream_handle.await;
    }

    #[tokio::test]
    async fn test_proxy_server_blocks_disallowed() {
        use std::time::Duration;
        use tokio::net::TcpListener;

        // Configure proxy with only crates.io allowed
        let server = ProxyServer::with_defaults();

        // Find free port for proxy
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        drop(proxy_listener);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Spawn proxy server
        let server_handle =
            tokio::spawn(async move { server.run_tcp(proxy_addr, cancel_clone).await });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect to proxy
        let mut client = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();

        // Send CONNECT request to blocked domain
        let connect_req = "CONNECT evil.com:443 HTTP/1.1\r\nHost: evil.com\r\n\r\n";
        client.write_all(connect_req.as_bytes()).await.unwrap();

        // Read response - should be 403
        let mut response = vec![0u8; 1024];
        let n = client.read(&mut response).await.unwrap();
        let response_str = String::from_utf8_lossy(&response[..n]);
        assert!(
            response_str.contains("403 Forbidden"),
            "Expected 403, got: {response_str}"
        );

        // Cleanup
        cancel.cancel();
        let _ = server_handle.await;
    }
}
