//! vsock client for host-to-guest communication.
//!
//! Firecracker exposes vsock via a Unix domain socket on the host.
//! To connect to a guest port, the host connects to the UDS and sends
//! a `CONNECT <port>\n` command, then receives `OK <port>\n` on success.

use crate::error::{FirecrackerError, Result};
use crate::protocol::{GuestRequest, GuestResponse};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::debug;

/// Client for communicating with the guest runtime over vsock.
pub struct VsockClient {
    uds_path: PathBuf,
    port: u32,
}

impl VsockClient {
    /// Create a new vsock client.
    pub fn new(uds_path: PathBuf, port: u32) -> Self {
        Self { uds_path, port }
    }

    /// Connect to the guest vsock using Firecracker's CONNECT protocol.
    async fn connect(&self) -> Result<UnixStream> {
        debug!("Connecting to vsock UDS: {}", self.uds_path.display());

        let mut stream = UnixStream::connect(&self.uds_path)
            .await
            .map_err(|e| FirecrackerError::VsockConnectionFailed(format!(
                "Failed to connect to {}: {}",
                self.uds_path.display(), e
            )))?;

        let connect_cmd = format!("CONNECT {}\n", self.port);
        debug!("Sending CONNECT command for port {}", self.port);

        stream.write_all(connect_cmd.as_bytes()).await.map_err(|e| {
            FirecrackerError::VsockConnectionFailed(format!("Failed to send CONNECT: {}", e))
        })?;

        let mut reader = BufReader::new(&mut stream);
        let mut response = String::new();
        reader.read_line(&mut response).await.map_err(|e| {
            FirecrackerError::VsockConnectionFailed(format!("Failed to read CONNECT response: {}", e))
        })?;

        debug!("CONNECT response: {}", response.trim());

        if response.starts_with("OK ") {
            debug!("vsock connection established to port {}", self.port);
            Ok(stream)
        } else {
            Err(FirecrackerError::VsockConnectionFailed(format!(
                "vsock CONNECT failed: {}",
                response.trim()
            )))
        }
    }

    /// Send a request and receive a response.
    async fn request(&self, req: &GuestRequest) -> Result<GuestResponse> {
        let mut stream = self.connect().await?;

        let encoded = serde_json::to_vec(req)?;
        let len = encoded.len() as u32;

        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.map_err(|e| {
            FirecrackerError::VsockError(format!("Failed to read response length: {}", e))
        })?;
        let len = u32::from_be_bytes(len_buf) as usize;

        if len > 10 * 1024 * 1024 {
            return Err(FirecrackerError::ProtocolError(format!(
                "Response too large: {} bytes",
                len
            )));
        }

        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.map_err(|e| {
            FirecrackerError::VsockError(format!("Failed to read response body: {}", e))
        })?;

        serde_json::from_slice(&buf).map_err(|e| {
            FirecrackerError::ProtocolError(format!("Invalid response JSON: {}", e))
        })
    }

    /// Ping the guest to check if it's ready.
    pub async fn ping(&self) -> Result<()> {
        debug!("Sending ping to guest");
        let response = self.request(&GuestRequest::ping()).await?;

        match response {
            GuestResponse::Pong => Ok(()),
            other => Err(FirecrackerError::ProtocolError(format!(
                "Unexpected response to ping: {:?}",
                other
            ))),
        }
    }

    /// Invoke a function on the guest.
    pub async fn invoke(
        &self,
        function_name: &str,
        payload: Vec<u8>,
        trace_id: &str,
    ) -> Result<GuestResponse> {
        debug!(function = %function_name, trace_id = %trace_id, "Invoking function");
        self.request(&GuestRequest::invoke(function_name, payload, trace_id))
            .await
    }

    /// Request graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        debug!("Sending shutdown request to guest");
        let response = self.request(&GuestRequest::shutdown()).await?;

        match response {
            GuestResponse::ShutdownAck => Ok(()),
            other => Err(FirecrackerError::ProtocolError(format!(
                "Unexpected response to shutdown: {:?}",
                other
            ))),
        }
    }
}
