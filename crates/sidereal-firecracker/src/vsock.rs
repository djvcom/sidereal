//! vsock client for host-to-guest communication.
//!
//! Firecracker exposes vsock via a Unix domain socket on the host.
//! To connect to a guest port, the host connects to the UDS and sends
//! a `CONNECT <port>\n` command, then receives `OK <port>\n` on success.

use crate::error::{FirecrackerError, Result};
use sidereal_proto::codec::{Codec, FrameHeader, MessageType, FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};
use sidereal_proto::{ControlMessage, Envelope, FunctionMessage, InvokeRequest, InvokeResponse, ProtocolError};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::debug;

/// Client for communicating with the guest runtime over vsock.
pub struct VsockClient {
    uds_path: PathBuf,
    port: u32,
    codec: std::sync::Mutex<Codec>,
}

impl VsockClient {
    /// Create a new vsock client.
    pub fn new(uds_path: PathBuf, port: u32) -> Self {
        Self {
            uds_path,
            port,
            codec: std::sync::Mutex::new(Codec::with_capacity(8192)),
        }
    }

    /// Connect to the guest vsock using Firecracker's CONNECT protocol.
    async fn connect(&self) -> Result<UnixStream> {
        debug!("Connecting to vsock UDS: {}", self.uds_path.display());

        let mut stream = UnixStream::connect(&self.uds_path).await.map_err(|e| {
            FirecrackerError::VsockConnectionFailed(format!(
                "Failed to connect to {}: {}",
                self.uds_path.display(),
                e
            ))
        })?;

        let connect_cmd = format!("CONNECT {}\n", self.port);
        debug!("Sending CONNECT command for port {}", self.port);

        stream
            .write_all(connect_cmd.as_bytes())
            .await
            .map_err(|e| {
                FirecrackerError::VsockConnectionFailed(format!("Failed to send CONNECT: {}", e))
            })?;

        let mut reader = BufReader::new(&mut stream);
        let mut response = String::new();
        reader.read_line(&mut response).await.map_err(|e| {
            FirecrackerError::VsockConnectionFailed(format!(
                "Failed to read CONNECT response: {}",
                e
            ))
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

    /// Send a function message and receive a response.
    async fn function_request(&self, envelope: Envelope<FunctionMessage>) -> Result<Envelope<FunctionMessage>> {
        let mut stream = self.connect().await?;

        // Encode the envelope
        let bytes = {
            let mut codec = self.codec.lock().unwrap();
            codec.encode(&envelope, MessageType::Function)
                .map_err(|e: ProtocolError| FirecrackerError::ProtocolError(e.to_string()))?
                .to_vec()
        };

        // Write frame
        stream.write_all(&bytes).await?;
        stream.flush().await?;

        // Read response header
        let mut header_buf = [0u8; FRAME_HEADER_SIZE];
        stream.read_exact(&mut header_buf).await.map_err(|e| {
            FirecrackerError::VsockError(format!("Failed to read response header: {}", e))
        })?;

        let header = FrameHeader::decode(&header_buf)
            .map_err(|e: ProtocolError| FirecrackerError::ProtocolError(e.to_string()))?;

        if header.message_type != MessageType::Function {
            return Err(FirecrackerError::ProtocolError(format!(
                "Expected Function response, got {:?}",
                header.message_type
            )));
        }

        let len = header.payload_len as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(FirecrackerError::ProtocolError(format!(
                "Response too large: {} bytes",
                len
            )));
        }

        // Read payload
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.map_err(|e| {
            FirecrackerError::VsockError(format!("Failed to read response body: {}", e))
        })?;

        Codec::decode::<Envelope<FunctionMessage>>(&buf)
            .map_err(|e: ProtocolError| FirecrackerError::ProtocolError(e.to_string()))
    }

    /// Send a control message and receive a response.
    async fn control_request(&self, envelope: Envelope<ControlMessage>) -> Result<Envelope<ControlMessage>> {
        let mut stream = self.connect().await?;

        // Encode the envelope
        let bytes = {
            let mut codec = self.codec.lock().unwrap();
            codec.encode(&envelope, MessageType::Control)
                .map_err(|e: ProtocolError| FirecrackerError::ProtocolError(e.to_string()))?
                .to_vec()
        };

        // Write frame
        stream.write_all(&bytes).await?;
        stream.flush().await?;

        // Read response header
        let mut header_buf = [0u8; FRAME_HEADER_SIZE];
        stream.read_exact(&mut header_buf).await.map_err(|e| {
            FirecrackerError::VsockError(format!("Failed to read response header: {}", e))
        })?;

        let header = FrameHeader::decode(&header_buf)
            .map_err(|e: ProtocolError| FirecrackerError::ProtocolError(e.to_string()))?;

        if header.message_type != MessageType::Control {
            return Err(FirecrackerError::ProtocolError(format!(
                "Expected Control response, got {:?}",
                header.message_type
            )));
        }

        let len = header.payload_len as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(FirecrackerError::ProtocolError(format!(
                "Response too large: {} bytes",
                len
            )));
        }

        // Read payload
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.map_err(|e| {
            FirecrackerError::VsockError(format!("Failed to read response body: {}", e))
        })?;

        Codec::decode::<Envelope<ControlMessage>>(&buf)
            .map_err(|e: ProtocolError| FirecrackerError::ProtocolError(e.to_string()))
    }

    /// Ping the guest to check if it's ready.
    pub async fn ping(&self) -> Result<()> {
        debug!("Sending ping to guest");
        let envelope = Envelope::new(ControlMessage::Ping);
        let response = self.control_request(envelope).await?;

        match response.payload {
            ControlMessage::Pong => Ok(()),
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
        metadata: Vec<(String, String)>,
    ) -> Result<InvokeResponse> {
        debug!(function = %function_name, "Invoking function");

        let request = InvokeRequest::new(function_name, payload);
        let mut envelope = Envelope::new(FunctionMessage::Invoke(request));
        envelope.header.metadata = metadata;

        let response = self.function_request(envelope).await?;

        match response.payload {
            FunctionMessage::Response(resp) => Ok(resp),
            FunctionMessage::Invoke(_) => Err(FirecrackerError::ProtocolError(
                "Got Invoke instead of Response".into(),
            )),
        }
    }

    /// Request graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        debug!("Sending shutdown request to guest");
        let envelope = Envelope::new(ControlMessage::Shutdown);
        let response = self.control_request(envelope).await?;

        match response.payload {
            ControlMessage::ShutdownAck => Ok(()),
            other => Err(FirecrackerError::ProtocolError(format!(
                "Unexpected response to shutdown: {:?}",
                other
            ))),
        }
    }
}
