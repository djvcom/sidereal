//! Host-side registration listener for worker function discovery.
//!
//! When a worker VM starts, it sends a registration message containing its
//! discovered functions. This module provides utilities to wait for and
//! process that registration on the host side.

use crate::error::{FirecrackerError, Result};
use sidereal_proto::codec::{Codec, FrameHeader, MessageType, FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};
use sidereal_proto::{Envelope, RegisterResponse, SchedulerMessage};
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::{debug, info};

const SCHEDULER_PORT: u32 = sidereal_proto::ports::SCHEDULER;

/// Information received from a worker's registration request.
#[derive(Debug, Clone)]
pub struct RegistrationInfo {
    /// Worker identifier from the request.
    pub worker_id: String,
    /// Functions the worker can handle.
    pub functions: Vec<String>,
    /// vsock CID if provided.
    pub vsock_cid: Option<u32>,
}

/// Wait for a worker to send its registration message over vsock.
///
/// This function:
/// 1. Connects to the vsock UDS for the SCHEDULER port
/// 2. Waits for an incoming CONNECT request from the guest
/// 3. Reads the registration message
/// 4. Sends a successful acknowledgement
/// 5. Returns the discovered functions
///
/// # Arguments
///
/// * `vsock_uds_path` - Path to the vsock Unix domain socket
/// * `timeout` - Maximum time to wait for registration
///
/// # Errors
///
/// Returns an error if:
/// - Failed to connect to the vsock UDS
/// - Timeout waiting for registration
/// - Protocol error during communication
pub async fn wait_for_registration(
    vsock_uds_path: &Path,
    timeout: Duration,
) -> Result<RegistrationInfo> {
    debug!(
        path = %vsock_uds_path.display(),
        timeout_secs = timeout.as_secs(),
        "waiting for worker registration"
    );

    let listener_path = format!("{}_{}", vsock_uds_path.display(), SCHEDULER_PORT);

    let listener = tokio::net::UnixListener::bind(&listener_path).map_err(|e| {
        FirecrackerError::VsockConnectionFailed(format!(
            "failed to bind listener at {listener_path}: {e}"
        ))
    })?;

    let cleanup_path = listener_path.clone();
    let _guard = scopeguard::guard((), |_| {
        let _ = std::fs::remove_file(&cleanup_path);
    });

    let (stream, _addr) = tokio::time::timeout(timeout, listener.accept())
        .await
        .map_err(|_| FirecrackerError::VmNotReady {
            timeout_secs: timeout.as_secs(),
            last_error: "timeout waiting for registration".to_owned(),
            console_log: String::new(),
        })?
        .map_err(|e| {
            FirecrackerError::VsockConnectionFailed(format!("failed to accept connection: {e}"))
        })?;

    debug!("accepted connection for registration");

    receive_registration(stream, timeout).await
}

/// Wait for registration using Firecracker's vsock CONNECT protocol.
///
/// Firecracker exposes vsock via a Unix domain socket. When the guest
/// connects to a port, Firecracker initiates a connection to the host
/// listener at `{vsock_uds_path}_{port}`.
///
/// # Arguments
///
/// * `vsock_uds_path` - Path to the Firecracker vsock UDS
/// * `timeout` - Maximum time to wait for registration
pub async fn wait_for_registration_vsock(
    vsock_uds_path: &Path,
    timeout: Duration,
) -> Result<RegistrationInfo> {
    debug!(
        path = %vsock_uds_path.display(),
        port = SCHEDULER_PORT,
        timeout_secs = timeout.as_secs(),
        "waiting for worker registration via vsock"
    );

    let mut stream = tokio::time::timeout(timeout, UnixStream::connect(vsock_uds_path))
        .await
        .map_err(|_| FirecrackerError::VmNotReady {
            timeout_secs: timeout.as_secs(),
            last_error: "timeout connecting to vsock".to_owned(),
            console_log: String::new(),
        })?
        .map_err(|e| {
            FirecrackerError::VsockConnectionFailed(format!("failed to connect to vsock UDS: {e}"))
        })?;

    let connect_cmd = format!("CONNECT {SCHEDULER_PORT}\n");
    stream
        .write_all(connect_cmd.as_bytes())
        .await
        .map_err(|e| {
            FirecrackerError::VsockConnectionFailed(format!("failed to send CONNECT: {e}"))
        })?;

    let mut reader = BufReader::new(&mut stream);
    let mut response = String::new();

    tokio::time::timeout(timeout, reader.read_line(&mut response))
        .await
        .map_err(|_| FirecrackerError::VmNotReady {
            timeout_secs: timeout.as_secs(),
            last_error: "timeout waiting for CONNECT response".to_owned(),
            console_log: String::new(),
        })?
        .map_err(|e| {
            FirecrackerError::VsockConnectionFailed(format!("failed to read CONNECT response: {e}"))
        })?;

    if !response.starts_with("OK ") {
        return Err(FirecrackerError::VsockConnectionFailed(format!(
            "vsock CONNECT failed: {}",
            response.trim()
        )));
    }

    debug!("vsock connection established, waiting for registration message");

    receive_registration(stream, timeout).await
}

async fn receive_registration(
    mut stream: UnixStream,
    timeout: Duration,
) -> Result<RegistrationInfo> {
    let mut header_buf = [0u8; FRAME_HEADER_SIZE];

    tokio::time::timeout(timeout, stream.read_exact(&mut header_buf))
        .await
        .map_err(|_| FirecrackerError::VmNotReady {
            timeout_secs: timeout.as_secs(),
            last_error: "timeout reading registration header".to_owned(),
            console_log: String::new(),
        })?
        .map_err(|e| FirecrackerError::ProtocolError(format!("failed to read header: {e}")))?;

    let header = FrameHeader::decode(&header_buf)
        .map_err(|e| FirecrackerError::ProtocolError(e.to_string()))?;

    if header.message_type != MessageType::Scheduler {
        return Err(FirecrackerError::ProtocolError(format!(
            "expected Scheduler message, got {:?}",
            header.message_type
        )));
    }

    #[allow(clippy::as_conversions)]
    let len = header.payload_len as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(FirecrackerError::ProtocolError(format!(
            "message too large: {len} bytes"
        )));
    }

    let mut payload_buf = vec![0u8; len];
    tokio::time::timeout(timeout, stream.read_exact(&mut payload_buf))
        .await
        .map_err(|_| FirecrackerError::VmNotReady {
            timeout_secs: timeout.as_secs(),
            last_error: "timeout reading registration payload".to_owned(),
            console_log: String::new(),
        })?
        .map_err(|e| FirecrackerError::ProtocolError(format!("failed to read payload: {e}")))?;

    let envelope: Envelope<SchedulerMessage> =
        Codec::decode(&payload_buf).map_err(|e| FirecrackerError::ProtocolError(e.to_string()))?;

    let request = match envelope.payload {
        SchedulerMessage::Register(req) => req,
        other => {
            return Err(FirecrackerError::ProtocolError(format!(
                "expected Register message, got {other:?}"
            )));
        }
    };

    info!(
        worker_id = %request.worker_id,
        functions = ?request.functions,
        "received registration from worker"
    );

    let response = RegisterResponse::success(30);
    let response_envelope =
        Envelope::response_to(&envelope.header, SchedulerMessage::RegisterAck(response));

    let mut codec = Codec::with_capacity(1024);
    let bytes = codec
        .encode(&response_envelope, MessageType::Scheduler)
        .map_err(|e| FirecrackerError::ProtocolError(e.to_string()))?;

    stream
        .write_all(bytes)
        .await
        .map_err(|e| FirecrackerError::ProtocolError(format!("failed to send response: {e}")))?;

    stream
        .flush()
        .await
        .map_err(|e| FirecrackerError::ProtocolError(format!("failed to flush response: {e}")))?;

    debug!("sent registration acknowledgement");

    Ok(RegistrationInfo {
        worker_id: request.worker_id,
        functions: request.functions,
        vsock_cid: request.vsock_cid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_info_fields() {
        let info = RegistrationInfo {
            worker_id: "test-worker".to_owned(),
            functions: vec!["greet".to_owned(), "farewell".to_owned()],
            vsock_cid: Some(3),
        };

        assert_eq!(info.worker_id, "test-worker");
        assert_eq!(info.functions.len(), 2);
        assert_eq!(info.vsock_cid, Some(3));
    }
}
