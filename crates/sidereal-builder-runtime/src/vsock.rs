//! Vsock communication for the builder runtime.
//!
//! Listens on vsock port 1028 for build requests from the host and
//! streams build output back.

use rkyv::api::high::HighSerializer;
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VsockAddr, VsockListener, VsockStream, VMADDR_CID_ANY};
use tracing::info;

use crate::protocol::{BuildMessage, BuildOutput, BuildRequest, BuildResult, BUILD_PORT};

/// Maximum message size (16 MB should be plenty for build output).
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Error type for vsock operations.
#[derive(Debug, thiserror::Error)]
pub enum VsockError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialisation error: {0}")]
    Serialisation(String),

    #[error("deserialisation error: {0}")]
    Deserialisation(String),

    #[error("message too large: {size} bytes (max {MAX_MESSAGE_SIZE})")]
    MessageTooLarge { size: usize },

    #[error("unexpected message type")]
    UnexpectedMessage,

    #[error("connection closed")]
    ConnectionClosed,
}

/// Wait for a single build request from the host.
///
/// The builder VM is designed to handle exactly one build per VM instance.
/// After the build completes, the VM shuts down.
pub async fn accept_build_request() -> Result<(VsockStream, BuildRequest), VsockError> {
    let addr = VsockAddr::new(VMADDR_CID_ANY, BUILD_PORT);
    let mut listener = VsockListener::bind(addr)?;

    info!(port = BUILD_PORT, "Listening for build request on vsock");

    // Accept a single connection
    let (mut stream, peer_addr) = listener.accept().await?;
    info!(peer = ?peer_addr, "Accepted connection from host");

    // Read the build request
    let request = receive_request(&mut stream).await?;

    Ok((stream, request))
}

/// Receive a build request from the stream.
async fn receive_request(stream: &mut VsockStream) -> Result<BuildRequest, VsockError> {
    // Read length header (4 bytes, little-endian)
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            VsockError::ConnectionClosed
        } else {
            VsockError::Io(e)
        }
    })?;

    #[allow(clippy::as_conversions)]
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        return Err(VsockError::MessageTooLarge { size: len });
    }

    // Read message payload
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            VsockError::ConnectionClosed
        } else {
            VsockError::Io(e)
        }
    })?;

    // Deserialise
    let message: BuildMessage = rkyv::from_bytes::<BuildMessage, RkyvError>(&buf)
        .map_err(|e| VsockError::Deserialisation(e.to_string()))?;

    match message {
        BuildMessage::Request(request) => Ok(request),
        _ => Err(VsockError::UnexpectedMessage),
    }
}

/// Send build output to the host.
pub async fn send_output(stream: &mut VsockStream, output: BuildOutput) -> Result<(), VsockError> {
    let message = BuildMessage::Output(output);
    send_message(stream, &message).await
}

/// Send the final build result to the host.
pub async fn send_result(stream: &mut VsockStream, result: BuildResult) -> Result<(), VsockError> {
    let message = BuildMessage::Result(result);
    send_message(stream, &message).await
}

/// Send a message over the stream.
async fn send_message<T>(stream: &mut VsockStream, message: &T) -> Result<(), VsockError>
where
    T: Archive,
    T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
{
    let bytes = rkyv::to_bytes::<RkyvError>(message)
        .map_err(|e| VsockError::Serialisation(e.to_string()))?;

    #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
    let len = bytes.len() as u32;

    // Build the complete frame
    let mut buf = Vec::with_capacity(4 + bytes.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&bytes);

    stream.write_all(&buf).await?;
    stream.flush().await?;

    Ok(())
}
