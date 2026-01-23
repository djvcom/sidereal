//! Vsock communication for builder VMs.
//!
//! Handles the vsock protocol for receiving build requests and sending
//! build output/results back to the host.

use std::future::Future;
use std::io;

use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VsockAddr, VsockListener, VMADDR_CID_ANY};

use sidereal_build::protocol::{BuildMessage, BuildOutput, BuildRequest, BuildResult, BUILD_PORT};

/// Frame header size: 4 bytes for message length.
const FRAME_HEADER_SIZE: usize = 4;

/// Maximum message size (16 MB).
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Run the vsock server, handling a single build request.
///
/// The server listens on port 1028, accepts a single connection,
/// processes one build request, and returns.
pub async fn run_vsock_server<F, Fut>(build_fn: F) -> anyhow::Result<()>
where
    F: FnOnce(BuildRequest, MessageSender) -> Fut,
    Fut: Future<Output = anyhow::Result<BuildResult>>,
{
    eprintln!("[vsock] Listening on port {BUILD_PORT}...");

    let mut listener = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, BUILD_PORT))?;
    let (stream, addr) = listener.accept().await?;

    eprintln!("[vsock] Connection from CID {}", addr.cid());

    // Split stream for bidirectional communication
    let (reader, writer) = stream.into_split();
    let reader = tokio::io::BufReader::new(reader);
    let writer = tokio::io::BufWriter::new(writer);

    let mut receiver = MessageReceiver { reader };
    let sender = MessageSender { writer };

    // Receive build request
    let request = match receiver.receive::<BuildRequest>().await? {
        Some(req) => req,
        None => {
            return Err(anyhow::anyhow!(
                "Connection closed before receiving request"
            ));
        }
    };

    eprintln!("[vsock] Received build request: {}", request.build_id);

    // Execute build (streaming output via sender)
    let result = build_fn(request, sender).await?;

    eprintln!("[vsock] Build finished, sending result");

    // Result is sent by build_fn, we're done
    drop(result);

    Ok(())
}

/// Message sender for streaming build output.
pub struct MessageSender {
    writer: tokio::io::BufWriter<tokio_vsock::OwnedWriteHalf>,
}

impl MessageSender {
    /// Send a build output message.
    pub async fn send_output(&mut self, output: BuildOutput) -> io::Result<()> {
        let message = BuildMessage::Output(output);
        self.send(&message).await
    }

    /// Send the final build result.
    pub async fn send_result(&mut self, result: BuildResult) -> io::Result<()> {
        let message = BuildMessage::Result(result);
        self.send(&message).await
    }

    /// Send a message.
    async fn send<T>(&mut self, message: &T) -> io::Result<()>
    where
        T: Archive,
        T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    {
        let bytes = rkyv::to_bytes::<RkyvError>(message).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Serialisation failed: {e}"),
            )
        })?;

        if bytes.len() > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Message too large: {} > {MAX_MESSAGE_SIZE}", bytes.len()),
            ));
        }

        // Write length header
        #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
        let len = bytes.len() as u32;
        self.writer.write_all(&len.to_le_bytes()).await?;

        // Write message
        self.writer.write_all(&bytes).await?;
        self.writer.flush().await?;

        Ok(())
    }
}

/// Message receiver for reading build requests.
struct MessageReceiver {
    reader: tokio::io::BufReader<tokio_vsock::OwnedReadHalf>,
}

impl MessageReceiver {
    /// Receive a message.
    async fn receive<T>(&mut self) -> io::Result<Option<T>>
    where
        T: Archive,
        T::Archived: for<'a> CheckBytes<HighValidator<'a, RkyvError>>
            + Deserialize<T, HighDeserializer<RkyvError>>,
    {
        // Read length header
        let mut len_buf = [0u8; FRAME_HEADER_SIZE];
        match self.reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let len = u32::from_le_bytes(len_buf) as usize;

        if len > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Message too large: {len} > {MAX_MESSAGE_SIZE}"),
            ));
        }

        // Read message
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).await?;

        // Deserialise
        let message: T = rkyv::from_bytes::<T, RkyvError>(&buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Deserialisation failed: {e}"),
            )
        })?;

        Ok(Some(message))
    }
}
