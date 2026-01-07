//! Sidereal runtime - runs as /sbin/init inside Firecracker VMs.
//!
//! This binary:
//! 1. Mounts required filesystems (proc, sys, tmp)
//! 2. Sets up signal handlers
//! 3. Connects to host state server (optional)
//! 4. Listens on vsock for requests from the host
//! 5. Executes function invocations
//! 6. Handles graceful shutdown

use sidereal_proto::codec::{Codec, FrameHeader, MessageType, FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};
use sidereal_proto::ports::FUNCTION as VSOCK_PORT;
use sidereal_proto::{ControlMessage, Envelope, FunctionMessage, InvokeRequest, InvokeResponse, ProtocolError};
use sidereal_state::VsockStateClient;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

mod filesystem;
mod signals;

/// Global state client for functions to access KV/Queue/Lock.
static STATE_CLIENT: OnceLock<Arc<VsockStateClient>> = OnceLock::new();

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .init();

    info!("Sidereal runtime starting (PID 1)");

    if let Err(e) = filesystem::mount_filesystems() {
        error!("Failed to mount filesystems: {}", e);
    }

    signals::setup_signal_handlers();

    // Try to connect to host state server (optional - may not be configured)
    init_state_client().await;

    if let Err(e) = run_vsock_server().await {
        error!("vsock server error: {}", e);
        std::process::exit(1);
    }
}

/// Initialise connection to the host state server.
///
/// This is optional - if the state server isn't running, functions simply
/// won't have access to state primitives.
async fn init_state_client() {
    match VsockStateClient::connect().await {
        Ok(client) => {
            info!("Connected to host state server");
            let _ = STATE_CLIENT.set(Arc::new(client));
        }
        Err(e) => {
            warn!(error = %e, "State server not available - functions will not have state access");
        }
    }
}

/// Get the state client if available.
#[allow(dead_code)]
pub fn state_client() -> Option<Arc<VsockStateClient>> {
    STATE_CLIENT.get().cloned()
}

async fn run_vsock_server() -> Result<(), Box<dyn std::error::Error>> {
    use tokio_vsock::{VsockAddr, VsockListener, VMADDR_CID_ANY};

    let addr = VsockAddr::new(VMADDR_CID_ANY, VSOCK_PORT);
    let mut listener = VsockListener::bind(addr)?;

    info!(port = VSOCK_PORT, "Listening on vsock");

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                debug!(peer = ?peer_addr, "Accepted connection");
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream).await {
                        warn!(peer = ?peer_addr, error = %e, "Connection error");
                    }
                });
            }
            Err(e) => {
                if e.kind() == ErrorKind::Interrupted {
                    continue;
                }
                error!("Accept error: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn handle_connection(
    mut stream: tokio_vsock::VsockStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let codec = Mutex::new(Codec::with_capacity(8192));

    loop {
        // Read frame header
        let mut header_buf = [0u8; FRAME_HEADER_SIZE];
        match stream.read_exact(&mut header_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                debug!("Connection closed");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        let header = FrameHeader::decode(&header_buf)?;

        let len = header.payload_len as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(format!("Request too large: {} bytes", len).into());
        }

        // Read payload
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;

        // Process based on message type
        let shutdown = match header.message_type {
            MessageType::Function => {
                let envelope: Envelope<FunctionMessage> = Codec::decode(&buf)?;
                let response = handle_function_message(envelope).await;

                // Encode and send response
                let bytes = {
                    let mut c = codec.lock().unwrap();
                    c.encode(&response, MessageType::Function)
                        .map_err(|e: ProtocolError| e.to_string())?
                        .to_vec()
                };
                stream.write_all(&bytes).await?;
                stream.flush().await?;
                false
            }
            MessageType::Control => {
                let envelope: Envelope<ControlMessage> = Codec::decode(&buf)?;
                let (response, shutdown) = handle_control_message(envelope).await;

                // Encode and send response
                let bytes = {
                    let mut c = codec.lock().unwrap();
                    c.encode(&response, MessageType::Control)
                        .map_err(|e: ProtocolError| e.to_string())?
                        .to_vec()
                };
                stream.write_all(&bytes).await?;
                stream.flush().await?;
                shutdown
            }
            MessageType::State => {
                // State messages not handled by runtime yet
                warn!("Received unsupported State message");
                false
            }
        };

        if shutdown {
            info!("Shutdown acknowledged, exiting");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            std::process::exit(0);
        }
    }
}

async fn handle_function_message(envelope: Envelope<FunctionMessage>) -> Envelope<FunctionMessage> {
    match envelope.payload {
        FunctionMessage::Invoke(request) => {
            info!(function = %request.function_name, "Invoking function");

            match invoke_function(&request).await {
                Ok(response) => {
                    debug!(function = %request.function_name, status = response.status, "Function completed");
                    Envelope::response_to(&envelope.header, FunctionMessage::Response(response))
                }
                Err(e) => {
                    error!(function = %request.function_name, error = %e, "Function failed");
                    let response = InvokeResponse::error(e.to_string());
                    Envelope::response_to(&envelope.header, FunctionMessage::Response(response))
                }
            }
        }
        FunctionMessage::Response(_) => {
            warn!("Received unexpected Response message");
            let response = InvokeResponse::bad_request("Unexpected Response message");
            Envelope::response_to(&envelope.header, FunctionMessage::Response(response))
        }
    }
}

async fn handle_control_message(envelope: Envelope<ControlMessage>) -> (Envelope<ControlMessage>, bool) {
    match envelope.payload {
        ControlMessage::Ping => {
            debug!("Received ping");
            (
                Envelope::response_to(&envelope.header, ControlMessage::Pong),
                false,
            )
        }
        ControlMessage::Shutdown => {
            info!("Shutdown requested");
            (
                Envelope::response_to(&envelope.header, ControlMessage::ShutdownAck),
                true,
            )
        }
        ControlMessage::Pong | ControlMessage::ShutdownAck => {
            warn!("Received unexpected control response");
            (
                Envelope::response_to(&envelope.header, ControlMessage::Pong),
                false,
            )
        }
    }
}

async fn invoke_function(
    request: &InvokeRequest,
) -> Result<InvokeResponse, Box<dyn std::error::Error>> {
    // TODO: Implement actual function invocation
    // For now, return a placeholder response
    info!(
        function = %request.function_name,
        payload_len = request.payload.len(),
        "Function invocation (placeholder)"
    );

    let response = serde_json::json!({
        "message": format!("Function '{}' executed successfully (placeholder)", request.function_name),
        "payload_received": request.payload.len()
    });

    Ok(InvokeResponse::ok(serde_json::to_vec(&response)?))
}
