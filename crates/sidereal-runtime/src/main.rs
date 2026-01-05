//! Sidereal runtime - runs as /sbin/init inside Firecracker VMs.
//!
//! This binary:
//! 1. Mounts required filesystems (proc, sys, tmp)
//! 2. Sets up signal handlers
//! 3. Listens on vsock for requests from the host
//! 4. Executes function invocations
//! 5. Handles graceful shutdown

use sidereal_firecracker::protocol::{GuestRequest, GuestResponse, VSOCK_PORT};
use std::io::ErrorKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

mod filesystem;
mod signals;

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

    if let Err(e) = run_vsock_server().await {
        error!("vsock server error: {}", e);
        std::process::exit(1);
    }
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
    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                debug!("Connection closed");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 10 * 1024 * 1024 {
            return Err(format!("Request too large: {} bytes", len).into());
        }

        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;

        let request: GuestRequest = serde_json::from_slice(&buf)?;
        let response = handle_request(request).await;

        let encoded = serde_json::to_vec(&response)?;
        let len = encoded.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;

        if matches!(response, GuestResponse::ShutdownAck) {
            info!("Shutdown acknowledged, exiting");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            std::process::exit(0);
        }
    }
}

async fn handle_request(request: GuestRequest) -> GuestResponse {
    match request {
        GuestRequest::Ping => {
            debug!("Received ping");
            GuestResponse::pong()
        }

        GuestRequest::Invoke {
            function_name,
            payload,
            trace_id,
        } => {
            info!(function = %function_name, trace_id = %trace_id, "Invoking function");

            match invoke_function(&function_name, &payload).await {
                Ok((status, body)) => {
                    debug!(function = %function_name, status = status, "Function completed");
                    GuestResponse::result(status, body, trace_id)
                }
                Err(e) => {
                    error!(function = %function_name, error = %e, "Function failed");
                    GuestResponse::error(e.to_string(), trace_id)
                }
            }
        }

        GuestRequest::Shutdown => {
            info!("Shutdown requested");
            GuestResponse::shutdown_ack()
        }
    }
}

async fn invoke_function(
    function_name: &str,
    payload: &[u8],
) -> Result<(u16, Vec<u8>), Box<dyn std::error::Error>> {
    // TODO: Implement actual function invocation
    // For now, return a placeholder response
    info!(function = %function_name, payload_len = payload.len(), "Function invocation (placeholder)");

    let response = serde_json::json!({
        "message": format!("Function '{}' executed successfully (placeholder)", function_name),
        "payload_received": payload.len()
    });

    Ok((200, serde_json::to_vec(&response)?))
}
