//! Sidereal runtime - runs as /sbin/init inside Firecracker VMs.
//!
//! This binary:
//! 1. Mounts required filesystems (proc, sys, tmp)
//! 2. Sets up signal handlers
//! 3. Spawns the user's application
//! 4. Connects to host state server (optional)
//! 5. Listens on vsock for requests from the host
//! 6. Proxies function invocations to the user's HTTP server
//! 7. Handles graceful shutdown

use sidereal_proto::codec::{Codec, FrameHeader, MessageType, FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};
use sidereal_proto::ports::FUNCTION as VSOCK_PORT;
use sidereal_proto::{
    ControlMessage, Envelope, FunctionMessage, InvokeRequest, InvokeResponse, ProtocolError,
};
use sidereal_state::VsockStateClient;
use std::io::ErrorKind;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tracing::{debug, error, info, warn};

const APP_ENTRYPOINT: &str = "/app/binary";
const APP_HTTP_PORT: u16 = 3000;
const APP_READY_TIMEOUT: Duration = Duration::from_secs(60);
const APP_READY_POLL_INTERVAL: Duration = Duration::from_millis(100);

mod filesystem;
mod signals;

/// Global state client for functions to access KV/Queue/Lock.
static STATE_CLIENT: OnceLock<Arc<VsockStateClient>> = OnceLock::new();

/// HTTP client for proxying requests to the user's application.
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

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

    // Spawn the user's application if it exists
    if Path::new(APP_ENTRYPOINT).exists() {
        info!(entrypoint = APP_ENTRYPOINT, "Starting user application");

        // Generate worker ID from hostname or use a default
        let worker_id = std::fs::read_to_string("/etc/hostname")
            .map(|s| s.trim().to_owned())
            .unwrap_or_else(|_| "worker".to_owned());

        let mut child = match Command::new(APP_ENTRYPOINT)
            .env("SIDEREAL_WORKER_ID", &worker_id)
            .env("SIDEREAL_PORT", APP_HTTP_PORT.to_string())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                error!(error = %e, "Failed to spawn user application");
                std::process::exit(1);
            }
        };

        // Initialise the HTTP client for proxying to localhost
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to create HTTP client");
        let _ = HTTP_CLIENT.set(client);

        // Wait for the application to be ready
        if let Err(e) = wait_for_app_ready().await {
            error!(error = %e, "Application failed to start");
            let _ = child.kill().await;
            std::process::exit(1);
        }

        info!("User application ready");

        // Spawn a task to monitor the child process
        tokio::spawn(async move {
            match child.wait().await {
                Ok(status) => {
                    if status.success() {
                        info!("User application exited normally");
                    } else {
                        error!(code = ?status.code(), "User application exited with error");
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to wait for user application");
                }
            }
            // If the app exits, we should shut down
            std::process::exit(1);
        });
    } else {
        info!(
            entrypoint = APP_ENTRYPOINT,
            "No user application found, running in standalone mode"
        );
    }

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

/// Wait for the user's HTTP server to be ready.
async fn wait_for_app_ready() -> Result<(), Box<dyn std::error::Error>> {
    let client = HTTP_CLIENT.get().ok_or("HTTP client not initialised")?;
    let url = format!("http://127.0.0.1:{APP_HTTP_PORT}/health");
    let deadline = tokio::time::Instant::now() + APP_READY_TIMEOUT;

    debug!(url = %url, "Waiting for application to be ready");

    while tokio::time::Instant::now() < deadline {
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                return Ok(());
            }
            Ok(response) => {
                debug!(status = %response.status(), "Application not ready yet");
            }
            Err(e) => {
                debug!(error = %e, "Application not ready yet");
            }
        }
        tokio::time::sleep(APP_READY_POLL_INTERVAL).await;
    }

    Err("timeout waiting for application".into())
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

        #[allow(clippy::as_conversions)]
        let len = header.payload_len as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(format!("Request too large: {len} bytes").into());
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
                    let mut c = codec.lock().map_err(|_| "codec lock poisoned".to_owned())?;
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
                let (response, shutdown) = handle_control_message(&envelope);

                // Encode and send response
                let bytes = {
                    let mut c = codec.lock().map_err(|_| "codec lock poisoned".to_owned())?;
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
            MessageType::Scheduler => {
                // Scheduler messages are between workers and scheduler service
                warn!("Received unexpected Scheduler message on runtime channel");
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
        FunctionMessage::Invoke(request) => match invoke_function(&request).await {
            Ok(response) => {
                debug!(function = %request.function_name, status = response.status, "Function completed");
                Envelope::response_to(&envelope.header, FunctionMessage::Response(response))
            }
            Err(e) => {
                error!(function = %request.function_name, error = %e, "Function failed");
                let response = InvokeResponse::error(e.to_string());
                Envelope::response_to(&envelope.header, FunctionMessage::Response(response))
            }
        },
        FunctionMessage::Response(_) => {
            warn!("Received unexpected Response message");
            let response = InvokeResponse::bad_request("Unexpected Response message");
            Envelope::response_to(&envelope.header, FunctionMessage::Response(response))
        }
    }
}

fn handle_control_message(envelope: &Envelope<ControlMessage>) -> (Envelope<ControlMessage>, bool) {
    match &envelope.payload {
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
    info!(
        function = %request.function_name,
        payload_len = request.payload.len(),
        "Invoking function"
    );

    // Check if we have an HTTP client (user app is running)
    let Some(client) = HTTP_CLIENT.get() else {
        // No user app, return placeholder response
        let response = serde_json::json!({
            "error": "No user application running",
        });
        return Ok(InvokeResponse::error(serde_json::to_string(&response)?));
    };

    // Proxy the request to the user's HTTP server
    let url = format!("http://127.0.0.1:{APP_HTTP_PORT}/{}", request.function_name);

    let http_response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(request.payload.clone())
        .send()
        .await?;

    let status = http_response.status();
    let body = http_response.bytes().await?;

    debug!(
        function = %request.function_name,
        status = %status,
        body_len = body.len(),
        "Function response received"
    );

    if status.is_success() {
        Ok(InvokeResponse::ok(body.to_vec()))
    } else {
        // Include status code in the response
        Ok(InvokeResponse {
            status: status.as_u16().into(),
            headers: Vec::new(),
            body: body.to_vec(),
        })
    }
}
