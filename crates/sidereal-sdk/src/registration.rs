//! Runtime registration with the host scheduler.
//!
//! When running inside a Firecracker VM, this module handles registering
//! discovered functions with the host at startup via vsock.

use crate::registry::{get_functions, FunctionMetadata};
use crate::triggers::TriggerKind;
use sidereal_proto::codec::{Codec, FrameHeader, MessageType, FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};
use sidereal_proto::{
    Capacity, Envelope, RegisterRequest, RegisterResponse, SchedulerMessage, TriggerType,
};
use std::path::Path;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

const SCHEDULER_PORT: u32 = sidereal_proto::ports::SCHEDULER;
const VSOCK_PATH: &str = "/dev/vsock";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);
const HOST_CID: u32 = 2;

/// Errors that can occur during registration with the host.
#[derive(Error, Debug)]
pub enum RegistrationError {
    /// vsock device not available (not running in a VM).
    #[error("vsock not available: {0}")]
    VsockNotAvailable(String),

    /// Failed to connect to the host.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Protocol-level error during registration.
    #[error("protocol error: {0}")]
    ProtocolError(String),

    /// Host rejected the registration.
    #[error("registration rejected: {0}")]
    Rejected(String),

    /// No functions were discovered to register.
    #[error("no functions to register")]
    NoFunctions,

    /// Timed out waiting for response.
    #[error("timeout waiting for response")]
    Timeout,

    /// I/O error during communication.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for registration operations.
pub type RegistrationResult<T> = Result<T, RegistrationError>;

/// Information returned after successful registration.
pub struct RegistrationInfo {
    /// Names of the functions that were registered.
    pub functions: Vec<String>,
    /// Heartbeat interval the host expects (in seconds).
    pub heartbeat_interval_secs: u32,
}

fn trigger_kind_to_proto(kind: TriggerKind) -> TriggerType {
    match kind {
        TriggerKind::Http => TriggerType::Http,
        TriggerKind::Queue => TriggerType::Queue,
        TriggerKind::Schedule => TriggerType::Schedule,
    }
}

fn collect_functions() -> Vec<(&'static FunctionMetadata, TriggerType)> {
    get_functions()
        .map(|f| (f, trigger_kind_to_proto(f.trigger_kind)))
        .collect()
}

fn collect_unique_triggers(functions: &[(&FunctionMetadata, TriggerType)]) -> Vec<TriggerType> {
    let mut triggers = Vec::new();
    for (_, t) in functions {
        if !triggers.contains(t) {
            triggers.push(*t);
        }
    }
    triggers
}

fn build_register_request(
    worker_id: &str,
    functions: &[(&FunctionMetadata, TriggerType)],
) -> RegisterRequest {
    let function_names: Vec<String> = functions.iter().map(|(f, _)| f.name.to_owned()).collect();
    let _triggers = collect_unique_triggers(functions);

    RegisterRequest::new(worker_id, "vsock")
        .with_functions(function_names)
        .with_capacity(Capacity::default())
}

/// Check if vsock is available (running inside a VM).
#[must_use]
pub fn is_vsock_available() -> bool {
    Path::new(VSOCK_PATH).exists()
}

async fn connect_vsock() -> RegistrationResult<tokio_vsock::VsockStream> {
    let addr = tokio_vsock::VsockAddr::new(HOST_CID, SCHEDULER_PORT);
    let stream = tokio::time::timeout(DEFAULT_TIMEOUT, tokio_vsock::VsockStream::connect(addr))
        .await
        .map_err(|_| RegistrationError::Timeout)?
        .map_err(|e| RegistrationError::ConnectionFailed(e.to_string()))?;

    Ok(stream)
}

async fn send_registration(
    stream: &mut tokio_vsock::VsockStream,
    request: RegisterRequest,
) -> RegistrationResult<RegisterResponse> {
    let envelope = Envelope::new(SchedulerMessage::Register(request));

    let mut codec = Codec::with_capacity(4096);
    let bytes = codec
        .encode(&envelope, MessageType::Scheduler)
        .map_err(|e| RegistrationError::ProtocolError(e.to_string()))?;

    stream.write_all(bytes).await?;
    stream.flush().await?;

    let mut header_buf = [0u8; FRAME_HEADER_SIZE];
    tokio::time::timeout(DEFAULT_TIMEOUT, stream.read_exact(&mut header_buf))
        .await
        .map_err(|_| RegistrationError::Timeout)?
        .map_err(|e| RegistrationError::ProtocolError(format!("read header: {e}")))?;

    let header = FrameHeader::decode(&header_buf)
        .map_err(|e| RegistrationError::ProtocolError(e.to_string()))?;

    if header.message_type != MessageType::Scheduler {
        return Err(RegistrationError::ProtocolError(format!(
            "expected Scheduler message, got {:?}",
            header.message_type
        )));
    }

    #[allow(clippy::as_conversions)]
    let len = header.payload_len as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(RegistrationError::ProtocolError(format!(
            "response too large: {len} bytes"
        )));
    }

    let mut payload_buf = vec![0u8; len];
    tokio::time::timeout(DEFAULT_TIMEOUT, stream.read_exact(&mut payload_buf))
        .await
        .map_err(|_| RegistrationError::Timeout)?
        .map_err(|e| RegistrationError::ProtocolError(format!("read payload: {e}")))?;

    let response_envelope: Envelope<SchedulerMessage> =
        Codec::decode(&payload_buf).map_err(|e| RegistrationError::ProtocolError(e.to_string()))?;

    match response_envelope.payload {
        SchedulerMessage::RegisterAck(response) => Ok(response),
        other => Err(RegistrationError::ProtocolError(format!(
            "unexpected response: {other:?}"
        ))),
    }
}

/// Register discovered functions with the host scheduler.
///
/// This function collects all functions discovered via the `inventory` crate
/// and sends a registration request to the host over vsock. It retries up to
/// 3 times on connection failure.
///
/// # Arguments
///
/// * `worker_id` - Unique identifier for this worker instance.
///
/// # Returns
///
/// On success, returns information about the registered functions including
/// the heartbeat interval the host expects.
///
/// # Errors
///
/// Returns an error if:
/// - No functions were discovered
/// - Failed to connect to the host after retries
/// - The host rejected the registration
/// - A protocol error occurred
pub async fn register_with_host(worker_id: &str) -> RegistrationResult<RegistrationInfo> {
    let functions = collect_functions();

    if functions.is_empty() {
        return Err(RegistrationError::NoFunctions);
    }

    info!(
        worker_id = %worker_id,
        function_count = functions.len(),
        "registering functions with host"
    );

    for func in &functions {
        debug!(name = %func.0.name, trigger = ?func.1, "discovered function");
    }

    let request = build_register_request(worker_id, &functions);
    let function_names: Vec<String> = request.functions.clone();

    let mut last_error = None;

    for attempt in 1..=MAX_RETRIES {
        debug!(attempt, max = MAX_RETRIES, "attempting registration");

        match connect_vsock().await {
            Ok(mut stream) => match send_registration(&mut stream, request.clone()).await {
                Ok(response) => {
                    if response.success {
                        info!(
                            worker_id = %worker_id,
                            functions = ?function_names,
                            heartbeat_interval = response.heartbeat_interval_secs,
                            "registration successful"
                        );
                        return Ok(RegistrationInfo {
                            functions: function_names,
                            heartbeat_interval_secs: response.heartbeat_interval_secs,
                        });
                    }
                    let msg = response
                        .error_message
                        .unwrap_or_else(|| "unknown error".to_owned());
                    return Err(RegistrationError::Rejected(msg));
                }
                Err(e) => {
                    warn!(attempt, error = %e, "registration attempt failed");
                    last_error = Some(e);
                }
            },
            Err(e) => {
                warn!(attempt, error = %e, "connection attempt failed");
                last_error = Some(e);
            }
        }

        if attempt < MAX_RETRIES {
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    Err(last_error.unwrap_or(RegistrationError::Timeout))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_kind_conversion() {
        assert_eq!(trigger_kind_to_proto(TriggerKind::Http), TriggerType::Http);
        assert_eq!(
            trigger_kind_to_proto(TriggerKind::Queue),
            TriggerType::Queue
        );
        assert_eq!(
            trigger_kind_to_proto(TriggerKind::Schedule),
            TriggerType::Schedule
        );
    }

    #[test]
    fn build_request() {
        let request = build_register_request("test-worker", &[]);
        assert_eq!(request.worker_id, "test-worker");
        assert_eq!(request.address, "vsock");
        assert!(request.functions.is_empty());
    }

    #[test]
    fn unique_triggers() {
        let triggers = collect_unique_triggers(&[]);
        assert!(triggers.is_empty());
    }
}
