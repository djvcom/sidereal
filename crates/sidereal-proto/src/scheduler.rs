//! Scheduler protocol messages.
//!
//! These messages are used for communication between workers and the scheduler:
//!
//! - **Worker → Scheduler**: Registration, heartbeats, status updates
//! - **Scheduler → Worker**: Registration acknowledgement, drain commands, health pings

use rkyv::{Archive, Deserialize, Serialize};

/// Scheduler message types.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SchedulerMessage {
    // Worker -> Scheduler
    /// Worker registration request.
    Register(RegisterRequest),
    /// Periodic heartbeat from worker.
    Heartbeat(HeartbeatRequest),
    /// Worker deregistration request.
    Deregister(DeregisterRequest),
    /// Explicit status update from worker.
    StatusUpdate(StatusUpdateRequest),

    // Scheduler -> Worker
    /// Registration acknowledgement.
    RegisterAck(RegisterResponse),
    /// Heartbeat acknowledgement.
    HeartbeatAck(HeartbeatResponse),
    /// Deregistration acknowledgement.
    DeregisterAck,
    /// Graceful shutdown signal.
    Drain(DrainRequest),
    /// Active health check request.
    Ping,
    /// Active health check response.
    Pong,
}

/// Worker registration request.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RegisterRequest {
    /// Unique worker identifier.
    pub worker_id: String,
    /// Network address for function dispatch.
    pub address: String,
    /// vsock CID if running in Firecracker VM.
    pub vsock_cid: Option<u32>,
    /// Functions this worker can handle.
    pub functions: Vec<String>,
    /// Worker capacity.
    pub capacity: Capacity,
    /// Supported trigger types.
    pub triggers: Vec<TriggerType>,
    /// Worker capabilities (e.g., GPU, special hardware).
    pub capabilities: Vec<String>,
    /// Metadata for version tracking, rolling deploys, etc.
    pub metadata: Vec<(String, String)>,
}

impl RegisterRequest {
    /// Creates a new registration request with minimal required fields.
    #[must_use]
    pub fn new(worker_id: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            address: address.into(),
            vsock_cid: None,
            functions: Vec::new(),
            capacity: Capacity::default(),
            triggers: vec![TriggerType::Http],
            capabilities: Vec::new(),
            metadata: Vec::new(),
        }
    }

    /// Sets the vsock CID.
    #[must_use]
    pub const fn with_vsock_cid(mut self, cid: u32) -> Self {
        self.vsock_cid = Some(cid);
        self
    }

    /// Adds functions this worker can handle.
    #[must_use]
    pub fn with_functions(mut self, functions: Vec<String>) -> Self {
        self.functions = functions;
        self
    }

    /// Sets the worker capacity.
    #[must_use]
    pub const fn with_capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = capacity;
        self
    }
}

/// Registration response from scheduler.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RegisterResponse {
    /// Whether registration was successful.
    pub success: bool,
    /// Error code if registration failed.
    pub error_code: Option<SchedulerErrorCode>,
    /// Error message if registration failed.
    pub error_message: Option<String>,
    /// Heartbeat interval the worker should use (seconds).
    pub heartbeat_interval_secs: u32,
}

impl RegisterResponse {
    /// Creates a successful registration response.
    #[must_use]
    pub const fn success(heartbeat_interval_secs: u32) -> Self {
        Self {
            success: true,
            error_code: None,
            error_message: None,
            heartbeat_interval_secs,
        }
    }

    /// Creates a failed registration response.
    #[must_use]
    pub fn error(code: SchedulerErrorCode, message: impl Into<String>) -> Self {
        Self {
            success: false,
            error_code: Some(code),
            error_message: Some(message.into()),
            heartbeat_interval_secs: 0,
        }
    }
}

/// Worker heartbeat request.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRequest {
    /// Worker identifier.
    pub worker_id: String,
    /// Current concurrent invocation count.
    pub current_load: u32,
    /// Memory used in MB.
    pub memory_used_mb: u32,
    /// Number of active invocations.
    pub active_invocations: u32,
    /// Errors since last heartbeat.
    pub error_count: u32,
    /// Average latency in milliseconds since last heartbeat.
    pub avg_latency_ms: u32,
}

impl HeartbeatRequest {
    /// Creates a new heartbeat request.
    #[must_use]
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            current_load: 0,
            memory_used_mb: 0,
            active_invocations: 0,
            error_count: 0,
            avg_latency_ms: 0,
        }
    }
}

/// Heartbeat response from scheduler.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResponse {
    /// Whether the heartbeat was accepted.
    pub accepted: bool,
    /// Commands for the worker to execute.
    pub commands: Vec<WorkerCommand>,
}

impl HeartbeatResponse {
    /// Creates an accepted heartbeat response.
    #[must_use]
    pub const fn accepted() -> Self {
        Self {
            accepted: true,
            commands: Vec::new(),
        }
    }

    /// Creates a response with commands.
    #[must_use]
    pub const fn with_commands(commands: Vec<WorkerCommand>) -> Self {
        Self {
            accepted: true,
            commands,
        }
    }
}

/// Commands that can be sent to workers via heartbeat response.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum WorkerCommand {
    /// Update heartbeat interval.
    SetHeartbeatInterval { interval_secs: u32 },
    /// Prepare for shutdown.
    PrepareShutdown,
}

/// Worker deregistration request.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DeregisterRequest {
    /// Worker identifier.
    pub worker_id: String,
    /// Reason for deregistration.
    pub reason: DeregisterReason,
}

/// Reason for worker deregistration.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeregisterReason {
    /// Normal shutdown.
    Shutdown,
    /// Worker is restarting.
    Restart,
    /// Worker encountered an error.
    Error,
}

/// Status update request from worker.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StatusUpdateRequest {
    /// Worker identifier.
    pub worker_id: String,
    /// New status.
    pub status: WorkerStatusProto,
    /// Optional reason for status change.
    pub reason: Option<String>,
}

/// Worker status for protocol messages.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatusProto {
    /// Worker is starting up.
    Starting,
    /// Worker is healthy and ready.
    Healthy,
    /// Worker is operational but degraded.
    Degraded,
    /// Worker is unhealthy.
    Unhealthy,
    /// Worker is draining (finishing current requests).
    Draining,
    /// Worker has terminated.
    Terminated,
}

/// Drain request from scheduler to worker.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DrainRequest {
    /// Maximum time to complete in-flight requests (seconds).
    pub timeout_secs: u32,
    /// Reason for drain.
    pub reason: DrainReason,
}

impl DrainRequest {
    /// Creates a new drain request.
    #[must_use]
    pub const fn new(timeout_secs: u32) -> Self {
        Self {
            timeout_secs,
            reason: DrainReason::ScaleDown,
        }
    }

    /// Creates a drain request for shutdown.
    #[must_use]
    pub const fn shutdown(timeout_secs: u32) -> Self {
        Self {
            timeout_secs,
            reason: DrainReason::Shutdown,
        }
    }
}

/// Reason for drain request.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainReason {
    /// Scaling down.
    ScaleDown,
    /// Scheduled shutdown.
    Shutdown,
    /// Worker is unhealthy.
    Unhealthy,
    /// Rolling update in progress.
    RollingUpdate,
}

/// Worker capacity information.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Capacity {
    /// Maximum concurrent invocations.
    pub max_concurrent: u32,
    /// Memory available in MB.
    pub memory_mb: u32,
    /// CPU in millicores (1000 = 1 CPU).
    pub cpu_millicores: u32,
}

impl Default for Capacity {
    fn default() -> Self {
        Self {
            max_concurrent: 10,
            memory_mb: 512,
            cpu_millicores: 1000,
        }
    }
}

/// Trigger types a worker can handle.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerType {
    /// HTTP request trigger.
    Http,
    /// Queue message trigger.
    Queue,
    /// Scheduled (cron) trigger.
    Schedule,
}

/// Scheduler error codes (range 60-69).
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SchedulerErrorCode {
    /// Worker not found in registry.
    WorkerNotFound = 60,
    /// Worker already registered.
    WorkerAlreadyRegistered = 61,
    /// Invalid capacity configuration.
    InvalidCapacity = 62,
    /// Registration failed.
    RegistrationFailed = 63,
    /// Heartbeat from unknown worker.
    UnknownWorker = 64,
    /// Worker is in invalid state for operation.
    InvalidState = 65,
}

impl SchedulerErrorCode {
    /// Returns the numeric value of this error code.
    #[must_use]
    #[allow(clippy::as_conversions)]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Creates an error code from a numeric value.
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            60 => Some(Self::WorkerNotFound),
            61 => Some(Self::WorkerAlreadyRegistered),
            62 => Some(Self::InvalidCapacity),
            63 => Some(Self::RegistrationFailed),
            64 => Some(Self::UnknownWorker),
            65 => Some(Self::InvalidState),
            _ => None,
        }
    }

    /// Checks if this is a scheduler error (60-69).
    #[must_use]
    pub const fn is_scheduler_error(self) -> bool {
        matches!(self.as_u8(), 60..=69)
    }
}

impl std::fmt::Display for SchedulerErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkerNotFound => write!(f, "worker_not_found"),
            Self::WorkerAlreadyRegistered => write!(f, "worker_already_registered"),
            Self::InvalidCapacity => write!(f, "invalid_capacity"),
            Self::RegistrationFailed => write!(f, "registration_failed"),
            Self::UnknownWorker => write!(f, "unknown_worker"),
            Self::InvalidState => write!(f, "invalid_state"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_request_builder() {
        let req = RegisterRequest::new("worker-1", "192.168.1.1:8080")
            .with_vsock_cid(3)
            .with_functions(vec!["greet".into(), "farewell".into()])
            .with_capacity(Capacity {
                max_concurrent: 20,
                memory_mb: 1024,
                cpu_millicores: 2000,
            });

        assert_eq!(req.worker_id, "worker-1");
        assert_eq!(req.vsock_cid, Some(3));
        assert_eq!(req.functions.len(), 2);
        assert_eq!(req.capacity.max_concurrent, 20);
    }

    #[test]
    fn scheduler_error_code_roundtrip() {
        let codes = [
            SchedulerErrorCode::WorkerNotFound,
            SchedulerErrorCode::WorkerAlreadyRegistered,
            SchedulerErrorCode::InvalidCapacity,
            SchedulerErrorCode::RegistrationFailed,
            SchedulerErrorCode::UnknownWorker,
            SchedulerErrorCode::InvalidState,
        ];

        for code in codes {
            let value = code.as_u8();
            let restored = SchedulerErrorCode::from_u8(value);
            assert_eq!(restored, Some(code));
            assert!(code.is_scheduler_error());
        }
    }

    #[test]
    fn heartbeat_response_with_commands() {
        let response = HeartbeatResponse::with_commands(vec![
            WorkerCommand::SetHeartbeatInterval { interval_secs: 10 },
            WorkerCommand::PrepareShutdown,
        ]);

        assert!(response.accepted);
        assert_eq!(response.commands.len(), 2);
    }
}
