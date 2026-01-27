//! Error types for Firecracker operations.

use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FirecrackerError {
    #[error("Firecracker binary not found at {0}")]
    BinaryNotFound(PathBuf),

    #[error("Kernel image not found at {0}")]
    KernelNotFound(PathBuf),

    #[error("Rootfs not found at {0}")]
    RootfsNotFound(PathBuf),

    #[error("KVM not available: {0}")]
    KvmNotAvailable(String),

    #[error("Failed to start VM: {0}")]
    VmStartFailed(String),

    #[error("Failed to configure VM: {0}")]
    VmConfigFailed(String),

    #[error(
        "VM not ready after {timeout_secs} seconds: {last_error}\n\nConsole output:\n{console_log}"
    )]
    VmNotReady {
        timeout_secs: u64,
        last_error: String,
        console_log: String,
    },

    #[error("vsock connection failed: {0}")]
    VsockConnectionFailed(String),

    #[error("vsock communication error: {0}")]
    VsockError(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Cross-compilation failed: {0}")]
    CrossCompileFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("VM shutdown failed: {0}")]
    ShutdownFailed(String),

    #[error("Firecracker API error: {status} - {message}")]
    ApiError { status: u16, message: String },
}

pub type Result<T> = std::result::Result<T, FirecrackerError>;
