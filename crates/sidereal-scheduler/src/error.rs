//! Error types for the scheduler.

use thiserror::Error;

/// Scheduler errors.
#[derive(Error, Debug)]
pub enum SchedulerError {
    /// Worker not found.
    #[error("worker not found: {0}")]
    WorkerNotFound(String),

    /// Worker already registered.
    #[error("worker already registered: {0}")]
    WorkerAlreadyRegistered(String),

    /// Invalid worker state transition.
    #[error("invalid state transition from {from:?} to {to:?}")]
    InvalidStateTransition {
        from: crate::registry::WorkerStatus,
        to: crate::registry::WorkerStatus,
    },

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Valkey/Redis error.
    #[error("valkey error: {0}")]
    Valkey(#[from] deadpool_redis::PoolError),

    /// Redis command error.
    #[error("redis error: {0}")]
    Redis(#[from] deadpool_redis::redis::RedisError),

    /// Serialisation error.
    #[error("serialisation error: {0}")]
    Serialisation(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for scheduler operations.
pub type Result<T> = std::result::Result<T, SchedulerError>;
