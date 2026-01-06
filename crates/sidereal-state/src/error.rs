use thiserror::Error;

#[derive(Debug, Error)]
pub enum KvError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Operation timed out")]
    Timeout,

    #[error("CAS conflict: value changed")]
    Conflict,

    #[error("Serialisation error: {0}")]
    Serialisation(String),

    #[error("Backend error: {0}")]
    Backend(String),
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Operation timed out")]
    Timeout,

    #[error("Queue not found: {0}")]
    QueueNotFound(String),

    #[error("Message not found: {0}")]
    MessageNotFound(String),

    #[error("Serialisation error: {0}")]
    Serialisation(String),

    #[error("Backend error: {0}")]
    Backend(String),
}

#[derive(Debug, Error)]
pub enum LockError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Operation timed out")]
    Timeout,

    #[error("Lock already held")]
    AlreadyHeld,

    #[error("Lock not held or expired")]
    NotHeld,

    #[error("Backend error: {0}")]
    Backend(String),
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("Backend not configured: {0}")]
    NotConfigured(String),

    #[error("Unsupported backend: {0}")]
    UnsupportedBackend(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Connection error: {0}")]
    Connection(String),
}
