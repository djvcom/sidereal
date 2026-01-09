//! Error types for secrets management.

use std::fmt;

use thiserror::Error;

/// Errors that can occur during secrets operations.
#[derive(Debug, Error)]
pub enum SecretsError {
    /// Secret not found at any scope.
    #[error("secret not found: {name}")]
    NotFound {
        /// The name of the secret that was not found.
        name: String,
    },

    /// Backend not configured.
    #[error("secrets backend not configured")]
    NotConfigured,

    /// Unsupported backend type.
    #[error("unsupported secrets backend: {0}")]
    UnsupportedBackend(String),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Configuration(String),

    /// Backend connection error.
    #[error("connection error: {0}")]
    Connection(String),

    /// Operation not supported by this backend.
    #[error("operation not supported: {0}")]
    NotSupported(String),

    /// Encryption error.
    #[error("encryption error")]
    Encryption(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Decryption error.
    #[error("decryption error")]
    Decryption(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Master key not found.
    #[error("master key not found")]
    MasterKeyNotFound,

    /// Insecure key file permissions.
    #[error("insecure key file permissions: {path} has mode {mode:o}")]
    InsecureKeyFile {
        /// The path to the insecure key file.
        path: String,
        /// The file mode (permissions) in octal.
        mode: u32,
    },

    /// Database error.
    #[error("database error: {0}")]
    Database(String),

    /// Backend error.
    #[error("backend error: {0}")]
    Backend(String),
}

/// A redacted wrapper for error contexts that might contain sensitive data.
///
/// Implements `Debug` and `Display` to show only `[REDACTED]`, preventing
/// accidental logging of sensitive information.
pub struct Redacted<T>(T);

impl<T> Redacted<T> {
    /// Creates a new redacted wrapper around a value.
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    /// Consumes the wrapper and returns the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl<T> fmt::Display for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}
