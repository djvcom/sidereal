//! Error types for the protocol.

use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

/// Protocol errors.
#[derive(Error, Debug)]
pub enum ProtocolError {
    /// Unsupported protocol version.
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u16),

    /// Invalid message payload.
    #[error("invalid payload: {0}")]
    InvalidPayload(String),

    /// Message exceeded deadline.
    #[error("deadline exceeded")]
    DeadlineExceeded,

    /// Message too large.
    #[error("message too large: {size} bytes (max {max})")]
    MessageTooLarge { size: usize, max: usize },

    /// Invalid frame header.
    #[error("invalid frame header: {0}")]
    InvalidFrameHeader(String),

    /// Unknown message type.
    #[error("unknown message type: {0}")]
    UnknownMessageType(u16),

    /// Serialisation error.
    #[error("serialisation error: {0}")]
    Serialisation(String),

    /// Deserialisation error.
    #[error("deserialisation error: {0}")]
    Deserialisation(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Protocol-level error codes.
///
/// These are included in error responses to allow structured error handling.
/// Codes are grouped by category:
/// - 1-19: Protocol errors
/// - 20-39: Function errors
/// - 50-59: Internal errors
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ErrorCode {
    // Protocol errors (1-19)
    /// Unsupported protocol version.
    UnsupportedVersion = 1,
    /// Invalid message payload.
    InvalidPayload = 2,
    /// Request deadline exceeded.
    DeadlineExceeded = 3,
    /// Message exceeds size limit.
    MessageTooLarge = 4,

    // Function errors (20-39)
    /// Function not found.
    FunctionNotFound = 20,
    /// Function execution failed.
    FunctionFailed = 21,
    /// Invalid function input.
    InvalidInput = 22,

    // Internal errors (50-59)
    /// Internal server error.
    InternalError = 50,
}

impl ErrorCode {
    /// Returns the numeric value of this error code.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Creates an error code from a numeric value.
    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::UnsupportedVersion),
            2 => Some(Self::InvalidPayload),
            3 => Some(Self::DeadlineExceeded),
            4 => Some(Self::MessageTooLarge),
            20 => Some(Self::FunctionNotFound),
            21 => Some(Self::FunctionFailed),
            22 => Some(Self::InvalidInput),
            50 => Some(Self::InternalError),
            _ => None,
        }
    }

    /// Checks if this is a protocol error (1-19).
    #[must_use]
    pub const fn is_protocol_error(self) -> bool {
        matches!(self.as_u8(), 1..=19)
    }

    /// Checks if this is a function error (20-39).
    #[must_use]
    pub const fn is_function_error(self) -> bool {
        matches!(self.as_u8(), 20..=39)
    }

    /// Checks if this is an internal error (50-59).
    #[must_use]
    pub const fn is_internal_error(self) -> bool {
        matches!(self.as_u8(), 50..=59)
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion => write!(f, "unsupported_version"),
            Self::InvalidPayload => write!(f, "invalid_payload"),
            Self::DeadlineExceeded => write!(f, "deadline_exceeded"),
            Self::MessageTooLarge => write!(f, "message_too_large"),
            Self::FunctionNotFound => write!(f, "function_not_found"),
            Self::FunctionFailed => write!(f, "function_failed"),
            Self::InvalidInput => write!(f, "invalid_input"),
            Self::InternalError => write!(f, "internal_error"),
        }
    }
}

/// State operation error codes (30-49).
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StateErrorCode {
    /// Resource not found.
    NotFound = 30,
    /// Conflict (e.g., CAS failure).
    Conflict = 31,
    /// Lock already held by another client.
    LockHeld = 32,
    /// Operation timed out.
    Timeout = 33,
    /// State backend not configured.
    NotConfigured = 34,
    /// Backend error.
    BackendError = 35,
}

impl StateErrorCode {
    /// Returns the numeric value of this error code.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Creates an error code from a numeric value.
    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            30 => Some(Self::NotFound),
            31 => Some(Self::Conflict),
            32 => Some(Self::LockHeld),
            33 => Some(Self::Timeout),
            34 => Some(Self::NotConfigured),
            35 => Some(Self::BackendError),
            _ => None,
        }
    }
}

impl std::fmt::Display for StateErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "not_found"),
            Self::Conflict => write!(f, "conflict"),
            Self::LockHeld => write!(f, "lock_held"),
            Self::Timeout => write!(f, "timeout"),
            Self::NotConfigured => write!(f, "not_configured"),
            Self::BackendError => write!(f, "backend_error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_roundtrip() {
        let codes = [
            ErrorCode::UnsupportedVersion,
            ErrorCode::InvalidPayload,
            ErrorCode::DeadlineExceeded,
            ErrorCode::MessageTooLarge,
            ErrorCode::FunctionNotFound,
            ErrorCode::FunctionFailed,
            ErrorCode::InvalidInput,
            ErrorCode::InternalError,
        ];

        for code in codes {
            let value = code.as_u8();
            let restored = ErrorCode::from_u8(value);
            assert_eq!(restored, Some(code));
        }
    }

    #[test]
    fn error_code_categories() {
        assert!(ErrorCode::UnsupportedVersion.is_protocol_error());
        assert!(ErrorCode::FunctionNotFound.is_function_error());
        assert!(ErrorCode::InternalError.is_internal_error());

        assert!(!ErrorCode::FunctionNotFound.is_protocol_error());
    }

    #[test]
    fn state_error_code_roundtrip() {
        let codes = [
            StateErrorCode::NotFound,
            StateErrorCode::Conflict,
            StateErrorCode::LockHeld,
            StateErrorCode::Timeout,
            StateErrorCode::NotConfigured,
            StateErrorCode::BackendError,
        ];

        for code in codes {
            let value = code.as_u8();
            let restored = StateErrorCode::from_u8(value);
            assert_eq!(restored, Some(code));
        }
    }

    #[test]
    fn error_code_display() {
        assert_eq!(ErrorCode::FunctionNotFound.to_string(), "function_not_found");
        assert_eq!(StateErrorCode::Conflict.to_string(), "conflict");
    }
}
