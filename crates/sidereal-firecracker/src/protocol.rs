//! Wire protocol for host-guest communication over vsock.
//!
//! Messages are length-prefixed JSON:
//! - 4 bytes: message length (big-endian u32)
//! - N bytes: JSON-encoded message

use serde::{Deserialize, Serialize};

/// Default vsock port for Sidereal runtime communication.
pub const VSOCK_PORT: u32 = 1024;

/// Request from host to guest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GuestRequest {
    /// Health check / readiness probe.
    #[serde(rename = "ping")]
    Ping,

    /// Invoke a function.
    #[serde(rename = "invoke")]
    Invoke {
        function_name: String,
        payload: Vec<u8>,
        trace_id: String,
    },

    /// Graceful shutdown request.
    #[serde(rename = "shutdown")]
    Shutdown,
}

/// Response from guest to host.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GuestResponse {
    /// Pong response to ping.
    #[serde(rename = "pong")]
    Pong,

    /// Successful function result.
    #[serde(rename = "result")]
    Result {
        status: u16,
        body: Vec<u8>,
        trace_id: String,
    },

    /// Error response.
    #[serde(rename = "error")]
    Error { message: String, trace_id: String },

    /// Shutdown acknowledgement.
    #[serde(rename = "shutdown_ack")]
    ShutdownAck,
}

impl GuestRequest {
    pub fn ping() -> Self {
        Self::Ping
    }

    pub fn invoke(function_name: impl Into<String>, payload: Vec<u8>, trace_id: impl Into<String>) -> Self {
        Self::Invoke {
            function_name: function_name.into(),
            payload,
            trace_id: trace_id.into(),
        }
    }

    pub fn shutdown() -> Self {
        Self::Shutdown
    }
}

impl GuestResponse {
    pub fn pong() -> Self {
        Self::Pong
    }

    pub fn result(status: u16, body: Vec<u8>, trace_id: impl Into<String>) -> Self {
        Self::Result {
            status,
            body,
            trace_id: trace_id.into(),
        }
    }

    pub fn error(message: impl Into<String>, trace_id: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
            trace_id: trace_id.into(),
        }
    }

    pub fn shutdown_ack() -> Self {
        Self::ShutdownAck
    }
}
