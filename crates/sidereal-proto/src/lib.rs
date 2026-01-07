//! Wire protocol types for Sidereal inter-component communication.
//!
//! This crate provides a unified, high-performance wire protocol based on rkyv
//! zero-copy serialisation. It supports:
//!
//! - Function invocation (gateway → worker, worker → worker)
//! - State operations (KV, Queue, Lock via vsock)
//! - Control messages (ping, shutdown)
//! - OpenTelemetry context propagation via generic metadata
//!
//! # Wire Format
//!
//! All messages use a common envelope format with an 8-byte frame header:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │              Frame Header (8 bytes, fixed)               │
//! ├──────────────┬──────────────┬────────────────────────────┤
//! │  Version (2) │ Msg Type (2) │    Payload Length (4)      │
//! ├──────────────┴──────────────┴────────────────────────────┤
//! │                 rkyv-serialised Envelope                  │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use sidereal_proto::{Envelope, EnvelopeHeader, FunctionMessage, InvokeRequest};
//!
//! let envelope = Envelope {
//!     header: EnvelopeHeader::new(),
//!     payload: FunctionMessage::Invoke(InvokeRequest {
//!         function_name: "greet".into(),
//!         payload: b"hello".to_vec(),
//!         http_method: Some("POST".into()),
//!         http_headers: vec![],
//!     }),
//! };
//! ```

pub mod codec;
mod control;
mod envelope;
mod error;
mod function;
mod state;
mod types;

#[cfg(feature = "otel")]
mod propagation;

// Re-export core types
pub use codec::{Codec, FrameHeader, MessageType, CURRENT_VERSION, FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};
pub use control::ControlMessage;
pub use envelope::{Envelope, EnvelopeHeader};
pub use error::{ErrorCode, ProtocolError, StateErrorCode};
pub use function::{FunctionMessage, InvokeRequest, InvokeResponse};
pub use state::{QueueMessageData, StateMessage, StateRequest, StateResponse};
pub use types::CorrelationId;

#[cfg(feature = "otel")]
pub use propagation::{MetadataCarrier, MetadataExtractor};

/// Protocol version constants.
pub mod version {
    /// Current protocol version.
    pub const CURRENT: u16 = 1;

    /// Minimum supported protocol version.
    pub const MIN_SUPPORTED: u16 = 1;
}

/// vsock port assignments.
pub mod ports {
    /// Function invocation port.
    pub const FUNCTION: u32 = 1024;

    /// State operations port.
    pub const STATE: u32 = 1025;

    /// Control messages port.
    pub const CONTROL: u32 = 1026;
}
