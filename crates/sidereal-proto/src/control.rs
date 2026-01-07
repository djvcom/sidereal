//! Control message types.

use rkyv::{Archive, Deserialize, Serialize};

/// Control messages for health checks and lifecycle management.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlMessage {
    /// Health check request.
    Ping,

    /// Health check response.
    Pong,

    /// Graceful shutdown request.
    Shutdown,

    /// Shutdown acknowledgement.
    ShutdownAck,
}

impl ControlMessage {
    /// Returns the response message for this control message.
    ///
    /// Returns `None` for response messages (Pong, ShutdownAck).
    #[must_use]
    pub fn response(&self) -> Option<Self> {
        match self {
            Self::Ping => Some(Self::Pong),
            Self::Shutdown => Some(Self::ShutdownAck),
            Self::Pong | Self::ShutdownAck => None,
        }
    }

    /// Checks if this is a request message.
    #[must_use]
    pub fn is_request(&self) -> bool {
        matches!(self, Self::Ping | Self::Shutdown)
    }

    /// Checks if this is a response message.
    #[must_use]
    pub fn is_response(&self) -> bool {
        matches!(self, Self::Pong | Self::ShutdownAck)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_pong() {
        let ping = ControlMessage::Ping;
        assert!(ping.is_request());
        assert!(!ping.is_response());
        assert_eq!(ping.response(), Some(ControlMessage::Pong));

        let pong = ControlMessage::Pong;
        assert!(!pong.is_request());
        assert!(pong.is_response());
        assert_eq!(pong.response(), None);
    }

    #[test]
    fn shutdown_ack() {
        let shutdown = ControlMessage::Shutdown;
        assert!(shutdown.is_request());
        assert_eq!(shutdown.response(), Some(ControlMessage::ShutdownAck));

        let ack = ControlMessage::ShutdownAck;
        assert!(ack.is_response());
        assert_eq!(ack.response(), None);
    }
}
