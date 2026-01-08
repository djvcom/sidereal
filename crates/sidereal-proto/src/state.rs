//! State operation message types.

use rkyv::{Archive, Deserialize, Serialize};

use crate::error::StateErrorCode;

/// State operation messages.
///
/// Used for worker → state coordinator communication via vsock.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StateMessage {
    /// State operation request.
    Request(StateRequest),

    /// State operation response.
    Response(StateResponse),
}

/// State operation requests.
///
/// Maps to the `KvBackend`, `QueueBackend`, and `LockBackend` trait methods.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StateRequest {
    // KV operations
    /// Get a value by key.
    KvGet { key: String },

    /// Put a value with optional TTL.
    KvPut {
        key: String,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    },

    /// Delete a key.
    KvDelete { key: String },

    /// Check if a key exists.
    KvExists { key: String },

    /// List keys with a prefix.
    KvList {
        prefix: String,
        limit: u32,
        cursor: Option<String>,
    },

    /// Compare-and-swap operation.
    KvCas {
        key: String,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    },

    // Queue operations
    /// Publish a message to a queue.
    QueuePublish { queue: String, message: Vec<u8> },

    /// Receive a message from a queue.
    QueueReceive {
        queue: String,
        visibility_timeout_secs: u64,
    },

    /// Acknowledge a message (remove from queue).
    QueueAck { queue: String, message_id: String },

    /// Negative acknowledge (return to queue).
    QueueNack { queue: String, message_id: String },

    // Lock operations
    /// Acquire a lock (blocking).
    LockAcquire { resource: String, ttl_secs: u64 },

    /// Try to acquire a lock (non-blocking).
    LockTryAcquire { resource: String, ttl_secs: u64 },

    /// Release a held lock.
    LockRelease { resource: String, token: String },

    /// Refresh a lock's TTL.
    LockRefresh {
        resource: String,
        token: String,
        ttl_secs: u64,
    },
}

/// State operation responses.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StateResponse {
    // KV responses
    /// Result of KvGet.
    KvValue(Option<Vec<u8>>),

    /// Result of KvPut, KvDelete, KvCas.
    KvSuccess(bool),

    /// Result of KvList.
    KvList {
        keys: Vec<String>,
        cursor: Option<String>,
    },

    // Queue responses
    /// Result of QueuePublish.
    QueuePublished { message_id: String },

    /// Result of QueueReceive.
    QueueMessage(Option<QueueMessageData>),

    /// Result of QueueAck, QueueNack.
    QueueSuccess,

    // Lock responses
    /// Result of LockAcquire.
    LockAcquired { token: String },

    /// Result of LockTryAcquire.
    LockTryResult { token: Option<String> },

    /// Result of LockRelease, LockRefresh.
    LockSuccess,

    // Error response
    /// Error from any operation.
    Error {
        code: StateErrorCode,
        message: String,
    },
}

impl StateResponse {
    /// Creates an error response.
    #[must_use]
    pub fn error(code: StateErrorCode, message: impl Into<String>) -> Self {
        Self::Error {
            code,
            message: message.into(),
        }
    }

    /// Checks if this is an error response.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }

    /// Gets the error details if this is an error response.
    #[must_use]
    pub fn as_error(&self) -> Option<(StateErrorCode, &str)> {
        match self {
            Self::Error { code, message } => Some((*code, message.as_str())),
            _ => None,
        }
    }
}

/// Queue message data returned by QueueReceive.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueueMessageData {
    /// Unique message ID.
    pub id: String,

    /// Message payload.
    pub payload: Vec<u8>,

    /// Number of delivery attempts.
    pub attempt: u32,

    /// Timestamp when message was enqueued (nanoseconds since epoch).
    pub enqueued_at_ns: u64,
}

impl QueueMessageData {
    /// Creates a new queue message data.
    #[must_use]
    pub fn new(id: impl Into<String>, payload: Vec<u8>, attempt: u32, enqueued_at_ns: u64) -> Self {
        Self {
            id: id.into(),
            payload,
            attempt,
            enqueued_at_ns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_request_kv_operations() {
        let get = StateRequest::KvGet {
            key: "user:123".into(),
        };
        let put = StateRequest::KvPut {
            key: "user:123".into(),
            value: b"data".to_vec(),
            ttl_secs: Some(3600),
        };
        let cas = StateRequest::KvCas {
            key: "counter".into(),
            expected: Some(b"1".to_vec()),
            new: b"2".to_vec(),
        };

        match get {
            StateRequest::KvGet { key } => assert_eq!(key, "user:123"),
            _ => panic!("expected KvGet"),
        }

        match put {
            StateRequest::KvPut { ttl_secs, .. } => assert_eq!(ttl_secs, Some(3600)),
            _ => panic!("expected KvPut"),
        }

        match cas {
            StateRequest::KvCas { expected, .. } => assert_eq!(expected, Some(b"1".to_vec())),
            _ => panic!("expected KvCas"),
        }
    }

    #[test]
    fn state_request_queue_operations() {
        let publish = StateRequest::QueuePublish {
            queue: "tasks".into(),
            message: b"task data".to_vec(),
        };
        let receive = StateRequest::QueueReceive {
            queue: "tasks".into(),
            visibility_timeout_secs: 30,
        };

        match publish {
            StateRequest::QueuePublish { queue, .. } => assert_eq!(queue, "tasks"),
            _ => panic!("expected QueuePublish"),
        }

        match receive {
            StateRequest::QueueReceive {
                visibility_timeout_secs,
                ..
            } => assert_eq!(visibility_timeout_secs, 30),
            _ => panic!("expected QueueReceive"),
        }
    }

    #[test]
    fn state_request_lock_operations() {
        let acquire = StateRequest::LockAcquire {
            resource: "orders:456".into(),
            ttl_secs: 60,
        };
        let release = StateRequest::LockRelease {
            resource: "orders:456".into(),
            token: "lock-token-abc".into(),
        };

        match acquire {
            StateRequest::LockAcquire { ttl_secs, .. } => assert_eq!(ttl_secs, 60),
            _ => panic!("expected LockAcquire"),
        }

        match release {
            StateRequest::LockRelease { token, .. } => assert_eq!(token, "lock-token-abc"),
            _ => panic!("expected LockRelease"),
        }
    }

    #[test]
    fn state_response_kv() {
        let found = StateResponse::KvValue(Some(b"data".to_vec()));
        let not_found = StateResponse::KvValue(None);
        let success = StateResponse::KvSuccess(true);

        match found {
            StateResponse::KvValue(Some(v)) => assert_eq!(v, b"data"),
            _ => panic!("expected KvValue with data"),
        }

        match not_found {
            StateResponse::KvValue(None) => {}
            _ => panic!("expected KvValue with None"),
        }

        match success {
            StateResponse::KvSuccess(s) => assert!(s),
            _ => panic!("expected KvSuccess"),
        }
    }

    #[test]
    fn state_response_error() {
        let error = StateResponse::error(StateErrorCode::NotFound, "key not found");
        assert!(error.is_error());

        let (code, msg) = error.as_error().unwrap();
        assert_eq!(code, StateErrorCode::NotFound);
        assert_eq!(msg, "key not found");
    }

    #[test]
    fn queue_message_data() {
        let msg = QueueMessageData::new("msg-123", b"payload".to_vec(), 1, 1234567890_000_000_000);
        assert_eq!(msg.id, "msg-123");
        assert_eq!(msg.payload, b"payload");
        assert_eq!(msg.attempt, 1);
    }
}
