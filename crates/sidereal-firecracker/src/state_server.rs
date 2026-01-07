//! Vsock state server for handling state requests from guests.
//!
//! This module provides a server that handles state operations (KV, Queue, Lock)
//! from guests over vsock. The server dispatches requests to configured state
//! backends.

use std::sync::Arc;
use std::time::Duration;

use sidereal_proto::codec::{Codec, FrameHeader, MessageType, FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};
use sidereal_proto::{
    Envelope, QueueMessageData, StateErrorCode, StateMessage, StateRequest, StateResponse,
};
use sidereal_state::{
    KvBackend, KvError, LockBackend, LockError, LockOps, QueueBackend, QueueError,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

/// State backends available to the server.
pub struct StateBackends {
    pub kv: Option<Arc<dyn KvBackend>>,
    pub queue: Option<Arc<dyn QueueBackend>>,
    pub lock: Option<Arc<dyn LockBackend>>,
    pub lock_ops: Option<Arc<dyn LockOps>>,
}

impl StateBackends {
    /// Create a new empty backends container.
    pub fn new() -> Self {
        Self {
            kv: None,
            queue: None,
            lock: None,
            lock_ops: None,
        }
    }

    /// Set the KV backend.
    pub fn with_kv(mut self, kv: Arc<dyn KvBackend>) -> Self {
        self.kv = Some(kv);
        self
    }

    /// Set the queue backend.
    pub fn with_queue(mut self, queue: Arc<dyn QueueBackend>) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Set the lock backend.
    pub fn with_lock(mut self, lock: Arc<dyn LockBackend>, lock_ops: Arc<dyn LockOps>) -> Self {
        self.lock = Some(lock);
        self.lock_ops = Some(lock_ops);
        self
    }
}

impl Default for StateBackends {
    fn default() -> Self {
        Self::new()
    }
}

/// Handler for state requests from a single connection.
pub struct StateRequestHandler {
    backends: Arc<StateBackends>,
    codec: Mutex<Codec>,
}

impl StateRequestHandler {
    /// Create a new state request handler.
    pub fn new(backends: Arc<StateBackends>) -> Self {
        Self {
            backends,
            codec: Mutex::new(Codec::with_capacity(8192)),
        }
    }

    /// Handle a single connection, processing state requests until the connection closes.
    pub async fn handle_connection<S>(&self, mut stream: S) -> Result<(), std::io::Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        loop {
            // Read frame header
            let mut header_buf = [0u8; FRAME_HEADER_SIZE];
            match stream.read_exact(&mut header_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("State connection closed");
                    return Ok(());
                }
                Err(e) => return Err(e),
            }

            let header = FrameHeader::decode(&header_buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

            if header.message_type != MessageType::State {
                warn!("Received non-state message type: {:?}", header.message_type);
                continue;
            }

            let len = header.payload_len as usize;
            if len > MAX_MESSAGE_SIZE {
                error!("State request too large: {} bytes", len);
                continue;
            }

            // Read payload
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf).await?;

            // Decode envelope
            let envelope: Envelope<StateMessage> = Codec::decode(&buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

            // Handle request
            let response = match envelope.payload {
                StateMessage::Request(request) => {
                    let response = self.handle_request(request).await;
                    Envelope::response_to(&envelope.header, StateMessage::Response(response))
                }
                StateMessage::Response(_) => {
                    warn!("Received response instead of request");
                    continue;
                }
            };

            // Encode and send response
            let bytes = {
                let mut codec = self.codec.lock().await;
                codec
                    .encode(&response, MessageType::State)
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })?
                    .to_vec()
            };

            stream.write_all(&bytes).await?;
            stream.flush().await?;
        }
    }

    /// Handle a single state request.
    async fn handle_request(&self, request: StateRequest) -> StateResponse {
        match request {
            // KV operations
            StateRequest::KvGet { key } => self.handle_kv_get(&key).await,
            StateRequest::KvPut {
                key,
                value,
                ttl_secs,
            } => self.handle_kv_put(&key, value, ttl_secs).await,
            StateRequest::KvDelete { key } => self.handle_kv_delete(&key).await,
            StateRequest::KvExists { key } => self.handle_kv_exists(&key).await,
            StateRequest::KvList {
                prefix,
                limit,
                cursor,
            } => {
                self.handle_kv_list(&prefix, limit as usize, cursor.as_deref())
                    .await
            }
            StateRequest::KvCas { key, expected, new } => {
                self.handle_kv_cas(&key, expected, new).await
            }

            // Queue operations
            StateRequest::QueuePublish { queue, message } => {
                self.handle_queue_publish(&queue, message).await
            }
            StateRequest::QueueReceive {
                queue,
                visibility_timeout_secs,
            } => {
                self.handle_queue_receive(&queue, visibility_timeout_secs)
                    .await
            }
            StateRequest::QueueAck { queue, message_id } => {
                self.handle_queue_ack(&queue, &message_id).await
            }
            StateRequest::QueueNack { queue, message_id } => {
                self.handle_queue_nack(&queue, &message_id).await
            }

            // Lock operations
            StateRequest::LockAcquire { resource, ttl_secs } => {
                self.handle_lock_acquire(&resource, ttl_secs).await
            }
            StateRequest::LockTryAcquire { resource, ttl_secs } => {
                self.handle_lock_try_acquire(&resource, ttl_secs).await
            }
            StateRequest::LockRelease { resource, token } => {
                self.handle_lock_release(&resource, &token).await
            }
            StateRequest::LockRefresh {
                resource,
                token,
                ttl_secs,
            } => self.handle_lock_refresh(&resource, &token, ttl_secs).await,
        }
    }

    // KV handlers

    async fn handle_kv_get(&self, key: &str) -> StateResponse {
        let Some(kv) = &self.backends.kv else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "KV backend not configured",
            );
        };

        match kv.get(key).await {
            Ok(value) => StateResponse::KvValue(value),
            Err(e) => kv_error_to_response(e),
        }
    }

    async fn handle_kv_put(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> StateResponse {
        let Some(kv) = &self.backends.kv else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "KV backend not configured",
            );
        };

        let ttl = ttl_secs.map(Duration::from_secs);
        match kv.put(key, &value, ttl).await {
            Ok(()) => StateResponse::KvSuccess(true),
            Err(e) => kv_error_to_response(e),
        }
    }

    async fn handle_kv_delete(&self, key: &str) -> StateResponse {
        let Some(kv) = &self.backends.kv else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "KV backend not configured",
            );
        };

        match kv.delete(key).await {
            Ok(deleted) => StateResponse::KvSuccess(deleted),
            Err(e) => kv_error_to_response(e),
        }
    }

    async fn handle_kv_exists(&self, key: &str) -> StateResponse {
        let Some(kv) = &self.backends.kv else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "KV backend not configured",
            );
        };

        match kv.exists(key).await {
            Ok(exists) => StateResponse::KvSuccess(exists),
            Err(e) => kv_error_to_response(e),
        }
    }

    async fn handle_kv_list(
        &self,
        prefix: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> StateResponse {
        let Some(kv) = &self.backends.kv else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "KV backend not configured",
            );
        };

        match kv.list(prefix, limit, cursor).await {
            Ok((keys, next_cursor)) => StateResponse::KvList {
                keys,
                cursor: next_cursor,
            },
            Err(e) => kv_error_to_response(e),
        }
    }

    async fn handle_kv_cas(
        &self,
        key: &str,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    ) -> StateResponse {
        let Some(kv) = &self.backends.kv else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "KV backend not configured",
            );
        };

        match kv.cas(key, expected.as_deref(), &new).await {
            Ok(success) => StateResponse::KvSuccess(success),
            Err(e) => kv_error_to_response(e),
        }
    }

    // Queue handlers

    async fn handle_queue_publish(&self, queue: &str, message: Vec<u8>) -> StateResponse {
        let Some(q) = &self.backends.queue else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "Queue backend not configured",
            );
        };

        match q.publish(queue, &message).await {
            Ok(id) => StateResponse::QueuePublished {
                message_id: id.to_string(),
            },
            Err(e) => queue_error_to_response(e),
        }
    }

    async fn handle_queue_receive(
        &self,
        queue: &str,
        visibility_timeout_secs: u64,
    ) -> StateResponse {
        let Some(q) = &self.backends.queue else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "Queue backend not configured",
            );
        };

        let timeout = Duration::from_secs(visibility_timeout_secs);
        match q.receive(queue, timeout).await {
            Ok(None) => StateResponse::QueueMessage(None),
            Ok(Some(msg)) => {
                let enqueued_at_ns = msg
                    .enqueued_at
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);

                StateResponse::QueueMessage(Some(QueueMessageData {
                    id: msg.id.to_string(),
                    payload: msg.payload,
                    attempt: msg.attempt,
                    enqueued_at_ns,
                }))
            }
            Err(e) => queue_error_to_response(e),
        }
    }

    async fn handle_queue_ack(&self, queue: &str, message_id: &str) -> StateResponse {
        let Some(q) = &self.backends.queue else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "Queue backend not configured",
            );
        };

        let id = sidereal_state::MessageId::new(message_id);
        match q.ack(queue, &id).await {
            Ok(()) => StateResponse::QueueSuccess,
            Err(e) => queue_error_to_response(e),
        }
    }

    async fn handle_queue_nack(&self, queue: &str, message_id: &str) -> StateResponse {
        let Some(q) = &self.backends.queue else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "Queue backend not configured",
            );
        };

        let id = sidereal_state::MessageId::new(message_id);
        match q.nack(queue, &id).await {
            Ok(()) => StateResponse::QueueSuccess,
            Err(e) => queue_error_to_response(e),
        }
    }

    // Lock handlers

    async fn handle_lock_acquire(&self, resource: &str, ttl_secs: u64) -> StateResponse {
        let Some(lock) = &self.backends.lock else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "Lock backend not configured",
            );
        };

        let ttl = Duration::from_secs(ttl_secs);
        match lock.acquire(resource, ttl).await {
            Ok(guard) => StateResponse::LockAcquired {
                token: guard.token().to_string(),
            },
            Err(e) => lock_error_to_response(e),
        }
    }

    async fn handle_lock_try_acquire(&self, resource: &str, ttl_secs: u64) -> StateResponse {
        let Some(lock) = &self.backends.lock else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "Lock backend not configured",
            );
        };

        let ttl = Duration::from_secs(ttl_secs);
        match lock.try_acquire(resource, ttl).await {
            Ok(Some(guard)) => StateResponse::LockTryResult {
                token: Some(guard.token().to_string()),
            },
            Ok(None) => StateResponse::LockTryResult { token: None },
            Err(e) => lock_error_to_response(e),
        }
    }

    async fn handle_lock_release(&self, resource: &str, token: &str) -> StateResponse {
        let Some(lock_ops) = &self.backends.lock_ops else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "Lock backend not configured",
            );
        };

        match lock_ops.release(resource, token).await {
            Ok(()) => StateResponse::LockSuccess,
            Err(e) => lock_error_to_response(e),
        }
    }

    async fn handle_lock_refresh(
        &self,
        resource: &str,
        token: &str,
        ttl_secs: u64,
    ) -> StateResponse {
        let Some(lock_ops) = &self.backends.lock_ops else {
            return StateResponse::error(
                StateErrorCode::NotConfigured,
                "Lock backend not configured",
            );
        };

        let ttl = Duration::from_secs(ttl_secs);
        match lock_ops.refresh(resource, token, ttl).await {
            Ok(()) => StateResponse::LockSuccess,
            Err(e) => lock_error_to_response(e),
        }
    }
}

fn kv_error_to_response(e: KvError) -> StateResponse {
    match e {
        KvError::Timeout => StateResponse::error(StateErrorCode::Timeout, "Operation timed out"),
        KvError::Conflict => {
            StateResponse::error(StateErrorCode::Conflict, "CAS conflict: value changed")
        }
        KvError::Connection(msg) => StateResponse::error(StateErrorCode::BackendError, msg),
        KvError::Serialisation(msg) => StateResponse::error(StateErrorCode::BackendError, msg),
        KvError::Backend(msg) => StateResponse::error(StateErrorCode::BackendError, msg),
    }
}

fn queue_error_to_response(e: QueueError) -> StateResponse {
    match e {
        QueueError::Timeout => StateResponse::error(StateErrorCode::Timeout, "Operation timed out"),
        QueueError::QueueNotFound(name) => {
            StateResponse::error(StateErrorCode::NotFound, format!("Queue not found: {name}"))
        }
        QueueError::MessageNotFound(id) => {
            StateResponse::error(StateErrorCode::NotFound, format!("Message not found: {id}"))
        }
        QueueError::Connection(msg) => StateResponse::error(StateErrorCode::BackendError, msg),
        QueueError::Serialisation(msg) => StateResponse::error(StateErrorCode::BackendError, msg),
        QueueError::Backend(msg) => StateResponse::error(StateErrorCode::BackendError, msg),
    }
}

fn lock_error_to_response(e: LockError) -> StateResponse {
    match e {
        LockError::Timeout => StateResponse::error(StateErrorCode::Timeout, "Operation timed out"),
        LockError::AlreadyHeld => {
            StateResponse::error(StateErrorCode::LockHeld, "Lock already held")
        }
        LockError::NotHeld => {
            StateResponse::error(StateErrorCode::NotFound, "Lock not held or expired")
        }
        LockError::Connection(msg) => StateResponse::error(StateErrorCode::BackendError, msg),
        LockError::Backend(msg) => StateResponse::error(StateErrorCode::BackendError, msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backends_builder() {
        let backends = StateBackends::new();
        assert!(backends.kv.is_none());
        assert!(backends.queue.is_none());
        assert!(backends.lock.is_none());
    }

    #[test]
    fn error_mapping() {
        let kv_timeout = kv_error_to_response(KvError::Timeout);
        assert!(matches!(
            kv_timeout,
            StateResponse::Error {
                code: StateErrorCode::Timeout,
                ..
            }
        ));

        let queue_not_found = queue_error_to_response(QueueError::QueueNotFound("test".into()));
        assert!(matches!(
            queue_not_found,
            StateResponse::Error {
                code: StateErrorCode::NotFound,
                ..
            }
        ));

        let lock_held = lock_error_to_response(LockError::AlreadyHeld);
        assert!(matches!(
            lock_held,
            StateResponse::Error {
                code: StateErrorCode::LockHeld,
                ..
            }
        ));
    }
}
