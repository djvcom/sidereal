//! Vsock-based state client for Firecracker guest communication.
//!
//! This module provides state backend implementations that communicate with
//! a host-side state server over vsock. Used when running inside a Firecracker VM.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sidereal_proto::codec::{Codec, FrameHeader, MessageType, FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};
use sidereal_proto::ports::STATE as STATE_PORT;
use sidereal_proto::{Envelope, StateErrorCode, StateMessage, StateRequest, StateResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_vsock::{VsockAddr, VsockStream, VMADDR_CID_HOST};

use crate::error::{KvError, LockError, QueueError};
use crate::traits::{KvBackend, LockBackend, LockOps, QueueBackend};
use crate::types::{LockGuard, Message, MessageId};

/// State client that communicates with the host over vsock.
///
/// This client implements all state backend traits by sending requests
/// to a VsockStateServer running on the host.
pub struct VsockStateClient {
    inner: Arc<VsockStateClientInner>,
}

struct VsockStateClientInner {
    stream: Mutex<VsockStream>,
    codec: Mutex<Codec>,
}

impl VsockStateClient {
    /// Connect to the host state server.
    pub async fn connect() -> Result<Self, std::io::Error> {
        Self::connect_to(VMADDR_CID_HOST, STATE_PORT).await
    }

    /// Connect to a specific CID and port (for testing).
    pub async fn connect_to(cid: u32, port: u32) -> Result<Self, std::io::Error> {
        let addr = VsockAddr::new(cid, port);
        let stream = VsockStream::connect(addr).await?;

        Ok(Self {
            inner: Arc::new(VsockStateClientInner {
                stream: Mutex::new(stream),
                codec: Mutex::new(Codec::with_capacity(8192)),
            }),
        })
    }

    async fn send_request(&self, request: StateRequest) -> Result<StateResponse, std::io::Error> {
        let envelope = Envelope::new(StateMessage::Request(request));

        let bytes = {
            let mut codec = self.inner.codec.lock().await;
            codec
                .encode(&envelope, MessageType::State)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
                .to_vec()
        };

        let mut stream = self.inner.stream.lock().await;

        stream.write_all(&bytes).await?;
        stream.flush().await?;

        let mut header_buf = [0u8; FRAME_HEADER_SIZE];
        stream.read_exact(&mut header_buf).await?;

        let header = FrameHeader::decode(&header_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        #[allow(clippy::as_conversions)]
        if header.payload_len as usize > MAX_MESSAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Response too large: {} bytes", header.payload_len),
            ));
        }

        #[allow(clippy::as_conversions)]
        let mut payload_buf = vec![0u8; header.payload_len as usize];
        stream.read_exact(&mut payload_buf).await?;

        let response_envelope: Envelope<StateMessage> = Codec::decode(&payload_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        match response_envelope.payload {
            StateMessage::Response(response) => Ok(response),
            StateMessage::Request(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Received request instead of response",
            )),
        }
    }
}

impl Clone for VsockStateClient {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

fn state_error_to_kv(code: StateErrorCode, message: String) -> KvError {
    match code {
        StateErrorCode::Conflict => KvError::Conflict,
        StateErrorCode::Timeout => KvError::Timeout,
        StateErrorCode::NotFound
        | StateErrorCode::NotConfigured
        | StateErrorCode::BackendError
        | StateErrorCode::LockHeld => KvError::Backend(message),
    }
}

fn state_error_to_queue(code: StateErrorCode, message: String) -> QueueError {
    match code {
        StateErrorCode::NotFound => QueueError::QueueNotFound(message),
        StateErrorCode::Timeout => QueueError::Timeout,
        StateErrorCode::NotConfigured
        | StateErrorCode::BackendError
        | StateErrorCode::Conflict
        | StateErrorCode::LockHeld => QueueError::Backend(message),
    }
}

fn state_error_to_lock(code: StateErrorCode, message: String) -> LockError {
    match code {
        StateErrorCode::LockHeld => LockError::AlreadyHeld,
        StateErrorCode::NotFound => LockError::NotHeld,
        StateErrorCode::Timeout => LockError::Timeout,
        StateErrorCode::NotConfigured | StateErrorCode::BackendError | StateErrorCode::Conflict => {
            LockError::Backend(message)
        }
    }
}

#[async_trait]
impl KvBackend for VsockStateClient {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        let response = self
            .send_request(StateRequest::KvGet {
                key: key.to_owned(),
            })
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        match response {
            StateResponse::KvValue(value) => Ok(value),
            StateResponse::Error { code, message } => Err(state_error_to_kv(code, message)),
            other => Err(KvError::Backend(format!("Unexpected response: {other:?}"))),
        }
    }

    async fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KvError> {
        let response = self
            .send_request(StateRequest::KvPut {
                key: key.to_owned(),
                value: value.to_vec(),
                ttl_secs: ttl.map(|d| d.as_secs()),
            })
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        match response {
            StateResponse::KvSuccess(true) => Ok(()),
            StateResponse::KvSuccess(false) => Err(KvError::Backend("Put operation failed".into())),
            StateResponse::Error { code, message } => Err(state_error_to_kv(code, message)),
            other => Err(KvError::Backend(format!("Unexpected response: {other:?}"))),
        }
    }

    async fn delete(&self, key: &str) -> Result<bool, KvError> {
        let response = self
            .send_request(StateRequest::KvDelete {
                key: key.to_owned(),
            })
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        match response {
            StateResponse::KvSuccess(deleted) => Ok(deleted),
            StateResponse::Error { code, message } => Err(state_error_to_kv(code, message)),
            other => Err(KvError::Backend(format!("Unexpected response: {other:?}"))),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        let response = self
            .send_request(StateRequest::KvExists {
                key: key.to_owned(),
            })
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        match response {
            StateResponse::KvSuccess(exists) => Ok(exists),
            StateResponse::Error { code, message } => Err(state_error_to_kv(code, message)),
            other => Err(KvError::Backend(format!("Unexpected response: {other:?}"))),
        }
    }

    async fn list(
        &self,
        prefix: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), KvError> {
        #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
        let limit_u32 = limit as u32;
        let response = self
            .send_request(StateRequest::KvList {
                prefix: prefix.to_owned(),
                limit: limit_u32,
                cursor: cursor.map(String::from),
            })
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        match response {
            StateResponse::KvList { keys, cursor } => Ok((keys, cursor)),
            StateResponse::Error { code, message } => Err(state_error_to_kv(code, message)),
            other => Err(KvError::Backend(format!("Unexpected response: {other:?}"))),
        }
    }

    async fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool, KvError> {
        let response = self
            .send_request(StateRequest::KvCas {
                key: key.to_owned(),
                expected: expected.map(<[u8]>::to_vec),
                new: new.to_vec(),
            })
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        match response {
            StateResponse::KvSuccess(success) => Ok(success),
            StateResponse::Error { code, message } => Err(state_error_to_kv(code, message)),
            other => Err(KvError::Backend(format!("Unexpected response: {other:?}"))),
        }
    }
}

#[async_trait]
impl QueueBackend for VsockStateClient {
    async fn publish(&self, queue: &str, message: &[u8]) -> Result<MessageId, QueueError> {
        let response = self
            .send_request(StateRequest::QueuePublish {
                queue: queue.to_owned(),
                message: message.to_vec(),
            })
            .await
            .map_err(|e| QueueError::Connection(e.to_string()))?;

        match response {
            StateResponse::QueuePublished { message_id } => Ok(MessageId::new(message_id)),
            StateResponse::Error { code, message } => Err(state_error_to_queue(code, message)),
            other => Err(QueueError::Backend(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }

    async fn receive(
        &self,
        queue: &str,
        visibility_timeout: Duration,
    ) -> Result<Option<Message>, QueueError> {
        let response = self
            .send_request(StateRequest::QueueReceive {
                queue: queue.to_owned(),
                visibility_timeout_secs: visibility_timeout.as_secs(),
            })
            .await
            .map_err(|e| QueueError::Connection(e.to_string()))?;

        match response {
            StateResponse::QueueMessage(None) => Ok(None),
            StateResponse::QueueMessage(Some(data)) => {
                let enqueued_at =
                    std::time::UNIX_EPOCH + std::time::Duration::from_nanos(data.enqueued_at_ns);

                Ok(Some(Message {
                    id: MessageId::new(data.id),
                    payload: data.payload,
                    attempt: data.attempt,
                    enqueued_at,
                }))
            }
            StateResponse::Error { code, message } => Err(state_error_to_queue(code, message)),
            other => Err(QueueError::Backend(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }

    async fn ack(&self, queue: &str, message_id: &MessageId) -> Result<(), QueueError> {
        let response = self
            .send_request(StateRequest::QueueAck {
                queue: queue.to_owned(),
                message_id: message_id.to_string(),
            })
            .await
            .map_err(|e| QueueError::Connection(e.to_string()))?;

        match response {
            StateResponse::QueueSuccess => Ok(()),
            StateResponse::Error { code, message } => Err(state_error_to_queue(code, message)),
            other => Err(QueueError::Backend(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }

    async fn nack(&self, queue: &str, message_id: &MessageId) -> Result<(), QueueError> {
        let response = self
            .send_request(StateRequest::QueueNack {
                queue: queue.to_owned(),
                message_id: message_id.to_string(),
            })
            .await
            .map_err(|e| QueueError::Connection(e.to_string()))?;

        match response {
            StateResponse::QueueSuccess => Ok(()),
            StateResponse::Error { code, message } => Err(state_error_to_queue(code, message)),
            other => Err(QueueError::Backend(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl LockOps for VsockStateClient {
    async fn release(&self, resource: &str, token: &str) -> Result<(), LockError> {
        let response = self
            .send_request(StateRequest::LockRelease {
                resource: resource.to_owned(),
                token: token.to_owned(),
            })
            .await
            .map_err(|e| LockError::Connection(e.to_string()))?;

        match response {
            StateResponse::LockSuccess => Ok(()),
            StateResponse::Error { code, message } => Err(state_error_to_lock(code, message)),
            other => Err(LockError::Backend(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }

    async fn refresh(&self, resource: &str, token: &str, ttl: Duration) -> Result<(), LockError> {
        let response = self
            .send_request(StateRequest::LockRefresh {
                resource: resource.to_owned(),
                token: token.to_owned(),
                ttl_secs: ttl.as_secs(),
            })
            .await
            .map_err(|e| LockError::Connection(e.to_string()))?;

        match response {
            StateResponse::LockSuccess => Ok(()),
            StateResponse::Error { code, message } => Err(state_error_to_lock(code, message)),
            other => Err(LockError::Backend(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl LockBackend for VsockStateClient {
    async fn acquire(&self, resource: &str, ttl: Duration) -> Result<LockGuard, LockError> {
        let response = self
            .send_request(StateRequest::LockAcquire {
                resource: resource.to_owned(),
                ttl_secs: ttl.as_secs(),
            })
            .await
            .map_err(|e| LockError::Connection(e.to_string()))?;

        match response {
            StateResponse::LockAcquired { token } => {
                #[allow(clippy::as_conversions)]
                let guard = LockGuard::new(
                    resource.to_owned(),
                    token,
                    Arc::new(self.clone()) as Arc<dyn LockOps>,
                );
                Ok(guard)
            }
            StateResponse::Error { code, message } => Err(state_error_to_lock(code, message)),
            other => Err(LockError::Backend(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }

    async fn try_acquire(
        &self,
        resource: &str,
        ttl: Duration,
    ) -> Result<Option<LockGuard>, LockError> {
        let response = self
            .send_request(StateRequest::LockTryAcquire {
                resource: resource.to_owned(),
                ttl_secs: ttl.as_secs(),
            })
            .await
            .map_err(|e| LockError::Connection(e.to_string()))?;

        match response {
            StateResponse::LockTryResult { token: Some(token) } => {
                #[allow(clippy::as_conversions)]
                let guard = LockGuard::new(
                    resource.to_owned(),
                    token,
                    Arc::new(self.clone()) as Arc<dyn LockOps>,
                );
                Ok(Some(guard))
            }
            StateResponse::LockTryResult { token: None } => Ok(None),
            StateResponse::Error { code, message } => Err(state_error_to_lock(code, message)),
            other => Err(LockError::Backend(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }
}

/// Provider wrapper for VsockStateClient that can be used with StateProvider.
pub struct VsockLockProvider {
    client: VsockStateClient,
}

impl VsockLockProvider {
    pub const fn new(client: VsockStateClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl LockBackend for VsockLockProvider {
    async fn acquire(&self, resource: &str, ttl: Duration) -> Result<LockGuard, LockError> {
        self.client.acquire(resource, ttl).await
    }

    async fn try_acquire(
        &self,
        resource: &str,
        ttl: Duration,
    ) -> Result<Option<LockGuard>, LockError> {
        self.client.try_acquire(resource, ttl).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_error_code_mapping() {
        let kv_conflict = state_error_to_kv(StateErrorCode::Conflict, "test".into());
        assert!(matches!(kv_conflict, KvError::Conflict));

        let queue_timeout = state_error_to_queue(StateErrorCode::Timeout, "test".into());
        assert!(matches!(queue_timeout, QueueError::Timeout));

        let lock_held = state_error_to_lock(StateErrorCode::LockHeld, "test".into());
        assert!(matches!(lock_held, LockError::AlreadyHeld));
    }
}
