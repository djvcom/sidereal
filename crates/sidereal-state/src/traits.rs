use std::time::Duration;

use async_trait::async_trait;

use crate::error::{KvError, LockError, QueueError};
use crate::types::{LockGuard, Message, MessageId};

#[async_trait]
pub trait KvBackend: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError>;

    async fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KvError>;

    async fn delete(&self, key: &str) -> Result<bool, KvError>;

    async fn exists(&self, key: &str) -> Result<bool, KvError>;

    async fn list(
        &self,
        prefix: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), KvError>;

    async fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool, KvError>;
}

#[async_trait]
pub trait QueueBackend: Send + Sync {
    async fn publish(&self, queue: &str, message: &[u8]) -> Result<MessageId, QueueError>;

    async fn receive(
        &self,
        queue: &str,
        visibility_timeout: Duration,
    ) -> Result<Option<Message>, QueueError>;

    async fn ack(&self, queue: &str, message_id: &MessageId) -> Result<(), QueueError>;

    async fn nack(&self, queue: &str, message_id: &MessageId) -> Result<(), QueueError>;
}

#[async_trait]
pub trait LockBackend: Send + Sync {
    async fn acquire(&self, resource: &str, ttl: Duration) -> Result<LockGuard, LockError>;

    async fn try_acquire(
        &self,
        resource: &str,
        ttl: Duration,
    ) -> Result<Option<LockGuard>, LockError>;
}

#[async_trait]
pub trait LockOps: Send + Sync {
    async fn release(&self, resource: &str, token: &str) -> Result<(), LockError>;

    async fn refresh(&self, resource: &str, token: &str, ttl: Duration) -> Result<(), LockError>;
}
