use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::error::LockError;
use crate::traits::LockOps;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageId(pub String);

impl MessageId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: MessageId,
    pub payload: Vec<u8>,
    pub attempt: u32,
    pub enqueued_at: SystemTime,
}

impl Message {
    pub fn new(id: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            id: MessageId::new(id),
            payload,
            attempt: 1,
            enqueued_at: SystemTime::now(),
        }
    }
}

pub struct LockGuard {
    resource: String,
    token: String,
    ops: Arc<dyn LockOps>,
    released: AtomicBool,
}

impl LockGuard {
    pub fn new(resource: String, token: String, ops: Arc<dyn LockOps>) -> Self {
        Self {
            resource,
            token,
            ops,
            released: AtomicBool::new(false),
        }
    }

    pub fn resource(&self) -> &str {
        &self.resource
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub async fn refresh(&self, ttl: Duration) -> Result<(), LockError> {
        if self.released.load(Ordering::SeqCst) {
            return Err(LockError::NotHeld);
        }
        self.ops.refresh(&self.resource, &self.token, ttl).await
    }

    pub async fn release(self) -> Result<(), LockError> {
        self.release_internal().await
    }

    async fn release_internal(&self) -> Result<(), LockError> {
        if self.released.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.ops.release(&self.resource, &self.token).await
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if !self.released.load(Ordering::SeqCst) {
            let ops = self.ops.clone();
            let resource = self.resource.clone();
            let token = self.token.clone();

            tokio::spawn(async move {
                if let Err(e) = ops.release(&resource, &token).await {
                    tracing::warn!(
                        resource = %resource,
                        error = %e,
                        "Failed to release lock on drop"
                    );
                }
            });
        }
    }
}

impl fmt::Debug for LockGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LockGuard")
            .field("resource", &self.resource)
            .field("released", &self.released.load(Ordering::SeqCst))
            .finish()
    }
}
