use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::error::{KvError, LockError, QueueError};
use crate::traits::{KvBackend, LockBackend, LockOps, QueueBackend};
use crate::types::{LockGuard, Message, MessageId};

#[derive(Debug, Clone)]
struct KvEntry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryKv {
    data: Arc<RwLock<HashMap<String, KvEntry>>>,
}

impl MemoryKv {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl KvBackend for MemoryKv {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(entry) => {
                if let Some(expires_at) = entry.expires_at {
                    if Instant::now() >= expires_at {
                        drop(data);
                        let mut data = self.data.write().await;
                        data.remove(key);
                        return Ok(None);
                    }
                }
                Ok(Some(entry.value.clone()))
            }
            None => Ok(None),
        }
    }

    async fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KvError> {
        let mut data = self.data.write().await;
        let expires_at = ttl.map(|d| Instant::now() + d);
        data.insert(
            key.to_string(),
            KvEntry {
                value: value.to_vec(),
                expires_at,
            },
        );
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, KvError> {
        let mut data = self.data.write().await;
        Ok(data.remove(key).is_some())
    }

    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(entry) => {
                if let Some(expires_at) = entry.expires_at {
                    if Instant::now() >= expires_at {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn list(
        &self,
        prefix: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), KvError> {
        let data = self.data.read().await;
        let now = Instant::now();

        let mut keys: Vec<_> = data
            .iter()
            .filter(|(k, entry)| {
                k.starts_with(prefix)
                    && entry.expires_at.map_or(true, |exp| now < exp)
                    && cursor.map_or(true, |c| k.as_str() > c)
            })
            .map(|(k, _)| k.clone())
            .collect();

        keys.sort();
        keys.truncate(limit + 1);

        let next_cursor = if keys.len() > limit { keys.pop() } else { None };

        Ok((keys, next_cursor))
    }

    async fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool, KvError> {
        let mut data = self.data.write().await;
        let now = Instant::now();

        let current = data.get(key).and_then(|entry| {
            if entry.expires_at.map_or(true, |exp| now < exp) {
                Some(entry.value.as_slice())
            } else {
                None
            }
        });

        match (expected, current) {
            (None, None) => {
                data.insert(
                    key.to_string(),
                    KvEntry {
                        value: new.to_vec(),
                        expires_at: None,
                    },
                );
                Ok(true)
            }
            (Some(exp), Some(cur)) if exp == cur => {
                data.insert(
                    key.to_string(),
                    KvEntry {
                        value: new.to_vec(),
                        expires_at: None,
                    },
                );
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

#[derive(Debug, Clone)]
struct QueueEntry {
    message: Message,
    visible_at: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryQueue {
    queues: Arc<Mutex<HashMap<String, VecDeque<QueueEntry>>>>,
}

impl MemoryQueue {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl QueueBackend for MemoryQueue {
    async fn publish(&self, queue: &str, message: &[u8]) -> Result<MessageId, QueueError> {
        let mut queues = self.queues.lock().await;
        let queue_data = queues.entry(queue.to_string()).or_default();

        let id = MessageId::new(Uuid::new_v4().to_string());
        let entry = QueueEntry {
            message: Message {
                id: id.clone(),
                payload: message.to_vec(),
                attempt: 0,
                enqueued_at: SystemTime::now(),
            },
            visible_at: Instant::now(),
        };

        queue_data.push_back(entry);
        Ok(id)
    }

    async fn receive(
        &self,
        queue: &str,
        visibility_timeout: Duration,
    ) -> Result<Option<Message>, QueueError> {
        let mut queues = self.queues.lock().await;
        let queue_data = match queues.get_mut(queue) {
            Some(q) => q,
            None => return Ok(None),
        };

        let now = Instant::now();
        for entry in queue_data.iter_mut() {
            if entry.visible_at <= now {
                entry.visible_at = now + visibility_timeout;
                entry.message.attempt += 1;
                return Ok(Some(entry.message.clone()));
            }
        }

        Ok(None)
    }

    async fn ack(&self, queue: &str, message_id: &MessageId) -> Result<(), QueueError> {
        let mut queues = self.queues.lock().await;
        let queue_data = queues
            .get_mut(queue)
            .ok_or_else(|| QueueError::QueueNotFound(queue.to_string()))?;

        let initial_len = queue_data.len();
        queue_data.retain(|entry| entry.message.id != *message_id);

        if queue_data.len() == initial_len {
            return Err(QueueError::MessageNotFound(message_id.to_string()));
        }

        Ok(())
    }

    async fn nack(&self, queue: &str, message_id: &MessageId) -> Result<(), QueueError> {
        let mut queues = self.queues.lock().await;
        let queue_data = queues
            .get_mut(queue)
            .ok_or_else(|| QueueError::QueueNotFound(queue.to_string()))?;

        for entry in queue_data.iter_mut() {
            if entry.message.id == *message_id {
                entry.visible_at = Instant::now();
                return Ok(());
            }
        }

        Err(QueueError::MessageNotFound(message_id.to_string()))
    }
}

#[derive(Debug, Clone)]
struct LockEntry {
    token: String,
    expires_at: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryLock {
    locks: Arc<Mutex<HashMap<String, LockEntry>>>,
}

impl MemoryLock {
    pub fn new() -> Self {
        Self::default()
    }

    fn make_guard(self: &Arc<Self>, resource: &str, token: &str) -> LockGuard {
        LockGuard::new(
            resource.to_string(),
            token.to_string(),
            self.clone() as Arc<dyn LockOps>,
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryLockProvider {
    inner: Arc<MemoryLock>,
}

impl MemoryLockProvider {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MemoryLock::new()),
        }
    }
}

#[async_trait]
impl LockOps for MemoryLock {
    async fn release(&self, resource: &str, token: &str) -> Result<(), LockError> {
        let mut locks = self.locks.lock().await;
        match locks.get(resource) {
            Some(entry) if entry.token == token => {
                locks.remove(resource);
                Ok(())
            }
            Some(_) => Err(LockError::NotHeld),
            None => Ok(()),
        }
    }

    async fn refresh(&self, resource: &str, token: &str, ttl: Duration) -> Result<(), LockError> {
        let mut locks = self.locks.lock().await;
        match locks.get_mut(resource) {
            Some(entry) if entry.token == token => {
                entry.expires_at = Instant::now() + ttl;
                Ok(())
            }
            _ => Err(LockError::NotHeld),
        }
    }
}

#[async_trait]
impl LockBackend for Arc<MemoryLock> {
    async fn acquire(&self, resource: &str, ttl: Duration) -> Result<LockGuard, LockError> {
        loop {
            match self.try_acquire(resource, ttl).await? {
                Some(guard) => return Ok(guard),
                None => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    async fn try_acquire(
        &self,
        resource: &str,
        ttl: Duration,
    ) -> Result<Option<LockGuard>, LockError> {
        let mut locks = self.locks.lock().await;
        let now = Instant::now();

        if let Some(entry) = locks.get(resource) {
            if entry.expires_at > now {
                return Ok(None);
            }
        }

        let token = Uuid::new_v4().to_string();
        locks.insert(
            resource.to_string(),
            LockEntry {
                token: token.clone(),
                expires_at: now + ttl,
            },
        );

        Ok(Some(self.make_guard(resource, &token)))
    }
}

#[async_trait]
impl LockOps for MemoryLockProvider {
    async fn release(&self, resource: &str, token: &str) -> Result<(), LockError> {
        self.inner.release(resource, token).await
    }

    async fn refresh(&self, resource: &str, token: &str, ttl: Duration) -> Result<(), LockError> {
        self.inner.refresh(resource, token, ttl).await
    }
}

#[async_trait]
impl LockBackend for MemoryLockProvider {
    async fn acquire(&self, resource: &str, ttl: Duration) -> Result<LockGuard, LockError> {
        loop {
            match self.try_acquire(resource, ttl).await? {
                Some(guard) => return Ok(guard),
                None => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    async fn try_acquire(
        &self,
        resource: &str,
        ttl: Duration,
    ) -> Result<Option<LockGuard>, LockError> {
        let mut locks = self.inner.locks.lock().await;
        let now = Instant::now();

        if let Some(entry) = locks.get(resource) {
            if entry.expires_at > now {
                return Ok(None);
            }
        }

        let token = Uuid::new_v4().to_string();
        locks.insert(
            resource.to_string(),
            LockEntry {
                token: token.clone(),
                expires_at: now + ttl,
            },
        );

        drop(locks);

        Ok(Some(LockGuard::new(
            resource.to_string(),
            token,
            Arc::new(self.clone()) as Arc<dyn LockOps>,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn kv_basic_operations() {
        let kv = MemoryKv::new();

        assert!(kv.get("key1").await.unwrap().is_none());

        kv.put("key1", b"value1", None).await.unwrap();
        assert_eq!(kv.get("key1").await.unwrap(), Some(b"value1".to_vec()));

        assert!(kv.delete("key1").await.unwrap());
        assert!(kv.get("key1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn kv_ttl() {
        let kv = MemoryKv::new();

        kv.put("key1", b"value1", Some(Duration::from_millis(50)))
            .await
            .unwrap();
        assert!(kv.get("key1").await.unwrap().is_some());

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(kv.get("key1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn kv_cas() {
        let kv = MemoryKv::new();

        assert!(kv.cas("key1", None, b"value1").await.unwrap());
        assert!(!kv.cas("key1", None, b"value2").await.unwrap());
        assert!(kv.cas("key1", Some(b"value1"), b"value2").await.unwrap());
        assert_eq!(kv.get("key1").await.unwrap(), Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn queue_basic_operations() {
        let queue = MemoryQueue::new();

        let id = queue.publish("test", b"message1").await.unwrap();
        let msg = queue
            .receive("test", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(msg.id, id);
        assert_eq!(msg.payload, b"message1");
        assert_eq!(msg.attempt, 1);

        queue.ack("test", &id).await.unwrap();
        assert!(queue
            .receive("test", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn queue_visibility_timeout() {
        let queue = MemoryQueue::new();

        queue.publish("test", b"message1").await.unwrap();
        let msg1 = queue
            .receive("test", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();

        assert!(queue
            .receive("test", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none());

        tokio::time::sleep(Duration::from_millis(60)).await;

        let msg2 = queue
            .receive("test", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg1.id, msg2.id);
        assert_eq!(msg2.attempt, 2);
    }

    #[tokio::test]
    async fn lock_basic_operations() {
        let lock = Arc::new(MemoryLock::new());

        let guard = lock
            .try_acquire("resource1", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();

        assert!(lock
            .try_acquire("resource1", Duration::from_secs(10))
            .await
            .unwrap()
            .is_none());

        guard.release().await.unwrap();

        assert!(lock
            .try_acquire("resource1", Duration::from_secs(10))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn lock_expiry() {
        let lock = Arc::new(MemoryLock::new());

        let _guard = lock
            .try_acquire("resource1", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(Duration::from_millis(60)).await;

        assert!(lock
            .try_acquire("resource1", Duration::from_secs(10))
            .await
            .unwrap()
            .is_some());
    }
}
