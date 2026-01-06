use std::sync::Arc;

use crate::config::{KvConfig, LockConfig, QueueConfig, StateConfig};
use crate::error::StateError;
use crate::traits::{KvBackend, LockBackend, QueueBackend};

#[cfg(feature = "memory")]
use crate::memory::{MemoryKv, MemoryLockProvider, MemoryQueue};

#[cfg(feature = "valkey")]
use crate::valkey::{ValkeyKv, ValkeyLockProvider};

#[cfg(feature = "postgres")]
use crate::postgres::PostgresQueue;

#[derive(Clone, Default)]
pub struct StateProvider {
    kv: Option<Arc<dyn KvBackend>>,
    queue: Option<Arc<dyn QueueBackend>>,
    lock: Option<Arc<dyn LockBackend>>,
}

impl StateProvider {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn from_config(config: &StateConfig) -> Result<Self, StateError> {
        let kv = match &config.kv {
            Some(c) => Some(Self::create_kv_backend(c).await?),
            None => None,
        };

        let queue = match &config.queue {
            Some(c) => Some(Self::create_queue_backend(c).await?),
            None => None,
        };

        let lock = match &config.lock {
            Some(c) => Some(Self::create_lock_backend(c).await?),
            None => None,
        };

        Ok(Self { kv, queue, lock })
    }

    pub fn with_kv(mut self, kv: Arc<dyn KvBackend>) -> Self {
        self.kv = Some(kv);
        self
    }

    pub fn with_queue(mut self, queue: Arc<dyn QueueBackend>) -> Self {
        self.queue = Some(queue);
        self
    }

    pub fn with_lock(mut self, lock: Arc<dyn LockBackend>) -> Self {
        self.lock = Some(lock);
        self
    }

    pub fn kv(&self) -> Result<Arc<dyn KvBackend>, StateError> {
        self.kv
            .clone()
            .ok_or(StateError::NotConfigured("kv".to_string()))
    }

    pub fn queue(&self) -> Result<Arc<dyn QueueBackend>, StateError> {
        self.queue
            .clone()
            .ok_or(StateError::NotConfigured("queue".to_string()))
    }

    pub fn lock(&self) -> Result<Arc<dyn LockBackend>, StateError> {
        self.lock
            .clone()
            .ok_or(StateError::NotConfigured("lock".to_string()))
    }

    async fn create_kv_backend(config: &KvConfig) -> Result<Arc<dyn KvBackend>, StateError> {
        match config {
            #[cfg(feature = "memory")]
            KvConfig::Memory => Ok(Arc::new(MemoryKv::new())),

            #[cfg(feature = "valkey")]
            KvConfig::Valkey {
                url,
                namespace,
                pool_size,
            } => {
                let kv = ValkeyKv::new(url, namespace.clone(), *pool_size)
                    .await
                    .map_err(|e| StateError::Connection(e.to_string()))?;
                Ok(Arc::new(kv))
            }

            #[allow(unreachable_patterns)]
            _ => Err(StateError::UnsupportedBackend(
                "No suitable KV backend enabled".to_string(),
            )),
        }
    }

    async fn create_queue_backend(
        config: &QueueConfig,
    ) -> Result<Arc<dyn QueueBackend>, StateError> {
        match config {
            #[cfg(feature = "memory")]
            QueueConfig::Memory => Ok(Arc::new(MemoryQueue::new())),

            #[cfg(feature = "postgres")]
            QueueConfig::Postgres { url, table } => {
                let queue = PostgresQueue::new(url, table.clone())
                    .await
                    .map_err(|e| StateError::Connection(e.to_string()))?;
                Ok(Arc::new(queue))
            }

            #[allow(unreachable_patterns)]
            _ => Err(StateError::UnsupportedBackend(
                "No suitable queue backend enabled".to_string(),
            )),
        }
    }

    async fn create_lock_backend(config: &LockConfig) -> Result<Arc<dyn LockBackend>, StateError> {
        match config {
            #[cfg(feature = "memory")]
            LockConfig::Memory => Ok(Arc::new(MemoryLockProvider::new())),

            #[cfg(feature = "valkey")]
            LockConfig::Valkey {
                url,
                namespace,
                pool_size,
            } => {
                let lock = ValkeyLockProvider::new(url, namespace.clone(), *pool_size)
                    .await
                    .map_err(|e| StateError::Connection(e.to_string()))?;
                Ok(Arc::new(lock))
            }

            #[allow(unreachable_patterns)]
            _ => Err(StateError::UnsupportedBackend(
                "No suitable lock backend enabled".to_string(),
            )),
        }
    }
}

impl std::fmt::Debug for StateProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateProvider")
            .field("kv", &self.kv.is_some())
            .field("queue", &self.queue.is_some())
            .field("lock", &self.lock.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn provider_from_empty_config() {
        let config = StateConfig::default();
        let provider = StateProvider::from_config(&config).await.unwrap();

        assert!(provider.kv().is_err());
        assert!(provider.queue().is_err());
        assert!(provider.lock().is_err());
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn provider_from_memory_config() {
        let config = StateConfig {
            kv: Some(KvConfig::Memory),
            queue: Some(QueueConfig::Memory),
            lock: Some(LockConfig::Memory),
        };
        let provider = StateProvider::from_config(&config).await.unwrap();

        assert!(provider.kv().is_ok());
        assert!(provider.queue().is_ok());
        assert!(provider.lock().is_ok());
    }

    #[cfg(feature = "memory")]
    #[test]
    fn provider_builder_pattern() {
        let provider = StateProvider::new()
            .with_kv(Arc::new(MemoryKv::new()))
            .with_queue(Arc::new(MemoryQueue::new()))
            .with_lock(Arc::new(MemoryLockProvider::new()));

        assert!(provider.kv().is_ok());
        assert!(provider.queue().is_ok());
        assert!(provider.lock().is_ok());
    }
}
