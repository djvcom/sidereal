//! Valkey/Redis adapter for KV and Lock backends.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use deadpool_redis::{Config, Pool, Runtime};
use redis::AsyncCommands;
use uuid::Uuid;

use crate::error::{KvError, LockError};
use crate::traits::{KvBackend, LockBackend, LockOps};
use crate::types::LockGuard;

/// Valkey/Redis KV backend.
#[derive(Clone)]
pub struct ValkeyKv {
    pool: Pool,
    namespace: Option<String>,
}

impl ValkeyKv {
    /// Create a new Valkey KV backend.
    pub async fn new(
        url: &str,
        namespace: Option<String>,
        pool_size: usize,
    ) -> Result<Self, KvError> {
        let config = Config::from_url(url);
        let pool = config
            .builder()
            .map_err(|e| KvError::Connection(e.to_string()))?
            .max_size(pool_size)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| KvError::Connection(e.to_string()))?;

        // Test the connection
        let mut conn = pool
            .get()
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        redis::cmd("PING")
            .query_async::<String>(&mut *conn)
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        Ok(Self { pool, namespace })
    }

    fn prefixed_key(&self, key: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:{}", ns, key),
            None => key.to_string(),
        }
    }

    fn strip_prefix<'a>(&self, key: &'a str) -> &'a str {
        match &self.namespace {
            Some(ns) => key
                .strip_prefix(ns)
                .and_then(|k| k.strip_prefix(':'))
                .unwrap_or(key),
            None => key,
        }
    }
}

#[async_trait]
impl KvBackend for ValkeyKv {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        let prefixed = self.prefixed_key(key);
        let result: Option<Vec<u8>> = conn
            .get(&prefixed)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))?;

        Ok(result)
    }

    async fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KvError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        let prefixed = self.prefixed_key(key);

        match ttl {
            Some(duration) => {
                let seconds = duration.as_secs().max(1);
                conn.set_ex::<_, _, ()>(&prefixed, value, seconds)
                    .await
                    .map_err(|e| KvError::Backend(e.to_string()))?;
            }
            None => {
                conn.set::<_, _, ()>(&prefixed, value)
                    .await
                    .map_err(|e| KvError::Backend(e.to_string()))?;
            }
        }

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, KvError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        let prefixed = self.prefixed_key(key);
        let deleted: i64 = conn
            .del(&prefixed)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))?;

        Ok(deleted > 0)
    }

    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        let prefixed = self.prefixed_key(key);
        let exists: bool = conn
            .exists(&prefixed)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))?;

        Ok(exists)
    }

    async fn list(
        &self,
        prefix: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), KvError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        let pattern = self.prefixed_key(&format!("{}*", prefix));
        let start_cursor: u64 = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);

        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(start_cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(limit)
            .query_async(&mut *conn)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))?;

        let stripped_keys: Vec<String> = keys
            .into_iter()
            .map(|k| self.strip_prefix(&k).to_string())
            .collect();

        let next = if next_cursor == 0 {
            None
        } else {
            Some(next_cursor.to_string())
        };

        Ok((stripped_keys, next))
    }

    async fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool, KvError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KvError::Connection(e.to_string()))?;

        let prefixed = self.prefixed_key(key);

        // Use a Lua script for atomic compare-and-swap
        let script = redis::Script::new(
            r#"
            local current = redis.call('GET', KEYS[1])
            local expected = ARGV[1]
            local new_value = ARGV[2]

            if expected == '' then
                -- Expecting key to not exist
                if current == false then
                    redis.call('SET', KEYS[1], new_value)
                    return 1
                else
                    return 0
                end
            else
                -- Expecting specific value
                if current == expected then
                    redis.call('SET', KEYS[1], new_value)
                    return 1
                else
                    return 0
                end
            end
            "#,
        );

        let expected_arg: Vec<u8> = expected.map(|e| e.to_vec()).unwrap_or_default();

        let result: i64 = script
            .key(&prefixed)
            .arg(expected_arg)
            .arg(new)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))?;

        Ok(result == 1)
    }
}

/// Valkey/Redis Lock backend.
#[derive(Clone)]
pub struct ValkeyLock {
    pool: Pool,
    namespace: Option<String>,
}

impl ValkeyLock {
    /// Create a new Valkey lock backend.
    pub async fn new(
        url: &str,
        namespace: Option<String>,
        pool_size: usize,
    ) -> Result<Self, LockError> {
        let config = Config::from_url(url);
        let pool = config
            .builder()
            .map_err(|e| LockError::Connection(e.to_string()))?
            .max_size(pool_size)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| LockError::Connection(e.to_string()))?;

        // Test the connection
        let mut conn = pool
            .get()
            .await
            .map_err(|e| LockError::Connection(e.to_string()))?;

        redis::cmd("PING")
            .query_async::<String>(&mut *conn)
            .await
            .map_err(|e| LockError::Connection(e.to_string()))?;

        Ok(Self { pool, namespace })
    }

    fn lock_key(&self, resource: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:lock:{}", ns, resource),
            None => format!("lock:{}", resource),
        }
    }
}

#[async_trait]
impl LockOps for ValkeyLock {
    async fn release(&self, resource: &str, token: &str) -> Result<(), LockError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| LockError::Connection(e.to_string()))?;

        let key = self.lock_key(resource);

        // Use Lua script for safe release (only delete if we own the lock)
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
            "#,
        );

        let _: i64 = script
            .key(&key)
            .arg(token)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| LockError::Backend(e.to_string()))?;

        Ok(())
    }

    async fn refresh(&self, resource: &str, token: &str, ttl: Duration) -> Result<(), LockError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| LockError::Connection(e.to_string()))?;

        let key = self.lock_key(resource);
        let seconds = ttl.as_secs().max(1) as i64;

        // Use Lua script to refresh only if we own the lock
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('EXPIRE', KEYS[1], ARGV[2])
            else
                return 0
            end
            "#,
        );

        let result: i64 = script
            .key(&key)
            .arg(token)
            .arg(seconds)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| LockError::Backend(e.to_string()))?;

        if result == 0 {
            return Err(LockError::NotHeld);
        }

        Ok(())
    }
}

/// Provider that holds an Arc<ValkeyLock> for use with LockBackend.
#[derive(Clone)]
pub struct ValkeyLockProvider {
    inner: Arc<ValkeyLock>,
}

impl ValkeyLockProvider {
    /// Create a new Valkey lock provider.
    pub async fn new(
        url: &str,
        namespace: Option<String>,
        pool_size: usize,
    ) -> Result<Self, LockError> {
        let lock = ValkeyLock::new(url, namespace, pool_size).await?;
        Ok(Self {
            inner: Arc::new(lock),
        })
    }
}

#[async_trait]
impl LockOps for ValkeyLockProvider {
    async fn release(&self, resource: &str, token: &str) -> Result<(), LockError> {
        self.inner.release(resource, token).await
    }

    async fn refresh(&self, resource: &str, token: &str, ttl: Duration) -> Result<(), LockError> {
        self.inner.refresh(resource, token, ttl).await
    }
}

#[async_trait]
impl LockBackend for ValkeyLockProvider {
    async fn acquire(&self, resource: &str, ttl: Duration) -> Result<LockGuard, LockError> {
        loop {
            match self.try_acquire(resource, ttl).await? {
                Some(guard) => return Ok(guard),
                None => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    async fn try_acquire(
        &self,
        resource: &str,
        ttl: Duration,
    ) -> Result<Option<LockGuard>, LockError> {
        let mut conn = self
            .inner
            .pool
            .get()
            .await
            .map_err(|e| LockError::Connection(e.to_string()))?;

        let key = self.inner.lock_key(resource);
        let token = Uuid::new_v4().to_string();
        let seconds = ttl.as_secs().max(1);

        // SET key token NX EX seconds
        let result: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(&token)
            .arg("NX")
            .arg("EX")
            .arg(seconds)
            .query_async(&mut *conn)
            .await
            .map_err(|e| LockError::Backend(e.to_string()))?;

        if result.is_some() {
            Ok(Some(LockGuard::new(
                resource.to_string(),
                token,
                Arc::new(self.clone()) as Arc<dyn LockOps>,
            )))
        } else {
            Ok(None)
        }
    }
}

impl std::fmt::Debug for ValkeyKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValkeyKv")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ValkeyLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValkeyLock")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ValkeyLockProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValkeyLockProvider")
            .field("inner", &self.inner)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests require a running Valkey/Redis instance
    // Run with: cargo test --features valkey -- --ignored

    #[tokio::test]
    #[ignore = "requires Valkey/Redis instance at 127.0.0.1:6379"]
    async fn kv_basic_operations() {
        let kv = ValkeyKv::new("redis://127.0.0.1:6379", Some("test".to_string()), 5)
            .await
            .expect("Failed to connect to Valkey");

        // Clean up any previous test data
        let _ = kv.delete("test_key").await;

        // Test get non-existent
        assert!(kv.get("test_key").await.unwrap().is_none());

        // Test put and get
        kv.put("test_key", b"test_value", None).await.unwrap();
        assert_eq!(
            kv.get("test_key").await.unwrap(),
            Some(b"test_value".to_vec())
        );

        // Test exists
        assert!(kv.exists("test_key").await.unwrap());

        // Test delete
        assert!(kv.delete("test_key").await.unwrap());
        assert!(!kv.exists("test_key").await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires Valkey/Redis instance at 127.0.0.1:6379"]
    async fn kv_ttl() {
        let kv = ValkeyKv::new("redis://127.0.0.1:6379", Some("test".to_string()), 5)
            .await
            .expect("Failed to connect to Valkey");

        kv.put("ttl_key", b"value", Some(Duration::from_secs(1)))
            .await
            .unwrap();
        assert!(kv.exists("ttl_key").await.unwrap());

        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(!kv.exists("ttl_key").await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires Valkey/Redis instance at 127.0.0.1:6379"]
    async fn kv_cas() {
        let kv = ValkeyKv::new("redis://127.0.0.1:6379", Some("test".to_string()), 5)
            .await
            .expect("Failed to connect to Valkey");

        // Clean up
        let _ = kv.delete("cas_key").await;

        // CAS on non-existent key
        assert!(kv.cas("cas_key", None, b"value1").await.unwrap());

        // CAS should fail when expecting None but key exists
        assert!(!kv.cas("cas_key", None, b"value2").await.unwrap());

        // CAS should succeed with correct expected value
        assert!(kv.cas("cas_key", Some(b"value1"), b"value2").await.unwrap());

        // Verify the value changed
        assert_eq!(kv.get("cas_key").await.unwrap(), Some(b"value2".to_vec()));

        // Clean up
        let _ = kv.delete("cas_key").await;
    }

    #[tokio::test]
    #[ignore = "requires Valkey/Redis instance at 127.0.0.1:6379"]
    async fn lock_basic_operations() {
        let lock = ValkeyLockProvider::new("redis://127.0.0.1:6379", Some("test".to_string()), 5)
            .await
            .expect("Failed to connect to Valkey");

        // Acquire lock
        let guard = lock
            .try_acquire("test_resource", Duration::from_secs(10))
            .await
            .unwrap()
            .expect("Failed to acquire lock");

        // Try to acquire again (should fail)
        assert!(lock
            .try_acquire("test_resource", Duration::from_secs(10))
            .await
            .unwrap()
            .is_none());

        // Release
        guard.release().await.unwrap();

        // Should be able to acquire again
        let guard2 = lock
            .try_acquire("test_resource", Duration::from_secs(10))
            .await
            .unwrap()
            .expect("Failed to acquire lock after release");

        guard2.release().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires Valkey/Redis instance at 127.0.0.1:6379"]
    async fn lock_refresh() {
        let lock = ValkeyLockProvider::new("redis://127.0.0.1:6379", Some("test".to_string()), 5)
            .await
            .expect("Failed to connect to Valkey");

        let guard = lock
            .try_acquire("refresh_resource", Duration::from_secs(2))
            .await
            .unwrap()
            .expect("Failed to acquire lock");

        // Refresh the lock
        guard.refresh(Duration::from_secs(10)).await.unwrap();

        // Wait longer than original TTL
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Lock should still be held (was refreshed)
        assert!(lock
            .try_acquire("refresh_resource", Duration::from_secs(10))
            .await
            .unwrap()
            .is_none());

        guard.release().await.unwrap();
    }
}
