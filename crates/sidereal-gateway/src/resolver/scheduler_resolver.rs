//! Scheduler-based function resolver that reads placement data from Valkey.

use async_trait::async_trait;
use dashmap::DashMap;
use deadpool_redis::{redis::AsyncCommands, Config, Pool, Runtime};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::{FunctionInfo, FunctionResolver};
use crate::backend::WorkerAddress;
use crate::config::SchedulerResolverConfig;
use crate::error::GatewayError;

/// Resolver that reads placement data from the scheduler's Valkey store.
#[derive(Debug)]
pub struct SchedulerResolver {
    pool: Pool,
    key_prefix: String,
    vsock_uds_path: PathBuf,
    cache: Option<PlacementCache>,
}

/// Local cache for placement data.
#[derive(Debug)]
struct PlacementCache {
    entries: DashMap<String, CacheEntry>,
    ttl: Duration,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    info: Option<FunctionInfo>,
    cached_at: Instant,
}

impl PlacementCache {
    fn new(ttl_secs: u64) -> Self {
        Self {
            entries: DashMap::new(),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    #[allow(clippy::option_option)]
    fn get(&self, function: &str) -> Option<Option<FunctionInfo>> {
        self.entries.get(function).and_then(|entry| {
            if entry.cached_at.elapsed() < self.ttl {
                Some(entry.info.clone())
            } else {
                None
            }
        })
    }

    fn set(&self, function: &str, info: Option<FunctionInfo>) {
        self.entries.insert(
            function.to_owned(),
            CacheEntry {
                info,
                cached_at: Instant::now(),
            },
        );
    }
}

/// Worker status as stored in placement data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum WorkerStatus {
    Starting,
    Healthy,
    Degraded,
    Unhealthy,
    Draining,
    Terminated,
}

impl WorkerStatus {
    const fn is_available(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }
}

/// Worker endpoint information from placement store.
#[derive(Debug, Clone, Deserialize)]
struct WorkerEndpoint {
    #[allow(dead_code)]
    worker_id: String,
    address: SocketAddr,
    vsock_cid: Option<u32>,
    status: WorkerStatus,
}

impl SchedulerResolver {
    /// Creates a new scheduler resolver from configuration.
    pub async fn from_config(config: &SchedulerResolverConfig) -> Result<Self, GatewayError> {
        let cfg = Config::from_url(&config.valkey_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| GatewayError::Config(format!("Failed to create Valkey pool: {e}")))?;

        // Test connection
        let mut conn = pool
            .get()
            .await
            .map_err(|e| GatewayError::Config(format!("Failed to connect to Valkey: {e}")))?;

        let _: String = deadpool_redis::redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| GatewayError::Config(format!("Valkey ping failed: {e}")))?;

        let cache = if config.enable_cache {
            Some(PlacementCache::new(config.cache_ttl_secs))
        } else {
            None
        };

        Ok(Self {
            pool,
            key_prefix: "sidereal:placement:".to_owned(),
            vsock_uds_path: config.vsock_uds_path.clone(),
            cache,
        })
    }

    /// Creates a resolver for testing with a pre-configured pool.
    #[cfg(test)]
    pub fn with_pool(pool: Pool, vsock_uds_path: PathBuf) -> Self {
        Self {
            pool,
            key_prefix: "sidereal:placement:".to_string(),
            vsock_uds_path,
            cache: Some(PlacementCache::new(5)),
        }
    }

    fn placement_key(&self, function: &str) -> String {
        format!("{}{}", self.key_prefix, function)
    }

    fn all_placements_pattern(&self) -> String {
        format!("{}*", self.key_prefix)
    }

    fn endpoint_to_address(&self, endpoint: &WorkerEndpoint) -> WorkerAddress {
        if let Some(cid) = endpoint.vsock_cid {
            WorkerAddress::Vsock {
                uds_path: self.vsock_uds_path.clone(),
                port: cid,
            }
        } else {
            WorkerAddress::Http {
                url: format!("http://{}", endpoint.address),
            }
        }
    }

    async fn fetch_placement(&self, function: &str) -> Result<Option<FunctionInfo>, GatewayError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| GatewayError::PlacementStore(format!("Connection failed: {e}")))?;

        let key = self.placement_key(function);
        let data: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| GatewayError::PlacementStore(format!("Get failed: {e}")))?;

        match data {
            None => Ok(None),
            Some(json) => {
                let endpoints: Vec<WorkerEndpoint> = serde_json::from_str(&json).map_err(|e| {
                    GatewayError::PlacementStore(format!("Failed to parse placement: {e}"))
                })?;

                if endpoints.is_empty() {
                    return Ok(None);
                }

                let has_available = endpoints.iter().any(|e| e.status.is_available());
                let has_starting = endpoints.iter().any(|e| e.status == WorkerStatus::Starting);

                if has_available {
                    let addresses = endpoints
                        .iter()
                        .filter(|e| e.status.is_available())
                        .map(|e| self.endpoint_to_address(e))
                        .collect();

                    Ok(Some(FunctionInfo {
                        name: function.to_owned(),
                        backend_addresses: addresses,
                    }))
                } else if has_starting {
                    Err(GatewayError::ServiceProvisioning(function.to_owned()))
                } else {
                    Err(GatewayError::AllWorkersUnhealthy(function.to_owned()))
                }
            }
        }
    }
}

#[async_trait]
impl FunctionResolver for SchedulerResolver {
    async fn resolve(&self, function_name: &str) -> Result<Option<FunctionInfo>, GatewayError> {
        // Check cache first
        if let Some(cache) = &self.cache {
            if let Some(cached) = cache.get(function_name) {
                return Ok(cached);
            }
        }

        let result = self.fetch_placement(function_name).await;

        // Cache successful lookups (including None for not found)
        if let Some(cache) = &self.cache {
            if let Ok(ref info) = result {
                cache.set(function_name, info.clone());
            }
        }

        result
    }

    async fn list_functions(&self) -> Result<Vec<FunctionInfo>, GatewayError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| GatewayError::PlacementStore(format!("Connection failed: {e}")))?;

        let pattern = self.all_placements_pattern();

        let keys: Vec<String> = deadpool_redis::redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|e| GatewayError::PlacementStore(format!("Keys scan failed: {e}")))?;

        let mut functions = Vec::new();
        let prefix_len = self.key_prefix.len();

        for key in keys {
            let function_name = &key[prefix_len..];
            if let Ok(Some(info)) = self.resolve(function_name).await {
                functions.push(info);
            }
        }

        Ok(functions)
    }

    async fn refresh(&self) -> Result<(), GatewayError> {
        // Clear the local cache to force refresh
        if let Some(cache) = &self.cache {
            cache.entries.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_ttl_expiry() {
        let cache = PlacementCache::new(0); // 0 second TTL = immediate expiry

        let info = FunctionInfo {
            name: "test".to_string(),
            backend_addresses: vec![WorkerAddress::Http {
                url: "http://localhost:8080".to_string(),
            }],
        };

        cache.set("test", Some(info));

        // Entry should be expired immediately
        std::thread::sleep(Duration::from_millis(10));
        assert!(cache.get("test").is_none());
    }

    #[test]
    fn cache_hit_within_ttl() {
        let cache = PlacementCache::new(60); // 60 second TTL

        let info = FunctionInfo {
            name: "test".to_string(),
            backend_addresses: vec![WorkerAddress::Http {
                url: "http://localhost:8080".to_string(),
            }],
        };

        cache.set("test", Some(info.clone()));

        let cached = cache.get("test");
        assert!(cached.is_some());
        let cached_info = cached.unwrap();
        assert!(cached_info.is_some());
        assert_eq!(cached_info.unwrap().name, "test");
    }

    #[test]
    fn cache_miss() {
        let cache = PlacementCache::new(60);
        assert!(cache.get("missing").is_none());
    }

    #[test]
    fn worker_status_availability() {
        assert!(WorkerStatus::Healthy.is_available());
        assert!(WorkerStatus::Degraded.is_available());
        assert!(!WorkerStatus::Starting.is_available());
        assert!(!WorkerStatus::Unhealthy.is_available());
        assert!(!WorkerStatus::Draining.is_available());
        assert!(!WorkerStatus::Terminated.is_available());
    }

    #[test]
    fn endpoint_to_http_address() {
        let resolver = SchedulerResolver {
            pool: deadpool_redis::Config::from_url("redis://localhost:6379")
                .create_pool(Some(Runtime::Tokio1))
                .unwrap(),
            key_prefix: "sidereal:placement:".to_string(),
            vsock_uds_path: PathBuf::from("/var/run/firecracker/vsock"),
            cache: None,
        };

        let endpoint = WorkerEndpoint {
            worker_id: "w1".to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            vsock_cid: None,
            status: WorkerStatus::Healthy,
        };

        let addr = resolver.endpoint_to_address(&endpoint);
        match addr {
            WorkerAddress::Http { url } => assert_eq!(url, "http://127.0.0.1:8080"),
            _ => panic!("Expected HTTP address"),
        }
    }

    #[test]
    fn endpoint_to_vsock_address() {
        let resolver = SchedulerResolver {
            pool: deadpool_redis::Config::from_url("redis://localhost:6379")
                .create_pool(Some(Runtime::Tokio1))
                .unwrap(),
            key_prefix: "sidereal:placement:".to_string(),
            vsock_uds_path: PathBuf::from("/var/run/firecracker/vsock"),
            cache: None,
        };

        let endpoint = WorkerEndpoint {
            worker_id: "vm-w1".to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            vsock_cid: Some(42),
            status: WorkerStatus::Healthy,
        };

        let addr = resolver.endpoint_to_address(&endpoint);
        match addr {
            WorkerAddress::Vsock { uds_path, port } => {
                assert_eq!(uds_path, PathBuf::from("/var/run/firecracker/vsock"));
                assert_eq!(port, 42);
            }
            _ => panic!("Expected Vsock address"),
        }
    }
}
