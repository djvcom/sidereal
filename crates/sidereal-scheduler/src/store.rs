//! Placement store for caching worker placements.

use async_trait::async_trait;
use deadpool_redis::{redis::AsyncCommands, Config, Pool, Runtime};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::sync::broadcast;

use crate::config::ValkeyConfig;
use crate::error::{Result, SchedulerError};
use crate::registry::{WorkerId, WorkerStatus};

/// Trait for placement storage backends.
#[async_trait]
pub trait PlacementStore: Send + Sync {
    /// Gets workers for a function with availability status.
    async fn get_workers(&self, function: &str) -> Result<WorkerAvailability>;

    /// Sets workers for a function.
    async fn set_workers(&self, function: &str, workers: Vec<WorkerEndpoint>) -> Result<()>;

    /// Removes a worker from all placements.
    async fn remove_worker(&self, worker_id: &WorkerId) -> Result<()>;

    /// Subscribes to placement changes.
    fn subscribe(&self) -> broadcast::Receiver<PlacementChange>;
}

/// Worker availability status for placement queries.
#[derive(Debug, Clone)]
pub enum WorkerAvailability {
    /// Healthy workers available.
    Available(Vec<WorkerEndpoint>),
    /// Workers are starting up.
    Provisioning,
    /// Workers exist but are unhealthy.
    AllUnhealthy(Vec<WorkerEndpoint>),
    /// Function not found in placement data.
    NotFound,
}

/// Worker endpoint information for placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerEndpoint {
    /// Worker identifier.
    pub worker_id: WorkerId,
    /// Network address.
    pub address: SocketAddr,
    /// vsock CID if available.
    pub vsock_cid: Option<u32>,
    /// Current status.
    pub status: WorkerStatus,
}

impl WorkerEndpoint {
    /// Returns true if the worker is available for requests.
    #[must_use]
    pub const fn is_available(&self) -> bool {
        self.status.is_available()
    }
}

/// Placement change notification.
#[derive(Debug, Clone)]
pub enum PlacementChange {
    /// Placement updated for a function.
    Updated { function: String },
    /// Worker removed from all placements.
    WorkerRemoved { worker_id: WorkerId },
}

/// Valkey-backed placement store.
pub struct ValkeyPlacementStore {
    pool: Pool,
    ttl_secs: u64,
    key_prefix: String,
    change_sender: broadcast::Sender<PlacementChange>,
}

impl ValkeyPlacementStore {
    /// Creates a new Valkey placement store.
    pub async fn new(config: &ValkeyConfig) -> Result<Self> {
        let cfg = Config::from_url(&config.url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| SchedulerError::Config(e.to_string()))?;

        // Test connection
        let mut conn = pool.get().await?;
        let _: String = deadpool_redis::redis::cmd("PING")
            .query_async(&mut conn)
            .await?;

        let (change_sender, _) = broadcast::channel(1024);

        Ok(Self {
            pool,
            ttl_secs: config.placement_ttl_secs,
            key_prefix: "sidereal:placement:".to_owned(),
            change_sender,
        })
    }

    fn placement_key(&self, function: &str) -> String {
        format!("{}{}", self.key_prefix, function)
    }

    fn all_placements_pattern(&self) -> String {
        format!("{}*", self.key_prefix)
    }
}

#[async_trait]
impl PlacementStore for ValkeyPlacementStore {
    async fn get_workers(&self, function: &str) -> Result<WorkerAvailability> {
        let mut conn = self.pool.get().await?;
        let key = self.placement_key(function);

        let data: Option<String> = conn.get(&key).await?;

        match data {
            None => Ok(WorkerAvailability::NotFound),
            Some(json) => {
                let endpoints: Vec<WorkerEndpoint> = serde_json::from_str(&json)
                    .map_err(|e| SchedulerError::Serialisation(e.to_string()))?;

                if endpoints.is_empty() {
                    return Ok(WorkerAvailability::NotFound);
                }

                let has_available = endpoints.iter().any(WorkerEndpoint::is_available);
                let has_starting = endpoints.iter().any(|e| e.status == WorkerStatus::Starting);

                if has_available {
                    let available: Vec<_> = endpoints
                        .into_iter()
                        .filter(WorkerEndpoint::is_available)
                        .collect();
                    Ok(WorkerAvailability::Available(available))
                } else if has_starting {
                    Ok(WorkerAvailability::Provisioning)
                } else {
                    Ok(WorkerAvailability::AllUnhealthy(endpoints))
                }
            }
        }
    }

    async fn set_workers(&self, function: &str, workers: Vec<WorkerEndpoint>) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let key = self.placement_key(function);

        let json = serde_json::to_string(&workers)
            .map_err(|e| SchedulerError::Serialisation(e.to_string()))?;

        conn.set_ex::<_, _, ()>(&key, &json, self.ttl_secs).await?;

        // Notify subscribers
        let _ = self.change_sender.send(PlacementChange::Updated {
            function: function.to_owned(),
        });

        Ok(())
    }

    async fn remove_worker(&self, worker_id: &WorkerId) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let pattern = self.all_placements_pattern();

        // Get all placement keys
        let keys: Vec<String> = deadpool_redis::redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await?;

        // Remove worker from each placement
        for key in keys {
            let data: Option<String> = conn.get(&key).await?;
            if let Some(json) = data {
                let mut endpoints: Vec<WorkerEndpoint> = serde_json::from_str(&json)
                    .map_err(|e| SchedulerError::Serialisation(e.to_string()))?;

                let original_len = endpoints.len();
                endpoints.retain(|e| &e.worker_id != worker_id);

                if endpoints.len() != original_len {
                    let new_json = serde_json::to_string(&endpoints)
                        .map_err(|e| SchedulerError::Serialisation(e.to_string()))?;
                    conn.set_ex::<_, _, ()>(&key, &new_json, self.ttl_secs)
                        .await?;
                }
            }
        }

        // Notify subscribers
        let _ = self.change_sender.send(PlacementChange::WorkerRemoved {
            worker_id: worker_id.clone(),
        });

        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<PlacementChange> {
        self.change_sender.subscribe()
    }
}

impl std::fmt::Debug for ValkeyPlacementStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValkeyPlacementStore")
            .field("ttl_secs", &self.ttl_secs)
            .field("key_prefix", &self.key_prefix)
            .finish_non_exhaustive()
    }
}

/// In-memory placement store for testing.
#[derive(Debug)]
pub struct InMemoryPlacementStore {
    placements: dashmap::DashMap<String, Vec<WorkerEndpoint>>,
    change_sender: broadcast::Sender<PlacementChange>,
}

impl InMemoryPlacementStore {
    /// Creates a new in-memory placement store.
    #[must_use]
    pub fn new() -> Self {
        let (change_sender, _) = broadcast::channel(1024);
        Self {
            placements: dashmap::DashMap::new(),
            change_sender,
        }
    }
}

impl Default for InMemoryPlacementStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PlacementStore for InMemoryPlacementStore {
    async fn get_workers(&self, function: &str) -> Result<WorkerAvailability> {
        match self.placements.get(function) {
            None => Ok(WorkerAvailability::NotFound),
            Some(endpoints) => {
                if endpoints.is_empty() {
                    return Ok(WorkerAvailability::NotFound);
                }

                let has_available = endpoints.iter().any(WorkerEndpoint::is_available);
                let has_starting = endpoints.iter().any(|e| e.status == WorkerStatus::Starting);

                if has_available {
                    let available: Vec<_> = endpoints
                        .iter()
                        .filter(|e| e.is_available())
                        .cloned()
                        .collect();
                    Ok(WorkerAvailability::Available(available))
                } else if has_starting {
                    Ok(WorkerAvailability::Provisioning)
                } else {
                    Ok(WorkerAvailability::AllUnhealthy(endpoints.clone()))
                }
            }
        }
    }

    async fn set_workers(&self, function: &str, workers: Vec<WorkerEndpoint>) -> Result<()> {
        self.placements.insert(function.to_owned(), workers);
        let _ = self.change_sender.send(PlacementChange::Updated {
            function: function.to_owned(),
        });
        Ok(())
    }

    async fn remove_worker(&self, worker_id: &WorkerId) -> Result<()> {
        for mut entry in self.placements.iter_mut() {
            entry.value_mut().retain(|e| &e.worker_id != worker_id);
        }
        let _ = self.change_sender.send(PlacementChange::WorkerRemoved {
            worker_id: worker_id.clone(),
        });
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<PlacementChange> {
        self.change_sender.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_endpoint(id: &str, status: WorkerStatus) -> WorkerEndpoint {
        WorkerEndpoint {
            worker_id: id.to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            vsock_cid: None,
            status,
        }
    }

    #[tokio::test]
    async fn in_memory_set_and_get() {
        let store = InMemoryPlacementStore::new();

        let endpoints = vec![
            make_endpoint("w1", WorkerStatus::Healthy),
            make_endpoint("w2", WorkerStatus::Healthy),
        ];

        store.set_workers("greet", endpoints).await.unwrap();

        let result = store.get_workers("greet").await.unwrap();
        match result {
            WorkerAvailability::Available(workers) => {
                assert_eq!(workers.len(), 2);
            }
            _ => panic!("Expected Available"),
        }
    }

    #[tokio::test]
    async fn in_memory_not_found() {
        let store = InMemoryPlacementStore::new();
        let result = store.get_workers("missing").await.unwrap();
        assert!(matches!(result, WorkerAvailability::NotFound));
    }

    #[tokio::test]
    async fn in_memory_all_unhealthy() {
        let store = InMemoryPlacementStore::new();

        let endpoints = vec![
            make_endpoint("w1", WorkerStatus::Unhealthy),
            make_endpoint("w2", WorkerStatus::Unhealthy),
        ];

        store.set_workers("greet", endpoints).await.unwrap();

        let result = store.get_workers("greet").await.unwrap();
        assert!(matches!(result, WorkerAvailability::AllUnhealthy(_)));
    }

    #[tokio::test]
    async fn in_memory_provisioning() {
        let store = InMemoryPlacementStore::new();

        let endpoints = vec![make_endpoint("w1", WorkerStatus::Starting)];

        store.set_workers("greet", endpoints).await.unwrap();

        let result = store.get_workers("greet").await.unwrap();
        assert!(matches!(result, WorkerAvailability::Provisioning));
    }

    #[tokio::test]
    async fn in_memory_remove_worker() {
        let store = InMemoryPlacementStore::new();

        let endpoints = vec![
            make_endpoint("w1", WorkerStatus::Healthy),
            make_endpoint("w2", WorkerStatus::Healthy),
        ];

        store.set_workers("greet", endpoints).await.unwrap();
        store.remove_worker(&"w1".to_string()).await.unwrap();

        let result = store.get_workers("greet").await.unwrap();
        match result {
            WorkerAvailability::Available(workers) => {
                assert_eq!(workers.len(), 1);
                assert_eq!(workers[0].worker_id, "w2");
            }
            _ => panic!("Expected Available"),
        }
    }
}
