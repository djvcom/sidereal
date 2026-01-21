//! In-memory local service registry for single-node deployments.

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::{debug, info};

use super::{RegistryError, Result, ServiceRegistry};
use crate::Transport;

/// In-memory service registry for single-node deployments.
///
/// Uses a concurrent hash map for thread-safe access without locking.
/// Services are stored in memory and lost on restart.
#[derive(Debug, Default)]
pub struct LocalRegistry {
    services: DashMap<String, Transport>,
}

impl LocalRegistry {
    /// Creates a new empty local registry.
    pub fn new() -> Self {
        Self {
            services: DashMap::new(),
        }
    }

    /// Returns the number of registered services.
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// Returns true if no services are registered.
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }
}

#[async_trait]
impl ServiceRegistry for LocalRegistry {
    async fn register(&self, name: &str, endpoint: Transport) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        match self.services.entry(name.to_owned()) {
            Entry::Occupied(_) => {
                debug!(service = %name, "Service already registered");
                Err(RegistryError::AlreadyRegistered(name.to_owned()))
            }
            Entry::Vacant(entry) => {
                info!(service = %name, endpoint = %endpoint, "Service registered");
                entry.insert(endpoint);
                Ok(())
            }
        }
    }

    async fn deregister(&self, name: &str) -> Result<()> {
        if self.services.remove(name).is_some() {
            info!(service = %name, "Service deregistered");
        } else {
            debug!(service = %name, "Service not found for deregistration");
        }
        Ok(())
    }

    async fn lookup(&self, name: &str) -> Result<Option<Transport>> {
        Ok(self.services.get(name).map(|r| r.value().clone()))
    }

    async fn list(&self) -> Result<Vec<(String, Transport)>> {
        Ok(self
            .services
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_lookup() {
        let registry = LocalRegistry::new();
        let endpoint = Transport::unix("/tmp/test.sock");

        registry
            .register("test-service", endpoint.clone())
            .await
            .unwrap();

        let found = registry.lookup("test-service").await.unwrap();
        assert_eq!(found, Some(endpoint));
    }

    #[tokio::test]
    async fn lookup_missing_returns_none() {
        let registry = LocalRegistry::new();

        let found = registry.lookup("nonexistent").await.unwrap();
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn duplicate_registration_fails() {
        let registry = LocalRegistry::new();
        let endpoint = Transport::unix("/tmp/test.sock");

        registry
            .register("test-service", endpoint.clone())
            .await
            .unwrap();

        let result = registry.register("test-service", endpoint).await;
        assert!(matches!(result, Err(RegistryError::AlreadyRegistered(_))));
    }

    #[tokio::test]
    async fn deregister_removes_service() {
        let registry = LocalRegistry::new();
        let endpoint = Transport::unix("/tmp/test.sock");

        registry.register("test-service", endpoint).await.unwrap();
        assert!(!registry.is_empty());

        registry.deregister("test-service").await.unwrap();
        assert!(registry.is_empty());

        let found = registry.lookup("test-service").await.unwrap();
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn deregister_missing_succeeds() {
        let registry = LocalRegistry::new();

        // Should not error
        registry.deregister("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn list_returns_all_services() {
        let registry = LocalRegistry::new();

        registry
            .register("service-a", Transport::unix("/tmp/a.sock"))
            .await
            .unwrap();
        registry
            .register(
                "service-b",
                Transport::tcp("127.0.0.1:8080".parse().unwrap()),
            )
            .await
            .unwrap();

        let services = registry.list().await.unwrap();
        assert_eq!(services.len(), 2);

        let names: Vec<_> = services.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"service-a"));
        assert!(names.contains(&"service-b"));
    }
}
