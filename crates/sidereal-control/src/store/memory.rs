//! In-memory deployment store for testing.

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;

use crate::error::{ControlError, ControlResult};
use crate::types::{DeploymentId, DeploymentRecord, PersistedState, ProjectId};

use super::{DeploymentFilter, DeploymentStore};

/// In-memory deployment store for testing.
///
/// This implementation is not suitable for production use as data is lost
/// when the process exits.
#[derive(Debug, Default)]
pub struct MemoryStore {
    deployments: RwLock<HashMap<String, DeploymentRecord>>,
    active: RwLock<HashMap<(String, String), String>>,
}

impl MemoryStore {
    /// Create a new empty in-memory store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DeploymentStore for MemoryStore {
    async fn insert(&self, record: &DeploymentRecord) -> ControlResult<()> {
        let mut deployments = self
            .deployments
            .write()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        let key = record.data.id.as_str().to_owned();
        if deployments.contains_key(&key) {
            return Err(ControlError::internal(format!(
                "deployment {key} already exists"
            )));
        }

        deployments.insert(key, record.clone());
        Ok(())
    }

    async fn get(&self, id: &DeploymentId) -> ControlResult<Option<DeploymentRecord>> {
        let deployments = self
            .deployments
            .read()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        Ok(deployments.get(id.as_str()).cloned())
    }

    async fn update_state(
        &self,
        id: &DeploymentId,
        state: PersistedState,
        error: Option<&str>,
    ) -> ControlResult<()> {
        let mut deployments = self
            .deployments
            .write()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        let record = deployments
            .get_mut(id.as_str())
            .ok_or_else(|| ControlError::DeploymentNotFound(id.to_string()))?;

        record.state = state;
        record.data.error = error.map(ToOwned::to_owned);
        record.data.updated_at = chrono::Utc::now();

        Ok(())
    }

    async fn list(&self, filter: &DeploymentFilter) -> ControlResult<Vec<DeploymentRecord>> {
        let deployments = self
            .deployments
            .read()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        let mut results: Vec<_> = deployments
            .values()
            .filter(|r| {
                if let Some(ref project_id) = filter.project_id {
                    if r.data.project_id.as_str() != project_id.as_str() {
                        return false;
                    }
                }
                if let Some(ref env) = filter.environment {
                    if &r.data.environment != env {
                        return false;
                    }
                }
                if let Some(state) = filter.state {
                    if r.state != state {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        results.sort_by(|a, b| b.data.created_at.cmp(&a.data.created_at));

        #[allow(clippy::as_conversions)]
        let offset = filter.offset.unwrap_or(0) as usize;
        let results: Vec<_> = results.into_iter().skip(offset).collect();

        if let Some(limit) = filter.limit {
            #[allow(clippy::as_conversions)]
            Ok(results.into_iter().take(limit as usize).collect())
        } else {
            Ok(results)
        }
    }

    async fn get_active(
        &self,
        project_id: &ProjectId,
        environment: &str,
    ) -> ControlResult<Option<DeploymentRecord>> {
        let deployment_id = {
            let active = self
                .active
                .read()
                .map_err(|_| ControlError::internal("lock poisoned"))?;

            let key = (project_id.as_str().to_owned(), environment.to_owned());
            match active.get(&key) {
                Some(id) => id.clone(),
                None => return Ok(None),
            }
        };

        self.get(&DeploymentId::new(deployment_id)).await
    }

    async fn set_active(
        &self,
        project_id: &ProjectId,
        environment: &str,
        deployment_id: &DeploymentId,
    ) -> ControlResult<()> {
        let mut active = self
            .active
            .write()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        let key = (project_id.as_str().to_owned(), environment.to_owned());
        active.insert(key, deployment_id.as_str().to_owned());

        Ok(())
    }

    async fn clear_active(&self, project_id: &ProjectId, environment: &str) -> ControlResult<()> {
        let mut active = self
            .active
            .write()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        let key = (project_id.as_str().to_owned(), environment.to_owned());
        active.remove(&key);

        Ok(())
    }

    async fn delete(&self, id: &DeploymentId) -> ControlResult<()> {
        let mut deployments = self
            .deployments
            .write()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        if deployments.remove(id.as_str()).is_none() {
            return Err(ControlError::DeploymentNotFound(id.to_string()));
        }

        let mut active = self
            .active
            .write()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        active.retain(|_, v| v != id.as_str());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DeploymentData, FunctionMetadata, FunctionTrigger};

    fn test_deployment() -> DeploymentRecord {
        let data = DeploymentData::new(
            ProjectId::new("test-project"),
            "production".to_owned(),
            "abc123def456".to_owned(),
            "s3://bucket/artifact.rootfs".to_owned(),
            vec![FunctionMetadata {
                name: "handler".to_owned(),
                trigger: FunctionTrigger::Http {
                    method: "GET".to_owned(),
                    path: "/hello".to_owned(),
                },
                memory_mb: 128,
                vcpus: 1,
            }],
        );
        DeploymentRecord::new(data)
    }

    #[tokio::test]
    async fn insert_and_get() {
        let store = MemoryStore::new();

        let record = test_deployment();
        let id = record.data.id.clone();

        store.insert(&record).await.expect("insert failed");

        let retrieved = store
            .get(&id)
            .await
            .expect("get failed")
            .expect("deployment not found");

        assert_eq!(retrieved.data.id, id);
        assert_eq!(retrieved.data.project_id.as_str(), "test-project");
        assert_eq!(retrieved.data.environment, "production");
        assert_eq!(retrieved.state, PersistedState::Pending);
    }

    #[tokio::test]
    async fn duplicate_insert_fails() {
        let store = MemoryStore::new();

        let record = test_deployment();

        store.insert(&record).await.expect("first insert failed");
        assert!(store.insert(&record).await.is_err());
    }

    #[tokio::test]
    async fn update_state() {
        let store = MemoryStore::new();

        let record = test_deployment();
        let id = record.data.id.clone();

        store.insert(&record).await.expect("insert failed");

        store
            .update_state(&id, PersistedState::Registering, None)
            .await
            .expect("update failed");

        let retrieved = store
            .get(&id)
            .await
            .expect("get failed")
            .expect("not found");
        assert_eq!(retrieved.state, PersistedState::Registering);

        store
            .update_state(&id, PersistedState::Failed, Some("test error"))
            .await
            .expect("update failed");

        let retrieved = store
            .get(&id)
            .await
            .expect("get failed")
            .expect("not found");
        assert_eq!(retrieved.state, PersistedState::Failed);
        assert_eq!(retrieved.data.error.as_deref(), Some("test error"));
    }

    #[tokio::test]
    async fn update_nonexistent_fails() {
        let store = MemoryStore::new();

        let result = store
            .update_state(
                &DeploymentId::new("nonexistent"),
                PersistedState::Active,
                None,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn active_deployment() {
        let store = MemoryStore::new();

        let record = test_deployment();
        let id = record.data.id.clone();
        let project_id = record.data.project_id.clone();

        store.insert(&record).await.expect("insert failed");

        let active = store
            .get_active(&project_id, "production")
            .await
            .expect("get_active failed");
        assert!(active.is_none());

        store
            .set_active(&project_id, "production", &id)
            .await
            .expect("set_active failed");

        let active = store
            .get_active(&project_id, "production")
            .await
            .expect("get_active failed")
            .expect("should have active deployment");
        assert_eq!(active.data.id, id);

        store
            .clear_active(&project_id, "production")
            .await
            .expect("clear_active failed");

        let active = store
            .get_active(&project_id, "production")
            .await
            .expect("get_active failed");
        assert!(active.is_none());
    }

    #[tokio::test]
    async fn list_with_filters() {
        let store = MemoryStore::new();

        let mut record1 = test_deployment();
        record1.data.environment = "staging".to_owned();
        let id1 = record1.data.id.clone();

        let record2 = test_deployment();
        let id2 = record2.data.id.clone();

        store.insert(&record1).await.expect("insert failed");
        store.insert(&record2).await.expect("insert failed");

        let all = store
            .list(&DeploymentFilter::new().with_project(ProjectId::new("test-project")))
            .await
            .expect("list failed");
        assert_eq!(all.len(), 2);

        let staging = store
            .list(
                &DeploymentFilter::new()
                    .with_project(ProjectId::new("test-project"))
                    .with_environment("staging"),
            )
            .await
            .expect("list failed");
        assert_eq!(staging.len(), 1);
        assert_eq!(staging[0].data.id, id1);

        let production = store
            .list(
                &DeploymentFilter::new()
                    .with_project(ProjectId::new("test-project"))
                    .with_environment("production"),
            )
            .await
            .expect("list failed");
        assert_eq!(production.len(), 1);
        assert_eq!(production[0].data.id, id2);
    }

    #[tokio::test]
    async fn list_pagination() {
        let store = MemoryStore::new();

        for i in 0..5 {
            let mut record = test_deployment();
            record.data.environment = format!("env-{i}");
            store.insert(&record).await.expect("insert failed");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let page1 = store
            .list(
                &DeploymentFilter::new()
                    .with_project(ProjectId::new("test-project"))
                    .with_limit(2),
            )
            .await
            .expect("list failed");
        assert_eq!(page1.len(), 2);

        let page2 = store
            .list(
                &DeploymentFilter::new()
                    .with_project(ProjectId::new("test-project"))
                    .with_limit(2)
                    .with_offset(2),
            )
            .await
            .expect("list failed");
        assert_eq!(page2.len(), 2);

        assert_ne!(page1[0].data.id, page2[0].data.id);
    }

    #[tokio::test]
    async fn delete_clears_active() {
        let store = MemoryStore::new();

        let record = test_deployment();
        let id = record.data.id.clone();
        let project_id = record.data.project_id.clone();

        store.insert(&record).await.expect("insert failed");
        store
            .set_active(&project_id, "production", &id)
            .await
            .expect("set_active failed");

        store.delete(&id).await.expect("delete failed");

        let active = store
            .get_active(&project_id, "production")
            .await
            .expect("get_active failed");
        assert!(active.is_none());
    }
}
