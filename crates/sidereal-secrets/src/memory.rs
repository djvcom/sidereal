//! In-memory secrets backend for testing and development.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::error::SecretsError;
use crate::traits::SecretsBackend;
use crate::types::{SecretContext, SecretMetadata, SecretScope, SecretValue, SecretVersion};

/// A single version of a secret.
#[derive(Debug, Clone)]
struct VersionedValue {
    value: SecretValue,
    version: SecretVersion,
    #[allow(dead_code)] // Reserved for future version listing with timestamps
    created_at: SystemTime,
}

/// An entry in the secrets store with all versions.
#[derive(Debug, Clone)]
struct SecretEntry {
    name: String,
    scope: SecretScope,
    versions: Vec<VersionedValue>,
    created_at: SystemTime,
    updated_at: SystemTime,
}

impl SecretEntry {
    fn current(&self) -> Option<&VersionedValue> {
        self.versions.last()
    }

    fn current_version(&self) -> Option<&SecretVersion> {
        self.current().map(|v| &v.version)
    }
}

/// Storage key combining scope and name.
fn storage_key(scope: &SecretScope, name: &str) -> String {
    format!("{}:{}", scope.to_key(), name)
}

/// In-memory secrets backend.
///
/// Stores secrets in memory with support for:
/// - Three-tier scope resolution (Environment → Project → Global)
/// - Version history for each secret
/// - Concurrent access via `RwLock`
///
/// This backend is intended for testing and local development.
/// Secrets are not persisted across restarts.
#[derive(Debug, Clone, Default)]
pub struct MemorySecrets {
    data: Arc<RwLock<HashMap<String, SecretEntry>>>,
}

impl MemorySecrets {
    /// Creates a new in-memory secrets backend.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SecretsBackend for MemorySecrets {
    async fn get(
        &self,
        name: &str,
        context: &SecretContext,
    ) -> Result<Option<SecretValue>, SecretsError> {
        let data = self.data.read().await;

        for scope in context.resolution_order() {
            let key = storage_key(&scope, name);
            if let Some(entry) = data.get(&key) {
                if let Some(current) = entry.current() {
                    return Ok(Some(current.value.clone()));
                }
            }
        }

        Ok(None)
    }

    async fn get_at_scope(
        &self,
        name: &str,
        scope: &SecretScope,
    ) -> Result<Option<SecretValue>, SecretsError> {
        let data = self.data.read().await;
        let key = storage_key(scope, name);

        if let Some(entry) = data.get(&key) {
            if let Some(current) = entry.current() {
                return Ok(Some(current.value.clone()));
            }
        }

        Ok(None)
    }

    async fn get_version(
        &self,
        name: &str,
        scope: &SecretScope,
        version: &SecretVersion,
    ) -> Result<Option<SecretValue>, SecretsError> {
        let data = self.data.read().await;
        let key = storage_key(scope, name);

        if let Some(entry) = data.get(&key) {
            for v in &entry.versions {
                if &v.version == version {
                    return Ok(Some(v.value.clone()));
                }
            }
        }

        Ok(None)
    }

    async fn set(
        &self,
        name: &str,
        value: SecretValue,
        scope: &SecretScope,
    ) -> Result<SecretVersion, SecretsError> {
        let mut data = self.data.write().await;
        let key = storage_key(scope, name);
        let now = SystemTime::now();
        let version = SecretVersion::generate();

        let versioned = VersionedValue {
            value,
            version: version.clone(),
            created_at: now,
        };

        if let Some(entry) = data.get_mut(&key) {
            entry.versions.push(versioned);
            entry.updated_at = now;
        } else {
            let entry = SecretEntry {
                name: name.to_owned(),
                scope: scope.clone(),
                versions: vec![versioned],
                created_at: now,
                updated_at: now,
            };
            data.insert(key, entry);
        }

        tracing::info!(
            secret.name = name,
            secret.scope = %scope,
            secret.version = %version,
            secret.operation = "set",
            "Secret stored"
        );

        Ok(version)
    }

    async fn delete(&self, name: &str, scope: &SecretScope) -> Result<bool, SecretsError> {
        let mut data = self.data.write().await;
        let key = storage_key(scope, name);

        let existed = data.remove(&key).is_some();

        if existed {
            tracing::info!(
                secret.name = name,
                secret.scope = %scope,
                secret.operation = "delete",
                "Secret deleted"
            );
        }

        Ok(existed)
    }

    async fn exists(&self, name: &str, context: &SecretContext) -> Result<bool, SecretsError> {
        let data = self.data.read().await;

        for scope in context.resolution_order() {
            let key = storage_key(&scope, name);
            if data.contains_key(&key) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn list(
        &self,
        prefix: &str,
        scope: &SecretScope,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), SecretsError> {
        let data = self.data.read().await;
        let scope_key = scope.to_key();

        let mut names: Vec<_> = data
            .iter()
            .filter(|(key, entry)| {
                key.starts_with(&format!("{scope_key}:"))
                    && entry.name.starts_with(prefix)
                    && cursor.map_or(true, |c| entry.name.as_str() > c)
            })
            .map(|(_, entry)| entry.name.clone())
            .collect();

        names.sort();
        names.truncate(limit + 1);

        let next_cursor = if names.len() > limit {
            names.pop()
        } else {
            None
        };

        Ok((names, next_cursor))
    }

    async fn metadata(
        &self,
        name: &str,
        scope: &SecretScope,
    ) -> Result<Option<SecretMetadata>, SecretsError> {
        let data = self.data.read().await;
        let key = storage_key(scope, name);

        if let Some(entry) = data.get(&key) {
            if let Some(version) = entry.current_version() {
                return Ok(Some(SecretMetadata::with_timestamps(
                    &entry.name,
                    entry.scope.clone(),
                    version.clone(),
                    entry.created_at,
                    entry.updated_at,
                )));
            }
        }

        Ok(None)
    }

    async fn versions(
        &self,
        name: &str,
        scope: &SecretScope,
        limit: usize,
    ) -> Result<Vec<SecretVersion>, SecretsError> {
        let data = self.data.read().await;
        let key = storage_key(scope, name);

        if let Some(entry) = data.get(&key) {
            let versions: Vec<_> = entry
                .versions
                .iter()
                .rev()
                .take(limit)
                .map(|v| v.version.clone())
                .collect();
            return Ok(versions);
        }

        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_set_get() {
        let backend = MemorySecrets::new();
        let scope = SecretScope::global();
        let ctx = SecretContext::new();

        backend
            .set("API_KEY", SecretValue::new("secret123"), &scope)
            .await
            .unwrap();

        let value = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "secret123");
    }

    #[tokio::test]
    async fn scope_resolution_environment_wins() {
        let backend = MemorySecrets::new();

        backend
            .set(
                "API_KEY",
                SecretValue::new("global-key"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        backend
            .set(
                "API_KEY",
                SecretValue::new("project-key"),
                &SecretScope::project("myapp"),
            )
            .await
            .unwrap();

        backend
            .set(
                "API_KEY",
                SecretValue::new("env-key"),
                &SecretScope::environment("myapp", "prod"),
            )
            .await
            .unwrap();

        let ctx = SecretContext::new()
            .with_project("myapp")
            .with_environment("prod");

        let value = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "env-key");
    }

    #[tokio::test]
    async fn scope_resolution_fallback_to_project() {
        let backend = MemorySecrets::new();

        backend
            .set(
                "API_KEY",
                SecretValue::new("global-key"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        backend
            .set(
                "API_KEY",
                SecretValue::new("project-key"),
                &SecretScope::project("myapp"),
            )
            .await
            .unwrap();

        let ctx = SecretContext::new()
            .with_project("myapp")
            .with_environment("prod");

        let value = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "project-key");
    }

    #[tokio::test]
    async fn scope_resolution_fallback_to_global() {
        let backend = MemorySecrets::new();

        backend
            .set(
                "API_KEY",
                SecretValue::new("global-key"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        let ctx = SecretContext::new()
            .with_project("myapp")
            .with_environment("prod");

        let value = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "global-key");
    }

    #[tokio::test]
    async fn versioning() {
        let backend = MemorySecrets::new();
        let scope = SecretScope::global();

        let v1 = backend
            .set("API_KEY", SecretValue::new("value1"), &scope)
            .await
            .unwrap();

        let v2 = backend
            .set("API_KEY", SecretValue::new("value2"), &scope)
            .await
            .unwrap();

        assert_ne!(v1, v2);

        let current = backend
            .get_at_scope("API_KEY", &scope)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.expose(), "value2");

        let old = backend
            .get_version("API_KEY", &scope, &v1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old.expose(), "value1");

        let versions = backend.versions("API_KEY", &scope, 10).await.unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0], v2);
        assert_eq!(versions[1], v1);
    }

    #[tokio::test]
    async fn delete() {
        let backend = MemorySecrets::new();
        let scope = SecretScope::global();
        let ctx = SecretContext::new();

        backend
            .set("API_KEY", SecretValue::new("secret"), &scope)
            .await
            .unwrap();

        assert!(backend.exists("API_KEY", &ctx).await.unwrap());

        let deleted = backend.delete("API_KEY", &scope).await.unwrap();
        assert!(deleted);

        assert!(!backend.exists("API_KEY", &ctx).await.unwrap());

        let deleted_again = backend.delete("API_KEY", &scope).await.unwrap();
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn list_secrets() {
        let backend = MemorySecrets::new();
        let scope = SecretScope::global();

        backend
            .set("DB_HOST", SecretValue::new("localhost"), &scope)
            .await
            .unwrap();
        backend
            .set("DB_PORT", SecretValue::new("5432"), &scope)
            .await
            .unwrap();
        backend
            .set("API_KEY", SecretValue::new("secret"), &scope)
            .await
            .unwrap();

        let (names, _) = backend.list("DB_", &scope, 10, None).await.unwrap();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"DB_HOST".to_string()));
        assert!(names.contains(&"DB_PORT".to_string()));

        let (all_names, _) = backend.list("", &scope, 10, None).await.unwrap();
        assert_eq!(all_names.len(), 3);
    }

    #[tokio::test]
    async fn metadata() {
        let backend = MemorySecrets::new();
        let scope = SecretScope::project("myapp");

        let version = backend
            .set("API_KEY", SecretValue::new("secret"), &scope)
            .await
            .unwrap();

        let meta = backend.metadata("API_KEY", &scope).await.unwrap().unwrap();

        assert_eq!(meta.name, "API_KEY");
        assert_eq!(meta.scope, scope);
        assert_eq!(meta.version, version);
    }

    #[tokio::test]
    async fn not_found_returns_none() {
        let backend = MemorySecrets::new();
        let ctx = SecretContext::new();

        let value = backend.get("NONEXISTENT", &ctx).await.unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn concurrent_writes() {
        use std::sync::Arc;

        let backend = Arc::new(MemorySecrets::new());
        let scope = SecretScope::global();

        let mut handles = Vec::new();
        for i in 0..10 {
            let backend = Arc::clone(&backend);
            let scope = scope.clone();
            handles.push(tokio::spawn(async move {
                backend
                    .set(
                        "CONCURRENT_KEY",
                        SecretValue::new(format!("value_{i}")),
                        &scope,
                    )
                    .await
                    .unwrap()
            }));
        }

        let versions: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(versions.len(), 10);
        let unique_versions: std::collections::HashSet<_> =
            versions.iter().map(|v| v.as_str()).collect();
        assert_eq!(unique_versions.len(), 10);

        let all_versions = backend
            .versions("CONCURRENT_KEY", &scope, 100)
            .await
            .unwrap();
        assert_eq!(all_versions.len(), 10);
    }

    #[tokio::test]
    async fn concurrent_reads_and_writes() {
        use std::sync::Arc;

        let backend = Arc::new(MemorySecrets::new());
        let scope = SecretScope::global();
        let ctx = SecretContext::new();

        backend
            .set("RW_KEY", SecretValue::new("initial"), &scope)
            .await
            .unwrap();

        let mut handles = Vec::new();

        for i in 0..5 {
            let backend = Arc::clone(&backend);
            let scope = scope.clone();
            handles.push(tokio::spawn(async move {
                backend
                    .set("RW_KEY", SecretValue::new(format!("write_{i}")), &scope)
                    .await
                    .unwrap();
            }));
        }

        for _ in 0..5 {
            let backend = Arc::clone(&backend);
            let ctx = ctx.clone();
            handles.push(tokio::spawn(async move {
                let value = backend.get("RW_KEY", &ctx).await.unwrap();
                assert!(value.is_some());
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let value = backend.get("RW_KEY", &ctx).await.unwrap().unwrap();
        assert!(value.expose().starts_with("write_") || value.expose() == "initial");
    }

    #[tokio::test]
    async fn concurrent_reads_see_consistent_state() {
        use std::sync::Arc;

        let backend = Arc::new(MemorySecrets::new());
        let scope = SecretScope::global();

        // Set up a secret with multiple versions
        for i in 0..10 {
            backend
                .set(
                    "CONSISTENT_KEY",
                    SecretValue::new(format!("value_{i}")),
                    &scope,
                )
                .await
                .unwrap();
        }

        // Spawn many concurrent reads - each should see a complete value
        let mut handles = Vec::new();
        for _ in 0..50 {
            let backend = Arc::clone(&backend);
            let ctx = SecretContext::new();
            handles.push(tokio::spawn(async move {
                let value = backend.get("CONSISTENT_KEY", &ctx).await.unwrap();
                let exposed = value.unwrap().expose().to_owned();
                // Value must be one of the valid versions, never partial/corrupted
                assert!(
                    exposed.starts_with("value_"),
                    "Got unexpected value: {exposed}"
                );
                let num: usize = exposed.strip_prefix("value_").unwrap().parse().unwrap();
                assert!(num < 10, "Version number out of range: {num}");
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn scope_isolation_environment_does_not_see_other_environment() {
        let backend = MemorySecrets::new();

        // Set the same secret in two different environments
        let prod_scope = SecretScope::environment("myapp", "production");
        let staging_scope = SecretScope::environment("myapp", "staging");

        backend
            .set("API_KEY", SecretValue::new("prod-secret"), &prod_scope)
            .await
            .unwrap();

        backend
            .set(
                "API_KEY",
                SecretValue::new("staging-secret"),
                &staging_scope,
            )
            .await
            .unwrap();

        // Production context should only see production secret
        let prod_ctx = SecretContext::new()
            .with_project("myapp")
            .with_environment("production");
        let prod_value = backend.get("API_KEY", &prod_ctx).await.unwrap().unwrap();
        assert_eq!(prod_value.expose(), "prod-secret");

        // Staging context should only see staging secret
        let staging_ctx = SecretContext::new()
            .with_project("myapp")
            .with_environment("staging");
        let staging_value = backend.get("API_KEY", &staging_ctx).await.unwrap().unwrap();
        assert_eq!(staging_value.expose(), "staging-secret");

        // get_at_scope should respect exact scope
        let prod_direct = backend
            .get_at_scope("API_KEY", &prod_scope)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prod_direct.expose(), "prod-secret");

        let staging_direct = backend
            .get_at_scope("API_KEY", &staging_scope)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(staging_direct.expose(), "staging-secret");
    }

    #[tokio::test]
    async fn scope_isolation_projects_are_separate() {
        let backend = MemorySecrets::new();

        // Set the same secret in two different projects
        let project_a_scope = SecretScope::project("project-a");
        let project_b_scope = SecretScope::project("project-b");

        backend
            .set(
                "DATABASE_URL",
                SecretValue::new("postgres://a"),
                &project_a_scope,
            )
            .await
            .unwrap();

        backend
            .set(
                "DATABASE_URL",
                SecretValue::new("postgres://b"),
                &project_b_scope,
            )
            .await
            .unwrap();

        // Project A context should see project A secret
        let ctx_a = SecretContext::new().with_project("project-a");
        let value_a = backend.get("DATABASE_URL", &ctx_a).await.unwrap().unwrap();
        assert_eq!(value_a.expose(), "postgres://a");

        // Project B context should see project B secret
        let ctx_b = SecretContext::new().with_project("project-b");
        let value_b = backend.get("DATABASE_URL", &ctx_b).await.unwrap().unwrap();
        assert_eq!(value_b.expose(), "postgres://b");

        // Global context should NOT see either project-scoped secret
        let global_ctx = SecretContext::new();
        let global_value = backend.get("DATABASE_URL", &global_ctx).await.unwrap();
        assert!(
            global_value.is_none(),
            "Global context should not see project-scoped secrets"
        );

        // List should show secrets per scope
        let (list_a, _) = backend.list("", &project_a_scope, 100, None).await.unwrap();
        assert_eq!(list_a.len(), 1);
        assert!(list_a.contains(&"DATABASE_URL".to_owned()));

        let (list_b, _) = backend.list("", &project_b_scope, 100, None).await.unwrap();
        assert_eq!(list_b.len(), 1);

        // Global list should be empty
        let (list_global, _) = backend
            .list("", &SecretScope::global(), 100, None)
            .await
            .unwrap();
        assert!(list_global.is_empty(), "No secrets stored at global scope");
    }
}
