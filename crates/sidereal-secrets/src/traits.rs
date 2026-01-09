//! Traits for secrets backend implementations.

use async_trait::async_trait;

use crate::error::SecretsError;
use crate::types::{SecretContext, SecretMetadata, SecretScope, SecretValue, SecretVersion};

/// Backend trait for secrets storage.
///
/// Implementations provide the underlying storage mechanism for secrets,
/// whether that's in-memory, environment variables, or encrypted database
/// storage.
///
/// # Scope Resolution
///
/// The `get` method with a `SecretContext` searches scopes in order from most
/// specific to least specific:
/// 1. Environment scope (project + environment)
/// 2. Project scope
/// 3. Global scope
///
/// This allows environment-specific overrides while falling back to broader
/// scopes.
#[async_trait]
pub trait SecretsBackend: Send + Sync {
    /// Retrieves a secret, searching through scopes based on the context.
    ///
    /// Returns the first matching secret found in the resolution order, or
    /// `None` if the secret is not found at any scope.
    async fn get(
        &self,
        name: &str,
        context: &SecretContext,
    ) -> Result<Option<SecretValue>, SecretsError>;

    /// Retrieves a secret at a specific scope.
    ///
    /// Unlike `get`, this does not perform scope resolution - it only
    /// searches the exact scope specified.
    async fn get_at_scope(
        &self,
        name: &str,
        scope: &SecretScope,
    ) -> Result<Option<SecretValue>, SecretsError>;

    /// Retrieves a specific version of a secret.
    ///
    /// Returns `None` if the secret or version is not found.
    async fn get_version(
        &self,
        name: &str,
        scope: &SecretScope,
        version: &SecretVersion,
    ) -> Result<Option<SecretValue>, SecretsError>;

    /// Stores a secret at the specified scope.
    ///
    /// Returns the version identifier for the newly stored secret.
    /// Each call creates a new version; the backend manages version history.
    async fn set(
        &self,
        name: &str,
        value: SecretValue,
        scope: &SecretScope,
    ) -> Result<SecretVersion, SecretsError>;

    /// Deletes a secret at the specified scope.
    ///
    /// Returns `true` if the secret existed and was deleted, `false` if it
    /// did not exist.
    async fn delete(&self, name: &str, scope: &SecretScope) -> Result<bool, SecretsError>;

    /// Checks if a secret exists, searching through scopes based on context.
    async fn exists(&self, name: &str, context: &SecretContext) -> Result<bool, SecretsError>;

    /// Lists secret names matching a prefix at the specified scope.
    ///
    /// Returns a tuple of (names, cursor) for pagination. If `cursor` is
    /// `Some`, there are more results available.
    async fn list(
        &self,
        prefix: &str,
        scope: &SecretScope,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), SecretsError>;

    /// Retrieves metadata for a secret at the specified scope.
    ///
    /// Returns `None` if the secret is not found.
    async fn metadata(
        &self,
        name: &str,
        scope: &SecretScope,
    ) -> Result<Option<SecretMetadata>, SecretsError>;

    /// Lists available versions for a secret.
    ///
    /// Returns versions in reverse chronological order (newest first).
    async fn versions(
        &self,
        name: &str,
        scope: &SecretScope,
        limit: usize,
    ) -> Result<Vec<SecretVersion>, SecretsError>;
}
