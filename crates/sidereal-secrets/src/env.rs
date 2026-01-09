//! Environment variable secrets backend.
//!
//! A read-only backend that reads secrets from environment variables.
//! This is primarily for local development and backwards compatibility.

use std::env;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::error::SecretsError;
use crate::traits::SecretsBackend;
use crate::types::{SecretContext, SecretMetadata, SecretScope, SecretValue, SecretVersion};

/// Default environment variable prefix.
const DEFAULT_PREFIX: &str = "SIDEREAL";

/// Environment variable secrets backend.
///
/// Reads secrets from environment variables with a configurable prefix.
/// This backend is read-only: `set()` and `delete()` operations return
/// `NotSupported` errors.
///
/// # Variable Naming
///
/// Secrets are read from variables named `{PREFIX}_{NAME}`:
/// - `SIDEREAL_API_KEY` → secret named `API_KEY` (with default prefix)
/// - `MYAPP_DATABASE_URL` → secret named `DATABASE_URL` (with `MYAPP` prefix)
///
/// # Limitations
///
/// - **No scoping**: All secrets are effectively global
/// - **No versioning**: Always returns version `"env"`
/// - **Read-only**: Cannot set or delete secrets
/// - **No listing**: `list()` returns an empty list
#[derive(Debug, Clone)]
pub struct EnvSecrets {
    prefix: String,
}

impl Default for EnvSecrets {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvSecrets {
    /// Creates a new environment secrets backend with the default prefix.
    pub fn new() -> Self {
        Self {
            prefix: DEFAULT_PREFIX.to_owned(),
        }
    }

    /// Creates a new environment secrets backend with a custom prefix.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Returns the configured prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Constructs the environment variable name for a secret.
    fn env_var_name(&self, name: &str) -> String {
        format!("{}_{}", self.prefix, name)
    }
}

#[async_trait]
impl SecretsBackend for EnvSecrets {
    async fn get(
        &self,
        name: &str,
        _context: &SecretContext,
    ) -> Result<Option<SecretValue>, SecretsError> {
        let var_name = self.env_var_name(name);

        match env::var(&var_name) {
            Ok(value) => {
                tracing::debug!(
                    secret.name = name,
                    secret.env_var = %var_name,
                    secret.operation = "get",
                    "Secret read from environment"
                );
                Ok(Some(SecretValue::new(value)))
            }
            Err(env::VarError::NotPresent) => Ok(None),
            Err(env::VarError::NotUnicode(_)) => {
                tracing::warn!(
                    secret.name = name,
                    secret.env_var = %var_name,
                    "Environment variable contains invalid UTF-8"
                );
                Ok(None)
            }
        }
    }

    async fn get_at_scope(
        &self,
        name: &str,
        _scope: &SecretScope,
    ) -> Result<Option<SecretValue>, SecretsError> {
        // Env backend ignores scope
        self.get(name, &SecretContext::new()).await
    }

    async fn get_version(
        &self,
        name: &str,
        _scope: &SecretScope,
        version: &SecretVersion,
    ) -> Result<Option<SecretValue>, SecretsError> {
        // Env backend only has one version
        if version.as_str() == "env" {
            self.get(name, &SecretContext::new()).await
        } else {
            Ok(None)
        }
    }

    async fn set(
        &self,
        _name: &str,
        _value: SecretValue,
        _scope: &SecretScope,
    ) -> Result<SecretVersion, SecretsError> {
        Err(SecretsError::NotSupported(
            "env backend is read-only".to_owned(),
        ))
    }

    async fn delete(&self, _name: &str, _scope: &SecretScope) -> Result<bool, SecretsError> {
        Err(SecretsError::NotSupported(
            "env backend is read-only".to_owned(),
        ))
    }

    async fn exists(&self, name: &str, _context: &SecretContext) -> Result<bool, SecretsError> {
        let var_name = self.env_var_name(name);
        Ok(env::var(&var_name).is_ok())
    }

    async fn list(
        &self,
        _prefix: &str,
        _scope: &SecretScope,
        _limit: usize,
        _cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), SecretsError> {
        // Could scan env vars but that seems like a security concern
        // Better to not expose what env vars exist
        Ok((Vec::new(), None))
    }

    async fn metadata(
        &self,
        name: &str,
        scope: &SecretScope,
    ) -> Result<Option<SecretMetadata>, SecretsError> {
        if self.exists(name, &SecretContext::new()).await? {
            let now = SystemTime::now();
            Ok(Some(SecretMetadata::with_timestamps(
                name,
                scope.clone(),
                SecretVersion::new("env"),
                now,
                now,
            )))
        } else {
            Ok(None)
        }
    }

    async fn versions(
        &self,
        name: &str,
        _scope: &SecretScope,
        _limit: usize,
    ) -> Result<Vec<SecretVersion>, SecretsError> {
        if self.exists(name, &SecretContext::new()).await? {
            Ok(vec![SecretVersion::new("env")])
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[tokio::test]
    async fn read_from_env() {
        env::set_var("SIDEREAL_TEST_SECRET", "test-value");

        let backend = EnvSecrets::new();
        let ctx = SecretContext::new();

        let value = backend.get("TEST_SECRET", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "test-value");

        env::remove_var("SIDEREAL_TEST_SECRET");
    }

    #[tokio::test]
    async fn custom_prefix() {
        env::set_var("MYAPP_API_KEY", "custom-key");

        let backend = EnvSecrets::with_prefix("MYAPP");
        let ctx = SecretContext::new();

        let value = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "custom-key");

        env::remove_var("MYAPP_API_KEY");
    }

    #[tokio::test]
    async fn not_found() {
        let backend = EnvSecrets::new();
        let ctx = SecretContext::new();

        let value = backend
            .get("DEFINITELY_NOT_SET_VAR_12345", &ctx)
            .await
            .unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn set_returns_error() {
        let backend = EnvSecrets::new();
        let scope = SecretScope::global();

        let result = backend.set("TEST", SecretValue::new("value"), &scope).await;

        assert!(matches!(result, Err(SecretsError::NotSupported(_))));
    }

    #[tokio::test]
    async fn delete_returns_error() {
        let backend = EnvSecrets::new();
        let scope = SecretScope::global();

        let result = backend.delete("TEST", &scope).await;

        assert!(matches!(result, Err(SecretsError::NotSupported(_))));
    }

    #[tokio::test]
    async fn exists_check() {
        env::set_var("SIDEREAL_EXISTS_TEST", "value");

        let backend = EnvSecrets::new();
        let ctx = SecretContext::new();

        assert!(backend.exists("EXISTS_TEST", &ctx).await.unwrap());
        assert!(!backend.exists("NOT_EXISTS_TEST", &ctx).await.unwrap());

        env::remove_var("SIDEREAL_EXISTS_TEST");
    }

    #[tokio::test]
    async fn version_is_env() {
        env::set_var("SIDEREAL_VERSIONED_TEST", "value");

        let backend = EnvSecrets::new();
        let scope = SecretScope::global();

        let versions = backend
            .versions("VERSIONED_TEST", &scope, 10)
            .await
            .unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].as_str(), "env");

        env::remove_var("SIDEREAL_VERSIONED_TEST");
    }
}
