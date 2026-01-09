//! Provider factory for secrets backends.

use std::sync::Arc;

use crate::config::{SecretsBackendConfig, SecretsConfig};
use crate::error::SecretsError;
use crate::traits::SecretsBackend;
use crate::types::SecretContext;

#[cfg(feature = "memory")]
use crate::memory::MemorySecrets;

#[cfg(feature = "env")]
use crate::env::EnvSecrets;

#[cfg(feature = "native")]
use crate::native::NativeSecrets;

/// Provider for secrets backends.
///
/// Manages the creation and access to secrets backends based on configuration.
/// Follows the same factory pattern as `StateProvider` in sidereal-state.
#[derive(Clone, Default)]
#[must_use]
pub struct SecretsProvider {
    backend: Option<Arc<dyn SecretsBackend>>,
    context: SecretContext,
}

impl SecretsProvider {
    /// Creates a new empty provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a provider from configuration.
    pub async fn from_config(config: &SecretsConfig) -> Result<Self, SecretsError> {
        let backend = Self::create_backend(&config.backend).await?;
        let context = config.to_context();

        Ok(Self {
            backend: Some(backend),
            context,
        })
    }

    /// Sets the backend for this provider.
    pub fn with_backend(mut self, backend: Arc<dyn SecretsBackend>) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Sets the context for this provider.
    pub fn with_context(mut self, context: SecretContext) -> Self {
        self.context = context;
        self
    }

    /// Returns the configured backend.
    pub fn backend(&self) -> Result<Arc<dyn SecretsBackend>, SecretsError> {
        self.backend.clone().ok_or(SecretsError::NotConfigured)
    }

    /// Returns the configured context.
    pub const fn context(&self) -> &SecretContext {
        &self.context
    }

    async fn create_backend(
        config: &SecretsBackendConfig,
    ) -> Result<Arc<dyn SecretsBackend>, SecretsError> {
        match config {
            #[cfg(feature = "memory")]
            SecretsBackendConfig::Memory => Ok(Arc::new(MemorySecrets::new())),

            #[cfg(feature = "env")]
            SecretsBackendConfig::Env { prefix } => Ok(Arc::new(EnvSecrets::with_prefix(prefix))),

            #[cfg(feature = "native")]
            SecretsBackendConfig::Native {
                db_path,
                identity_path,
            } => {
                let backend = NativeSecrets::new_or_create(db_path, identity_path).await?;
                Ok(Arc::new(backend))
            }

            #[allow(unreachable_patterns)]
            _ => Err(SecretsError::UnsupportedBackend(
                "No suitable secrets backend enabled".to_owned(),
            )),
        }
    }
}

impl std::fmt::Debug for SecretsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretsProvider")
            .field("backend", &self.backend.is_some())
            .field("context", &self.context)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn provider_from_empty_config() {
        let config = SecretsConfig::default();
        let provider = SecretsProvider::from_config(&config).await.unwrap();

        assert!(provider.backend().is_ok());
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn provider_from_memory_config() {
        let config = SecretsConfig {
            backend: SecretsBackendConfig::Memory,
            project: Some("myapp".to_string()),
            environment: Some("prod".to_string()),
        };
        let provider = SecretsProvider::from_config(&config).await.unwrap();

        assert!(provider.backend().is_ok());
        assert_eq!(provider.context().project_id, Some("myapp".to_string()));
        assert_eq!(provider.context().environment, Some("prod".to_string()));
    }

    #[cfg(feature = "env")]
    #[tokio::test]
    async fn provider_from_env_config() {
        let config = SecretsConfig {
            backend: SecretsBackendConfig::Env {
                prefix: "MYAPP".to_string(),
            },
            project: None,
            environment: None,
        };
        let provider = SecretsProvider::from_config(&config).await.unwrap();

        assert!(provider.backend().is_ok());
    }

    #[cfg(feature = "memory")]
    #[test]
    fn provider_builder_pattern() {
        let provider = SecretsProvider::new()
            .with_backend(Arc::new(MemorySecrets::new()))
            .with_context(SecretContext::new().with_project("test"));

        assert!(provider.backend().is_ok());
        assert_eq!(provider.context().project_id, Some("test".to_string()));
    }
}
