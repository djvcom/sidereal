//! Core types for secrets management.

use std::fmt;
use std::time::SystemTime;

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Scope at which a secret is stored.
///
/// Secrets are resolved in order from most specific (Environment) to least
/// specific (Global). This allows environment-specific overrides while
/// falling back to broader scopes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SecretScope {
    /// Global scope - available to all projects and environments.
    Global,

    /// Project scope - available to all environments within a project.
    Project {
        /// The project identifier.
        project_id: String,
    },

    /// Environment scope - specific to a project and environment.
    Environment {
        /// The project identifier.
        project_id: String,
        /// The environment name (e.g., "production", "staging").
        environment: String,
    },
}

impl SecretScope {
    /// Creates a global scope.
    #[must_use]
    pub const fn global() -> Self {
        Self::Global
    }

    /// Creates a project scope.
    #[must_use]
    pub fn project(project_id: impl Into<String>) -> Self {
        Self::Project {
            project_id: project_id.into(),
        }
    }

    /// Creates an environment scope.
    #[must_use]
    pub fn environment(project_id: impl Into<String>, environment: impl Into<String>) -> Self {
        Self::Environment {
            project_id: project_id.into(),
            environment: environment.into(),
        }
    }

    /// Returns a string key for storage.
    #[must_use]
    pub fn to_key(&self) -> String {
        match self {
            Self::Global => "global".to_owned(),
            Self::Project { project_id } => format!("project:{project_id}"),
            Self::Environment {
                project_id,
                environment,
            } => format!("env:{project_id}:{environment}"),
        }
    }
}

impl fmt::Display for SecretScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Global => write!(f, "global"),
            Self::Project { project_id } => write!(f, "project:{project_id}"),
            Self::Environment {
                project_id,
                environment,
            } => write!(f, "env:{project_id}:{environment}"),
        }
    }
}

/// A secret value with automatic memory zeroisation.
///
/// The value is stored as a `SecretString` which prevents accidental logging
/// and ensures memory is zeroed when dropped.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SecretValue {
    #[zeroize(skip)]
    inner: SecretString,
}

impl SecretValue {
    /// Creates a new secret value from a string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            inner: SecretString::from(value.into()),
        }
    }

    /// Creates a new secret value from bytes (UTF-8 encoded).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, std::str::Utf8Error> {
        let s = std::str::from_utf8(bytes)?;
        Ok(Self::new(s))
    }

    /// Exposes the secret value for use.
    ///
    /// Be careful when using this method - the returned reference should
    /// not be logged, stored, or otherwise exposed.
    #[must_use]
    pub fn expose(&self) -> &str {
        self.inner.expose_secret()
    }

    /// Returns the length of the secret value in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.expose_secret().len()
    }

    /// Returns true if the secret value is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.expose_secret().is_empty()
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl PartialEq for SecretValue {
    /// Constant-time comparison to prevent timing attacks.
    fn eq(&self, other: &Self) -> bool {
        let self_bytes = self.inner.expose_secret().as_bytes();
        let other_bytes = other.inner.expose_secret().as_bytes();

        if self_bytes.len() != other_bytes.len() {
            return false;
        }

        self_bytes.ct_eq(other_bytes).into()
    }
}

impl Eq for SecretValue {}

/// A version identifier for a secret.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SecretVersion(String);

impl SecretVersion {
    /// Creates a new version identifier.
    #[must_use]
    pub fn new(version: impl Into<String>) -> Self {
        Self(version.into())
    }

    /// Generates a new unique version identifier.
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Returns the version as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecretVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for SecretVersion {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SecretVersion {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Metadata about a stored secret.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretMetadata {
    /// The secret name.
    pub name: String,

    /// The scope at which this secret is stored.
    pub scope: SecretScope,

    /// The current version of the secret.
    pub version: SecretVersion,

    /// When the secret was first created.
    pub created_at: SystemTime,

    /// When the secret was last updated.
    pub updated_at: SystemTime,
}

impl SecretMetadata {
    /// Creates new metadata for a secret.
    pub fn new(name: impl Into<String>, scope: SecretScope, version: SecretVersion) -> Self {
        let now = SystemTime::now();
        Self {
            name: name.into(),
            scope,
            version,
            created_at: now,
            updated_at: now,
        }
    }

    /// Creates metadata with specific timestamps.
    pub fn with_timestamps(
        name: impl Into<String>,
        scope: SecretScope,
        version: SecretVersion,
        created_at: SystemTime,
        updated_at: SystemTime,
    ) -> Self {
        Self {
            name: name.into(),
            scope,
            version,
            created_at,
            updated_at,
        }
    }
}

/// Context for resolving secrets across scopes.
///
/// When set on a backend or provider, this context is used to determine
/// which scopes to search when resolving a secret.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretContext {
    /// The current project, if any.
    pub project_id: Option<String>,

    /// The current environment, if any.
    pub environment: Option<String>,
}

impl SecretContext {
    /// Creates a new empty context.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the project for this context.
    #[must_use]
    pub fn with_project(mut self, project_id: impl Into<String>) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Sets the environment for this context.
    #[must_use]
    pub fn with_environment(mut self, environment: impl Into<String>) -> Self {
        self.environment = Some(environment.into());
        self
    }

    /// Returns the scopes to search in resolution order (most specific first).
    #[must_use]
    pub fn resolution_order(&self) -> Vec<SecretScope> {
        let mut scopes = Vec::with_capacity(3);

        if let (Some(project_id), Some(environment)) = (&self.project_id, &self.environment) {
            scopes.push(SecretScope::environment(project_id, environment));
        }

        if let Some(project_id) = &self.project_id {
            scopes.push(SecretScope::project(project_id));
        }

        scopes.push(SecretScope::global());
        scopes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_scope_to_key() {
        assert_eq!(SecretScope::global().to_key(), "global");
        assert_eq!(SecretScope::project("myapp").to_key(), "project:myapp");
        assert_eq!(
            SecretScope::environment("myapp", "prod").to_key(),
            "env:myapp:prod"
        );
    }

    #[test]
    fn secret_value_redacted_debug() {
        let value = SecretValue::new("super-secret");
        let debug = format!("{value:?}");
        assert_eq!(debug, "[REDACTED]");
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn secret_value_expose() {
        let value = SecretValue::new("super-secret");
        assert_eq!(value.expose(), "super-secret");
    }

    #[test]
    fn secret_context_resolution_order() {
        let ctx = SecretContext::new()
            .with_project("myapp")
            .with_environment("prod");

        let scopes = ctx.resolution_order();
        assert_eq!(scopes.len(), 3);
        assert_eq!(scopes[0], SecretScope::environment("myapp", "prod"));
        assert_eq!(scopes[1], SecretScope::project("myapp"));
        assert_eq!(scopes[2], SecretScope::global());
    }

    #[test]
    fn secret_context_project_only() {
        let ctx = SecretContext::new().with_project("myapp");

        let scopes = ctx.resolution_order();
        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes[0], SecretScope::project("myapp"));
        assert_eq!(scopes[1], SecretScope::global());
    }

    #[test]
    fn secret_context_global_only() {
        let ctx = SecretContext::new();

        let scopes = ctx.resolution_order();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0], SecretScope::global());
    }

    #[test]
    fn secret_value_uses_constant_time_comparison() {
        // Equal values
        let value1 = SecretValue::new("correct_password_12345");
        let value2 = SecretValue::new("correct_password_12345");
        assert_eq!(value1, value2);

        // Different values, same length
        let value3 = SecretValue::new("wrong_password__12345");
        assert_ne!(value1, value3);

        // Different values, different lengths
        let value4 = SecretValue::new("short");
        assert_ne!(value1, value4);

        // Empty comparison
        let empty1 = SecretValue::new("");
        let empty2 = SecretValue::new("");
        assert_eq!(empty1, empty2);

        // Empty vs non-empty
        assert_ne!(empty1, value1);
    }

    #[test]
    fn secret_value_empty_string() {
        let empty = SecretValue::new("");
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.expose(), "");
    }

    #[test]
    fn secret_value_large_value() {
        let large_data = "x".repeat(1024 * 1024);
        let large_secret = SecretValue::new(large_data.clone());

        assert_eq!(large_secret.len(), 1024 * 1024);
        assert_eq!(large_secret.expose(), &large_data);
    }

    #[test]
    fn secret_value_special_characters() {
        // Unicode characters
        let unicode = SecretValue::new("Hello 世界 🔐 مرحبا");
        assert!(unicode.len() > 13);
        assert!(unicode.expose().contains('🔐'));

        // Control characters
        let control = SecretValue::new("line1\nline2\ttab\rreturn\0null");
        assert!(control.expose().contains('\n'));
        assert!(control.expose().contains('\0'));

        // Special characters that might need escaping
        let special = SecretValue::new("quote\"backslash\\percent%underscore_");
        assert_eq!(special.expose(), "quote\"backslash\\percent%underscore_");
    }

    #[test]
    fn secret_value_from_bytes_invalid_utf8() {
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let result = SecretValue::from_bytes(&invalid_utf8);

        assert!(result.is_err(), "Invalid UTF-8 must be rejected");
    }
}
