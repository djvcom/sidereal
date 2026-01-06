//! Function resolution abstractions.

mod static_resolver;

pub use static_resolver::StaticResolver;

use async_trait::async_trait;
use regex::Regex;
use std::sync::LazyLock;

use crate::backend::WorkerAddress;
use crate::error::GatewayError;

/// Maximum length for a function name.
pub const MAX_FUNCTION_NAME_LENGTH: usize = 64;

/// Regex pattern for valid function names.
static FUNCTION_NAME_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z][a-z0-9_]*$").unwrap());

/// Information about a resolved function.
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub backend_address: WorkerAddress,
}

/// Resolves function names to backend addresses.
#[async_trait]
pub trait FunctionResolver: Send + Sync + std::fmt::Debug {
    /// Resolve a function by name.
    async fn resolve(&self, function_name: &str) -> Result<Option<FunctionInfo>, GatewayError>;

    /// List all known functions (for health/debug endpoints).
    async fn list_functions(&self) -> Result<Vec<FunctionInfo>, GatewayError>;

    /// Refresh the resolver's cache (if applicable).
    async fn refresh(&self) -> Result<(), GatewayError> {
        Ok(())
    }
}

/// Validate a function name.
pub fn validate_function_name(name: &str) -> Result<(), GatewayError> {
    // Check length
    if name.is_empty() {
        return Err(GatewayError::InvalidFunctionName(
            "function name cannot be empty".into(),
        ));
    }

    if name.len() > MAX_FUNCTION_NAME_LENGTH {
        return Err(GatewayError::InvalidFunctionName(format!(
            "function name exceeds maximum length of {} characters",
            MAX_FUNCTION_NAME_LENGTH
        )));
    }

    // Check for path traversal
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return Err(GatewayError::InvalidFunctionName(
            "function name contains invalid characters".into(),
        ));
    }

    // Check pattern
    if !FUNCTION_NAME_PATTERN.is_match(name) {
        return Err(GatewayError::InvalidFunctionName(
            "function name must start with a letter and contain only lowercase letters, numbers, and underscores".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_valid_names() {
        assert!(validate_function_name("hello").is_ok());
        assert!(validate_function_name("hello_world").is_ok());
        assert!(validate_function_name("hello123").is_ok());
        assert!(validate_function_name("a").is_ok());
    }

    #[test]
    fn validate_invalid_empty() {
        assert!(validate_function_name("").is_err());
    }

    #[test]
    fn validate_invalid_too_long() {
        let long_name = "a".repeat(MAX_FUNCTION_NAME_LENGTH + 1);
        assert!(validate_function_name(&long_name).is_err());
    }

    #[test]
    fn validate_invalid_path_traversal() {
        assert!(validate_function_name("..").is_err());
        assert!(validate_function_name("../etc").is_err());
        assert!(validate_function_name("foo/bar").is_err());
        assert!(validate_function_name("foo\\bar").is_err());
    }

    #[test]
    fn validate_invalid_pattern() {
        assert!(validate_function_name("123abc").is_err()); // starts with number
        assert!(validate_function_name("Hello").is_err()); // uppercase
        assert!(validate_function_name("hello-world").is_err()); // hyphen
        assert!(validate_function_name("_hello").is_err()); // starts with underscore
    }
}
