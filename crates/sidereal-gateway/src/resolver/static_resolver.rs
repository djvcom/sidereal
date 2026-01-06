//! Static configuration-based function resolver.

use async_trait::async_trait;
use std::collections::HashMap;

use super::{FunctionInfo, FunctionResolver};
use crate::backend::WorkerAddress;
use crate::config::{BackendAddress, RoutingConfig};
use crate::error::GatewayError;

/// Resolver that uses static configuration for function routing.
#[derive(Debug)]
pub struct StaticResolver {
    functions: HashMap<String, WorkerAddress>,
}

impl StaticResolver {
    /// Create a new static resolver from routing configuration.
    pub fn from_config(config: &RoutingConfig) -> Result<Self, GatewayError> {
        let functions = match config {
            RoutingConfig::Static { functions } => functions
                .iter()
                .map(|(name, addr)| {
                    let worker_addr = match addr {
                        BackendAddress::Http { url } => WorkerAddress::Http { url: url.clone() },
                        BackendAddress::Vsock { uds_path, port } => WorkerAddress::Vsock {
                            uds_path: uds_path.clone(),
                            port: *port,
                        },
                    };
                    (name.clone(), worker_addr)
                })
                .collect(),
            RoutingConfig::Discovery { .. } => {
                return Err(GatewayError::Config(
                    "Discovery routing not yet implemented".into(),
                ));
            }
        };

        Ok(Self { functions })
    }

    /// Create a resolver with a single HTTP function (for testing).
    pub fn single(name: impl Into<String>, url: impl Into<String>) -> Self {
        let mut functions = HashMap::new();
        functions.insert(name.into(), WorkerAddress::Http { url: url.into() });
        Self { functions }
    }
}

#[async_trait]
impl FunctionResolver for StaticResolver {
    async fn resolve(&self, function_name: &str) -> Result<Option<FunctionInfo>, GatewayError> {
        Ok(self.functions.get(function_name).map(|addr| FunctionInfo {
            name: function_name.to_string(),
            backend_address: addr.clone(),
        }))
    }

    async fn list_functions(&self) -> Result<Vec<FunctionInfo>, GatewayError> {
        Ok(self
            .functions
            .iter()
            .map(|(name, addr)| FunctionInfo {
                name: name.clone(),
                backend_address: addr.clone(),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_existing_function() {
        let resolver = StaticResolver::single("hello", "http://localhost:7850");
        let result = resolver.resolve("hello").await.unwrap();

        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.name, "hello");
        match info.backend_address {
            WorkerAddress::Http { url } => assert_eq!(url, "http://localhost:7850"),
            _ => panic!("Expected HTTP address"),
        }
    }

    #[tokio::test]
    async fn resolve_missing_function() {
        let resolver = StaticResolver::single("hello", "http://localhost:7850");
        let result = resolver.resolve("unknown").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn list_functions() {
        let resolver = StaticResolver::single("hello", "http://localhost:7850");
        let functions = resolver.list_functions().await.unwrap();

        assert_eq!(functions.len(), 1);
        assert_eq!(functions[0].name, "hello");
    }
}
