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
    functions: HashMap<String, Vec<WorkerAddress>>,
}

impl StaticResolver {
    /// Create a new static resolver from routing configuration.
    pub fn from_config(config: &RoutingConfig) -> Result<Self, GatewayError> {
        let functions = match config {
            RoutingConfig::Static { functions, .. } => functions
                .iter()
                .map(|(name, backend_config)| {
                    let addresses: Vec<WorkerAddress> = backend_config
                        .addresses
                        .iter()
                        .map(|addr| match addr {
                            BackendAddress::Http { url } => {
                                WorkerAddress::Http { url: url.clone() }
                            }
                            BackendAddress::Vsock { uds_path, port } => WorkerAddress::Vsock {
                                uds_path: uds_path.clone(),
                                port: *port,
                            },
                        })
                        .collect();
                    (name.clone(), addresses)
                })
                .collect(),
            RoutingConfig::Discovery { .. } => {
                return Err(GatewayError::Config(
                    "Discovery routing not yet implemented".into(),
                ));
            }
            RoutingConfig::Scheduler(_) => {
                return Err(GatewayError::Config(
                    "Use SchedulerResolver for scheduler routing mode".into(),
                ));
            }
        };

        Ok(Self { functions })
    }

    /// Create a resolver with a single HTTP function (for testing).
    pub fn single(name: impl Into<String>, url: impl Into<String>) -> Self {
        let mut functions = HashMap::new();
        functions.insert(name.into(), vec![WorkerAddress::Http { url: url.into() }]);
        Self { functions }
    }

    /// Create a resolver with multiple backends for a function (for testing).
    pub fn with_backends(name: impl Into<String>, urls: Vec<String>) -> Self {
        let mut functions = HashMap::new();
        let backends = urls
            .into_iter()
            .map(|url| WorkerAddress::Http { url })
            .collect();
        functions.insert(name.into(), backends);
        Self { functions }
    }
}

#[async_trait]
impl FunctionResolver for StaticResolver {
    async fn resolve(&self, function_name: &str) -> Result<Option<FunctionInfo>, GatewayError> {
        Ok(self
            .functions
            .get(function_name)
            .map(|addresses| FunctionInfo {
                name: function_name.to_string(),
                backend_addresses: addresses.clone(),
            }))
    }

    async fn list_functions(&self) -> Result<Vec<FunctionInfo>, GatewayError> {
        Ok(self
            .functions
            .iter()
            .map(|(name, addresses)| FunctionInfo {
                name: name.clone(),
                backend_addresses: addresses.clone(),
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
        assert_eq!(info.backend_addresses.len(), 1);
        match &info.backend_addresses[0] {
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

    #[tokio::test]
    async fn resolve_with_multiple_backends() {
        let resolver = StaticResolver::with_backends(
            "hello",
            vec![
                "http://backend1:8080".into(),
                "http://backend2:8080".into(),
                "http://backend3:8080".into(),
            ],
        );
        let result = resolver.resolve("hello").await.unwrap();

        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.backend_addresses.len(), 3);
    }
}
