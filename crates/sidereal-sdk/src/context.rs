//! Runtime context provided to functions.
//!
//! The `Context` provides access to platform services like KV storage,
//! queues, secrets, logging, and typed configuration.

use crate::config::{ConfigError, ConfigManager};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Runtime context provided to every function invocation.
///
/// # Example
///
/// ```ignore
/// #[sidereal_sdk::function]
/// async fn process_order(
///     req: HttpRequest<Order>,
///     ctx: Context,
/// ) -> HttpResponse<Receipt> {
///     // Access KV store
///     let item = ctx.kv().get::<Item>(&req.body.item_id).await?;
///
///     // Access secrets
///     let api_key = ctx.secret("STRIPE_API_KEY").await?;
///
///     // Logging
///     ctx.log().info("Processing order", &[("order_id", &req.body.id)]);
///
///     // ...
/// }
/// ```
#[derive(Clone)]
pub struct Context {
    inner: Arc<ContextInner>,
}

struct ContextInner {
    environment: String,
    function_name: String,
    request_id: String,
    deadline: Option<Instant>,
    kv: KvClient,
    secrets: SecretsClient,
    config: Option<ConfigManager>,
}

impl Context {
    /// Create a new context for local development.
    pub fn new_dev(function_name: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ContextInner {
                environment: "development".to_string(),
                function_name: function_name.into(),
                request_id: generate_request_id(),
                deadline: None,
                kv: KvClient::new_in_memory(),
                secrets: SecretsClient::new_env(),
                config: None,
            }),
        }
    }

    /// Create a new context with a configuration manager.
    pub fn with_config(function_name: impl Into<String>, config: ConfigManager) -> Self {
        let environment = config.active_environment().to_string();
        Self {
            inner: Arc::new(ContextInner {
                environment,
                function_name: function_name.into(),
                request_id: generate_request_id(),
                deadline: None,
                kv: KvClient::new_in_memory(),
                secrets: SecretsClient::new_env(),
                config: Some(config),
            }),
        }
    }

    /// Get the current environment name.
    pub fn environment(&self) -> &str {
        &self.inner.environment
    }

    /// Get the function name.
    pub fn function_name(&self) -> &str {
        &self.inner.function_name
    }

    /// Get the unique request ID.
    pub fn request_id(&self) -> &str {
        &self.inner.request_id
    }

    /// Get the remaining time before the deadline.
    pub fn remaining_time(&self) -> Option<Duration> {
        self.inner
            .deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
    }

    /// Access the key-value store.
    pub fn kv(&self) -> &KvClient {
        &self.inner.kv
    }

    /// Get a secret by name.
    pub async fn secret(&self, name: &str) -> Result<String, ContextError> {
        self.inner.secrets.get(name).await
    }

    /// Create a logger for structured logging.
    pub fn log(&self) -> Logger {
        Logger {
            request_id: self.inner.request_id.clone(),
            function_name: self.inner.function_name.clone(),
        }
    }

    /// Access typed configuration from the `[app.*]` sections in sidereal.toml.
    ///
    /// # Example
    ///
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct StripeConfig {
    ///     api_key: String,
    ///     webhook_secret: String,
    /// }
    ///
    /// #[sidereal_sdk::function]
    /// async fn process_payment(req: HttpRequest<PaymentRequest>, ctx: Context) -> HttpResponse<()> {
    ///     let stripe: StripeConfig = ctx.config("stripe")?;
    ///     // Use stripe.api_key...
    /// }
    /// ```
    pub fn config<T: DeserializeOwned>(&self, section: &str) -> Result<T, ConfigError> {
        match &self.inner.config {
            Some(config_manager) => config_manager.section(section),
            None => Err(ConfigError::SectionNotFound(format!(
                "{} (no configuration loaded)",
                section
            ))),
        }
    }

    /// Access the underlying ConfigManager if available.
    pub fn config_manager(&self) -> Option<&ConfigManager> {
        self.inner.config.as_ref()
    }
}

/// Key-value store client.
#[derive(Clone)]
pub struct KvClient {
    inner: Arc<KvClientInner>,
}

enum KvClientInner {
    InMemory(std::sync::RwLock<HashMap<String, Vec<u8>>>),
}

impl KvClient {
    fn new_in_memory() -> Self {
        Self {
            inner: Arc::new(KvClientInner::InMemory(std::sync::RwLock::new(
                HashMap::new(),
            ))),
        }
    }

    /// Get a value from the KV store.
    pub async fn get<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, ContextError> {
        match &*self.inner {
            KvClientInner::InMemory(map) => {
                let map = map.read().map_err(|_| ContextError::Internal)?;
                match map.get(key) {
                    Some(bytes) => {
                        let value = serde_json::from_slice(bytes)
                            .map_err(|e| ContextError::Deserialisation(e.to_string()))?;
                        Ok(Some(value))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    /// Put a value into the KV store.
    pub async fn put<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<(), ContextError> {
        let bytes =
            serde_json::to_vec(value).map_err(|e| ContextError::Serialisation(e.to_string()))?;

        match &*self.inner {
            KvClientInner::InMemory(map) => {
                let mut map = map.write().map_err(|_| ContextError::Internal)?;
                map.insert(key.to_string(), bytes);
                Ok(())
            }
        }
    }

    /// Delete a key from the KV store.
    pub async fn delete(&self, key: &str) -> Result<bool, ContextError> {
        match &*self.inner {
            KvClientInner::InMemory(map) => {
                let mut map = map.write().map_err(|_| ContextError::Internal)?;
                Ok(map.remove(key).is_some())
            }
        }
    }
}

/// Secrets client for accessing sensitive configuration.
#[derive(Clone)]
struct SecretsClient {
    inner: Arc<SecretsClientInner>,
}

enum SecretsClientInner {
    Env,
}

impl SecretsClient {
    fn new_env() -> Self {
        Self {
            inner: Arc::new(SecretsClientInner::Env),
        }
    }

    async fn get(&self, name: &str) -> Result<String, ContextError> {
        match &*self.inner {
            SecretsClientInner::Env => std::env::var(name)
                .map_err(|_| ContextError::SecretNotFound(name.to_string())),
        }
    }
}

/// Structured logger.
pub struct Logger {
    request_id: String,
    function_name: String,
}

impl Logger {
    pub fn info(&self, message: &str, fields: &[(&str, &str)]) {
        let fields_str: String = fields
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!(
            "[INFO] [{}] [{}] {} {}",
            self.request_id, self.function_name, message, fields_str
        );
    }

    pub fn warn(&self, message: &str, fields: &[(&str, &str)]) {
        let fields_str: String = fields
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!(
            "[WARN] [{}] [{}] {} {}",
            self.request_id, self.function_name, message, fields_str
        );
    }

    pub fn error(&self, message: &str, fields: &[(&str, &str)]) {
        let fields_str: String = fields
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!(
            "[ERROR] [{}] [{}] {} {}",
            self.request_id, self.function_name, message, fields_str
        );
    }
}

/// Errors that can occur when using context services.
#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("secret not found: {0}")]
    SecretNotFound(String),

    #[error("serialisation error: {0}")]
    Serialisation(String),

    #[error("deserialisation error: {0}")]
    Deserialisation(String),

    #[error("internal error")]
    Internal,
}

fn generate_request_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let random: u32 = rand_simple();
    format!("{:x}-{:08x}", timestamp, random)
}

fn rand_simple() -> u32 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish() as u32
}
