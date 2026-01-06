use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StateConfig {
    #[serde(default)]
    pub kv: Option<KvConfig>,
    #[serde(default)]
    pub queue: Option<QueueConfig>,
    #[serde(default)]
    pub lock: Option<LockConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum KvConfig {
    #[default]
    Memory,
    #[cfg(feature = "valkey")]
    Valkey {
        url: String,
        #[serde(default)]
        namespace: Option<String>,
        #[serde(default = "default_pool_size")]
        pool_size: usize,
    },
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum QueueConfig {
    #[default]
    Memory,
    #[cfg(feature = "postgres")]
    Postgres {
        url: String,
        #[serde(default = "default_queue_table")]
        table: String,
    },
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum LockConfig {
    #[default]
    Memory,
    #[cfg(feature = "valkey")]
    Valkey {
        url: String,
        #[serde(default)]
        namespace: Option<String>,
        #[serde(default = "default_pool_size")]
        pool_size: usize,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum BackendConfig {
    Kv(KvConfig),
    Queue(QueueConfig),
    Lock(LockConfig),
}

#[cfg(feature = "valkey")]
fn default_pool_size() -> usize {
    10
}

#[cfg(feature = "postgres")]
fn default_queue_table() -> String {
    "sidereal_queue".to_string()
}
