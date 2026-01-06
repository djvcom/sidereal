mod error;
mod traits;
mod types;

#[cfg(feature = "memory")]
mod memory;

#[cfg(feature = "valkey")]
mod valkey;

#[cfg(feature = "postgres")]
mod postgres;

#[cfg(feature = "config")]
mod config;

#[cfg(feature = "config")]
mod provider;

pub use error::{KvError, LockError, QueueError, StateError};
pub use traits::{KvBackend, LockBackend, LockOps, QueueBackend};
pub use types::{LockGuard, Message, MessageId};

#[cfg(feature = "memory")]
pub use memory::{MemoryKv, MemoryLock, MemoryLockProvider, MemoryQueue};

#[cfg(feature = "valkey")]
pub use valkey::{ValkeyKv, ValkeyLock, ValkeyLockProvider};

#[cfg(feature = "postgres")]
pub use postgres::PostgresQueue;

#[cfg(feature = "config")]
pub use config::{BackendConfig, KvConfig, LockConfig, QueueConfig, StateConfig};

#[cfg(feature = "config")]
pub use provider::StateProvider;
