//! Secure secrets management for Sidereal.
//!
//! This crate provides a trait-based abstraction for secrets storage with
//! pluggable backends. Secrets are automatically protected in memory using
//! the `secrecy` and `zeroize` crates.
//!
//! # Backends
//!
//! - **Memory** (`memory` feature): In-memory storage for testing
//! - **Env** (`env` feature): Read-only environment variable backend
//! - **Native** (`native` feature): Encrypted SQLite storage using `age`
//!
//! # Scope Resolution
//!
//! Secrets support three-tier scoping with automatic resolution:
//!
//! 1. **Environment**: Most specific, project + environment pair
//! 2. **Project**: Available across all environments
//! 3. **Global**: Available everywhere
//!
//! When retrieving a secret, the backend searches from most specific to
//! least specific, returning the first match.
//!
//! # Example
//!
//! ```rust,ignore
//! use sidereal_secrets::{SecretsBackend, SecretValue, SecretScope, SecretContext};
//!
//! // Store a secret at environment scope
//! backend.set("API_KEY", SecretValue::new("sk_live_..."), &SecretScope::environment("myapp", "prod")).await?;
//!
//! // Retrieve with context (searches environment -> project -> global)
//! let ctx = SecretContext::new()
//!     .with_project("myapp")
//!     .with_environment("prod");
//! let value = backend.get("API_KEY", &ctx).await?;
//! ```

mod error;
mod traits;
mod types;

#[cfg(feature = "memory")]
mod memory;

#[cfg(feature = "env")]
mod env;

#[cfg(feature = "native")]
mod native;

#[cfg(feature = "config")]
mod config;

#[cfg(feature = "config")]
mod provider;

pub use error::{Redacted, SecretsError};
pub use traits::SecretsBackend;
pub use types::{SecretContext, SecretMetadata, SecretScope, SecretValue, SecretVersion};

#[cfg(feature = "memory")]
pub use memory::MemorySecrets;

#[cfg(feature = "env")]
pub use env::EnvSecrets;

#[cfg(feature = "native")]
pub use native::NativeSecrets;

#[cfg(feature = "config")]
pub use config::SecretsConfig;

#[cfg(feature = "config")]
pub use provider::SecretsProvider;
