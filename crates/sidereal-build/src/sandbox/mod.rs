//! Sandboxed compilation using bubblewrap.
//!
//! Provides isolated build environments for compiling untrusted user code.

mod bubblewrap;
mod compile;
mod fetch;

pub use bubblewrap::{BubblewrapBuilder, SandboxLimits};
pub use compile::{CompileOutput, SandboxConfig, SandboxedCompiler};
pub use fetch::{fetch_dependencies, FetchConfig, FetchOutput};
