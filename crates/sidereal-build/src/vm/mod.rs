//! Firecracker VM-based compilation.
//!
//! This module provides `FirecrackerCompiler` which compiles Rust code
//! inside isolated Firecracker microVMs instead of bubblewrap sandboxes.

mod compiler;

pub use compiler::{FirecrackerCompiler, VmCompilerConfig};
