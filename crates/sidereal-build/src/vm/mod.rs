//! Firecracker VM-based build pipeline.
//!
//! This module provides `FirecrackerCompiler` which runs a complete build
//! pipeline inside isolated Firecracker microVMs. The VM handles cloning,
//! caching, compilation, project discovery, artifact creation, and upload.

mod compiler;

pub use compiler::{
    BuildInput, BuildOutput as VmBuildOutput, FirecrackerCompiler, VmCompilerConfig,
};
