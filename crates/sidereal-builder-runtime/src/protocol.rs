//! Wire protocol types for builder VM communication.
//!
//! These types mirror the protocol defined in sidereal-build::protocol
//! but are kept local to avoid pulling in heavy dependencies.
//!
//! Paths are represented as strings in the wire protocol for rkyv compatibility.

use std::path::PathBuf;

use rkyv::{Archive, Deserialize, Serialize};

/// Vsock port for builder VM communication.
pub const BUILD_PORT: u32 = 1028;

/// Request sent to a builder VM to initiate compilation.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BuildRequest {
    /// Unique identifier for this build.
    pub build_id: String,

    /// Mount point for source code inside the VM (as string for wire format).
    pub source_path: String,

    /// Output directory for compilation artifacts (as string for wire format).
    pub target_path: String,

    /// Cargo registry cache directory (as string for wire format).
    pub cargo_home: String,

    /// Rust target triple (e.g., "x86_64-unknown-linux-musl").
    pub target_triple: String,

    /// Whether to build in release mode.
    pub release: bool,

    /// Whether to use locked dependencies (--locked).
    pub locked: bool,
}

impl BuildRequest {
    /// Get source path as `PathBuf`.
    #[must_use]
    pub fn source_path(&self) -> PathBuf {
        PathBuf::from(&self.source_path)
    }

    /// Get target path as `PathBuf`.
    #[must_use]
    pub fn target_path(&self) -> PathBuf {
        PathBuf::from(&self.target_path)
    }

    /// Get cargo home as `PathBuf`.
    #[must_use]
    pub fn cargo_home(&self) -> PathBuf {
        PathBuf::from(&self.cargo_home)
    }
}

/// Output streamed during the build process.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum BuildOutput {
    /// Standard output line from cargo.
    Stdout(String),

    /// Standard error line from cargo.
    Stderr(String),

    /// Progress update during compilation.
    Progress {
        /// Current build stage (e.g., "Compiling", "Linking").
        stage: String,
        /// Progress percentage (0.0 to 1.0).
        progress: f32,
    },
}

/// Final result of a build operation.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct BuildResult {
    /// Whether the build succeeded.
    pub success: bool,

    /// Exit code from cargo.
    pub exit_code: i32,

    /// Information about compiled binaries.
    pub binaries: Vec<BinaryInfo>,

    /// Total build duration in seconds.
    pub duration_secs: f64,

    /// Combined stdout from the build.
    pub stdout: String,

    /// Combined stderr from the build.
    pub stderr: String,
}

impl BuildResult {
    /// Create a successful build result.
    #[must_use]
    pub fn success(binaries: Vec<BinaryInfo>, duration_secs: f64) -> Self {
        Self {
            success: true,
            exit_code: 0,
            binaries,
            duration_secs,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    /// Create a failed build result.
    #[must_use]
    pub fn failure(exit_code: i32, stderr: String, duration_secs: f64) -> Self {
        Self {
            success: false,
            exit_code,
            binaries: Vec::new(),
            duration_secs,
            stdout: String::new(),
            stderr,
        }
    }

    /// Add stdout to the result.
    #[must_use]
    pub fn with_stdout(mut self, stdout: String) -> Self {
        self.stdout = stdout;
        self
    }

    /// Add stderr to the result.
    #[must_use]
    pub fn with_stderr(mut self, stderr: String) -> Self {
        self.stderr = stderr;
        self
    }
}

/// Information about a compiled binary.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BinaryInfo {
    /// Binary name (without extension).
    pub name: String,

    /// Path to the compiled binary inside the VM (as string for wire format).
    pub path: String,

    /// Path to the crate directory containing Cargo.toml (as string for wire format).
    pub crate_dir: String,
}

impl BinaryInfo {
    /// Create new binary info.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        path: impl Into<String>,
        crate_dir: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            crate_dir: crate_dir.into(),
        }
    }
}

/// Message envelope for builder VM communication.
///
/// Wraps all messages with a type discriminant for protocol handling.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum BuildMessage {
    /// Build request from host to VM.
    Request(BuildRequest),

    /// Streaming output from VM to host.
    Output(BuildOutput),

    /// Final result from VM to host.
    Result(BuildResult),
}

impl From<BuildRequest> for BuildMessage {
    fn from(request: BuildRequest) -> Self {
        Self::Request(request)
    }
}

impl From<BuildOutput> for BuildMessage {
    fn from(output: BuildOutput) -> Self {
        Self::Output(output)
    }
}

impl From<BuildResult> for BuildMessage {
    fn from(result: BuildResult) -> Self {
        Self::Result(result)
    }
}
