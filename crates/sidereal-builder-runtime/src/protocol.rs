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

/// Git repository configuration for cloning inside the VM.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct GitConfig {
    /// Repository URL (HTTPS or SSH).
    pub repo_url: String,
    /// Branch name.
    pub branch: String,
    /// Commit SHA to checkout.
    pub commit_sha: String,
    /// Private SSH key contents for private repos (optional).
    pub ssh_key: Option<String>,
    /// Subdirectory path within the repo (optional).
    pub subpath: Option<String>,
}

/// S3-compatible storage configuration for artifacts and caches.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    /// S3 endpoint URL (e.g., "https://s3.example.com").
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// AWS region.
    pub region: String,
    /// Access key ID.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
}

/// Cache configuration for registry and target caches.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CacheConfig {
    /// S3 key for cargo registry cache (compressed archive).
    pub registry_key: Option<String>,
    /// S3 key for target cache (compressed archive).
    pub target_key: Option<String>,
}

/// Runtime binary configuration.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// S3 key to download the runtime binary from.
    pub s3_key: String,
}

/// Request sent to a builder VM to initiate a full build pipeline.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BuildRequest {
    /// Unique identifier for this build.
    pub build_id: String,

    /// Project identifier.
    pub project_id: String,

    /// Git repository configuration.
    pub git: GitConfig,

    /// S3 storage configuration.
    pub s3: S3Config,

    /// Cache configuration (optional).
    pub cache: Option<CacheConfig>,

    /// Runtime binary configuration.
    pub runtime: RuntimeConfig,

    /// Rust target triple (e.g., "x86_64-unknown-linux-musl").
    pub target_triple: String,

    /// Whether to build in release mode.
    pub release: bool,

    /// Whether to use locked dependencies (--locked).
    pub locked: bool,

    /// S3 key prefix for uploading the artifact.
    pub artifact_key_prefix: String,
}

impl BuildRequest {
    /// Get source path as `PathBuf` (hardcoded to /source in VM).
    #[must_use]
    pub fn source_path(&self) -> PathBuf {
        PathBuf::from("/source")
    }

    /// Get target path as `PathBuf` (hardcoded to /target in VM).
    #[must_use]
    pub fn target_path(&self) -> PathBuf {
        PathBuf::from("/target")
    }

    /// Get cargo home as `PathBuf` (hardcoded to /cargo in VM).
    #[must_use]
    pub fn cargo_home(&self) -> PathBuf {
        PathBuf::from("/cargo")
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

    /// Cloning the git repository.
    Cloning {
        /// Repository URL being cloned.
        repo: String,
    },

    /// Pulling caches from S3.
    PullingCache,

    /// Pushing caches to S3.
    PushingCache,

    /// Creating the artifact rootfs image.
    CreatingArtifact,

    /// Uploading artifact to S3.
    UploadingArtifact {
        /// Upload progress (0.0 to 1.0).
        progress: f32,
    },

    /// Discovering deployable projects.
    DiscoveringProjects,

    /// Downloading runtime binary.
    DownloadingRuntime,
}

/// Information about a discovered function in the built artifact.
#[derive(
    Archive,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct FunctionInfo {
    /// Function name.
    pub name: String,
    /// HTTP route (if HTTP-triggered).
    pub route: Option<String>,
    /// HTTP method (if HTTP-triggered).
    pub method: Option<String>,
    /// Queue name (if queue-triggered).
    pub queue: Option<String>,
}

/// Final result of a build operation.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct BuildResult {
    /// Whether the build succeeded.
    pub success: bool,

    /// Exit code from cargo (0 if artifact creation failed after compile).
    pub exit_code: i32,

    /// Total build duration in seconds.
    pub duration_secs: f64,

    /// S3 URL of the uploaded artifact (if successful).
    pub artifact_url: Option<String>,

    /// SHA-256 hash of the artifact (if successful).
    pub artifact_hash: Option<String>,

    /// Discovered functions in the built project.
    pub functions: Vec<FunctionInfo>,

    /// Combined stdout from the build.
    pub stdout: String,

    /// Combined stderr from the build.
    pub stderr: String,

    /// Error message (if build failed).
    pub error_message: Option<String>,
}

impl BuildResult {
    /// Create a successful build result with artifact information.
    #[must_use]
    pub fn success(
        artifact_url: String,
        artifact_hash: String,
        functions: Vec<FunctionInfo>,
        duration_secs: f64,
    ) -> Self {
        Self {
            success: true,
            exit_code: 0,
            duration_secs,
            artifact_url: Some(artifact_url),
            artifact_hash: Some(artifact_hash),
            functions,
            stdout: String::new(),
            stderr: String::new(),
            error_message: None,
        }
    }

    /// Create a successful compilation result (no artifact yet).
    ///
    /// Used when compilation succeeded but artifact creation/upload
    /// will happen in a subsequent step.
    #[must_use]
    pub fn compile_success(duration_secs: f64) -> Self {
        Self {
            success: true,
            exit_code: 0,
            duration_secs,
            artifact_url: None,
            artifact_hash: None,
            functions: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            error_message: None,
        }
    }

    /// Create a failed build result.
    #[must_use]
    pub fn failure(error: impl Into<String>, exit_code: i32, duration_secs: f64) -> Self {
        Self {
            success: false,
            exit_code,
            duration_secs,
            artifact_url: None,
            artifact_hash: None,
            functions: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            error_message: Some(error.into()),
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
