//! Build worker implementation.
//!
//! Processes builds from the queue.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::artifact::{Artifact, ArtifactBuilder, ArtifactStore, BuildInput};
use crate::discovery;
use crate::error::{BuildError, BuildStage, CancelReason};
use crate::forge_auth::ForgeAuth;
use crate::queue::{BuildHandle, BuildQueue};
use crate::sandbox::{
    fetch_dependencies, FetchConfig, SandboxConfig, SandboxLimits, SandboxedCompiler,
};
use crate::source::SourceManager;
use crate::types::{ArtifactId, BuildRequest, BuildStatus, FunctionMetadata};

use super::{LimitsConfig, PathsConfig};

/// Payload sent to the callback URL when a build completes.
#[derive(Debug, Serialize)]
pub struct BuildCompletedCallback {
    /// Build ID.
    pub build_id: String,
    /// Project identifier.
    pub project_id: String,
    /// Target environment.
    pub environment: String,
    /// Git commit SHA.
    pub commit_sha: String,
    /// URL to the artifact in object storage.
    pub artifact_url: String,
    /// Functions discovered in the build.
    pub functions: Vec<CallbackFunctionMetadata>,
    /// Whether the build succeeded.
    pub success: bool,
    /// Error message (if build failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Function metadata in the format expected by sidereal-control.
#[derive(Debug, Serialize)]
pub struct CallbackFunctionMetadata {
    /// Function name.
    pub name: String,
    /// Trigger configuration.
    pub trigger: CallbackFunctionTrigger,
    /// Memory limit in MB.
    pub memory_mb: u32,
    /// Number of vCPUs.
    pub vcpus: u8,
}

/// Function trigger types for the callback.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CallbackFunctionTrigger {
    /// HTTP request trigger.
    Http {
        /// HTTP method.
        method: String,
        /// URL path pattern.
        path: String,
    },
    /// Queue message trigger.
    Queue {
        /// Queue name.
        queue: String,
    },
}

impl From<&FunctionMetadata> for CallbackFunctionMetadata {
    fn from(f: &FunctionMetadata) -> Self {
        let trigger = if let Some(queue) = &f.queue {
            CallbackFunctionTrigger::Queue {
                queue: queue.clone(),
            }
        } else {
            CallbackFunctionTrigger::Http {
                method: f.method.clone().unwrap_or_else(|| "GET".to_owned()),
                path: f.route.clone().unwrap_or_else(|| "/".to_owned()),
            }
        };

        Self {
            name: f.name.clone(),
            trigger,
            memory_mb: 128,
            vcpus: 1,
        }
    }
}

/// Successful build output containing artifact details and function metadata.
struct BuildOutput {
    artifact_id: ArtifactId,
    artifact_url: String,
    functions: Vec<FunctionMetadata>,
}

/// Build worker that processes builds from the queue.
pub struct BuildWorker {
    id: usize,
    queue: Arc<BuildQueue>,
    source_manager: Arc<SourceManager>,
    compiler: Arc<SandboxedCompiler>,
    artifact_builder: Arc<ArtifactBuilder>,
    artifact_store: Arc<ArtifactStore>,
    http_client: reqwest::Client,
    runtime_path: PathBuf,
    target_base_dir: PathBuf,
}

impl BuildWorker {
    /// Create a new build worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created.
    pub fn new(
        id: usize,
        queue: Arc<BuildQueue>,
        paths: &PathsConfig,
        limits: &LimitsConfig,
        artifact_store: Arc<ArtifactStore>,
        forge_auth: ForgeAuth,
    ) -> Result<Self, BuildError> {
        let source_manager = Arc::new(SourceManager::new(
            &paths.checkouts,
            &paths.caches,
            forge_auth,
        ));

        let sandbox_config = SandboxConfig {
            limits: SandboxLimits {
                timeout: limits.timeout(),
                memory_limit_mb: limits.memory_limit_mb,
                ..SandboxLimits::default()
            },
            ..SandboxConfig::default()
        };
        let compiler = Arc::new(SandboxedCompiler::new(sandbox_config));

        let artifact_builder = Arc::new(ArtifactBuilder::new(&paths.artifacts));

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| BuildError::Internal(format!("failed to create HTTP client: {e}")))?;

        Ok(Self {
            id,
            queue,
            source_manager,
            compiler,
            artifact_builder,
            artifact_store,
            http_client,
            runtime_path: paths.runtime.clone(),
            target_base_dir: paths.caches.join("targets"),
        })
    }

    /// Run the worker loop until the cancellation token is triggered.
    pub async fn run(&self, cancel: CancellationToken) {
        info!(worker_id = self.id, "build worker started");

        loop {
            tokio::select! {
                biased;

                () = cancel.cancelled() => {
                    info!(worker_id = self.id, "build worker shutting down");
                    break;
                }

                result = self.queue.next() => {
                    if let Some((request, handle)) = result {
                        self.process_build(request, handle).await;
                    }
                }
            }
        }

        info!(worker_id = self.id, "build worker stopped");
    }

    async fn process_build(&self, request: BuildRequest, handle: Arc<BuildHandle>) {
        let build_id = request.id.clone();
        let project_id = request.project_id.clone();
        let branch = request.branch.clone();
        let commit_sha = request.commit_sha.clone();
        let environment = request
            .environment
            .clone()
            .unwrap_or_else(|| "default".to_owned());
        let callback_url = request.callback_url.clone();

        info!(
            worker_id = self.id,
            build_id = %build_id,
            project = %project_id,
            branch = %branch,
            commit = %request.short_sha(),
            "starting build"
        );

        let result = self.execute_build(&request, &handle.token).await;

        match result {
            Ok(output) => {
                info!(
                    build_id = %build_id,
                    artifact_id = %output.artifact_id,
                    "build completed successfully"
                );
                self.queue.update_status(
                    &build_id,
                    BuildStatus::Completed {
                        artifact_id: output.artifact_id,
                    },
                );

                if let Some(url) = callback_url {
                    let callback = BuildCompletedCallback {
                        build_id: build_id.to_string(),
                        project_id: project_id.to_string(),
                        environment: environment.clone(),
                        commit_sha: commit_sha.clone(),
                        artifact_url: output.artifact_url,
                        functions: output
                            .functions
                            .iter()
                            .map(CallbackFunctionMetadata::from)
                            .collect(),
                        success: true,
                        error: None,
                    };
                    self.send_callback(&url, callback).await;
                }
            }
            Err(e) => {
                if handle.token.is_cancelled() {
                    warn!(build_id = %build_id, "build was cancelled");
                } else {
                    error!(build_id = %build_id, error = %e, "build failed");
                    let (stage, error_msg) = error_to_stage_and_message(&e);
                    self.queue.update_status(
                        &build_id,
                        BuildStatus::Failed {
                            error: error_msg.clone(),
                            stage,
                        },
                    );

                    if let Some(url) = callback_url {
                        let callback = BuildCompletedCallback {
                            build_id: build_id.to_string(),
                            project_id: project_id.to_string(),
                            environment: environment.clone(),
                            commit_sha: commit_sha.clone(),
                            artifact_url: String::new(),
                            functions: Vec::new(),
                            success: false,
                            error: Some(error_msg),
                        };
                        self.send_callback(&url, callback).await;
                    }
                }
            }
        }
    }

    async fn send_callback(&self, url: &str, callback: BuildCompletedCallback) {
        info!(
            url = %url,
            build_id = %callback.build_id,
            success = callback.success,
            "sending build completion callback"
        );

        match self.http_client.post(url).json(&callback).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    info!(build_id = %callback.build_id, "callback sent successfully");
                } else {
                    warn!(
                        build_id = %callback.build_id,
                        status = %response.status(),
                        "callback returned non-success status"
                    );
                }
            }
            Err(e) => {
                error!(
                    build_id = %callback.build_id,
                    error = %e,
                    "failed to send callback"
                );
            }
        }
    }

    async fn execute_build(
        &self,
        request: &BuildRequest,
        cancel: &CancellationToken,
    ) -> Result<BuildOutput, BuildError> {
        // Phase 1: Checkout source
        self.queue
            .update_status(&request.id, BuildStatus::CheckingOut);

        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: CancelReason::UserRequested,
            });
        }

        let mut checkout = self
            .source_manager
            .checkout(&request.project_id, &request.repo_url, &request.commit_sha)
            .await?;

        if let Some(ref subpath) = request.path {
            let subdir = checkout.path.join(subpath);
            if !subdir.exists() {
                return Err(BuildError::CompileFailed {
                    stderr: format!("subdirectory '{}' not found in repository", subpath),
                    exit_code: 0,
                });
            }
            checkout.path = subdir.clone();
            checkout.cargo_toml = {
                let toml = subdir.join("Cargo.toml");
                toml.exists().then_some(toml)
            };
            checkout.cargo_lock = {
                let lock = subdir.join("Cargo.lock");
                lock.exists().then_some(lock)
            };
        }

        info!(
            build_id = %request.id,
            path = %checkout.path.display(),
            "source checked out"
        );

        // Phase 2: Fetch dependencies (outside sandbox, needs network)
        self.queue
            .update_status(&request.id, BuildStatus::FetchingDeps);

        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: CancelReason::UserRequested,
            });
        }

        let fetch_config = FetchConfig {
            cargo_home: self.compiler.cargo_home().to_owned(),
            ..FetchConfig::default()
        };

        let fetch_output = fetch_dependencies(
            &request.project_id,
            &checkout,
            &fetch_config,
            cancel.clone(),
        )
        .await?;

        info!(
            build_id = %request.id,
            duration_secs = fetch_output.duration.as_secs_f32(),
            "dependencies fetched"
        );

        // Phase 3: Compile (sandboxed)
        self.queue
            .update_status(&request.id, BuildStatus::Compiling { progress: None });

        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: CancelReason::UserRequested,
            });
        }

        let target_dir = self.target_base_dir.join(request.id.as_str());
        let compile_output = self
            .compiler
            .compile(&request.project_id, &checkout, &target_dir, cancel.clone())
            .await?;

        info!(
            build_id = %request.id,
            binary = %compile_output.binary_path.display(),
            duration_secs = compile_output.duration.as_secs(),
            "compilation completed"
        );

        // Phase 3: Discover functions
        let functions = discovery::discover_from_source(&checkout.path).unwrap_or_default();
        info!(
            build_id = %request.id,
            function_count = functions.len(),
            "functions discovered"
        );

        // Keep a copy of functions for the callback
        let discovered_functions = functions.clone();

        // Phase 4: Generate artifact
        self.queue
            .update_status(&request.id, BuildStatus::GeneratingArtifact);

        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: CancelReason::UserRequested,
            });
        }

        let output_dir = PathBuf::from("/tmp")
            .join("sidereal-build")
            .join(request.id.as_str());

        let artifact = self.artifact_builder.build(BuildInput {
            project_id: &request.project_id,
            branch: &request.branch,
            commit_sha: &request.commit_sha,
            runtime_binary: &self.runtime_path,
            user_binary: &compile_output.binary_path,
            functions,
            output_dir: &output_dir,
        })?;

        let artifact_id = artifact.id.clone();

        // Phase 5: Upload artifact
        let rootfs_path = output_dir.join("rootfs.ext4");
        let artifact_url = self.upload_artifact(&artifact, &rootfs_path).await?;

        // Cleanup
        self.cleanup_build_dirs(&output_dir, &target_dir).await;

        Ok(BuildOutput {
            artifact_id,
            artifact_url,
            functions: discovered_functions,
        })
    }

    async fn upload_artifact(
        &self,
        artifact: &Artifact,
        rootfs_path: &Path,
    ) -> Result<String, BuildError> {
        let artifact_url = self.artifact_store.upload(artifact, rootfs_path).await?;
        info!(
            artifact_id = %artifact.id,
            project = %artifact.project_id,
            branch = %artifact.branch,
            url = %artifact_url,
            "artifact uploaded"
        );
        Ok(artifact_url)
    }

    async fn cleanup_build_dirs(&self, output_dir: &Path, target_dir: &Path) {
        if let Err(e) = tokio::fs::remove_dir_all(output_dir).await {
            warn!(
                path = %output_dir.display(),
                error = %e,
                "failed to clean up build output"
            );
        }
        if let Err(e) = tokio::fs::remove_dir_all(target_dir).await {
            warn!(
                path = %target_dir.display(),
                error = %e,
                "failed to clean up target directory"
            );
        }
    }
}

/// Map a BuildError to a stage and error message.
fn error_to_stage_and_message(e: &BuildError) -> (BuildStage, String) {
    match e {
        BuildError::GitClone { .. }
        | BuildError::GitFetch(_)
        | BuildError::GitCheckout { .. }
        | BuildError::InvalidBranchName { .. }
        | BuildError::SymlinkEscape { .. } => (BuildStage::Checkout, e.to_string()),

        BuildError::CargoFetch(_) | BuildError::MissingLockfile => {
            (BuildStage::FetchDeps, e.to_string())
        }

        BuildError::AuditFailed { .. }
        | BuildError::BlockedCrate { .. }
        | BuildError::ExternalDependency { .. } => (BuildStage::Audit, e.to_string()),

        BuildError::RootfsGeneration(_)
        | BuildError::ArtifactStorage(_)
        | BuildError::ArtifactTooLarge { .. }
        | BuildError::ArtifactSigning(_)
        | BuildError::ArtifactVerification(_) => (BuildStage::Artifact, e.to_string()),

        // Default to Compile stage for sandbox, compilation, timeout, cancel, and other errors
        _ => (BuildStage::Compile, e.to_string()),
    }
}
