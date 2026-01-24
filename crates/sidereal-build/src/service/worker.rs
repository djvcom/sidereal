//! Build worker implementation.
//!
//! Processes builds from the queue.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::artifact::{Artifact, ArtifactBuilder, ArtifactStore, BuildInput};
use crate::cache::{CacheConfig, CacheManager, CacheResult};
use crate::discovery;
use crate::error::{BuildError, BuildStage, CancelReason};
use crate::forge_auth::ForgeAuth;
use crate::project::{discover_projects, DeployableProject};
use crate::queue::{BuildHandle, BuildQueue};
use crate::sandbox::{fetch_dependencies, FetchConfig};
use crate::source::SourceManager;
use crate::types::{ArtifactId, BuildRequest, BuildStatus, FunctionMetadata};
use crate::vm::{FirecrackerCompiler, VmCompilerConfig};

use super::{LimitsConfig, PathsConfig, VmConfig};

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
    compiler: Arc<FirecrackerCompiler>,
    artifact_builder: Arc<ArtifactBuilder>,
    artifact_store: Arc<ArtifactStore>,
    cache_manager: Option<Arc<CacheManager>>,
    http_client: reqwest::Client,
    runtime_path: PathBuf,
    target_base_dir: PathBuf,
}

impl BuildWorker {
    /// Create a new build worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the compiler cannot be initialised.
    pub fn new(
        id: usize,
        queue: Arc<BuildQueue>,
        paths: &PathsConfig,
        limits: &LimitsConfig,
        artifact_store: Arc<ArtifactStore>,
        forge_auth: ForgeAuth,
        vm_config: &VmConfig,
    ) -> Result<Self, BuildError> {
        Self::with_cache(
            id,
            queue,
            paths,
            limits,
            artifact_store,
            forge_auth,
            vm_config,
            None,
        )
    }

    /// Create a new build worker with optional cache configuration.
    pub fn with_cache(
        id: usize,
        queue: Arc<BuildQueue>,
        paths: &PathsConfig,
        limits: &LimitsConfig,
        artifact_store: Arc<ArtifactStore>,
        forge_auth: ForgeAuth,
        vm_config: &VmConfig,
        cache_config: Option<CacheConfig>,
    ) -> Result<Self, BuildError> {
        let source_manager = Arc::new(SourceManager::new(
            &paths.checkouts,
            &paths.caches,
            forge_auth,
        ));

        let vm_compiler_config = VmCompilerConfig {
            kernel_path: vm_config.kernel_path.clone(),
            builder_rootfs: vm_config.builder_rootfs.clone(),
            cargo_cache_dir: paths.caches.join("cargo"),
            vcpu_count: vm_config.vcpu_count,
            mem_size_mib: vm_config.mem_size_mib,
            target: vm_config.target.clone(),
            timeout: limits.timeout(),
        };
        let work_dir = paths.artifacts.join("vm-work");
        let compiler = Arc::new(FirecrackerCompiler::new(vm_compiler_config, work_dir)?);

        let artifact_builder = Arc::new(ArtifactBuilder::new(&paths.artifacts));

        let cache_manager = cache_config
            .filter(|c| c.enabled)
            .map(|c| CacheManager::new(c))
            .transpose()?
            .map(Arc::new);

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
            cache_manager,
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

        // Phase 2: Pull caches (if enabled)
        let target_dir = self
            .target_base_dir
            .join(request.project_id.as_str())
            .join(&request.branch);

        if let Some(ref cache_manager) = self.cache_manager {
            self.queue
                .update_status(&request.id, BuildStatus::PullingCache);

            if cancel.is_cancelled() {
                return Err(BuildError::Cancelled {
                    reason: CancelReason::UserRequested,
                });
            }

            let registry_result = cache_manager
                .pull_registry(self.compiler.cargo_home())
                .await;
            match registry_result {
                Ok(CacheResult::Hit) => {
                    info!(build_id = %request.id, "registry cache hit");
                }
                Ok(CacheResult::Miss) => {
                    debug!(build_id = %request.id, "registry cache miss");
                }
                Err(e) => {
                    warn!(build_id = %request.id, error = %e, "failed to pull registry cache");
                }
            }

            let target_result = cache_manager
                .pull_target(&request.project_id, &request.branch, &target_dir)
                .await;
            match target_result {
                Ok(CacheResult::Hit) => {
                    info!(build_id = %request.id, "target cache hit");
                }
                Ok(CacheResult::Miss) => {
                    debug!(build_id = %request.id, "target cache miss");
                }
                Err(e) => {
                    warn!(build_id = %request.id, error = %e, "failed to pull target cache");
                }
            }
        }

        // Phase 3: Fetch dependencies (outside sandbox, needs network)
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

        // Phase 4: Compile workspace (sandboxed)
        self.queue
            .update_status(&request.id, BuildStatus::Compiling { progress: None });

        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: CancelReason::UserRequested,
            });
        }

        // Create channel for build status updates
        let (status_tx, mut status_rx) = tokio::sync::mpsc::channel::<BuildStatus>(100);
        let queue_clone = Arc::clone(&self.queue);
        let build_id_clone = request.id.clone();

        // Spawn task to receive status updates and store them as logs
        let log_task = tokio::spawn(async move {
            while let Some(status) = status_rx.recv().await {
                if let BuildStatus::Compiling {
                    progress: Some(line),
                } = status
                {
                    queue_clone.append_log(&build_id_clone, line);
                }
            }
        });

        let compile_output = self
            .compiler
            .compile_workspace(
                &request.project_id,
                &checkout,
                &target_dir,
                cancel.clone(),
                Some(&status_tx),
            )
            .await;

        // Drop sender to signal we're done, wait for log task to finish
        drop(status_tx);
        let _ = log_task.await;

        let compile_output = compile_output?;

        info!(
            build_id = %request.id,
            binary_count = compile_output.binaries.len(),
            duration_secs = compile_output.duration.as_secs(),
            "workspace compilation completed"
        );

        // Phase 5: Discover deployable projects
        self.queue
            .update_status(&request.id, BuildStatus::DiscoveringProjects);

        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: CancelReason::UserRequested,
            });
        }

        let deployable_projects = discover_projects(&checkout, &compile_output.binaries)?;

        if deployable_projects.is_empty() {
            return Err(BuildError::NoDeployableProjects);
        }

        info!(
            build_id = %request.id,
            project_count = deployable_projects.len(),
            projects = ?deployable_projects.iter().map(|p| &p.name).collect::<Vec<_>>(),
            "discovered deployable projects"
        );

        // Phase 6: Generate and upload artifacts
        self.queue
            .update_status(&request.id, BuildStatus::GeneratingArtifact);

        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: CancelReason::UserRequested,
            });
        }

        let first_project = &deployable_projects[0];
        let (artifact_id, artifact_url, functions) =
            self.build_project_artifact(request, first_project).await?;

        // Phase 7: Push caches (if enabled)
        if let Some(ref cache_manager) = self.cache_manager {
            self.queue
                .update_status(&request.id, BuildStatus::PushingCache);

            if let Err(e) = cache_manager
                .push_registry(self.compiler.cargo_home())
                .await
            {
                warn!(build_id = %request.id, error = %e, "failed to push registry cache");
            }

            if let Err(e) = cache_manager
                .push_target(&request.project_id, &request.branch, &target_dir)
                .await
            {
                warn!(build_id = %request.id, error = %e, "failed to push target cache");
            }
        }

        // Phase 8: Cleanup (only if caching is disabled, otherwise keep target for incremental)
        if self.cache_manager.is_none() {
            self.cleanup_build_dirs(&target_dir).await;
        }

        Ok(BuildOutput {
            artifact_id,
            artifact_url,
            functions,
        })
    }

    async fn build_project_artifact(
        &self,
        request: &BuildRequest,
        project: &DeployableProject,
    ) -> Result<(ArtifactId, String, Vec<FunctionMetadata>), BuildError> {
        let functions = discovery::discover_from_source(&project.path).unwrap_or_default();

        info!(
            build_id = %request.id,
            project = %project.name,
            function_count = functions.len(),
            "functions discovered for project"
        );

        let discovered_functions = functions.clone();

        let output_dir = PathBuf::from("/tmp")
            .join("sidereal-build")
            .join(request.id.as_str())
            .join(&project.name);

        let artifact = self.artifact_builder.build(BuildInput {
            project_id: &request.project_id,
            branch: &request.branch,
            commit_sha: &request.commit_sha,
            runtime_binary: &self.runtime_path,
            user_binary: &project.binary_path,
            functions,
            output_dir: &output_dir,
        })?;

        let artifact_id = artifact.id.clone();

        let rootfs_path = output_dir.join("rootfs.ext4");
        let artifact_url = self.upload_artifact(&artifact, &rootfs_path).await?;

        if let Err(e) = tokio::fs::remove_dir_all(&output_dir).await {
            warn!(
                path = %output_dir.display(),
                error = %e,
                "failed to clean up build output"
            );
        }

        Ok((artifact_id, artifact_url, discovered_functions))
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

    async fn cleanup_build_dirs(&self, target_dir: &Path) {
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

        BuildError::Cache(_) | BuildError::SnapshotNotFound(_) => {
            (BuildStage::Cache, e.to_string())
        }

        BuildError::CargoFetch(_) | BuildError::MissingLockfile => {
            (BuildStage::FetchDeps, e.to_string())
        }

        BuildError::AuditFailed { .. }
        | BuildError::BlockedCrate { .. }
        | BuildError::ExternalDependency { .. } => (BuildStage::Audit, e.to_string()),

        BuildError::NoDeployableProjects => (BuildStage::Discovery, e.to_string()),

        BuildError::RootfsGeneration(_)
        | BuildError::ArtifactStorage(_)
        | BuildError::ArtifactTooLarge { .. }
        | BuildError::ArtifactSigning(_)
        | BuildError::ArtifactVerification(_) => (BuildStage::Artifact, e.to_string()),

        _ => (BuildStage::Compile, e.to_string()),
    }
}
