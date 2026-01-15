//! Build worker implementation.
//!
//! Processes builds from the queue.

use std::path::PathBuf;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::artifact::{Artifact, ArtifactBuilder, ArtifactStore};
use crate::discovery;
use crate::error::{BuildError, BuildStage, CancelReason};
use crate::queue::{BuildHandle, BuildQueue};
use crate::sandbox::{SandboxConfig, SandboxLimits, SandboxedCompiler};
use crate::source::SourceManager;
use crate::types::{ArtifactId, BuildRequest, BuildStatus};

use super::{LimitsConfig, PathsConfig};

/// Build worker that processes builds from the queue.
pub struct BuildWorker {
    id: usize,
    queue: Arc<BuildQueue>,
    source_manager: Arc<SourceManager>,
    compiler: Arc<SandboxedCompiler>,
    artifact_builder: Arc<ArtifactBuilder>,
    artifact_store: Arc<ArtifactStore>,
    runtime_path: PathBuf,
    target_base_dir: PathBuf,
}

impl BuildWorker {
    /// Create a new build worker.
    #[must_use]
    pub fn new(
        id: usize,
        queue: Arc<BuildQueue>,
        paths: &PathsConfig,
        limits: &LimitsConfig,
        artifact_store: Arc<ArtifactStore>,
    ) -> Self {
        let source_manager = Arc::new(SourceManager::new(&paths.checkouts, &paths.caches));

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

        Self {
            id,
            queue,
            source_manager,
            compiler,
            artifact_builder,
            artifact_store,
            runtime_path: paths.runtime.clone(),
            target_base_dir: paths.caches.join("targets"),
        }
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
            Ok(artifact_id) => {
                info!(
                    build_id = %build_id,
                    artifact_id = %artifact_id,
                    "build completed successfully"
                );
                self.queue
                    .update_status(&build_id, BuildStatus::Completed { artifact_id });
            }
            Err(e) => {
                if handle.token.is_cancelled() {
                    warn!(build_id = %build_id, "build was cancelled");
                } else {
                    error!(build_id = %build_id, error = %e, "build failed");
                    let (stage, error) = error_to_stage_and_message(&e);
                    self.queue
                        .update_status(&build_id, BuildStatus::Failed { error, stage });
                }
            }
        }
    }

    async fn execute_build(
        &self,
        request: &BuildRequest,
        cancel: &CancellationToken,
    ) -> Result<ArtifactId, BuildError> {
        // Phase 1: Checkout source
        self.queue
            .update_status(&request.id, BuildStatus::CheckingOut);

        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: CancelReason::UserRequested,
            });
        }

        let checkout = self
            .source_manager
            .checkout(&request.project_id, &request.repo_url, &request.commit_sha)
            .await?;

        info!(
            build_id = %request.id,
            path = %checkout.path.display(),
            "source checked out"
        );

        // Phase 2: Compile
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

        let artifact = self.artifact_builder.build(
            &request.project_id,
            &request.branch,
            &request.commit_sha,
            &self.runtime_path,
            &compile_output.binary_path,
            functions,
            &output_dir,
        )?;

        let artifact_id = artifact.id.clone();

        // Phase 5: Upload artifact
        let rootfs_path = output_dir.join("rootfs.ext4");
        self.upload_artifact(&artifact, &rootfs_path).await?;

        // Cleanup
        self.cleanup_build_dirs(&output_dir, &target_dir).await;

        Ok(artifact_id)
    }

    async fn upload_artifact(
        &self,
        artifact: &Artifact,
        rootfs_path: &PathBuf,
    ) -> Result<(), BuildError> {
        self.artifact_store.upload(artifact, rootfs_path).await?;
        info!(
            artifact_id = %artifact.id,
            project = %artifact.project_id,
            branch = %artifact.branch,
            "artifact uploaded"
        );
        Ok(())
    }

    async fn cleanup_build_dirs(&self, output_dir: &PathBuf, target_dir: &PathBuf) {
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

        BuildError::SandboxSetup(_)
        | BuildError::CompileFailed { .. }
        | BuildError::Timeout { .. }
        | BuildError::Cancelled { .. } => (BuildStage::Compile, e.to_string()),

        BuildError::RootfsGeneration(_)
        | BuildError::ArtifactStorage(_)
        | BuildError::ArtifactTooLarge { .. }
        | BuildError::ArtifactSigning(_)
        | BuildError::ArtifactVerification(_) => (BuildStage::Artifact, e.to_string()),

        _ => (BuildStage::Compile, e.to_string()),
    }
}
