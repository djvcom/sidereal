//! Build worker implementation.
//!
//! Processes builds from the queue using Firecracker VMs.

use std::sync::Arc;

use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::error::{BuildError, BuildStage, CancelReason};
use crate::forge_auth::ForgeAuth;
use crate::protocol::S3Config;
use crate::queue::{BuildHandle, BuildQueue};
use crate::types::{ArtifactId, BuildRequest, BuildStatus, FunctionMetadata};
use crate::vm::{BuildInput, FirecrackerCompiler, VmCompilerConfig};

use super::{LimitsConfig, PathsConfig, StorageConfig, VmConfig};

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
    compiler: Arc<FirecrackerCompiler>,
    forge_auth: ForgeAuth,
    http_client: reqwest::Client,
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
        forge_auth: ForgeAuth,
        vm_config: &VmConfig,
        storage_config: &StorageConfig,
    ) -> Result<Self, BuildError> {
        let s3_config = storage_config_to_s3(storage_config)?;

        let vm_compiler_config = VmCompilerConfig {
            kernel_path: vm_config.kernel_path.clone(),
            builder_rootfs: vm_config.builder_rootfs.clone(),
            vcpu_count: vm_config.vcpu_count,
            mem_size_mib: vm_config.mem_size_mib,
            target: vm_config.target.clone(),
            timeout: limits.timeout(),
            s3: s3_config,
            runtime_s3_key: vm_config.runtime_s3_key.clone(),
            additional_proxy_domains: Vec::new(),
        };
        let work_dir = paths.artifacts.join("vm-work");
        let compiler = Arc::new(FirecrackerCompiler::new(vm_compiler_config, work_dir)?);

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| BuildError::Internal(format!("failed to create HTTP client: {e}")))?;

        Ok(Self {
            id,
            queue,
            compiler,
            forge_auth,
            http_client,
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
        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: CancelReason::UserRequested,
            });
        }

        // Get SSH key for private repos from forge auth
        let ssh_key = self.forge_auth.ssh_key_for_url(&request.repo_url);

        // Build the input for the VM
        let build_input = BuildInput {
            build_id: request.id.to_string(),
            project_id: request.project_id.clone(),
            repo_url: request.repo_url.clone(),
            branch: request.branch.clone(),
            commit_sha: request.commit_sha.clone(),
            ssh_key,
            subpath: request.path.clone(),
            registry_cache_key: None, // TODO: implement cache key generation
            target_cache_key: None,
        };

        // Create channel for build status updates
        let (status_tx, mut status_rx) = tokio::sync::mpsc::channel::<BuildStatus>(100);
        let queue_clone = Arc::clone(&self.queue);
        let build_id_clone = request.id.clone();

        // Spawn task to receive status updates
        let log_task = tokio::spawn(async move {
            while let Some(status) = status_rx.recv().await {
                queue_clone.update_status(&build_id_clone, status.clone());
                if let BuildStatus::Compiling {
                    progress: Some(line),
                } = status
                {
                    queue_clone.append_log(&build_id_clone, line);
                }
            }
        });

        // Execute build in VM
        let vm_output = self
            .compiler
            .build(&build_input, cancel.clone(), Some(&status_tx))
            .await;

        // Drop sender to signal we're done, wait for log task to finish
        drop(status_tx);
        let _ = log_task.await;

        let vm_output = vm_output?;

        // Generate artifact ID from the hash
        let artifact_id = ArtifactId::from(format!("art_{}", &vm_output.artifact_hash[..16]));

        info!(
            build_id = %request.id,
            artifact_id = %artifact_id,
            function_count = vm_output.functions.len(),
            duration_secs = vm_output.duration_secs,
            "VM build pipeline completed"
        );

        Ok(BuildOutput {
            artifact_id,
            artifact_url: vm_output.artifact_url,
            functions: vm_output.functions,
        })
    }
}

/// Convert StorageConfig to S3Config for the VM.
fn storage_config_to_s3(config: &StorageConfig) -> Result<S3Config, BuildError> {
    if config.storage_type != "s3" {
        return Err(BuildError::ConfigParse(
            "VM-based builds require S3 storage configuration".to_owned(),
        ));
    }

    let endpoint = config.endpoint.clone().ok_or_else(|| {
        BuildError::ConfigParse("S3 endpoint is required for VM builds".to_owned())
    })?;

    let access_key_id = config.access_key_id.clone().ok_or_else(|| {
        BuildError::ConfigParse("S3 access_key_id is required for VM builds".to_owned())
    })?;

    let secret_access_key = config.secret_access_key.clone().ok_or_else(|| {
        BuildError::ConfigParse("S3 secret_access_key is required for VM builds".to_owned())
    })?;

    Ok(S3Config {
        endpoint,
        bucket: config.path.clone(),
        region: config
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_owned()),
        access_key_id,
        secret_access_key,
    })
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
