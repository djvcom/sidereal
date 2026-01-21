//! Dependency fetching (outside sandbox).
//!
//! Downloads crate dependencies before sandboxed compilation.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

use crate::error::{BuildError, BuildResult};
use crate::source::SourceCheckout;
use crate::types::ProjectId;

/// Configuration for dependency fetching.
#[derive(Debug, Clone)]
pub struct FetchConfig {
    /// Path to cargo home directory for caching crates.
    pub cargo_home: PathBuf,
    /// Maximum time to wait for fetch to complete.
    pub timeout: Duration,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            cargo_home: PathBuf::from("/var/lib/sidereal/cargo"),
            timeout: Duration::from_secs(300),
        }
    }
}

/// Output from a successful dependency fetch.
#[derive(Debug)]
pub struct FetchOutput {
    /// Time taken to fetch dependencies.
    pub duration: Duration,
    /// Stderr from cargo fetch.
    pub stderr: String,
}

/// Fetch dependencies for a project.
///
/// This runs `cargo fetch` outside the sandbox to download crates from the
/// network. The downloaded crates are stored in `cargo_home` which can then
/// be mounted read-only in the sandboxed compilation step.
#[instrument(skip(checkout, cancel), fields(project = %project_id))]
pub async fn fetch_dependencies(
    project_id: &ProjectId,
    checkout: &SourceCheckout,
    config: &FetchConfig,
    cancel: CancellationToken,
) -> BuildResult<FetchOutput> {
    info!("fetching dependencies");

    std::fs::create_dir_all(&config.cargo_home)?;

    let mut cmd = TokioCommand::new("cargo");
    cmd.arg("fetch")
        .arg("--locked")
        .current_dir(&checkout.path)
        .env("CARGO_HOME", &config.cargo_home)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    debug!("spawning cargo fetch");
    let start = Instant::now();

    let mut child = cmd
        .spawn()
        .map_err(|e| BuildError::CargoFetch(format!("failed to spawn cargo: {e}")))?;

    let stderr = child.stderr.take();

    let stderr_task = tokio::spawn(async move {
        let mut lines = Vec::new();
        if let Some(stderr) = stderr {
            let reader = BufReader::new(stderr);
            let mut reader_lines = reader.lines();
            while let Ok(Some(line)) = reader_lines.next_line().await {
                lines.push(line);
            }
        }
        lines
    });

    let result = tokio::select! {
        () = cancel.cancelled() => {
            warn!("fetch cancelled");
            child.kill().await.ok();
            return Err(BuildError::Cancelled {
                reason: crate::error::CancelReason::UserRequested,
            });
        }
        result = timeout(config.timeout, child.wait()) => {
            match result {
                Ok(Ok(status)) => status,
                Ok(Err(e)) => {
                    return Err(BuildError::CargoFetch(format!("process error: {e}")));
                }
                Err(_) => {
                    child.kill().await.ok();
                    return Err(BuildError::Timeout {
                        limit: config.timeout,
                    });
                }
            }
        }
    };

    let duration = start.elapsed();
    let stderr_lines = stderr_task.await.unwrap_or_default();
    let stderr_str = stderr_lines.join("\n");

    if !result.success() {
        let exit_code = result.code().unwrap_or(-1);
        return Err(BuildError::CargoFetch(format!(
            "cargo fetch failed (exit code {exit_code}): {stderr_str}"
        )));
    }

    info!(
        duration_secs = duration.as_secs_f32(),
        "dependencies fetched"
    );

    Ok(FetchOutput {
        duration,
        stderr: stderr_str,
    })
}
