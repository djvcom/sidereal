//! Sandboxed compilation using bubblewrap.

use std::path::{Path, PathBuf};
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

use super::bubblewrap::{BubblewrapBuilder, SandboxLimits};

/// Configuration for the sandboxed compiler.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    /// Resource limits.
    pub limits: SandboxLimits,
    /// Rust target triple.
    pub target: String,
    /// Path to cargo home (registry cache).
    pub cargo_home: PathBuf,
    /// Path to toolchain (e.g., /nix/store/...-rust or /usr).
    pub toolchain_paths: Vec<PathBuf>,
    /// Additional compiler flags.
    pub extra_rustflags: Vec<String>,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            limits: SandboxLimits::default(),
            target: "x86_64-unknown-linux-musl".to_owned(),
            cargo_home: PathBuf::from("/var/lib/sidereal/cargo"),
            toolchain_paths: vec![PathBuf::from("/usr")],
            extra_rustflags: Vec::new(),
        }
    }
}

/// Output from a successful compilation.
#[derive(Debug)]
pub struct CompileOutput {
    /// Path to the compiled binary.
    pub binary_path: PathBuf,
    /// Time taken to compile.
    pub duration: Duration,
    /// Stdout from cargo (warnings, etc.).
    pub stdout: String,
    /// Stderr from cargo.
    pub stderr: String,
}

/// Output from a successful workspace compilation.
#[derive(Debug)]
pub struct WorkspaceCompileOutput {
    /// All built binaries in the workspace.
    pub binaries: Vec<BinaryInfo>,
    /// Time taken to compile.
    pub duration: Duration,
    /// Stdout from cargo (warnings, etc.).
    pub stdout: String,
    /// Stderr from cargo.
    pub stderr: String,
}

/// Information about a compiled binary.
#[derive(Debug, Clone)]
pub struct BinaryInfo {
    /// Binary name (without extension).
    pub name: String,
    /// Path to the compiled binary.
    pub path: PathBuf,
    /// Path to the crate directory containing Cargo.toml.
    pub crate_dir: PathBuf,
}

/// Sandboxed compiler using bubblewrap.
pub struct SandboxedCompiler {
    config: SandboxConfig,
}

impl SandboxedCompiler {
    /// Create a new sandboxed compiler.
    #[must_use]
    pub const fn new(config: SandboxConfig) -> Self {
        Self { config }
    }

    /// Return the cargo home directory path.
    #[must_use]
    pub fn cargo_home(&self) -> &Path {
        &self.config.cargo_home
    }

    /// Compile the project in a sandbox.
    #[instrument(skip(self, checkout, cancel), fields(project = %project_id))]
    pub async fn compile(
        &self,
        project_id: &ProjectId,
        checkout: &SourceCheckout,
        target_dir: &Path,
        cancel: CancellationToken,
    ) -> BuildResult<CompileOutput> {
        info!("starting sandboxed compilation");

        // Build the bubblewrap command
        let mut builder = BubblewrapBuilder::new();

        // Mount toolchain paths read-only
        for path in &self.config.toolchain_paths {
            if path.exists() {
                builder = builder.bind_ro(path, path);
            }
        }

        // Always bind /nix read-only if it exists (for NixOS)
        let nix_store = Path::new("/nix");
        if nix_store.exists() {
            builder = builder.bind_ro(nix_store, nix_store);
        }

        // Mount cargo registry read-only (pre-fetched dependencies)
        if self.config.cargo_home.exists() {
            builder = builder.bind_ro(&self.config.cargo_home, "/cargo");
        }

        // Mount source read-only
        builder = builder.bind_ro(&checkout.path, "/build/src");

        // Mount target directory read-write
        std::fs::create_dir_all(target_dir)?;
        builder = builder.bind_rw(target_dir, "/build/target");

        // Set environment
        let path_env = std::env::var("PATH").unwrap_or_default();
        builder = builder
            .env("PATH", &path_env)
            .env("CARGO_HOME", "/cargo")
            .env("CARGO_TARGET_DIR", "/build/target")
            .env("HOME", "/build")
            .env("USER", "build")
            .cwd("/build/src");

        // Pass through cross-compilation environment variables
        for var in [
            "CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER",
            "CC_x86_64_unknown_linux_musl",
        ] {
            if let Ok(value) = std::env::var(var) {
                builder = builder.env(var, value);
            }
        }

        // Add rustflags if specified
        if !self.config.extra_rustflags.is_empty() {
            let rustflags = self.config.extra_rustflags.join(" ");
            builder = builder.env("RUSTFLAGS", rustflags);
        }

        // Build cargo command arguments
        let cargo_args = [
            "build",
            "--release",
            "--target",
            &self.config.target,
            "--locked",
        ];

        // Create the sandboxed command
        let std_cmd = builder.build("cargo", &cargo_args);

        // Convert to tokio Command
        let mut cmd = TokioCommand::from(std_cmd);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        debug!("spawning sandboxed cargo build");
        let start = Instant::now();

        // Spawn the process
        let mut child = cmd
            .spawn()
            .map_err(|e| BuildError::SandboxSetup(format!("failed to spawn bwrap: {e}")))?;

        // Capture output streams
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        // Spawn tasks to read output
        let stdout_task = tokio::spawn(async move {
            let mut lines = Vec::new();
            if let Some(stdout) = stdout {
                let reader = BufReader::new(stdout);
                let mut reader_lines = reader.lines();
                while let Ok(Some(line)) = reader_lines.next_line().await {
                    lines.push(line);
                }
            }
            lines
        });

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

        // Wait for completion with timeout and cancellation
        let result = tokio::select! {
            () = cancel.cancelled() => {
                warn!("compilation cancelled");
                child.kill().await.ok();
                return Err(BuildError::Cancelled {
                    reason: crate::error::CancelReason::UserRequested,
                });
            }
            result = timeout(self.config.limits.timeout, child.wait()) => {
                match result {
                    Ok(Ok(status)) => status,
                    Ok(Err(e)) => {
                        return Err(BuildError::SandboxSetup(format!("process error: {e}")));
                    }
                    Err(_) => {
                        child.kill().await.ok();
                        return Err(BuildError::Timeout {
                            limit: self.config.limits.timeout,
                        });
                    }
                }
            }
        };

        let duration = start.elapsed();

        // Collect output
        let stdout_lines = stdout_task.await.unwrap_or_default();
        let stderr_lines = stderr_task.await.unwrap_or_default();

        let stdout_str = stdout_lines.join("\n");
        let stderr_str = stderr_lines.join("\n");

        // Check exit status
        if !result.success() {
            let exit_code = result.code().unwrap_or(-1);
            return Err(BuildError::CompileFailed {
                stderr: stderr_str,
                exit_code,
            });
        }

        // Find the compiled binary
        let binary_name = find_binary_name(checkout)?;
        let binary_path = target_dir
            .join(&self.config.target)
            .join("release")
            .join(&binary_name);

        if !binary_path.exists() {
            return Err(BuildError::CompileFailed {
                stderr: format!("binary not found at {}", binary_path.display()),
                exit_code: 0,
            });
        }

        info!(
            binary = %binary_path.display(),
            duration_secs = duration.as_secs_f32(),
            "compilation complete"
        );

        Ok(CompileOutput {
            binary_path,
            duration,
            stdout: stdout_str,
            stderr: stderr_str,
        })
    }

    /// Compile the entire workspace in a sandbox.
    ///
    /// Builds all crates in the workspace and returns information about all
    /// binaries that were built.
    #[instrument(skip(self, checkout, cancel), fields(project = %project_id))]
    pub async fn compile_workspace(
        &self,
        project_id: &ProjectId,
        checkout: &SourceCheckout,
        target_dir: &Path,
        cancel: CancellationToken,
    ) -> BuildResult<WorkspaceCompileOutput> {
        info!("starting sandboxed workspace compilation");

        let mut builder = BubblewrapBuilder::new();

        for path in &self.config.toolchain_paths {
            if path.exists() {
                builder = builder.bind_ro(path, path);
            }
        }

        let nix_store = Path::new("/nix");
        if nix_store.exists() {
            builder = builder.bind_ro(nix_store, nix_store);
        }

        if self.config.cargo_home.exists() {
            builder = builder.bind_ro(&self.config.cargo_home, "/cargo");
        }

        builder = builder.bind_ro(&checkout.path, "/build/src");

        std::fs::create_dir_all(target_dir)?;
        builder = builder.bind_rw(target_dir, "/build/target");

        let path_env = std::env::var("PATH").unwrap_or_default();
        builder = builder
            .env("PATH", &path_env)
            .env("CARGO_HOME", "/cargo")
            .env("CARGO_TARGET_DIR", "/build/target")
            .env("HOME", "/build")
            .env("USER", "build")
            .cwd("/build/src");

        for var in [
            "CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER",
            "CC_x86_64_unknown_linux_musl",
        ] {
            if let Ok(value) = std::env::var(var) {
                builder = builder.env(var, value);
            }
        }

        if !self.config.extra_rustflags.is_empty() {
            let rustflags = self.config.extra_rustflags.join(" ");
            builder = builder.env("RUSTFLAGS", rustflags);
        }

        let cargo_args = [
            "build",
            "--workspace",
            "--release",
            "--target",
            &self.config.target,
            "--locked",
        ];

        let std_cmd = builder.build("cargo", &cargo_args);

        let mut cmd = TokioCommand::from(std_cmd);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        debug!("spawning sandboxed cargo build --workspace");
        let start = Instant::now();

        let mut child = cmd
            .spawn()
            .map_err(|e| BuildError::SandboxSetup(format!("failed to spawn bwrap: {e}")))?;

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        let stdout_task = tokio::spawn(async move {
            let mut lines = Vec::new();
            if let Some(stdout) = stdout {
                let reader = BufReader::new(stdout);
                let mut reader_lines = reader.lines();
                while let Ok(Some(line)) = reader_lines.next_line().await {
                    lines.push(line);
                }
            }
            lines
        });

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
                warn!("compilation cancelled");
                child.kill().await.ok();
                return Err(BuildError::Cancelled {
                    reason: crate::error::CancelReason::UserRequested,
                });
            }
            result = timeout(self.config.limits.timeout, child.wait()) => {
                match result {
                    Ok(Ok(status)) => status,
                    Ok(Err(e)) => {
                        return Err(BuildError::SandboxSetup(format!("process error: {e}")));
                    }
                    Err(_) => {
                        child.kill().await.ok();
                        return Err(BuildError::Timeout {
                            limit: self.config.limits.timeout,
                        });
                    }
                }
            }
        };

        let duration = start.elapsed();

        let stdout_lines = stdout_task.await.unwrap_or_default();
        let stderr_lines = stderr_task.await.unwrap_or_default();

        let stdout_str = stdout_lines.join("\n");
        let stderr_str = stderr_lines.join("\n");

        if !result.success() {
            let exit_code = result.code().unwrap_or(-1);
            return Err(BuildError::CompileFailed {
                stderr: stderr_str,
                exit_code,
            });
        }

        let binaries =
            discover_workspace_binaries(&checkout.path, target_dir, &self.config.target)?;

        info!(
            binary_count = binaries.len(),
            duration_secs = duration.as_secs_f32(),
            "workspace compilation complete"
        );

        Ok(WorkspaceCompileOutput {
            binaries,
            duration,
            stdout: stdout_str,
            stderr: stderr_str,
        })
    }
}

/// Find the binary name from Cargo.toml.
fn find_binary_name(checkout: &SourceCheckout) -> BuildResult<String> {
    let cargo_toml = checkout
        .cargo_toml
        .as_ref()
        .ok_or_else(|| BuildError::CompileFailed {
            stderr: "no Cargo.toml found".to_owned(),
            exit_code: 0,
        })?;

    // Read and parse Cargo.toml
    let content = std::fs::read_to_string(cargo_toml)?;
    let manifest: toml::Value =
        toml::from_str(&content).map_err(|e| BuildError::CompileFailed {
            stderr: format!("failed to parse Cargo.toml: {e}"),
            exit_code: 0,
        })?;

    // Try to find [[bin]] section first
    if let Some(bins) = manifest.get("bin").and_then(|b| b.as_array()) {
        if let Some(bin) = bins.first() {
            if let Some(name) = bin.get("name").and_then(|n| n.as_str()) {
                return Ok(name.to_owned());
            }
        }
    }

    // Fall back to package name
    manifest
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .map(|s| s.replace('-', "_"))
        .ok_or_else(|| BuildError::CompileFailed {
            stderr: "could not determine binary name from Cargo.toml".to_owned(),
            exit_code: 0,
        })
}

/// Discover all binaries built in the workspace.
///
/// Scans Cargo.toml files to find binary targets and matches them with
/// compiled binaries in the target directory.
fn discover_workspace_binaries(
    workspace_root: &Path,
    target_dir: &Path,
    target_triple: &str,
) -> BuildResult<Vec<BinaryInfo>> {
    let release_dir = target_dir.join(target_triple).join("release");
    let mut binaries = Vec::new();

    if !release_dir.exists() {
        return Ok(binaries);
    }

    discover_binaries_recursive(workspace_root, workspace_root, &release_dir, &mut binaries)?;

    Ok(binaries)
}

fn discover_binaries_recursive(
    workspace_root: &Path,
    dir: &Path,
    release_dir: &Path,
    binaries: &mut Vec<BinaryInfo>,
) -> BuildResult<()> {
    let cargo_toml = dir.join("Cargo.toml");

    if cargo_toml.exists() {
        let content = std::fs::read_to_string(&cargo_toml)?;
        let manifest: toml::Value =
            toml::from_str(&content).map_err(|e| BuildError::CompileFailed {
                stderr: format!("failed to parse {}: {e}", cargo_toml.display()),
                exit_code: 0,
            })?;

        let bin_names = extract_binary_names(&manifest);

        for name in bin_names {
            let binary_path = release_dir.join(&name);
            if binary_path.exists() {
                binaries.push(BinaryInfo {
                    name,
                    path: binary_path,
                    crate_dir: dir.to_owned(),
                });
            }
        }
    }

    let entries = std::fs::read_dir(dir).ok();
    if let Some(entries) = entries {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name != "target" && !name.starts_with('.') {
                    discover_binaries_recursive(workspace_root, &path, release_dir, binaries)?;
                }
            }
        }
    }

    Ok(())
}

fn extract_binary_names(manifest: &toml::Value) -> Vec<String> {
    let mut names = Vec::new();

    if let Some(bins) = manifest.get("bin").and_then(|b| b.as_array()) {
        for bin in bins {
            if let Some(name) = bin.get("name").and_then(|n| n.as_str()) {
                names.push(name.to_owned());
            }
        }
    }

    if names.is_empty() {
        if let Some(package) = manifest.get("package") {
            if let Some(name) = package.get("name").and_then(|n| n.as_str()) {
                let has_lib =
                    manifest.get("lib").is_some() || std::path::Path::new("src/lib.rs").exists();

                let has_main = std::path::Path::new("src/main.rs").exists();

                if has_main || !has_lib {
                    names.push(name.replace('-', "_"));
                }
            }
        }
    }

    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_config_defaults() {
        let config = SandboxConfig::default();
        assert_eq!(config.target, "x86_64-unknown-linux-musl");
        assert_eq!(config.limits.timeout, Duration::from_secs(600));
    }
}
