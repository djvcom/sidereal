//! Cargo build execution for the builder runtime.
//!
//! Executes cargo zigbuild and streams output back to the host via vsock.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio_vsock::VsockStream;
use tracing::{debug, error, info, warn};

use crate::protocol::{BinaryInfo, BuildOutput, BuildRequest, BuildResult};
use crate::vsock::{send_output, send_result, VsockError};

/// Execute a cargo build based on the request.
///
/// Streams stdout/stderr back to the host and returns the final result.
pub async fn execute_build(
    stream: &mut VsockStream,
    request: &BuildRequest,
) -> Result<(), VsockError> {
    let start_time = Instant::now();

    info!(
        build_id = %request.build_id,
        source = %request.source_path,
        target = %request.target_path,
        "Starting build"
    );

    // Set up environment
    setup_environment(&request.cargo_home(), &request.target_path())?;

    // Build cargo command
    let mut cmd = build_cargo_command(request);

    // Spawn the process
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            error!(error = %e, "Failed to spawn cargo");
            let result = BuildResult::failure(-1, format!("Failed to spawn cargo: {e}"), 0.0);
            send_result(stream, result).await?;
            return Ok(());
        }
    };

    // Stream stdout
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let mut stdout_lines = Vec::new();
    let mut stderr_lines = Vec::new();

    // Process stdout
    if let Some(stdout) = stdout {
        let mut reader = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            stdout_lines.push(line.clone());
            if let Err(e) = send_output(stream, BuildOutput::Stdout(line)).await {
                warn!(error = %e, "Failed to send stdout");
            }
        }
    }

    // Process stderr
    if let Some(stderr) = stderr {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            stderr_lines.push(line.clone());
            if let Err(e) = send_output(stream, BuildOutput::Stderr(line)).await {
                warn!(error = %e, "Failed to send stderr");
            }
        }
    }

    // Wait for process to complete
    let status = match child.wait().await {
        Ok(status) => status,
        Err(e) => {
            error!(error = %e, "Failed to wait for cargo");
            let result = BuildResult::failure(-1, format!("Failed to wait for cargo: {e}"), 0.0);
            send_result(stream, result).await?;
            return Ok(());
        }
    };

    // Log captured output for debugging
    if !stderr_lines.is_empty() {
        error!(stderr = %stderr_lines.join("\n"), "Cargo stderr output");
    }
    if !stdout_lines.is_empty() {
        info!(stdout = %stdout_lines.join("\n"), "Cargo stdout output");
    }

    let duration_secs = start_time.elapsed().as_secs_f64();

    #[allow(clippy::as_conversions)]
    let exit_code = status.code().unwrap_or(-1) as i32;

    if status.success() {
        info!(duration_secs, "Build completed successfully");

        // Discover compiled binaries
        let binaries = discover_binaries(request).await;

        let result = BuildResult::success(binaries, duration_secs)
            .with_stdout(stdout_lines.join("\n"))
            .with_stderr(stderr_lines.join("\n"));

        send_result(stream, result).await?;
    } else {
        error!(exit_code, "Build failed");

        let result = BuildResult::failure(exit_code, stderr_lines.join("\n"), duration_secs)
            .with_stdout(stdout_lines.join("\n"));

        send_result(stream, result).await?;
    }

    Ok(())
}

/// Set up the build environment.
fn setup_environment(cargo_home: &Path, target_dir: &Path) -> Result<(), VsockError> {
    // Ensure directories exist
    std::fs::create_dir_all(cargo_home).map_err(VsockError::Io)?;
    std::fs::create_dir_all(target_dir).map_err(VsockError::Io)?;

    Ok(())
}

/// Build the cargo command for the build request.
fn build_cargo_command(request: &BuildRequest) -> Command {
    let mut cmd = Command::new("cargo");

    // Use regular build (zigbuild triggers rustup network calls)
    cmd.arg("build");

    // Build all workspace members
    cmd.arg("--workspace");

    // Target specification
    cmd.arg("--target");
    cmd.arg(&request.target_triple);

    // Release mode
    if request.release {
        cmd.arg("--release");
    }

    // Locked dependencies
    if request.locked {
        cmd.arg("--locked");
    }

    // Set working directory
    cmd.current_dir(&request.source_path);

    // Environment variables
    cmd.env("CARGO_HOME", &request.cargo_home);
    cmd.env("CARGO_TARGET_DIR", &request.target_path);

    // SSL certificates
    cmd.env("SSL_CERT_FILE", "/etc/ssl/certs/ca-bundle.crt");
    cmd.env("SSL_CERT_DIR", "/etc/ssl/certs");

    // Offline mode - VM has no network access
    cmd.env("CARGO_NET_OFFLINE", "true");
    // Use the system rustup/cargo installation and prevent auto-updates
    cmd.env("RUSTUP_HOME", "/usr/local/rustup");
    cmd.env("RUSTUP_TOOLCHAIN", "1.92-x86_64-unknown-linux-gnu");
    cmd.env("RUSTUP_UPDATE_ROOT", "file:///nonexistent");

    // Disable incremental compilation (not useful for ephemeral VMs)
    cmd.env("CARGO_INCREMENTAL", "0");

    // Configure stdout/stderr
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    cmd
}

/// Discover compiled binaries in the target directory.
async fn discover_binaries(request: &BuildRequest) -> Vec<BinaryInfo> {
    let mut binaries = Vec::new();

    let target_dir = request.target_path();
    let profile = if request.release { "release" } else { "debug" };
    let bin_dir = target_dir.join(&request.target_triple).join(profile);

    debug!(path = %bin_dir.display(), "Scanning for binaries");

    if !bin_dir.exists() {
        warn!(path = %bin_dir.display(), "Binary directory does not exist");
        return binaries;
    }

    // Read directory entries
    let mut entries = match tokio::fs::read_dir(&bin_dir).await {
        Ok(entries) => entries,
        Err(e) => {
            warn!(error = %e, "Failed to read binary directory");
            return binaries;
        }
    };

    // Look for ELF binaries
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        // Skip non-files
        if !path.is_file() {
            continue;
        }

        // Skip files with extensions (we want bare executables)
        if path.extension().is_some() {
            continue;
        }

        // Get file name
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_owned(),
            None => continue,
        };

        // Skip common non-binary files (hidden files and .d files)
        if name.starts_with('.') {
            continue;
        }

        // Check if it's an ELF binary by reading the magic bytes
        if !is_elf_binary(&path).await {
            continue;
        }

        info!(name = %name, path = %path.display(), "Found binary");

        // Try to find the crate directory
        let crate_dir = find_crate_dir(&request.source_path(), &name)
            .await
            .unwrap_or_else(|| request.source_path());

        binaries.push(BinaryInfo::new(
            name,
            path.display().to_string(),
            crate_dir.display().to_string(),
        ));
    }

    binaries
}

/// Check if a file is an ELF binary by reading magic bytes.
async fn is_elf_binary(path: &Path) -> bool {
    let Ok(file) = tokio::fs::File::open(path).await else {
        return false;
    };

    let mut reader = BufReader::new(file);
    let mut magic = [0u8; 4];

    if reader.read_exact(&mut magic).await.is_err() {
        return false;
    }

    // ELF magic: 0x7f 'E' 'L' 'F'
    magic == [0x7f, b'E', b'L', b'F']
}

/// Find the crate directory for a binary name.
///
/// Searches for a Cargo.toml that defines a binary with the given name.
async fn find_crate_dir(source_dir: &Path, binary_name: &str) -> Option<PathBuf> {
    // First check if the root Cargo.toml has this binary
    let root_cargo = source_dir.join("Cargo.toml");
    if let Some(crate_dir) = check_cargo_toml(&root_cargo, binary_name).await {
        return Some(crate_dir);
    }

    // Check common workspace member locations
    let workspace_dirs = ["crates", "packages", "libs", "apps"];

    for dir in workspace_dirs {
        let workspace_path = source_dir.join(dir);
        if !workspace_path.exists() {
            continue;
        }

        let Ok(mut entries) = tokio::fs::read_dir(&workspace_path).await else {
            continue;
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let cargo_toml = entry.path().join("Cargo.toml");
            if let Some(crate_dir) = check_cargo_toml(&cargo_toml, binary_name).await {
                return Some(crate_dir);
            }
        }
    }

    None
}

/// Check if a Cargo.toml defines a binary with the given name.
async fn check_cargo_toml(path: &Path, binary_name: &str) -> Option<PathBuf> {
    let content = tokio::fs::read_to_string(path).await.ok()?;

    // Simple check: look for the package name matching the binary name
    // This is a heuristic - proper parsing would require a TOML parser
    if content.contains(&format!("name = \"{binary_name}\""))
        || content.contains(&format!("name = '{binary_name}'"))
    {
        return path.parent().map(Path::to_path_buf);
    }

    // Check for [[bin]] sections
    if content.contains("[[bin]]") && content.contains(binary_name) {
        return path.parent().map(Path::to_path_buf);
    }

    None
}
