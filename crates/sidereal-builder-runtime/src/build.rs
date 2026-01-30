//! Cargo build execution for the builder runtime.
//!
//! Executes cargo build and streams output back to the host via vsock.

use std::collections::HashMap;
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

    // Take stdout and stderr for concurrent reading
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let mut stdout_lines = Vec::new();
    let mut stderr_lines = Vec::new();

    // Create line readers for both streams
    let mut stdout_reader = stdout.map(|s| BufReader::new(s).lines());
    let mut stderr_reader = stderr.map(|s| BufReader::new(s).lines());

    let mut stdout_done = stdout_reader.is_none();
    let mut stderr_done = stderr_reader.is_none();

    // Read stdout and stderr concurrently using select
    while !stdout_done || !stderr_done {
        tokio::select! {
            line = async {
                match stdout_reader.as_mut() {
                    Some(reader) => reader.next_line().await,
                    None => std::future::pending().await,
                }
            }, if !stdout_done => {
                match line {
                    Ok(Some(line)) => {
                        stdout_lines.push(line.clone());
                        if let Err(e) = send_output(stream, BuildOutput::Stdout(line)).await {
                            warn!(error = %e, "Failed to send stdout");
                        }
                    }
                    Ok(None) => {
                        stdout_done = true;
                    }
                    Err(e) => {
                        warn!(error = %e, "Error reading stdout");
                        stdout_done = true;
                    }
                }
            }
            line = async {
                match stderr_reader.as_mut() {
                    Some(reader) => reader.next_line().await,
                    None => std::future::pending().await,
                }
            }, if !stderr_done => {
                match line {
                    Ok(Some(line)) => {
                        stderr_lines.push(line.clone());
                        if let Err(e) = send_output(stream, BuildOutput::Stderr(line)).await {
                            warn!(error = %e, "Failed to send stderr");
                        }
                    }
                    Ok(None) => {
                        stderr_done = true;
                    }
                    Err(e) => {
                        warn!(error = %e, "Error reading stderr");
                        stderr_done = true;
                    }
                }
            }
        }
    }

    // Wait for process to complete (should already be done since pipes are closed)
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

    cmd.arg("zigbuild");

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

    // Route cargo traffic through the vsock proxy bridge on the host
    cmd.env("HTTP_PROXY", "http://127.0.0.1:1080");
    cmd.env("HTTPS_PROXY", "http://127.0.0.1:1080");
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

    let crate_dirs = get_binary_crate_dirs(&request.source_path()).await;

    let mut entries = match tokio::fs::read_dir(&bin_dir).await {
        Ok(entries) => entries,
        Err(e) => {
            warn!(error = %e, "Failed to read binary directory");
            return binaries;
        }
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        if path.extension().is_some() {
            continue;
        }

        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_owned(),
            None => continue,
        };

        if name.starts_with('.') {
            continue;
        }

        if !is_elf_binary(&path).await {
            continue;
        }

        info!(name = %name, path = %path.display(), "Found binary");

        let crate_dir = crate_dirs
            .get(&name)
            .cloned()
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

/// Get a mapping of binary names to their crate directories using cargo metadata.
async fn get_binary_crate_dirs(source_dir: &Path) -> HashMap<String, PathBuf> {
    let mut map = HashMap::new();

    let output = match Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(source_dir)
        .env("CARGO_HOME", "/cargo")
        .env("RUSTUP_HOME", "/usr/local/rustup")
        .env("RUSTUP_TOOLCHAIN", "1.92-x86_64-unknown-linux-gnu")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
    {
        Ok(output) if output.status.success() => output.stdout,
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                status = ?output.status,
                stderr = %stderr,
                "cargo metadata failed"
            );
            return map;
        }
        Err(e) => {
            warn!(error = %e, "Failed to run cargo metadata");
            return map;
        }
    };

    let json: serde_json::Value = match serde_json::from_slice(&output) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "Failed to parse cargo metadata JSON");
            return map;
        }
    };

    let Some(packages) = json.get("packages").and_then(|p| p.as_array()) else {
        return map;
    };

    for package in packages {
        let Some(manifest_path) = package.get("manifest_path").and_then(|p| p.as_str()) else {
            continue;
        };
        let crate_dir = match Path::new(manifest_path).parent() {
            Some(dir) => dir.to_path_buf(),
            None => continue,
        };

        let Some(targets) = package.get("targets").and_then(|t| t.as_array()) else {
            continue;
        };

        for target in targets {
            let Some(kinds) = target.get("kind").and_then(|k| k.as_array()) else {
                continue;
            };
            let is_bin = kinds.iter().any(|k| k.as_str() == Some("bin"));
            if !is_bin {
                continue;
            }

            let Some(name) = target.get("name").and_then(|n| n.as_str()) else {
                continue;
            };

            debug!(binary = %name, crate_dir = %crate_dir.display(), "Mapped binary to crate");
            map.insert(name.to_owned(), crate_dir.clone());
        }
    }

    info!(
        binary_count = map.len(),
        "Loaded binary crate mappings from cargo metadata"
    );
    map
}
