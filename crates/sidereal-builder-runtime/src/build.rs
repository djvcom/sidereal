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

use crate::protocol::{BinaryInfo, BuildOutput, BuildRequest};
use crate::vsock::{send_output, VsockError};

/// Result of a compilation.
#[derive(Debug)]
pub struct CompileResult {
    /// Whether compilation succeeded.
    pub success: bool,
    /// Exit code from cargo.
    pub exit_code: i32,
    /// Compilation duration in seconds.
    pub duration_secs: f64,
    /// Captured stdout.
    pub stdout: String,
    /// Captured stderr.
    pub stderr: String,
}

impl CompileResult {
    /// Create a failure result for spawn/wait errors.
    fn spawn_error(error: &str) -> Self {
        Self {
            success: false,
            exit_code: -1,
            duration_secs: 0.0,
            stdout: String::new(),
            stderr: error.to_owned(),
        }
    }
}

/// Execute cargo build and stream output back to the host.
///
/// Returns the compilation result without sending the final BuildResult.
/// The caller is responsible for constructing and sending the final result.
pub async fn compile(
    stream: &mut VsockStream,
    request: &BuildRequest,
) -> Result<CompileResult, VsockError> {
    let start_time = Instant::now();

    info!(
        build_id = %request.build_id,
        source = %request.source_path().display(),
        target = %request.target_path().display(),
        "Starting compilation"
    );

    setup_environment(&request.cargo_home(), &request.target_path())?;

    let mut cmd = build_cargo_command(request);

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            error!(error = %e, "Failed to spawn cargo");
            return Ok(CompileResult::spawn_error(&format!(
                "Failed to spawn cargo: {e}"
            )));
        }
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let mut stdout_lines = Vec::new();
    let mut stderr_lines = Vec::new();

    let mut stdout_reader = stdout.map(|s| BufReader::new(s).lines());
    let mut stderr_reader = stderr.map(|s| BufReader::new(s).lines());

    let mut stdout_done = stdout_reader.is_none();
    let mut stderr_done = stderr_reader.is_none();

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
                    Ok(None) => stdout_done = true,
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
                    Ok(None) => stderr_done = true,
                    Err(e) => {
                        warn!(error = %e, "Error reading stderr");
                        stderr_done = true;
                    }
                }
            }
        }
    }

    let status = match child.wait().await {
        Ok(status) => status,
        Err(e) => {
            error!(error = %e, "Failed to wait for cargo");
            return Ok(CompileResult::spawn_error(&format!(
                "Failed to wait for cargo: {e}"
            )));
        }
    };

    let duration_secs = start_time.elapsed().as_secs_f64();

    #[allow(clippy::as_conversions)]
    let exit_code = status.code().unwrap_or(-1) as i32;

    if status.success() {
        info!(duration_secs, "Compilation completed successfully");
    } else {
        error!(exit_code, "Compilation failed");
    }

    Ok(CompileResult {
        success: status.success(),
        exit_code,
        duration_secs,
        stdout: stdout_lines.join("\n"),
        stderr: stderr_lines.join("\n"),
    })
}

/// Discover compiled binaries in the target directory.
pub async fn discover_binaries(request: &BuildRequest) -> Vec<BinaryInfo> {
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

fn setup_environment(cargo_home: &Path, target_dir: &Path) -> Result<(), VsockError> {
    std::fs::create_dir_all(cargo_home).map_err(VsockError::Io)?;
    std::fs::create_dir_all(target_dir).map_err(VsockError::Io)?;
    Ok(())
}

fn build_cargo_command(request: &BuildRequest) -> Command {
    let mut cmd = Command::new("cargo");

    cmd.arg("zigbuild");
    cmd.arg("--workspace");

    cmd.arg("--target");
    cmd.arg(&request.target_triple);

    if request.release {
        cmd.arg("--release");
    }

    if request.locked {
        cmd.arg("--locked");
    }

    cmd.current_dir(request.source_path());

    cmd.env("CARGO_HOME", request.cargo_home());
    cmd.env("CARGO_TARGET_DIR", request.target_path());

    cmd.env("SSL_CERT_FILE", "/etc/ssl/certs/ca-bundle.crt");
    cmd.env("SSL_CERT_DIR", "/etc/ssl/certs");

    cmd.env("HTTP_PROXY", "http://127.0.0.1:1080");
    cmd.env("HTTPS_PROXY", "http://127.0.0.1:1080");

    cmd.env("RUSTUP_HOME", "/usr/local/rustup");
    cmd.env("RUSTUP_TOOLCHAIN", "1.92-x86_64-unknown-linux-gnu");
    cmd.env("RUSTUP_UPDATE_ROOT", "file:///nonexistent");

    cmd.env("CARGO_INCREMENTAL", "0");

    // Ensure cache directories go to writable tmpfs, not read-only rootfs
    cmd.env("HOME", "/tmp");
    cmd.env("XDG_CACHE_HOME", "/tmp/.cache");

    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    cmd
}

async fn is_elf_binary(path: &Path) -> bool {
    let Ok(file) = tokio::fs::File::open(path).await else {
        return false;
    };

    let mut reader = BufReader::new(file);
    let mut magic = [0u8; 4];

    if reader.read_exact(&mut magic).await.is_err() {
        return false;
    }

    magic == [0x7f, b'E', b'L', b'F']
}

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
