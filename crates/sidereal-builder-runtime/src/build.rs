//! Build execution for the builder runtime.
//!
//! Handles running cargo zigbuild and streaming output back to the host.

use std::path::Path;
use std::process::Stdio;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use sidereal_build::protocol::{BinaryInfo, BuildOutput, BuildRequest, BuildResult};

use crate::vsock::MessageSender;

/// Execute a build request and stream output back to the host.
pub async fn execute_build(
    request: BuildRequest,
    mut sender: MessageSender,
) -> anyhow::Result<BuildResult> {
    let start = Instant::now();

    eprintln!("[build] Starting build: {}", request.build_id);
    eprintln!("[build] Source: {}", request.source_path);
    eprintln!("[build] Target: {}", request.target_path);
    eprintln!("[build] Triple: {}", request.target_triple);

    // Set up cargo environment
    std::env::set_var("CARGO_HOME", &request.cargo_home);
    std::env::set_var("CARGO_TARGET_DIR", &request.target_path);

    // Build cargo arguments
    let mut args = vec!["zigbuild", "--workspace"];

    if request.release {
        args.push("--release");
    }

    args.push("--target");
    args.push(&request.target_triple);

    if request.locked {
        args.push("--locked");
    }

    // Spawn cargo process
    let mut child = Command::new("cargo")
        .args(&args)
        .current_dir(&request.source_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    eprintln!("[build] Spawned cargo process");

    // Stream stdout and stderr
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let mut stdout_lines = Vec::new();
    let mut stderr_lines = Vec::new();

    // Process stdout
    if let Some(stdout) = stdout {
        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("[cargo stdout] {line}");
            stdout_lines.push(line.clone());
            // Send output to host (ignore errors if connection closed)
            let _ = sender.send_output(BuildOutput::Stdout(line)).await;
        }
    }

    // Process stderr
    if let Some(stderr) = stderr {
        let reader = BufReader::new(stderr);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("[cargo stderr] {line}");
            stderr_lines.push(line.clone());
            // Send output to host (ignore errors if connection closed)
            let _ = sender.send_output(BuildOutput::Stderr(line)).await;
        }
    }

    // Wait for process to complete
    let status = child.wait().await?;
    let duration = start.elapsed();

    eprintln!("[build] Cargo exited with: {:?}", status.code());

    let stdout_str = stdout_lines.join("\n");
    let stderr_str = stderr_lines.join("\n");

    // Build result
    let result = if status.success() {
        // Discover built binaries
        let binaries = discover_binaries(
            Path::new(&request.source_path),
            Path::new(&request.target_path),
            &request.target_triple,
            request.release,
        )?;

        eprintln!("[build] Found {} binaries", binaries.len());
        for binary in &binaries {
            eprintln!("[build]   - {} at {}", binary.name, binary.path);
        }

        BuildResult::success(binaries, duration.as_secs_f64())
            .with_stdout(stdout_str)
            .with_stderr(stderr_str)
    } else {
        let exit_code = status.code().unwrap_or(-1);
        BuildResult::failure(exit_code, stderr_str, duration.as_secs_f64()).with_stdout(stdout_str)
    };

    // Send result to host
    sender.send_result(result.clone()).await?;

    Ok(result)
}

/// Discover binaries built in the target directory.
fn discover_binaries(
    source_dir: &Path,
    target_dir: &Path,
    target_triple: &str,
    release: bool,
) -> anyhow::Result<Vec<BinaryInfo>> {
    let profile = if release { "release" } else { "debug" };
    let binary_dir = target_dir.join(target_triple).join(profile);

    let mut binaries = Vec::new();

    if !binary_dir.exists() {
        return Ok(binaries);
    }

    // Scan for Cargo.toml files to find binary targets
    discover_binaries_recursive(source_dir, source_dir, &binary_dir, &mut binaries)?;

    Ok(binaries)
}

fn discover_binaries_recursive(
    _workspace_root: &Path,
    dir: &Path,
    binary_dir: &Path,
    binaries: &mut Vec<BinaryInfo>,
) -> anyhow::Result<()> {
    let cargo_toml = dir.join("Cargo.toml");

    if cargo_toml.exists() {
        let content = std::fs::read_to_string(&cargo_toml)?;

        // Simple TOML parsing for binary names
        let bin_names = extract_binary_names(&content, dir);

        for name in bin_names {
            let binary_path = binary_dir.join(&name);
            if binary_path.exists() {
                binaries.push(BinaryInfo::new(
                    name,
                    binary_path.to_string_lossy().to_string(),
                    dir.to_string_lossy().to_string(),
                ));
            }
        }
    }

    // Recursively scan subdirectories
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name != "target" && !name.starts_with('.') {
                    discover_binaries_recursive(dir, &path, binary_dir, binaries)?;
                }
            }
        }
    }

    Ok(())
}

/// Extract binary names from Cargo.toml content.
fn extract_binary_names(content: &str, crate_dir: &Path) -> Vec<String> {
    let mut names = Vec::new();

    // Parse [[bin]] sections
    let mut in_bin_section = false;
    for line in content.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("[[bin]]") {
            in_bin_section = true;
            continue;
        }

        if trimmed.starts_with('[') && in_bin_section {
            in_bin_section = false;
        }

        if in_bin_section && trimmed.starts_with("name") {
            if let Some(name) = extract_string_value(trimmed) {
                names.push(name);
            }
        }
    }

    // If no [[bin]] sections, check for [package] name with src/main.rs
    if names.is_empty() {
        let main_rs = crate_dir.join("src/main.rs");
        if main_rs.exists() {
            for line in content.lines() {
                let trimmed = line.trim();
                if trimmed.starts_with("name") {
                    if let Some(name) = extract_string_value(trimmed) {
                        // Binary names use underscores, not hyphens
                        names.push(name.replace('-', "_"));
                        break;
                    }
                }
            }
        }
    }

    names
}

/// Extract a string value from a TOML line like `name = "foo"`.
fn extract_string_value(line: &str) -> Option<String> {
    let parts: Vec<&str> = line.splitn(2, '=').collect();
    if parts.len() != 2 {
        return None;
    }

    let value = parts[1].trim();
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        Some(value[1..value.len() - 1].to_owned())
    } else {
        None
    }
}
