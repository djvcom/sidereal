//! Implementation of the `sidereal deploy` command.

use sidereal_firecracker::{VmConfig, VmManager};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use thiserror::Error;
use tokio::process::Command;
use tracing::warn;

#[derive(Error, Debug)]
pub enum DeployError {
    #[error("Configuration not found: sidereal.toml")]
    ConfigNotFound,

    #[error("Build failed: {0}")]
    BuildFailed(String),

    #[error("Cross-compilation failed: {0}")]
    CrossCompileFailed(String),

    #[error("Firecracker not found. Install with: nix develop")]
    FirecrackerNotFound,

    #[error("KVM not available: {0}")]
    KvmNotAvailable(String),

    #[error("Missing file: {0}")]
    MissingFile(String),

    #[error("Firecracker error: {0}")]
    Firecracker(#[from] sidereal_firecracker::FirecrackerError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Arguments for the deploy command.
pub struct DeployArgs {
    /// Deploy to local Firecracker VM
    pub local: bool,

    /// Keep VM running after deployment (for debugging)
    pub keep_alive: bool,

    /// Skip building the project (use existing binary)
    pub skip_build: bool,
}

impl Default for DeployArgs {
    fn default() -> Self {
        Self {
            local: true,
            keep_alive: false,
            skip_build: false,
        }
    }
}

pub async fn run(args: DeployArgs) -> Result<(), DeployError> {
    if args.local {
        run_local_deploy(args).await
    } else {
        println!("Remote deployment not yet implemented.");
        println!("Use --local for local Firecracker deployment.");
        Ok(())
    }
}

async fn run_local_deploy(args: DeployArgs) -> Result<(), DeployError> {
    println!("Sidereal Local Deployment");
    println!("=========================");
    println!();

    check_prerequisites()?;

    let sidereal_dir = get_sidereal_dir()?;
    let kernel_path = sidereal_dir.join("kernel/vmlinux");
    let rootfs_path = sidereal_dir.join("rootfs/builder.ext4");
    let work_dir = sidereal_dir.join("vms");

    std::fs::create_dir_all(&work_dir)?;

    if !kernel_path.exists() {
        return Err(DeployError::MissingFile(format!(
            "Kernel not found at {}. Deploy via NixOS or copy manually.",
            kernel_path.display()
        )));
    }

    if !rootfs_path.exists() {
        return Err(DeployError::MissingFile(format!(
            "Rootfs not found at {}. Build with: just build-rootfs",
            rootfs_path.display()
        )));
    }

    if !args.skip_build {
        println!("Building for Firecracker (x86_64-unknown-linux-musl)...");
        build_for_firecracker().await?;
    }

    let runtime_binary = find_runtime_binary()?;
    println!("Runtime binary: {}", runtime_binary.display());

    println!("Starting Firecracker VM...");
    let vm_manager = VmManager::new(&work_dir)?;

    let config = VmConfig::new(kernel_path, rootfs_path)
        .with_vcpus(1)
        .with_memory(128)
        .with_cid(3);

    let mut vm = vm_manager.start(config).await?;

    println!("Waiting for VM to be ready...");
    match vm.wait_ready(Duration::from_secs(30)).await {
        Ok(()) => {
            println!();
            println!("VM is ready!");
            println!("  CID: {}", vm.cid());
            println!("  vsock path: {}", vm.vsock_uds_path().display());
        }
        Err(e) => {
            warn!("VM may not be fully ready: {}", e);
        }
    }

    if args.keep_alive {
        println!();
        println!("VM is running. Press Ctrl+C to stop.");
        tokio::signal::ctrl_c().await?;
    }

    println!();
    println!("Shutting down VM...");
    vm.shutdown().await?;

    println!("Deployment complete.");
    Ok(())
}

fn check_prerequisites() -> Result<(), DeployError> {
    if which::which("firecracker").is_err() {
        return Err(DeployError::FirecrackerNotFound);
    }

    VmManager::check_kvm().map_err(|e| DeployError::KvmNotAvailable(e.to_string()))?;

    if !PathBuf::from("sidereal.toml").exists() {
        return Err(DeployError::ConfigNotFound);
    }

    Ok(())
}

fn get_sidereal_dir() -> Result<PathBuf, DeployError> {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_owned());
    let sidereal_dir = PathBuf::from(home).join(".sidereal");
    std::fs::create_dir_all(&sidereal_dir)?;
    Ok(sidereal_dir)
}

async fn build_for_firecracker() -> Result<(), DeployError> {
    let status = Command::new("cargo")
        .args([
            "build",
            "--release",
            "--target",
            "x86_64-unknown-linux-musl",
            "-p",
            "sidereal-runtime",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .map_err(|e| DeployError::BuildFailed(e.to_string()))?;

    if !status.success() {
        return Err(DeployError::CrossCompileFailed(
            "cargo build failed".to_owned(),
        ));
    }

    Ok(())
}

const RUNTIME_BINARY_NAME: &str = "sidereal-runtime";
const MUSL_TARGET: &str = "x86_64-unknown-linux-musl";

fn find_runtime_binary() -> Result<PathBuf, DeployError> {
    try_relative_release_path()
        .or_else(try_workspace_release_path)
        .or_else(try_relative_debug_path)
        .ok_or_else(|| {
            DeployError::BuildFailed(
                "Runtime binary not found. Run 'cargo build --release --target x86_64-unknown-linux-musl -p sidereal-runtime' first.".to_owned(),
            )
        })
}

fn try_relative_release_path() -> Option<PathBuf> {
    let path = PathBuf::from(format!(
        "target/{MUSL_TARGET}/release/{RUNTIME_BINARY_NAME}"
    ));
    path.exists().then_some(path)
}

fn try_relative_debug_path() -> Option<PathBuf> {
    let path = PathBuf::from(format!("target/{MUSL_TARGET}/debug/{RUNTIME_BINARY_NAME}"));
    path.exists().then_some(path)
}

fn try_workspace_release_path() -> Option<PathBuf> {
    let workspace_root = find_workspace_root_via_cargo()?;
    let path = workspace_root.join(format!(
        "target/{MUSL_TARGET}/release/{RUNTIME_BINARY_NAME}"
    ));
    path.exists().then_some(path)
}

const SEARCH_STR: &str = "\"workspace_root\":\"";

fn find_workspace_root_via_cargo() -> Option<PathBuf> {
    let cargo = which::which("cargo").ok()?;
    let output = std::process::Command::new(&cargo)
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let metadata = String::from_utf8(output.stdout).ok()?;
    let start = metadata.find(SEARCH_STR)?;
    let rest = &metadata[start + SEARCH_STR.len()..];
    let end = rest.find('"')?;
    Some(PathBuf::from(&rest[..end]))
}
