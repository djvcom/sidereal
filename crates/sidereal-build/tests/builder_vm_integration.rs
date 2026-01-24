#![allow(missing_docs)]
//! Integration test for the builder VM.
//!
//! This test boots a Firecracker VM with the builder rootfs and verifies
//! the builder runtime starts correctly by connecting via vsock.
//!
//! Run with: cargo test --package sidereal-build --test builder_vm_integration -- --ignored
//!
//! Requirements:
//! - /dev/kvm must be accessible
//! - Firecracker must be in PATH
//! - BUILDER_KERNEL_PATH and BUILDER_ROOTFS_PATH environment variables must be set
//!
//! Example:
//! ```bash
//! export BUILDER_KERNEL_PATH=/nix/store/.../vmlinux
//! export BUILDER_ROOTFS_PATH=$(nix build .#sidereal-builder-rootfs --print-out-paths)
//! cargo test --package sidereal-build --test builder_vm_integration -- --ignored --nocapture
//! ```

use std::path::PathBuf;
use std::time::Duration;

use rkyv::api::high::HighSerializer;
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use sidereal_firecracker::config::VmConfig;
use sidereal_firecracker::vm::VmManager;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::timeout;

const BUILDER_PORT: u32 = 1028;
const VM_BOOT_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Wire protocol types (mirrored from builder-runtime).
#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub struct BuildRequest {
    pub build_id: String,
    pub source_path: String,
    pub target_path: String,
    pub cargo_home: String,
    pub target_triple: String,
    pub release: bool,
    pub locked: bool,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub enum BuildMessage {
    Request(BuildRequest),
    Output(BuildOutput),
    Result(BuildResult),
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub enum BuildOutput {
    Stdout(String),
    Stderr(String),
    Progress { stage: String, progress: f32 },
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub struct BuildResult {
    pub success: bool,
    pub exit_code: i32,
    pub binaries: Vec<BinaryInfo>,
    pub duration_secs: f64,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub struct BinaryInfo {
    pub name: String,
    pub path: String,
    pub crate_dir: String,
}

impl From<BuildRequest> for BuildMessage {
    fn from(req: BuildRequest) -> Self {
        Self::Request(req)
    }
}

#[tokio::test]
#[ignore = "requires Firecracker and KVM"]
async fn test_builder_vm_boot() {
    // Check environment
    let kernel_path = std::env::var("BUILDER_KERNEL_PATH")
        .map(PathBuf::from)
        .expect("BUILDER_KERNEL_PATH environment variable must be set");

    let rootfs_path = std::env::var("BUILDER_ROOTFS_PATH")
        .map(PathBuf::from)
        .expect("BUILDER_ROOTFS_PATH environment variable must be set");

    println!("Kernel: {}", kernel_path.display());
    println!("Rootfs: {}", rootfs_path.display());

    assert!(kernel_path.exists(), "Kernel not found: {kernel_path:?}");
    assert!(rootfs_path.exists(), "Rootfs not found: {rootfs_path:?}");

    // Create temp directory for VM sockets
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Create VM manager
    let vm_manager = VmManager::new(temp_dir.path()).expect("Failed to create VM manager");

    // Configure VM
    let config = VmConfig::new(kernel_path, rootfs_path)
        .with_vcpus(2)
        .with_memory(4096) // 4GB for compilation
        .with_cid(3)
        .with_boot_args("console=ttyS0 reboot=k panic=1 pci=off");

    println!("Starting VM...");

    // Start VM
    let vm = timeout(VM_BOOT_TIMEOUT, vm_manager.start(config))
        .await
        .expect("VM boot timed out")
        .expect("Failed to start VM");

    println!("VM started, vsock UDS: {}", vm.vsock_uds_path().display());

    // Wait for the builder runtime to start listening
    let vsock_path = vm.vsock_uds_path().to_path_buf();
    let connected = timeout(CONNECT_TIMEOUT, async {
        let mut attempts = 0;
        loop {
            attempts += 1;
            println!("Connection attempt {attempts}...");

            // Try to connect via vsock UDS
            match connect_vsock(&vsock_path, BUILDER_PORT).await {
                Ok(stream) => {
                    println!("Connected to builder runtime via vsock");
                    return Ok::<_, std::io::Error>(stream);
                }
                Err(e) => {
                    if attempts >= 20 {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    })
    .await;

    match connected {
        Ok(Ok(mut stream)) => {
            println!("Successfully connected to builder runtime!");

            // Send a dummy build request (will fail because no source mounted, but proves
            // the runtime is working)
            let request = BuildRequest {
                build_id: "test-build-001".to_owned(),
                source_path: "/source".to_owned(),
                target_path: "/target".to_owned(),
                cargo_home: "/cargo".to_owned(),
                target_triple: "x86_64-unknown-linux-musl".to_owned(),
                release: true,
                locked: false,
            };

            let message = BuildMessage::Request(request);
            if let Err(e) = send_message(&mut stream, &message).await {
                println!("Failed to send build request: {e}");
            } else {
                println!("Sent build request, waiting for response...");

                // Read response (expecting failure since no source is mounted)
                match timeout(Duration::from_secs(10), receive_message(&mut stream)).await {
                    Ok(Ok(response)) => {
                        println!("Received response: {response:?}");
                    }
                    Ok(Err(e)) => {
                        println!("Error reading response: {e}");
                    }
                    Err(_) => {
                        println!("Timeout waiting for response");
                    }
                }
            }
        }
        Ok(Err(e)) => {
            panic!("Failed to connect to builder runtime: {e}");
        }
        Err(_) => {
            panic!("Timeout connecting to builder runtime");
        }
    }

    println!("Test passed!");
}

/// Connect to vsock via the Firecracker UDS bridge.
async fn connect_vsock(uds_path: &PathBuf, port: u32) -> std::io::Result<UnixStream> {
    // Firecracker exposes vsock via a Unix domain socket
    // Connection format: connect to UDS, then send "CONNECT {port}\n"
    let mut stream = UnixStream::connect(uds_path).await?;

    let connect_cmd = format!("CONNECT {port}\n");
    stream.write_all(connect_cmd.as_bytes()).await?;

    // Read the response (OK or error)
    let mut response = vec![0u8; 64];
    let n = stream.read(&mut response).await?;
    let response_str = String::from_utf8_lossy(&response[..n]);

    if response_str.starts_with("OK") {
        Ok(stream)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!("vsock connect failed: {response_str}"),
        ))
    }
}

/// Send a message over the stream.
async fn send_message<T>(stream: &mut UnixStream, message: &T) -> std::io::Result<()>
where
    T: Archive,
    T: for<'a> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
{
    let bytes = rkyv::to_bytes::<RkyvError>(message)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

    #[allow(clippy::cast_possible_truncation)]
    let len = bytes.len() as u32;

    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;

    Ok(())
}

/// Receive a message from the stream.
async fn receive_message(stream: &mut UnixStream) -> std::io::Result<BuildMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;

    #[allow(clippy::cast_possible_truncation)]
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > 16 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Message too large",
        ));
    }

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    let message: BuildMessage = rkyv::from_bytes::<BuildMessage, RkyvError>(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

    Ok(message)
}
