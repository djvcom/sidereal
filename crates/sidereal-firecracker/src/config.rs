//! VM configuration types.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for a Firecracker microVM.
#[derive(Debug, Clone)]
pub struct VmConfig {
    /// Path to the kernel image (vmlinux).
    pub kernel_path: PathBuf,

    /// Path to the rootfs image.
    pub rootfs_path: PathBuf,

    /// Number of vCPUs to allocate.
    pub vcpu_count: u8,

    /// Memory size in MiB.
    pub mem_size_mib: u32,

    /// vsock context ID (CID) for this VM.
    pub vsock_cid: u32,

    /// Boot arguments for the kernel.
    pub boot_args: String,
}

impl Default for VmConfig {
    fn default() -> Self {
        Self {
            kernel_path: PathBuf::new(),
            rootfs_path: PathBuf::new(),
            vcpu_count: 1,
            mem_size_mib: 128,
            vsock_cid: 3, // Guest CID starts at 3 (0=hypervisor, 1=local, 2=host)
            boot_args: "console=ttyS0 reboot=k panic=1 pci=off".to_string(),
        }
    }
}

impl VmConfig {
    pub fn new(kernel_path: PathBuf, rootfs_path: PathBuf) -> Self {
        Self {
            kernel_path,
            rootfs_path,
            ..Default::default()
        }
    }

    pub fn with_vcpus(mut self, count: u8) -> Self {
        self.vcpu_count = count;
        self
    }

    pub fn with_memory(mut self, mib: u32) -> Self {
        self.mem_size_mib = mib;
        self
    }

    pub fn with_cid(mut self, cid: u32) -> Self {
        self.vsock_cid = cid;
        self
    }

    pub fn with_boot_args(mut self, args: impl Into<String>) -> Self {
        self.boot_args = args.into();
        self
    }
}

/// Firecracker API request types for VM configuration.
pub mod api {
    use super::*;

    #[derive(Debug, Serialize)]
    pub struct BootSource {
        pub kernel_image_path: String,
        pub boot_args: String,
    }

    #[derive(Debug, Serialize)]
    pub struct Drive {
        pub drive_id: String,
        pub path_on_host: String,
        pub is_root_device: bool,
        pub is_read_only: bool,
    }

    #[derive(Debug, Serialize)]
    pub struct MachineConfig {
        pub vcpu_count: u8,
        pub mem_size_mib: u32,
    }

    #[derive(Debug, Serialize)]
    pub struct Vsock {
        pub vsock_id: String,
        pub guest_cid: u32,
        pub uds_path: String,
    }

    #[derive(Debug, Serialize)]
    pub struct InstanceActionInfo {
        pub action_type: String,
    }

    #[derive(Debug, Deserialize)]
    pub struct ErrorResponse {
        pub fault_message: String,
    }
}
