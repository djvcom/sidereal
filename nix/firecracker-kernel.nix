# Custom kernel for Firecracker VMs
#
# Firecracker VMs don't have an initrd, so all required drivers must be built
# into the kernel (=y), not as modules (=m). This kernel config ensures the
# essential drivers are built-in: virtio-blk, virtio-net, vsock, and ext4.
{ pkgs }:

pkgs.linuxPackages_latest.kernel.override {
  structuredExtraConfig = with pkgs.lib.kernel; {
    # Firecracker requires these drivers built-in (not modules)
    # Block devices
    VIRTIO = yes;
    VIRTIO_PCI = yes;
    VIRTIO_MMIO = yes;
    VIRTIO_BLK = yes;

    # Networking
    VIRTIO_NET = yes;

    # vsock for host-guest communication
    VSOCKETS = yes;
    VIRTIO_VSOCKETS = yes;

    # Filesystem (ext4 for rootfs)
    EXT4_FS = yes;

    # Minimal console
    SERIAL_8250 = yes;
    SERIAL_8250_CONSOLE = yes;

    # Required by Firecracker
    KVM_GUEST = yes;
    PARAVIRT = yes;
  };
}
