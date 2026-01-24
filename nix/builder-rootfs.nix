# Builder VM rootfs image (~500MB-1GB)
#
# This rootfs contains everything needed to compile Rust code:
# - sidereal-builder-runtime as /sbin/init
# - Rust toolchain with musl target
# - cargo-zigbuild and Zig for cross-compilation
# - gcc for proc-macro compilation
# - Essential libraries (glibc, openssl)
# - Shell and coreutils for cargo scripts
#
# Usage:
#   let
#     builderRootfs = import ./builder-rootfs.nix {
#       inherit pkgs;
#       sidereal-builder-runtime = pkgs.sidereal-builder-runtime;
#     };
#   in builderRootfs
#
{ pkgs, sidereal-builder-runtime }:

let
  makeExt4Fs = import ./make-ext4-fs.nix { inherit pkgs; };

  # Rust toolchain with musl target
  rustToolchain =
    if pkgs ? rust-bin then
      pkgs.rust-bin.stable.latest.default.override {
        targets = [ "x86_64-unknown-linux-musl" ];
      }
    else
      pkgs.rustc;

  # Minimal busybox for shell scripts
  busybox = pkgs.busybox;

in
makeExt4Fs {
  name = "sidereal-builder-rootfs";
  size = "3G";
  label = "builder";
  # Zig has many small files (Windows cross-compilation support), so we need
  # more inodes than the default allocation
  bytesPerInode = 4096;

  contents = [
    # Init process (builder runtime)
    {
      source = "${sidereal-builder-runtime}/bin/sidereal-builder-runtime";
      target = "/sbin/init";
    }

    # Rust toolchain
    {
      source = rustToolchain;
      target = "/opt/rust";
    }

    # Build tools
    {
      source = pkgs.cargo-zigbuild;
      target = "/opt/cargo-zigbuild";
    }
    {
      source = pkgs.zig;
      target = "/opt/zig";
    }
    {
      source = pkgs.gcc;
      target = "/opt/gcc";
    }

    # Essential libraries
    {
      source = pkgs.glibc;
      target = "/opt/glibc";
    }
    {
      source = pkgs.openssl.out;
      target = "/opt/openssl";
    }
    {
      source = pkgs.openssl.dev;
      target = "/opt/openssl-dev";
    }
    {
      source = pkgs.pkg-config;
      target = "/opt/pkg-config";
    }

    # Shell for cargo-zigbuild scripts (uses busybox for minimal footprint)
    {
      source = busybox;
      target = "/opt/busybox";
    }

    # CA certificates for HTTPS (cargo fetch)
    {
      source = pkgs.cacert;
      target = "/opt/cacert";
    }

    # Git for cargo (some build scripts use git)
    {
      source = pkgs.gitMinimal;
      target = "/opt/git";
    }
  ];
}
