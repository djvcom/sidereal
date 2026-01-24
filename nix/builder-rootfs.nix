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
  rustToolchainOrig =
    if pkgs ? rust-bin then
      pkgs.rust-bin.stable.latest.default.override {
        targets = [ "x86_64-unknown-linux-musl" ];
      }
    else
      pkgs.rustc;

  # Patch the Rust toolchain binaries to use /lib64 interpreter and /opt/glibc libs
  # This is necessary because nix-built binaries have hardcoded nix store paths
  rustToolchain = pkgs.runCommand "rust-toolchain-patched" {
    nativeBuildInputs = [ pkgs.patchelf ];
  } ''
    cp -r ${rustToolchainOrig} $out
    chmod -R u+w $out

    # Patch all ELF binaries in bin/
    for f in $out/bin/*; do
      if [ -f "$f" ] && [ -x "$f" ]; then
        # Check if it's an ELF binary
        if head -c 4 "$f" | grep -q "ELF"; then
          patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 \
                   --set-rpath /opt/glibc/lib:/opt/gcc/lib \
                   "$f" 2>/dev/null || true
        fi
      fi
    done

    # Also patch libraries that might be executed (like rustc_driver)
    find $out/lib -name "*.so*" -type f 2>/dev/null | while read f; do
      if head -c 4 "$f" | grep -q "ELF"; then
        patchelf --set-rpath /opt/glibc/lib:/opt/gcc/lib "$f" 2>/dev/null || true
      fi
    done || true
  '';

  # Patch cargo-zigbuild to use /lib64 interpreter
  cargoZigbuildPatched = pkgs.runCommand "cargo-zigbuild-patched" {
    nativeBuildInputs = [ pkgs.patchelf ];
  } ''
    cp -r ${pkgs.cargo-zigbuild} $out
    chmod -R u+w $out

    for f in $out/bin/*; do
      if [ -f "$f" ] && [ -x "$f" ]; then
        if head -c 4 "$f" | grep -q "ELF"; then
          patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 \
                   --set-rpath /opt/glibc/lib:/opt/gcc/lib \
                   "$f" 2>/dev/null || true
        fi
      fi
    done
  '';

  # Patch git to use /lib64 interpreter
  gitPatched = pkgs.runCommand "git-patched" {
    nativeBuildInputs = [ pkgs.patchelf ];
  } ''
    cp -r ${pkgs.gitMinimal} $out
    chmod -R u+w $out

    find $out -type f -executable | while read f; do
      if head -c 4 "$f" 2>/dev/null | grep -q "ELF"; then
        patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 \
                 --set-rpath /opt/glibc/lib:/opt/gcc/lib \
                 "$f" 2>/dev/null || true
      fi
    done || true
  '';

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

    # Build tools (patched to use /lib64 interpreter)
    {
      source = cargoZigbuildPatched;
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

    # Dynamic linker symlink (required for patched binaries)
    {
      source = "${pkgs.glibc}/lib/ld-linux-x86-64.so.2";
      target = "/lib64/ld-linux-x86-64.so.2";
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

    # Git for cargo (some build scripts use git), patched for /lib64
    {
      source = gitPatched;
      target = "/opt/git";
    }
  ];
}
