# Builder VM rootfs image
#
# This creates a minimal rootfs for compiling Rust code inside Firecracker VMs.
# Contains: Rust toolchain, cargo-zigbuild, zig, gcc, git, and essential libraries.
#
# Build:
#   docker build -t sidereal-builder-rootfs .
#
# Extract rootfs:
#   docker create --name tmp sidereal-builder-rootfs
#   docker export tmp | tar -C /path/to/staging -xf -
#   docker rm tmp

FROM rust:1.92-bookworm

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libssl-dev \
    pkg-config \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install musl target for static linking
RUN rustup target add x86_64-unknown-linux-musl

# Install cargo-zigbuild
RUN cargo install cargo-zigbuild

# Install zig (required by cargo-zigbuild)
ARG ZIG_VERSION=0.13.0
RUN curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-linux-x86_64-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt \
    && ln -s /opt/zig-linux-x86_64-${ZIG_VERSION}/zig /usr/local/bin/zig

# Create mount points for virtio-blk drives
RUN mkdir -p /source /target /cargo

# Ensure /sbin exists for init
RUN mkdir -p /sbin

# The builder runtime will be copied as /sbin/init by the build script
# (can't COPY from host during nix build, so this is done externally)

# Create symlinks directly to toolchain binaries (bypassing rustup shims which try to sync)
RUN TOOLCHAIN_BIN="/usr/local/rustup/toolchains/1.92-x86_64-unknown-linux-gnu/bin" && \
    ln -sf "$TOOLCHAIN_BIN/cargo" /usr/bin/cargo && \
    ln -sf "$TOOLCHAIN_BIN/rustc" /usr/bin/rustc && \
    ln -sf /usr/local/cargo/bin/cargo-zigbuild /usr/bin/cargo-zigbuild && \
    ln -sf /usr/local/bin/zig /usr/bin/zig

# Clean up cargo cache to reduce image size
RUN rm -rf /usr/local/cargo/registry /usr/local/cargo/git

# Set up environment for builds
ENV CARGO_HOME=/cargo
ENV PATH="/usr/local/cargo/bin:${PATH}"
