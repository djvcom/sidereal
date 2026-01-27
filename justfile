# Sidereal development commands

# Default recipe - list available commands
default:
    @just --list

# Run all checks (format, lint, test)
check: fmt-check lint test

# Format all code
fmt:
    cargo fmt --all
    nixfmt-rfc-style *.nix

# Check formatting without modifying
fmt-check:
    cargo fmt --all -- --check
    nixfmt-rfc-style --check *.nix

# Run lints
lint:
    cargo clippy --all-targets --all-features -- -D warnings
    statix check .
    deadnix .

# Run tests
test:
    cargo test --all

# Build all crates
build:
    cargo build --all

# Build for WASM target
build-wasm:
    cargo build --target wasm32-wasip1 -p sidereal-sdk --release

# Run the CLI in development mode
run *ARGS:
    cargo run -p sidereal-cli -- {{ARGS}}

# Clean build artifacts
clean:
    cargo clean

# Watch for changes and rebuild
watch:
    cargo watch -x 'build --all'

# Deploy to local NixOS system
deploy:
    nix flake update sidereal --flake /etc/nixos
    sudo nixos-rebuild switch --flake /etc/nixos#terminus

# Build the builder-runtime binary (static musl)
build-runtime:
    cargo build --release -p sidereal-builder-runtime --target x86_64-unknown-linux-musl

# Build the builder VM rootfs from Docker (includes smoke tests)
build-rootfs: build-runtime
    ./docker/builder-rootfs/build-rootfs.sh

# Deploy the builder rootfs to the sidereal data directory
deploy-rootfs: build-rootfs
    sudo dd if=target/builder-rootfs.ext4 of=/var/lib/sidereal/rootfs/builder.ext4 bs=4M status=progress
    sudo chown sidereal:sidereal /var/lib/sidereal/rootfs/builder.ext4

# Rebuild and deploy everything (NixOS config + rootfs + restart)
deploy-all: deploy-rootfs
    nix flake update sidereal --flake /etc/nixos
    sudo nixos-rebuild switch --flake /etc/nixos#terminus
    sudo systemctl restart sidereal

# Run the e2e build test
e2e:
    cargo run -p sidereal-cli -- push --url http://localhost:8422

# Quick rebuild cycle: runtime binary + rootfs + deploy + restart
rebuild: deploy-rootfs
    sudo systemctl restart sidereal
