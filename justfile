# Sidereal development commands

# Default recipe - list available commands
default:
    @just --list

# Run all checks (format, lint, test)
check: fmt-check lint test

# Format all code
fmt:
    cargo fmt
    nixfmt-rfc-style *.nix

# Check formatting without modifying
fmt-check:
    cargo fmt -- --check
    nixfmt-rfc-style --check *.nix

# Run lints
lint:
    cargo clippy --all-targets --all-features -- -D warnings
    statix check .
    deadnix .

# Run tests
test:
    cargo test

# Build
build:
    cargo build

# Run the telemetry server
run *ARGS:
    cargo run -- {{ARGS}}

# Run via Docker Compose (builds image, starts MinIO + OTel Collector + tracegen)
docker:
    docker compose up --build

# Clean build artifacts
clean:
    cargo clean
