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
