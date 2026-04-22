default:
    @just --list

check: fmt-check lint test

fmt:
    cargo fmt
    nixfmt *.nix

fmt-check:
    cargo fmt -- --check

lint:
    cargo clippy --all-targets --all-features -- -D warnings
    statix check .
    deadnix .

test:
    cargo test

build:
    cargo build

run *ARGS:
    cargo run -- {{ARGS}}

docker:
    docker compose up --build

clean:
    cargo clean
