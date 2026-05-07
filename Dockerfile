FROM rust:bookworm AS builder

WORKDIR /build

RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

COPY rust-toolchain.toml ./
RUN rustup show

COPY Cargo.toml Cargo.lock ./
COPY crates/sidereal/ crates/sidereal/
COPY crates/sidereal-ai/Cargo.toml crates/sidereal-ai/Cargo.toml
COPY crates/sidereal-ai/src/ crates/sidereal-ai/src/

ARG FEATURES="s3"
RUN cargo build --release --features "sidereal/${FEATURES}" -p sidereal

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/sidereal /usr/local/bin/sidereal

EXPOSE 4317 4318 3100

ENTRYPOINT ["sidereal"]
