FROM rust:1.80-bookworm AS builder

WORKDIR /build

RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY tests/ tests/
COPY benches/ benches/

ARG FEATURES="s3"
RUN cargo build --release --features "${FEATURES}"

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/sidereal /usr/local/bin/sidereal

EXPOSE 4317 4318 3100

ENTRYPOINT ["sidereal"]
