# Sidereal

Self-hosted observability backend for traces, metrics, and logs.

Sidereal receives standard [OpenTelemetry](https://opentelemetry.io/) data via
OTLP (gRPC and HTTP), converts it to [Apache Arrow](https://arrow.apache.org/)
format, stores it as [Parquet](https://parquet.apache.org/) files on object
storage, and queries it with [Apache DataFusion](https://datafusion.apache.org/).

```text
OTLP gRPC (:4317) ─┐                                    ┌─ Object Storage
                    ├→ Redaction → Arrow Conversion → Buffer → Parquet ──┤  (local/S3/GCS/Azure)
OTLP HTTP (:4318) ─┘                                    └───────────────┘
                                                                 │
                                                        DataFusion Query
                                                         (ListingTable)
                                                                 │
                                                        Query API (:3100)
                                                        ├── /sql
                                                        ├── /errors
                                                        └── /deployments
```

## Quickstart

### Docker Compose

The fastest way to evaluate Sidereal is with Docker Compose, which starts
the server alongside an OpenTelemetry Collector and a trace generator:

```sh
docker compose up --build
```

Once running, query the ingested data:

```sh
curl -s http://localhost:3100/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT service_name, count(*) as span_count FROM traces GROUP BY service_name"}'
```

### From source

With [Nix](https://nixos.org/) installed:

```sh
nix develop
just run
```

Or with a Rust toolchain (1.80+):

```sh
cargo run
```

Sidereal starts three servers on the default
[OTLP ports](https://opentelemetry.io/docs/specs/otel/protocol/exporter/)
and a query API on port 3100. Point any OpenTelemetry SDK or Collector at
`localhost:4317` (gRPC) or `localhost:4318` (HTTP) to begin ingesting data.

## Configuration

Configuration is loaded in priority order:

1. Compiled-in defaults
2. `telemetry.toml` in the working directory
3. Environment variables prefixed with `TELEMETRY_`

See [`telemetry.example.toml`](telemetry.example.toml) for a full reference
with all available options.

### Authentication

An optional API key may be configured to protect all data endpoints. When set,
requests must include a valid `Authorization: Bearer <key>` or `X-API-Key: <key>`
header. Health and readiness probes remain open.

```toml
[auth]
api_key = "your-secret-key"
```

The key can also be set via the `TELEMETRY_AUTH_API_KEY` environment variable.

### Query engine

Memory usage during query execution can be bounded to prevent a single query
from exhausting system resources:

```toml
[query]
memory_limit_bytes = 268435456  # 256 MiB (default)
timeout_secs = 30               # default
```

## Servers

| Server    | Default port | Protocol | Purpose                           |
|-----------|-------------|----------|-----------------------------------|
| gRPC      | 4317        | tonic    | OTLP gRPC ingestion              |
| HTTP OTLP | 4318        | axum     | OTLP HTTP ingestion              |
| Query API | 3100        | axum     | SQL queries, errors, deployments  |

### Query API endpoints

| Method | Path                                                | Description                          |
|--------|-----------------------------------------------------|--------------------------------------|
| GET    | `/health`                                           | Health check                         |
| GET    | `/ready`                                            | Readiness probe (verifies DataFusion)|
| POST   | `/sql`                                              | Raw SQL query                        |
| POST   | `/traces`                                           | Structured trace query               |
| POST   | `/metrics`                                          | Structured metric query              |
| POST   | `/logs`                                             | Structured log query                 |
| GET    | `/errors`                                           | List error groups                    |
| GET    | `/errors/stats`                                     | Error statistics                     |
| GET    | `/errors/compare`                                   | Compare errors across time/versions  |
| GET    | `/errors/:fingerprint`                              | Error group details                  |
| GET    | `/errors/:fingerprint/samples`                      | Error samples                        |
| GET    | `/errors/:fingerprint/timeline`                     | Error timeline                       |
| GET    | `/deployments`                                      | List deployments                     |
| GET    | `/deployments/services/{service}/versions`          | Service version history              |
| GET    | `/deployments/services/{service}/versions/{v}/errors` | Errors introduced by a deployment  |

## Storage backends

Data is stored as Parquet files with Hive-style partitioning
(`{signal}/date=YYYY-MM-DD/hour=HH/{ulid}.parquet`), enabling automatic
partition pruning on time-range queries.

| Backend | Config type | Feature flag      | Use case                          |
|---------|-------------|-------------------|-----------------------------------|
| Local   | `local`     | *(default)*       | Development, single-node          |
| S3      | `s3`        | `--features s3`   | Production (AWS, MinIO, Garage)   |
| GCS     | `gcs`       | `--features gcs`  | Production (Google Cloud)         |
| Azure   | `azure`     | `--features azure` | Production (Azure Blob Storage)  |
| Memory  | `memory`    | *(default)*       | Testing only                      |

Retention is managed by the storage backend's lifecycle policies. See
[`storage.rs`](src/storage.rs) for detailed examples of configuring
retention rules for each backend.

## Features

- **OTLP ingestion** — gRPC and HTTP with protobuf and JSON content types
- **Semantic convention columns** — frequently-queried OTel attributes
  (e.g. `service.name`, `http.request.method`) are extracted to dedicated
  Arrow columns for efficient querying
- **CBOR attribute encoding** — remaining attributes are stored as CBOR binary,
  queryable via the `cbor_extract` UDF
- **PII redaction** — configurable ingestion-time redaction pipeline with
  drop, mask, and HMAC-hash actions
- **Error tracking** — content-addressed fingerprinting, grouping, timeline,
  and cross-version comparison
- **Deployment tracking** — correlate deployments with newly introduced errors
- **Backpressure** — configurable buffer limits with 413/503 responses
- **Graceful shutdown** — in-flight requests drain and buffers flush on SIGTERM

## Development

Prerequisites: [Nix](https://nixos.org/) (recommended) or Rust 1.80+ with
`pkg-config` and `libssl-dev`.

```sh
nix develop          # enter dev shell
just check           # format, lint, and test
just test            # run tests only
just lint            # clippy + nix linters
just build           # build release binary
```

## Licence

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
