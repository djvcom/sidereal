# Sidereal

Self-hosted observability backend for traces, metrics, and logs.

Sidereal receives standard [OpenTelemetry](https://opentelemetry.io/) data via
OTLP (gRPC and HTTP), converts it to [Apache Arrow](https://arrow.apache.org/)
format, stores it as [Parquet](https://parquet.apache.org/) files on object
storage, and queries it with [Apache DataFusion](https://datafusion.apache.org/).
A companion service, `sidereal-ai`, answers natural-language questions about
the stored telemetry using a model of your choosing.

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
                                                                 │
                                                        sidereal-ai (:3200)
                                                        ├── /ask
                                                        └── chat page
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
  -d '{"sql": "SELECT \"service.name\", count(*) AS span_count FROM traces GROUP BY \"service.name\""}'
```

### From source

With [Nix](https://nixos.org/) installed:

```sh
nix develop
just run
```

Or with a Rust toolchain (1.95+):

```sh
cargo run
```

Sidereal starts three servers on the default
[OTLP ports](https://opentelemetry.io/docs/specs/otel/protocol/exporter/)
and a query API on port 3100. Point any OpenTelemetry SDK or Collector at
`localhost:4317` (gRPC) or `localhost:4318` (HTTP) to begin ingesting data.

## Asking questions in natural language

The `sidereal-ai` companion service turns questions like *"which services are
erroring right now?"* or *"did error rates change after yesterday's payments
deployment?"* into SQL and API calls against Sidereal, and answers with the
evidence attached: every response lists the queries that produced it, so
answers can be verified rather than trusted.

Bring your own model — an Anthropic API key, a local
[Ollama](https://ollama.com/) instance, or any OpenAI-compatible endpoint:

```toml
# ~/.config/sidereal-ai/config.toml

[sidereal]
url = "http://127.0.0.1:3100"

[model]
provider = "anthropic"        # or "ollama", "openai"
# name = "claude-opus-4-8"    # defaults per provider
# api_key = "sk-ant-..."      # falls back to ANTHROPIC_API_KEY
# base_url = "..."            # for self-hosted endpoints
```

Run it and open the chat page at `http://127.0.0.1:3200`, or ask over HTTP:

```sh
cargo run -p sidereal-ai

curl -s http://127.0.0.1:3200/ask \
  -H 'Content-Type: application/json' \
  -d '{"question": "Which services have the most errors in the last hour?"}'
```

Settings can also be provided as `SIDEREAL_AI_`-prefixed environment
variables (e.g. `SIDEREAL_AI_MODEL_PROVIDER=ollama`). To see the whole
loop working against generated telemetry, run `just ai-demo`.

## Querying by attribute

Every attribute a service sends is stored as a column of its own, with
its own type, as well as in the complete attribute blob. Nothing has to
be declared first. Two SQL functions read an attribute by name:

```sql
-- What share of crawler requests for product pages were told the
-- product is in stock?
SELECT CAST(COUNT(*) FILTER (WHERE attr('product.in_stock')) AS DOUBLE) / COUNT(*)
FROM traces
WHERE attr('user_agent.original') LIKE '%Googlebot%'
  AND attr('http.route') = '/products/{id}'
  AND resource_attr('deployment.environment.name') = 'production'
```

- `attr('key')` reads an attribute of the span, log record or data point
- `resource_attr('key')` reads an attribute of the resource that emitted it

Both resolve to the underlying column while the query is planned, so
filters, grouping and aggregation on any attribute cost what they would
on a built-in column. Integers, floats and booleans keep their types;
arrays, maps and byte strings are available only through the blob with
`cbor_extract`. An attribute nobody has sent yet reads as `NULL`, so a
query can be written before the first service is instrumented.

A key sent with different types by different services keeps one column
per type, and reads through these functions as text. Attribute columns
appear in `SELECT *` under names of the form `attr.<type>.<key>` and
`resource.<type>.<key>`.

## Reliability conditions

A reliability condition is a promise about a kind of request, stated in
the terms of whoever depends on it: *crawler requests for a product page
are told whether the product is in stock, 99% of the time, within
800 ms*. Nothing has to error for such a promise to be broken, so it is
defined over what requests carry rather than over any service's health.

```json
{
  "id": "googlebot-product-availability",
  "name": "Googlebot sees product availability",
  "requests": {"all": [
    {"attribute": "user_agent.original", "op": "contains", "value": "Googlebot"},
    {"attribute": "http.route", "op": "equals", "value": "/products/{id}"}
  ]},
  "success": {
    "all": [{"attribute": "product.in_stock", "op": "exists"}],
    "within_ms": 800
  },
  "target": {"percent": 99.0, "window_days": 30, "minimum_requests": 100},
  "impact": {"statement": "Google disapproves our listings", "severity": "high"},
  "notify": [{"name": "Search marketing", "channel": "email", "address": "search@example.com"}],
  "trial_days": 7
}
```

A request is a whole trace: each test passes if *any* span of the trace
passes it, so the service that knows who is asking and the service that
knows how it went need not be the same. Speed is the caller's wait, the
duration of the entry span.

| Method | Path                              | Purpose                                      |
|--------|-----------------------------------|----------------------------------------------|
| POST   | `/conditions/preview`             | How do these requests behave today?          |
| GET    | `/conditions/attributes`          | Which attributes can a condition refer to?   |
| POST   | `/conditions`                     | Create a condition                           |
| GET    | `/conditions`                     | Every condition with its current standing    |
| GET    | `/conditions/{id}`                | One condition with its current standing      |
| PUT    | `/conditions/{id}`                | Change a condition                           |
| DELETE | `/conditions/{id}`                | Retire a condition                           |
| GET    | `/conditions/{id}/history`        | Every revision: who changed what, and when   |
| GET    | `/conditions/{id}/backtest`       | How it would have fared over stored history  |

`/conditions/preview` needs no condition to exist. Given a description of
some requests it returns how many there were, how often they succeeded,
how long they took, real examples of each outcome, which service
versions took part and in what share of the failures, a suggested target
that is already being met, and the same findings as plain sentences.

A condition's standing is one of `healthy`, `at_risk` (most of the
failure allowance used), `breached`, `not_enough_data` or `paused`. A new
condition can start with a trial, during which it is measured and shown
but flagged as not yet notifying anyone.

Definitions are stored in the bucket under `config/conditions/`, one
object per revision, so the record of changes is the list of objects.
**A bucket lifecycle rule that expires telemetry must not cover the
`config/` prefix.** Changes are attributed to the `X-Sidereal-Author`
request header.

Conditions are evaluated when asked for. Continuous monitoring and
notifications are not implemented yet: `notify` is recorded but nothing
is sent.

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
cache_bytes = 268435456         # 256 MiB (default), 0 disables
metadata_cache_bytes = 268435456  # 256 MiB (default)
```

`cache_bytes` sizes an in-memory cache of object storage reads. Flushed
files never change, so repeated queries over the same data are answered
without further requests to remote storage.

## Servers

| Server      | Default port | Protocol | Purpose                            |
|-------------|-------------|----------|-------------------------------------|
| gRPC        | 4317        | tonic    | OTLP gRPC ingestion                |
| HTTP OTLP   | 4318        | axum     | OTLP HTTP ingestion                |
| Query API   | 3100        | axum     | SQL queries, errors, deployments    |
| sidereal-ai | 3200        | axum     | Natural-language questions, chat UI |

### Query API endpoints

| Method | Path                                                | Description                          |
|--------|-----------------------------------------------------|--------------------------------------|
| GET    | `/health`                                           | Health check                         |
| GET    | `/ready`                                            | Readiness probe (verifies DataFusion)|
| POST   | `/sql`                                              | Raw SQL query                        |
| POST   | `/traces`                                           | Structured trace query               |
| GET    | `/traces/{trace_id}`                                | Every span of one trace, oldest first |
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

Retention is built in: configure a `[retention]` section and a background
sweep deletes date partitions older than the configured number of days on
any backend. Provider lifecycle rules remain an alternative for cloud
object stores; see [`storage.rs`](crates/sidereal/src/storage.rs) for
examples.

```toml
[retention]
days = 30
```

The bucket is the only source of truth. Sidereal keeps a catalogue of
its files in memory so that queries never list the bucket, and rebuilds
it from storage on every start: there is no other database, and nothing
besides the bucket to back up or recover. Objects may be removed from
underneath it, for example by a lifecycle rule, without breaking
anything: a query that meets a missing file answers from the data that
remains, and a periodic refresh reconciles the rest.

```toml
[catalogue]
refresh_interval_secs = 300  # default
```

Every flush writes a new small file, so a background compactor merges
the files of each hour partition into fewer, larger, sorted ones: first
in groups while the hour is still receiving data, then into its final
form once it has gone quiet. The swap is recorded in the bucket beside
the data, so queries never see rows twice or lose them, and a restart
finishes or undoes whatever a crash interrupted.

```toml
[compaction]
enabled = true            # default
settle_secs = 900         # quiet time before an hour takes its final form
target_file_bytes = 268435456
```

Bucket lifecycle rules expire objects by the time they were written,
whereas `[retention]` goes by the date of the telemetry inside them.
Compaction rewrites data into new objects, which restarts that clock
for a lifecycle rule but not for `[retention]`.

Crash safety is built in too: configure a `[wal]` section and buffered
telemetry is appended to a local write-ahead log before it is
acknowledged, then replayed on startup if the process died before a
flush. Segments are removed automatically once their contents are
confirmed in object storage.

```toml
[wal]
path = "/var/lib/sidereal/wal"
```

## Features

- **OTLP ingestion** — gRPC and HTTP with protobuf and JSON content types
- **Semantic convention columns** — frequently-queried OTel attributes
  (e.g. `service.name`, `http.request.method`) are extracted to dedicated
  Arrow columns for efficient querying
- **Attributes as columns** — every attribute is also stored as its own
  typed column, so `attr('any.key')` filters and groups as fast as a
  built-in column, with nothing to declare in advance
- **Complete attribute record** — all attributes are also kept as CBOR
  binary, queryable via the `cbor_extract` UDF, including the arrays and
  maps that have no column form
- **PII redaction** — configurable ingestion-time redaction pipeline with
  drop, mask, and HMAC-hash actions
- **Error tracking** — content-addressed fingerprinting, grouping, timeline,
  and cross-version comparison
- **Deployment tracking** — correlate deployments with newly introduced errors
- **Natural-language querying** — the `sidereal-ai` companion answers
  questions over the stored telemetry with verifiable provenance, using
  Anthropic, Ollama, or any OpenAI-compatible model
- **Crash safety** — optional write-ahead log makes acknowledged telemetry
  survive a crash, with automatic replay on startup
- **Trace lookup by ID** — an in-memory index of each file's trace IDs
  sends `GET /traces/{trace_id}` straight to the files holding the trace,
  with no time range needed and no storage requests to find them
- **Compaction** — small flushed files are merged in the background into
  large sorted ones, crash-safely and without a second database
- **Backpressure** — configurable buffer limits with 413/503 responses
- **Graceful shutdown** — in-flight requests drain and buffers flush on SIGTERM

## Development

Prerequisites: [Nix](https://nixos.org/) (recommended) or Rust 1.95+ with
`pkg-config` and `libssl-dev`.

```sh
nix develop          # enter dev shell
just check           # format, lint, and test
just test            # run tests only
just lint            # clippy + nix linters
just build           # build release binary
just bench           # end-to-end benchmark (ingest, flush, fixed query set)
just ai-demo         # end-to-end demo: ingest sample data, ask questions
```

## Licence

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
