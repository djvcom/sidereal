#!/usr/bin/env bash
#
# End-to-end demonstration of the AI query companion.
#
# Starts a fresh Sidereal server, ingests a small realistic telemetry story
# (a checkout flow across three services where payments v1.4.2 is deployed
# mid-window and starts failing), starts sidereal-ai against it, and asks
# triage questions. Pass --keep to leave everything running afterwards so
# the chat page can be used interactively.
#
# Model selection honours SIDEREAL_AI_MODEL_* if already set; otherwise
# defaults to Anthropic when ANTHROPIC_API_KEY is present, and a local
# Ollama model when it is not (starting ollama and pulling the model if
# necessary).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

OTLP_HTTP_PORT=14318
QUERY_PORT=13100
AI_PORT=13200
OLLAMA_MODEL_DEFAULT="qwen3:1.7b"

WORKDIR="$(mktemp -d /tmp/sidereal-ai-demo.XXXXXX)"
SERVER_PID=""
AI_PID=""
OLLAMA_PID=""

cleanup() {
    [[ -n "$AI_PID" ]] && kill "$AI_PID" 2>/dev/null || true
    [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
    [[ -n "$OLLAMA_PID" ]] && kill "$OLLAMA_PID" 2>/dev/null || true
    rm -rf "$WORKDIR"
}
trap cleanup EXIT INT TERM

log() { printf '\033[1;36m[demo]\033[0m %s\n' "$*"; }

wait_for() {
    local url="$1" name="$2"
    for _ in $(seq 1 60); do
        curl -sf "$url" >/dev/null 2>&1 && return 0
        sleep 1
    done
    log "$name did not become healthy at $url"
    return 1
}

pretty() {
    if command -v jq >/dev/null 2>&1; then jq .; else cat; echo; fi
}

# ---------------------------------------------------------------------------
# Model provider
# ---------------------------------------------------------------------------

if [[ -z "${SIDEREAL_AI_MODEL_PROVIDER:-}" ]]; then
    if [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
        export SIDEREAL_AI_MODEL_PROVIDER=anthropic
        log "using Anthropic (ANTHROPIC_API_KEY is set)"
    else
        export SIDEREAL_AI_MODEL_PROVIDER=ollama
        export SIDEREAL_AI_MODEL_NAME="${SIDEREAL_AI_MODEL_NAME:-$OLLAMA_MODEL_DEFAULT}"
        log "using Ollama with $SIDEREAL_AI_MODEL_NAME"
        if ! curl -sf http://127.0.0.1:11434/api/tags >/dev/null 2>&1; then
            command -v ollama >/dev/null 2>&1 || {
                log "ollama is not installed; enter the dev shell (nix develop) or set ANTHROPIC_API_KEY"
                exit 1
            }
            log "starting ollama"
            ollama serve >"$WORKDIR/ollama.log" 2>&1 &
            OLLAMA_PID=$!
            wait_for http://127.0.0.1:11434/api/tags "ollama"
        fi
        if ! ollama list 2>/dev/null | grep -q "${SIDEREAL_AI_MODEL_NAME%%:*}"; then
            log "pulling $SIDEREAL_AI_MODEL_NAME"
            ollama pull "$SIDEREAL_AI_MODEL_NAME"
        fi
    fi
fi

# ---------------------------------------------------------------------------
# Sidereal server with fresh storage
# ---------------------------------------------------------------------------

log "building binaries"
cargo build -q -p sidereal -p sidereal-ai --manifest-path "$REPO_ROOT/Cargo.toml"

cat > "$WORKDIR/telemetry.toml" <<EOF
[server]
grpc_addr = "127.0.0.1:14317"
http_addr = "127.0.0.1:$OTLP_HTTP_PORT"
query_addr = "127.0.0.1:$QUERY_PORT"

[storage]
type = "local"
path = "$WORKDIR/data"

[buffer]
max_batch_size = 10
flush_interval_secs = 1
EOF

log "starting sidereal (query API on :$QUERY_PORT)"
(cd "$WORKDIR" && exec "$REPO_ROOT/target/debug/sidereal") >"$WORKDIR/sidereal.log" 2>&1 &
SERVER_PID=$!
wait_for "http://127.0.0.1:$QUERY_PORT/health" "sidereal"

# ---------------------------------------------------------------------------
# Telemetry story
#
# payments v1.4.1 serves checkouts cleanly for half an hour; v1.4.2 is then
# deployed and roughly half of its checkout calls start timing out.
# ---------------------------------------------------------------------------

NOW_NS=$(date +%s%N)
MINUTE_NS=60000000000

rand_hex() { openssl rand -hex "$1"; }

post_otlp() {
    local path="$1" body="$2"
    local response
    response=$(curl -s -w '\n%{http_code}' -X POST "http://127.0.0.1:$OTLP_HTTP_PORT$path" \
        -H 'Content-Type: application/json' -d "$body")
    local code="${response##*$'\n'}"
    if [[ "$code" != "200" ]]; then
        log "ingest to $path failed with HTTP $code: ${response%$'\n'*}"
        exit 1
    fi
}

resource() {
    local service="$1" version="$2"
    cat <<EOF
{"attributes":[
  {"key":"service.name","value":{"stringValue":"$service"}},
  {"key":"service.version","value":{"stringValue":"$version"}},
  {"key":"deployment.environment.name","value":{"stringValue":"production"}}
]}
EOF
}

span() {
    local trace_id="$1" span_id="$2" parent="$3" name="$4" start="$5" end="$6" status="$7"
    local parent_field=""
    [[ -n "$parent" ]] && parent_field="\"parentSpanId\":\"$parent\","
    local status_field='{"code":1}'
    [[ "$status" == "error" ]] && status_field='{"code":2,"message":"payment gateway timed out"}'
    cat <<EOF
{"traceId":"$trace_id","spanId":"$span_id",$parent_field"name":"$name","kind":2,
 "startTimeUnixNano":"$start","endTimeUnixNano":"$end",
 "attributes":[{"key":"http.request.method","value":{"stringValue":"POST"}}],
 "status":$status_field}
EOF
}

send_traces() {
    local version="$1" offset_minutes="$2" count="$3" fail_every="$4"
    local frontend_spans="" checkout_spans="" payments_spans=""
    for i in $(seq 1 "$count"); do
        local trace_id fe_id co_id pa_id start dur_pay status sep=""
        trace_id=$(rand_hex 16)
        fe_id=$(rand_hex 8); co_id=$(rand_hex 8); pa_id=$(rand_hex 8)
        start=$((NOW_NS - offset_minutes * MINUTE_NS + i * MINUTE_NS / 2))
        status="ok"; dur_pay=300000000
        if [[ "$fail_every" -gt 0 && $((i % fail_every)) -eq 0 ]]; then
            status="error"; dur_pay=2500000000
        fi
        [[ -n "$frontend_spans" ]] && sep=","
        frontend_spans+="$sep$(span "$trace_id" "$fe_id" "" "POST /api/checkout" "$start" $((start + dur_pay + 200000000)) ok)"
        checkout_spans+="$sep$(span "$trace_id" "$co_id" "$fe_id" "CheckoutService/Complete" $((start + 50000000)) $((start + dur_pay + 150000000)) "$status")"
        payments_spans+="$sep$(span "$trace_id" "$pa_id" "$co_id" "PaymentsService/Charge" $((start + 100000000)) $((start + 100000000 + dur_pay)) "$status")"
    done
    post_otlp /v1/traces "{\"resourceSpans\":[
          {\"resource\":$(resource frontend 2.3.0),\"scopeSpans\":[{\"spans\":[$frontend_spans]}]},
          {\"resource\":$(resource checkout 1.9.4),\"scopeSpans\":[{\"spans\":[$checkout_spans]}]},
          {\"resource\":$(resource payments "$version"),\"scopeSpans\":[{\"spans\":[$payments_spans]}]}
        ]}"
}

send_deployment() {
    local version="$1" offset_minutes="$2"
    local ts=$((NOW_NS - offset_minutes * MINUTE_NS))
    post_otlp /v1/logs "{\"resourceLogs\":[{\"resource\":$(resource payments "$version"),\"scopeLogs\":[{\"logRecords\":[
          {\"timeUnixNano\":\"$ts\",\"severityNumber\":9,
           \"body\":{\"stringValue\":\"deployment of payments $version\"},
           \"attributes\":[
             {\"key\":\"event.name\",\"value\":{\"stringValue\":\"deployment\"}},
             {\"key\":\"deployment.id\",\"value\":{\"stringValue\":\"deploy-payments-$version\"}},
             {\"key\":\"deployment.status\",\"value\":{\"stringValue\":\"succeeded\"}}
           ]}
        ]}]}]}"
}

log "ingesting telemetry: payments v1.4.1 (healthy) then v1.4.2 (failing)"
send_deployment "1.4.1" 55
send_traces "1.4.1" 50 20 0
send_deployment "1.4.2" 25
send_traces "1.4.2" 20 20 2

log "waiting for buffers to flush"
sleep 4
SPAN_COUNT=$(curl -sf -X POST "http://127.0.0.1:$QUERY_PORT/sql" \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT count(*) AS spans FROM traces","format":"json"}')
log "ingested: $SPAN_COUNT"

# ---------------------------------------------------------------------------
# sidereal-ai
# ---------------------------------------------------------------------------

mkdir -p "$WORKDIR/xdg"
log "starting sidereal-ai (chat on http://127.0.0.1:$AI_PORT)"
XDG_CONFIG_HOME="$WORKDIR/xdg" \
    SIDEREAL_URL="http://127.0.0.1:$QUERY_PORT" \
    SIDEREAL_LISTEN_ADDRESS="127.0.0.1:$AI_PORT" \
    RUST_LOG=info \
    "$REPO_ROOT/target/debug/sidereal-ai" >"$WORKDIR/sidereal-ai.log" 2>&1 &
AI_PID=$!
wait_for "http://127.0.0.1:$AI_PORT/health" "sidereal-ai"

ask() {
    local question="$1"
    log "Q: $question"
    curl -s -m 600 -X POST "http://127.0.0.1:$AI_PORT/ask" \
        -H 'Content-Type: application/json' \
        -d "{\"question\":\"$question\"}" | pretty
    echo
}

ask "Which services are reporting traces, and how many spans does each have?"
ask "Which service has the highest error rate, and is the problem tied to a particular version?"
ask "What deployments happened recently, and does the timing line up with any change in errors?"

if [[ "$KEEP" -eq 1 ]]; then
    log "leaving services running; chat at http://127.0.0.1:$AI_PORT (Ctrl-C to stop)"
    wait "$AI_PID"
else
    log "done; pass --keep to leave the stack running for interactive use"
fi
