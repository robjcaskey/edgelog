#!/usr/bin/env bash
set -euo pipefail

ROOT="${EDGLOG_SPAM_ROOT:-/tmp/edgelog-spam-trio}"
DURATION="${SPAM_DURATION:-5}"
BURST="${SPAM_BURST:-1000}"
TICK_MS="${SPAM_TICK_MS:-100}"
PAYLOAD_BYTES="${SPAM_PAYLOAD_BYTES:-48}"

cd "$(dirname "$0")/.."

rm -rf "$ROOT"
mkdir -p "$ROOT"/{buffers,config,logs,out}
touch "$ROOT/logs/api.log" "$ROOT/logs/worker.log"

cargo build --bins --examples

cat >"$ROOT/config/api.conf" <<'CONFIG'
mode=exclude
healthcheck
graphql.introspection
sample=25
throttle_per_second=250
ring api-errors 64 level=ERROR
ring api-slow 64 slow=true
ring api-all 128 *
metrics=on
metric_label node=on
metric_label ring=on
metric_label outcome=on
CONFIG

cat >"$ROOT/config/worker.conf" <<'CONFIG'
mode=exclude
job.heartbeat
sample=50
throttle_per_second=250
ring worker-errors 64 level=ERROR
ring worker-retries 64 job.retry
ring worker-slow 64 slow=true
metrics=on
metric_label node=on
metric_label ring=on
metric_label outcome=on
CONFIG

cat >"$ROOT/config/db.conf" <<'CONFIG'
mode=exclude
db.vacuum_heartbeat
sample=100
throttle_per_second=250
ring db-errors 64 level=ERROR
ring db-locks 64 db.lock_wait
ring db-slow 64 slow=true
metrics=on
metric_label node=on
metric_label ring=on
metric_label outcome=on
CONFIG

pids=()

cleanup() {
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
}

trap cleanup EXIT

target/debug/edgelog \
  --config "$ROOT/config/api.conf" \
  --input "$ROOT/logs/api.log" \
  --from-end \
  --buffers-dir "$ROOT/buffers/api" \
  --node-id spam-api \
  >"$ROOT/out/api.filtered" \
  2>"$ROOT/out/api.edgelog.err" &
pids+=("$!")
api_edgelog_pid="$!"

target/debug/edgelog \
  --config "$ROOT/config/worker.conf" \
  --input "$ROOT/logs/worker.log" \
  --from-end \
  --buffers-dir "$ROOT/buffers/worker" \
  --node-id spam-worker \
  >"$ROOT/out/worker.filtered" \
  2>"$ROOT/out/worker.edgelog.err" &
pids+=("$!")
worker_edgelog_pid="$!"

target/debug/examples/spam_trio \
  --role api-gateway \
  --service-id api-a \
  --log-file "$ROOT/logs/api.log" \
  --duration-seconds "$DURATION" \
  --burst "$BURST" \
  --tick-ms "$TICK_MS" \
  --payload-bytes "$PAYLOAD_BYTES" &
api_app_pid="$!"
pids+=("$!")

target/debug/examples/spam_trio \
  --role job-worker \
  --service-id worker-a \
  --log-file "$ROOT/logs/worker.log" \
  --duration-seconds "$DURATION" \
  --burst "$BURST" \
  --tick-ms "$TICK_MS" \
  --payload-bytes "$PAYLOAD_BYTES" &
worker_app_pid="$!"
pids+=("$!")

(
  target/debug/examples/spam_trio \
    --role db-server \
    --service-id db-legacy-a \
    --stdout \
    --duration-seconds "$DURATION" \
    --burst "$BURST" \
    --tick-ms "$TICK_MS" \
    --payload-bytes "$PAYLOAD_BYTES" |
    target/debug/edgelog \
      --config "$ROOT/config/db.conf" \
      --buffers-dir "$ROOT/buffers/db" \
      --node-id spam-db
) >"$ROOT/out/db.filtered" 2>"$ROOT/out/db.edgelog.err" &
db_pipeline_pid="$!"
pids+=("$!")

wait "$api_app_pid"
wait "$worker_app_pid"
wait "$db_pipeline_pid"

sleep 1
kill "$api_edgelog_pid" "$worker_edgelog_pid" 2>/dev/null || true
wait "$api_edgelog_pid" "$worker_edgelog_pid" 2>/dev/null || true

trap - EXIT
cleanup

echo "spam trio demo root: $ROOT"
echo "duration=${DURATION}s burst=${BURST} tick_ms=${TICK_MS} payload_bytes=${PAYLOAD_BYTES}"
echo
echo "source logs:"
wc -l "$ROOT/logs/api.log" "$ROOT/logs/worker.log"
echo
echo "filtered stdout captures:"
wc -l "$ROOT/out/api.filtered" "$ROOT/out/worker.filtered" "$ROOT/out/db.filtered"
echo
echo "ring buffers:"
find "$ROOT/buffers" -maxdepth 3 -type f -name '*.log' -print -exec wc -l {} \;
