#!/usr/bin/env bash
# End-to-end load scenarios against the docker-compose.dev.yml stack (PG + Redis + silo).
#
#   docker compose -f docker-compose.dev.yml up -d --wait
#   scripts/bench.sh                 # all scenarios
#   scripts/bench.sh s3_hot churn    # selected scenarios
#
# Env: BENCH_DIR (default /tmp/imagelib-bench), C (concurrency, 64), D (measure duration, 20s),
#      N (dataset objects, 1000). Results: $BENCH_DIR/results/<scenario>.{json,txt,log}
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BENCH_DIR=${BENCH_DIR:-/tmp/imagelib-bench}
DATA=$BENCH_DIR/data
RES=$BENCH_DIR/results
C=${C:-64}
D=${D:-20s}
N=${N:-1000}
PORT=18200
DC="docker compose -f $ROOT/docker-compose.dev.yml"
REDIS_ADDR=localhost:56379

mkdir -p "$RES"
go build -o "$BENCH_DIR/imagelib" "$ROOT/cmd"
go build -o "$BENCH_DIR/loadtest" "$ROOT/cmd/loadtest"
[ -f "$DATA/manifest.tsv" ] || "$BENCH_DIR/loadtest" gen -dir "$DATA" -n "$N"

APP_PID=""

reset_state() {
  $DC exec -T silo sh -c 'mcli rm -r --force local/images/bench' >/dev/null 2>&1 || true
  $DC exec -T redis sh -c "redis-cli --scan --pattern 'bench:*' | xargs -r redis-cli del" >/dev/null
  $DC exec -T postgres psql -U postgres -qc 'drop database if exists bench with (force)' -c 'create database bench' 2>&1 | grep -v NOTICE || true
}

# start_app <name> [ENV=VALUE ...]
start_app() {
  local name=$1; shift
  reset_state
  env -i PATH="$PATH" HOME="$HOME" \
    ADDR=127.0.0.1:$PORT INSTANCE_ID=bench LOCAL_PATH="$DATA" \
    DATABASE_URL='postgres://postgres:postgres@localhost:55432/bench?sslmode=disable' \
    REDIS_ADDR=$REDIS_ADDR REDIS_PREFIX=bench: S3_PREFIX=bench \
    S3_ENDPOINT=http://localhost:59000 S3_REGION=us-east-1 S3_BUCKET=images S3_FORCE_PATH_STYLE=true \
    S3_PUBLIC_URL=http://localhost:59000/images B2_APPLICATION_KEY_ID=siloadmin B2_APPLICATION_KEY=siloadmin-secret \
    S3_CAPACITY=1GiB MIGRATE_THRESHOLD=2 MIGRATE_WORKERS=8 EVENT_BUFFER=65536 \
    "$@" "$BENCH_DIR/imagelib" >"$RES/$name.log" 2>&1 &
  APP_PID=$!
  for _ in $(seq 50); do curl -fs "localhost:$PORT/healthz" >/dev/null 2>&1 && return; sleep 0.2; done
  echo "app failed to start"; cat "$RES/$name.log"; exit 1
}

stop_app() {
  [ -n "$APP_PID" ] && kill -INT "$APP_PID" 2>/dev/null && wait "$APP_PID" 2>/dev/null || true
  APP_PID=""
}
trap 'stop_app; $DC unpause redis silo >/dev/null 2>&1 || true' EXIT

load() { # load <label> <duration> [extra loadtest flags]
  local label=$1 dur=$2; shift 2
  "$BENCH_DIR/loadtest" run -url "http://127.0.0.1:$PORT" -manifest "$DATA/manifest.tsv" \
    -c "$C" -d "$dur" -label "$label" -redis "$REDIS_ADDR" "$@"
}

warm() { # drive traffic so the hot set migrates, then let the scheduler drain
  load warmup "$1" >/dev/null
  sleep 3
}

app_counts() {
  printf 'migrations=%s evictions=%s\n' \
    "$(grep -c 'migrated to s3' "$RES/$1.log" || true)" "$(grep -c 'evicted from s3' "$RES/$1.log" || true)"
}

scenario_local_only() {   # baseline: nothing ever migrates
  start_app local_only MIGRATE_THRESHOLD=1e12
  load local_only "$D" -out "$RES/local_only.json" | tee "$RES/local_only.txt"
  stop_app
}

scenario_local_only_nocache() { # same, index cache off (isolates the cache effect)
  start_app local_only_nocache MIGRATE_THRESHOLD=1e12 INDEX_CACHE_TTL=0
  load local_only_nocache "$D" -out "$RES/local_only_nocache.json" | tee "$RES/local_only_nocache.txt"
  stop_app
}

scenario_cache_fixed_rate() { # cache on vs off at the same offered load (fair tail comparison)
  local rate=${RATE:-8000}
  for ttl in 1s 0; do
    start_app "cache_rate_ttl$ttl" MIGRATE_THRESHOLD=1e12 INDEX_CACHE_TTL=$ttl
    load "cache_rate_ttl$ttl" "$D" -rate "$rate" -out "$RES/cache_rate_ttl$ttl.json" | tee "$RES/cache_rate_ttl$ttl.txt"
    stop_app
  done
}

scenario_s3_direct() {    # baseline without imagelib: the same dataset straight from silo's public URL
  $DC cp "$DATA" silo:/tmp/benchdata >/dev/null
  $DC exec -T silo sh -c 'mcli mirror --overwrite /tmp/benchdata local/images/direct >/dev/null && rm -rf /tmp/benchdata'
  "$BENCH_DIR/loadtest" run -url http://localhost:59000/images/direct -manifest "$DATA/manifest.tsv" \
    -c "$C" -d "$D" -label s3_direct -out "$RES/s3_direct.json" | tee "$RES/s3_direct.txt"
  $DC exec -T silo sh -c 'mcli rm -r --force local/images/direct' >/dev/null
}

scenario_s3_hot() {       # hot set served from S3, index cache on
  start_app s3_hot
  warm 15s
  load s3_hot "$D" -out "$RES/s3_hot.json" | tee "$RES/s3_hot.txt"
  stop_app
}

scenario_s3_hot_nocache() { # same, index cache off
  start_app s3_hot_nocache INDEX_CACHE_TTL=0
  warm 15s
  load s3_hot_nocache "$D" -out "$RES/s3_hot_nocache.json" | tee "$RES/s3_hot_nocache.txt"
  stop_app
}

scenario_churn() {        # capacity ≪ working set: continuous migrate/evict
  start_app churn S3_CAPACITY=48MiB EVICT_MIN_IDLE=2s EVICT_DELETE_DELAY=3s EVICT_INTERVAL=2s
  load churn 40s -timeline -bytes-key bench:s3:bytes -out "$RES/churn.json" | tee "$RES/churn.txt"
  app_counts churn | tee -a "$RES/churn.txt"
  stop_app
}

outage() { # outage <name> <service> [env...]
  local name=$1 svc=$2; shift 2
  start_app "$name" "$@"
  warm 15s
  ( sleep 10; $DC pause "$svc" >/dev/null 2>&1; echo "paused $svc at t=10s"; sleep 10; $DC unpause "$svc" >/dev/null 2>&1; echo "unpaused $svc at t=20s" ) &
  local chaos=$!
  load "$name" 35s -timeline -out "$RES/$name.json" | tee "$RES/$name.txt"
  wait $chaos
  grep -E 'breaker|index lost|leadership' "$RES/$name.log" | tee -a "$RES/$name.txt" || true
  stop_app
}

scenario_redis_outage() { outage redis_outage redis; }
scenario_s3_outage()    { outage s3_outage silo S3_UPSTREAM_TIMEOUT=2s S3_BREAKER_COOLDOWN=5s; }

ALL=(local_only local_only_nocache cache_fixed_rate s3_direct s3_hot s3_hot_nocache churn redis_outage s3_outage)
for s in "${@:-${ALL[@]}}"; do
  echo "################ $s"
  "scenario_$s"
done
reset_state
$DC exec -T postgres psql -U postgres -qc 'drop database if exists bench with (force)' >/dev/null 2>&1 || true
echo "results in $RES"
