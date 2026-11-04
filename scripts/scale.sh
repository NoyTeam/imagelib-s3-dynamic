#!/usr/bin/env bash
# Large-scale test of the scheduler's bulk operations (index rebuild, recount, reconcile,
# orphan scan, heat GC, eviction) against isolated PostgreSQL 18 and Redis 7 containers.
#
#   scripts/scale.sh                                  # production-sized defaults (~15 min)
#   SCALE_PG_ROWS=3000000 SCALE_HEAT=1000000 scripts/scale.sh   # quick run
#
# Env: SCALE_PG_ROWS (30M), SCALE_HEAT (10M heat entries), SCALE_LEGACY_REBUILD (1M; 0 skips),
#      SCALE_OUT (JSON results, default /tmp/imagelib-scale.json), KEEP=1 keeps the containers.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
PG=imagelib-scale-pg
REDIS=imagelib-scale-redis
export SCALE_OUT=${SCALE_OUT:-/tmp/imagelib-scale.json}
export SCALE_DATABASE_URL=postgres://postgres:postgres@localhost:55433/postgres?sslmode=disable
export SCALE_REDIS_ADDR=localhost:56380

cleanup() { [ "${KEEP:-0}" = 1 ] || docker rm -f "$PG" "$REDIS" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker rm -f "$PG" "$REDIS" >/dev/null 2>&1 || true

docker run -d --name "$PG" -p 55433:5432 -e POSTGRES_PASSWORD=postgres --shm-size=1g \
  postgres:18-alpine -c shared_buffers=1GB -c maintenance_work_mem=1GB -c max_wal_size=8GB \
  -c work_mem=64MB >/dev/null
# Production-like Redis: noeviction, no persistence (the test measures command latency, not fsync).
docker run -d --name "$REDIS" -p 56380:6379 redis:7-alpine \
  redis-server --maxmemory-policy noeviction --save '' --appendonly no >/dev/null

for _ in $(seq 60); do
  docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && docker exec "$REDIS" redis-cli ping >/dev/null 2>&1 && break
  sleep 1
done

cd "$ROOT"
go test -tags scale -run TestScale -count=1 -v -timeout 3h ./internal/scheduler/ 2>&1 \
  | grep -vE '^\s*$' | grep -vE 'reconcile: (restored|dropped)|orphan object in bucket|evicted from s3'
echo "results: $SCALE_OUT"
