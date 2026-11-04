# imagelib-s3-dynamic

[![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](go.mod)
[![Storage](https://img.shields.io/badge/storage-S3%20compatible-569A31)](#s3--bucket)

**A self-tiering image origin.** Local disk stays the source of truth; objects that turn
hot are copied to a capacity-bounded S3 bucket and served from there via reverse proxy.
Cold objects are evicted by LRU, keeping the bucket within a configured size budget.

```
request ─▶ imagelib ─┬─ index says "in S3" ─▶ reverse proxy to the bucket's public URL
                     └─ otherwise ──────────▶ stream from local disk ─▶ async "origin read" event
                                                                           │
                         leader-elected scheduler ◀────── Redis stream ◀───┘
                         heat ≥ threshold → upload → PG + Redis index
                         over budget      → LRU eviction → delayed bucket delete
```

---

## Table of contents

- [Features](#features)
- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Operations](#operations)
- [Development](#development)
- [Performance](#performance)
- [Limitations](#limitations)
- [Project layout](#project-layout)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Heat-driven migration.** Each origin read adds to an exponentially decaying score;
  an object is uploaded once the score reaches a threshold.
- **Hard capacity budget.** The bucket never holds more than `S3_CAPACITY` bytes. Space is
  reserved before each upload, and LRU eviction frees it.
- **Anti-thrash admission.** A new object may only evict objects that have been idle for
  `EVICT_MIN_IDLE`, so two hot objects that don't fit together can't keep evicting each other.
- **Zero-copy request path.** Local objects are streamed straight from disk, with `Range`,
  `HEAD` and conditional requests supported. S3 objects are reverse-proxied without buffering.
- **Always correct, degrades to local.**
  - S3 returning an error (404/403/5xx) or timing out → the request falls back to local disk
    before any byte is written to the client.
  - Redis or S3 unhealthy → circuit breakers route everything to local.
  - Hung dependencies → short timeouts keep requests from stalling for long.
- **Cheap at scale.** An in-process index cache (Ristretto, positive and negative entries)
  plus per-key `singleflight` makes Redis lookups scale with the number of distinct hot keys,
  not with request rate. Origin-read events are aggregated per key before they are published.
- **Self-healing state.**
  - PostgreSQL is the durable record of which objects live in S3.
  - The Redis index is rebuilt automatically if it is lost.
  - A periodic reconciler repairs drift between PG, Redis and the bucket, including orphan objects.
- **Single-leader scheduler.** Leadership uses a fenced Redis lock. When a leader shuts down, it
  keeps the lock until all of its in-flight work has stopped, so two leaders never overlap.
- **Schema auto-push.** The embedded `.gcorm` schema is applied on startup (advisory-locked);
  destructive changes are refused unless explicitly allowed.
- **No caching policy imposed.** The S3 tier stores no `Cache-Control` metadata and strips any
  `Cache-Control`/`Expires` coming from the bucket, so both tiers return identical caching
  headers. Set the cache policy at your CDN or edge.

## How it works

### Request path

1. The request path is normalized into an object key. `..` segments are resolved,
   dot-files are rejected, and symlinks cannot escape `LOCAL_PATH`.
2. The proxy checks whether the key lives in S3. It reads the in-process cache first and asks
   Redis only on a miss; concurrent misses for the same key share a single Redis call.
3. **In S3** → the request is reverse-proxied to `S3_PUBLIC_URL/S3_PREFIX/key`. `Cookie`,
   `Authorization` and the query string are not forwarded, and `Set-Cookie` and cache headers
   are removed from the response.
4. **Not in S3** → the file is streamed from local disk and an origin-read event is queued
   without blocking the request. HEAD requests and fallbacks for objects already in S3 are not
   counted as origin reads.

Every response carries `X-Storage-Tier: s3 | local`.

### Scheduler (leader only)

- **Consume events.** Origin-read events are read from a Redis stream through a consumer group.
- **Update heat.** Each object's score is updated as
  `score = score · 2^(−Δt / MIGRATE_HALF_LIFE) + reads`.
- **Migrate.** When the score reaches `MIGRATE_THRESHOLD`:
  1. reserve space, evicting LRU objects that are idle for at least `EVICT_MIN_IDLE`;
  2. upload the file;
  3. verify the local file did not change during the upload;
  4. record the object in PostgreSQL, then add it to the Redis index.

  If any step fails, the earlier steps are rolled back.
- **Evict.** The object is removed from the index first, so proxies serve it locally right away.
  PostgreSQL is then updated, and the bucket object is deleted after `EVICT_DELETE_DELAY`.
  This gives in-flight downloads time to finish.
- **Self-heal.**
  - Index watchdog: rebuilds the Redis index from PG when it disappears.
  - Reconciler: repairs PG ↔ Redis drift and the bytes counter, and finds orphan objects in the
    bucket.

### State

| Store | Holds |
|---|---|
| Local disk (`LOCAL_PATH`) | All objects; the source of truth |
| PostgreSQL (`images` table) | Durable tier state per key: `status`, `size`, `mime_type`, `hits`, `last_access_at`, `migrated_at` |
| Redis (`REDIS_PREFIX` + …) | `s3:objects` (index), `s3:lru`, `s3:bytes`, `s3:epoch` (index validity marker), `s3:trash` (pending deletes), `heat`, `events` (stream), `leader` |
| S3 bucket (`S3_PREFIX/…`) | Copies of hot objects, publicly readable |

The public URL is never stored: it is derived from `key`, `S3_PREFIX` and `S3_PUBLIC_URL`.

## Quick start

**Prerequisites:** Go 1.27+, Docker (for the local dependency stack).

```sh
git clone https://github.com/NoyTeam/imagelib-s3-dynamic.git
cd imagelib-s3-dynamic

# 1. Start PostgreSQL, Redis and an S3-compatible store (pgsty/silo, a MinIO fork)
#    with a public-read bucket named "images".
docker compose -f docker-compose.dev.yml up -d --wait

# 2. Configure. The values below match docker-compose.dev.yml.
cp .env.example .env
cat >> .env <<'EOF'
DATABASE_URL=postgres://postgres:postgres@localhost:55432/imagelib?sslmode=disable
REDIS_ADDR=localhost:56379
LOCAL_PATH=./data
S3_ENDPOINT=http://localhost:59000
S3_REGION=us-east-1
S3_BUCKET=images
S3_PREFIX=cache
S3_PUBLIC_URL=http://localhost:59000/images
S3_FORCE_PATH_STYLE=true
B2_APPLICATION_KEY_ID=siloadmin
B2_APPLICATION_KEY=siloadmin-secret
MIGRATE_THRESHOLD=3
EOF

# 3. Put some images in place and run. The schema is created on first start.
mkdir -p data/photos && cp /path/to/some.jpg data/photos/cat.jpg
go run ./cmd
```

```sh
tier() { curl -s -o /dev/null -D - localhost:8080/photos/cat.jpg | grep -i x-storage-tier; }
tier; tier; tier        # X-Storage-Tier: local  (3 origin reads reach MIGRATE_THRESHOLD=3)
sleep 3                 # event flush (1s) + upload + index cache TTL (1s)
tier                    # X-Storage-Tier: s3
curl -s localhost:8080/healthz
```

Only `GET` requests count as origin reads; `HEAD` requests do not.

`.env` is loaded from the working directory and never overrides variables that are already
set in the environment. The silo console is available at <http://localhost:59001>.

## Configuration

All configuration comes from environment variables, optionally loaded from a `.env` file.
An invalid value aborts startup with a message that names the variable.

- **Durations** use Go syntax: `300ms`, `30s`, `1h30m`.
- **Sizes** accept `KiB/MiB/GiB/TiB` and `K/M/G/T` (powers of 1024), or `KB/MB/GB/TB`
  (powers of 1000).

### Process

| Variable | Default | Description |
|---|---|---|
| `ADDR` | `:8080` | HTTP listen address. The scheduler role serves only `/healthz` here. |
| `ROLE` | `all` | `all` (proxy + scheduler), `proxy`, or `scheduler`. |
| `INSTANCE_ID` | `hostname-pid` | **Must be unique per process.** Used as the leader-lock value and as the stream consumer name. |

### PostgreSQL

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/imagelib?sslmode=disable` | Required by all roles. |
| `DB_AUTO_PUSH` | `true` | Apply the embedded schema on startup. |
| `DB_PUSH_ALLOW_DESTRUCTIVE` | `false` | Allow column drops and lossy type changes during auto-push. |

### Redis

| Variable | Default | Description |
|---|---|---|
| `REDIS_ADDR` / `REDIS_PASSWORD` / `REDIS_DB` | `localhost:6379` / – / `0` | Connection settings. |
| `REDIS_PREFIX` | `imagelib:` | Namespace for all keys. |
| `REDIS_TIMEOUT` | `1s` | Dial, read and write timeout of the client. |
| `REDIS_LOOKUP_TIMEOUT` | `100ms` | Per-request index lookup timeout. On timeout the request is served from local disk. |
| `REDIS_BREAKER_THRESHOLD` / `REDIS_BREAKER_COOLDOWN` | `5` / `5s` | Open the breaker after N consecutive lookup failures; probe again after the cooldown. `0` disables the breaker. |
| `INDEX_CACHE_TTL` | `1s` | In-process index cache TTL. `0` disables the cache. **Must be shorter than `EVICT_DELETE_DELAY`.** |
| `INDEX_CACHE_MAX_KEYS` | `100000` | Maximum number of cached keys. |

### Local origin

| Variable | Default | Description |
|---|---|---|
| `LOCAL_PATH` | `./data` | Root directory of all objects. Scheduler processes must be able to read the same files. |

### S3 / bucket

| Variable | Default | Description |
|---|---|---|
| `S3_ENDPOINT` | `https://s3.us-west-004.backblazeb2.com` | S3 API endpoint, used for uploads, deletes and listing. |
| `S3_REGION` | `us-west-004` | Signing region; must match the endpoint. |
| `S3_BUCKET` | – | **Required for the scheduler.** |
| `B2_APPLICATION_KEY_ID` / `B2_APPLICATION_KEY` | – | Access key pair. Any S3-compatible provider works. Only write and delete permissions are needed. |
| `S3_PREFIX` | – | Key prefix inside the bucket. **Do not change it once objects exist.** |
| `S3_PUBLIC_URL` | – | **Required for the proxy.** Public base URL of the bucket root, e.g. `https://f004.backblazeb2.com/file/<bucket>` or a CDN origin. |
| `S3_FORCE_PATH_STYLE` | `false` | Use path-style API addressing. Set to `true` for MinIO and silo. |
| `S3_UPSTREAM_TIMEOUT` | `10s` | Time to wait for response headers from the bucket before falling back to local. 2–3s is recommended in production. |
| `S3_BREAKER_THRESHOLD` / `S3_BREAKER_COOLDOWN` | `5` / `30s` | Open the S3 breaker after N consecutive 5xx responses, timeouts or connection errors. 404 and 403 do not count. |

### Migration

| Variable | Default | Description |
|---|---|---|
| `MIGRATE_THRESHOLD` | `3` | Heat score that triggers a migration (≈ origin reads within one half-life). |
| `MIGRATE_HALF_LIFE` | `1h` | Decay half-life of the heat score. |
| `MIGRATE_WORKERS` | `4` | Concurrent uploads. |
| `MIGRATE_MIN_SIZE` / `MIGRATE_MAX_SIZE` | `0` / `512MiB` | Objects outside this range always stay local. Empty files never migrate. |
| `MIGRATE_RETRY_BACKOFF` | `1m` | How long a migration rejected for lack of evictable space waits before it is retried. |

### Capacity and eviction

| Variable | Default | Description |
|---|---|---|
| `S3_CAPACITY` | `10GiB` | Hard upper bound on the total bytes of indexed objects. |
| `EVICT_MIN_IDLE` | `10m` | Admitting a new object may only evict objects idle at least this long. `0` means plain LRU. |
| `EVICT_INTERVAL` | `1m` | Interval of the periodic capacity enforcement, which also applies a lowered capacity. |
| `EVICT_DELETE_DELAY` | `30s` | Grace period between de-indexing an object and deleting it from the bucket. |

### Events, scheduler and self-healing

| Variable | Default | Description |
|---|---|---|
| `EVENT_BUFFER` | `8192` | Per-process buffer of unpublished events. When it is full, events are dropped and the request is never blocked. |
| `EVENT_FLUSH_INTERVAL` | `1s` | Per-key aggregation window before events are written to the stream. |
| `EVENT_STREAM_MAXLEN` | `100000` | Approximate cap on the stream length. |
| `LRU_TOUCH_INTERVAL` | `1s` | Batch interval for LRU access-time updates of S3 hits. |
| `STATS_FLUSH_INTERVAL` | `10s` | Flush interval of `hits` and `last_access_at` to PostgreSQL. |
| `LEADER_TTL` | `15s` | Leader lock TTL. Failover takes about one TTL; the lock is renewed every TTL/3. |
| `INDEX_CHECK_INTERVAL` | `10s` | How often the leader checks the Redis index and rebuilds it if lost. |
| `RECONCILE_INTERVAL` | `1h` | PG ↔ Redis ↔ bucket reconciliation interval. Also runs right after taking leadership. `0` disables it. |
| `RECONCILE_ORPHANS` | `log` | What to do with bucket objects under the prefix that nothing references: `off`, `log` or `delete`. `delete` requires a non-empty `S3_PREFIX`. |
| `RECONCILE_ORPHAN_MIN_AGE` | `1h` | Never treat objects younger than this as orphans. |

## Deployment

```sh
go build -o imagelib ./cmd
./imagelib            # reads .env from the working directory, if present
```

| Topology | Setup |
|---|---|
| Single node | One process with `ROLE=all`. |
| Replicated | N processes with `ROLE=all` behind a load balancer. Only the elected leader runs the scheduler. |
| Split | Many `ROLE=proxy` processes plus at least two `ROLE=scheduler` processes for failover. |

**Checklist:**

- [ ] The bucket allows anonymous reads at `S3_PUBLIC_URL`. Otherwise every request falls back
      to local disk.
- [ ] Every process has a unique `INSTANCE_ID`.
- [ ] Scheduler processes can read the same files as the proxies (`LOCAL_PATH`).
- [ ] `EVICT_DELETE_DELAY` is identical on all processes and greater than `INDEX_CACHE_TTL`.
- [ ] `S3_PREFIX` is dedicated to this service if `RECONCILE_ORPHANS=delete`.
- [ ] Redis uses `maxmemory-policy noeviction`. Partial key eviction is repaired only by the
      reconciler.
- [ ] **Backblaze B2:** buckets keep all file versions by default, and S3 `DeleteObject` only
      hides a file. Add a lifecycle rule that keeps only the latest version
      (`daysFromHidingToDeleting: 1`). Otherwise storage use will exceed `S3_CAPACITY`.
- [ ] Caching policy is set at your CDN or edge. imagelib returns the same cache headers from
      both tiers.

## Operations

### Health

`GET /healthz` returns `200` when PostgreSQL and Redis are reachable, and `503` otherwise.
An open circuit breaker is reported but keeps `200`, because the service still serves
correctly from local disk.

```json
{
  "status": "ok", "postgres": "ok", "redis": "ok",
  "redis_breaker": "closed", "s3_breaker": "closed",
  "index_cache_enabled": "true", "index_cache_hits": "19886", "index_cache_misses": "118",
  "index_redis_lookups": "7", "index_lookups_shared": "116"
}
```

### Logs worth alerting on

| Message | Meaning |
|---|---|
| `circuit breaker opened` | Redis or S3 is unhealthy; traffic is being served from local disk. |
| `s3 index lost, rebuilding from postgres` | The Redis index disappeared and was rebuilt. |
| `reconcile fixed drift` | The reconciler found and repaired inconsistencies. |
| `lost scheduler leadership` | The leader lost its lock; another instance takes over. |
| `event buffer full, dropping origin events` | Event publishing is saturated. Heat is approximate, but check Redis. |

### Schema changes

The schema lives in `schema/schema.gcorm` and is embedded into the binary. Regenerate the
client with `gco generate`. Destructive changes are refused on startup with a list of the
offending changes. Review them, then start once with `DB_PUSH_ALLOW_DESTRUCTIVE=true` or run
`gco db push --force`.

## Development

```sh
docker compose -f docker-compose.dev.yml up -d --wait     # PG + Redis + silo

go test -race ./...                                       # unit, behaviour and stress tests
go test -tags integration -count=1 ./internal/app/        # end-to-end against PG/Redis/silo

# Micro-benchmarks (BENCH_REDIS_ADDR uses a real Redis instead of miniredis)
BENCH_REDIS_ADDR=localhost:56379 go test -run '^$' -bench . -benchmem ./internal/...

# Load scenarios: local-only, S3 hot set, cache on/off, capacity churn, Redis/S3 outages
scripts/bench.sh                     # all scenarios (~8 min), results in /tmp/imagelib-bench/results
C=16 scripts/bench.sh local_only     # selected scenarios with a different concurrency
```

The load generator can also be used on its own:

```sh
go run ./cmd/loadtest gen -dir /tmp/data -n 1000
go run ./cmd/loadtest run -url http://127.0.0.1:8080 -manifest /tmp/data/manifest.tsv \
    -c 64 -d 30s -dist zipf -redis localhost:56379 -timeline
```

It verifies the length of every response, verifies a sample by SHA-256, and reports TTFB and
total latency percentiles per tier, the change in Redis command counts, and an optional
per-second timeline.

## Performance

Measured on an Apple M1 Pro laptop with all components on the same machine.

| Result | Value |
|---|---|
| Requests in load tests / errors / content mismatches | 1,763,442 / 0 / 0 |
| Local tier, 64 connections | ~14k req/s, ~4.7 GB/s, TTFB p50 2.8 ms |
| Index cache at a fixed 8,000 req/s (on vs off) | TTFB p50 0.95 vs 2.62 ms · p99 4.7 vs 9.2 ms · Redis commands/request −78% |
| Cached index lookup | 83 ns, 0 allocations (uncached Redis round trip: 56 µs) |
| Proxy overhead per 32 KiB S3-tier request | ~19 µs (in-process) |
| Capacity under churn (48 MiB budget, 309 MiB working set) | Peak usage 98.9% of the budget; never exceeded |
| Redis paused for 10 s under load | Full fallback to local within ~1 s, 0 errors |

## Limitations

- **Stalled transfers.** If S3 hangs *after* response headers were sent, that transfer
  stalls until S3 recovers, because it can no longer fall back to local disk.
  `S3_UPSTREAM_TIMEOUT` only bounds the wait for headers.
- **Probe cost.** While the S3 breaker is half-open, one user request per cooldown is used as
  the probe and may wait the full upstream timeout.
- **Lazy detection of missing objects.** Objects that the index lists but that have
  disappeared from the bucket are detected only when they are requested (404 → stale event).
  The reconciler does not `HEAD` every object.
- **Local changes are not propagated.** Changing or deleting a local file does not invalidate
  its S3 copy until the copy is evicted.
- **Single-machine benchmarks.** Performance has not yet been measured on a real B2 or CDN
  path or with many instances under load.

## Project layout

```
cmd/
  main.go              entrypoint (config → app.Run)
  loadtest/            dataset generator and HTTP load tester
internal/
  app/                 wiring of all components; integration tests
  proxy/               request path: index lookup (cache + singleflight), S3 reverse proxy, local serving
  scheduler/           leader election, event consumption, heat, migration, LRU capacity control, reconcile
  state/               Redis index, LRU, trash, bytes counter (Lua-scripted atomic updates)
  events/              origin-read events: non-blocking, per-key aggregated publisher
  breaker/             consecutive-failure circuit breaker
  storage/             local root (os.Root) and S3 client
  config/              environment parsing and validation
  database/ cache/     PostgreSQL (incl. embedded schema push) and Redis clients
  server/              Echo server, /healthz
schema/                .gcorm schema (embedded)
gen/                   generated GCORM client (do not edit)
scripts/bench.sh       end-to-end load scenarios
docker-compose.dev.yml PostgreSQL, Redis, pgsty/silo for development and tests
```

## Contributing

Issues and pull requests are welcome. Before submitting:

1. `gofmt -l .` prints nothing and `go vet ./... && go vet -tags integration ./...` passes.
2. `go test -race ./...` and the integration tests pass.
3. Behaviour changes come with tests. Performance-sensitive changes include benchmark or
   `scripts/bench.sh` results.

## License

[MIT](LICENSE) © Arsfy
