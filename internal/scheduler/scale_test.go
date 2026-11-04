//go:build scale

// Large-scale test of the scheduler's bulk operations against real PostgreSQL and Redis,
// sized like a production deployment (defaults: 30M images rows, 3.5M S3-resident objects
// ≈ 1 TiB at 300 KB, 10M heat entries). Run it with scripts/scale.sh, which starts isolated
// PostgreSQL 18 / Redis 7 containers.
//
// Every phase is measured for wall time, peak Go heap, the longest Redis command (SLOWLOG) and,
// most importantly, the latency of a concurrent "proxy" prober issuing HEXISTS with the
// production lookup timeout (100 ms) every 2 ms: a bulk operation must never make request-path
// lookups time out.
package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"imagelib-s3-dynamic/gen/client"
	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/database"
	"imagelib-s3-dynamic/internal/state"
)

func scaleEnvInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

func scaleEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// scaleKey must match the SQL expression used for seeding.
func scaleKey(i int) string { return fmt.Sprintf("img/%03d/%09d.jpg", i%1000, i) }

// isS3Row: 7 of every 60 rows are S3-resident (11.7% → 3.5M of 30M).
func isS3Row(i int) bool { return i%60 < 7 }

// --- measurement ---

type phaseResult struct {
	Phase         string  `json:"phase"`
	Objects       int     `json:"objects"`
	Seconds       float64 `json:"seconds"`
	HeapPeakMB    float64 `json:"heap_peak_mb"`
	ProbeCount    int     `json:"probe_count"`
	ProbeP50ms    float64 `json:"probe_p50_ms"`
	ProbeP99ms    float64 `json:"probe_p99_ms"`
	ProbeMaxms    float64 `json:"probe_max_ms"`
	ProbeTimeouts int64   `json:"probe_timeouts"`
	SlowMaxms     float64 `json:"redis_slowest_cmd_ms"`
	SlowCmd       string  `json:"redis_slowest_cmd"`
	Notes         string  `json:"notes,omitempty"`
}

type measurer struct {
	t      *testing.T
	admin  *redis.Client
	probe  *redis.Client
	key    string
	sample []string

	stop     chan struct{}
	wg       sync.WaitGroup
	lat      []time.Duration
	timeouts atomic.Int64
	heapPeak atomic.Uint64
}

func (m *measurer) begin() {
	ctx := context.Background()
	m.admin.ConfigSet(ctx, "slowlog-log-slower-than", "5000") // 5 ms
	m.admin.ConfigSet(ctx, "slowlog-max-len", "1024")
	m.admin.SlowLogReset(ctx)
	runtime.GC()
	m.heapPeak.Store(0)
	m.lat = m.lat[:0]
	m.timeouts.Store(0)
	m.stop = make(chan struct{})

	m.wg.Go(func() { // heap sampler
		var ms runtime.MemStats
		t := time.NewTicker(20 * time.Millisecond)
		defer t.Stop()
		for {
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > m.heapPeak.Load() {
				m.heapPeak.Store(ms.HeapAlloc)
			}
			select {
			case <-m.stop:
				return
			case <-t.C:
			}
		}
	})
	m.wg.Go(func() { // proxy-like lookup prober
		t := time.NewTicker(2 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-t.C:
			}
			k := m.sample[rand.IntN(len(m.sample))]
			c, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			start := time.Now()
			err := m.probe.HExists(c, m.key, k).Err()
			cancel()
			m.lat = append(m.lat, time.Since(start))
			if err != nil {
				m.timeouts.Add(1)
			}
		}
	})
}

func (m *measurer) end(phase string, objects int, start time.Time, notes string) phaseResult {
	elapsed := time.Since(start)
	close(m.stop)
	m.wg.Wait()
	r := phaseResult{
		Phase: phase, Objects: objects, Seconds: round(elapsed.Seconds(), 2),
		HeapPeakMB: round(float64(m.heapPeak.Load())/(1<<20), 1),
		ProbeCount: len(m.lat), ProbeTimeouts: m.timeouts.Load(), Notes: notes,
	}
	if len(m.lat) > 0 {
		slices.Sort(m.lat)
		q := func(p float64) float64 {
			return round(float64(m.lat[int(p*float64(len(m.lat)-1))].Microseconds())/1000, 2)
		}
		r.ProbeP50ms, r.ProbeP99ms, r.ProbeMaxms = q(0.5), q(0.99), q(1)
	}
	entries, _ := m.admin.SlowLogGet(context.Background(), 1024).Result()
	for _, e := range entries {
		ms := float64(e.Duration.Microseconds()) / 1000
		if len(e.Args) > 0 && strings.EqualFold(e.Args[0], "slowlog") {
			continue
		}
		if ms > r.SlowMaxms {
			r.SlowMaxms = round(ms, 2)
			r.SlowCmd = strings.ToUpper(e.Args[0])
		}
	}
	m.t.Logf("%-34s objects=%-9d %7.2fs heap=%7.1fMB probe p99=%6.2fms max=%7.2fms timeouts=%-4d slowest=%s %.1fms %s",
		r.Phase, r.Objects, r.Seconds, r.HeapPeakMB, r.ProbeP99ms, r.ProbeMaxms, r.ProbeTimeouts, r.SlowCmd, r.SlowMaxms, notes)
	return r
}

func round(v float64, d int) float64 {
	p := 1.0
	for range d {
		p *= 10
	}
	return float64(int64(v*p+0.5)) / p
}

// --- fake bucket ---

// scaleBucket lists every S3-resident key of the seeded dataset plus a number of orphans,
// without materialising millions of strings.
type scaleBucket struct {
	rows, orphans int
	deleted       atomic.Int64
}

func (b *scaleBucket) Put(context.Context, string, io.ReaderAt, int64, string) error { return nil }
func (b *scaleBucket) Delete(context.Context, string) error                          { b.deleted.Add(1); return nil }
func (b *scaleBucket) List(ctx context.Context, fn func(string, int64, time.Time) error) error {
	old := time.Now().Add(-48 * time.Hour)
	for i := 1; i <= b.rows; i++ {
		if !isS3Row(i) {
			continue
		}
		if err := fn(scaleKey(i), 300000, old); err != nil {
			return err
		}
	}
	for i := range b.orphans {
		if err := fn(fmt.Sprintf("orphan/%06d.jpg", i), 300000, old); err != nil {
			return err
		}
	}
	return nil
}

// --- the test ---

func TestScale(t *testing.T) {
	rows := scaleEnvInt("SCALE_PG_ROWS", 30_000_000)
	heatN := scaleEnvInt("SCALE_HEAT", 10_000_000)
	legacySubset := scaleEnvInt("SCALE_LEGACY_REBUILD", 1_000_000)
	const drift, orphans, evictN = 1000, 1000, 1000
	ctx := context.Background()

	// --- PostgreSQL: fresh database, schema via the production push path, bulk seed.
	adminDSN := scaleEnv("SCALE_DATABASE_URL", "postgres://postgres:postgres@localhost:55433/postgres?sslmode=disable")
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.Ping(); err != nil {
		t.Skipf("scale postgres not reachable (run scripts/scale.sh): %v", err)
	}
	dbName := fmt.Sprintf("scale_%d", time.Now().Unix())
	if _, err := admin.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)")
	u, _ := url.Parse(adminDSN)
	u.Path = "/" + dbName
	db, err := database.NewPostgres(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := database.PushSchema(ctx, db, u.String(), config.DBPushConfig{}); err != nil {
		t.Fatal(err)
	}

	seedStart := time.Now()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO images (key, size, status, hits, migrated_at, last_access_at, updated_at)
		SELECT 'img/' || lpad((i % 1000)::text, 3, '0') || '/' || lpad(i::text, 9, '0') || '.jpg',
		       200000 + (i::bigint * 7919) % 200001,
		       CASE WHEN i % 60 < 7 THEN 's3' ELSE 'local' END,
		       1 + i % 50,
		       CASE WHEN i % 60 < 7 THEN now() - (i % 86400) * interval '1 second' END,
		       now() - (i % 604800) * interval '1 second',
		       now() - (i % 604800) * interval '1 second'
		FROM generate_series(1, $1) AS i`, rows); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "ANALYZE images"); err != nil {
		t.Fatal(err)
	}
	var s3Rows int
	var s3Bytes int64
	if err := db.QueryRowContext(ctx, `SELECT count(*), sum(size) FROM images WHERE status='s3'`).Scan(&s3Rows, &s3Bytes); err != nil {
		t.Fatal(err)
	}
	var tableMB float64
	_ = db.QueryRowContext(ctx, `SELECT pg_total_relation_size('images')/1048576.0`).Scan(&tableMB)
	t.Logf("seeded %d rows (%d s3, %.2f TiB) in %s; images table %.0f MB",
		rows, s3Rows, float64(s3Bytes)/(1<<40), time.Since(seedStart).Round(time.Second), tableMB)

	// --- Redis: isolated instance, production client settings.
	redisAddr := scaleEnv("SCALE_REDIS_ADDR", "localhost:56380")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, ReadTimeout: time.Second, WriteTimeout: time.Second, MaxRetries: 1, ContextTimeoutEnabled: true})
	defer rdb.Close()
	adminRedis := redis.NewClient(&redis.Options{Addr: redisAddr, ReadTimeout: time.Minute})
	defer adminRedis.Close()
	probe := redis.NewClient(&redis.Options{Addr: redisAddr, ContextTimeoutEnabled: true, MaxRetries: -1})
	defer probe.Close()
	if err := adminRedis.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	keys := state.NewKeys("scale:")
	index := state.NewIndex(rdb, keys)

	bucket := &scaleBucket{rows: rows, orphans: orphans}
	cfg := config.SchedulerConfig{
		CapacityBytes: s3Bytes, Threshold: 3, HalfLife: time.Hour, Workers: 4, MaxSize: 512 << 20,
		EvictInterval: time.Hour, DeleteDelay: time.Hour, ReconcileInterval: time.Hour,
		ReconcileOrphans: "log", OrphanMinAge: time.Hour, IndexCheckInterval: time.Hour, LeaderTTL: 15 * time.Second,
	}
	s := New(cfg, "scale", rdb, index, nil, bucket, NewPGStore(client.New(db, client.WithDialect("postgresql"))))

	sample := make([]string, 0, 10000)
	for i := 1; len(sample) < cap(sample); i += 37 {
		sample = append(sample, scaleKey(i))
	}
	m := &measurer{t: t, admin: adminRedis, probe: probe, key: keys.Objects, sample: sample}
	var results []phaseResult
	usedMemory := func() int64 {
		info, _ := adminRedis.Info(ctx, "memory").Result()
		for _, l := range strings.Split(info, "\r\n") {
			if v, ok := strings.CutPrefix(l, "used_memory:"); ok {
				n, _ := strconv.ParseInt(v, 10, 64)
				return n
			}
		}
		return 0
	}

	// ===== legacy baseline: single-transaction rebuild (previous implementation), on a subset
	if legacySubset > 0 {
		m.begin()
		start := time.Now()
		var entries []state.Entry
		rs, err := db.QueryContext(ctx, `SELECT key, size, updated_at FROM images WHERE status='s3' ORDER BY key LIMIT $1`, legacySubset)
		if err != nil {
			t.Fatal(err)
		}
		for rs.Next() {
			var e state.Entry
			_ = rs.Scan(&e.Key, &e.Size, &e.LastAccess)
			entries = append(entries, e)
		}
		rs.Close()
		_, err = adminRedis.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Del(ctx, "legacy:objects", "legacy:lru")
			for _, e := range entries {
				p.HSet(ctx, "legacy:objects", e.Key, e.Size)
				p.ZAdd(ctx, "legacy:lru", redis.Z{Score: float64(e.LastAccess.UnixMilli()), Member: e.Key})
			}
			return nil
		})
		results = append(results, m.end("legacy: rebuild in one MULTI", len(entries), start, fmt.Sprintf("err=%v", err)))
		entries = nil
		adminRedis.Unlink(ctx, "legacy:objects", "legacy:lru")
		time.Sleep(2 * time.Second) // let the lazy free finish
	}

	// ===== 1. rebuild index from PG (streaming)
	memBefore := usedMemory()
	m.begin()
	start := time.Now()
	if err := s.rebuildIndex(ctx); err != nil {
		t.Fatal(err)
	}
	r := m.end("rebuild index from PG", s3Rows, start, "")
	results = append(results, r)
	memIndex := usedMemory() - memBefore
	total, _ := index.TotalBytes(ctx)
	count, _ := rdb.HLen(ctx, keys.Objects).Result()
	if int(count) != s3Rows || total != s3Bytes {
		t.Fatalf("rebuild: %d objects / %d bytes, want %d / %d", count, total, s3Rows, s3Bytes)
	}
	t.Logf("redis memory for the index: %.0f MB (%.0f B/object)", float64(memIndex)/(1<<20), float64(memIndex)/float64(s3Rows))

	// ===== legacy baseline: recount with one Lua script (previous implementation)
	m.begin()
	start = time.Now()
	legacy := redis.NewScript(`local t=0 for _,v in ipairs(redis.call('HVALS',KEYS[1])) do t=t+tonumber(v) end return t`)
	sum, err := legacy.Run(ctx, adminRedis, []string{keys.Objects}).Int64()
	results = append(results, m.end("legacy: recount via Lua HVALS", s3Rows, start, fmt.Sprintf("sum ok=%v err=%v", sum == s3Bytes, err)))

	// ===== 2. recount (incremental HSCAN under the index lock)
	_ = rdb.Set(ctx, keys.Bytes, 12345, 0).Err() // inject counter drift
	m.begin()
	start = time.Now()
	before, after, err := s.recount(ctx)
	if err != nil || after != s3Bytes || before != 12345 {
		t.Fatalf("recount: %d → %d (%v), want → %d", before, after, err, s3Bytes)
	}
	results = append(results, m.end("recount bytes (HSCAN)", s3Rows, start, "drift fixed"))

	// ===== 3. reconcile PG ↔ Redis with injected drift (orphan scan off)
	for i := range drift { // lost index entries (partial Redis loss, no counter update)
		k := scaleKey(i*60*7 + 1)
		rdb.HDel(ctx, keys.Objects, k)
		rdb.ZRem(ctx, keys.LRU, k)
	}
	for i := range drift { // stale entries for rows PG says are local
		k := scaleKey(i*60*7 + 30)
		rdb.HSet(ctx, keys.Objects, k, 300000)
		rdb.ZAdd(ctx, keys.LRU, redis.Z{Score: 1, Member: k})
	}
	s.cfg.ReconcileOrphans = "off"
	m.begin()
	start = time.Now()
	var rep reconcileReport
	if err := s.reconcilePGToIndex(ctx, &rep); err != nil {
		t.Fatal(err)
	}
	if err := s.reconcileIndexToPG(ctx, &rep); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.recount(ctx); err != nil {
		t.Fatal(err)
	}
	results = append(results, m.end("reconcile PG↔index (+recount)", s3Rows, start, fmt.Sprintf("restored=%d dropped=%d", rep.restored, rep.dropped)))
	if rep.restored != drift || rep.dropped != drift {
		t.Fatalf("reconcile fixed %d/%d, want %d/%d", rep.restored, rep.dropped, drift, drift)
	}
	if total, _ := index.TotalBytes(ctx); total != s3Bytes {
		t.Fatalf("after reconcile total=%d want %d", total, s3Bytes)
	}
	// Dropped entries went to the trash; clear it so the orphan scan sees a clean bucket model.
	rdb.Unlink(ctx, keys.Trash)

	// ===== 4. orphan scan over the bucket listing
	s.cfg.ReconcileOrphans = "log"
	rep = reconcileReport{}
	m.begin()
	start = time.Now()
	if err := s.reconcileOrphans(ctx, &rep); err != nil {
		t.Fatal(err)
	}
	results = append(results, m.end("orphan scan (bucket listing)", s3Rows+orphans, start, fmt.Sprintf("orphans=%d", rep.orphans)))
	if rep.orphans != orphans {
		t.Fatalf("found %d orphans, want %d", rep.orphans, orphans)
	}

	// ===== 5. heat GC over a large heat hash (half the entries are stale)
	now := time.Now()
	for start := 0; start < heatN; start += 1000 {
		fields := make([]any, 0, 2000)
		for i := start; i < min(start+1000, heatN); i++ {
			at := now
			if i%2 == 0 {
				at = now.Add(-24 * time.Hour) // decayed to ~0
			}
			fields = append(fields, scaleKey(i), encodeHeat(2, at))
		}
		rdb.HSet(ctx, keys.Heat, fields...)
	}
	m.begin()
	start = time.Now()
	removed, err := s.gcHeat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	results = append(results, m.end("heat GC pass", heatN, start, fmt.Sprintf("removed=%d", removed)))
	if removed < heatN/2-heatN/100 || removed > heatN/2 {
		t.Fatalf("heat gc removed %d, want ~%d", removed, heatN/2)
	}

	// ===== 6. mass eviction at full index size (capacity lowered by ~evictN objects)
	s.cfg.CapacityBytes = s3Bytes - int64(evictN)*300000
	m.begin()
	start = time.Now()
	s.enforce(ctx)
	deleted, _ := rdb.ZCard(ctx, keys.Trash).Result()
	results = append(results, m.end("evict to lowered capacity", int(deleted), start, fmt.Sprintf("%.2f ms/eviction", float64(time.Since(start).Microseconds())/1000/float64(max(deleted, 1)))))
	if total, _ := index.TotalBytes(ctx); total > s.cfg.CapacityBytes {
		t.Fatalf("capacity not enforced: %d > %d", total, s.cfg.CapacityBytes)
	}

	// --- report
	for _, r := range results {
		if !strings.HasPrefix(r.Phase, "legacy") && r.ProbeTimeouts > 0 {
			t.Errorf("%s: %d proxy lookups timed out", r.Phase, r.ProbeTimeouts)
		}
	}
	if out := os.Getenv("SCALE_OUT"); out != "" {
		b, _ := json.MarshalIndent(map[string]any{
			"pg_rows": rows, "s3_objects": s3Rows, "s3_tib": round(float64(s3Bytes)/(1<<40), 3),
			"heat_entries": heatN, "seed_seconds": round(time.Since(seedStart).Seconds(), 0),
			"images_table_mb": round(tableMB, 0), "redis_index_mb": round(float64(memIndex)/(1<<20), 0),
			"redis_index_bytes_per_object": round(float64(memIndex)/float64(s3Rows), 0),
			"phases":                       results,
		}, "", "  ")
		_ = os.WriteFile(out, b, 0o644)
	}
}
