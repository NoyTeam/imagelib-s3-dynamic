//go:build integration

// End-to-end tests against real PostgreSQL, Redis and pgsty/silo.
//
//	docker compose -f docker-compose.dev.yml up -d --wait
//	go test -tags integration -count=1 -v ./internal/app/
//
// Endpoints can be overridden with IT_DATABASE_URL, IT_REDIS_ADDR, IT_S3_ENDPOINT,
// IT_S3_PUBLIC_URL, IT_S3_BUCKET, IT_S3_ACCESS_KEY, IT_S3_SECRET_KEY.
package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/proxy"
	"imagelib-s3-dynamic/internal/state"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type harness struct {
	t        *testing.T
	base     config.Config
	db       *sql.DB
	rdb      *redis.Client
	keys     state.Keys
	files    map[string][]byte
	s3Public string
}

type instance struct {
	id     string
	url    string
	cancel context.CancelFunc
	done   chan struct{} // closed when Run returns
	err    error
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	adminDSN := env("IT_DATABASE_URL", "postgres://postgres:postgres@localhost:55432/imagelib?sslmode=disable")
	run := fmt.Sprintf("it%d", time.Now().UnixNano())

	// Fresh database per run: also exercises auto push on an empty database.
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Ping(); err != nil {
		t.Skipf("postgres not reachable (start docker-compose.dev.yml): %v", err)
	}
	if _, err := admin.Exec(`CREATE DATABASE ` + run); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP DATABASE IF EXISTS ` + run + ` WITH (FORCE)`)
		admin.Close()
	})
	u, _ := url.Parse(adminDSN)
	u.Path = "/" + run
	dsn := u.String()

	rdb := redis.NewClient(&redis.Options{Addr: env("IT_REDIS_ADDR", "localhost:56379")})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })

	dir := t.TempDir()
	files := map[string][]byte{}
	for name, size := range map[string]int{"photos/a.jpg": 6000, "photos/b.jpg": 6000, "photos/c.png": 3000} {
		b := make([]byte, size)
		_, _ = rand.Read(b)
		p := filepath.Join(dir, name)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		files[name] = b
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	prefix := run + ":"
	publicURL := env("IT_S3_PUBLIC_URL", "http://localhost:59000/images")
	h := &harness{
		t: t, db: db, rdb: rdb, files: files,
		keys:     state.NewKeys(prefix),
		s3Public: publicURL + "/" + run,
		base: config.Config{
			Addr:      "127.0.0.1:0",
			Role:      config.RoleAll,
			Database:  dsn,
			DBPush:    config.DBPushConfig{Enabled: true},
			LocalPath: dir,
			Redis:     config.RedisConfig{Addr: env("IT_REDIS_ADDR", "localhost:56379"), Prefix: prefix},
			Storage: config.StorageConfig{
				Endpoint:       env("IT_S3_ENDPOINT", "http://localhost:59000"),
				Region:         "us-east-1",
				Bucket:         env("IT_S3_BUCKET", "images"),
				AccessKey:      env("IT_S3_ACCESS_KEY", "siloadmin"),
				SecretKey:      env("IT_S3_SECRET_KEY", "siloadmin-secret"),
				Prefix:         run, // isolate objects of this run
				PublicURL:      publicURL,
				ForcePathStyle: true,
				CacheControl:   "public, max-age=60",
			},
			Proxy: config.ProxyConfig{
				EventBuffer: 1024, StreamMaxLen: 10000,
				TouchInterval: 100 * time.Millisecond, UpstreamTimeout: 5 * time.Second,
			},
			Scheduler: config.SchedulerConfig{
				CapacityBytes:      10000, // fits one 6000-byte object + the 3000-byte one
				Threshold:          2,
				HalfLife:           time.Hour,
				Workers:            2,
				MaxSize:            1 << 20,
				EvictInterval:      200 * time.Millisecond,
				EvictMinIdle:       time.Second,
				RetryBackoff:       200 * time.Millisecond,
				DeleteDelay:        500 * time.Millisecond,
				StatsFlushInterval: 200 * time.Millisecond,
				LeaderTTL:          time.Second,
			},
		},
	}
	t.Cleanup(func() {
		ctx := context.Background()
		var cursor uint64
		for {
			ks, next, _ := rdb.Scan(ctx, cursor, prefix+"*", 100).Result()
			if len(ks) > 0 {
				rdb.Del(ctx, ks...)
			}
			if cursor = next; cursor == 0 {
				break
			}
		}
	})
	return h
}

func (h *harness) start(id string) *instance {
	h.t.Helper()
	cfg := h.base
	cfg.InstanceID = id
	ctx, cancel := context.WithCancel(context.Background())
	addrCh := make(chan net.Addr, 1)
	inst := &instance{id: id, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(inst.done)
		inst.err = Run(ctx, cfg, WithListenerAddr(func(a net.Addr) { addrCh <- a }))
	}()
	select {
	case a := <-addrCh:
		inst.url = "http://" + a.String()
	case <-inst.done:
		h.t.Fatalf("instance %s failed to start: %v", id, inst.err)
	case <-time.After(30 * time.Second):
		h.t.Fatalf("instance %s did not start", id)
	}
	h.t.Cleanup(inst.stop)
	return inst
}

func (i *instance) stop() {
	i.cancel()
	select {
	case <-i.done:
	case <-time.After(15 * time.Second):
		panic("instance " + i.id + " did not stop")
	}
}

// get fetches key through an instance and checks the body against the local file.
func (h *harness) get(inst *instance, key string) (tier string) {
	h.t.Helper()
	resp, err := http.Get(inst.url + "/" + key)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		h.t.Fatalf("GET %s via %s: status %d", key, inst.id, resp.StatusCode)
	}
	if !bytes.Equal(body, h.files[key]) {
		h.t.Fatalf("GET %s via %s (%s): body mismatch", key, inst.id, resp.Header.Get(proxy.TierHeader))
	}
	return resp.Header.Get(proxy.TierHeader)
}

func (h *harness) bucketStatus(key string) int {
	h.t.Helper()
	resp, err := http.Get(h.s3Public + "/" + key)
	if err != nil {
		h.t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func (h *harness) leader() string {
	v, _ := h.rdb.Get(context.Background(), h.keys.Leader).Result()
	return v
}

func (h *harness) dbStatus(key string) string {
	var st string
	_ = h.db.QueryRow(`SELECT status FROM images WHERE key = $1`, key).Scan(&st)
	return st
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestEndToEnd(t *testing.T) {
	h := newHarness(t)
	one := h.start("one")
	two := h.start("two")

	t.Run("auto push created schema", func(t *testing.T) {
		var cols []string
		rows, err := h.db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'images' ORDER BY ordinal_position`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var c string
			_ = rows.Scan(&c)
			cols = append(cols, c)
		}
		got := strings.Join(cols, ",")
		if !strings.Contains(got, "status") || strings.Contains(got, "url") {
			t.Fatalf("unexpected images columns: %s", got)
		}
		var pushes int
		_ = h.db.QueryRow(`SELECT count(*) FROM __gco_schema_pushes`).Scan(&pushes)
		if pushes != 2 { // first instance applies, second records a no-op
			t.Fatalf("schema pushes = %d, want 2", pushes)
		}
	})

	t.Run("hot object migrates to silo", func(t *testing.T) {
		if tier := h.get(one, "photos/a.jpg"); tier != "local" {
			t.Fatalf("first read tier = %s", tier)
		}
		h.get(two, "photos/a.jpg")
		eventually(t, "a in s3", 5*time.Second, func() bool { return h.dbStatus("photos/a.jpg") == "s3" })
		for _, inst := range []*instance{one, two} {
			if tier := h.get(inst, "photos/a.jpg"); tier != "s3" {
				t.Fatalf("tier via %s = %s, want s3", inst.id, tier)
			}
		}
		if st := h.bucketStatus("photos/a.jpg"); st != 200 {
			t.Fatalf("public bucket GET = %d", st)
		}
		// Range requests are proxied too.
		req, _ := http.NewRequest(http.MethodGet, one.url+"/photos/a.jpg", nil)
		req.Header.Set("Range", "bytes=10-19")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 206 || !bytes.Equal(b, h.files["photos/a.jpg"][10:20]) {
			t.Fatalf("range: %d %d bytes", resp.StatusCode, len(b))
		}
	})

	t.Run("recently used object is not evicted", func(t *testing.T) {
		h.get(one, "photos/a.jpg") // touch a
		h.get(one, "photos/b.jpg")
		h.get(two, "photos/b.jpg")
		time.Sleep(500 * time.Millisecond)
		if h.dbStatus("photos/b.jpg") == "s3" || h.dbStatus("photos/a.jpg") != "s3" {
			t.Fatal("b must not displace recently used a")
		}
	})

	t.Run("idle object is evicted by LRU", func(t *testing.T) {
		time.Sleep(1200 * time.Millisecond) // a becomes idle (EvictMinIdle = 1s)
		h.get(one, "photos/b.jpg")
		eventually(t, "b in s3", 5*time.Second, func() bool { return h.dbStatus("photos/b.jpg") == "s3" })
		if h.dbStatus("photos/a.jpg") != "local" {
			t.Fatal("a should have been evicted")
		}
		if tier := h.get(two, "photos/b.jpg"); tier != "s3" {
			t.Fatalf("b tier = %s", tier)
		}
		eventually(t, "a deleted from silo", 5*time.Second, func() bool { return h.bucketStatus("photos/a.jpg") == 404 })
		total, _ := h.rdb.Get(context.Background(), h.keys.Bytes).Int64()
		if total > h.base.Scheduler.CapacityBytes {
			t.Fatalf("capacity exceeded: %d", total)
		}
	})

	t.Run("leader failover", func(t *testing.T) {
		old := h.leader()
		insts := map[string]*instance{"one": one, "two": two}
		survivor := two
		if old == "two" {
			survivor = one
		}
		insts[old].stop()
		eventually(t, "new leader", 10*time.Second, func() bool { return h.leader() == survivor.id })

		h.get(survivor, "photos/c.png")
		h.get(survivor, "photos/c.png")
		eventually(t, "c in s3 via new leader", 5*time.Second, func() bool { return h.dbStatus("photos/c.png") == "s3" })
		if tier := h.get(survivor, "photos/c.png"); tier != "s3" {
			t.Fatalf("c tier = %s", tier)
		}
	})

	t.Run("index rebuilt from postgres after redis loss", func(t *testing.T) {
		one.stop()
		two.stop()
		ctx := context.Background()
		h.rdb.Del(ctx, h.keys.Objects, h.keys.LRU, h.keys.Bytes, h.keys.Leader)
		three := h.start("three")
		eventually(t, "index rebuilt", 10*time.Second, func() bool {
			ok, _ := h.rdb.HExists(ctx, h.keys.Objects, "photos/b.jpg").Result()
			return ok
		})
		if tier := h.get(three, "photos/b.jpg"); tier != "s3" {
			t.Fatalf("b tier after rebuild = %s", tier)
		}
		if total, _ := h.rdb.Get(ctx, h.keys.Bytes).Int64(); total != 9000 {
			t.Fatalf("rebuilt total = %d, want 9000", total)
		}
	})
}
