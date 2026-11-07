package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/events"
	"imagelib-s3-dynamic/internal/state"
	"imagelib-s3-dynamic/internal/storage"
)

type fixture struct {
	h       *Handler
	mr      *miniredis.Miniredis
	rdb     *redis.Client
	index   *state.Index
	upHits  chan string  // recent upstream paths (non-blocking, may drop)
	upTotal atomic.Int64 // all upstream requests
	upSeen  int64        // upTotal at the last upstreamCalls()
	dir     string
	s3Paths map[string]string // path → body served by the fake bucket

	mu       sync.Mutex
	s3Status int           // if set, the fake bucket answers with this status
	s3Delay  time.Duration // response delay of the fake bucket
	s3Cut    bool          // if set, the fake bucket cuts the body after half of it
	lastUp   *http.Request // last request seen by the fake bucket
}

func (f *fixture) setS3(path, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if body == "" {
		delete(f.s3Paths, path)
		return
	}
	f.s3Paths[path] = body
}

func (f *fixture) lastUpstream() *http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastUp
}

// do serves an arbitrary request through the handler.
func (f *fixture) do(req *http.Request) *http.Response {
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	return rec.Result()
}

func testProxyConfig() config.ProxyConfig {
	return config.ProxyConfig{
		TouchInterval: 10 * time.Millisecond, UpstreamTimeout: time.Second,
		LookupTimeout: 200 * time.Millisecond,
		IndexCacheTTL: time.Second, IndexCacheMaxKeys: 1000,
		RedisBreakerThreshold: 3, RedisBreakerCooldown: 100 * time.Millisecond,
		S3BreakerThreshold: 3, S3BreakerCooldown: 100 * time.Millisecond,
	}
}

func newFixture(t *testing.T, opts ...func(*config.ProxyConfig)) *fixture {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	f := &fixture{mr: mr, rdb: rdb, upHits: make(chan string, 256), s3Paths: map[string]string{}, dir: t.TempDir()}
	bucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.upTotal.Add(1)
		select {
		case f.upHits <- r.URL.Path:
		default:
		}
		if r.Header.Get("Cookie") != "" {
			t.Errorf("cookie leaked to upstream")
		}
		f.mu.Lock()
		status, delay, cut := f.s3Status, f.s3Delay, f.s3Cut
		f.lastUp = r.Clone(context.Background())
		body, ok := f.s3Paths[r.URL.Path]
		f.mu.Unlock()
		time.Sleep(delay)
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Set-Cookie", "x=y")
		w.Header().Set("Cache-Control", "public, max-age=31536000")
		w.Header().Set("Expires", "Thu, 01 Jan 2099 00:00:00 GMT")
		if cut { // promise the full length, then drop the connection half-way
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			_, _ = io.WriteString(w, body[:len(body)/2])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(bucket.Close)

	local, err := storage.NewLocal(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { local.Close() })

	f.index = state.NewIndex(rdb, state.NewKeys("t:"))
	if err := f.index.Rebuild(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	pub := events.NewPublisher(rdb, f.index.Keys().Events, 1000, 64, 50*time.Millisecond)
	namer := storage.NewObjectNamer(config.StorageConfig{Prefix: "img", PublicURL: bucket.URL + "/file/bkt"})
	cfg := testProxyConfig()
	for _, o := range opts {
		o(&cfg)
	}
	h, err := New(cfg, local, f.index, namer, pub)
	if err != nil {
		t.Fatal(err)
	}
	f.h = h
	t.Cleanup(func() { f.h.Close(context.Background()) })
	return f
}

func (f *fixture) writeLocal(t *testing.T, key, body string) {
	t.Helper()
	p := filepath.Join(f.dir, key)
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) get(t *testing.T, path string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Cookie", "session=secret")
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	return rec.Result()
}

func (f *fixture) waitEvents(t *testing.T, n int) []events.Event {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msgs, _ := f.rdb.XRange(context.Background(), f.index.Keys().Events, "-", "+").Result()
		if len(msgs) >= n {
			out := make([]events.Event, 0, len(msgs))
			for _, m := range msgs {
				e, _ := events.Decode(m)
				out = append(out, e)
			}
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d events", n)
	return nil
}

func body(t *testing.T, r *http.Response) string {
	t.Helper()
	b, _ := io.ReadAll(r.Body)
	return string(b)
}

func TestLocalMissServesAndEmitsEvent(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "a/cat.jpg", "meow")

	resp := f.get(t, "/a/cat.jpg")
	if resp.StatusCode != 200 || body(t, resp) != "meow" || resp.Header.Get(TierHeader) != "local" {
		t.Fatalf("unexpected response %d %v", resp.StatusCode, resp.Header)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/jpeg" {
		t.Fatalf("content-type = %q", ct)
	}
	evs := f.waitEvents(t, 1)
	if evs[0].Key != "a/cat.jpg" || evs[0].Size != 4 || evs[0].Stale {
		t.Fatalf("event = %+v", evs[0])
	}

	if resp := f.get(t, "/missing.jpg"); resp.StatusCode != 404 {
		t.Fatalf("missing: %d", resp.StatusCode)
	}
	if resp := f.get(t, "/.env"); resp.StatusCode != 404 {
		t.Fatalf("dotfile: %d", resp.StatusCode)
	}
}

func TestS3HitIsProxied(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "a/cat jpg.png", "local-copy")
	f.s3Paths["/file/bkt/img/a/cat jpg.png"] = "s3-copy"
	ctx := context.Background()
	_, _ = f.index.Add(ctx, "a/cat jpg.png", 10, time.Now().Add(-time.Hour))

	resp := f.get(t, "/a/cat%20jpg.png")
	if resp.StatusCode != 200 || body(t, resp) != "s3-copy" || resp.Header.Get(TierHeader) != "s3" {
		t.Fatalf("unexpected response %d %v", resp.StatusCode, resp.Header)
	}
	if resp.Header.Get("Set-Cookie") != "" {
		t.Fatal("upstream Set-Cookie must be stripped")
	}
	if resp.Header.Get("Cache-Control") != "" || resp.Header.Get("Expires") != "" {
		t.Fatal("upstream cache headers must be stripped")
	}
	if p := <-f.upHits; p != "/file/bkt/img/a/cat jpg.png" {
		t.Fatalf("upstream path = %q", p)
	}

	// LRU touch is flushed asynchronously.
	deadline := time.Now().Add(time.Second)
	for {
		score, _ := f.rdb.ZScore(ctx, f.index.Keys().LRU, "a/cat jpg.png").Result()
		if time.Since(time.UnixMilli(int64(score))) < time.Minute {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lru not touched")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestS3MissFallsBackToLocal(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "dog.gif", "woof")
	_, _ = f.index.Add(context.Background(), "dog.gif", 4, time.Now()) // indexed but absent in bucket

	resp := f.get(t, "/dog.gif")
	if resp.StatusCode != 200 || body(t, resp) != "woof" || resp.Header.Get(TierHeader) != "local" {
		t.Fatalf("unexpected response %d %v", resp.StatusCode, resp.Header)
	}
	evs := f.waitEvents(t, 1)
	if !evs[0].Stale || evs[0].Key != "dog.gif" {
		t.Fatalf("expected stale event, got %+v", evs[0])
	}
}

func TestRedisDownServesLocal(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "x.png", "px")
	f.rdb.Close() // simulate outage

	resp := f.get(t, "/x.png")
	if resp.StatusCode != 200 || body(t, resp) != "px" {
		t.Fatalf("unexpected response %d", resp.StatusCode)
	}
}

// upstreamCalls returns the number of upstream requests since the previous call.
func (f *fixture) upstreamCalls() int {
	total := f.upTotal.Load()
	n := total - f.upSeen
	f.upSeen = total
	return int(n)
}

func TestRedisBreakerSkipsRedisWhileOpen(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "p.png", "local")
	f.s3Paths["/file/bkt/img/p.png"] = "s3"
	_, _ = f.index.Add(context.Background(), "p.png", 5, time.Now())

	f.mr.SetError("LOADING simulated outage")
	for range 3 {
		if tier := f.get(t, "/p.png").Header.Get(TierHeader); tier != "local" {
			t.Fatalf("tier during outage = %s", tier)
		}
	}
	if f.h.Health()["redis_breaker"] != "open" {
		t.Fatalf("breaker = %v", f.h.Health())
	}
	before := f.mr.CommandCount()
	for range 10 {
		f.get(t, "/p.png")
	}
	// Only async publisher writes may reach Redis; no lookups while open.
	if lookups := f.mr.CommandCount() - before; lookups > 2 {
		t.Fatalf("open breaker still sent %d commands", lookups)
	}

	f.mr.SetError("")
	time.Sleep(120 * time.Millisecond) // cooldown → next request probes
	if tier := f.get(t, "/p.png").Header.Get(TierHeader); tier != "s3" {
		t.Fatalf("tier after recovery = %s", tier)
	}
	if f.h.Health()["redis_breaker"] != "closed" {
		t.Fatalf("breaker = %v", f.h.Health())
	}
}

func TestHungRedisFailsFast(t *testing.T) {
	// A server that accepts connections and never answers (e.g. Redis stuck / partitioned).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
		}
	}()
	hung := redis.NewClient(&redis.Options{Addr: ln.Addr().String(), ContextTimeoutEnabled: true, MaxRetries: 1})
	t.Cleanup(func() { hung.Close() })

	f := newFixture(t)
	f.writeLocal(t, "h.png", "local")
	f.h.lookup.index = state.NewIndex(hung, state.NewKeys("t:"))

	start := time.Now()
	resp := f.get(t, "/h.png")
	if resp.StatusCode != 200 || resp.Header.Get(TierHeader) != "local" {
		t.Fatalf("unexpected %d %s", resp.StatusCode, resp.Header.Get(TierHeader))
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("hung redis stalled the request for %s", took)
	}
}

func TestS3BreakerBypassesFailingBucket(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "q.png", "local")
	f.s3Paths["/file/bkt/img/q.png"] = "s3"
	_, _ = f.index.Add(context.Background(), "q.png", 5, time.Now())

	f.mu.Lock()
	f.s3Status = http.StatusBadGateway
	f.mu.Unlock()
	for range 3 {
		if body(t, f.get(t, "/q.png")) != "local" {
			t.Fatal("must fall back to local on 5xx")
		}
	}
	if f.upstreamCalls() != 3 || f.h.Health()["s3_breaker"] != "open" {
		t.Fatalf("breaker = %v", f.h.Health())
	}
	for range 10 {
		if tier := f.get(t, "/q.png").Header.Get(TierHeader); tier != "local" {
			t.Fatalf("tier while open = %s", tier)
		}
	}
	if n := f.upstreamCalls(); n != 0 {
		t.Fatalf("open breaker still called upstream %d times", n)
	}
	// Already-migrated objects served locally because S3 fails are not origin reads:
	// no heat events, and in particular no stale events.
	time.Sleep(150 * time.Millisecond)
	if n, _ := f.rdb.XLen(context.Background(), f.index.Keys().Events).Result(); n != 0 {
		t.Fatalf("S3 fallbacks must not emit events, got %d", n)
	}

	f.mu.Lock()
	f.s3Status = 0
	f.mu.Unlock()
	time.Sleep(120 * time.Millisecond)
	if tier := f.get(t, "/q.png").Header.Get(TierHeader); tier != "s3" {
		t.Fatalf("probe tier = %s", tier)
	}
	if f.h.Health()["s3_breaker"] != "closed" {
		t.Fatalf("breaker = %v", f.h.Health())
	}
}

func TestS3TimeoutCountsAsFailure(t *testing.T) {
	f := newFixture(t, func(c *config.ProxyConfig) { c.UpstreamTimeout = 50 * time.Millisecond; c.S3BreakerThreshold = 1 })
	f.writeLocal(t, "slow.png", "local")
	f.s3Paths["/file/bkt/img/slow.png"] = "s3"
	_, _ = f.index.Add(context.Background(), "slow.png", 5, time.Now())
	f.mu.Lock()
	f.s3Delay = 300 * time.Millisecond
	f.mu.Unlock()

	start := time.Now()
	if body(t, f.get(t, "/slow.png")) != "local" {
		t.Fatal("timeout must fall back to local")
	}
	if took := time.Since(start); took > 250*time.Millisecond {
		t.Fatalf("waited %s, upstream timeout not applied", took)
	}
	if f.h.Health()["s3_breaker"] != "open" {
		t.Fatalf("breaker = %v", f.h.Health())
	}
}

func TestS3NotFoundDoesNotTripBreaker(t *testing.T) {
	f := newFixture(t)
	for i := range 5 {
		key := fmt.Sprintf("nf%d.png", i)
		f.writeLocal(t, key, "x")
		_, _ = f.index.Add(context.Background(), key, 1, time.Now())
		f.get(t, "/"+key)
	}
	if f.h.Health()["s3_breaker"] != "closed" {
		t.Fatal("404s are per-object staleness, not an outage")
	}
}

func TestIndexCacheCoalescesAndCaches(t *testing.T) {
	f := newFixture(t, func(c *config.ProxyConfig) { c.IndexCacheTTL = 300 * time.Millisecond })
	f.writeLocal(t, "hot.png", "local")
	f.s3Paths["/file/bkt/img/hot.png"] = "s3"
	_, _ = f.index.Add(context.Background(), "hot.png", 5, time.Now())

	// A burst of concurrent requests for one key → (about) one Redis lookup.
	var wg sync.WaitGroup
	for range 200 {
		wg.Go(func() {
			if tier := f.get(t, "/hot.png").Header.Get(TierHeader); tier != "s3" {
				t.Errorf("tier = %s", tier)
			}
		})
	}
	wg.Wait()
	f.h.lookup.cache.Wait()
	burst := f.h.lookup.redisCalls.Load()
	if burst > 5 {
		t.Fatalf("200 concurrent requests made %d redis lookups", burst)
	}

	// Within TTL: served from the in-process cache.
	for range 100 {
		f.get(t, "/hot.png")
	}
	if n := f.h.lookup.redisCalls.Load(); n != burst {
		t.Fatalf("cached requests hit redis: %d → %d", burst, n)
	}

	// After TTL: one refresh.
	time.Sleep(350 * time.Millisecond)
	f.get(t, "/hot.png")
	if n := f.h.lookup.redisCalls.Load(); n != burst+1 {
		t.Fatalf("expected one refresh after ttl, got %d → %d", burst, n)
	}
}

func TestIndexCacheNegativeEntries(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "cold.png", "local")
	f.get(t, "/cold.png")
	f.h.lookup.cache.Wait()
	for range 50 {
		f.get(t, "/cold.png")
	}
	if n := f.h.lookup.redisCalls.Load(); n != 1 {
		t.Fatalf("not-in-s3 must be cached too, got %d lookups", n)
	}
}

func TestStaleResponseInvalidatesCache(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "gone.png", "local")
	_, _ = f.index.Add(context.Background(), "gone.png", 5, time.Now()) // indexed, missing in bucket

	if body(t, f.get(t, "/gone.png")) != "local" {
		t.Fatal("must fall back")
	}
	f.h.lookup.cache.Wait()
	f.upstreamCalls()
	for range 10 {
		f.get(t, "/gone.png")
	}
	if n := f.upstreamCalls(); n != 0 {
		t.Fatalf("stale key still sent to S3 %d times within ttl", n)
	}
}

func TestIndexCacheDisabled(t *testing.T) {
	f := newFixture(t, func(c *config.ProxyConfig) { c.IndexCacheTTL = 0 })
	f.writeLocal(t, "n.png", "x")
	for range 3 {
		f.get(t, "/n.png")
	}
	if f.h.lookup.cache != nil || f.h.lookup.redisCalls.Load() != 3 {
		t.Fatalf("cache disabled: calls = %d", f.h.lookup.redisCalls.Load())
	}
}
