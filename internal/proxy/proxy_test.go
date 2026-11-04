package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	rdb     *redis.Client
	index   *state.Index
	upHits  chan string
	dir     string
	s3Paths map[string]string // path → body served by the fake bucket
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	f := &fixture{rdb: rdb, upHits: make(chan string, 16), s3Paths: map[string]string{}, dir: t.TempDir()}
	bucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.upHits <- r.URL.Path
		if r.Header.Get("Cookie") != "" {
			t.Errorf("cookie leaked to upstream")
		}
		body, ok := f.s3Paths[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Set-Cookie", "x=y")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(bucket.Close)

	local, err := storage.NewLocal(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { local.Close() })

	f.index = state.NewIndex(rdb, state.NewKeys("t:"))
	pub := events.NewPublisher(rdb, f.index.Keys().Events, 1000, 64)
	namer := storage.NewObjectNamer(config.StorageConfig{Prefix: "img", PublicURL: bucket.URL + "/file/bkt"})
	f.h = New(config.ProxyConfig{TouchInterval: 10 * time.Millisecond, UpstreamTimeout: time.Second}, local, f.index, namer, pub)
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
