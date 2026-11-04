package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"imagelib-s3-dynamic/internal/config"
)

// --- HTTP semantics of the local tier ---

func TestHeadLocalHasHeadersNoBodyNoEvent(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "h.png", "12345")

	resp := f.do(httptest.NewRequest(http.MethodHead, "/h.png", nil))
	if resp.StatusCode != 200 || resp.Header.Get("Content-Length") != "5" || body(t, resp) != "" {
		t.Fatalf("HEAD: %d len=%s", resp.StatusCode, resp.Header.Get("Content-Length"))
	}
	time.Sleep(250 * time.Millisecond) // > publisher flush interval
	if n, _ := f.rdb.XLen(context.Background(), f.index.Keys().Events).Result(); n != 0 {
		t.Fatalf("HEAD must not count as a read, got %d events", n)
	}
}

func TestUnsupportedMethods(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "m.png", "x")
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		resp := f.do(httptest.NewRequest(m, "/m.png", nil))
		if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "GET, HEAD" {
			t.Errorf("%s: %d allow=%q", m, resp.StatusCode, resp.Header.Get("Allow"))
		}
	}
}

func TestConditionalRequestsLocal(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "c.png", "content")
	first := f.get(t, "/c.png")
	etag, lm := first.Header.Get("Etag"), first.Header.Get("Last-Modified")
	if etag == "" || lm == "" {
		t.Fatalf("validators missing: etag=%q last-modified=%q", etag, lm)
	}

	req := httptest.NewRequest(http.MethodGet, "/c.png", nil)
	req.Header.Set("If-None-Match", etag)
	if resp := f.do(req); resp.StatusCode != http.StatusNotModified {
		t.Fatalf("If-None-Match: %d", resp.StatusCode)
	}
	req = httptest.NewRequest(http.MethodGet, "/c.png", nil)
	req.Header.Set("If-Modified-Since", lm)
	if resp := f.do(req); resp.StatusCode != http.StatusNotModified {
		t.Fatalf("If-Modified-Since: %d", resp.StatusCode)
	}

	// ETag changes when the file changes.
	time.Sleep(10 * time.Millisecond)
	f.writeLocal(t, "c.png", "content-v2")
	if f.get(t, "/c.png").Header.Get("Etag") == etag {
		t.Fatal("etag must change with the file")
	}
}

func TestRangeLocal(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "r.bin", "0123456789")
	req := httptest.NewRequest(http.MethodGet, "/r.bin", nil)
	req.Header.Set("Range", "bytes=2-5")
	resp := f.do(req)
	if resp.StatusCode != http.StatusPartialContent || body(t, resp) != "2345" || resp.Header.Get("Content-Range") != "bytes 2-5/10" {
		t.Fatalf("range: %d %q %q", resp.StatusCode, body(t, resp), resp.Header.Get("Content-Range"))
	}
	req = httptest.NewRequest(http.MethodGet, "/r.bin", nil)
	req.Header.Set("Range", "bytes=50-60")
	if resp := f.do(req); resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("unsatisfiable range: %d", resp.StatusCode)
	}
}

func TestMissingObjectNoEvent(t *testing.T) {
	f := newFixture(t)
	if resp := f.get(t, "/nope.png"); resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	time.Sleep(250 * time.Millisecond)
	if n, _ := f.rdb.XLen(context.Background(), f.index.Keys().Events).Result(); n != 0 {
		t.Fatalf("404 must not produce origin events, got %d", n)
	}
}

func TestPathTraversalNeverEscapesRoot(t *testing.T) {
	f := newFixture(t)
	// A secret next to (outside) the storage root.
	secret := filepath.Join(filepath.Dir(f.dir), "secret.txt")
	if err := os.WriteFile(secret, []byte("TOP SECRET"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(secret) })
	f.writeLocal(t, "ok/b.png", "fine")

	for _, p := range []string{
		"/../secret.txt", "/%2e%2e/secret.txt", "/ok/../../secret.txt", "/ok/%2e%2e/%2e%2e/secret.txt",
		"/.git/config", "/ok/.hidden", "//../secret.txt",
	} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.URL.Path = mustUnescape(t, p)
		resp := f.do(req)
		if b := body(t, resp); resp.StatusCode == 200 || bytes.Contains([]byte(b), []byte("SECRET")) {
			t.Errorf("%s: %d %q", p, resp.StatusCode, b)
		}
	}
	// Dot segments that stay inside the root resolve normally.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.URL.Path = "/ok/x/../b.png"
	if resp := f.do(req); resp.StatusCode != 200 || body(t, resp) != "fine" {
		t.Fatalf("in-root dot segments: %d", resp.StatusCode)
	}
}

func mustUnescape(t *testing.T, p string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://x"+p, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req.URL.Path
}

// --- S3 tier ---

func TestS3ProxyRequestHygiene(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "a b/ü.png", "local")
	f.setS3("/file/bkt/img/a b/ü.png", "s3")
	_, _ = f.index.Add(context.Background(), "a b/ü.png", 5, time.Now())

	req := httptest.NewRequest(http.MethodGet, "/a%20b/%C3%BC.png?w=100&token=abc", nil)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Cookie", "sid=1")
	req.Header.Set("Range", "bytes=0-0")
	resp := f.do(req)
	if resp.Header.Get(TierHeader) != "s3" {
		t.Fatalf("tier %s", resp.Header.Get(TierHeader))
	}
	up := f.lastUpstream()
	switch {
	case up.Header.Get("Authorization") != "":
		t.Error("Authorization leaked upstream")
	case up.Header.Get("Cookie") != "":
		t.Error("Cookie leaked upstream")
	case up.URL.RawQuery != "":
		t.Errorf("query string forwarded: %q", up.URL.RawQuery)
	case up.Header.Get("Range") != "bytes=0-0":
		t.Error("Range must be forwarded")
	case up.URL.EscapedPath() != "/file/bkt/img/a%20b/%C3%BC.png":
		t.Errorf("escaped upstream path %q", up.URL.EscapedPath())
	}
}

func TestHeadS3IsProxiedWithoutEvent(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "hs.png", "local")
	f.setS3("/file/bkt/img/hs.png", "s3")
	_, _ = f.index.Add(context.Background(), "hs.png", 5, time.Now())

	resp := f.do(httptest.NewRequest(http.MethodHead, "/hs.png", nil))
	if resp.StatusCode != 200 || resp.Header.Get(TierHeader) != "s3" || f.lastUpstream().Method != http.MethodHead {
		t.Fatalf("HEAD s3: %d tier=%s", resp.StatusCode, resp.Header.Get(TierHeader))
	}
	time.Sleep(250 * time.Millisecond)
	if n, _ := f.rdb.XLen(context.Background(), f.index.Keys().Events).Result(); n != 0 {
		t.Fatalf("S3 hits must not produce origin events, got %d", n)
	}
}

func TestS3ForbiddenFallsBackAndMarksStale(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "p.png", "local")
	_, _ = f.index.Add(context.Background(), "p.png", 5, time.Now())
	f.mu.Lock()
	f.s3Status = http.StatusForbidden // e.g. bucket made private
	f.mu.Unlock()

	resp := f.get(t, "/p.png")
	if resp.StatusCode != 200 || body(t, resp) != "local" {
		t.Fatalf("fallback: %d", resp.StatusCode)
	}
	evs := f.waitEvents(t, 1)
	if !evs[0].Stale {
		t.Fatal("403 should be reported as stale index entry")
	}
	if f.h.Health()["s3_breaker"] != "closed" {
		t.Fatal("403 must not trip the S3 breaker")
	}
}

// --- concurrency stress ---

// Every response must be 200 with the exact object bytes while the index flips between
// tiers, S3 copies disappear and re-appear, and caches go stale underneath.
func TestStressTierFlipsNeverCorruptResponses(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	f := newFixture(t, func(c *config.ProxyConfig) { c.IndexCacheTTL = 20 * time.Millisecond })
	const keys = 20
	content := make(map[string]string, keys)
	for i := range keys {
		k := fmt.Sprintf("k/%02d.bin", i)
		b := make([]byte, 512+i*97)
		_, _ = rand.Read(b)
		content[k] = string(b)
		f.writeLocal(t, k, content[k])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var chaos sync.WaitGroup
	chaos.Go(func() { // randomly migrate / evict / lose S3 copies
		for ctx.Err() == nil {
			k := fmt.Sprintf("k/%02d.bin", rnd(keys))
			switch rnd(3) {
			case 0:
				f.setS3("/file/bkt/img/"+k, content[k])
				_, _ = f.index.Add(ctx, k, int64(len(content[k])), time.Now())
			case 1:
				_, _ = f.index.Remove(ctx, k)
			case 2:
				f.setS3("/file/bkt/img/"+k, "") // object vanished from the bucket
			}
			time.Sleep(time.Millisecond)
		}
	})

	var (
		mu    sync.Mutex
		tiers = map[string]int{}
		reqs  int
	)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for ctx.Err() == nil {
				k := fmt.Sprintf("k/%02d.bin", rnd(keys))
				resp := f.get(t, "/"+k)
				b, _ := io.ReadAll(resp.Body)
				if resp.StatusCode != 200 || string(b) != content[k] {
					t.Errorf("%s: status %d, body ok=%v", k, resp.StatusCode, string(b) == content[k])
					return
				}
				mu.Lock()
				tiers[resp.Header.Get(TierHeader)]++
				reqs++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	chaos.Wait()
	if tiers["s3"] == 0 || tiers["local"] == 0 {
		t.Fatalf("both tiers must be exercised: %v", tiers)
	}
	t.Logf("%d requests, tiers %v, redis lookups %d", reqs, tiers, f.h.lookup.redisCalls.Load())
}

func rnd(n int) int {
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	return int(v.Int64())
}
