package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"imagelib-s3-dynamic/internal/metrics"
)

// scrape renders the registry the way vmagent sees it.
func scrape(t *testing.T, f *fixture) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Handler(metrics.NewRegistry(f.h.Collector())).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status %d", rec.Code)
	}
	return rec.Body.String()
}

func counter(t *testing.T, body, series string) string {
	t.Helper()
	for line := range strings.SplitSeq(body, "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			return v
		}
	}
	t.Fatalf("series %s not exported", series)
	return ""
}

func TestRequestMetrics(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "m/local.png", "0123456789")
	f.writeLocal(t, "m/s3.png", "local-copy")
	f.writeLocal(t, "m/stale.png", "stale")
	f.s3Paths["/file/bkt/img/m/s3.png"] = "s3-body"
	ctx := context.Background()
	_, _ = f.index.Add(ctx, "m/s3.png", 7, time.Now())
	_, _ = f.index.Add(ctx, "m/stale.png", 5, time.Now()) // indexed but missing in bucket

	req := func(tier, code string) float64 {
		return testutil.ToFloat64(metrics.Requests.WithLabelValues(tier, code))
	}
	tierBytes := func(tier string) prometheus.Counter { return metrics.RequestBytes.WithLabelValues(tier) }
	local200, local206, local404, s3200 := req("local", "200"), req("local", "206"), req("local", "404"), req("s3", "200")
	localBytes, s3Bytes := testutil.ToFloat64(tierBytes("local")), testutil.ToFloat64(tierBytes("s3"))
	origin := testutil.ToFloat64(metrics.LocalServes.WithLabelValues(metrics.LocalOrigin))
	staleServes := testutil.ToFloat64(metrics.LocalServes.WithLabelValues(metrics.LocalStaleFallback))
	missing := testutil.ToFloat64(metrics.S3UpstreamErrors.WithLabelValues(metrics.UpstreamObjectMissing))

	f.get(t, "/m/local.png")
	r := httptest.NewRequest(http.MethodGet, "/m/local.png", nil)
	r.Header.Set("Range", "bytes=0-3")
	if resp := f.do(r); resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status %d", resp.StatusCode)
	}
	f.get(t, "/m/nope.png")
	f.get(t, "/m/s3.png")
	f.get(t, "/m/stale.png")

	checks := []struct {
		name      string
		got, want float64
	}{
		{"local 200", req("local", "200") - local200, 2}, // local.png + stale fallback
		{"local 206", req("local", "206") - local206, 1},
		{"local 404", req("local", "404") - local404, 1},
		{"s3 200", req("s3", "200") - s3200, 1},
		{"local bytes", testutil.ToFloat64(tierBytes("local")) - localBytes, 10 + 4 + 5 + float64(len("404 page not found\n"))},
		{"s3 bytes", testutil.ToFloat64(tierBytes("s3")) - s3Bytes, 7},
		{"origin serves", testutil.ToFloat64(metrics.LocalServes.WithLabelValues(metrics.LocalOrigin)) - origin, 3},
		{"stale serves", testutil.ToFloat64(metrics.LocalServes.WithLabelValues(metrics.LocalStaleFallback)) - staleServes, 1},
		{"object_missing", testutil.ToFloat64(metrics.S3UpstreamErrors.WithLabelValues(metrics.UpstreamObjectMissing)) - missing, 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}

	body := scrape(t, f)
	for _, name := range []string{
		"imagelib_index_cache_hits_total", "imagelib_index_cache_misses_total",
		"imagelib_index_redis_lookups_total", "imagelib_index_redis_errors_total",
		"imagelib_index_lookups_coalesced_total", "imagelib_index_breaker_blocked_total",
		"imagelib_events_published_total", "imagelib_events_dropped_total", "imagelib_event_buffer_queued",
		"imagelib_request_duration_seconds_bucket", "go_goroutines", "process_cpu_seconds_total",
	} {
		if !strings.Contains(body, "\n"+name) {
			t.Errorf("%s not exported", name)
		}
	}
	if v := counter(t, body, "imagelib_index_redis_lookups_total"); v == "0" {
		t.Errorf("redis lookups = %s", v)
	}
}

// A stream cut mid-body makes ReverseProxy panic with http.ErrAbortHandler (only under a real
// http.Server); the partial response must still be counted.
func TestAbortedS3StreamIsCounted(t *testing.T) {
	f := newFixture(t)
	body := strings.Repeat("z", 64<<10)
	f.writeLocal(t, "cut.png", body)
	f.s3Paths["/file/bkt/img/cut.png"] = body
	_, _ = f.index.Add(context.Background(), "cut.png", int64(len(body)), time.Now())
	f.mu.Lock()
	f.s3Cut = true
	f.mu.Unlock()

	srv := httptest.NewServer(f.h)
	defer srv.Close()
	reqs := metrics.Requests.WithLabelValues("s3", "200")
	bytes := metrics.RequestBytes.WithLabelValues("s3")
	reqs0, bytes0 := testutil.ToFloat64(reqs), testutil.ToFloat64(bytes)

	resp, err := http.Get(srv.URL + "/cut.png")
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err == nil || n >= int64(len(body)) {
		t.Fatalf("expected a truncated body, got %d bytes, err %v", n, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for testutil.ToFloat64(reqs)-reqs0 != 1 {
		if time.Now().After(deadline) {
			t.Fatal("aborted s3 response not counted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if d := testutil.ToFloat64(bytes) - bytes0; d != float64(n) {
		t.Fatalf("s3 bytes = %v, client received %d", d, n)
	}
}

func TestBreakerStateMetric(t *testing.T) {
	f := newFixture(t)
	f.writeLocal(t, "b.png", "b")
	_, _ = f.index.Add(context.Background(), "b.png", 1, time.Now())
	f.mu.Lock()
	f.s3Status = http.StatusBadGateway
	f.mu.Unlock()

	g := metrics.BreakerState.WithLabelValues("s3-upstream")
	if v := testutil.ToFloat64(g); v != 0 {
		t.Fatalf("initial state %v", v)
	}
	before := testutil.ToFloat64(metrics.S3UpstreamErrors.WithLabelValues(metrics.UpstreamServerError))
	for range 3 { // threshold
		f.get(t, "/b.png")
	}
	if v := testutil.ToFloat64(g); v != 1 {
		t.Fatalf("state after failures %v, want 1 (open)", v)
	}
	if d := testutil.ToFloat64(metrics.S3UpstreamErrors.WithLabelValues(metrics.UpstreamServerError)) - before; d != 3 {
		t.Fatalf("server_error delta %v", d)
	}
}
