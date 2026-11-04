package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"imagelib-s3-dynamic/internal/breaker"
	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/events"
	"imagelib-s3-dynamic/internal/state"
	"imagelib-s3-dynamic/internal/storage"
)

// benchRedis uses BENCH_REDIS_ADDR (a real Redis) when set, otherwise miniredis.
func benchRedis(b *testing.B) (*redis.Client, string) {
	b.Helper()
	if addr := os.Getenv("BENCH_REDIS_ADDR"); addr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 256})
		b.Cleanup(func() { rdb.Close() })
		return rdb, "bench:" + fmt.Sprint(time.Now().UnixNano()) + ":"
	}
	mr := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	b.Cleanup(func() { rdb.Close() })
	return rdb, "bench:"
}

func cleanupPrefix(b *testing.B, rdb *redis.Client, prefix string) {
	b.Cleanup(func() {
		ctx := context.Background()
		keys, _ := rdb.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
	})
}

func benchLookup(b *testing.B, ttl time.Duration, keys int) {
	rdb, prefix := benchRedis(b)
	cleanupPrefix(b, rdb, prefix)
	idx := state.NewIndex(rdb, state.NewKeys(prefix))
	ctx := context.Background()
	_ = idx.Rebuild(ctx, nil)
	for i := range keys {
		if i%2 == 0 {
			_, _ = idx.Add(ctx, fmt.Sprintf("k%d", i), 1, time.Now())
		}
	}
	l, err := newIndexLookup(idx, breaker.New("b", 0, time.Second), time.Second, ttl, 100000)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(l.close)
	names := make([]string, keys)
	for i := range names {
		names[i] = fmt.Sprintf("k%d", i)
	}
	if l.cache != nil { // steady state: hot keys already cached (Ristretto admits after a few hits)
		for range 3 {
			for _, n := range names {
				l.inS3(ctx, n)
			}
			l.cache.Wait()
		}
	}
	l.redisCalls.Store(0)
	b.ResetTimer()
	var worker atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(worker.Add(1)) * 7919 // spread workers over the key space
		for pb.Next() {
			l.inS3(ctx, names[i%keys])
			i++
		}
	})
	b.ReportMetric(float64(l.redisCalls.Load())/float64(b.N), "redis/op")
}

func BenchmarkLookupCached(b *testing.B)         { benchLookup(b, time.Minute, 1000) }
func BenchmarkLookupUncached(b *testing.B)       { benchLookup(b, 0, 1000) }
func BenchmarkLookupUncachedHotKey(b *testing.B) { benchLookup(b, 0, 1) }
func BenchmarkLookupCachedManyKeys(b *testing.B) { benchLookup(b, time.Minute, 50000) }

// benchHandler serves a 32 KiB object either from local disk or via the S3 proxy path.
func benchHandler(b *testing.B, tier string) (*Handler, string) {
	b.Helper()
	dir := b.TempDir()
	payload := strings.Repeat("x", 32<<10)
	_ = os.WriteFile(filepath.Join(dir, "obj.jpg"), []byte(payload), 0o644)
	bucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, payload)
	}))
	b.Cleanup(bucket.Close)

	rdb, prefix := benchRedis(b)
	cleanupPrefix(b, rdb, prefix)
	idx := state.NewIndex(rdb, state.NewKeys(prefix))
	_ = idx.Rebuild(context.Background(), nil)
	if tier == "s3" {
		_, _ = idx.Add(context.Background(), "obj.jpg", int64(len(payload)), time.Now())
	}
	local, _ := storage.NewLocal(dir)
	b.Cleanup(func() { local.Close() })
	pub := events.NewPublisher(rdb, idx.Keys().Events, 10000, 1<<16, time.Second)
	cfg := testProxyConfig()
	cfg.IndexCacheTTL = time.Second
	cfg.UpstreamTimeout = 5 * time.Second
	h, err := New(cfg, local, idx, storage.NewObjectNamer(config.StorageConfig{PublicURL: bucket.URL}), pub)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { h.Close(context.Background()) })
	return h, payload
}

func benchServe(b *testing.B, tier string) {
	h, payload := benchHandler(b, tier)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/obj.jpg", nil))
			if rec.Code != 200 || rec.Body.Len() != len(payload) || rec.Header().Get(TierHeader) != tier {
				b.Fatalf("unexpected %d %d %s", rec.Code, rec.Body.Len(), rec.Header().Get(TierHeader))
			}
		}
	})
}

func BenchmarkServeLocal32K(b *testing.B)   { benchServe(b, "local") }
func BenchmarkServeS3Proxy32K(b *testing.B) { benchServe(b, "s3") }
