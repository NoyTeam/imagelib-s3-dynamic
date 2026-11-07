package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func BenchmarkObserveRequest(b *testing.B) {
	for b.Loop() {
		ObserveRequest(TierLocal, 200, 32<<10, 3*time.Millisecond)
	}
}

// Worst case: every core updates the same series (one cache line bounces between cores).
func BenchmarkObserveRequestParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ObserveRequest(TierLocal, 200, 32<<10, 3*time.Millisecond)
		}
	})
}

func BenchmarkCounterIncParallel(b *testing.B) {
	c := LocalServes.WithLabelValues(LocalOrigin)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkHistogramObserve(b *testing.B) {
	h := RequestDuration.WithLabelValues(TierS3)
	for b.Loop() {
		h.Observe(0.003)
	}
}

func BenchmarkScrape(b *testing.B) {
	h := Handler(NewRegistry())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept-Encoding", "gzip") // vmagent default
	for b.Loop() {
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
}
