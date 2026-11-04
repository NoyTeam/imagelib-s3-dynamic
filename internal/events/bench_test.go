package events

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// BenchmarkSubmit measures the request-path cost of reporting an origin read.
func BenchmarkSubmit(b *testing.B) {
	mr := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	p := NewPublisher(rdb, "ev", 1000, 1<<16, time.Second)
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	now := time.Now()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			p.Submit(Event{Key: keys[i%len(keys)], Size: 1, At: now})
			i++
		}
	})
	b.StopTimer()
	p.Close(context.Background())
	pub, dropped := p.Stats()
	n, _ := rdb.XLen(context.Background(), "ev").Result()
	b.ReportMetric(float64(n)/float64(pub+dropped), "entries/read")
	b.ReportMetric(float64(dropped)/float64(pub+dropped), "dropped/read")
}
