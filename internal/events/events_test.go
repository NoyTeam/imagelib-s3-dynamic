package events

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPublisherAggregatesPerKey(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	p := NewPublisher(rdb, "ev", 0, 10000)

	now := time.Now()
	for i := range 1000 {
		p.Submit(Event{Key: "hot", Size: 10, At: now.Add(time.Duration(i) * time.Millisecond)})
	}
	p.Submit(Event{Key: "cold", Size: 3, At: now})
	p.Close(context.Background())

	msgs, err := rdb.XRange(context.Background(), "ev", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) > 4 {
		t.Fatalf("1001 reads became %d stream entries; expected per-key aggregation", len(msgs))
	}
	reads := map[string]int64{}
	var latest time.Time
	for _, m := range msgs {
		e, ok := Decode(m)
		if !ok {
			t.Fatal("decode")
		}
		reads[e.Key] += e.Reads()
		if e.Key == "hot" && e.At.After(latest) {
			latest = e.At
		}
	}
	if reads["hot"] != 1000 || reads["cold"] != 1 {
		t.Fatalf("reads = %v", reads)
	}
	if !latest.Equal(now.Add(999 * time.Millisecond).Truncate(time.Millisecond)) {
		t.Fatalf("latest access not kept: %v", latest)
	}
	if pub, dropped := p.Stats(); pub != 1001 || dropped != 0 {
		t.Fatalf("stats published=%d dropped=%d", pub, dropped)
	}
}

func TestDecodeDefaultsCountToOne(t *testing.T) {
	e, ok := Decode(redis.XMessage{Values: map[string]any{"k": "a", "s": "5", "t": "1"}})
	if !ok || e.Reads() != 1 {
		t.Fatalf("legacy entry: %+v", e)
	}
}
