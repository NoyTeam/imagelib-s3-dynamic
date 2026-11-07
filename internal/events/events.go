package events

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"imagelib-s3-dynamic/internal/metrics"
)

// Event is emitted every time a request is served from local storage. The publisher merges
// events for the same key within a flush window, so one stream entry may stand for Count reads.
type Event struct {
	Key   string
	Size  int64
	At    time.Time
	Count int64 // number of reads represented; 0 is treated as 1
	// Stale is set when the index pointed at S3 but S3 did not have the object.
	Stale bool
}

func (e Event) Values() map[string]any {
	v := map[string]any{
		"k": e.Key,
		"s": e.Size,
		"t": e.At.UnixMilli(),
	}
	if e.Stale {
		v["x"] = "1"
	}
	if e.Count > 1 {
		v["c"] = e.Count
	}
	return v
}

// Reads returns how many local reads the event represents.
func (e Event) Reads() int64 {
	if e.Count < 1 {
		return 1
	}
	return e.Count
}

// Decode parses a stream message. ok is false for malformed messages.
func Decode(msg redis.XMessage) (e Event, ok bool) {
	key, _ := msg.Values["k"].(string)
	if key == "" {
		return Event{}, false
	}
	e.Key = key
	if s, _ := msg.Values["s"].(string); s != "" {
		e.Size, _ = strconv.ParseInt(s, 10, 64)
	}
	if t, _ := msg.Values["t"].(string); t != "" {
		ms, _ := strconv.ParseInt(t, 10, 64)
		e.At = time.UnixMilli(ms)
	} else {
		e.At = time.Now()
	}
	e.Stale = msg.Values["x"] == "1"
	e.Count = 1
	if c, _ := msg.Values["c"].(string); c != "" {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil && n > 0 {
			e.Count = n
		}
	}
	return e, true
}

// Publisher asynchronously appends events to the stream. Submit never blocks the
// request path: when the buffer is full events are dropped (heat is approximate anyway).
// Events for the same key are merged per flush window, so stream writes scale with the
// number of distinct keys read from local, not with request rate.
type Publisher struct {
	rdb    redis.UniversalClient
	stream string
	maxLen int64
	flush  time.Duration
	ch     chan Event
	done   chan struct{}
	once   sync.Once

	dropped   atomic.Int64
	published atomic.Int64
	lastWarn  atomic.Int64 // unix nanos of the last drop warning
}

const dropWarnEvery = 10 * time.Second

// warnDropped logs at most once per dropWarnEvery, however fast events are being dropped.
func (p *Publisher) warnDropped(msg string, args ...any) {
	now := time.Now().UnixNano()
	last := p.lastWarn.Load()
	if now-last < int64(dropWarnEvery) || !p.lastWarn.CompareAndSwap(last, now) {
		return
	}
	slog.Warn(msg, append(args, "dropped_total", p.dropped.Load())...)
}

// NewPublisher starts a publisher. flushInterval is the aggregation window: longer windows
// merge more reads of the same key into one stream entry (heat latency grows accordingly).
func NewPublisher(rdb redis.UniversalClient, stream string, maxLen int64, buffer int, flushInterval time.Duration) *Publisher {
	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond
	}
	p := &Publisher{
		rdb:    rdb,
		stream: stream,
		maxLen: maxLen,
		flush:  flushInterval,
		ch:     make(chan Event, buffer),
		done:   make(chan struct{}),
	}
	go p.loop()
	return p
}

// Submit enqueues an event without blocking.
func (p *Publisher) Submit(e Event) {
	select {
	case p.ch <- e:
	default:
		p.dropped.Add(1)
		p.warnDropped("event buffer full, dropping origin events")
	}
}

// Stats returns published and dropped counts.
func (p *Publisher) Stats() (published, dropped int64) {
	return p.published.Load(), p.dropped.Load()
}

// Collector exports publish/drop counts and the buffer fill level, read at scrape time.
func (p *Publisher) Collector() prometheus.Collector {
	return metrics.FuncCollector(
		metrics.Func{Name: "events_published_total", Help: "Local reads published to the event stream.",
			Value: func() float64 { return float64(p.published.Load()) }},
		metrics.Func{Name: "events_dropped_total", Help: "Local reads dropped (buffer full or stream write failed).",
			Value: func() float64 { return float64(p.dropped.Load()) }},
		metrics.Func{Name: "event_buffer_queued", Help: "Events waiting in the publisher buffer.", Gauge: true,
			Value: func() float64 { return float64(len(p.ch)) }},
	)
}

// Close flushes buffered events (bounded by ctx) and stops the publisher.
func (p *Publisher) Close(ctx context.Context) {
	p.once.Do(func() { close(p.ch) })
	select {
	case <-p.done:
	case <-ctx.Done():
	}
}

const batchKeys = 4096 // flush early once this many distinct keys are pending

func (p *Publisher) loop() {
	defer close(p.done)
	pending := make(map[string]*Event, batchKeys)
	timer := time.NewTimer(p.flush)
	defer timer.Stop()

	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := make([]Event, 0, len(pending))
		for _, e := range pending {
			batch = append(batch, *e)
		}
		p.write(batch)
		clear(pending)
	}

	for {
		select {
		case e, ok := <-p.ch:
			if !ok {
				flush()
				return
			}
			if cur, ok := pending[e.Key]; ok {
				cur.Count = cur.Reads() + e.Reads()
				if e.At.After(cur.At) {
					cur.At = e.At
				}
				if e.Size > 0 {
					cur.Size = e.Size
				}
				cur.Stale = cur.Stale || e.Stale
			} else {
				ev := e
				pending[e.Key] = &ev
			}
			if len(pending) >= batchKeys {
				flush()
			}
		case <-timer.C:
			flush()
			timer.Reset(p.flush)
		}
	}
}

func (p *Publisher) write(batch []Event) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, e := range batch {
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: p.stream,
				MaxLen: p.maxLen,
				Approx: true,
				Values: e.Values(),
			})
		}
		return nil
	})
	var reads int64
	for _, e := range batch {
		reads += e.Reads()
	}
	if err != nil {
		p.dropped.Add(reads)
		p.warnDropped("publish origin events failed", "count", reads, "err", err)
		return
	}
	p.published.Add(reads)
}
