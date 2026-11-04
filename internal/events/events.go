package events

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Event is emitted every time a request is served from local storage.
type Event struct {
	Key  string
	Size int64
	At   time.Time
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
	return v
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
	return e, true
}

// Publisher asynchronously appends events to the stream. Submit never blocks the
// request path: when the buffer is full events are dropped (heat is approximate anyway).
type Publisher struct {
	rdb    redis.UniversalClient
	stream string
	maxLen int64
	ch     chan Event
	done   chan struct{}
	once   sync.Once

	dropped   atomic.Int64
	published atomic.Int64
}

func NewPublisher(rdb redis.UniversalClient, stream string, maxLen int64, buffer int) *Publisher {
	p := &Publisher{
		rdb:    rdb,
		stream: stream,
		maxLen: maxLen,
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
		if p.dropped.Add(1)%1000 == 1 {
			slog.Warn("event buffer full, dropping origin events", "dropped_total", p.dropped.Load())
		}
	}
}

// Stats returns published and dropped counts.
func (p *Publisher) Stats() (published, dropped int64) {
	return p.published.Load(), p.dropped.Load()
}

// Close flushes buffered events (bounded by ctx) and stops the publisher.
func (p *Publisher) Close(ctx context.Context) {
	p.once.Do(func() { close(p.ch) })
	select {
	case <-p.done:
	case <-ctx.Done():
	}
}

const (
	batchSize     = 256
	flushInterval = 50 * time.Millisecond
)

func (p *Publisher) loop() {
	defer close(p.done)
	batch := make([]Event, 0, batchSize)
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		p.write(batch)
		batch = batch[:0]
	}

	for {
		select {
		case e, ok := <-p.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, e)
			if len(batch) >= batchSize {
				flush()
			}
		case <-timer.C:
			flush()
			timer.Reset(flushInterval)
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
	if err != nil {
		p.dropped.Add(int64(len(batch)))
		slog.Warn("publish origin events failed", "count", len(batch), "err", err)
		return
	}
	p.published.Add(int64(len(batch)))
}
