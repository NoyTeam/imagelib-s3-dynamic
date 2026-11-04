package proxy

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"imagelib-s3-dynamic/internal/state"
)

// maxPendingTouches bounds memory between flushes; beyond it touches are dropped
// (LRU order is approximate by design).
const maxPendingTouches = 100_000

// toucher batches LRU access-time updates for S3 hits so the hot path does
// a single Redis round trip (the index lookup).
type toucher struct {
	index    *state.Index
	interval time.Duration

	mu      sync.Mutex
	pending map[string]struct{}

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newToucher(index *state.Index, interval time.Duration) *toucher {
	t := &toucher{
		index:    index,
		interval: interval,
		pending:  make(map[string]struct{}),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go t.loop()
	return t
}

func (t *toucher) add(key string) {
	t.mu.Lock()
	if len(t.pending) < maxPendingTouches {
		t.pending[key] = struct{}{}
	}
	t.mu.Unlock()
}

func (t *toucher) loop() {
	defer close(t.done)
	tick := time.NewTicker(t.interval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			t.flush()
		case <-t.stop:
			t.flush()
			return
		}
	}
}

func (t *toucher) flush() {
	t.mu.Lock()
	if len(t.pending) == 0 {
		t.mu.Unlock()
		return
	}
	keys := make([]string, 0, len(t.pending))
	for k := range t.pending {
		keys = append(keys, k)
	}
	t.pending = make(map[string]struct{})
	t.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const chunk = 1000
	for i := 0; i < len(keys); i += chunk {
		end := min(i+chunk, len(keys))
		if err := t.index.Touch(ctx, keys[i:end], time.Now()); err != nil {
			slog.Warn("lru touch failed", "count", end-i, "err", err)
			return
		}
	}
}

func (t *toucher) close(ctx context.Context) {
	t.once.Do(func() { close(t.stop) })
	select {
	case <-t.done:
	case <-ctx.Done():
	}
}
