package proxy

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"golang.org/x/sync/singleflight"

	"imagelib-s3-dynamic/internal/breaker"
	"imagelib-s3-dynamic/internal/state"
)

// indexLookup answers "is key served from S3?" while keeping Redis load independent of
// request rate:
//
//	Ristretto (positive + negative entries, TTL) ─miss─▶ singleflight per key ─▶ Redis HEXISTS
//
// Staleness is bounded by ttl and is always safe: a stale "in S3" hits an object that is kept
// for EVICT_DELETE_DELAY (> ttl) after eviction, or falls back to local on 404; a stale
// "not in S3" just serves from local (the source of truth) a little longer.
type indexLookup struct {
	index   *state.Index
	breaker *breaker.Breaker
	timeout time.Duration
	ttl     time.Duration
	cache   *ristretto.Cache[string, bool] // nil when caching is disabled
	sf      singleflight.Group

	hits, misses, redisCalls, coalesced atomic.Int64
}

func newIndexLookup(index *state.Index, br *breaker.Breaker, timeout, ttl time.Duration, maxKeys int64) (*indexLookup, error) {
	l := &indexLookup{index: index, breaker: br, timeout: timeout, ttl: ttl}
	if ttl > 0 && maxKeys > 0 {
		c, err := ristretto.NewCache(&ristretto.Config[string, bool]{
			NumCounters:        maxKeys * 10, // ~10x tracked keys for TinyLFU admission accuracy
			MaxCost:            maxKeys,      // cost 1 per entry ⇒ MaxCost is the entry limit
			BufferItems:        64,
			IgnoreInternalCost: true,
		})
		if err != nil {
			return nil, err
		}
		l.cache = c
	}
	return l, nil
}

func (l *indexLookup) close() {
	if l.cache != nil {
		l.cache.Close()
	}
}

// inS3 never fails: on any Redis trouble it answers false (serve local).
func (l *indexLookup) inS3(ctx context.Context, key string) bool {
	if l.cache != nil {
		if v, ok := l.cache.Get(key); ok {
			l.hits.Add(1)
			return v
		}
		l.misses.Add(1)
	}
	if !l.breaker.Allow() {
		return false
	}
	ch := l.sf.DoChan(key, func() (any, error) {
		return l.fetch(key)
	})
	select {
	case r := <-ch:
		if r.Shared {
			l.coalesced.Add(1)
		}
		if r.Err != nil {
			return false
		}
		return r.Val.(bool)
	case <-ctx.Done():
		return false // client went away; the shared call keeps going for the others
	}
}

// fetch performs the single shared Redis call. It deliberately uses its own context so one
// caller disconnecting doesn't fail the lookup for everybody waiting on it.
func (l *indexLookup) fetch(key string) (bool, error) {
	l.redisCalls.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), l.timeout)
	defer cancel()
	v, err := l.index.Contains(ctx, key)
	if err != nil {
		l.breaker.Failure()
		slog.Warn("index lookup failed, serving local", "key", key, "err", err)
		return false, err
	}
	l.breaker.Success()
	l.remember(key, v)
	return v, nil
}

func (l *indexLookup) remember(key string, inS3 bool) {
	if l.cache != nil {
		l.cache.SetWithTTL(key, inS3, 1, l.ttl)
	}
}

func (l *indexLookup) stats() map[string]string {
	return map[string]string{
		"index_cache_hits":     strconv.FormatInt(l.hits.Load(), 10),
		"index_cache_misses":   strconv.FormatInt(l.misses.Load(), 10),
		"index_redis_lookups":  strconv.FormatInt(l.redisCalls.Load(), 10),
		"index_lookups_shared": strconv.FormatInt(l.coalesced.Load(), 10),
		"index_cache_enabled":  strconv.FormatBool(l.cache != nil),
	}
}
