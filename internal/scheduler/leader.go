package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Elector implements single-leader election with a Redis lock (SET NX PX + fenced renew).
type Elector struct {
	rdb redis.UniversalClient
	key string
	id  string
	ttl time.Duration
}

func NewElector(rdb redis.UniversalClient, key, id string, ttl time.Duration) *Elector {
	return &Elector{rdb: rdb, key: key, id: id, ttl: ttl}
}

var renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// Run campaigns until ctx is done. Whenever leadership is acquired, lead is invoked with a
// context that is cancelled as soon as leadership is lost; Run waits for lead to return
// before campaigning again so two leaders never run concurrently inside this process.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context)) {
	interval := e.ttl / 3
	for {
		ok, err := e.rdb.SetNX(ctx, e.key, e.id, e.ttl).Result()
		if err != nil && ctx.Err() == nil {
			slog.Warn("leader election failed", "err", err)
		}
		if ok {
			slog.Info("acquired scheduler leadership", "id", e.id)
			e.hold(ctx, lead, interval)
			slog.Info("released scheduler leadership", "id", e.id)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// hold runs lead while renewing the lock. On shutdown it keeps renewing until lead has
// fully stopped and only then releases the lock, so a successor never overlaps with us.
func (e *Elector) hold(ctx context.Context, lead func(ctx context.Context), interval time.Duration) {
	leadCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		lead(leadCtx)
	}()

	tick := time.NewTicker(interval)
	defer tick.Stop()
	shutdown := ctx.Done()
	owned := true
	lastRenew := time.Now()
	for {
		select {
		case <-shutdown:
			shutdown = nil // stop lead, but keep the lock until it has returned
			cancel()
		case <-done:
			if owned {
				rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
				if err := releaseScript.Run(rctx, e.rdb, []string{e.key}, e.id).Err(); err != nil {
					slog.Warn("release leadership failed", "err", err)
				}
				rcancel()
			}
			return
		case <-tick.C:
			if !owned {
				continue
			}
			rctx, rcancel := context.WithTimeout(context.Background(), interval)
			n, err := renewScript.Run(rctx, e.rdb, []string{e.key}, e.id, e.ttl.Milliseconds()).Int64()
			rcancel()
			switch {
			case err == nil && n == 1:
				lastRenew = time.Now()
			case err == nil:
				slog.Warn("lost scheduler leadership (lock taken over)")
				owned = false
				cancel()
			case time.Since(lastRenew) > e.ttl-interval:
				// Can't prove we still own the lock before it may expire: step down.
				slog.Warn("lost scheduler leadership (renew failing)", "err", err)
				owned = false
				cancel()
			default:
				slog.Warn("leadership renew failed, retrying", "err", err)
			}
		}
	}
}
