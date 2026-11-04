// Package scheduler is the asynchronous migration scheduler. It only runs on the elected leader:
//
//	consume origin event stream → update decayed heat
//	heat ≥ threshold            → upload local object to S3 → write PG + Redis index
//	capacity controller         → evict LRU objects so S3 total stays ≤ S3_CAPACITY
package scheduler

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/events"
	"imagelib-s3-dynamic/internal/state"
	"imagelib-s3-dynamic/internal/storage"
)

const consumerGroup = "migrator"

// Uploader is the subset of S3 operations the scheduler needs.
type Uploader interface {
	Put(ctx context.Context, key string, body io.ReaderAt, size int64, contentType string) error
	Delete(ctx context.Context, key string) error
	// List calls fn for every object under the configured prefix (keys without prefix).
	List(ctx context.Context, fn func(key string, size int64, modified time.Time) error) error
}

type Scheduler struct {
	cfg   config.SchedulerConfig
	id    string
	rdb   redis.UniversalClient
	keys  state.Keys
	index *state.Index
	local *storage.Local
	s3    Uploader
	store Store
	stats *stats
	now   func() time.Time

	// busy serialises all work on a key (migration, eviction, trash deletion) inside the leader.
	busyMu sync.Mutex
	busy   map[string]struct{}
	// backoff holds keys whose migration was rejected for lack of evictable space.
	backoff map[string]time.Time

	// indexMu: index rebuilds take it exclusively; every compound PG+index write holds it
	// shared, so a rebuild never interleaves with (and silently undoes) such a write.
	indexMu sync.RWMutex

	// capMu serialises capacity decisions; reserved counts bytes of in-flight uploads.
	capMu    sync.Mutex
	reserved int64

	migrateCh chan migrateJob
}

type migrateJob struct {
	key  string
	size int64
}

func New(cfg config.SchedulerConfig, id string, rdb redis.UniversalClient, index *state.Index,
	local *storage.Local, s3 Uploader, st Store) *Scheduler {
	return &Scheduler{
		cfg:       cfg,
		id:        id,
		rdb:       rdb,
		keys:      index.Keys(),
		index:     index,
		local:     local,
		s3:        s3,
		store:     st,
		stats:     newStats(),
		now:       time.Now,
		busy:      make(map[string]struct{}),
		backoff:   make(map[string]time.Time),
		migrateCh: make(chan migrateJob, cfg.Workers*64),
	}
}

// Lead runs all scheduler loops until ctx is cancelled (leadership lost or shutdown).
func (s *Scheduler) Lead(ctx context.Context) {
	if err := s.ensureIndex(ctx); err != nil {
		slog.Error("index rebuild failed, scheduler not started", "err", err)
		return
	}
	if err := s.ensureGroup(ctx); err != nil {
		slog.Error("create consumer group failed, scheduler not started", "err", err)
		return
	}

	var wg sync.WaitGroup
	for range s.cfg.Workers {
		wg.Go(func() { s.worker(ctx) })
	}
	wg.Go(func() { s.capacityLoop(ctx) })
	wg.Go(func() { s.indexCheckLoop(ctx) })
	wg.Go(func() { s.reconcileLoop(ctx) })
	wg.Go(func() { s.trashLoop(ctx) })
	wg.Go(func() { s.statsLoop(ctx) })
	wg.Go(func() { s.heatGCLoop(ctx) })
	s.consume(ctx)
	wg.Wait()
}

// ---------------------------------------------------------------------------
// setup

func (s *Scheduler) ensureIndex(ctx context.Context) error {
	ok, err := s.index.Initialized(ctx)
	if err != nil || ok {
		return err
	}
	return s.rebuildIndex(ctx)
}

// rebuildIndex replaces the Redis index with the PG view (PG is the durable source of truth).
func (s *Scheduler) rebuildIndex(ctx context.Context) error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if ok, err := s.index.Initialized(ctx); err != nil || ok {
		return err // someone (a previous loop iteration) already rebuilt it
	}
	entries, err := s.store.listS3(ctx)
	if err != nil {
		return err
	}
	slog.Info("rebuilding S3 index from postgres", "objects", len(entries))
	return s.index.Rebuild(ctx, entries)
}

// indexCheckLoop detects an index lost while we lead (Redis flushed/restarted without
// persistence, keys evicted) and rebuilds it. Until then proxies simply serve local and
// index writes fail with state.ErrNotInitialized.
func (s *Scheduler) indexCheckLoop(ctx context.Context) {
	tick := time.NewTicker(s.cfg.IndexCheckInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		ok, err := s.index.Initialized(ctx)
		if err != nil || ok {
			continue
		}
		slog.Warn("s3 index lost, rebuilding from postgres")
		if err := s.rebuildIndex(ctx); err != nil && ctx.Err() == nil {
			slog.Error("index rebuild failed", "err", err)
		}
	}
}

func (s *Scheduler) ensureGroup(ctx context.Context) error {
	err := s.rdb.XGroupCreateMkStream(ctx, s.keys.Events, consumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// event consumption & heat

func (s *Scheduler) consume(ctx context.Context) {
	// Take over anything previous leaders read but never acknowledged.
	start := "0-0"
	for ctx.Err() == nil {
		msgs, next, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: s.keys.Events, Group: consumerGroup, Consumer: s.id,
			MinIdle: 0, Start: start, Count: 256,
		}).Result()
		if err != nil {
			slog.Warn("claim pending events failed", "err", err)
			break
		}
		s.handleBatch(ctx, msgs)
		if next == "0-0" || next == "" {
			break
		}
		start = next
	}
	s.cleanupConsumers(ctx)

	for ctx.Err() == nil {
		res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: consumerGroup, Consumer: s.id,
			Streams: []string{s.keys.Events, ">"},
			Count:   512, Block: 2 * time.Second,
		}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("read events failed", "err", err)
			if strings.HasPrefix(err.Error(), "NOGROUP") {
				// Stream/group lost (FLUSHALL, eviction): recreate and keep consuming.
				if gerr := s.ensureGroup(ctx); gerr == nil {
					slog.Info("recreated event consumer group")
					continue
				}
			}
			sleep(ctx, time.Second)
			continue
		}
		for _, st := range res {
			s.handleBatch(ctx, st.Messages)
		}
	}
}

func (s *Scheduler) cleanupConsumers(ctx context.Context) {
	consumers, err := s.rdb.XInfoConsumers(ctx, s.keys.Events, consumerGroup).Result()
	if err != nil {
		return
	}
	for _, c := range consumers {
		if c.Name != s.id && c.Pending == 0 {
			s.rdb.XGroupDelConsumer(ctx, s.keys.Events, consumerGroup, c.Name)
		}
	}
}

type keyAgg struct {
	hits  int64
	size  int64
	last  time.Time
	stale bool
}

func (s *Scheduler) handleBatch(ctx context.Context, msgs []redis.XMessage) {
	if len(msgs) == 0 {
		return
	}
	ids := make([]string, 0, len(msgs))
	aggs := make(map[string]*keyAgg)
	for _, m := range msgs {
		ids = append(ids, m.ID)
		e, ok := events.Decode(m)
		if !ok {
			continue
		}
		a := aggs[e.Key]
		if a == nil {
			a = &keyAgg{}
			aggs[e.Key] = a
		}
		if e.Size > 0 {
			a.hits += e.Reads()
			a.size = e.Size
		}
		if e.At.After(a.last) {
			a.last = e.At
		}
		a.stale = a.stale || e.Stale
	}

	if err := s.processAggs(ctx, aggs); err != nil {
		slog.Warn("process events failed", "err", err)
	}
	// Heat is approximate: acknowledge even on partial failure to avoid poison batches.
	if err := s.rdb.XAck(ctx, s.keys.Events, consumerGroup, ids...).Err(); err != nil && ctx.Err() == nil {
		slog.Warn("ack events failed", "err", err)
	}
}

func (s *Scheduler) processAggs(ctx context.Context, aggs map[string]*keyAgg) error {
	keys := make([]string, 0, len(aggs))
	for k, a := range aggs {
		if a.stale {
			s.handleStale(ctx, k)
		}
		if a.hits > 0 {
			keys = append(keys, k)
			s.stats.add(k, a.hits, a.last, a.size)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	slices.Sort(keys)

	// Update decayed heat: score = score·2^(-Δt/halfLife) + hits.
	raw, err := s.rdb.HMGet(ctx, s.keys.Heat, keys...).Result()
	if err != nil {
		return err
	}
	scores := make([]float64, len(keys))
	fields := make([]any, 0, len(keys)*2)
	for i, k := range keys {
		a := aggs[k]
		old, at := decodeHeat(raw[i])
		scores[i] = s.decay(old, at, a.last) + float64(a.hits)
		fields = append(fields, k, encodeHeat(scores[i], a.last))
	}
	if err := s.rdb.HSet(ctx, s.keys.Heat, fields...).Err(); err != nil {
		return err
	}

	// Candidates above threshold that are not already in S3.
	var cands []string
	for i, k := range keys {
		if scores[i]+heatEpsilon >= s.cfg.Threshold && s.sizeEligible(aggs[k].size) {
			cands = append(cands, k)
		}
	}
	if len(cands) == 0 {
		return nil
	}
	cmds, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, k := range cands {
			p.HExists(ctx, s.keys.Objects, k)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i, k := range cands {
		if cmds[i].(*redis.BoolCmd).Val() {
			continue
		}
		s.enqueue(k, aggs[k].size)
	}
	return nil
}

// heatEpsilon absorbs the tiny decay between events so N quick hits reach a threshold of N.
const heatEpsilon = 0.01

func (s *Scheduler) decay(score float64, from, to time.Time) float64 {
	if score == 0 || from.IsZero() {
		return score
	}
	dt := to.Sub(from)
	if dt <= 0 {
		return score
	}
	return score * math.Exp2(-dt.Seconds()/s.cfg.HalfLife.Seconds())
}

func (s *Scheduler) sizeEligible(size int64) bool {
	return size > 0 && size >= s.cfg.MinSize && size <= s.cfg.MaxSize && size <= s.cfg.CapacityBytes
}

func encodeHeat(score float64, at time.Time) string {
	return strconv.FormatFloat(score, 'g', 6, 64) + "|" + strconv.FormatInt(at.UnixMilli(), 10)
}

func decodeHeat(v any) (float64, time.Time) {
	str, _ := v.(string)
	sc, ts, ok := strings.Cut(str, "|")
	if !ok {
		return 0, time.Time{}
	}
	score, _ := strconv.ParseFloat(sc, 64)
	ms, _ := strconv.ParseInt(ts, 10, 64)
	return score, time.UnixMilli(ms)
}

// heatGCLoop drops heat entries that have decayed to insignificance.
func (s *Scheduler) heatGCLoop(ctx context.Context) {
	tick := time.NewTicker(s.cfg.HalfLife)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		floor := s.cfg.Threshold / 100
		now := s.now()
		var cursor uint64
		removed := 0
		for ctx.Err() == nil {
			kv, next, err := s.rdb.HScan(ctx, s.keys.Heat, cursor, "", 1000).Result()
			if err != nil {
				slog.Warn("heat gc scan failed", "err", err)
				break
			}
			var dead []string
			for i := 0; i+1 < len(kv); i += 2 {
				score, at := decodeHeat(kv[i+1])
				if s.decay(score, at, now) < floor {
					dead = append(dead, kv[i])
				}
			}
			if len(dead) > 0 {
				s.rdb.HDel(ctx, s.keys.Heat, dead...)
				removed += len(dead)
			}
			if cursor = next; cursor == 0 {
				break
			}
		}
		if removed > 0 {
			slog.Info("heat gc", "removed", removed)
		}
	}
}

// ---------------------------------------------------------------------------
// key locks

func (s *Scheduler) tryLock(key string) bool {
	s.busyMu.Lock()
	defer s.busyMu.Unlock()
	if _, ok := s.busy[key]; ok {
		return false
	}
	s.busy[key] = struct{}{}
	return true
}

func (s *Scheduler) unlock(key string) {
	s.busyMu.Lock()
	delete(s.busy, key)
	s.busyMu.Unlock()
}

// ---------------------------------------------------------------------------
// migration

func (s *Scheduler) enqueue(key string, size int64) {
	s.busyMu.Lock()
	until, backingOff := s.backoff[key]
	if backingOff && s.now().After(until) {
		delete(s.backoff, key)
		backingOff = false
	}
	s.busyMu.Unlock()
	if backingOff {
		return
	}
	if !s.tryLock(key) {
		return // already queued or in progress
	}
	select {
	case s.migrateCh <- migrateJob{key: key, size: size}:
	default:
		s.unlock(key) // queue full; the next event for this key will retry
	}
}

func (s *Scheduler) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Release locks of jobs that will never run.
			for {
				select {
				case j := <-s.migrateCh:
					s.unlock(j.key)
				default:
					return
				}
			}
		case j := <-s.migrateCh:
			err := s.migrate(ctx, j.key)
			switch {
			case errors.Is(err, errNoSpace):
				s.busyMu.Lock()
				s.backoff[j.key] = s.now().Add(s.cfg.RetryBackoff)
				s.busyMu.Unlock()
				slog.Debug("migration deferred: no idle objects to evict", "key", j.key)
			case err != nil && ctx.Err() == nil:
				slog.Warn("migration failed", "key", j.key, "err", err)
			}
			s.unlock(j.key)
		}
	}
}

func (s *Scheduler) migrate(ctx context.Context, key string) error {
	// While the index is lost "not indexed" means nothing; wait for the rebuild.
	if ok, err := s.index.Initialized(ctx); err != nil || !ok {
		return err
	}
	if ok, err := s.index.Contains(ctx, key); err != nil || ok {
		return err
	}
	f, info, err := s.local.Open(key)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	size := info.Size()
	if !s.sizeEligible(size) {
		return nil
	}

	if err := s.reserve(ctx, size); err != nil {
		return err
	}
	defer s.release(size)

	// A previous copy may be waiting in the trash; we are about to overwrite it.
	if _, err := s.index.CancelDelete(ctx, key); err != nil {
		return err
	}

	contentType := storage.DetectContentType(key, f)
	start := s.now()
	upCtx, cancel := context.WithTimeout(ctx, uploadTimeout(size))
	defer cancel()
	if err := s.s3.Put(upCtx, key, f, size, contentType); err != nil {
		return err
	}

	// The local file must not have changed while uploading.
	if after, err := f.Stat(); err != nil || after.Size() != size || !after.ModTime().Equal(info.ModTime()) {
		s.deleteQuietly(key)
		return errors.New("local file changed during upload")
	}

	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	now := s.now()
	if err := s.store.markS3(ctx, key, contentType, size, now); err != nil {
		s.deleteQuietly(key)
		return err
	}
	total, err := s.index.Add(ctx, key, size, now)
	if errors.Is(err, state.ErrNotInitialized) {
		// Index was lost mid-commit. PG already says s3 and the object is uploaded, which is
		// exactly what the pending rebuild (blocked on indexMu until we return) will restore.
		slog.Warn("index lost during migration commit; rebuild will restore it", "key", key)
		return nil
	}
	if err != nil {
		// Roll PG back so it never claims an object Redis doesn't serve.
		bg, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = s.store.markLocal(bg, key, now)
		s.deleteQuietly(key)
		return err
	}
	slog.Info("migrated to s3", "key", key, "size", size, "took", s.now().Sub(start).Round(time.Millisecond),
		"s3_bytes", total, "capacity", s.cfg.CapacityBytes)
	return nil
}

func uploadTimeout(size int64) time.Duration {
	// 1 minute base + 1s per MiB (assumes ≥1 MiB/s to the bucket).
	return time.Minute + time.Duration(size>>20)*time.Second
}

func (s *Scheduler) deleteQuietly(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.s3.Delete(ctx, key); err != nil {
		slog.Warn("cleanup s3 object failed", "key", key, "err", err)
	}
}

// handleStale removes an object from the index when proxies observed it missing in S3.
func (s *Scheduler) handleStale(ctx context.Context, key string) {
	if !s.tryLock(key) {
		return
	}
	defer s.unlock(key)
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	freed, err := s.index.Remove(ctx, key)
	if err != nil {
		slog.Warn("remove stale index entry failed", "key", key, "err", err)
		return
	}
	if freed < 0 {
		return
	}
	if err := s.store.markLocal(ctx, key, s.now()); err != nil {
		slog.Warn("mark stale object local failed", "key", key, "err", err)
	}
	slog.Warn("removed stale s3 index entry", "key", key, "size", freed)
}

// ---------------------------------------------------------------------------
// capacity controller (LRU)

var errNoSpace = errors.New("no evictable space in s3")

// reserve makes room for size bytes by evicting least recently used objects. To avoid
// thrashing when the hot set exceeds capacity, only objects idle for at least
// EvictMinIdle may be evicted to admit a new one.
func (s *Scheduler) reserve(ctx context.Context, size int64) error {
	s.capMu.Lock()
	defer s.capMu.Unlock()
	for {
		total, err := s.index.TotalBytes(ctx)
		if err != nil {
			return err
		}
		if total+s.reserved+size <= s.cfg.CapacityBytes {
			s.reserved += size
			return nil
		}
		ok, err := s.evictOne(ctx, s.now().Add(-s.cfg.EvictMinIdle))
		if err != nil {
			return err
		}
		if !ok {
			return errNoSpace
		}
	}
}

func (s *Scheduler) release(size int64) {
	s.capMu.Lock()
	s.reserved -= size
	s.capMu.Unlock()
}

// enforce evicts until total + reserved fits the configured capacity.
func (s *Scheduler) enforce(ctx context.Context) {
	s.capMu.Lock()
	defer s.capMu.Unlock()
	evicted := 0
	for ctx.Err() == nil {
		total, err := s.index.TotalBytes(ctx)
		if err != nil {
			slog.Warn("capacity check failed", "err", err)
			return
		}
		if total+s.reserved <= s.cfg.CapacityBytes {
			break
		}
		ok, err := s.evictOne(ctx, time.Time{})
		if err != nil {
			slog.Warn("eviction failed", "err", err)
			return
		}
		if !ok {
			break
		}
		evicted++
	}
	if evicted > 0 {
		slog.Info("capacity enforced", "evicted", evicted)
	}
}

// evictOne evicts the least recently used object that is not busy. If idleBefore is set,
// only objects last accessed before it qualify. Caller holds capMu.
func (s *Scheduler) evictOne(ctx context.Context, idleBefore time.Time) (bool, error) {
	cands, err := s.index.OldestEntries(ctx, 32)
	if err != nil {
		return false, err
	}
	for _, c := range cands {
		if !idleBefore.IsZero() && c.LastAccess.After(idleBefore) {
			return false, nil // sorted by access time: nothing older remains
		}
		if !s.tryLock(c.Key) {
			continue
		}
		err := s.evict(ctx, c.Key)
		s.unlock(c.Key)
		return err == nil, err
	}
	return false, nil
}

// evict de-indexes key (proxies immediately fall back to local), marks it local in PG and
// schedules the S3 delete after a grace period so in-flight proxied responses can finish.
func (s *Scheduler) evict(ctx context.Context, key string) error {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	freed, err := s.index.Remove(ctx, key)
	if err != nil {
		return err
	}
	now := s.now()
	if err := s.index.ScheduleDelete(ctx, key, now.Add(s.cfg.DeleteDelay)); err != nil {
		return err
	}
	if err := s.store.markLocal(ctx, key, now); err != nil {
		slog.Warn("mark evicted object local failed", "key", key, "err", err)
	}
	slog.Info("evicted from s3", "key", key, "size", freed)
	return nil
}

func (s *Scheduler) capacityLoop(ctx context.Context) {
	s.enforce(ctx) // capacity may have been lowered in config
	tick := time.NewTicker(s.cfg.EvictInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		s.enforce(ctx)
		s.pruneBackoff()
	}
}

// trashLoop deletes de-indexed objects from S3 once their grace period expired.
func (s *Scheduler) trashLoop(ctx context.Context) {
	interval := min(max(s.cfg.DeleteDelay/2, 100*time.Millisecond), 5*time.Second)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		keys, err := s.index.DueDeletes(ctx, s.now(), 100)
		if err != nil {
			slog.Warn("list due deletes failed", "err", err)
			continue
		}
		for _, key := range keys {
			if !s.tryLock(key) {
				continue // being re-migrated; it will cancel the delete
			}
			if claimed, err := s.index.CancelDelete(ctx, key); err == nil && claimed {
				if err := s.s3.Delete(ctx, key); err != nil {
					slog.Warn("s3 delete failed, retrying later", "key", key, "err", err)
					_ = s.index.ScheduleDelete(ctx, key, s.now().Add(time.Minute))
				}
			}
			s.unlock(key)
		}
	}
}

// ---------------------------------------------------------------------------
// stats

func (s *Scheduler) statsLoop(ctx context.Context) {
	tick := time.NewTicker(s.cfg.StatsFlushInterval)
	defer tick.Stop()
	flush := func(ctx context.Context) {
		if rows := s.stats.take(); len(rows) > 0 {
			if err := s.store.flushStats(ctx, rows); err != nil {
				slog.Warn("stats flush failed", "rows", len(rows), "err", err)
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			bg, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			flush(bg)
			cancel()
			return
		case <-tick.C:
			flush(ctx)
		}
	}
}

func (s *Scheduler) pruneBackoff() {
	now := s.now()
	s.busyMu.Lock()
	for k, until := range s.backoff {
		if now.After(until) {
			delete(s.backoff, k)
		}
	}
	s.busyMu.Unlock()
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
