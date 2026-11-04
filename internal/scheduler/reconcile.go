package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"imagelib-s3-dynamic/internal/state"
)

// reconcileLoop periodically repairs drift between PG (durable truth), the Redis index and
// the bucket. Drift can come from failed best-effort rollbacks, partial Redis data loss
// (e.g. maxmemory eviction) or crashes between steps.
func (s *Scheduler) reconcileLoop(ctx context.Context) {
	if s.cfg.ReconcileInterval <= 0 {
		return
	}
	tick := time.NewTicker(s.cfg.ReconcileInterval)
	defer tick.Stop()
	for {
		// Run once right after taking leadership: a crashed predecessor is the main drift source.
		if err := s.reconcile(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("reconcile failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

type reconcileReport struct {
	restored, dropped, finished int // PG↔index fixes
	bytesBefore, bytesAfter     int64
	orphans, orphansDeleted     int
}

func (s *Scheduler) reconcile(ctx context.Context) error {
	// Never let one run (e.g. listing a hung bucket) outlive its interval by much.
	ctx, cancel := context.WithTimeout(ctx, max(s.cfg.ReconcileInterval, time.Minute))
	defer cancel()
	if ok, err := s.index.Initialized(ctx); err != nil || !ok {
		return err // index check loop will rebuild first
	}
	start := s.now()
	var rep reconcileReport

	// 1. PG ↔ Redis index, streamed in pages with one batched lookup per page on the other
	//    side, so memory and per-command time stay bounded with millions of objects.
	//    Snapshots are not atomic: every difference is re-verified under the key lock.
	if err := s.reconcilePGToIndex(ctx, &rep); err != nil {
		return err
	}
	if err := s.reconcileIndexToPG(ctx, &rep); err != nil {
		return err
	}

	// 2. Bytes counter drift.
	var err error
	rep.bytesBefore, rep.bytesAfter, err = s.recount(ctx)
	if err != nil {
		return err
	}

	// 3. Bucket objects nobody knows about.
	if s.cfg.ReconcileOrphans != "off" {
		if err := s.reconcileOrphans(ctx, &rep); err != nil {
			return err
		}
	}

	attrs := []any{"took", s.now().Sub(start).Round(time.Millisecond),
		"restored", rep.restored, "dropped", rep.dropped, "finished_evictions", rep.finished,
		"orphans", rep.orphans, "orphans_deleted", rep.orphansDeleted}
	if rep.bytesBefore != rep.bytesAfter {
		attrs = append(attrs, "bytes_before", rep.bytesBefore, "bytes_after", rep.bytesAfter)
	}
	if rep.restored+rep.dropped+rep.finished+rep.orphans > 0 || rep.bytesBefore != rep.bytesAfter {
		slog.Warn("reconcile fixed drift", attrs...)
	} else {
		slog.Info("reconcile clean", attrs...)
	}
	return nil
}

// reconcilePGToIndex finds S3 rows in PG that are missing from the index.
func (s *Scheduler) reconcilePGToIndex(ctx context.Context, rep *reconcileReport) error {
	return s.store.forEachS3(ctx, func(page []state.Entry) error {
		for start := 0; start < len(page); start += reconcileBatch {
			chunk := page[start:min(start+reconcileBatch, len(page))]
			keys := make([]string, len(chunk))
			for i, e := range chunk {
				keys[i] = e.Key
			}
			sizes, err := s.index.Sizes(ctx, keys)
			if err != nil {
				return err
			}
			for i, n := range sizes {
				if n < 0 {
					s.reconcileKey(ctx, keys[i], rep)
				}
			}
		}
		return ctx.Err()
	})
}

// reconcileIndexToPG finds indexed keys that PG does not consider S3-resident.
func (s *Scheduler) reconcileIndexToPG(ctx context.Context, rep *reconcileReport) error {
	return s.index.ScanKeys(ctx, func(keys []string) error {
		inPG, err := s.store.filterS3(ctx, keys)
		if err != nil {
			return err
		}
		for _, k := range keys {
			if _, ok := inPG[k]; !ok {
				s.reconcileKey(ctx, k, rep)
			}
		}
		return ctx.Err()
	})
}

// reconcileBatch is the number of keys checked per batched Redis/PG lookup.
const reconcileBatch = 1000

// reconcileKey makes the index agree with PG for one key.
func (s *Scheduler) reconcileKey(ctx context.Context, key string, rep *reconcileReport) {
	if !s.tryLock(key) {
		return // being migrated/evicted right now; next round
	}
	defer s.unlock(key)
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()

	r, err := s.store.find(ctx, key)
	if err != nil {
		slog.Warn("reconcile: load row failed", "key", key, "err", err)
		return
	}
	size, err := s.index.Size(ctx, key)
	if err != nil {
		slog.Warn("reconcile: index lookup failed", "key", key, "err", err)
		return
	}
	pgSaysS3 := r != nil && r.Status == StatusS3
	now := s.now()

	switch {
	case pgSaysS3 && size < 0:
		trashed, err := s.index.InTrash(ctx, key)
		if err != nil {
			return
		}
		if trashed || r.Size <= 0 {
			// An eviction de-indexed it but failed to update PG: finish the eviction.
			if err := s.store.markLocal(ctx, key, now); err == nil {
				if !trashed {
					_ = s.index.ScheduleDelete(ctx, key, now.Add(s.cfg.DeleteDelay))
				}
				rep.finished++
				slog.Warn("reconcile: finished interrupted eviction", "key", key)
			}
			return
		}
		// Index lost the entry. Restore it; the capacity loop trims if this overflows.
		if _, err := s.index.Add(ctx, key, r.Size, r.LastAccess); err == nil {
			rep.restored++
			slog.Warn("reconcile: restored missing index entry", "key", key, "size", r.Size)
		}
	case !pgSaysS3 && size >= 0:
		// Index serves an object PG doesn't consider migrated: drop it and delete later.
		if _, err := s.index.Remove(ctx, key); err == nil {
			_ = s.index.ScheduleDelete(ctx, key, now.Add(s.cfg.DeleteDelay))
			rep.dropped++
			slog.Warn("reconcile: dropped index entry unknown to postgres", "key", key, "size", size)
		}
	}
}

// reconcileOrphans finds bucket objects under the prefix that are neither indexed nor
// waiting in the trash (e.g. uploads whose PG/Redis commit was lost in a crash). The bucket
// listing is processed in batches: one HMGET + one pipelined trash lookup per batch, and only
// the (rare) candidates are re-verified one by one under the key lock.
func (s *Scheduler) reconcileOrphans(ctx context.Context, rep *reconcileReport) error {
	cutoff := s.now().Add(-s.cfg.OrphanMinAge)
	batch := make([]string, 0, reconcileBatch)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		defer func() { batch = batch[:0] }()
		sizes, err := s.index.Sizes(ctx, batch)
		if err != nil {
			return err
		}
		var unindexed []string
		for i, n := range sizes {
			if n < 0 {
				unindexed = append(unindexed, batch[i])
			}
		}
		trashed, err := s.index.TrashedAmong(ctx, unindexed)
		if err != nil {
			return err
		}
		for _, key := range unindexed {
			if !trashed[key] {
				s.handleOrphan(ctx, key, rep)
			}
		}
		return nil
	}
	err := s.s3.List(ctx, func(key string, _ int64, modified time.Time) error {
		if ctx.Err() != nil {
			return errStopList
		}
		// Young objects may belong to an upload that is committing right now.
		if modified.After(cutoff) {
			return nil
		}
		batch = append(batch, key)
		if len(batch) == cap(batch) {
			return flush()
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopList) {
		return err
	}
	return flush()
}

var errStopList = errors.New("stop listing")

// handleOrphan re-verifies a candidate under its key lock and reports or deletes it.
func (s *Scheduler) handleOrphan(ctx context.Context, key string, rep *reconcileReport) {
	if !s.tryLock(key) {
		return
	}
	defer s.unlock(key)
	size, err := s.index.Size(ctx, key)
	trashed, terr := s.index.InTrash(ctx, key)
	if err != nil || terr != nil || size >= 0 || trashed {
		return
	}
	rep.orphans++
	if s.cfg.ReconcileOrphans == "delete" {
		if err := s.s3.Delete(ctx, key); err != nil {
			slog.Warn("reconcile: delete orphan failed", "key", key, "err", err)
		} else {
			rep.orphansDeleted++
			slog.Info("reconcile: deleted orphan object", "key", key)
		}
	} else if rep.orphans <= 20 {
		slog.Warn("reconcile: orphan object in bucket (RECONCILE_ORPHANS=log)", "key", key)
	}
}
