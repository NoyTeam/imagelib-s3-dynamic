package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"
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

	// 1. PG ↔ Redis index. Snapshots are not atomic, so every difference is re-verified
	//    per key under the key lock before acting.
	pgS3, err := s.store.listS3(ctx)
	if err != nil {
		return err
	}
	indexed, err := s.index.All(ctx)
	if err != nil {
		return err
	}
	inPG := make(map[string]struct{}, len(pgS3))
	for _, e := range pgS3 {
		inPG[e.Key] = struct{}{}
		if _, ok := indexed[e.Key]; !ok {
			s.reconcileKey(ctx, e.Key, &rep)
		}
	}
	for key := range indexed {
		if _, ok := inPG[key]; !ok {
			s.reconcileKey(ctx, key, &rep)
		}
	}

	// 2. Bytes counter drift.
	rep.bytesBefore, rep.bytesAfter, err = s.index.Recount(ctx)
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

var errStopList = errors.New("stop listing")

// reconcileOrphans finds bucket objects under the prefix that are neither indexed nor
// waiting in the trash (e.g. uploads whose PG/Redis commit was lost in a crash).
func (s *Scheduler) reconcileOrphans(ctx context.Context, rep *reconcileReport) error {
	cutoff := s.now().Add(-s.cfg.OrphanMinAge)
	var candidates []string
	err := s.s3.List(ctx, func(key string, _ int64, modified time.Time) error {
		if ctx.Err() != nil {
			return errStopList
		}
		// Young objects may belong to an upload that is committing right now.
		if modified.After(cutoff) {
			return nil
		}
		if size, err := s.index.Size(ctx, key); err != nil || size >= 0 {
			return err
		}
		candidates = append(candidates, key)
		return nil
	})
	if err != nil && !errors.Is(err, errStopList) {
		return err
	}

	for _, key := range candidates {
		if !s.tryLock(key) {
			continue
		}
		size, err := s.index.Size(ctx, key)
		trashed, terr := s.index.InTrash(ctx, key)
		if err != nil || terr != nil || size >= 0 || trashed {
			s.unlock(key)
			continue
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
		s.unlock(key)
	}
	return nil
}
