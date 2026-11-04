package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/events"
)

// --- heat ---

func TestHeatDecaysByHalfLife(t *testing.T) {
	s := &Scheduler{cfg: config.SchedulerConfig{HalfLife: time.Hour}}
	t0 := time.Unix(1_000_000, 0)
	cases := []struct {
		dt   time.Duration
		want float64
	}{{0, 8}, {time.Hour, 4}, {2 * time.Hour, 2}, {30 * time.Minute, 8 / math.Sqrt2}, {-time.Minute, 8}}
	for _, c := range cases {
		if got := s.decay(8, t0, t0.Add(c.dt)); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("decay(8, Δ=%s) = %v, want %v", c.dt, got, c.want)
		}
	}
	if s.decay(5, time.Time{}, t0) != 5 {
		t.Error("zero timestamp must not decay")
	}
	score, at := decodeHeat(encodeHeat(2.5, t0))
	if score != 2.5 || !at.Equal(t0) {
		t.Errorf("heat codec round trip: %v %v", score, at)
	}
	if s, at := decodeHeat("garbage"); s != 0 || !at.IsZero() {
		t.Error("malformed heat must decode to zero")
	}
}

func TestSpacedOutHitsDoNotMigrate(t *testing.T) {
	e := newEnv(t, 1000, func(c *config.SchedulerConfig) { c.HalfLife = 50 * time.Millisecond })
	e.file(t, "slow.png", "s")
	e.start(t)
	for range 4 {
		e.hit(t, "slow.png", 1)
		time.Sleep(300 * time.Millisecond) // 6 half-lives: previous hit is ~gone
	}
	if e.indexed("slow.png") {
		t.Fatal("hits spread far beyond the half-life must not reach the threshold")
	}
}

func TestAggregatedEventCountsAsManyReads(t *testing.T) {
	e := newEnv(t, 1000)
	e.file(t, "agg.png", "aaa")
	e.start(t)
	ev := events.Event{Key: "agg.png", Size: 3, At: time.Now(), Count: 7}
	_ = e.rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: e.index.Keys().Events, Values: ev.Values()}).Err()
	eventually(t, "migrated from a single aggregated event", func() bool { return e.indexed("agg.png") })
	eventually(t, "7 hits recorded", func() bool { _, h := e.store.get("agg.png"); return h == 7 })
}

func TestHeatGCDropsColdEntries(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1000, func(c *config.SchedulerConfig) { c.HalfLife = 30 * time.Millisecond })
	old := time.Now().Add(-time.Hour)
	_ = e.rdb.HSet(ctx, e.index.Keys().Heat, "cold", encodeHeat(1, old), "warm", encodeHeat(1e9, time.Now())).Err()
	e.start(t)
	eventually(t, "cold heat removed", func() bool {
		ok, _ := e.rdb.HExists(ctx, e.index.Keys().Heat, "cold").Result()
		return !ok
	})
	if ok, _ := e.rdb.HExists(ctx, e.index.Keys().Heat, "warm").Result(); !ok {
		t.Fatal("hot entry must be kept")
	}
}

// --- eligibility ---

func TestSizeEligibility(t *testing.T) {
	e := newEnv(t, 50, func(c *config.SchedulerConfig) { c.MinSize = 3; c.MaxSize = 40 })
	files := map[string]int{"empty.png": 0, "tiny.png": 2, "ok.png": 10, "big.png": 41, "edge.png": 40}
	for k, n := range files {
		e.file(t, k, strings.Repeat("x", n))
	}
	e.start(t)
	for k, n := range files {
		e.hit(t, k, int64(n))
		e.hit(t, k, int64(n))
	}
	eventually(t, "eligible objects migrated", func() bool { return e.indexed("ok.png") && e.indexed("edge.png") })
	time.Sleep(100 * time.Millisecond)
	for _, k := range []string{"empty.png", "tiny.png", "big.png"} {
		if e.indexed(k) || e.s3.has(k) {
			t.Errorf("%s must stay local", k)
		}
	}
}

func TestObjectLargerThanCapacityNeverMigrates(t *testing.T) {
	e := newEnv(t, 10)
	e.file(t, "huge.png", strings.Repeat("x", 11))
	if err := e.sched.ensureIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.sched.migrate(context.Background(), "huge.png"); err != nil || e.s3.has("huge.png") {
		t.Fatalf("err=%v uploaded=%v", err, e.s3.has("huge.png"))
	}
}

func TestMissingLocalFileIsSkipped(t *testing.T) {
	e := newEnv(t, 100)
	_ = e.sched.ensureIndex(context.Background())
	if err := e.sched.migrate(context.Background(), "ghost.png"); err != nil {
		t.Fatalf("missing file must be a silent skip, got %v", err)
	}
}

// --- failure handling ---

func TestFileChangedDuringUploadIsRolledBack(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1000)
	e.file(t, "chg.png", "v1")
	_ = e.sched.ensureIndex(ctx)
	e.s3.onPut = func(string) {
		time.Sleep(10 * time.Millisecond)
		e.file(t, "chg.png", "version-2") // size + mtime change while uploading
	}
	err := e.sched.migrate(ctx, "chg.png")
	if err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("expected change detection, got %v", err)
	}
	if e.indexed("chg.png") || e.s3.has("chg.png") {
		t.Fatal("stale upload must be deleted and not indexed")
	}
	if st, _ := e.store.get("chg.png"); st == StatusS3 {
		t.Fatal("PG must not claim s3")
	}
}

func TestPGFailureDeletesUpload(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1000)
	e.file(t, "pg.png", "data")
	_ = e.sched.ensureIndex(ctx)
	e.store.markS3Err = errors.New("pg down")
	if err := e.sched.migrate(ctx, "pg.png"); err == nil {
		t.Fatal("expected error")
	}
	if e.indexed("pg.png") || e.s3.has("pg.png") {
		t.Fatal("upload must be cleaned up when PG commit fails")
	}
	if total, _ := e.index.TotalBytes(ctx); total != 0 {
		t.Fatalf("capacity reservation leaked: total=%d reserved=%d", total, e.sched.reserved)
	}
	if e.sched.reserved != 0 {
		t.Fatalf("reserved bytes leaked: %d", e.sched.reserved)
	}
}

func TestTrashDeleteFailureIsRetried(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1000, func(c *config.SchedulerConfig) { c.DeleteDelay = 10 * time.Millisecond })
	_ = e.sched.ensureIndex(ctx)
	e.s3.putAt("t.png", "x", time.Now())
	_, _ = e.index.Add(ctx, "t.png", 1, time.Now())
	if err := e.sched.evict(ctx, "t.png"); err != nil {
		t.Fatal(err)
	}
	e.s3.mu.Lock()
	e.s3.deleteErr = errors.New("bucket unavailable")
	e.s3.mu.Unlock()
	e.start(t)

	eventually(t, "delete rescheduled ~1 minute later", func() bool {
		score, err := e.rdb.ZScore(ctx, e.index.Keys().Trash, "t.png").Result()
		return err == nil && time.UnixMilli(int64(score)).After(time.Now().Add(50*time.Second))
	})
	if !e.s3.has("t.png") {
		t.Fatal("object must still exist after a failed delete")
	}
}

func TestEvictedObjectReMigratedCancelsPendingDelete(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1000, func(c *config.SchedulerConfig) { c.DeleteDelay = 300 * time.Millisecond })
	e.file(t, "back.png", "back")
	e.start(t)
	e.hit(t, "back.png", 4)
	e.hit(t, "back.png", 4)
	eventually(t, "migrated", func() bool { return e.indexed("back.png") })

	e.sched.capMu.Lock()
	_ = e.sched.evict(ctx, "back.png")
	e.sched.capMu.Unlock()
	e.hit(t, "back.png", 4) // still hot → comes straight back
	eventually(t, "re-migrated", func() bool { return e.indexed("back.png") })
	time.Sleep(500 * time.Millisecond) // past the original delete time
	if !e.s3.has("back.png") {
		t.Fatal("pending delete must be cancelled by the re-migration")
	}
}

// --- leader election ---

func TestElectorStepsDownWhenLockStolen(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	var leading atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewElector(rdb, "L", "me", 300*time.Millisecond).Run(ctx, func(lctx context.Context) {
		leading.Add(1)
		<-lctx.Done()
		leading.Add(-1)
	})
	eventually(t, "leading", func() bool { return leading.Load() == 1 })

	_ = mr.Set("L", "intruder") // someone else owns the lock now
	eventually(t, "stepped down", func() bool { return leading.Load() == 0 })
	if v, _ := mr.Get("L"); v != "intruder" {
		t.Fatalf("must not release a lock it doesn't own, got %q", v)
	}
}

func TestElectorStepsDownWhenRedisUnreachable(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	defer rdb.Close()

	var leading atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewElector(rdb, "L", "me", 300*time.Millisecond).Run(ctx, func(lctx context.Context) {
		leading.Add(1)
		<-lctx.Done()
		leading.Add(-1)
	})
	eventually(t, "leading", func() bool { return leading.Load() == 1 })
	mr.SetError("LOADING down")
	start := time.Now()
	eventually(t, "stepped down", func() bool { return leading.Load() == 0 })
	if d := time.Since(start); d > 600*time.Millisecond {
		t.Fatalf("stepped down after %s; must happen before the 300ms lock can expire elsewhere", d)
	}
}

// --- concurrency stress ---

// Random hot keys with a tiny capacity force continuous migrate/evict/delete churn.
// Invariants checked continuously and at the end:
//   - indexed bytes never exceed capacity
//   - the bytes counter equals the sum of indexed sizes
//   - every indexed key exists in the bucket; every bucket object is indexed or in the trash
//   - PG agrees with the index for every indexed key
func TestStressCapacityInvariantsUnderChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	ctx := context.Background()
	const capacity = 200
	e := newEnv(t, capacity, func(c *config.SchedulerConfig) {
		c.Threshold = 1
		c.Workers = 4
		c.EvictMinIdle = 0
		c.DeleteDelay = 5 * time.Millisecond
		c.EvictInterval = 5 * time.Millisecond
	})
	sizes := map[string]int{}
	for i := range 60 {
		k := fmt.Sprintf("o%02d.png", i)
		sizes[k] = 5 + i%40
		if err := os.WriteFile(filepath.Join(e.dir, k), []byte(strings.Repeat("x", sizes[k])), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	e.start(t)
	eventually(t, "index ready", func() bool { ok, _ := e.index.Initialized(ctx); return ok })

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var violations atomic.Int64
	var maxSeen atomic.Int64
	wg.Go(func() { // capacity sampler
		for {
			select {
			case <-stop:
				return
			default:
			}
			total, _ := e.index.TotalBytes(ctx)
			if total > maxSeen.Load() {
				maxSeen.Store(total)
			}
			if total > capacity {
				violations.Add(1)
			}
			time.Sleep(time.Millisecond)
		}
	})
	wg.Go(func() { // zipf-ish traffic
		for {
			select {
			case <-stop:
				return
			default:
			}
			i := int(math.Min(59, math.Abs(rand.NormFloat64())*15))
			k := fmt.Sprintf("o%02d.png", i)
			ev := events.Event{Key: k, Size: int64(sizes[k]), At: time.Now()}
			_ = e.rdb.XAdd(ctx, &redis.XAddArgs{Stream: e.index.Keys().Events, Values: ev.Values()}).Err()
			time.Sleep(200 * time.Microsecond)
		}
	})
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()
	time.Sleep(100 * time.Millisecond) // let in-flight work settle

	if v := violations.Load(); v > 0 {
		t.Fatalf("capacity exceeded in %d samples (max %d > %d)", v, maxSeen.Load(), capacity)
	}
	indexed, _ := e.index.All(ctx)
	var sum int64
	for k, n := range indexed {
		sum += n
		if !e.s3.has(k) {
			t.Errorf("indexed %s missing from bucket", k)
		}
		if st, _ := e.store.get(k); st != StatusS3 {
			t.Errorf("indexed %s has PG status %q", k, st)
		}
	}
	if total, _ := e.index.TotalBytes(ctx); total != sum {
		t.Errorf("bytes counter %d != sum of sizes %d", total, sum)
	}
	e.s3.mu.Lock()
	objects := make([]string, 0, len(e.s3.objects))
	for k := range e.s3.objects {
		objects = append(objects, k)
	}
	deletes := len(e.s3.deletes)
	e.s3.mu.Unlock()
	for _, k := range objects {
		if _, ok := indexed[k]; ok {
			continue
		}
		if trashed, _ := e.index.InTrash(ctx, k); !trashed && !e.sched.isBusy(k) {
			t.Errorf("bucket object %s is neither indexed nor scheduled for deletion", k)
		}
	}
	if deletes == 0 {
		t.Fatal("expected eviction churn")
	}
	t.Logf("max bytes %d/%d, indexed %d objects, %d S3 deletes", maxSeen.Load(), capacity, len(indexed), deletes)
}

func TestTrashBacklogDrainsConcurrently(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1<<30, func(c *config.SchedulerConfig) { c.DeleteDelay = 10 * time.Millisecond })
	_ = e.sched.ensureIndex(ctx)
	e.s3.deleteDelay = 20 * time.Millisecond // ~B2 latency; sequential would take 24s
	past := time.Now().Add(-time.Second)
	for i := range 1200 {
		k := fmt.Sprintf("t%04d", i)
		e.s3.putAt(k, "x", past)
		_ = e.index.ScheduleDelete(ctx, k, past)
	}
	start := time.Now()
	e.start(t)
	eventually(t, "backlog drained", func() bool {
		n, _ := e.rdb.ZCard(ctx, e.index.Keys().Trash).Result()
		e.s3.mu.Lock()
		left := len(e.s3.objects)
		e.s3.mu.Unlock()
		return n == 0 && left == 0
	})
	t.Logf("1200 deletes drained in %s", time.Since(start).Round(time.Millisecond))
}

func TestOrphanScanAcrossBatches(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1<<30, func(c *config.SchedulerConfig) { c.ReconcileOrphans = "delete" })
	_ = e.sched.ensureIndex(ctx)
	old := time.Now().Add(-2 * time.Hour)
	for i := range 2500 { // spans 3 listing batches
		k := fmt.Sprintf("o%04d", i)
		e.s3.putAt(k, "x", old)
		switch {
		case i%500 == 7: // orphan
		case i%500 == 9: // evicted, awaiting delete
			_ = e.index.ScheduleDelete(ctx, k, time.Now().Add(time.Hour))
		default:
			e.store.set(k, StatusS3, 1)
			_, _ = e.index.Add(ctx, k, 1, time.Now())
		}
	}
	if err := e.sched.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range 2500 {
		k := fmt.Sprintf("o%04d", i)
		if want := i%500 != 7; e.s3.has(k) != want {
			t.Errorf("%s: present=%v, want %v", k, e.s3.has(k), want)
		}
	}
}
