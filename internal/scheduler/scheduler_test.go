package scheduler

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/events"
	"imagelib-s3-dynamic/internal/state"
	"imagelib-s3-dynamic/internal/storage"
)

type fakeS3 struct {
	onPut       func(key string) // test hook, runs after the body was read
	deleteErr   error            // if set, Delete fails
	deleteDelay time.Duration    // simulated per-delete latency
	mu          sync.Mutex
	objects     map[string]string
	modified    map[string]time.Time
	deletes     []string
}

func (f *fakeS3) Put(_ context.Context, key string, body io.ReaderAt, size int64, _ string) error {
	b, err := io.ReadAll(io.NewSectionReader(body, 0, size))
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.objects[key] = string(b)
	f.modified[key] = time.Now()
	f.mu.Unlock()
	if f.onPut != nil {
		f.onPut(key)
	}
	return nil
}

func (f *fakeS3) List(_ context.Context, fn func(string, int64, time.Time) error) error {
	f.mu.Lock()
	type obj struct {
		k string
		n int64
		m time.Time
	}
	var objs []obj
	for k, v := range f.objects {
		objs = append(objs, obj{k, int64(len(v)), f.modified[k]})
	}
	f.mu.Unlock()
	for _, o := range objs {
		if err := fn(o.k, o.n, o.m); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeS3) putAt(key, body string, at time.Time) {
	f.mu.Lock()
	f.objects[key] = body
	f.modified[key] = at
	f.mu.Unlock()
}

func (f *fakeS3) Delete(_ context.Context, key string) error {
	time.Sleep(f.deleteDelay)
	f.mu.Lock()
	if f.deleteErr != nil {
		f.mu.Unlock()
		return f.deleteErr
	}
	delete(f.objects, key)
	f.deletes = append(f.deletes, key)
	f.mu.Unlock()
	return nil
}

func (f *fakeS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

type fakeStore struct {
	onMarkS3  func(key string) // test hook, runs before the write
	markS3Err error
	mu        sync.Mutex
	status    map[string]string
	sizes     map[string]int64
	hits      map[string]int64
}

func (f *fakeStore) set(key, status string, size int64) {
	f.mu.Lock()
	f.status[key] = status
	f.sizes[key] = size
	f.mu.Unlock()
}

func (f *fakeStore) markS3(_ context.Context, key, _ string, size int64, _ time.Time) error {
	if f.onMarkS3 != nil {
		f.onMarkS3(key)
	}
	if f.markS3Err != nil {
		return f.markS3Err
	}
	f.set(key, StatusS3, size)
	return nil
}

func (f *fakeStore) find(_ context.Context, key string) (*row, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.status[key]
	if !ok {
		return nil, nil
	}
	return &row{Status: st, Size: f.sizes[key], LastAccess: time.Now().Add(-time.Hour)}, nil
}

func (f *fakeStore) markLocal(_ context.Context, key string, _ time.Time) error {
	f.mu.Lock()
	f.status[key] = StatusLocal
	f.mu.Unlock()
	return nil
}

// fakePage is tiny so tests exercise multi-page streaming.
const fakePage = 3

func (f *fakeStore) forEachS3(_ context.Context, fn func([]state.Entry) error) error {
	f.mu.Lock()
	var all []state.Entry
	for k, st := range f.status {
		if st == StatusS3 {
			all = append(all, state.Entry{Key: k, Size: f.sizes[k], LastAccess: time.Now().Add(-time.Hour)})
		}
	}
	f.mu.Unlock()
	slices.SortFunc(all, func(a, b state.Entry) int { return strings.Compare(a.Key, b.Key) })
	for start := 0; start < len(all); start += fakePage {
		if err := fn(all[start:min(start+fakePage, len(all))]); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeStore) filterS3(_ context.Context, keys []string) (map[string]struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]struct{}{}
	for _, k := range keys {
		if f.status[k] == StatusS3 {
			out[k] = struct{}{}
		}
	}
	return out, nil
}

func (f *fakeStore) flushStats(_ context.Context, rows map[string]*statRow) error {
	f.mu.Lock()
	for k, r := range rows {
		f.hits[k] += r.hits
	}
	f.mu.Unlock()
	return nil
}

func (f *fakeStore) get(key string) (string, int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status[key], f.hits[key]
}

type env struct {
	rdb   *redis.Client
	index *state.Index
	s3    *fakeS3
	store *fakeStore
	dir   string
	sched *Scheduler
}

func newEnv(t *testing.T, capacity int64, opts ...func(*config.SchedulerConfig)) *env {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	dir := t.TempDir()
	local, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { local.Close() })

	e := &env{
		rdb:   rdb,
		index: state.NewIndex(rdb, state.NewKeys("t:")),
		s3:    &fakeS3{objects: map[string]string{}, modified: map[string]time.Time{}},
		store: &fakeStore{status: map[string]string{}, sizes: map[string]int64{}, hits: map[string]int64{}},
		dir:   dir,
	}
	cfg := config.SchedulerConfig{
		CapacityBytes:      capacity,
		Threshold:          2,
		HalfLife:           time.Hour,
		Workers:            2,
		MaxSize:            1 << 20,
		EvictInterval:      20 * time.Millisecond,
		DeleteDelay:        50 * time.Millisecond,
		StatsFlushInterval: 20 * time.Millisecond,
		LeaderTTL:          time.Second,
		RetryBackoff:       time.Minute,
		IndexCheckInterval: 20 * time.Millisecond,
		ReconcileOrphans:   "log",
		OrphanMinAge:       time.Hour,
	}
	for _, o := range opts {
		o(&cfg)
	}
	e.sched = New(cfg, "test", rdb, e.index, local, e.s3, e.store)
	e.sched.readBlock = 50 * time.Millisecond
	return e
}

func (e *env) start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.sched.Lead(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
}

func (e *env) file(t *testing.T, key, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(e.dir, key), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (e *env) hit(t *testing.T, key string, size int64) {
	t.Helper()
	ev := events.Event{Key: key, Size: size, At: time.Now()}
	if err := e.rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: e.index.Keys().Events, Values: ev.Values()}).Err(); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (e *env) indexed(key string) bool {
	ok, _ := e.index.Contains(context.Background(), key)
	return ok
}

func TestHotObjectMigrates(t *testing.T) {
	e := newEnv(t, 1<<20)
	e.file(t, "hot.jpg", "hot!")
	e.file(t, "cold.jpg", "cold")
	e.start(t)

	e.hit(t, "cold.jpg", 4) // below threshold (2)
	e.hit(t, "hot.jpg", 4)
	e.hit(t, "hot.jpg", 4)

	eventually(t, "hot.jpg migrated", func() bool { return e.indexed("hot.jpg") })
	if !e.s3.has("hot.jpg") {
		t.Fatal("object not uploaded")
	}
	eventually(t, "stats flushed", func() bool {
		st, hits := e.store.get("hot.jpg")
		return st == StatusS3 && hits == 2
	})
	time.Sleep(50 * time.Millisecond)
	if e.indexed("cold.jpg") || e.s3.has("cold.jpg") {
		t.Fatal("cold object must stay local")
	}
	if total, _ := e.index.TotalBytes(context.Background()); total != 4 {
		t.Fatalf("total = %d", total)
	}
}

func TestCapacityEvictsLRU(t *testing.T) {
	e := newEnv(t, 10) // room for one 6-byte object only
	e.file(t, "a.png", "aaaaaa")
	e.file(t, "b.png", "bbbbbb")
	e.start(t)

	e.hit(t, "a.png", 6)
	e.hit(t, "a.png", 6)
	eventually(t, "a migrated", func() bool { return e.indexed("a.png") })

	e.hit(t, "b.png", 6)
	e.hit(t, "b.png", 6)
	eventually(t, "b migrated", func() bool { return e.indexed("b.png") })

	if e.indexed("a.png") {
		t.Fatal("a should have been evicted to make room for b")
	}
	if st, _ := e.store.get("a.png"); st != StatusLocal {
		t.Fatalf("a status = %q", st)
	}
	if total, _ := e.index.TotalBytes(context.Background()); total > 10 {
		t.Fatalf("capacity exceeded: %d", total)
	}
	// Deletion happens after the grace period.
	eventually(t, "a deleted from s3", func() bool { return !e.s3.has("a.png") })
	if !e.s3.has("b.png") {
		t.Fatal("b must remain in s3")
	}
}

func TestRecentObjectsNotEvictedForAdmission(t *testing.T) {
	e := newEnv(t, 10, func(c *config.SchedulerConfig) { c.EvictMinIdle = time.Hour })
	e.file(t, "a.png", "aaaaaa")
	e.file(t, "b.png", "bbbbbb")
	e.start(t)

	e.hit(t, "a.png", 6)
	e.hit(t, "a.png", 6)
	eventually(t, "a migrated", func() bool { return e.indexed("a.png") })

	for range 5 {
		e.hit(t, "b.png", 6)
	}
	time.Sleep(200 * time.Millisecond)
	if !e.indexed("a.png") || e.indexed("b.png") {
		t.Fatal("recently used a must not be evicted to admit b")
	}
	e.sched.busyMu.Lock()
	_, backingOff := e.sched.backoff["b.png"]
	e.sched.busyMu.Unlock()
	if !backingOff {
		t.Fatal("b should be backing off")
	}
}

func TestCapacityShrinkEnforced(t *testing.T) {
	e := newEnv(t, 10)
	now := time.Now()
	// Index built from PG with more than capacity (e.g. S3_CAPACITY was lowered).
	_ = e.index.Rebuild(context.Background(), []state.Entry{
		{Key: "old", Size: 8, LastAccess: now.Add(-time.Hour)},
		{Key: "new", Size: 8, LastAccess: now},
	})
	e.start(t)
	eventually(t, "old evicted", func() bool { return !e.indexed("old") && e.indexed("new") })
}

func TestStaleEntryRemoved(t *testing.T) {
	e := newEnv(t, 100)
	e.start(t)
	eventually(t, "index ready", func() bool { ok, _ := e.index.Initialized(context.Background()); return ok })
	_, _ = e.index.Add(context.Background(), "gone.png", 5, time.Now())
	ev := events.Event{Key: "gone.png", At: time.Now(), Stale: true}
	_ = e.rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: e.index.Keys().Events, Values: ev.Values()}).Err()
	eventually(t, "stale removed", func() bool { return !e.indexed("gone.png") })
}

func TestElectorSingleLeader(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	var mu sync.Mutex
	active, maxActive, leads := 0, 0, map[string]int{}
	lead := func(id string) func(context.Context) {
		return func(ctx context.Context) {
			mu.Lock()
			active++
			leads[id]++
			maxActive = max(maxActive, active)
			mu.Unlock()
			<-ctx.Done()
			mu.Lock()
			active--
			mu.Unlock()
		}
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done1 := make(chan struct{})
	go func() { defer close(done1); NewElector(rdb, "L", "one", 300*time.Millisecond).Run(ctx1, lead("one")) }()
	time.Sleep(50 * time.Millisecond)
	go NewElector(rdb, "L", "two", 300*time.Millisecond).Run(ctx2, lead("two"))

	time.Sleep(300 * time.Millisecond)
	cancel1() // leader one shuts down and releases the lock
	<-done1
	eventually(t, "two takes over", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return leads["two"] == 1
	})
	mu.Lock()
	defer mu.Unlock()
	if maxActive != 1 || leads["one"] != 1 {
		t.Fatalf("maxActive=%d leads=%v", maxActive, leads)
	}
}

func TestIndexLossRebuiltWhileLeading(t *testing.T) {
	e := newEnv(t, 100)
	e.store.set("kept.png", StatusS3, 7)
	e.start(t)
	eventually(t, "initial build", func() bool { return e.indexed("kept.png") })

	e.rdb.FlushAll(context.Background()) // Redis restarted without persistence
	eventually(t, "rebuilt after loss", func() bool { return e.indexed("kept.png") })
	if total, _ := e.index.TotalBytes(context.Background()); total != 7 {
		t.Fatalf("total = %d", total)
	}
}

func TestReconcile(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1000)
	now := time.Now()
	_ = e.index.Rebuild(ctx, nil)

	// PG says s3, index lost the entry → restored.
	e.store.set("restore.png", StatusS3, 5)
	e.s3.putAt("restore.png", "12345", now.Add(-2*time.Hour))
	// Index has it, PG says local → dropped and queued for deletion.
	e.store.set("drop.png", StatusLocal, 3)
	_, _ = e.index.Add(ctx, "drop.png", 3, now)
	e.s3.putAt("drop.png", "abc", now.Add(-2*time.Hour))
	// Eviction de-indexed + trashed it but PG update failed → finish eviction.
	e.store.set("evicting.png", StatusS3, 4)
	_ = e.index.ScheduleDelete(ctx, "evicting.png", now.Add(time.Hour))
	e.s3.putAt("evicting.png", "abcd", now.Add(-2*time.Hour))
	// Consistent object.
	e.store.set("ok.png", StatusS3, 2)
	_, _ = e.index.Add(ctx, "ok.png", 2, now)
	e.s3.putAt("ok.png", "ok", now.Add(-2*time.Hour))
	// Orphans: old one is reported, young one is ignored (upload may be committing).
	e.s3.putAt("orphan-old.png", "zz", now.Add(-2*time.Hour))
	e.s3.putAt("orphan-young.png", "zz", now)
	// Counter drift.
	_ = e.rdb.Set(ctx, e.index.Keys().Bytes, 999, 0).Err()

	if err := e.sched.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if !e.indexed("restore.png") {
		t.Error("restore.png should be restored")
	}
	if e.indexed("drop.png") {
		t.Error("drop.png should be dropped")
	}
	if in, _ := e.index.InTrash(ctx, "drop.png"); !in {
		t.Error("drop.png should be scheduled for deletion")
	}
	if st, _ := e.store.get("evicting.png"); st != StatusLocal || e.indexed("evicting.png") {
		t.Errorf("evicting.png: status %s indexed %v", st, e.indexed("evicting.png"))
	}
	if total, _ := e.index.TotalBytes(ctx); total != 5+2 {
		t.Errorf("total = %d, want 7", total)
	}
	if !e.s3.has("orphan-old.png") {
		t.Error("log mode must not delete orphans")
	}

	// Delete mode removes only the old orphan; trashed/indexed objects are untouched.
	e.sched.cfg.ReconcileOrphans = "delete"
	if err := e.sched.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if e.s3.has("orphan-old.png") {
		t.Error("old orphan should be deleted")
	}
	for _, k := range []string{"orphan-young.png", "restore.png", "ok.png", "drop.png", "evicting.png"} {
		if !e.s3.has(k) {
			t.Errorf("%s must not be deleted by orphan cleanup", k)
		}
	}
}

func TestNoMigrationWhileIndexLost(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1000)
	e.file(t, "x.png", "xx")
	_ = e.index.Rebuild(ctx, nil)
	e.rdb.Del(ctx, e.index.Keys().Epoch)

	if err := e.sched.migrate(ctx, "x.png"); err != nil {
		t.Fatal(err)
	}
	if e.s3.has("x.png") {
		t.Fatal("must not upload while the index is lost")
	}
}

func TestIndexLostMidCommitKeepsObject(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1000)
	e.file(t, "y.png", "yyyy")
	_ = e.index.Rebuild(ctx, nil)
	// Redis loses the index between the PG commit and the index write.
	e.store.onMarkS3 = func(string) { e.rdb.Del(ctx, e.index.Keys().Epoch) }

	if err := e.sched.migrate(ctx, "y.png"); err != nil {
		t.Fatal(err)
	}
	if st, _ := e.store.get("y.png"); st != StatusS3 || !e.s3.has("y.png") {
		t.Fatalf("PG status %q, in s3 %v: must be kept for the rebuild", st, e.s3.has("y.png"))
	}
	if err := e.sched.rebuildIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if !e.indexed("y.png") {
		t.Fatal("rebuild must restore y.png")
	}
}

func TestRecoversFromFlushAll(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, 1000)
	e.file(t, "z.png", "zz")
	e.start(t)
	eventually(t, "index ready", func() bool { ok, _ := e.index.Initialized(ctx); return ok })

	e.rdb.FlushAll(ctx) // index, heat, stream and consumer group all gone
	eventually(t, "index rebuilt", func() bool { ok, _ := e.index.Initialized(ctx); return ok })
	e.hit(t, "z.png", 2)
	e.hit(t, "z.png", 2)
	eventually(t, "consumer recovered and migrated", func() bool { return e.indexed("z.png") })
}
