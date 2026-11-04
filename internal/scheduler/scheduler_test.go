package scheduler

import (
	"context"
	"io"
	"os"
	"path/filepath"
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
	mu      sync.Mutex
	objects map[string]string
	deletes []string
}

func (f *fakeS3) Put(_ context.Context, key string, body io.ReaderAt, size int64, _ string) error {
	b, err := io.ReadAll(io.NewSectionReader(body, 0, size))
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.objects[key] = string(b)
	f.mu.Unlock()
	return nil
}

func (f *fakeS3) Delete(_ context.Context, key string) error {
	f.mu.Lock()
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
	mu     sync.Mutex
	status map[string]string
	hits   map[string]int64
	seed   []state.Entry
}

func (f *fakeStore) markS3(_ context.Context, key, _ string, _ int64, _ time.Time) error {
	f.mu.Lock()
	f.status[key] = StatusS3
	f.mu.Unlock()
	return nil
}

func (f *fakeStore) markLocal(_ context.Context, key string, _ time.Time) error {
	f.mu.Lock()
	f.status[key] = StatusLocal
	f.mu.Unlock()
	return nil
}

func (f *fakeStore) listS3(context.Context) ([]state.Entry, error) { return f.seed, nil }

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
		s3:    &fakeS3{objects: map[string]string{}},
		store: &fakeStore{status: map[string]string{}, hits: map[string]int64{}},
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
	}
	for _, o := range opts {
		o(&cfg)
	}
	e.sched = New(cfg, "test", rdb, e.index, local, e.s3, e.store)
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
	e.store.seed = []state.Entry{
		{Key: "old", Size: 8, LastAccess: now.Add(-time.Hour)},
		{Key: "new", Size: 8, LastAccess: now},
	}
	e.start(t)
	eventually(t, "old evicted", func() bool { return !e.indexed("old") && e.indexed("new") })
}

func TestStaleEntryRemoved(t *testing.T) {
	e := newEnv(t, 100)
	e.start(t)
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
