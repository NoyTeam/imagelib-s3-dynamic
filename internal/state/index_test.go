package state

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestIndex(t *testing.T) (*Index, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	idx := NewIndex(rdb, NewKeys("t:"))
	if err := idx.Rebuild(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	return idx, mr
}

func TestIndexAddRemoveAccounting(t *testing.T) {
	ctx := context.Background()
	idx, _ := newTestIndex(t)
	now := time.Now()

	if total, err := idx.Add(ctx, "a", 100, now); err != nil || total != 100 {
		t.Fatalf("add a: total=%d err=%v", total, err)
	}
	// Re-adding must not double count.
	if total, _ := idx.Add(ctx, "a", 100, now); total != 100 {
		t.Fatalf("re-add a: total=%d", total)
	}
	if total, _ := idx.Add(ctx, "b", 50, now.Add(time.Second)); total != 150 {
		t.Fatalf("add b: total=%d", total)
	}
	if ok, _ := idx.Contains(ctx, "a"); !ok {
		t.Fatal("a should be indexed")
	}

	oldest, _ := idx.Oldest(ctx, 1)
	if len(oldest) != 1 || oldest[0] != "a" {
		t.Fatalf("oldest = %v, want [a]", oldest)
	}
	// Touch a → b becomes the LRU victim. Touching a missing key must not add it.
	if err := idx.Touch(ctx, []string{"a", "ghost"}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	oldest, _ = idx.Oldest(ctx, 10)
	if len(oldest) != 2 || oldest[0] != "b" {
		t.Fatalf("oldest after touch = %v, want [b a]", oldest)
	}

	if freed, _ := idx.Remove(ctx, "b"); freed != 50 {
		t.Fatalf("remove b freed %d", freed)
	}
	if freed, _ := idx.Remove(ctx, "b"); freed != -1 {
		t.Fatalf("second remove b freed %d, want -1", freed)
	}
	if total, _ := idx.TotalBytes(ctx); total != 100 {
		t.Fatalf("total = %d, want 100", total)
	}
}

func TestIndexRebuildAndTrash(t *testing.T) {
	ctx := context.Background()
	idx, _ := newTestIndex(t)
	now := time.Now()

	if ok, _ := idx.Initialized(ctx); !ok {
		t.Fatal("rebuilt index should be initialized")
	}
	_, _ = idx.Add(ctx, "stale", 999, now)
	if err := idx.Rebuild(ctx, []Entry{{Key: "x", Size: 10, LastAccess: now}, {Key: "y", Size: 5, LastAccess: now}}); err != nil {
		t.Fatal(err)
	}
	if total, _ := idx.TotalBytes(ctx); total != 15 {
		t.Fatalf("total = %d, want 15", total)
	}
	if ok, _ := idx.Contains(ctx, "stale"); ok {
		t.Fatal("rebuild should drop old entries")
	}

	_ = idx.ScheduleDelete(ctx, "x", now.Add(-time.Second))
	_ = idx.ScheduleDelete(ctx, "y", now.Add(time.Hour))
	due, _ := idx.DueDeletes(ctx, now, 10)
	if len(due) != 1 || due[0] != "x" {
		t.Fatalf("due = %v", due)
	}
	if ok, _ := idx.CancelDelete(ctx, "x"); !ok {
		t.Fatal("cancel x should succeed")
	}
	if ok, _ := idx.CancelDelete(ctx, "x"); ok {
		t.Fatal("second cancel x should be a no-op")
	}
}

func TestIndexLostRefusesWrites(t *testing.T) {
	ctx := context.Background()
	idx, mr := newTestIndex(t)
	_, _ = idx.Add(ctx, "a", 10, time.Now())

	mr.FlushAll() // Redis lost its data
	if ok, _ := idx.Initialized(ctx); ok {
		t.Fatal("flushed index must report not initialized")
	}
	if _, err := idx.Add(ctx, "b", 5, time.Now()); err != ErrNotInitialized {
		t.Fatalf("add on lost index: %v", err)
	}
	if _, err := idx.Remove(ctx, "a"); err != ErrNotInitialized {
		t.Fatalf("remove on lost index: %v", err)
	}
	if mr.Exists("t:s3:bytes") {
		t.Fatal("a refused write must not resurrect the bytes counter")
	}
}

func TestIndexRecount(t *testing.T) {
	ctx := context.Background()
	idx, mr := newTestIndex(t)
	_, _ = idx.Add(ctx, "a", 10, time.Now())
	_, _ = idx.Add(ctx, "b", 5, time.Now())
	_ = mr.Set("t:s3:bytes", "999") // drift

	before, after, err := idx.Recount(ctx)
	if err != nil || before != 999 || after != 15 {
		t.Fatalf("recount = %d → %d, %v", before, after, err)
	}
	if total, _ := idx.TotalBytes(ctx); total != 15 {
		t.Fatalf("total = %d", total)
	}
}
