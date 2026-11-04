package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

func TestIndexRecountPrimitives(t *testing.T) {
	ctx := context.Background()
	idx, mr := newTestIndex(t)
	_, _ = idx.Add(ctx, "a", 10, time.Now())
	_, _ = idx.Add(ctx, "b", 5, time.Now())
	_ = mr.Set("t:s3:bytes", "999") // drift

	sum, err := idx.SumSizes(ctx)
	if err != nil || sum != 15 {
		t.Fatalf("sum = %d, %v", sum, err)
	}
	if err := idx.SetBytes(ctx, sum); err != nil {
		t.Fatal(err)
	}
	if total, _ := idx.TotalBytes(ctx); total != 15 {
		t.Fatalf("total = %d", total)
	}
	mr.Del("t:s3:epoch")
	if err := idx.SetBytes(ctx, 1); err != ErrNotInitialized {
		t.Fatalf("SetBytes on lost index: %v", err)
	}
}

func TestRebuildFromPagesSwapsAtomically(t *testing.T) {
	ctx := context.Background()
	idx, mr := newTestIndex(t)
	_, _ = idx.Add(ctx, "old", 999, time.Now())

	now := time.Now()
	pages := [][]Entry{}
	for p := range 5 { // 5 pages × 2500 entries: crosses the internal 1000-entry command batches
		var page []Entry
		for i := range 2500 {
			page = append(page, Entry{Key: fmt.Sprintf("k%d-%d", p, i), Size: 2, LastAccess: now.Add(time.Duration(i) * time.Millisecond)})
		}
		pages = append(pages, page)
	}
	calls := 0
	err := idx.RebuildFrom(ctx, func(yield func([]Entry) error) error {
		for _, p := range pages {
			calls++
			if calls == 3 {
				// Mid-rebuild, readers still see the previous index.
				if ok, _ := idx.Contains(ctx, "old"); !ok {
					t.Error("old index must stay visible until the swap")
				}
			}
			if err := yield(p); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := idx.Contains(ctx, "old"); ok {
		t.Fatal("old entries must be gone after the swap")
	}
	if total, _ := idx.TotalBytes(ctx); total != 5*2500*2 {
		t.Fatalf("total = %d", total)
	}
	if m, _ := mr.ZMembers("t:s3:lru"); len(m) != 12500 {
		t.Fatalf("lru size = %d", len(m))
	}
	for _, k := range mr.Keys() {
		if strings.HasSuffix(k, ":rebuild") {
			t.Fatalf("temporary key %s left behind", k)
		}
	}

	// Empty source → empty but initialized index.
	if err := idx.RebuildFrom(ctx, func(func([]Entry) error) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if ok, _ := idx.Initialized(ctx); !ok {
		t.Fatal("empty rebuild must still initialize the index")
	}
	if total, _ := idx.TotalBytes(ctx); total != 0 {
		t.Fatalf("total = %d", total)
	}

	// A failing source leaves the previous index untouched.
	_, _ = idx.Add(ctx, "keep", 1, now)
	boom := errors.New("pg down")
	if err := idx.RebuildFrom(ctx, func(yield func([]Entry) error) error {
		_ = yield([]Entry{{Key: "partial", Size: 1}})
		return boom
	}); err != boom {
		t.Fatalf("err = %v", err)
	}
	if ok, _ := idx.Contains(ctx, "keep"); !ok {
		t.Fatal("failed rebuild must not replace the index")
	}
	if ok, _ := idx.Contains(ctx, "partial"); ok {
		t.Fatal("failed rebuild must not leak partial data")
	}
}

func TestBatchLookups(t *testing.T) {
	ctx := context.Background()
	idx, _ := newTestIndex(t)
	_, _ = idx.Add(ctx, "a", 7, time.Now())
	_ = idx.ScheduleDelete(ctx, "t1", time.Now())

	sizes, err := idx.Sizes(ctx, []string{"a", "missing"})
	if err != nil || sizes[0] != 7 || sizes[1] != -1 {
		t.Fatalf("sizes = %v, %v", sizes, err)
	}
	trashed, err := idx.TrashedAmong(ctx, []string{"t1", "a", "nope"})
	if err != nil || !trashed["t1"] || trashed["a"] || trashed["nope"] {
		t.Fatalf("trashed = %v, %v", trashed, err)
	}

	for i := range 2500 {
		_, _ = idx.Add(ctx, fmt.Sprintf("s%d", i), 1, time.Now())
	}
	seen := map[string]bool{}
	if err := idx.ScanKeys(ctx, func(keys []string) error {
		for _, k := range keys {
			seen[k] = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2501 {
		t.Fatalf("scanned %d keys, want 2501", len(seen))
	}
}
