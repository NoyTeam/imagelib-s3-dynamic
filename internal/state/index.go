// Package state holds the Redis-backed shared state between proxies and the scheduler.
//
// Redis layout (all keys under the configured prefix):
//
//	s3:objects  HASH   key -> size   membership: object is served from S3
//	s3:lru      ZSET   key -> last access (unix ms)
//	s3:bytes    STRING total bytes of indexed objects
//	s3:trash    ZSET   key -> delete-after (unix ms), objects de-indexed but not yet deleted
//	heat        HASH   key -> "score|unixms" decayed hit score
//	events      STREAM origin (local) read events
//	leader      STRING scheduler leader lock
package state

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Keys struct {
	Objects string
	LRU     string
	Bytes   string
	Trash   string
	Heat    string
	Events  string
	Leader  string
}

func NewKeys(prefix string) Keys {
	return Keys{
		Objects: prefix + "s3:objects",
		LRU:     prefix + "s3:lru",
		Bytes:   prefix + "s3:bytes",
		Trash:   prefix + "s3:trash",
		Heat:    prefix + "heat",
		Events:  prefix + "events",
		Leader:  prefix + "leader",
	}
}

// Index is the authoritative-for-serving set of objects that currently live in S3.
// PostgreSQL holds the durable copy; the index can be rebuilt from it.
type Index struct {
	rdb  redis.UniversalClient
	keys Keys
}

func NewIndex(rdb redis.UniversalClient, keys Keys) *Index {
	return &Index{rdb: rdb, keys: keys}
}

func (i *Index) Keys() Keys { return i.keys }

// Contains reports whether key is served from S3. This is the hot-path lookup.
func (i *Index) Contains(ctx context.Context, key string) (bool, error) {
	n, err := i.rdb.HExists(ctx, i.keys.Objects, key).Result()
	return n, err
}

var addScript = redis.NewScript(`
if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
  redis.call('INCRBY', KEYS[3], ARGV[2])
end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
return tonumber(redis.call('GET', KEYS[3]) or '0')
`)

// Add atomically indexes key with size and returns the new total bytes.
func (i *Index) Add(ctx context.Context, key string, size int64, at time.Time) (int64, error) {
	return addScript.Run(ctx, i.rdb, []string{i.keys.Objects, i.keys.LRU, i.keys.Bytes},
		key, size, at.UnixMilli()).Int64()
}

var removeScript = redis.NewScript(`
local s = redis.call('HGET', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if not s then return -1 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('DECRBY', KEYS[3], s)
return tonumber(s)
`)

// Remove atomically de-indexes key. It returns the size that was freed, or -1 if absent.
func (i *Index) Remove(ctx context.Context, key string) (int64, error) {
	return removeScript.Run(ctx, i.rdb, []string{i.keys.Objects, i.keys.LRU, i.keys.Bytes}, key).Int64()
}

// TotalBytes returns the indexed size total.
func (i *Index) TotalBytes(ctx context.Context) (int64, error) {
	n, err := i.rdb.Get(ctx, i.keys.Bytes).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return n, err
}

// Initialized reports whether the index has been built (the bytes counter exists).
func (i *Index) Initialized(ctx context.Context) (bool, error) {
	n, err := i.rdb.Exists(ctx, i.keys.Bytes).Result()
	return n == 1, err
}

// Oldest returns up to n least recently used keys.
func (i *Index) Oldest(ctx context.Context, n int64) ([]string, error) {
	return i.rdb.ZRange(ctx, i.keys.LRU, 0, n-1).Result()
}

// LRUEntry is a key with its last access time.
type LRUEntry struct {
	Key        string
	LastAccess time.Time
}

// OldestEntries returns up to n least recently used keys with their last access time.
func (i *Index) OldestEntries(ctx context.Context, n int64) ([]LRUEntry, error) {
	zs, err := i.rdb.ZRangeWithScores(ctx, i.keys.LRU, 0, n-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]LRUEntry, len(zs))
	for j, z := range zs {
		key, _ := z.Member.(string)
		out[j] = LRUEntry{Key: key, LastAccess: time.UnixMilli(int64(z.Score))}
	}
	return out, nil
}

// Touch bumps LRU access time for keys that are still indexed (XX) and never moves time backwards (GT).
func (i *Index) Touch(ctx context.Context, keys []string, at time.Time) error {
	if len(keys) == 0 {
		return nil
	}
	members := make([]redis.Z, len(keys))
	score := float64(at.UnixMilli())
	for j, k := range keys {
		members[j] = redis.Z{Score: score, Member: k}
	}
	return i.rdb.ZAddArgs(ctx, i.keys.LRU, redis.ZAddArgs{XX: true, GT: true, Members: members}).Err()
}

// Entry is one indexed object used for rebuilding.
type Entry struct {
	Key        string
	Size       int64
	LastAccess time.Time
}

// Rebuild replaces the index content with entries.
func (i *Index) Rebuild(ctx context.Context, entries []Entry) error {
	var total int64
	_, err := i.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, i.keys.Objects, i.keys.LRU)
		for _, e := range entries {
			p.HSet(ctx, i.keys.Objects, e.Key, e.Size)
			p.ZAdd(ctx, i.keys.LRU, redis.Z{Score: float64(e.LastAccess.UnixMilli()), Member: e.Key})
			total += e.Size
		}
		p.Set(ctx, i.keys.Bytes, strconv.FormatInt(total, 10), 0)
		return nil
	})
	return err
}

// ScheduleDelete records that key must be deleted from S3 after due.
func (i *Index) ScheduleDelete(ctx context.Context, key string, due time.Time) error {
	return i.rdb.ZAdd(ctx, i.keys.Trash, redis.Z{Score: float64(due.UnixMilli()), Member: key}).Err()
}

// CancelDelete removes key from the trash. It returns true if a pending delete was cancelled.
func (i *Index) CancelDelete(ctx context.Context, key string) (bool, error) {
	n, err := i.rdb.ZRem(ctx, i.keys.Trash, key).Result()
	return n == 1, err
}

// DueDeletes returns up to n keys whose delete time has passed.
func (i *Index) DueDeletes(ctx context.Context, now time.Time, n int64) ([]string, error) {
	return i.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key: i.keys.Trash, Start: "-inf", Stop: strconv.FormatInt(now.UnixMilli(), 10),
		ByScore: true, Count: n,
	}).Result()
}
