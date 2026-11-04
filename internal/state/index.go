// Package state holds the Redis-backed shared state between proxies and the scheduler.
//
// Redis layout (all keys under the configured prefix):
//
//	s3:objects  HASH   key -> size   membership: object is served from S3
//	s3:lru      ZSET   key -> last access (unix ms)
//	s3:bytes    STRING total bytes of indexed objects
//	s3:epoch    STRING set when the index is (re)built; absent ⇒ index lost, must rebuild
//	s3:trash    ZSET   key -> delete-after (unix ms), objects de-indexed but not yet deleted
//	heat        HASH   key -> "score|unixms" decayed hit score
//	events      STREAM origin (local) read events
//	leader      STRING scheduler leader lock
package state

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Keys struct {
	Objects string
	LRU     string
	Bytes   string
	Epoch   string
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
		Epoch:   prefix + "s3:epoch",
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

// ErrNotInitialized is returned by writes while the index is lost (e.g. Redis was flushed).
// Writing then would make a partial index look valid; the leader rebuilds it from PG instead.
var ErrNotInitialized = errors.New("s3 index not initialized")

func mapScriptErr(err error) error {
	if err != nil && strings.Contains(err.Error(), "NOINDEX") {
		return ErrNotInitialized
	}
	return err
}

var addScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[4]) == 0 then return redis.error_reply('NOINDEX') end
if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
  redis.call('INCRBY', KEYS[3], ARGV[2])
end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
return tonumber(redis.call('GET', KEYS[3]) or '0')
`)

// Add atomically indexes key with size and returns the new total bytes.
func (i *Index) Add(ctx context.Context, key string, size int64, at time.Time) (int64, error) {
	n, err := addScript.Run(ctx, i.rdb, []string{i.keys.Objects, i.keys.LRU, i.keys.Bytes, i.keys.Epoch},
		key, size, at.UnixMilli()).Int64()
	return n, mapScriptErr(err)
}

var removeScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[4]) == 0 then return redis.error_reply('NOINDEX') end
local s = redis.call('HGET', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if not s then return -1 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('DECRBY', KEYS[3], s)
return tonumber(s)
`)

// Remove atomically de-indexes key. It returns the size that was freed, or -1 if absent.
func (i *Index) Remove(ctx context.Context, key string) (int64, error) {
	n, err := removeScript.Run(ctx, i.rdb, []string{i.keys.Objects, i.keys.LRU, i.keys.Bytes, i.keys.Epoch}, key).Int64()
	return n, mapScriptErr(err)
}

// TotalBytes returns the indexed size total.
func (i *Index) TotalBytes(ctx context.Context) (int64, error) {
	n, err := i.rdb.Get(ctx, i.keys.Bytes).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return n, err
}

// Initialized reports whether the index has been built and not lost since.
func (i *Index) Initialized(ctx context.Context) (bool, error) {
	n, err := i.rdb.Exists(ctx, i.keys.Epoch).Result()
	return n == 1, err
}

// Size returns the indexed size of key, or -1 if it is not indexed.
func (i *Index) Size(ctx context.Context, key string) (int64, error) {
	n, err := i.rdb.HGet(ctx, i.keys.Objects, key).Int64()
	if errors.Is(err, redis.Nil) {
		return -1, nil
	}
	return n, err
}

// All returns every indexed key with its size.
func (i *Index) All(ctx context.Context) (map[string]int64, error) {
	out := make(map[string]int64)
	var cursor uint64
	for {
		kv, next, err := i.rdb.HScan(ctx, i.keys.Objects, cursor, "", 1000).Result()
		if err != nil {
			return nil, err
		}
		for j := 0; j+1 < len(kv); j += 2 {
			n, _ := strconv.ParseInt(kv[j+1], 10, 64)
			out[kv[j]] = n
		}
		if cursor = next; cursor == 0 {
			return out, nil
		}
	}
}

var recountScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[3]) == 0 then return redis.error_reply('NOINDEX') end
local total = 0
for _, v in ipairs(redis.call('HVALS', KEYS[1])) do total = total + tonumber(v) end
local old = tonumber(redis.call('GET', KEYS[2]) or '0')
if old ~= total then redis.call('SET', KEYS[2], total) end
return {old, total}
`)

// Recount atomically recomputes the bytes counter from the object sizes.
func (i *Index) Recount(ctx context.Context) (before, after int64, err error) {
	res, err := recountScript.Run(ctx, i.rdb, []string{i.keys.Objects, i.keys.Bytes, i.keys.Epoch}).Int64Slice()
	if err != nil {
		return 0, 0, mapScriptErr(err)
	}
	return res[0], res[1], nil
}

// InTrash reports whether key is waiting for deletion.
func (i *Index) InTrash(ctx context.Context, key string) (bool, error) {
	_, err := i.rdb.ZScore(ctx, i.keys.Trash, key).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	return err == nil, err
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
		p.Set(ctx, i.keys.Epoch, strconv.FormatInt(time.Now().UnixMilli(), 10), 0)
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
