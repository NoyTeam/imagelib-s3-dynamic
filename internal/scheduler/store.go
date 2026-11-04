package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"imagelib-s3-dynamic/gen/client"
	"imagelib-s3-dynamic/gen/query"
	"imagelib-s3-dynamic/internal/state"
)

const (
	StatusLocal = "local"
	StatusS3    = "s3"
)

// Store persists durable object state.
type Store interface {
	markS3(ctx context.Context, key, mimeType string, size int64, at time.Time) error
	markLocal(ctx context.Context, key string, at time.Time) error
	// forEachS3 streams all S3-resident rows in key order, in pages.
	forEachS3(ctx context.Context, fn func([]state.Entry) error) error
	// filterS3 returns which of keys have PG status s3.
	filterS3(ctx context.Context, keys []string) (map[string]struct{}, error)
	find(ctx context.Context, key string) (*row, error)
	flushStats(ctx context.Context, rows map[string]*statRow) error
}

// row is the subset of an images row the scheduler needs. Size is -1 when unknown.
type row struct {
	Status     string
	Size       int64
	LastAccess time.Time
}

// NewPGStore returns a Store backed by PostgreSQL through the generated client.
func NewPGStore(gc *client.Client) Store { return pgStore{gc: gc} }

type pgStore struct {
	gc *client.Client
}

func (s pgStore) markS3(ctx context.Context, key, mimeType string, size int64, at time.Time) error {
	sets := []query.ImageSetClause{
		query.Image.Size.Set(size),
		query.Image.MimeType.Set(mimeType),
		query.Image.Status.Set(StatusS3),
		query.Image.MigratedAt.Set(at),
		query.Image.UpdatedAt.Set(at),
	}
	create := append([]query.ImageSetClause{query.Image.Key.Set(key)}, sets...)
	_, err := s.gc.Image.UpsertOne(ctx, query.Image.Key.Equals(key), create, sets)
	return err
}

func (s pgStore) markLocal(ctx context.Context, key string, at time.Time) error {
	_, err := s.gc.Image.Update().
		Where(query.Image.Key.Equals(key)).
		Set(query.Image.Status.Set(StatusLocal), query.Image.UpdatedAt.Set(at)).
		DoMany(ctx)
	return err
}

func (s pgStore) find(ctx context.Context, key string) (*row, error) {
	img, err := s.gc.Image.FindUnique(ctx, query.Image.Key.Equals(key))
	if err != nil || img == nil {
		return nil, err
	}
	r := &row{Status: img.Status, Size: -1, LastAccess: img.UpdatedAt}
	if img.Size != nil {
		r.Size = *img.Size
	}
	for _, t := range []*time.Time{img.MigratedAt, img.LastAccessAt} {
		if t != nil && t.After(r.LastAccess) {
			r.LastAccess = *t
		}
	}
	return r, nil
}

// pageSize bounds memory and statement time when walking millions of S3-resident rows.
const pageSize = 10000

// forEachS3 streams every S3-resident row in key order, page by page (keyset pagination on
// the partial index idx_images_s3_key), so memory stays bounded at tens of millions of rows.
func (s pgStore) forEachS3(ctx context.Context, fn func([]state.Entry) error) error {
	after := ""
	for {
		rows, err := s.gc.RawRows(ctx, `SELECT "key", COALESCE("size", 0) AS size,
			GREATEST("updated_at", COALESCE("migrated_at", "updated_at"), COALESCE("last_access_at", "updated_at")) AS last
			FROM "images" WHERE "status" = 's3' AND "key" > $1 ORDER BY "key" LIMIT $2`, after, pageSize)
		if err != nil {
			return fmt.Errorf("list s3 rows: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		page := make([]state.Entry, 0, len(rows))
		for _, r := range rows {
			e := state.Entry{Key: fmt.Sprint(r["key"])}
			e.Size, _ = r["size"].(int64)
			e.LastAccess, _ = r["last"].(time.Time)
			page = append(page, e)
		}
		if err := fn(page); err != nil {
			return err
		}
		after = page[len(page)-1].Key
		if len(rows) < pageSize {
			return nil
		}
	}
}

// filterS3 returns the subset of keys whose PG status is s3.
func (s pgStore) filterS3(ctx context.Context, keys []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := s.gc.RawRows(ctx, `SELECT "key" FROM "images" WHERE "key" = ANY($1) AND "status" = 's3'`, keys)
	if err != nil {
		return nil, fmt.Errorf("filter s3 rows: %w", err)
	}
	for _, r := range rows {
		out[fmt.Sprint(r["key"])] = struct{}{}
	}
	return out, nil
}

// stats buffers per-key origin hit counters and flushes them in bulk.
type stats struct {
	mu      sync.Mutex
	pending map[string]*statRow
}

type statRow struct {
	hits int64
	last time.Time
	size int64
}

func newStats() *stats { return &stats{pending: make(map[string]*statRow)} }

func (st *stats) add(key string, hits int64, last time.Time, size int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	r, ok := st.pending[key]
	if !ok {
		r = &statRow{}
		st.pending[key] = r
	}
	r.hits += hits
	if last.After(r.last) {
		r.last = last
	}
	if size > 0 {
		r.size = size
	}
}

func (st *stats) take() map[string]*statRow {
	st.mu.Lock()
	defer st.mu.Unlock()
	p := st.pending
	st.pending = make(map[string]*statRow)
	return p
}

// flushStats upserts accumulated hits/last_access_at into images in batches.
func (s pgStore) flushStats(ctx context.Context, rows map[string]*statRow) error {
	const batch = 500
	const cols = 5
	keys := make([]string, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	now := time.Now()
	for i := 0; i < len(keys); i += batch {
		end := min(i+batch, len(keys))
		var sb strings.Builder
		sb.WriteString(`INSERT INTO "images" ("key", "size", "hits", "last_access_at", "updated_at") VALUES `)
		args := make([]any, 0, (end-i)*cols)
		for j, k := range keys[i:end] {
			r := rows[k]
			if j > 0 {
				sb.WriteString(", ")
			}
			n := j * cols
			fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d)", n+1, n+2, n+3, n+4, n+5)
			var size *int64
			if r.size > 0 {
				size = &r.size
			}
			args = append(args, k, size, r.hits, r.last, now)
		}
		sb.WriteString(` ON CONFLICT ("key") DO UPDATE SET
			"hits" = "images"."hits" + EXCLUDED."hits",
			"last_access_at" = GREATEST("images"."last_access_at", EXCLUDED."last_access_at"),
			"size" = COALESCE(EXCLUDED."size", "images"."size"),
			"updated_at" = EXCLUDED."updated_at"`)
		if _, err := s.gc.RawExec(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("flush stats: %w", err)
		}
	}
	return nil
}
