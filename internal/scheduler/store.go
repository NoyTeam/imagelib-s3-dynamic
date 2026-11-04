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
	listS3(ctx context.Context) ([]state.Entry, error)
	flushStats(ctx context.Context, rows map[string]*statRow) error
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

func (s pgStore) listS3(ctx context.Context) ([]state.Entry, error) {
	rows, err := s.gc.Image.Query().Where(query.Image.Status.Equals(StatusS3)).Do(ctx)
	if err != nil {
		return nil, err
	}
	entries := make([]state.Entry, 0, len(rows))
	for _, r := range rows {
		e := state.Entry{Key: r.Key, LastAccess: r.UpdatedAt}
		if r.Size != nil {
			e.Size = *r.Size
		}
		for _, t := range []*time.Time{r.MigratedAt, r.LastAccessAt} {
			if t != nil && t.After(e.LastAccess) {
				e.LastAccess = *t
			}
		}
		entries = append(entries, e)
	}
	return entries, nil
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
