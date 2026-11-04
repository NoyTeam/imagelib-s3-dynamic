package database

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/arsfy/gcorm/pkg/tooling/dbpush"

	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/schema"
)

// PushSchema applies the embedded .gcorm schema to the live database
func PushSchema(ctx context.Context, db *sql.DB, dsn string, cfg config.DBPushConfig) error {
	res, err := dbpush.Push(ctx, db, dbpush.Options{
		SchemaFS:         schema.FS,
		SchemaRoot:       ".",
		DatabaseURL:      dsn,
		AllowDestructive: cfg.AllowDestructive,
	})
	if err != nil {
		if !cfg.AllowDestructive && strings.Contains(err.Error(), "destructive") {
			return fmt.Errorf("%w\n(review the changes, then set DB_PUSH_ALLOW_DESTRUCTIVE=true once or run `gco db push --force`)", err)
		}
		return err
	}
	if res.Noop {
		slog.Info("db schema up to date", "hash", res.SchemaHash)
		return nil
	}
	slog.Info("db schema pushed", "changes", res.ChangeCount, "statements", len(res.AppliedStatements), "hash", res.SchemaHash)
	for _, stmt := range res.AppliedStatements {
		slog.Debug("db push", "sql", stmt)
	}
	return nil
}
