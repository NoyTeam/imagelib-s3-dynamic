# Guide for Generated GCO Client

This document is generated automatically by `gco generate`.

## About GCORM

GCORM is a schema-first ORM toolkit for Go. Define models in `.gcorm` files (Prisma-like syntax), run the `gco` CLI, and use the generated type-safe `client`, `query`, and `model` packages in your application.

It covers code generation, query builders, migration planning, and database tooling.

For full documentation, advanced guides, and the latest reference, visit the
[GCORM Wiki](https://github.com/arsfy/gcorm/wiki).

## gco CLI Basics

The GCORM CLI is `gco`:

```sh
gco <command> [flags]
```

Common workflow:

```sh
# Initialize a schema (default: schema/schema.gcorm)
gco init

# Validate and format schema files
gco validate
gco fmt

# Generate Go client code (also writes this GUIDE.md)
gco generate
gco generate --schema schema
gco generate --dry-run

# Push schema changes to the database (dev)
gco db push

# Create / inspect migrations
gco migrate diff --name init
gco migrate init-sql --output init.sql
```

Useful flags (accepted by many commands):

- `--schema <path>` — schema directory or file
- `--config <path>` — `gco.config.yaml` / `gco.config.yml`

Other commands: `gco version`, `gco upgrade`, `gco help`.
See the [CLI Reference](https://github.com/arsfy/gcorm/wiki/CLI-Reference) on the wiki for the full list.

## Imports

```go
import (
    "context"
    "database/sql"
    "your-module/gen/client"
    "your-module/gen/query"
    "your-module/gen/model"
)
```

## Client Setup

```go
db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
if err != nil {
    panic(err)
}
defer db.Close()

c := client.New(db, client.WithDialect("postgresql"))
defer c.Close()

ctx := context.Background()
```

## Recommended Usage Pattern

Prefer staged builders for readability and composability:

```go
users, err := c.User.Query().
    Where(query.User.Email.Contains("@example.com")).
    OrderBy(query.User.CreatedAt.Desc()).
    Take(20).
    Do(ctx)
```

## CRUD Builder Pattern

- Create one: `c.<Model>.Create().Set(...).Do(ctx)`
- Update one: `c.<Model>.Update().Where(...).Set(...).Do(ctx)`
- Update many: `c.<Model>.Update().Where(...).Set(...).DoMany(ctx)`
- Delete one: `c.<Model>.Delete().Where(...).Do(ctx)`
- Delete many: `c.<Model>.Delete().Where(...).DoMany(ctx)`

## Bulk Inserts

Use `BulkCreate` for high-volume inserts, idempotent sync jobs, and join table writes:

```go
rows, err := c.Post.BulkCreate(posts).
    OnConflictDoNothing(query.PostIdColumn).
    Returning(query.PostIdColumn).
    BatchSize(1000).
    DoReturningValues(ctx)
```

- `Do(ctx)` executes batches and returns affected rows.
- `DoReturning(ctx)` returns inserted model rows on PostgreSQL.
- `DoReturningValues(ctx)` returns selected `RETURNING` columns as row maps.
- `CreateMany(ctx, data)` remains available and delegates to `BulkCreate(data).Do(ctx)`.

## Raw SQL

Use raw SQL for CTEs, `UPDATE ... FROM`, query-plan-sensitive joins, and PostgreSQL-specific features:

```go
posts, err := client.Raw[model.Post](ctx, c,
    `SELECT id, title, author_id FROM posts WHERE id = $1`, id)
```

- `client.Raw[T](ctx, c, sql, args...)` scans rows into a struct using `db` tags.
- `c.RawRows(ctx, sql, args...)` returns `[]map[string]any` for ad-hoc projections.
- `c.RawExec(ctx, sql, args...)` executes custom SQL and returns affected rows.

## Table and Column Constants

Generated query packages include constants for hand-written SQL:

```go
query.PostTable
query.PostIdColumn
query.PostAuthorIdColumn
```

## Models and Fields

### Image

- Client handle: `c.Image`
- Query namespace: `query.Image`
- Fields:
  - `Id` (String, id)
  - `Key` (String, unique)
  - `Size` (BigInt, optional)
  - `Mimetype` (String, optional)
  - `Status` (String)
  - `Hits` (BigInt)
  - `Lastaccessat` (DateTime, optional)
  - `Migratedat` (DateTime, optional)
  - `Createdat` (DateTime)
  - `Updatedat` (DateTime)

### When generating application code:

1. Prefer staged builders (`Query/Create/Update/Delete`) over one-shot calls.
2. Use `query.<Model>.<Field>` helpers for all conditions and set operations.
3. For optional fields, use `Set(value)` for non-null values and `SetNull()` to write NULL.
4. Use `DoMany` only when multiple-row side effects are intended.
5. Preserve explicit error handling on every DB operation.
