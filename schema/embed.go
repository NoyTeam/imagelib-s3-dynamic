// Package schema embeds the .gcorm schema so the binary can push it to the database on startup.
package schema

import "embed"

// FS holds the schema files at its root.
//
//go:embed *.gcorm
var FS embed.FS
