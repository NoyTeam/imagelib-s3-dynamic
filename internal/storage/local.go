package storage

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path"
	"strings"
)

// ErrInvalidKey is returned for keys that would escape the storage root or touch hidden files.
var ErrInvalidKey = errors.New("invalid object key")

// NormalizeKey converts a request path into a canonical object key ("a/b/c.jpg").
func NormalizeKey(p string) (string, error) {
	if strings.ContainsRune(p, 0) || strings.Contains(p, "\\") {
		return "", ErrInvalidKey
	}
	clean := strings.TrimPrefix(path.Clean("/"+p), "/")
	if clean == "" {
		return "", ErrInvalidKey
	}
	for _, seg := range strings.Split(clean, "/") {
		if strings.HasPrefix(seg, ".") {
			return "", ErrInvalidKey
		}
	}
	return clean, nil
}

// Local is the source-of-truth origin backed by a directory on disk.
type Local struct {
	root *os.Root
}

func NewLocal(dir string) (*Local, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open local root %q: %w", dir, err)
	}
	return &Local{root: root}, nil
}

func (l *Local) Close() error { return l.root.Close() }

// Open opens a regular file by normalized key. The returned error wraps fs.ErrNotExist
// for missing objects and directories. Symlinks escaping the root are rejected by os.Root.
func (l *Local) Open(key string) (*os.File, fs.FileInfo, error) {
	f, err := l.root.Open(key)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, nil, fmt.Errorf("%s: %w", key, fs.ErrNotExist)
	}
	return f, info, nil
}

// DetectContentType guesses a MIME type by extension, falling back to content sniffing.
func DetectContentType(name string, r io.ReaderAt) string {
	if ct := mime.TypeByExtension(path.Ext(name)); ct != "" {
		return ct
	}
	buf := make([]byte, 512)
	n, _ := r.ReadAt(buf, 0)
	return http.DetectContentType(buf[:n])
}
