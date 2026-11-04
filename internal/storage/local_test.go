package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeKey(t *testing.T) {
	ok := map[string]string{
		"/a/b.jpg":       "a/b.jpg",
		"a//b.jpg":       "a/b.jpg",
		"/a/./b.jpg":     "a/b.jpg",
		"/a/x/../b.jpg":  "a/b.jpg",
		"/../../etc/pwd": "etc/pwd", // cleaned under the root, never escapes
	}
	for in, want := range ok {
		got, err := NormalizeKey(in)
		if err != nil || got != want {
			t.Errorf("NormalizeKey(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "/", "/.env", "/a/.git/config", "a\\b", "a\x00b"} {
		if _, err := NormalizeKey(in); err == nil {
			t.Errorf("NormalizeKey(%q) should fail", in)
		}
	}
}

func TestLocalOpen(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "a.png"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	_ = os.WriteFile(outside, []byte("x"), 0o644)
	_ = os.Symlink(outside, filepath.Join(dir, "link"))

	l, err := NewLocal(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	f, info, err := l.Open("sub/a.png")
	if err != nil || info.Size() != 4 {
		t.Fatalf("open: %v", err)
	}
	f.Close()

	if _, _, err := l.Open("sub"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("directory should be not-exist, got %v", err)
	}
	if _, _, err := l.Open("link"); err == nil {
		t.Fatal("symlink escaping root must be rejected")
	}
}

func BenchmarkNormalizeKey(b *testing.B) {
	for b.Loop() {
		_, _ = NormalizeKey("/photos/2024/06/some-image-name_1920x1080.jpg")
	}
}
