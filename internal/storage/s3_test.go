package storage

import (
	"testing"

	"imagelib-s3-dynamic/internal/config"
)

func TestObjectNamer(t *testing.T) {
	cases := []struct {
		prefix, public, key string
		wantKey, wantURL    string
	}{
		{"", "https://cdn.example", "a/b.jpg", "a/b.jpg", "https://cdn.example/a/b.jpg"},
		{"cache", "https://f004.backblazeb2.com/file/bkt", "a/b.jpg", "cache/a/b.jpg", "https://f004.backblazeb2.com/file/bkt/cache/a/b.jpg"},
		{"cache", "http://x", "中文 名称.jpg", "cache/中文 名称.jpg", "http://x/cache/%E4%B8%AD%E6%96%87%20%E5%90%8D%E7%A7%B0.jpg"},
		{"p", "http://x", "a?b#c%d.png", "p/a?b#c%d.png", "http://x/p/a%3Fb%23c%25d.png"},
	}
	for _, c := range cases {
		n := NewObjectNamer(config.StorageConfig{Prefix: c.prefix, PublicURL: c.public})
		if got := n.ObjectKey(c.key); got != c.wantKey {
			t.Errorf("ObjectKey(%q) = %q, want %q", c.key, got, c.wantKey)
		}
		if got := n.PublicURL(c.key); got != c.wantURL {
			t.Errorf("PublicURL(%q) = %q, want %q", c.key, got, c.wantURL)
		}
	}
}

func TestDetectContentType(t *testing.T) {
	png := []byte("\x89PNG\r\n\x1a\n0000")
	for name, want := range map[string]string{
		"a.jpg":  "image/jpeg",
		"a.webp": "image/webp",
		"noext":  "image/png", // sniffed
	} {
		if got := DetectContentType(name, readerAt(png)); got != want {
			t.Errorf("%s: %q, want %q", name, got, want)
		}
	}
}

type readerAt []byte

func (r readerAt) ReadAt(p []byte, off int64) (int, error) {
	return copy(p, r[off:]), nil
}
