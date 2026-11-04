package config

import "testing"

func TestParseSize(t *testing.T) {
	cases := map[string]int64{
		"0":      0,
		"1024":   1024,
		"1KiB":   1024,
		"10GiB":  10 << 30,
		"10gb":   10_000_000_000,
		"1.5G":   3 << 29,
		"512 MB": 512_000_000,
	}
	for in, want := range cases {
		got, err := ParseSize(in)
		if err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "abc", "-1G", "1XB"} {
		if _, err := ParseSize(in); err == nil {
			t.Errorf("ParseSize(%q) should fail", in)
		}
	}
}

func TestOrphanDeleteRequiresPrefix(t *testing.T) {
	t.Setenv("S3_BUCKET", "b")
	t.Setenv("S3_PUBLIC_URL", "http://x")
	t.Setenv("RECONCILE_ORPHANS", "delete")
	t.Setenv("S3_PREFIX", "")
	if _, err := Load(); err == nil {
		t.Fatal("delete without prefix must be rejected")
	}
	t.Setenv("S3_PREFIX", "cache")
	if _, err := Load(); err != nil {
		t.Fatalf("delete with prefix: %v", err)
	}
}

func TestIndexCacheTTLMustBeBelowDeleteDelay(t *testing.T) {
	t.Setenv("S3_BUCKET", "b")
	t.Setenv("S3_PUBLIC_URL", "http://x")
	t.Setenv("EVICT_DELETE_DELAY", "5s")
	t.Setenv("INDEX_CACHE_TTL", "5s")
	if _, err := Load(); err == nil {
		t.Fatal("ttl >= delete delay must be rejected")
	}
	t.Setenv("INDEX_CACHE_TTL", "1s")
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("INDEX_CACHE_TTL", "0")
	if _, err := Load(); err != nil {
		t.Fatal("0 disables the cache:", err)
	}
}
