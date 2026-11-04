package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

type stubImages struct{ calls int }

func (s *stubImages) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.calls++
	_, _ = w.Write([]byte("image:" + r.URL.Path))
}

func (s *stubImages) Health() map[string]string {
	return map[string]string{"redis_breaker": "open", "s3_breaker": "closed"}
}

// unreachablePG returns a *sql.DB whose pings fail fast (nothing listens on the port).
func unreachablePG(t *testing.T) *sql.DB {
	t.Helper()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	db, err := sql.Open("pgx", "postgres://u:p@"+addr+"/db?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func healthz(t *testing.T, s *Server) (int, map[string]string) {
	t.Helper()
	rec := httptest.NewRecorder()
	s.e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("healthz body: %v", err)
	}
	return rec.Code, body
}

func TestHealthzReportsDependenciesAndBreakers(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	images := &stubImages{}
	s := New(unreachablePG(t), rdb, images)

	code, body := healthz(t, s)
	if code != http.StatusServiceUnavailable || body["status"] != "degraded" || body["postgres"] == "ok" || body["redis"] != "ok" {
		t.Fatalf("PG down: %d %v", code, body)
	}
	if body["redis_breaker"] != "open" || body["s3_breaker"] != "closed" {
		t.Fatalf("breaker details missing: %v", body)
	}
	if images.calls != 0 {
		t.Fatal("/healthz must not be routed to the image handler")
	}

	mr.Close() // Redis down as well
	if code, body = healthz(t, s); code != http.StatusServiceUnavailable || body["redis"] == "ok" {
		t.Fatalf("redis down: %d %v", code, body)
	}
}

func TestHealthzBoundedWhenDependencyHangs(t *testing.T) {
	// Redis that accepts connections but never answers.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	rdb := redis.NewClient(&redis.Options{Addr: ln.Addr().String(), ContextTimeoutEnabled: true, MaxRetries: -1})
	defer rdb.Close()
	s := New(unreachablePG(t), rdb, nil)

	start := time.Now()
	code, _ := healthz(t, s)
	if code != http.StatusServiceUnavailable || time.Since(start) > 3*time.Second {
		t.Fatalf("healthz took %s (code %d); must be bounded", time.Since(start), code)
	}
}

func TestImageRoutesAndSchedulerOnlyMode(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	images := &stubImages{}
	s := New(unreachablePG(t), rdb, images)
	for _, p := range []string{"/a.png", "/deep/path/b.jpg", "/healthz/x.png"} {
		rec := httptest.NewRecorder()
		s.e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Body.String() != "image:"+p {
			t.Errorf("%s routed to %q", p, rec.Body.String())
		}
	}

	// ROLE=scheduler: no image routes at all.
	s = New(unreachablePG(t), rdb, nil)
	rec := httptest.NewRecorder()
	s.e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/a.png", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("scheduler-only must not serve images, got %d", rec.Code)
	}
	_ = context.Background()
}
