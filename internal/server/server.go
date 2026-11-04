package server

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	e   *echo.Echo
	db  *sql.DB
	rdb *redis.Client
}

// New builds the HTTP server. images may be nil when the process only runs the scheduler.
func New(db *sql.DB, rdb *redis.Client, images http.Handler) *Server {
	s := &Server{
		e:   echo.New(),
		db:  db,
		rdb: rdb,
	}

	s.e.Use(middleware.Recover())
	s.e.GET("/healthz", s.healthz)
	if images != nil {
		// Static routes (e.g. /healthz) take precedence over the wildcard.
		s.e.Any("/*", echo.WrapHandler(images))
	}

	return s
}

func (s *Server) healthz(c *echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 2*time.Second)
	defer cancel()

	status := map[string]string{"status": "ok"}

	if err := s.db.PingContext(ctx); err != nil {
		status["postgres"] = err.Error()
		status["status"] = "degraded"
	} else {
		status["postgres"] = "ok"
	}

	if err := s.rdb.Ping(ctx).Err(); err != nil {
		status["redis"] = err.Error()
		status["status"] = "degraded"
	} else {
		status["redis"] = "ok"
	}

	code := http.StatusOK
	if status["status"] != "ok" {
		code = http.StatusServiceUnavailable
	}

	return c.JSON(code, status)
}

// Start serves until ctx is cancelled. onListen (optional) receives the bound address.
func (s *Server) Start(ctx context.Context, addr string, onListen func(net.Addr)) error {
	return (echo.StartConfig{Address: addr, HideBanner: true, ListenerAddrFunc: onListen}).Start(ctx, s.e)
}
