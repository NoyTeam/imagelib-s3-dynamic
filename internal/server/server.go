package server

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/redis/go-redis/v9"

	"imagelib-s3-dynamic/gen/client"
)

type Server struct {
	e   *echo.Echo
	db  *sql.DB
	rdb *redis.Client
	gc  *client.Client
}

func New(db *sql.DB, rdb *redis.Client, gc *client.Client) *Server {
	s := &Server{
		e:   echo.New(),
		db:  db,
		rdb: rdb,
		gc:  gc,
	}

	s.e.GET("/healthz", s.healthz)

	return s
}

func (s *Server) healthz(c *echo.Context) error {
	ctx := c.Request().Context()

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

func (s *Server) Start(ctx context.Context, addr string) error {
	return (echo.StartConfig{Address: addr}).Start(ctx, s.e)
}
