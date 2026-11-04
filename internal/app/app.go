package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"imagelib-s3-dynamic/gen/client"
	"imagelib-s3-dynamic/internal/cache"
	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/database"
	"imagelib-s3-dynamic/internal/events"
	"imagelib-s3-dynamic/internal/proxy"
	"imagelib-s3-dynamic/internal/scheduler"
	"imagelib-s3-dynamic/internal/server"
	"imagelib-s3-dynamic/internal/state"
	"imagelib-s3-dynamic/internal/storage"
)

type options struct {
	onListen func(net.Addr)
}

type Option func(*options)

// WithListenerAddr is called with the bound address once the HTTP server listens.
func WithListenerAddr(fn func(net.Addr)) Option {
	return func(o *options) { o.onListen = fn }
}

// Run starts the configured components and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg config.Config, opts ...Option) error {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()

	db, err := database.NewPostgres(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer db.Close()

	if cfg.DBPush.Enabled {
		if err := database.PushSchema(ctx, db, cfg.Database, cfg.DBPush); err != nil {
			return err
		}
	}

	rdb, err := cache.NewRedis(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer rdb.Close()

	local, err := storage.NewLocal(cfg.LocalPath)
	if err != nil {
		return fmt.Errorf("local storage: %w", err)
	}
	defer local.Close()

	index := state.NewIndex(rdb, state.NewKeys(cfg.Redis.Prefix))

	// Proxy: Redis lookup → S3 reverse proxy or local stream + async origin event.
	var images *proxy.Handler
	var handler http.Handler
	if cfg.RunsProxy() {
		pub := events.NewPublisher(rdb, index.Keys().Events, cfg.Proxy.StreamMaxLen, cfg.Proxy.EventBuffer)
		images = proxy.New(cfg.Proxy, local, index, storage.NewObjectNamer(cfg.Storage), pub)
		handler = images
	}

	// Scheduler: campaigns for leadership; only the leader consumes events and migrates.
	schedDone := make(chan struct{})
	if cfg.RunsScheduler() {
		sched := scheduler.New(cfg.Scheduler, cfg.InstanceID, rdb, index, local,
			storage.NewS3(cfg.Storage), scheduler.NewPGStore(client.New(db, client.WithDialect("postgresql"))))
		elector := scheduler.NewElector(rdb, index.Keys().Leader, cfg.InstanceID, cfg.Scheduler.LeaderTTL)
		go func() {
			defer close(schedDone)
			elector.Run(ctx, sched.Lead)
		}()
	} else {
		close(schedDone)
	}

	srv := server.New(db, rdb, handler)
	slog.Info("starting", "addr", cfg.Addr, "role", cfg.Role, "instance", cfg.InstanceID,
		"local", cfg.LocalPath, "s3_capacity", cfg.Scheduler.CapacityBytes)
	serveErr := srv.Start(ctx, cfg.Addr, o.onListen)
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if images != nil {
		images.Close(shutdownCtx)
	}
	select {
	case <-schedDone:
	case <-shutdownCtx.Done():
		slog.Warn("scheduler did not stop in time")
	}
	return serveErr
}
