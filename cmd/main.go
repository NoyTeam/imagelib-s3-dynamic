package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"imagelib-s3-dynamic/gen/client"
	"imagelib-s3-dynamic/internal/cache"
	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/database"
	"imagelib-s3-dynamic/internal/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()

	db, err := database.NewPostgres(ctx, cfg.Database)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer db.Close()

	rdb, err := cache.NewRedis(ctx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	srv := server.New(db, rdb, client.New(db, client.WithDialect("postgresql")))

	log.Printf("listening on %s", cfg.Addr)
	if err := srv.Start(ctx, cfg.Addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server: %v", err)
	}
}
