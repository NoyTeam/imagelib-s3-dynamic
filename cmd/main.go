package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"imagelib-s3-dynamic/internal/app"
	"imagelib-s3-dynamic/internal/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if err := app.Run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
