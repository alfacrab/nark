package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alfacrab/nark/internal/config"
	"github.com/alfacrab/nark/internal/observability"
)

func main() {
	if err := run(); err != nil {
		slog.Error("collector stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadCollector(config.OSSource())
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := observability.NewLogger(os.Stdout, cfg.Runtime)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var runErr error
	<-ctx.Done()
	log.Info("shutdown signal received")

	return errors.Join(runErr, shutdown(cfg, log))
}

func shutdown(
	cfg config.Collector,
	log *slog.Logger,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Runtime.ShutdownTimeout)
	defer cancel()

	<-ctx.Done()
	log.Warn("graceful stop timed out, forcing")

	log.Info("collector stopped cleanly")
	return nil
}
