package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/alfacrab/nark/internal/collector"
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

	registry := observability.NewRegistry()
	registry.Publish()
	metrics := collector.NewMetrics(registry)

	if err := observability.StartPush(cfg.Metrics, cfg.Runtime, log); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := collector.NewPublisherPool(ctx, cfg, metrics, log)
	if err != nil {
		return err
	}

	metrics.PoolGauges(pool)

	service := collector.NewService(cfg, pool, metrics, log)
	grpcServer := collector.NewGRPCServer(cfg, service, log)
	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.GRPCAddr, err)
	}

	grpcErrs := make(chan error, 1)

	go func() {
		log.Info("grpc server listening",
			slog.String("addr", cfg.GRPCAddr),
			slog.Int("workers", cfg.Pool.Workers),
			slog.Int("queue_size", cfg.Pool.QueueSize),
			slog.String("topic", cfg.Producer.Topic),
		)

		if err := grpcServer.Serve(listener); err != nil {
			grpcErrs <- err
			return
		}

		close(grpcErrs)
	}()

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
