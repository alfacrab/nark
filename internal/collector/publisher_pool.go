package collector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/thetuposoft/nark/internal/config"
	"github.com/thetuposoft/nark/internal/workerpool"
)

// newPublisherPool gives every worker a dedicated Kafka producer, so
// NARK_POOL_WORKERS is also the producer connection count.
func NewPublisherPool(
	ctx context.Context,
	cfg config.Collector,
	metrics *Metrics,
	log *slog.Logger,
) (*workerpool.Pool[*PublishJob], error) {
	factory := NewPublisherFactory(
		// func(int) (collector.Sender, error) { return kafkax.NewProducer(cfg.Producer), nil },
		func(int) (Sender, error) { return nil, nil },
		metrics,
		time.Now,
	)

	pool, err := workerpool.New(workerpool.Options[*PublishJob]{
		Workers:        cfg.Pool.Workers,
		QueueSize:      cfg.Pool.QueueSize,
		EnqueueTimeout: cfg.Pool.EnqueueTimeout,
		JobTimeout:     cfg.Producer.WriteTimeout * 2,
		OnError: func(job *PublishJob, err error) {
			log.Error("batch delivery failed",
				slog.Int("tracks", job.Tracks()),
				slog.Any("error", err),
			)
		},
	}, factory)
	if err != nil {
		return nil, fmt.Errorf("create worker pool: %w", err)
	}

	if err := pool.Start(ctx); err != nil {
		return nil, fmt.Errorf("start worker pool: %w", err)
	}
	return pool, nil
}
