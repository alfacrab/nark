package config

import (
	"os"
	"time"
)

// Overflow policies for the collector ingest queue.
const (
	// OverflowDrop keeps acknowledging batches when the queue is saturated and
	// counts the loss in nark_collector_tracks_dropped_total.
	OverflowDrop = "drop"
	// OverflowReject answers RESOURCE_EXHAUSTED so the SDK can retry later.
	OverflowReject = "reject"
)

type Source func(key string) (string, bool)

// OSSource reads configuration from the process environment.
func OSSource() Source { return os.LookupEnv }

// LoadCollector builds the collector configuration from src.
func LoadCollector(src Source) (Collector, error) {
	r := &reader{src: src}

	cfg := Collector{
		Runtime: r.runtime("nark-collector", ":9091"),
		Metrics: r.metrics(),

		Producer: Producer{
			Brokers:     r.list("NARK_KAFKA_BROKERS", []string{"localhost:9092"}),
			Topic:       r.str("NARK_KAFKA_TOPIC", "nark.tracks"),
			Acks:        r.enum("NARK_KAFKA_ACKS", "all", "all", "one", "none"),
			Compression: r.enum("NARK_KAFKA_COMPRESSION", "lz4", "none", "gzip", "snappy", "lz4", "zstd"),
			BatchSize:   r.integer("NARK_KAFKA_BATCH_SIZE", 200), BatchBytes: r.integer("NARK_KAFKA_BATCH_BYTES", 1<<20),
			BatchTimeout: r.duration("NARK_KAFKA_BATCH_TIMEOUT", 50*time.Millisecond),
			WriteTimeout: r.duration("NARK_KAFKA_WRITE_TIMEOUT", 5*time.Second),
		},

		Pool: Pool{
			Workers:        r.integer("NARK_POOL_WORKERS", 8),
			QueueSize:      r.integer("NARK_POOL_QUEUE_SIZE", 4096),
			EnqueueTimeout: r.duration("NARK_POOL_ENQUEUE_TIMEOUT", 0),
			OverflowPolicy: r.enum("NARK_POOL_OVERFLOW_POLICY", OverflowDrop, OverflowDrop, OverflowReject),
			FlushTimeout:   r.duration("NARK_POOL_FLUSH_TIMEOUT", 15*time.Second),
		},

		GRPCAddr:        r.str("NARK_GRPC_ADDR", ":9090"),
		MaxRecvMsgBytes: r.integer("NARK_GRPC_MAX_RECV_BYTES", 4<<20),
		MaxBatchTracks:  r.integer("NARK_MAX_BATCH_TRACKS", 500),
		MaxClockSkew:    r.duration("NARK_MAX_CLOCK_SKEW", time.Minute),
	}

	if err := r.err(); err != nil {
		return Collector{}, err
	}
	if err := cfg.validate(); err != nil {
		return Collector{}, err
	}
	return cfg, nil
}

func (r *reader) runtime(service, httpAddr string) Runtime {
	return Runtime{
		Service:         r.str("NARK_SERVICE", service),
		Env:             r.str("NARK_ENV", "local"),
		Instance:        r.str("NARK_INSTANCE", hostname()),
		LogLevel:        r.enum("NARK_LOG_LEVEL", "info", "debug", "info", "warn", "error"),
		LogFormat:       r.enum("NARK_LOG_FORMAT", "json", "json", "text"),
		HTTPAddr:        r.str("NARK_HTTP_ADDR", httpAddr),
		ShutdownTimeout: r.duration("NARK_SHUTDOWN_TIMEOUT", 20*time.Second),
	}
}

func (r *reader) metrics() Metrics {
	return Metrics{
		PushURL:      r.str("NARK_METRICS_PUSH_URL", ""),
		PushInterval: r.duration("NARK_METRICS_PUSH_INTERVAL", 10*time.Second),
		ExtraLabels:  r.str("NARK_METRICS_EXTRA_LABELS", ""),
	}
}
