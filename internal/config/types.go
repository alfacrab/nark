package config

import (
	"errors"
	"time"
)

// Runtime holds settings shared by every binary.
type Runtime struct {
	Service         string
	Env             string
	Instance        string
	LogLevel        string
	LogFormat       string
	HTTPAddr        string
	ShutdownTimeout time.Duration
}

func (r Runtime) validate() error {
	if r.ShutdownTimeout <= 0 {
		return errors.New("NARK_SHUTDOWN_TIMEOUT must be > 0")
	}
	return nil
}

// Metrics configures the VictoriaMetrics integration. Scraping is always
// available on Runtime.HTTPAddr; pushing is enabled by setting PushURL.
type Metrics struct {
	PushURL      string
	PushInterval time.Duration
	ExtraLabels  string
}

func (m Metrics) validate() error {
	if m.PushURL != "" && m.PushInterval <= 0 {
		return errors.New("NARK_METRICS_PUSH_INTERVAL must be > 0 when NARK_METRICS_PUSH_URL is set")
	}
	return nil
}

// Producer configures the Kafka side of the collector.
type Producer struct {
	Brokers      []string
	Topic        string
	Acks         string
	Compression  string
	BatchSize    int
	BatchBytes   int
	BatchTimeout time.Duration
	WriteTimeout time.Duration
}

// Pool configures the collector worker pool. Workers is the number of parallel
// Kafka connections: every worker owns a dedicated producer.
type Pool struct {
	Workers        int
	QueueSize      int
	EnqueueTimeout time.Duration
	OverflowPolicy string
	FlushTimeout   time.Duration
}

// Collector is the full configuration of the ingest service.
type Collector struct {
	Runtime  Runtime
	Metrics  Metrics
	Producer Producer
	Pool     Pool

	GRPCAddr        string
	MaxRecvMsgBytes int
	MaxBatchTracks  int
	// MaxClockSkew bounds how far into the future occurred_at may be before the
	// collector rewrites it to the ingest time.
	MaxClockSkew time.Duration
}

func (c Collector) validate() error {
	var errs []error
	errs = append(errs, c.Runtime.validate(), c.Metrics.validate())
	if len(c.Producer.Brokers) == 0 {
		errs = append(errs, errors.New("NARK_KAFKA_BROKERS must not be empty"))
	}
	if c.Producer.Topic == "" {
		errs = append(errs, errors.New("NARK_KAFKA_TOPIC must not be empty"))
	}
	if c.Pool.Workers <= 0 {
		errs = append(errs, errors.New("NARK_POOL_WORKERS must be > 0"))
	}
	if c.Pool.QueueSize <= 0 {
		errs = append(errs, errors.New("NARK_POOL_QUEUE_SIZE must be > 0"))
	}
	if c.MaxBatchTracks <= 0 {
		errs = append(errs, errors.New("NARK_MAX_BATCH_TRACKS must be > 0"))
	}
	if c.GRPCAddr == "" {
		errs = append(errs, errors.New("NARK_GRPC_ADDR must not be empty"))
	}
	return errors.Join(errs...)
}
