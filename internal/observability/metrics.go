package observability

import (
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/VictoriaMetrics/metrics"

	"github.com/thetuposoft/nark/internal/config"
)

// Registry is a thin wrapper around a VictoriaMetrics metric set.
//
// Each service owns one registry. It is exported over /metrics for scraping and,
// when NARK_METRICS_PUSH_URL is set, pushed to VictoriaMetrics directly, which
// is the usual choice for short-lived or autoscaled workloads.
type Registry struct {
	set *metrics.Set
}

// NewRegistry creates an unpublished registry. Call Publish to expose it.
func NewRegistry() *Registry {
	return &Registry{set: metrics.NewSet()}
}

// Counter returns the counter for name, creating it on first use.
func (r *Registry) Counter(name string) *metrics.Counter {
	return r.set.GetOrCreateCounter(name)
}

// Histogram returns the histogram for name, creating it on first use.
func (r *Registry) Histogram(name string) *metrics.Histogram {
	return r.set.GetOrCreateHistogram(name)
}

// Gauge registers a gauge sampled from f on every export.
func (r *Registry) Gauge(name string, f func() float64) *metrics.Gauge {
	return r.set.GetOrCreateGauge(name, f)
}

// Publish makes the registry visible to /metrics and to the push loop.
func (r *Registry) Publish() {
	metrics.RegisterSet(r.set)
}

// WritePrometheus writes the registry in Prometheus text exposition format.
func (r *Registry) WritePrometheus(w io.Writer) {
	r.set.WritePrometheus(w)
}

// StartPush starts pushing all published registries to VictoriaMetrics. It is a
// no-op when no push URL is configured.
func StartPush(cfg config.Metrics, rt config.Runtime, log *slog.Logger) error {
	if cfg.PushURL == "" {
		log.Info("metrics push disabled, scrape endpoint only", slog.String("http_addr", rt.HTTPAddr))
		return nil
	}

	labels := pushLabels(cfg, rt)
	if err := metrics.InitPush(cfg.PushURL, cfg.PushInterval, labels, true); err != nil {
		return fmt.Errorf("init metrics push: %w", err)
	}

	log.Info("metrics push started",
		slog.String("url", cfg.PushURL),
		slog.Duration("interval", cfg.PushInterval),
		slog.String("labels", labels),
	)
	return nil
}

// pushLabels builds the label set attached to every pushed time series.
func pushLabels(cfg config.Metrics, rt config.Runtime) string {
	parts := []string{
		fmt.Sprintf("job=%q", rt.Service),
		fmt.Sprintf("instance=%q", rt.Instance),
		fmt.Sprintf("env=%q", rt.Env),
	}
	if cfg.ExtraLabels != "" {
		parts = append(parts, cfg.ExtraLabels)
	}
	return strings.Join(parts, ",")
}
