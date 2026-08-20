package collector

import (
	"fmt"

	"github.com/VictoriaMetrics/metrics"

	"github.com/alfacrab/nark/internal/observability"
)

// Results reported on nark_collector_requests_total.
const (
	resultAccepted = "accepted"
	resultInvalid  = "invalid"
	resultOverflow = "overflow"
)

// Reasons reported on nark_collector_tracks_dropped_total.
const (
	dropQueueFull     = "queue_full"
	dropPublishFailed = "publish_failed"
)

// Metrics holds every time series produced by the collector. Counters with a
// bounded label set are created eagerly so they are exported as zero before the
// first event, which keeps alerting rules simple.
type Metrics struct {
	reg *observability.Registry

	requests        map[string]*metrics.Counter
	requestDuration *metrics.Histogram
	batchTracks     *metrics.Histogram

	publishOK       *metrics.Counter
	publishFailed   *metrics.Counter
	publishMessages *metrics.Counter
	publishDuration *metrics.Histogram
	queueWait       *metrics.Histogram
	clockSkew       *metrics.Counter
}

// NewMetrics registers the collector time series on reg.
func NewMetrics(reg *observability.Registry) *Metrics {
	m := &Metrics{
		reg:             reg,
		requests:        make(map[string]*metrics.Counter, 3),
		requestDuration: reg.Histogram("nark_collector_request_duration_seconds"),
		batchTracks:     reg.Histogram("nark_collector_batch_tracks"),
		publishOK:       reg.Counter(`nark_collector_publish_total{result="ok"}`),
		publishFailed:   reg.Counter(`nark_collector_publish_total{result="error"}`),
		publishMessages: reg.Counter("nark_collector_publish_messages_total"),
		publishDuration: reg.Histogram("nark_collector_publish_duration_seconds"),
		queueWait:       reg.Histogram("nark_collector_queue_wait_seconds"),
		clockSkew:       reg.Counter("nark_collector_clock_skew_corrected_total"),
	}

	for _, result := range []string{resultAccepted, resultInvalid, resultOverflow} {
		m.requests[result] = reg.Counter(fmt.Sprintf("nark_collector_requests_total{result=%q}", result))
	}
	for _, reason := range []string{dropQueueFull, dropPublishFailed} {
		reg.Counter(fmt.Sprintf("nark_collector_tracks_dropped_total{reason=%q}", reason))
	}

	return m
}

// PoolGauges exports the live worker pool state.
func (m *Metrics) PoolGauges(p poolStats) {
	m.reg.Gauge("nark_collector_pool_workers", func() float64 { return float64(p.Workers()) })
	m.reg.Gauge("nark_collector_pool_queue_depth", func() float64 { return float64(p.Queued()) })
	m.reg.Gauge("nark_collector_pool_queue_capacity", func() float64 { return float64(p.Capacity()) })
	m.reg.Gauge("nark_collector_pool_inflight", func() float64 { return float64(p.InFlight()) })
}

// poolStats is the read-only view of the pool needed for gauges.
type poolStats interface {
	Workers() int
	Queued() int
	Capacity() int
	InFlight() int
}

func (m *Metrics) request(result string, seconds float64) {
	if c, ok := m.requests[result]; ok {
		c.Inc()
	}
	m.requestDuration.Update(seconds)
}

func (m *Metrics) accepted(kind string, count int) {
	if count <= 0 {
		return
	}
	m.reg.Counter(fmt.Sprintf("nark_collector_tracks_total{kind=%q}", kind)).Add(count)
}

func (m *Metrics) rejected(reason string) {
	m.reg.Counter(fmt.Sprintf("nark_collector_tracks_rejected_total{reason=%q}", reason)).Inc()
}

func (m *Metrics) dropped(reason string, count int) {
	if count <= 0 {
		return
	}
	m.reg.Counter(fmt.Sprintf("nark_collector_tracks_dropped_total{reason=%q}", reason)).Add(count)
}
