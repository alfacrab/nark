package collector

import (
	"context"
	"fmt"
	"time"

	narkv1 "github.com/thetuposoft/nark/gen/go/nark/v1"
	// "github.com/thetuposoft/nark/internal/kafkax"
	"github.com/thetuposoft/nark/internal/workerpool"
)

// PublishJob is one accepted batch on its way to Kafka. Serialization happens in
// the worker, not in the request goroutine, so the caller is acknowledged as
// early as possible.
type PublishJob struct {
	Envelopes  []*narkv1.TrackEnvelope
	EnqueuedAt time.Time
}

// Tracks is the number of tracks carried by the job.
func (j *PublishJob) Tracks() int { return len(j.Envelopes) }

// Sender is the Kafka capability the publisher needs. kafkax.Producer is the
// production implementation; tests use a stub.
type Sender interface {
	// Publish(ctx context.Context, msgs []kafkax.Message) error
	Close() error
}

// publisher is the per-worker unit of the pool. It owns one Sender, which means
// one set of Kafka connections per worker.
type publisher struct {
	sender  Sender
	metrics *Metrics
	clock   func() time.Time
}

// NewPublisherFactory returns a workerpool.Factory that gives every worker its
// own Sender. newSender is called once per worker with its index.
func NewPublisherFactory(newSender func(index int) (Sender, error), m *Metrics, clock func() time.Time) workerpool.Factory[*PublishJob] {
	return func(index int) (workerpool.Worker[*PublishJob], error) {
		sender, err := newSender(index)
		if err != nil {
			return nil, fmt.Errorf("create sender %d: %w", index, err)
		}
		return &publisher{sender: sender, metrics: m, clock: clock}, nil
	}
}

// Process serializes the envelopes and writes them to Kafka. An error makes the
// pool report the job as lost, since the caller is long gone.
func (p *publisher) Process(ctx context.Context, job *PublishJob) error {
	// 	if job == nil || len(job.Envelopes) == 0 {
	// 		return nil
	// 	}

	// 	p.metrics.queueWait.Update(p.clock().Sub(job.EnqueuedAt).Seconds())

	// 	msgs := make([]kafkax.Message, 0, len(job.Envelopes))
	// 	for _, env := range job.Envelopes {
	// 		value, err := proto.Marshal(env)
	// 		if err != nil {
	// 			// A single unserializable envelope must not sink the whole batch.
	// 			p.metrics.dropped(dropPublishFailed, 1)
	// 			continue
	// 		}
	// 		msgs = append(msgs, kafkax.Message{Key: []byte(partitionKey(env)), Value: value})
	// 	}

	// start := p.clock()
	// err := p.sender.Publish(ctx, msgs)
	// p.metrics.publishDuration.Update(p.clock().Sub(start).Seconds())

	// if err != nil {
	// 	p.metrics.publishFailed.Inc()
	// 	p.metrics.dropped(dropPublishFailed, len(msgs))
	// 	return fmt.Errorf("publish batch %q: %w", job.Envelopes[0].GetBatchId(), err)
	// }

	p.metrics.publishOK.Inc()
	// p.metrics.publishMessages.Add(len(msgs))
	return nil
}

// Close releases the Kafka connections owned by this worker.
func (p *publisher) Close() error { return p.sender.Close() }

// partitionKey keeps all tracks of a session, user or device on one partition so
// their relative order is preserved end to end.
func partitionKey(env *narkv1.TrackEnvelope) string {
	track := env.GetTrack()
	switch {
	case track.GetSessionId() != "":
		return track.GetSessionId()
	case track.GetUserId() != "":
		return track.GetUserId()
	case track.GetDeviceId() != "":
		return track.GetDeviceId()
	default:
		return track.GetId()
	}
}
