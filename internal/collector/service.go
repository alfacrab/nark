package collector

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	narkv1 "github.com/alfacrab/nark/gen/go/nark/v1"
	"github.com/alfacrab/nark/internal/config"

	// "github.com/alfacrab/nark/internal/trackx"
	"github.com/alfacrab/nark/internal/workerpool"
)

// Submitter hands accepted batches to the worker pool.
type Submitter interface {
	Submit(job *PublishJob) error
}

// Service implements the TrackIngest gRPC API.
//
// The handler does validation and envelope construction only: everything that
// can block, including protobuf serialization and the Kafka round trip, happens
// on the worker pool after the response was returned.
type Service struct {
	narkv1.UnimplementedTrackIngestServiceServer

	cfg     config.Collector
	pool    Submitter
	metrics *Metrics
	log     *slog.Logger
	norm    *normalizer
	clock   func() time.Time
	newID   func() string
}

// Option customizes a Service. Used by tests to control time and identifiers.
type Option func(*Service)

// WithClock replaces the time source.
func WithClock(clock func() time.Time) Option {
	return func(s *Service) { s.clock = clock }
}

// WithIDGenerator replaces the identifier source.
func WithIDGenerator(newID func() string) Option {
	return func(s *Service) { s.newID = newID }
}

// NewService wires the ingest handler.
func NewService(cfg config.Collector, pool Submitter, m *Metrics, log *slog.Logger, opts ...Option) *Service {
	s := &Service{
		cfg:     cfg,
		pool:    pool,
		metrics: m,
		log:     log,
		clock:   time.Now,
		newID:   func() string { return uuid.NewString() },
	}
	for _, opt := range opts {
		opt(s)
	}

	s.norm = &normalizer{now: s.clock, newID: s.newID, maxClockSkew: cfg.MaxClockSkew}
	return s
}

// Push validates a batch, enqueues it for delivery and returns immediately.
func (s *Service) Push(ctx context.Context, req *narkv1.PushRequest) (*narkv1.PushResponse, error) {
	start := s.clock()

	if req == nil || len(req.GetTracks()) == 0 {
		s.metrics.request(resultInvalid, s.clock().Sub(start).Seconds())
		return nil, status.Error(codes.InvalidArgument, "at least one track is required")
	}
	if len(req.GetTracks()) > s.cfg.MaxBatchTracks {
		s.metrics.request(resultInvalid, s.clock().Sub(start).Seconds())
		return nil, status.Errorf(codes.InvalidArgument, "batch holds %d tracks, limit is %d",
			len(req.GetTracks()), s.cfg.MaxBatchTracks)
	}

	batchID := req.GetBatchId()
	if batchID == "" {
		batchID = s.newID()
	}
	client := normalizeClient(req.GetClient())
	receivedAt := timestamppb.New(s.clock())
	traceID := traceIDFromContext(ctx)

	resp := &narkv1.PushResponse{BatchId: batchID}
	envelopes := make([]*narkv1.TrackEnvelope, 0, len(req.GetTracks()))
	perKind := make(map[string]int, 4)
	skewed := 0

	for _, track := range req.GetTracks() {
		skewCorrected, err := s.norm.normalize(track)
		if err != nil {
			var rej rejection
			if !errors.As(err, &rej) {
				rej = rejection{reason: reasonEmptyTrack, detail: err.Error()}
			}
			s.metrics.rejected(rej.reason)
			resp.Rejected++
			resp.Errors = append(resp.Errors, &narkv1.TrackError{
				TrackId: track.GetId(),
				Reason:  rej.reason + ": " + rej.detail,
			})
			continue
		}
		if skewCorrected {
			skewed++
		}

		// perKind[trackx.KindLabel(track.GetKind())]++
		envelopes = append(envelopes, &narkv1.TrackEnvelope{
			Track:       track,
			Client:      client,
			BatchId:     batchID,
			ReceivedAt:  receivedAt,
			CollectorId: s.cfg.Runtime.Instance,
			TraceId:     traceID,
		})
	}

	if skewed > 0 {
		s.metrics.clockSkew.Add(skewed)
	}
	resp.Accepted = uint32(len(envelopes))

	if len(envelopes) == 0 {
		s.metrics.request(resultInvalid, s.clock().Sub(start).Seconds())
		return resp, nil
	}

	job := &PublishJob{Envelopes: envelopes, EnqueuedAt: s.clock()}
	if err := s.pool.Submit(job); err != nil {
		return s.handleSubmitError(err, start, batchID, resp)
	}

	for kind, count := range perKind {
		s.metrics.accepted(kind, count)
	}
	s.metrics.batchTracks.Update(float64(len(envelopes)))
	s.metrics.request(resultAccepted, s.clock().Sub(start).Seconds())

	return resp, nil
}

// handleSubmitError applies the configured overflow policy. Dropping keeps the
// ingest path fast under load; rejecting pushes the retry back to the SDK.
func (s *Service) handleSubmitError(err error, start time.Time, batchID string, resp *narkv1.PushResponse) (*narkv1.PushResponse, error) {
	if !errors.Is(err, workerpool.ErrQueueFull) && !errors.Is(err, workerpool.ErrClosed) {
		s.metrics.request(resultOverflow, s.clock().Sub(start).Seconds())
		return nil, status.Error(codes.Internal, "ingest queue unavailable")
	}

	s.metrics.dropped(dropQueueFull, int(resp.Accepted))
	s.metrics.request(resultOverflow, s.clock().Sub(start).Seconds())
	s.log.Warn("ingest queue saturated",
		slog.String("batch_id", batchID),
		slog.Int("tracks", int(resp.Accepted)),
		slog.String("policy", s.cfg.Pool.OverflowPolicy),
		slog.Any("error", err),
	)

	if s.cfg.Pool.OverflowPolicy == config.OverflowReject {
		return nil, status.Error(codes.ResourceExhausted, "ingest queue is full, retry later")
	}

	// Best-effort mode: the batch is lost but the caller is not blocked.
	resp.Accepted = 0
	return resp, nil
}
