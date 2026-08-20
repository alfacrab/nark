// Package workerpool implements a bounded, generic worker pool.
//
// The pool is deliberately tiny: a buffered job queue, a fixed number of
// workers and an explicit drain on shutdown. Every worker owns its own Worker
// instance, so the worker count doubles as the number of downstream connections
// (one Kafka producer per worker in the collector).
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrQueueFull means the queue was saturated and the job was not accepted.
	ErrQueueFull = errors.New("workerpool: queue is full")
	// ErrClosed means the pool no longer accepts jobs.
	ErrClosed = errors.New("workerpool: closed")
)

// Worker processes jobs of type T. One instance is created per pool worker and
// is only ever used by that worker, so implementations do not need to be
// goroutine-safe and may hold a dedicated connection.
type Worker[T any] interface {
	Process(ctx context.Context, job T) error
	Close() error
}

// Factory creates the Worker for the given zero-based worker index.
type Factory[T any] func(index int) (Worker[T], error)

// Options configures a Pool.
type Options[T any] struct {
	// Workers is the number of goroutines, and therefore connections, to run.
	Workers int
	// QueueSize is the number of jobs that may wait to be picked up.
	QueueSize int
	// EnqueueTimeout is how long Submit waits for queue space. Zero makes Submit
	// non-blocking, which is what the ingest path wants.
	EnqueueTimeout time.Duration
	// JobTimeout bounds a single Process call. Zero means no timeout.
	JobTimeout time.Duration
	// OnError is called for every failed or panicking job.
	OnError func(job T, err error)
}

// Pool runs Workers goroutines over a shared bounded queue.
type Pool[T any] struct {
	opts     Options[T]
	factory  Factory[T]
	jobs     chan T
	wg       sync.WaitGroup
	mtx      sync.RWMutex
	closed   bool
	started  bool
	workers  []Worker[T]
	inflight atomic.Int64
	base     context.Context
}

// New validates opts and returns a pool that is not running yet.
func New[T any](opts Options[T], factory Factory[T]) (*Pool[T], error) {
	switch {
	case opts.Workers <= 0:
		return nil, errors.New("workerpool: Workers must be > 0")
	case opts.QueueSize <= 0:
		return nil, errors.New("workerpool: QueueSize must be > 0")
	case factory == nil:
		return nil, errors.New("workerpool: Factory must not be nil")
	}

	return &Pool[T]{
		opts:    opts,
		factory: factory,
		jobs:    make(chan T, opts.QueueSize),
	}, nil
}

// Stateless adapts a plain function into a Factory for pools that do not need
// per-worker state.
func Stateless[T any](fn func(ctx context.Context, job T) error) Factory[T] {
	return func(int) (Worker[T], error) { return statelessWorker[T]{fn: fn}, nil }
}

// Start creates the workers and begins processing. ctx is used as the parent of
// job contexts for values only: its cancellation does not stop the pool, Close
// does.
func (p *Pool[T]) Start(ctx context.Context) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.started {
		return errors.New("workerpool: already started")
	}

	if p.closed {
		return ErrClosed
	}

	p.base = context.WithoutCancel(ctx)

	for i := range p.opts.Workers {
		w, err := p.factory(i)
		if err != nil {
			p.closed = true
			close(p.jobs)
			p.wg.Wait()

			return errors.Join(fmt.Errorf("workerpool: create worker %d: %w", i, err), p.closeWorkers())
		}

		p.workers = append(p.workers, w)
		p.wg.Add(1)

		go p.run(w)
	}

	p.started = true

	return nil
}

// Submit hands a job to the pool. It returns ErrQueueFull when the queue is
// saturated and ErrClosed after Close.
func (p *Pool[T]) Submit(job T) error {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	if p.closed {
		return ErrClosed
	}

	if !p.started {
		return errors.New("workerpool: not started")
	}

	select {
	case p.jobs <- job:
		return nil
	default:
	}

	if p.opts.EnqueueTimeout <= 0 {
		return ErrQueueFull
	}

	timer := time.NewTimer(p.opts.EnqueueTimeout)
	defer timer.Stop()

	select {
	case p.jobs <- job:
		return nil
	case <-timer.C:
		return ErrQueueFull
	}
}

// Close stops accepting jobs, drains what is already queued and closes every
// worker. It returns ctx.Err() when the drain does not finish in time; queued
// jobs are then abandoned.
func (p *Pool[T]) Close(ctx context.Context) error {
	p.mtx.Lock()

	if p.closed {
		p.mtx.Unlock()
		return nil
	}

	p.closed = true
	close(p.jobs)
	p.mtx.Unlock()

	drained := make(chan struct{})

	go func() {
		p.wg.Wait()
		close(drained)
	}()

	var drainErr error
	select {
	case <-drained:
	case <-ctx.Done():
		drainErr = fmt.Errorf("workerpool: drain interrupted with %d queued jobs: %w", len(p.jobs), ctx.Err())
	}

	p.mtx.Lock()
	closeErr := p.closeWorkers()
	p.mtx.Unlock()

	return errors.Join(drainErr, closeErr)
}

// Queued is the number of jobs waiting to be processed.
func (p *Pool[T]) Queued() int { return len(p.jobs) }

// Capacity is the queue size.
func (p *Pool[T]) Capacity() int { return cap(p.jobs) }

// InFlight is the number of jobs currently being processed.
func (p *Pool[T]) InFlight() int { return int(p.inflight.Load()) }

// Workers is the configured worker, and therefore connection, count.
func (p *Pool[T]) Workers() int { return p.opts.Workers }

func (p *Pool[T]) run(w Worker[T]) {
	defer p.wg.Done()

	for job := range p.jobs {
		p.process(w, job)
	}
}

func (p *Pool[T]) process(w Worker[T], job T) {
	p.inflight.Add(1)
	defer p.inflight.Add(-1)

	ctx := p.base
	if p.opts.JobTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.opts.JobTimeout)
		defer cancel()
	}

	defer func() {
		if r := recover(); r != nil {
			p.reportError(job, fmt.Errorf("workerpool: worker panic: %v", r))
		}
	}()

	if err := w.Process(ctx, job); err != nil {
		p.reportError(job, err)
	}
}

func (p *Pool[T]) reportError(job T, err error) {
	if p.opts.OnError != nil {
		p.opts.OnError(job, err)
	}
}

// closeWorkers must be called with p.mu held.
func (p *Pool[T]) closeWorkers() error {
	var errs []error

	for _, w := range p.workers {
		if err := w.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	p.workers = nil

	return errors.Join(errs...)
}

type statelessWorker[T any] struct {
	fn func(ctx context.Context, job T) error
}

func (s statelessWorker[T]) Process(ctx context.Context, job T) error { return s.fn(ctx, job) }
func (s statelessWorker[T]) Close() error                             { return nil }
