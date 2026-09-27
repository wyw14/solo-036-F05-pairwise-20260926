package compressor

import (
	"context"
	"sync"
	"time"

	"github.com/wyw14/solo-036/internal/pressure"
	"github.com/wyw14/solo-036/internal/run"
)

// Default slope parameters when a plan does not pin them.
const (
	DefaultSteps    = 12
	DefaultInterval = 200 * time.Millisecond
	// DefaultTolerance bounds how far field feedback may lag the command.
	DefaultTolerance = 0.5
	// DefaultFeedbackMaxAge bounds the age of feedback accepted at each tick.
	DefaultFeedbackMaxAge = 2 * time.Second
)

// FeedbackSource provides observed field pressure for a run. A missing sample
// (ok=false) means instrumentation has no fresh reading.
type FeedbackSource interface {
	Sample(runID string, commanded float64) (sample pressure.Sample, ok bool)
}

// Progress is invoked after every accepted ramp step.
type Progress func(runID string, commanded, observed float64, step int)

// Finished is invoked once when the ramp terminates, carrying the pressure
// actually reached and the stop reason.
type Finished func(report pressure.Report)

// echoFeedback simulates a perfect instrument: feedback follows the command.
type echoFeedback struct {
	pressure *pressure.Controller
}

func (e echoFeedback) Sample(runID string, commanded float64) (pressure.Sample, bool) {
	return pressure.Sample{RunID: runID, Value: e.pressure.Current(), At: time.Now().UTC()}, true
}

type Worker struct {
	pressure *pressure.Controller
	runs     *run.Store
	feedback FeedbackSource
	progress Progress
	finished Finished
	interval time.Duration

	mu   sync.Mutex
	done chan struct{}
	wg   sync.WaitGroup
}

func NewWorker(p *pressure.Controller, r *run.Store) *Worker {
	return &Worker{
		pressure: p,
		runs:     r,
		feedback: echoFeedback{pressure: p},
		interval: DefaultInterval,
		done:     make(chan struct{}),
	}
}

// WithFeedback replaces the field feedback source.
func (w *Worker) WithFeedback(source FeedbackSource) *Worker {
	if source != nil {
		w.feedback = source
	}
	return w
}

// OnProgress registers the per-step progress callback.
func (w *Worker) OnProgress(callback Progress) *Worker {
	w.progress = callback
	return w
}

// OnFinished registers the terminal callback.
func (w *Worker) OnFinished(callback Finished) *Worker {
	w.finished = callback
	return w
}

// WithInterval overrides the default tick interval.
func (w *Worker) WithInterval(interval time.Duration) *Worker {
	if interval > 0 {
		w.interval = interval
	}
	return w
}

// Done returns a channel closed when the most recently started ramp ends.
func (w *Worker) Done() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.done
}

// Ramp starts a slope-controlled pressurization in the background. The loop
// approaches plan.Target one step at a time, re-checking cancellation, the run
// state, the hard limit, and field feedback on every tick. Any interruption
// terminates the controller command with a Report before returning.
func (w *Worker) Ramp(ctx context.Context, plan pressure.Plan) {
	w.mu.Lock()
	w.done = make(chan struct{})
	done := w.done
	w.wg.Add(1)
	w.mu.Unlock()
	go func() {
		defer w.wg.Done()
		defer close(done)
		report := w.runRamp(ctx, plan)
		if w.finished != nil {
			w.finished(report)
		}
	}()
}

// Wait blocks until every background ramp has returned, or ctx expires.
func (w *Worker) Wait(ctx context.Context) error {
	finished := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
