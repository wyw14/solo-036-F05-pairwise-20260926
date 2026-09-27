package compressor

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/pressure"
	"github.com/wyw14/solo-036/internal/run"
)

func newHarness() (*pressure.Controller, *run.Store, *Worker) {
	p := pressure.NewController()
	p.Authorize()
	store := run.NewStore()
	worker := NewWorker(p, store).WithInterval(5 * time.Millisecond)
	return p, store, worker
}

func startedRun(store *run.Store) string {
	r := store.Create("station-a")
	return r.ID
}

// A healthy ramp follows the slope all the way to the target and reports
// completion with the pressure actually reached.
func TestRampReachesTargetAlongSlope(t *testing.T) {
	p, store, worker := newHarness()
	runID := startedRun(store)
	plan := pressure.Plan{RunID: runID, From: 0, Target: 4, Step: 1, Interval: time.Millisecond, Limit: 10, Tolerance: 0.5, FeedbackMaxAge: time.Second}

	steps := 0
	worker.OnProgress(func(id string, commanded, observed float64, step int) { steps++ }).Ramp(context.Background(), plan)
	<-worker.Done()

	report, ok := p.LastResult(runID)
	if !ok {
		t.Fatalf("expected terminal report for %s", runID)
	}
	if !report.Complete || report.Reason != pressure.ReasonCompleted {
		t.Fatalf("expected completed report, got %+v", report)
	}
	if report.Reached != 4 || p.Current() != 4 {
		t.Fatalf("expected reached/current 4, got %f/%f", report.Reached, p.Current())
	}
	if steps != 4 {
		t.Fatalf("expected four slope steps, got %d", steps)
	}
	if p.Active(runID) {
		t.Fatalf("command must be inactive after completion")
	}
}

// Cancellation mid-ramp must leave the reached pressure and a cancelled reason,
// and the old command must reject every further write.
func TestRampCancellationLeavesReachedAndFencesOldCommand(t *testing.T) {
	p, store, worker := newHarness()
	runID := startedRun(store)
	plan := pressure.Plan{RunID: runID, From: 0, Target: 10, Step: 1, Interval: 200 * time.Millisecond, Limit: 12, Tolerance: 0.5, FeedbackMaxAge: time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan struct{})
	worker.OnProgress(func(id string, commanded, observed float64, step int) {
		if step == 1 {
			close(first)
		}
	}).Ramp(ctx, plan)
	<-first
	cancel()
	<-worker.Done()

	report, ok := p.LastResult(runID)
	if !ok {
		t.Fatalf("expected terminal report")
	}
	if report.Reason != pressure.ReasonCancelled {
		t.Fatalf("expected cancelled reason, got %s", report.Reason)
	}
	if report.Reached != 1 {
		t.Fatalf("expected reached pressure 1, got %f", report.Reached)
	}
	if report.Reached >= report.Target {
		t.Fatalf("reached must be below target after interruption")
	}
	if err := p.Advance(runID, 9); err == nil {
		t.Fatalf("old command must not be allowed to continue after cancellation")
	}
	if p.Active(runID) {
		t.Fatalf("command must be inactive after cancellation")
	}
}

// A run flipping to cancelled in the store (operator cancel without context)
// also halts the ramp.
func TestRampStopsWhenRunCancelled(t *testing.T) {
	p, store, worker := newHarness()
	runID := startedRun(store)
	plan := pressure.Plan{RunID: runID, From: 0, Target: 10, Step: 1, Interval: 50 * time.Millisecond, Limit: 12, Tolerance: 0.5, FeedbackMaxAge: time.Second}

	first := make(chan struct{})
	worker.OnProgress(func(id string, commanded, observed float64, step int) {
		if step == 1 {
			close(first)
		}
	}).Ramp(context.Background(), plan)
	<-first
	if err := store.Cancel(runID); err != nil {
		t.Fatal(err)
	}
	<-worker.Done()

	report, ok := p.LastResult(runID)
	if !ok || report.Reason != pressure.ReasonCancelled {
		t.Fatalf("expected cancelled report, got %+v ok=%v", report, ok)
	}
}

type divergentFeedback struct{}

func (divergentFeedback) Sample(runID string, commanded float64) (pressure.Sample, bool) {
	return pressure.Sample{RunID: runID, Value: commanded + 5, At: time.Now().UTC()}, true
}

// Feedback running far ahead of the command trips the divergence check and
// records the observed pressure as the reached value.
func TestRampStopsOnFeedbackDivergence(t *testing.T) {
	p, store, worker := newHarness()
	runID := startedRun(store)
	worker.WithFeedback(divergentFeedback{})
	plan := pressure.Plan{RunID: runID, From: 0, Target: 10, Step: 1, Interval: time.Millisecond, Limit: 100, Tolerance: 0.5, FeedbackMaxAge: time.Second}

	worker.Ramp(context.Background(), plan)
	<-worker.Done()

	report, ok := p.LastResult(runID)
	if !ok || report.Reason != pressure.ReasonFeedbackDiverged {
		t.Fatalf("expected feedback divergence, got %+v ok=%v", report, ok)
	}
	if report.Reached != 6 {
		t.Fatalf("expected observed pressure 6 recorded, got %f", report.Reached)
	}
}

type staleFeedback struct{}

func (staleFeedback) Sample(runID string, commanded float64) (pressure.Sample, bool) {
	return pressure.Sample{RunID: runID, Value: commanded, At: time.Now().UTC().Add(-time.Hour)}, true
}

// Stale feedback must not be treated as confirmation: the ramp stops and names
// the unavailable-feedback cause.
func TestRampStopsOnStaleFeedback(t *testing.T) {
	p, store, worker := newHarness()
	runID := startedRun(store)
	worker.WithFeedback(staleFeedback{})
	plan := pressure.Plan{RunID: runID, From: 0, Target: 10, Step: 1, Interval: time.Millisecond, Limit: 12, Tolerance: 0.5, FeedbackMaxAge: 50 * time.Millisecond}

	worker.Ramp(context.Background(), plan)
	<-worker.Done()

	report, ok := p.LastResult(runID)
	if !ok || report.Reason != pressure.ReasonFeedbackUnavailable {
		t.Fatalf("expected unavailable feedback, got %+v ok=%v", report, ok)
	}
}

// A plan whose target exceeds the hard limit is rejected before any write.
func TestRampRejectsTargetOverLimit(t *testing.T) {
	p, store, worker := newHarness()
	runID := startedRun(store)
	plan := pressure.Plan{RunID: runID, From: 0, Target: 20, Step: 1, Interval: time.Millisecond, Limit: 12, Tolerance: 0.5, FeedbackMaxAge: time.Second}

	reports := make(chan pressure.Report, 1)
	worker.OnFinished(func(rep pressure.Report) { reports <- rep })
	worker.Ramp(context.Background(), plan)
	<-worker.Done()

	if p.Current() != 0 {
		t.Fatalf("no pressure may be written when target exceeds limit, got %f", p.Current())
	}
	report := <-reports
	if report.Reason != pressure.ReasonUnauthorized {
		t.Fatalf("expected unauthorized report, got %s", report.Reason)
	}
	if report.Reached != 0 {
		t.Fatalf("reached pressure must be 0 when ramp never started, got %f", report.Reached)
	}
}

// A foreign run ID can never drive a command it does not own.
func TestAdvanceRejectsForeignRun(t *testing.T) {
	p, store, worker := newHarness()
	owner := startedRun(store)
	plan := pressure.Plan{RunID: owner, From: 0, Target: 4, Step: 1, Interval: time.Hour, Limit: 10, Tolerance: 0.5, FeedbackMaxAge: time.Second}
	firstStep := make(chan struct{})
	worker.OnProgress(func(string, float64, float64, int) {
		select {
		case <-firstStep:
		default:
			close(firstStep)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); <-worker.Done() }()
	worker.Ramp(ctx, plan)
	<-firstStep

	if err := p.Begin(pressure.Plan{RunID: "other-run", From: 0, Target: 4, Step: 1, Interval: time.Millisecond, Limit: 10, Tolerance: 0.5, FeedbackMaxAge: time.Second}); err == nil {
		t.Fatalf("second run must not seize the compressor")
	}
	if err := p.Advance("other-run", 3); err == nil {
		t.Fatalf("foreign run write must be rejected")
	}
}
