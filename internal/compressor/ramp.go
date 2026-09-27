package compressor

import (
	"context"
	"math"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/pressure"
)

// runRamp is the blocking slope-approach loop. It is responsible for leaving a
// terminal pressure.Report on every exit path.
func (w *Worker) runRamp(ctx context.Context, plan pressure.Plan) pressure.Report {
	plan = normalize(plan, w.interval)

	if err := w.pressure.Begin(plan); err != nil {
		return pressure.Report{
			RunID:   plan.RunID,
			Reached: plan.From,
			Target:  plan.Target,
			Reason:  pressure.ReasonUnauthorized,
			At:      time.Now().UTC(),
		}
	}
	stopCh := w.pressure.StopChannel(plan.RunID)

	commanded := plan.From
	stepIndex := 0

	// Already at (or above) target: confirm once and finish.
	if commanded >= plan.Target {
		return w.finish(plan, commanded)
	}

	for {
		// Interruption gates run first: context, controller-level stop, and a
		// cancelled/missing run all abort before the next write.
		select {
		case <-ctx.Done():
			return w.abort(plan, commanded, pressure.ReasonCancelled)
		case <-stopCh:
			return w.lastReport(plan, commanded)
		default:
		}
		if record, ok := w.runs.Get(plan.RunID); !ok || record.State == model.StateCancelled {
			return w.abort(plan, commanded, pressure.ReasonCancelled)
		}

		next := math.Min(commanded+plan.Step, plan.Target)
		if err := w.pressure.Advance(plan.RunID, next); err != nil {
			// Advance has already closed the command with the cause.
			return w.lastReport(plan, commanded)
		}
		commanded = next
		stepIndex++

		// Collect and check field feedback before accepting the step.
		observed := commanded
		if w.feedback != nil {
			if sample, ok := w.feedback.Sample(plan.RunID, commanded); ok {
				w.pressure.Observe(sample)
				observed = sample.Value
			}
		}
		if err := w.pressure.VerifyFeedback(plan.RunID, commanded, plan.Tolerance, plan.FeedbackMaxAge, time.Now().UTC()); err != nil {
			return w.lastReport(plan, commanded)
		}

		if w.progress != nil {
			w.progress(plan.RunID, commanded, observed, stepIndex)
		}

		if commanded >= plan.Target {
			return w.finish(plan, commanded)
		}

		select {
		case <-ctx.Done():
			return w.abort(plan, commanded, pressure.ReasonCancelled)
		case <-stopCh:
			return w.lastReport(plan, commanded)
		case <-time.After(plan.Interval):
		}
	}
}

// finish confirms the ramp at the target.
func (w *Worker) finish(plan pressure.Plan, commanded float64) pressure.Report {
	if report, err := w.pressure.Complete(plan.RunID, time.Now().UTC()); err == nil {
		return report
	}
	return w.lastReport(plan, commanded)
}

// abort cancels the live command with an explicit reason, preserving the
// pressure actually reached.
func (w *Worker) abort(plan pressure.Plan, commanded float64, reason pressure.StopReason) pressure.Report {
	if report, ok := w.pressure.Cancel(plan.RunID, reason); ok {
		return report
	}
	return w.lastReport(plan, commanded)
}

// lastReport recovers the report already recorded by the controller, or
// synthesizes one if the command vanished unexpectedly.
func (w *Worker) lastReport(plan pressure.Plan, commanded float64) pressure.Report {
	if report, ok := w.pressure.LastResult(plan.RunID); ok {
		return report
	}
	return pressure.Report{
		RunID:   plan.RunID,
		Reached: commanded,
		Target:  plan.Target,
		Reason:  pressure.ReasonUnauthorized,
		At:      time.Now().UTC(),
	}
}

func normalize(plan pressure.Plan, fallbackInterval time.Duration) pressure.Plan {
	if plan.Interval <= 0 {
		plan.Interval = fallbackInterval
		if plan.Interval <= 0 {
			plan.Interval = DefaultInterval
		}
	}
	if plan.Step <= 0 {
		span := plan.Target - plan.From
		if span > 0 {
			plan.Step = span / DefaultSteps
		} else {
			plan.Step = math.Max(plan.Target, 1)
		}
	}
	if plan.Tolerance <= 0 {
		plan.Tolerance = DefaultTolerance
	}
	return plan
}
