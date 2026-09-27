package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-036/internal/compressor"
	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/pressure"
	"github.com/wyw14/solo-036/internal/run"
)

// startRamp installs a cancellable ramp for the run and hands the slope plan to
// the compressor worker. The ramp outlives the launch HTTP request: it is only
// interrupted by cancellation, shutdown, a limit/feedback fault, or completion.
func (r *Runtime) startRamp(runID string, target, limit, step float64, interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	r.mu.Lock()
	r.ramps[runID] = cancel
	r.mu.Unlock()

	plan := pressure.Plan{
		RunID:          runID,
		From:           r.Pressure.Current(),
		Target:         target,
		Step:           step,
		Interval:       interval,
		Limit:          limit,
		Tolerance:      compressor.DefaultTolerance,
		FeedbackMaxAge: compressor.DefaultFeedbackMaxAge,
	}
	r.Compressor.Ramp(ctx, plan)
}

// rampProgress mirrors every accepted ramp step into the observable control
// state so field operators can watch the pressure climb along the slope.
func (r *Runtime) rampProgress(runID string, commanded, observed float64, step int) {
	now := time.Now().UTC()
	r.mu.Lock()
	for stationID, pressureState := range r.control.Pressures {
		if pressureState.RunID != runID {
			continue
		}
		pressureState.Value = observed
		pressureState.Mode = model.CompressorRamping
		pressureState.ObservedAt = now
		r.control.Pressures[stationID] = pressureState
	}
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()
	_ = r.appendEvent(model.Event{
		RunID:   runID,
		Kind:    "pressurization_progress",
		Payload: fmt.Sprintf("step=%d/commanded=%.3f/observed=%.3f", step, commanded, observed),
		At:      now,
	})
}

// rampFinished is the single terminal hook for a ramp. On success the run is
// promoted to launched. Any interruption records the pressure actually reached
// and the reason, drives the run to cancelled, releases its routes, and takes
// the pressure controller offline so the old command can never be continued.
func (r *Runtime) rampFinished(report pressure.Report) {
	runID := report.RunID
	r.mu.Lock()
	delete(r.ramps, runID)
	r.mu.Unlock()

	// A cancellation landing on the final tick wins. run.Launch is an atomic
	// pressurizing->launched transition, so it only succeeds if the ramp beat
	// the cancellation; if the run is already cancelled it fails and the ramp is
	// settled as an interruption even though it reported completion.
	if report.Complete {
		if err := run.Launch(r.Runs, runID); err != nil {
			report.Complete = false
			if report.Reason == pressure.ReasonCompleted {
				report.Reason = pressure.ReasonCancelled
			}
		}
	}

	now := report.At
	r.mu.Lock()
	for stationID, pressureState := range r.control.Pressures {
		if pressureState.RunID != runID {
			continue
		}
		pressureState.ReachedPressure = report.Reached
		pressureState.Value = report.Reached
		pressureState.ObservedAt = now
		if report.Complete {
			pressureState.Mode = model.CompressorHolding
			pressureState.StopReason = ""
		} else {
			pressureState.Mode = model.CompressorStopped
			pressureState.StopReason = string(report.Reason)
		}
		r.control.Pressures[stationID] = pressureState
	}
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()

	if report.Complete {
		_ = r.appendEvent(model.Event{
			RunID:   runID,
			Kind:    "pressurization_reached",
			Payload: fmt.Sprintf("reached=%.3f/target=%.3f", report.Reached, report.Target),
			At:      now,
		})
		return
	}

	// Interrupted: make the interruption durable and fence the run off. The old
	// command is stopped and can never be continued.
	r.Pressure.Stop()
	if record, ok := r.Runs.Get(runID); ok && !record.State.Terminal() {
		_ = r.Runs.Cancel(runID)
	}
	r.releaseRoutesForRun(runID)
	severity := "high"
	if report.Reason == pressure.ReasonCancelled {
		// Operator or shutdown cancellation is an expected, commanded stop.
		severity = "warning"
	}
	message := fmt.Sprintf("pressurization aborted at %.3f of target %.3f: %s", report.Reached, report.Target, report.Reason)
	r.recordIncident(runID, severity, message)
	_ = r.appendEvent(model.Event{
		RunID:   runID,
		Kind:    "pressurization_aborted",
		Payload: fmt.Sprintf("reason=%s/reached=%.3f/target=%.3f", report.Reason, report.Reached, report.Target),
		At:      now,
	})
}

// releaseRoutesForRun releases every route still fenced to the run. Releasing
// is idempotent: racing a concurrent Cancel is harmless.
func (r *Runtime) releaseRoutesForRun(runID string) {
	r.mu.Lock()
	for id, routeState := range r.control.Routes {
		if routeState.RunID != runID {
			continue
		}
		r.Routes.Release(id, runID)
		routeState.RunID = ""
		routeState.Reserved = false
		routeState.LeaseEpoch = 0
		routeState.UpdatedAt = time.Now().UTC()
		r.control.Routes[id] = routeState
	}
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()
}

// claimCompressor atomically reserves the single physical compressor for a new
// run. While any ramp is live, a second launch is rejected rather than allowed
// to fight over the hardware.
func (r *Runtime) claimCompressor(runID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ramps) > 0 {
		return false
	}
	r.ramps[runID] = nil
	return true
}

// releaseCompressor surrenders a claim that never produced a running ramp.
func (r *Runtime) releaseCompressor(runID string) {
	r.mu.Lock()
	delete(r.ramps, runID)
	r.mu.Unlock()
}

// cancelRamp signals the run's background ramp, if any, to stop immediately.
func (r *Runtime) cancelRamp(runID string) {
	r.mu.Lock()
	cancel := r.ramps[runID]
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// cancelAllRamps aborts every background ramp (used during shutdown).
func (r *Runtime) cancelAllRamps() {
	r.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(r.ramps))
	for _, cancel := range r.ramps {
		cancels = append(cancels, cancel)
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// reconcileOrphanedRamps settles runs that were pressurizing when the process
// vanished. Their in-memory ramp command died with the process, so they must
// never resume: each is recorded with the pressure it last reached and an
// "interrupted" reason, driven to cancelled, and its routes released.
func (r *Runtime) reconcileOrphanedRamps(snap model.ControlSnapshot) {
	now := time.Now().UTC()
	orphaned := make(map[string]float64)
	for _, record := range snap.Runs {
		if record.State == model.StatePressurizing {
			orphaned[record.ID] = 0
		}
	}
	if len(orphaned) == 0 {
		return
	}

	r.mu.Lock()
	for stationID, pressureState := range r.control.Pressures {
		reached, ok := orphaned[pressureState.RunID]
		if !ok {
			continue
		}
		if pressureState.Value > reached {
			reached = pressureState.Value
		}
		orphaned[pressureState.RunID] = reached
		pressureState.Value = reached
		pressureState.ReachedPressure = reached
		pressureState.StopReason = string(pressure.ReasonInterrupted)
		pressureState.Mode = model.CompressorStopped
		pressureState.ObservedAt = now
		r.control.Pressures[stationID] = pressureState
	}
	for id, routeState := range r.control.Routes {
		if _, ok := orphaned[routeState.RunID]; !ok {
			continue
		}
		r.Routes.Release(id, routeState.RunID)
		routeState.RunID = ""
		routeState.Reserved = false
		routeState.LeaseEpoch = 0
		routeState.UpdatedAt = now
		r.control.Routes[id] = routeState
	}
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()

	// Reflect the last measured pressure on the recovered hardware controller,
	// but leave it unauthorized: the old command is gone and cannot continue.
	lastReached := 0.0
	for runID, reached := range orphaned {
		if reached > lastReached {
			lastReached = reached
		}
		if record, ok := r.Runs.Get(runID); ok && !record.State.Terminal() {
			_ = r.Runs.Cancel(runID)
		}
		r.recordIncident(runID, "high", fmt.Sprintf("pressurization interrupted mid-ramp; settled at %.3f: %s", reached, pressure.ReasonInterrupted))
		_ = r.appendEvent(model.Event{
			RunID:   runID,
			Kind:    "pressurization_aborted",
			Payload: fmt.Sprintf("reason=%s/reached=%.3f", pressure.ReasonInterrupted, reached),
			At:      now,
		})
	}
	compressor.Restore(r.Pressure, lastReached)
}
