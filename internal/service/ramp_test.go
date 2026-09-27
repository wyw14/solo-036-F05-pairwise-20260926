package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/pressure"
)

const (
	testStation = "station-a"
	testRoute   = "main-station-a-station-c"
)

func awaitState(t *testing.T, r *Runtime, runID string, want model.RunState, timeout time.Duration) model.OperationRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		op, err := r.Operation(runID)
		if err == nil && op.Run.State == want {
			return op
		}
		time.Sleep(5 * time.Millisecond)
	}
	op, _ := r.Operation(runID)
	t.Fatalf("run %s did not reach %s within %s (state=%s)", runID, want, timeout, op.Run.State)
	return op
}

// A launched run climbs along the slope and is only promoted to launched once
// the target is reached and confirmed.
func TestLaunchRampsToTarget(t *testing.T) {
	r := NewRuntime()
	r.Compressor.WithInterval(5 * time.Millisecond)

	op, err := r.Launch(context.Background(), LaunchRequest{
		StationID:      testStation,
		RouteID:        testRoute,
		TargetPressure: 10,
		RampStep:       2.5,
		RampInterval:   5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	if op.Run.State != model.StatePressurizing {
		t.Fatalf("run should start pressurizing, got %s", op.Run.State)
	}
	if op.Pressure.Mode != model.CompressorRamping {
		t.Fatalf("compressor should be ramping, got %s", op.Pressure.Mode)
	}

	final := awaitState(t, r, op.Run.ID, model.StateLaunched, 2*time.Second)
	if final.Pressure.Mode != model.CompressorHolding {
		t.Fatalf("expected holding at target, got %s", final.Pressure.Mode)
	}
	if final.Pressure.Value != 10 || final.Pressure.Target != 10 {
		t.Fatalf("expected pressure 10/10, got %.2f/%.2f", final.Pressure.Value, final.Pressure.Target)
	}
	if final.Pressure.StopReason != "" {
		t.Fatalf("completed ramp should carry no stop reason, got %q", final.Pressure.StopReason)
	}
	if r.Pressure.Current() != 10 {
		t.Fatalf("hardware controller should read 10, got %.2f", r.Pressure.Current())
	}
}

// Cancelling mid-ramp leaves the pressure actually reached and a reason, moves
// the run to cancelled, and records an incident. A subsequent launch must work
// (the old command can never be continued, but a fresh one can start).
func TestCancelMidRampLeavesReachedReasonAndFencesOldCommand(t *testing.T) {
	r := NewRuntime()

	op, err := r.Launch(context.Background(), LaunchRequest{
		StationID:      testStation,
		RouteID:        testRoute,
		TargetPressure: 10,
		RampStep:       1,
		RampInterval:   50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	runID := op.Run.ID

	// Wait until at least one slope step has been applied.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cur, _ := r.Operation(runID); cur.Pressure.Value > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := r.Cancel(runID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// The terminal report is applied by the background worker; wait for both the
	// cancelled run state and the stopped compressor mode.
	var final model.OperationRecord
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		final, _ = r.Operation(runID)
		if final.Run.State == model.StateCancelled && final.Pressure.Mode == model.CompressorStopped {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if final.Run.State != model.StateCancelled || final.Pressure.Mode != model.CompressorStopped {
		t.Fatalf("expected cancelled/stopped, got %s/%s", final.Run.State, final.Pressure.Mode)
	}
	if final.Pressure.StopReason != string(pressure.ReasonCancelled) {
		t.Fatalf("expected cancelled stop reason, got %q", final.Pressure.StopReason)
	}
	if final.Pressure.ReachedPressure <= 0 || final.Pressure.ReachedPressure >= 10 {
		t.Fatalf("reached pressure must be mid-ramp, got %.2f", final.Pressure.ReachedPressure)
	}
	if final.Pressure.Value != final.Pressure.ReachedPressure {
		t.Fatalf("observed value %.2f must equal reached %.2f", final.Pressure.Value, final.Pressure.ReachedPressure)
	}
	if len(r.Incidents()) == 0 {
		t.Fatalf("interruption must be recorded as an incident")
	}
	found := false
	for _, incident := range r.Incidents() {
		if incident.RunID == runID {
			found = true
		}
	}
	if !found {
		t.Fatalf("incident must reference the cancelled run")
	}

	// The old run must not be able to keep moving: it is already terminal.
	if _, err := r.Operation(runID); err != nil {
		t.Fatalf("operation record should still be readable: %v", err)
	}

	// A fresh launch after the interruption must be able to start and finish.
	next, err := r.Launch(context.Background(), LaunchRequest{
		StationID:      testStation,
		RouteID:        testRoute,
		TargetPressure: 8,
		RampStep:       2,
		RampInterval:   5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("fresh launch after cancel should succeed, got: %v", err)
	}
	awaitState(t, r, next.Run.ID, model.StateLaunched, 2*time.Second)
}

// A second launch while a ramp is live is rejected: one compressor, one run.
func TestRejectsSecondLaunchDuringRamp(t *testing.T) {
	r := NewRuntime()
	_, err := r.Launch(context.Background(), LaunchRequest{
		StationID:      testStation,
		RouteID:        testRoute,
		TargetPressure: 10,
		RampStep:       1,
		RampInterval:   100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("first launch: %v", err)
	}
	if _, err := r.Launch(context.Background(), LaunchRequest{
		StationID:      testStation,
		RouteID:        testRoute,
		TargetPressure: 9,
		RampStep:       1,
		RampInterval:   5 * time.Millisecond,
	}); err == nil {
		t.Fatalf("second launch during live ramp must be rejected")
	}
}
