package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/service"
)

func launchRequest(target, step float64, intervalMS int64) service.LaunchRequest {
	return service.LaunchRequest{
		StationID:      "station-a",
		RouteID:        "main-station-a-station-c",
		TargetPressure: target,
		RampStep:       step,
		RampIntervalMS: intervalMS,
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// 发射后应能沿途观测到压力按斜率逐步上升，到达目标后保压并推进到“已发射”。
func TestLaunchRampsGraduallyAndHolds(t *testing.T) {
	r := service.NewRuntime()
	r.Bootstrap()

	op, err := r.Launch(context.Background(), launchRequest(8, 2, 5))
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	id := op.Run.ID

	// 刚返回时应处于升压中，目标值已登记但压力尚未一次到位。
	if op.Run.State != model.StatePressurizing {
		t.Fatalf("initial state = %s, want pressurizing", op.Run.State)
	}
	if op.Ramp == nil || op.Ramp.Target != 8 || op.Ramp.Status != model.RampRunning {
		t.Fatalf("initial ramp = %+v", op.Ramp)
	}
	if op.Pressure.Value >= 8 {
		t.Fatalf("pressure reached target immediately: %.2f", op.Pressure.Value)
	}

	// 沿途检查：至少看到一次中间压力。
	sawIntermediate := false
	waitFor(t, 2*time.Second, func() bool {
		cur, err := r.Operation(id)
		if err != nil {
			return false
		}
		if cur.Pressure.Value > 0 && cur.Pressure.Value < 8 {
			sawIntermediate = true
		}
		if cur.Ramp != nil && cur.Ramp.Status == model.RampReached &&
			cur.Run.State == model.StateLaunched && cur.Pressure.Value == 8 {
			return sawIntermediate
		}
		return false
	})

	final, _ := r.Operation(id)
	if final.Run.StopReason != model.StopReached || final.Run.AchievedPressure != 8 {
		t.Fatalf("run outcome = %.2f/%s", final.Run.AchievedPressure, final.Run.StopReason)
	}
	if final.Pressure.Mode != model.CompressorHolding {
		t.Fatalf("mode = %s, want holding", final.Pressure.Mode)
	}
}

// 途中取消必须留下实际达到的压力和停机原因，且命令进入终态，旧命令不能再驱动压力。
func TestCancelMidRampRecordsAchievedAndReason(t *testing.T) {
	r := service.NewRuntime()
	r.Bootstrap()

	// 大目标、慢节拍，确保取消时还在升压途中（路径最薄弱管段承压 11.2）。
	op, err := r.Launch(context.Background(), launchRequest(10, 0.5, 40))
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	id := op.Run.ID

	// 等到有压力抬升后取消。
	waitFor(t, time.Second, func() bool {
		cur, err := r.Operation(id)
		return err == nil && cur.Pressure.Value > 0
	})

	if err := r.Cancel(id); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	final, err := r.Operation(id)
	if err != nil {
		t.Fatalf("operation: %v", err)
	}
	if final.Run.State != model.StateCancelled {
		t.Fatalf("state = %s, want cancelled", final.Run.State)
	}
	if final.Run.StopReason != model.StopCancelled {
		t.Fatalf("stop reason = %s, want cancelled", final.Run.StopReason)
	}
	if final.Run.AchievedPressure <= 0 || final.Run.AchievedPressure >= 10 {
		t.Fatalf("achieved = %.2f, want a partial pressure", final.Run.AchievedPressure)
	}
	if final.Ramp == nil || final.Ramp.Status != model.RampAborted {
		t.Fatalf("ramp = %+v, want aborted", final.Ramp)
	}
	if final.Pressure.Mode != model.CompressorStopped {
		t.Fatalf("mode = %s, want stopped", final.Pressure.Mode)
	}

	// 旧命令已终态，不能再触发跟踪等后续动作；路由租约也应已释放。
	if !final.Run.State.Terminal() {
		t.Fatal("cancelled run should be terminal")
	}
	if final.Route.Reserved {
		t.Fatal("route should be released after cancellation")
	}
}
