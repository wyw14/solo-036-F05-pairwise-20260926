package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/service"
)

// 升压途中进程崩溃：持久化快照里命令仍是 pressurizing。重启恢复后必须把它
// 落定为关停中断（留下实际压力与原因），且控制器未授权、旧命令无法继续。
func TestRecoveryAbortsInFlightRamp(t *testing.T) {
	dir := t.TempDir()

	rt1 := service.NewRuntime()
	if err := rt1.ConfigurePersistence(dir); err != nil {
		t.Fatalf("configure: %v", err)
	}
	op, err := rt1.Launch(context.Background(), launchRequest(10, 0.5, 40))
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	id := op.Run.ID

	waitFor(t, time.Second, func() bool {
		cur, err := rt1.Operation(id)
		return err == nil && cur.Pressure.Value > 0
	})
	mid, _ := rt1.Operation(id)
	snapshotValue := mid.Pressure.Value
	if err := rt1.PersistSnapshot(); err != nil {
		t.Fatalf("persist: %v", err)
	}
	// 模拟进程退出：关停后台升压（不再改写快照文件）。
	if err := rt1.Shutdown(context.Background()); err != nil && err != context.Canceled {
		t.Fatalf("shutdown: %v", err)
	}

	// 全新进程从同一目录恢复。
	rt2 := service.NewRuntime()
	if err := rt2.ConfigurePersistence(dir); err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	report, err := rt2.RecoverPersistent()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if !report.Ready || report.InterruptedRamps != 1 {
		t.Fatalf("report = %+v, want 1 interrupted ramp", report)
	}

	rec, err := rt2.Operation(id)
	if err != nil {
		t.Fatalf("operation: %v", err)
	}
	if rec.Run.State != model.StateCancelled {
		t.Fatalf("state = %s, want cancelled", rec.Run.State)
	}
	if rec.Run.StopReason != model.StopShutdown {
		t.Fatalf("stop reason = %s, want shutdown", rec.Run.StopReason)
	}
	if rec.Run.AchievedPressure != snapshotValue {
		t.Fatalf("achieved = %.2f, want snapshotted %.2f", rec.Run.AchievedPressure, snapshotValue)
	}
	if rec.Ramp == nil || rec.Ramp.Status != model.RampAborted {
		t.Fatalf("ramp = %+v, want aborted", rec.Ramp)
	}
	// 恢复后路由租约不得仍指向这条已死命令。
	if rec.Route.Reserved {
		t.Fatal("route lease should be released for the dead run")
	}
}
