package service

import (
	"context"
	"time"

	"github.com/wyw14/solo-036/internal/compressor"
	"github.com/wyw14/solo-036/internal/emergency"
	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/launcher"
	"github.com/wyw14/solo-036/internal/run"
	"github.com/wyw14/solo-036/internal/tracker"
)

// shutdownRampGrace 是关停时等待升压链完成留痕的最长时间。
const shutdownRampGrace = 2 * time.Second

func (r *Runtime) Bootstrap() {
	r.ensureControl()
	_ = journal.Healthy(r.Journal)
	_ = tracker.Ready(r.Tracker)
	_ = run.Health(r.Runs)
	_ = launcher.Ready(r.Launcher)
	_ = compressor.Healthy(r.Compressor)
	_ = emergency.Safe(r.Pressure)
}

// Shutdown 中断全部升压链并等待它们落定“实际压力 + 停机原因”，再停机硬件。
func (r *Runtime) Shutdown(ctx context.Context) error {
	dones := r.Compressor.Shutdown()
	deadline := time.NewTimer(shutdownRampGrace)
	defer deadline.Stop()
	for _, done := range dones {
		select {
		case <-done:
		case <-deadline.C:
		case <-ctx.Done():
		}
	}
	r.cancelRamp()
	r.Pressure.Stop()
	return ctx.Err()
}
