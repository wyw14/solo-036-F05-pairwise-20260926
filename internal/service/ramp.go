package service

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-036/internal/compressor"
	"github.com/wyw14/solo-036/internal/model"
)

// OnRampProgress 处理升压链的每一拍进展：刷新该站点的实时压力与压缩机模式，
// 供现场沿途检查（轮询 /operations/{id} 即可看到逐步逼近的过程）。
func (r *Runtime) OnRampProgress(progress compressor.Progress) {
	record, ok := r.Runs.Get(progress.RunID)
	if !ok {
		return
	}
	r.mu.Lock()
	if state, exists := r.control.Pressures[record.Station]; exists {
		state.RunID = progress.RunID
		state.Value = progress.Value
		state.Target = progress.Target
		state.Mode = progress.Status.Mode()
		state.ObservedAt = progress.At.UTC()
		r.control.Pressures[record.Station] = state
		r.control.Revision = model.NewRevision()
	}
	r.mu.Unlock()
}

// OnRampFinished 处理升压链结局：无论到达还是中断，都把实际达到的压力、
// 压缩机最终模式和停机原因落到控制快照；中断额外记一条异常事件。
func (r *Runtime) OnRampFinished(progress compressor.Progress) {
	record, ok := r.Runs.Get(progress.RunID)
	if !ok {
		return
	}
	r.mu.Lock()
	if state, exists := r.control.Pressures[record.Station]; exists {
		state.RunID = progress.RunID
		state.Value = progress.Value
		state.Target = progress.Target
		state.Mode = progress.Status.Mode()
		state.ObservedAt = progress.At.UTC()
		r.control.Pressures[record.Station] = state
		r.control.Revision = model.NewRevision()
	}
	r.mu.Unlock()

	reason := progress.Reason
	if reason == model.StopNone {
		reason = model.StopReached
	}
	_ = r.appendEvent(model.Event{
		RunID:   progress.RunID,
		Kind:    "ramp_finished",
		Payload: fmt.Sprintf("%s:%.3f/%.3f", reason, progress.Value, progress.Target),
		At:      progress.At.UTC(),
	})
	if progress.Status == model.RampAborted {
		r.recordIncident(progress.RunID, "high",
			fmt.Sprintf("pressurization aborted (%s) at %.3f of %.3f", reason, progress.Value, progress.Target))
	}
}

// reconcileRampsAfterRecovery 在进程重启恢复后对账：重启前仍在“升压中”的命令
// 不可能继续，必须落定为关停中断，留下实际达到的压力与停机原因；已经到达目标
// 的命令则登记 reached 结局。控制器始终恢复为未授权，杜绝旧命令继续写压。
func (r *Runtime) reconcileRampsAfterRecovery(pressures []model.PressureState) (interrupted, reached int) {
	observed := make(map[string]model.PressureState, len(pressures))
	for _, state := range pressures {
		observed[state.RunID] = state
	}
	for _, record := range r.Runs.Snapshot() {
		state, ok := observed[record.ID]
		if !ok {
			continue
		}
		switch record.State {
		case model.StatePressurizing:
			// 仍在升压途中就重启：按关停中断留痕并把命令置为取消。
			_ = r.Runs.RecordOutcome(record.ID, state.Value, model.StopShutdown)
			if err := r.Runs.Cancel(record.ID); err == nil {
				r.mu.Lock()
				r.control.Pressures[state.StationID] = stoppedPressure(state)
				// 释放仍指向这条已死命令的路由租约，与操作员取消语义一致。
				for routeID, routeState := range r.control.Routes {
					if routeState.RunID == record.ID {
						r.Routes.Release(routeID, record.ID)
						routeState.RunID = ""
						routeState.Reserved = false
						routeState.LeaseEpoch = 0
						r.control.Routes[routeID] = routeState
					}
				}
				r.control.Revision = model.NewRevision()
				r.mu.Unlock()
				interrupted++
			}
			_ = r.appendEvent(model.Event{
				RunID:   record.ID,
				Kind:    "ramp_finished",
				Payload: fmt.Sprintf("%s:%.3f/%.3f", model.StopShutdown, state.Value, state.Target),
			})
		case model.StateLaunched, model.StateTracking, model.StateReceiving:
			// 已到达目标并进入后续流程：登记 reached 结局，保持保压观测值。
			if record.StopReason == model.StopNone {
				_ = r.Runs.RecordOutcome(record.ID, state.Value, model.StopReached)
				reached++
			}
		}
	}
	return interrupted, reached
}

func stoppedPressure(state model.PressureState) model.PressureState {
	state.Mode = model.CompressorStopped
	return state
}

// buildRamp 依据命令记录与实时压力状态，组装对外暴露的升压进展。
func (r *Runtime) buildRamp(record model.Run, pressureState model.PressureState) model.RampStatus {
	status := model.RampStatus{
		RunID:     record.ID,
		Target:    record.TargetPressure,
		Achieved:  pressureState.Value,
		StepSize:  record.RampRate,
		UpdatedAt: time.Now().UTC(),
	}
	switch {
	case record.StopReason == model.StopReached:
		status.Status = model.RampReached
		status.StopReason = model.StopReached
		status.Achieved = record.AchievedPressure
	case record.StopReason.Terminal():
		status.Status = model.RampAborted
		status.StopReason = record.StopReason
		status.Achieved = record.AchievedPressure
	case record.State == model.StatePressurizing:
		status.Status = model.RampRunning
	default:
		status.Status = model.RampIdle
	}
	return status
}
