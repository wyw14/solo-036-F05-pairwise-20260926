package compressor

import (
	"context"
	"errors"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/pressure"
)

// feedbackTolerance 是写入设定值与观测反馈之间允许的最大偏差。
const feedbackTolerance = 1e-6

// Start 占用控制器并在后台启动一次按斜率升压。围栏（Begin）同步完成，
// 因此占用冲突、未授权、目标越限等错误会立即返回给调用方；成功后升压
// 在独立协程中运行，可通过返回的完成通道等待，或用 Cancel 中断。
func (w *Worker) Start(parent context.Context, plan Plan) (<-chan struct{}, error) {
	if plan.RunID == "" {
		return nil, errors.New("ramp requires a run id")
	}
	if plan.Target <= 0 {
		return nil, errors.New("ramp target must be positive")
	}
	if plan.Step <= 0 {
		return nil, errors.New("ramp step must be positive")
	}
	if plan.Limit > 0 && plan.Target > plan.Limit+1e-9 {
		return nil, errors.New("ramp target exceeds the pressure limit")
	}

	ctx, cancel := context.WithCancel(parent)
	active := &activeRamp{cancel: cancel, done: make(chan struct{}), reason: model.StopCancelled}

	w.mu.Lock()
	if _, busy := w.active[plan.RunID]; busy {
		w.mu.Unlock()
		cancel()
		return nil, errors.New("ramp already active for run " + plan.RunID)
	}
	w.active[plan.RunID] = active
	w.mu.Unlock()

	// 同步占用执行机构：失败则不留下后台协程。
	if err := w.pressure.Begin(plan.RunID, plan.Target, plan.Limit); err != nil {
		w.remove(plan.RunID)
		cancel()
		return nil, err
	}
	if err := w.runs.PlanRamp(plan.RunID, plan.Target, plan.Step); err != nil {
		_, _ = w.pressure.Abort(plan.RunID, model.StopApplyRejected)
		w.remove(plan.RunID)
		cancel()
		return nil, err
	}

	go w.runRamp(ctx, plan, active)
	return active.done, nil
}

// Cancel 请求中断某次升压并返回该升压的完成通道（调用方可等待留痕落定）。
// 返回 nil/false 表示该命令当前没有运行中的升压（可能已结束）。
func (w *Worker) Cancel(runID string) (<-chan struct{}, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	active, ok := w.active[runID]
	if !ok {
		return nil, false
	}
	active.setReason(model.StopCancelled)
	active.cancel()
	return active.done, true
}

// Shutdown 中断全部运行中的升压（原因为进程关停），返回需等待的完成通道集合。
func (w *Worker) Shutdown() []<-chan struct{} {
	w.mu.Lock()
	dones := make([]<-chan struct{}, 0, len(w.active))
	for _, active := range w.active {
		active.setReason(model.StopShutdown)
		active.cancel()
		dones = append(dones, active.done)
	}
	w.mu.Unlock()
	return dones
}

func (w *Worker) remove(runID string) {
	w.mu.Lock()
	delete(w.active, runID)
	w.mu.Unlock()
}

// runRamp 是升压链主体：按节拍逐步逼近目标，每一拍都核对取消、命令状态、
// 限值与观测反馈；任何中断都会留下实际达到的压力和停机原因。
func (w *Worker) runRamp(ctx context.Context, plan Plan, active *activeRamp) {
	defer func() {
		w.remove(plan.RunID)
		close(active.done)
	}()

	w.emit(Progress{
		RunID:  plan.RunID,
		Value:  w.pressure.Current(),
		Target: plan.Target,
		Status: model.RampRunning,
		At:     time.Now().UTC(),
	})

	timer := time.NewTimer(w.interval(plan))
	defer timer.Stop()

	for {
		// 1) 外部取消（操作员取消 / 进程关停）。
		if err := ctx.Err(); err != nil {
			w.abort(plan, active.stopReason())
			return
		}
		// 2) 命令本身是否仍然有效：缺失、已取消或进入终态都必须停机。
		if reason, gone := w.runGone(plan.RunID); gone {
			w.abort(plan, reason)
			return
		}
		// 3) 写入下一拍设定值（执行机构侧再次核对授权、围栏与限值）。
		value, err := w.pressure.StepToward(plan.RunID, plan.Step)
		if err != nil {
			w.abort(plan, classify(err))
			return
		}
		// 4) 观测反馈：回读实际压力，核对与设定值一致且未越限。
		if reason, ok := w.checkFeedback(plan, value); ok {
			w.abort(plan, reason)
			return
		}

		w.emit(Progress{
			RunID:  plan.RunID,
			Value:  value,
			Target: plan.Target,
			Status: model.RampRunning,
			At:     time.Now().UTC(),
		})

		if value >= plan.Target-1e-9 {
			w.reach(plan)
			return
		}

		// 5) 按设定斜率等待下一拍，期间保持可中断。
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(w.interval(plan))
		select {
		case <-ctx.Done():
			w.abort(plan, active.stopReason())
			return
		case <-timer.C:
		}
	}
}

// runGone 判断升压命令是否已失效，并给出对应的停机原因。
func (w *Worker) runGone(runID string) (model.StopReason, bool) {
	r, ok := w.runs.Get(runID)
	if !ok {
		return model.StopRunMissing, true
	}
	if r.State == model.StateCancelled {
		return model.StopCancelled, true
	}
	if r.State.Terminal() {
		return model.StopRunTerminal, true
	}
	return model.StopNone, false
}

// checkFeedback 回读执行机构反馈并核对。
func (w *Worker) checkFeedback(plan Plan, commanded float64) (model.StopReason, bool) {
	observed := w.pressure.Current()
	if plan.Limit > 0 && observed > plan.Limit+1e-9 {
		return model.StopLimitExceeded, true
	}
	diff := observed - commanded
	if diff < 0 {
		diff = -diff
	}
	if diff > feedbackTolerance {
		return model.StopFeedback, true
	}
	return model.StopNone, false
}

// reach 在到达目标后落定结局：保压、记录实际压力并推进命令状态。
func (w *Worker) reach(plan Plan) {
	achieved, err := w.pressure.Complete(plan.RunID)
	if err != nil {
		w.abort(plan, classify(err))
		return
	}
	_ = w.runs.RecordOutcome(plan.RunID, achieved, model.StopReached)
	// 升压完成才允许进入“已发射”阶段；若命令已被取消则不强行推进。
	if r, ok := w.runs.Get(plan.RunID); ok && r.State == model.StatePressurizing {
		_ = w.runs.Transition(plan.RunID, model.StateLaunched)
	}
	w.sink.OnRampFinished(Progress{
		RunID:  plan.RunID,
		Value:  achieved,
		Target: plan.Target,
		Status: model.RampReached,
		Reason: model.StopReached,
		At:     time.Now().UTC(),
	})
}

// abort 落定一次中断：记录实际达到的压力和停机原因，停机硬件并取消命令。
func (w *Worker) abort(plan Plan, reason model.StopReason) {
	achieved, err := w.pressure.Abort(plan.RunID, reason)
	if err != nil {
		// 控制器已无此命令（例如外部已停机）：以回读压力作为实际值留痕。
		achieved = w.pressure.Current()
	}
	_ = w.runs.RecordOutcome(plan.RunID, achieved, reason)
	// 终态命令保持原样，其余统一落到 cancelled，旧命令此后不得继续。
	if r, ok := w.runs.Get(plan.RunID); ok && !r.State.Terminal() {
		_ = w.runs.Cancel(plan.RunID)
	}
	w.sink.OnRampFinished(Progress{
		RunID:  plan.RunID,
		Value:  achieved,
		Target: plan.Target,
		Status: model.RampAborted,
		Reason: reason,
		At:     time.Now().UTC(),
	})
}

func (w *Worker) emit(progress Progress) {
	w.sink.OnRampProgress(progress)
}

// classify 把执行机构返回的错误映射为停机原因。
func classify(err error) model.StopReason {
	switch {
	case errors.Is(err, pressure.ErrLimit):
		return model.StopLimitExceeded
	case errors.Is(err, pressure.ErrFinished):
		return model.StopRunTerminal
	case errors.Is(err, pressure.ErrNotFenced):
		return model.StopRunTerminal
	case errors.Is(err, pressure.ErrNotAuthorized):
		return model.StopShutdown
	default:
		return model.StopApplyRejected
	}
}
