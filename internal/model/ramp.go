package model

import "time"

// CompressorStatus 是升压链自身的生命周期状态，比面向快照的 CompressorMode
// 更贴近执行过程：空闲、升压中、已达目标、已中断。
type CompressorStatus string

const (
	RampIdle    CompressorStatus = "idle"
	RampRunning CompressorStatus = "running"
	RampReached CompressorStatus = "reached"
	RampAborted CompressorStatus = "aborted"
)

// Mode 把升压链状态映射为控制快照使用的压缩机模式。
func (s CompressorStatus) Mode() CompressorMode {
	switch s {
	case RampRunning:
		return CompressorRamping
	case RampReached:
		return CompressorHolding
	case RampAborted:
		return CompressorStopped
	default:
		return CompressorStandby
	}
}

// StopReason 记录升压链停下来的原因。空值表示仍在运行或正常到达目标。
type StopReason string

const (
	StopNone          StopReason = ""
	StopReached       StopReason = "reached"
	StopCancelled     StopReason = "cancelled"
	StopShutdown      StopReason = "shutdown"
	StopRunMissing    StopReason = "run_missing"
	StopRunTerminal   StopReason = "run_terminal"
	StopLimitExceeded StopReason = "limit_exceeded"
	StopApplyRejected StopReason = "apply_rejected"
	StopFeedback      StopReason = "feedback_mismatch"
)

func (r StopReason) Terminal() bool { return r != StopNone }

// RampStatus 描述一次按斜率升压的实际进展，任何中断都会在这里留下
// 实际达到的压力 Achieved 与停机原因 StopReason。
type RampStatus struct {
	RunID      string           `json:"run_id"`
	Target     float64          `json:"target"`
	Achieved   float64          `json:"achieved"`
	StepSize   float64          `json:"step_size"`
	Limit      float64          `json:"limit"`
	Status     CompressorStatus `json:"status"`
	StopReason StopReason       `json:"stop_reason,omitempty"`
	StoppedAt  time.Time        `json:"stopped_at,omitempty"`
	UpdatedAt  time.Time        `json:"updated_at"`
}
