package compressor

import (
	"context"
	"sync"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/pressure"
	"github.com/wyw14/solo-036/internal/run"
)

// DefaultStepInterval 是升压链相邻两拍之间的默认节拍。
const DefaultStepInterval = 50 * time.Millisecond

// Plan 描述一次按斜率升压：从当前压力出发，每拍增加 Step，直到 Target，
// 全程不得超过 Limit。
type Plan struct {
	RunID    string
	Target   float64
	Step     float64
	Limit    float64
	Interval time.Duration
}

// Progress 是升压链向服务层汇报的一次进展或最终结局。
type Progress struct {
	RunID  string
	Value  float64
	Target float64
	Status model.CompressorStatus
	Reason model.StopReason
	At     time.Time
}

// ProgressSink 接收升压链的沿途进展与结局。由服务层实现，
// 用来刷新控制快照、追加日志、记录异常事件。
type ProgressSink interface {
	OnRampProgress(progress Progress)
	OnRampFinished(progress Progress)
}

type noopSink struct{}

func (noopSink) OnRampProgress(Progress) {}
func (noopSink) OnRampFinished(Progress) {}

type activeRamp struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu     sync.Mutex
	reason model.StopReason
}

func (a *activeRamp) setReason(reason model.StopReason) {
	a.mu.Lock()
	a.reason = reason
	a.mu.Unlock()
}

func (a *activeRamp) stopReason() model.StopReason {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reason
}

type Worker struct {
	pressure *pressure.Controller
	runs     *run.Store
	sink     ProgressSink

	mu     sync.Mutex
	active map[string]*activeRamp
}

func NewWorker(p *pressure.Controller, r *run.Store, sink ProgressSink) *Worker {
	if sink == nil {
		sink = noopSink{}
	}
	return &Worker{pressure: p, runs: r, sink: sink, active: make(map[string]*activeRamp)}
}

func (w *Worker) interval(plan Plan) time.Duration {
	if plan.Interval > 0 {
		return plan.Interval
	}
	return DefaultStepInterval
}

// SetSink 绑定进度回调。必须在第一次 Start 之前调用（构造期绑定）。
func (w *Worker) SetSink(sink ProgressSink) {
	if sink == nil {
		sink = noopSink{}
	}
	w.sink = sink
}
