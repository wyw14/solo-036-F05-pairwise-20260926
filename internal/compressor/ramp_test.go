package compressor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/compressor"
	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/pressure"
	"github.com/wyw14/solo-036/internal/run"
)

type recordingSink struct {
	mu       sync.Mutex
	progress []compressor.Progress
	finished []compressor.Progress
	stepped  chan struct{}
}

func newRecordingSink() *recordingSink {
	return &recordingSink{stepped: make(chan struct{}, 64)}
}

func (s *recordingSink) OnRampProgress(p compressor.Progress) {
	s.mu.Lock()
	s.progress = append(s.progress, p)
	s.mu.Unlock()
	select {
	case s.stepped <- struct{}{}:
	default:
	}
}

func (s *recordingSink) OnRampFinished(p compressor.Progress) {
	s.mu.Lock()
	s.finished = append(s.finished, p)
	s.mu.Unlock()
}

func (s *recordingSink) lastFinished() (compressor.Progress, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.finished) == 0 {
		return compressor.Progress{}, false
	}
	return s.finished[len(s.finished)-1], true
}

func setup(t *testing.T) (*pressure.Controller, *run.Store, string) {
	t.Helper()
	p := pressure.NewController()
	p.Authorize()
	runs := run.NewStore()
	created := runs.Create("station-a")
	return p, runs, created.ID
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ramp did not finish in time")
	}
}

func TestRampReachesTargetGradually(t *testing.T) {
	p, runs, id := setup(t)
	sink := newRecordingSink()
	w := compressor.NewWorker(p, runs, sink)

	done, err := w.Start(context.Background(), compressor.Plan{
		RunID: id, Target: 10, Step: 2.5, Limit: 12, Interval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitDone(t, done)

	if p.Current() != 10 {
		t.Fatalf("current = %.2f, want 10", p.Current())
	}
	achieved, reason, finished := p.Outcome(id)
	if !finished || reason != model.StopReached || achieved != 10 {
		t.Fatalf("outcome = %.2f/%s/%v", achieved, reason, finished)
	}
	sink.mu.Lock()
	n := len(sink.progress)
	sink.mu.Unlock()
	if n < 3 {
		t.Fatalf("expected gradual progress samples, got %d", n)
	}
	record, _ := runs.Get(id)
	if record.StopReason != model.StopReached || record.AchievedPressure != 10 {
		t.Fatalf("run outcome = %.2f/%s", record.AchievedPressure, record.StopReason)
	}
}

func TestRampCancelLeavesAchievedAndReason(t *testing.T) {
	p, runs, id := setup(t)
	sink := newRecordingSink()
	w := compressor.NewWorker(p, runs, sink)

	done, err := w.Start(context.Background(), compressor.Plan{
		RunID: id, Target: 100, Step: 5, Limit: 120, Interval: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// 等到至少完成一拍真实升压再取消。
	<-sink.stepped
	<-sink.stepped

	cancelDone, running := w.Cancel(id)
	if !running {
		t.Fatal("expected ramp to be running")
	}
	waitDone(t, cancelDone)
	waitDone(t, done)

	achieved, reason, finished := p.Outcome(id)
	if !finished || reason != model.StopCancelled {
		t.Fatalf("outcome = %.2f/%s/%v, want cancelled", achieved, reason, finished)
	}
	if achieved <= 0 || achieved >= 100 {
		t.Fatalf("achieved = %.2f, want partial pressure", achieved)
	}
	record, _ := runs.Get(id)
	if record.State != model.StateCancelled {
		t.Fatalf("run state = %s, want cancelled", record.State)
	}
	if record.StopReason != model.StopCancelled || record.AchievedPressure != achieved {
		t.Fatalf("run outcome mismatch: %.2f/%s", record.AchievedPressure, record.StopReason)
	}
	// 旧命令此后不能继续写入。
	if err := p.Apply(id, 99); !errors.Is(err, pressure.ErrFinished) {
		t.Fatalf("apply after cancel = %v, want ErrFinished", err)
	}
	fin, ok := sink.lastFinished()
	if !ok || fin.Status != model.RampAborted || fin.Reason != model.StopCancelled || fin.Value != achieved {
		t.Fatalf("finished callback = %+v ok=%v", fin, ok)
	}
}

func TestRampAbortsOnLimit(t *testing.T) {
	p, runs, id := setup(t)
	w := compressor.NewWorker(p, runs, nil)

	done, err := w.Start(context.Background(), compressor.Plan{
		RunID: id, Target: 100, Step: 2, Limit: 120, Interval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// 第一拍立即到 2；在第二拍（50ms 后）之前收紧限值到 3，
	// 下一拍将到 4，越限即停机。
	time.Sleep(10 * time.Millisecond)
	if err := p.SetLimit(id, 3); err != nil {
		t.Fatalf("set limit: %v", err)
	}
	waitDone(t, done)

	achieved, reason, finished := p.Outcome(id)
	if !finished || reason != model.StopLimitExceeded {
		t.Fatalf("outcome = %.2f/%s/%v, want limit_exceeded", achieved, reason, finished)
	}
	if achieved != 2 {
		t.Fatalf("achieved = %.2f, want 2 (last safe step)", achieved)
	}
}

func TestRampShutdownReason(t *testing.T) {
	p, runs, id := setup(t)
	sink := newRecordingSink()
	w := compressor.NewWorker(p, runs, sink)

	done, err := w.Start(context.Background(), compressor.Plan{
		RunID: id, Target: 100, Step: 5, Limit: 120, Interval: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	<-sink.stepped

	dones := w.Shutdown()
	if len(dones) != 1 {
		t.Fatalf("shutdown returned %d ramps, want 1", len(dones))
	}
	waitDone(t, done)

	_, reason, finished := p.Outcome(id)
	if !finished || reason != model.StopShutdown {
		t.Fatalf("reason = %s/%v, want shutdown", reason, finished)
	}
}

func TestStartRejectedWithoutAuthorization(t *testing.T) {
	p := pressure.NewController() // 未 Authorize
	runs := run.NewStore()
	id := runs.Create("station-a").ID
	w := compressor.NewWorker(p, runs, nil)

	if _, err := w.Start(context.Background(), compressor.Plan{
		RunID: id, Target: 10, Step: 1, Limit: 12,
	}); !errors.Is(err, pressure.ErrNotAuthorized) {
		t.Fatalf("start = %v, want ErrNotAuthorized", err)
	}
}

func TestParentContextCancelStopsRamp(t *testing.T) {
	p, runs, id := setup(t)
	sink := newRecordingSink()
	w := compressor.NewWorker(p, runs, sink)
	ctx, cancel := context.WithCancel(context.Background())

	done, err := w.Start(ctx, compressor.Plan{
		RunID: id, Target: 100, Step: 5, Limit: 120, Interval: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	<-sink.stepped
	cancel()
	waitDone(t, done)

	_, reason, finished := p.Outcome(id)
	// 父上下文取消默认登记为命令取消。
	if !finished || reason != model.StopCancelled {
		t.Fatalf("reason = %s/%v, want cancelled", reason, finished)
	}
}
