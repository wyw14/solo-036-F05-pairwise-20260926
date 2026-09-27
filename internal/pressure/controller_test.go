package pressure_test

import (
	"errors"
	"testing"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/pressure"
)

func newAuthorized() *pressure.Controller {
	c := pressure.NewController()
	c.Authorize()
	return c
}

func TestStepTowardApproachesTargetAndCompletes(t *testing.T) {
	c := newAuthorized()
	if err := c.Begin("run-1", 10, 12); err != nil {
		t.Fatalf("begin: %v", err)
	}
	// 0 -> 4 -> 8 -> 10（最后一拍夹到目标值，不超调）。
	want := []float64{4, 8, 10}
	for i, expected := range want {
		got, err := c.StepToward("run-1", 4)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if got != expected {
			t.Fatalf("step %d = %.2f, want %.2f", i, got, expected)
		}
	}
	achieved, err := c.Complete("run-1")
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if achieved != 10 {
		t.Fatalf("achieved = %.2f, want 10", achieved)
	}
	gotAchieved, reason, finished := c.Outcome("run-1")
	if !finished || reason != model.StopReached || gotAchieved != 10 {
		t.Fatalf("outcome = %.2f/%s/%v", gotAchieved, reason, finished)
	}
}

func TestFinishedRunCannotContinue(t *testing.T) {
	c := newAuthorized()
	if err := c.Begin("run-1", 10, 12); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := c.StepToward("run-1", 5); err != nil {
		t.Fatalf("step: %v", err)
	}
	if _, err := c.Abort("run-1", model.StopCancelled); err != nil {
		t.Fatalf("abort: %v", err)
	}
	// 旧命令此后既不能继续写入，也不能再步进。
	if err := c.Apply("run-1", 9); !errors.Is(err, pressure.ErrFinished) {
		t.Fatalf("apply after abort = %v, want ErrFinished", err)
	}
	if _, err := c.StepToward("run-1", 1); !errors.Is(err, pressure.ErrFinished) {
		t.Fatalf("step after abort = %v, want ErrFinished", err)
	}
	achieved, reason, finished := c.Outcome("run-1")
	if !finished || reason != model.StopCancelled || achieved != 5 {
		t.Fatalf("outcome = %.2f/%s/%v, want 5/cancelled/true", achieved, reason, finished)
	}
}

func TestFenceRejectsStrangerAndPreempt(t *testing.T) {
	c := newAuthorized()
	if err := c.Begin("run-1", 10, 12); err != nil {
		t.Fatalf("begin: %v", err)
	}
	// 别的命令不能抢占，也不能写入。
	if err := c.Begin("run-2", 10, 12); !errors.Is(err, pressure.ErrNotFenced) {
		t.Fatalf("preempt = %v, want ErrNotFenced", err)
	}
	if err := c.Apply("run-2", 1); !errors.Is(err, pressure.ErrNotFenced) {
		t.Fatalf("stranger apply = %v, want ErrNotFenced", err)
	}
}

func TestBeginRejectsTargetOverLimit(t *testing.T) {
	c := newAuthorized()
	if err := c.Begin("run-1", 13, 12); err == nil {
		t.Fatal("begin over limit should fail")
	}
}

func TestAbortStopsHardwareUntilReauthorized(t *testing.T) {
	c := newAuthorized()
	if err := c.Begin("run-1", 10, 12); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := c.Abort("run-1", model.StopLimitExceeded); err != nil {
		t.Fatalf("abort: %v", err)
	}
	// 停机后未重新授权，任何新命令都不能开始。
	if err := c.Begin("run-3", 10, 12); !errors.Is(err, pressure.ErrNotAuthorized) {
		t.Fatalf("begin while stopped = %v, want ErrNotAuthorized", err)
	}
	c.Authorize()
	if err := c.Begin("run-3", 10, 12); err != nil {
		t.Fatalf("begin after reauthorize: %v", err)
	}
}

func TestStopGlobalRecordsShutdown(t *testing.T) {
	c := newAuthorized()
	if err := c.Begin("run-1", 10, 12); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := c.StepToward("run-1", 3); err != nil {
		t.Fatalf("step: %v", err)
	}
	c.Stop()
	achieved, reason, finished := c.Outcome("run-1")
	if !finished || reason != model.StopShutdown || achieved != 3 {
		t.Fatalf("outcome = %.2f/%s/%v, want 3/shutdown/true", achieved, reason, finished)
	}
}
