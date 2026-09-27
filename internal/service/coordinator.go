package service

import (
	"context"
	"github.com/wyw14/solo-036/internal/compressor"
	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/launcher"
	"github.com/wyw14/solo-036/internal/permit"
	"github.com/wyw14/solo-036/internal/pressure"
	"github.com/wyw14/solo-036/internal/receiver"
	"github.com/wyw14/solo-036/internal/route"
	"github.com/wyw14/solo-036/internal/run"
	"github.com/wyw14/solo-036/internal/tracker"
	"github.com/wyw14/solo-036/internal/valve"
	"sync"
)

type Runtime struct {
	Runs       *run.Store
	Routes     *route.Coordinator
	Tracker    *tracker.Coordinator
	Lineage    *tracker.SourceLineage
	Journal    *journal.Store
	Pressure   *pressure.Controller
	Permits    *permit.Evaluator
	Launcher   *launcher.Controller
	Valves     *valve.Controller
	Receiver   *receiver.Controller
	Compressor *compressor.Worker
	mu         sync.Mutex
	control    *ControlState
	// rampCtx 与单次 HTTP 请求解耦：响应返回后升压链仍继续，直到到达目标、
	// 被显式取消或进程关停。
	rampCtx    context.Context
	cancelRamp context.CancelFunc
}

func NewRuntime() *Runtime {
	rampCtx, cancelRamp := context.WithCancel(context.Background())
	runs := run.NewStore()
	routes := route.NewCoordinator()
	tracks := tracker.NewCoordinator()
	lineage := tracker.NewSourceLineage()
	journalStore := journal.NewStore()
	p := pressure.NewController()
	permits := permit.NewEvaluator()
	launch := launcher.NewController(p, permits)
	valves := valve.NewController(p)
	recv := receiver.NewController()
	worker := compressor.NewWorker(p, runs, nil)
	runtime := &Runtime{
		Runs:       runs,
		Routes:     routes,
		Tracker:    tracks,
		Lineage:    lineage,
		Journal:    journalStore,
		Pressure:   p,
		Permits:    permits,
		Launcher:   launch,
		Valves:     valves,
		Receiver:   recv,
		Compressor: worker,
		control:    defaultControlState(),
		rampCtx:    rampCtx,
		cancelRamp: cancelRamp,
	}
	// 升压链沿途进展与结局由 Runtime 统一落到控制快照与日志。
	worker.SetSink(runtime)
	return runtime
}
