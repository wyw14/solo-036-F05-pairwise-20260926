package service

import (
	"context"
	"sync"

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
	ramps      map[string]context.CancelFunc
}

func NewRuntime() *Runtime {
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
	worker := compressor.NewWorker(p, runs)
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
		ramps:      make(map[string]context.CancelFunc),
	}
	// Field progress and the terminal report are driven by the ramp worker.
	worker.OnProgress(runtime.rampProgress).OnFinished(runtime.rampFinished)
	return runtime
}
