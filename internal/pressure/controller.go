package pressure

import (
	"sync"
)

type Controller struct {
	mu      sync.Mutex
	current float64
	enabled bool
	stopped bool
	history []float64
	cmd     *command
}

func NewController() *Controller { return &Controller{history: make([]float64, 0)} }

func (c *Controller) Authorize() {
	c.mu.Lock()
	c.enabled = true
	c.stopped = false
	c.mu.Unlock()
}

// Stop deauthorizes the controller and aborts any live ramp. The reached
// pressure and the cancellation reason are preserved on the command's report.
func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	c.enabled = false
	c.closeCommandLocked(ReasonCancelled)
}

// Reset aborts any live ramp and clears the controller for emergency
// depressurization. The terminal report is dropped along with command state.
func (c *Controller) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = 0
	c.history = nil
	c.enabled = false
	c.stopped = false
	c.cmd = nil
}

// Recover restores a measured pressure after a process restart. No command can
// be live across a restart, so callers must issue a fresh Begin before ramping.
func (c *Controller) Recover(value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = value
	c.enabled = false
	c.stopped = false
	c.cmd = nil
}
