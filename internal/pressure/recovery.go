package pressure

import "github.com/wyw14/solo-036/internal/model"

func (c *Controller) Recover(value float64) {
	c.mu.Lock()
	c.current = value
	c.enabled = false
	c.stopped = false
	c.activeRun = ""
	c.lastReason = model.StopNone
	c.mu.Unlock()
}
func (c *Controller) Reset() {
	c.mu.Lock()
	c.current = 0
	c.history = nil
	c.enabled = false
	c.stopped = false
	c.activeRun = ""
	c.target = 0
	c.limit = 0
	c.step = 0
	c.lastRun = ""
	c.lastAchieved = 0
	c.lastReason = model.StopNone
	c.mu.Unlock()
}
