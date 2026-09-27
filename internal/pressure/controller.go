package pressure

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

// 哨兵错误，供升压链区分停机原因。
var (
	ErrNotAuthorized = errors.New("pressure not authorized")
	ErrNotFenced     = errors.New("pressure not fenced to run")
	ErrFinished      = errors.New("run already finished")
	ErrLimit         = errors.New("pressure exceeds limit")
)

type Controller struct {
	mu sync.Mutex
	// current 是执行机构当前实际压力；history 记录每一次成功写入。
	current float64
	history []float64
	// enabled 表示关断已确认、允许建压；stopped 表示硬件已停机。
	enabled bool
	stopped bool
	// activeRun 是当前唯一被允许写压力的命令；一次升压结束（到达或中断）
	// 后清空，旧 runID 再来写入会被拒绝，必须重新 Begin。
	activeRun string
	// lastRun/lastAchieved/lastReason 留下上一条命令实际达到的压力和停机原因。
	lastRun      string
	lastAchieved float64
	lastReason   model.StopReason
	target       float64
	limit        float64
	step         float64
	updatedAt    time.Time
	stoppedAt    time.Time
}

func NewController() *Controller { return &Controller{history: make([]float64, 0)} }

// Authorize 在关断确认后放开建压许可（不绑定具体命令）。
func (c *Controller) Authorize() {
	c.mu.Lock()
	c.enabled = true
	c.stopped = false
	c.mu.Unlock()
}

// Begin 把一次升压命令绑定到控制器：登记目标值与限值，并占用唯一的写权限。
// 上一条命令未结束前，不能用不同 runID 抢占。
func (c *Controller) Begin(runID string, target, limit float64) error {
	if runID == "" {
		return fmt.Errorf("missing run")
	}
	if target <= 0 {
		return fmt.Errorf("target pressure must be positive")
	}
	if limit > 0 && target > limit {
		return fmt.Errorf("target %.2f exceeds limit %.2f", target, limit)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled || c.stopped {
		return ErrNotAuthorized
	}
	if c.activeRun != "" && c.activeRun != runID {
		return fmt.Errorf("%w %s: pressure already fenced to run %s", ErrNotFenced, runID, c.activeRun)
	}
	c.activeRun = runID
	c.target = target
	c.limit = limit
	c.updatedAt = time.Now().UTC()
	c.lastRun = ""
	c.lastAchieved = 0
	c.lastReason = model.StopNone
	return nil
}

// Apply 向执行机构写入一个压力设定值。只有当前占用 activeRun 的命令、
// 且在授权未停机、不超过限值时才被接受。
func (c *Controller) Apply(runID string, value float64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if runID == "" {
		return fmt.Errorf("missing run")
	}
	// 已落定结局的旧命令身份优先识别：它永远不能再继续。
	if c.lastRun == runID && c.lastReason.Terminal() {
		return fmt.Errorf("%w %s: %s at %.2f", ErrFinished, runID, c.lastReason, c.lastAchieved)
	}
	if !c.enabled || c.stopped {
		return ErrNotAuthorized
	}
	if c.activeRun != runID {
		return fmt.Errorf("%w %s", ErrNotFenced, runID)
	}
	if value < 0 {
		return fmt.Errorf("pressure must be non-negative")
	}
	if c.limit > 0 && value > c.limit+1e-9 {
		return fmt.Errorf("%w %.2f > %.2f", ErrLimit, value, c.limit)
	}
	c.current = value
	c.history = append(c.history, value)
	c.updatedAt = time.Now().UTC()
	return nil
}

// SetLimit 在升压途中收紧限值（现场沿途核对发现下游管段承压更低时使用）。
// 只能收紧、不能抬高，且只对当前占用的命令生效。下一拍即按新限值核对。
func (c *Controller) SetLimit(runID string, limit float64) error {
	if limit <= 0 {
		return fmt.Errorf("limit must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.activeRun != runID {
		return fmt.Errorf("%w %s", ErrNotFenced, runID)
	}
	if c.limit > 0 && limit > c.limit+1e-9 {
		return fmt.Errorf("limit can only be tightened: %.2f > %.2f", limit, c.limit)
	}
	c.limit = limit
	c.updatedAt = time.Now().UTC()
	return nil
}

// StepToward 按给定步长沿目标方向逼近：返回本拍应写入的设定值（不超过目标）。
// 它不睡眠，节拍由调用方控制。
func (c *Controller) StepToward(runID string, step float64) (float64, error) {
	if step <= 0 {
		return 0, fmt.Errorf("step must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if runID == "" {
		return 0, fmt.Errorf("missing run")
	}
	if c.lastRun == runID && c.lastReason.Terminal() {
		return 0, fmt.Errorf("%w %s", ErrFinished, runID)
	}
	if !c.enabled || c.stopped {
		return 0, ErrNotAuthorized
	}
	if c.activeRun != runID {
		return 0, fmt.Errorf("%w %s", ErrNotFenced, runID)
	}
	next := c.current + step
	if next >= c.target {
		next = c.target
	}
	if c.limit > 0 && next > c.limit+1e-9 {
		return 0, fmt.Errorf("%w %.2f > %.2f", ErrLimit, next, c.limit)
	}
	c.step = step
	c.current = next
	c.history = append(c.history, next)
	c.updatedAt = time.Now().UTC()
	return next, nil
}

// Complete 表示已到达目标：记录实际压力与 reached 原因并解除命令占用，
// 但硬件保持保压（不停机）。旧命令此后无法继续写入。
func (c *Controller) Complete(runID string) (float64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.activeRun != runID {
		return 0, fmt.Errorf("run %s does not own pressure", runID)
	}
	achieved := c.current
	c.finalizeLocked(runID, achieved, model.StopReached, false)
	return achieved, nil
}

// Abort 因取消、越限、上下文中断等原因停止一次升压：记录实际达到的压力与
// 停机原因，停机硬件并解除命令占用。旧命令此后无法继续写入。
func (c *Controller) Abort(runID string, reason model.StopReason) (float64, error) {
	if !reason.Terminal() || reason == model.StopReached {
		return 0, fmt.Errorf("abort requires an interruption reason")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.activeRun != runID && c.lastRun != runID {
		return 0, fmt.Errorf("run %s does not own pressure", runID)
	}
	achieved := c.current
	c.finalizeLocked(runID, achieved, reason, true)
	return achieved, nil
}

// finalized 落定一条命令的结局。stopHardware 为真时停机并撤销授权。
func (c *Controller) finalizeLocked(runID string, achieved float64, reason model.StopReason, stopHardware bool) {
	c.activeRun = ""
	c.lastRun = runID
	c.lastAchieved = achieved
	c.lastReason = reason
	c.updatedAt = time.Now().UTC()
	if stopHardware {
		c.stopped = true
		c.enabled = false
		c.stoppedAt = c.updatedAt
	}
}

// Stop 是不带命令归属的全局停机（进程关停等）。
func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	runID := c.activeRun
	c.stopped = true
	c.enabled = false
	if runID != "" {
		c.finalizeLocked(runID, c.current, model.StopShutdown, true)
	}
}

// Outcome 返回某条命令实际达到的压力与停机原因（仅在其结束后有值）。
func (c *Controller) Outcome(runID string) (achieved float64, reason model.StopReason, finished bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastRun == runID && c.lastReason.Terminal() {
		return c.lastAchieved, c.lastReason, true
	}
	return 0, model.StopNone, false
}

// Status 返回当前升压链的结构化状态，供观测与快照使用。
func (c *Controller) Status() model.RampStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := model.RampStatus{
		Target:    c.target,
		Achieved:  c.current,
		StepSize:  c.step,
		Limit:     c.limit,
		UpdatedAt: c.updatedAt,
	}
	switch {
	case c.activeRun != "":
		status.RunID = c.activeRun
		status.Status = model.RampRunning
	case c.lastReason == model.StopReached:
		status.RunID = c.lastRun
		status.Status = model.RampReached
		status.StopReason = c.lastReason
		status.Achieved = c.lastAchieved
		status.StoppedAt = c.stoppedAt
	case c.lastReason.Terminal():
		status.RunID = c.lastRun
		status.Status = model.RampAborted
		status.StopReason = c.lastReason
		status.Achieved = c.lastAchieved
		status.StoppedAt = c.stoppedAt
	default:
		status.Status = model.RampIdle
	}
	return status
}
