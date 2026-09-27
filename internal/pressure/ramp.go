package pressure

import (
	"fmt"
	"math"
	"time"
)

// StopReason records why a pressurization chain stopped.
type StopReason string

const (
	// ReasonCompleted means the target pressure was reached and confirmed by feedback.
	ReasonCompleted StopReason = "completed"
	// ReasonCancelled means the run was cancelled or its command context ended.
	ReasonCancelled StopReason = "cancelled"
	// ReasonLimitExceeded means a commanded or observed value crossed the safety limit.
	ReasonLimitExceeded StopReason = "limit_exceeded"
	// ReasonFeedbackDiverged means observed feedback deviated from the command too far.
	ReasonFeedbackDiverged StopReason = "feedback_diverged"
	// ReasonFeedbackUnavailable means no fresh feedback reached the controller.
	ReasonFeedbackUnavailable StopReason = "feedback_unavailable"
	// ReasonUnauthorized means the controller was stopped or the command was superseded.
	ReasonUnauthorized StopReason = "unauthorized"
	// ReasonInterrupted means the process vanished mid-ramp and recovery found an
	// orphaned pressurizing run with no live command.
	ReasonInterrupted StopReason = "interrupted"
)

// Plan describes a slope-based approach from the current pressure to the target.
type Plan struct {
	RunID     string
	From      float64
	Target    float64
	Step      float64       // pressure increment per tick (the slope magnitude)
	Interval  time.Duration // time between ticks
	Limit     float64       // hard pressure limit, never to be exceeded
	Tolerance float64       // maximum allowed |commanded - observed| gap
	// FeedbackMaxAge bounds how long feedback may lag the command. Zero means the
	// controller treats the lack of feedback as authoritative feedback at the
	// commanded value (test/dry-run mode); otherwise the ramp stalls and then stops.
	FeedbackMaxAge time.Duration
}

// Sample is one observed pressure feedback reading.
type Sample struct {
	RunID string
	Value float64
	At    time.Time
}

// Report is the final state of a ramp: the pressure actually reached and why it stopped.
type Report struct {
	RunID    string
	Reached  float64
	Target   float64
	Reason   StopReason
	At       time.Time
	Complete bool
}

// command tracks the single pressurization command currently allowed to write.
type command struct {
	runID    string
	target   float64
	limit    float64
	active   bool
	result   *Report
	last     *Sample
	stopCh   chan struct{}
	stopOnce bool
}

func (p Plan) validate() error {
	if p.RunID == "" {
		return fmt.Errorf("ramp plan requires a run")
	}
	if p.Target < 0 {
		return fmt.Errorf("ramp target must be non-negative")
	}
	if p.Limit > 0 && p.Target > p.Limit {
		return fmt.Errorf("ramp target %.3f exceeds limit %.3f", p.Target, p.Limit)
	}
	if p.Step <= 0 {
		return fmt.Errorf("ramp step must be positive")
	}
	if p.Interval <= 0 {
		return fmt.Errorf("ramp interval must be positive")
	}
	if p.From < 0 {
		return fmt.Errorf("ramp start must be non-negative")
	}
	if p.Limit > 0 && p.From > p.Limit {
		return fmt.Errorf("ramp start %.3f already exceeds limit %.3f", p.From, p.Limit)
	}
	if p.Tolerance < 0 {
		return fmt.Errorf("feedback tolerance must be non-negative")
	}
	return nil
}

// Begin installs a new pressurization command for the run. Any previous command
// is closed first so it can no longer write; after Begin the old run ID is
// rejected until this command finishes or is stopped.
func (c *Controller) Begin(plan Plan) error {
	if err := plan.validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled || c.stopped {
		return fmt.Errorf("pressure not authorized")
	}
	if c.cmd != nil && c.cmd.active && c.cmd.runID != plan.RunID {
		// A live command belongs to another run; reject without disturbing it.
		return fmt.Errorf("pressure commanded by run %s", c.cmd.runID)
	}
	if c.cmd != nil {
		c.closeCommandLocked(ReasonCancelled)
	}
	c.cmd = &command{
		runID:  plan.RunID,
		target: plan.Target,
		limit:  plan.Limit,
		active: true,
		stopCh: make(chan struct{}),
	}
	return nil
}

// Advance writes the next commanded value for the run. The command is rejected
// if the run no longer owns the controller, the controller was stopped, the
// value crosses the limit, or feedback has diverged. A rejected command closes
// the ramp with a report describing the cause.
func (c *Controller) Advance(runID string, value float64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd := c.cmd
	if cmd == nil || !cmd.active {
		return fmt.Errorf("no active pressure command for run %s", runID)
	}
	if cmd.runID != runID {
		return fmt.Errorf("pressure command belongs to run %s", cmd.runID)
	}
	if !c.enabled || c.stopped {
		c.finishLocked(cmd, c.current, ReasonUnauthorized)
		return fmt.Errorf("pressure not authorized")
	}
	if value < 0 {
		c.finishLocked(cmd, c.current, ReasonLimitExceeded)
		return fmt.Errorf("pressure %.3f must be non-negative", value)
	}
	if cmd.limit > 0 && value > cmd.limit+toleranceSlack(cmd.limit) {
		c.finishLocked(cmd, c.current, ReasonLimitExceeded)
		return fmt.Errorf("commanded pressure %.3f exceeds limit %.3f", value, cmd.limit)
	}
	if cmd.limit > 0 && c.current > cmd.limit+toleranceSlack(cmd.limit) {
		c.finishLocked(cmd, c.current, ReasonLimitExceeded)
		return fmt.Errorf("pressure %.3f already exceeds limit %.3f", c.current, cmd.limit)
	}
	if cmd.last != nil && cmd.last.RunID != runID {
		c.finishLocked(cmd, c.current, ReasonFeedbackDiverged)
		return fmt.Errorf("feedback belongs to run %s", cmd.last.RunID)
	}
	c.current = value
	c.history = append(c.history, value)
	return nil
}

// Observe records feedback from the field for a run. Stale feedback for a
// previous command is ignored rather than applied to the live command.
func (c *Controller) Observe(sample Sample) {
	if sample.At.IsZero() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd := c.cmd
	if cmd == nil || !cmd.active || cmd.runID != sample.RunID {
		return
	}
	if cmd.limit > 0 && sample.Value > cmd.limit+toleranceSlack(cmd.limit) {
		c.finishLocked(cmd, sample.Value, ReasonLimitExceeded)
		return
	}
	cmd.last = &sample
}

// VerifyFeedback checks the most recent feedback against the commanded value:
// it must exist (unless feedback is optional), be fresh, and stay within the
// tolerance band. A failed verification stops the command and reports the
// actual pressure reached.
func (c *Controller) VerifyFeedback(runID string, commanded float64, tolerance float64, maxAge time.Duration, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd := c.cmd
	if cmd == nil || !cmd.active || cmd.runID != runID {
		return fmt.Errorf("no active pressure command for run %s", runID)
	}
	if cmd.last == nil {
		if maxAge <= 0 {
			return nil
		}
		c.finishLocked(cmd, c.current, ReasonFeedbackUnavailable)
		return fmt.Errorf("no pressure feedback for run %s", runID)
	}
	if maxAge > 0 && now.Sub(cmd.last.At) > maxAge {
		c.finishLocked(cmd, c.current, ReasonFeedbackUnavailable)
		return fmt.Errorf("pressure feedback stale by %s", now.Sub(cmd.last.At).Round(time.Millisecond))
	}
	if math.Abs(cmd.last.Value-commanded) > tolerance && tolerance >= 0 {
		// Reached is the actual observed pressure, not the commanded one.
		c.finishLocked(cmd, cmd.last.Value, ReasonFeedbackDiverged)
		return fmt.Errorf("pressure feedback %.3f diverges from command %.3f", cmd.last.Value, commanded)
	}
	return nil
}

// Complete marks a successful ramp. The reached value is the confirmed
// feedback when available, otherwise the last commanded value.
func (c *Controller) Complete(runID string, at time.Time) (Report, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd := c.cmd
	if cmd == nil || cmd.runID != runID {
		return Report{}, fmt.Errorf("no pressure command for run %s", runID)
	}
	reached := c.current
	if cmd.last != nil {
		reached = cmd.last.Value
	}
	c.finishLocked(cmd, reached, ReasonCompleted)
	return *cmd.result, nil
}

// Cancel stops a command because of cancellation or shutdown. The ramp itself
// keeps the reached pressure untouched; callers depressurize separately.
func (c *Controller) Cancel(runID string, reason StopReason) (Report, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd := c.cmd
	if cmd == nil || cmd.runID != runID {
		return Report{}, false
	}
	if reason == "" {
		reason = ReasonCancelled
	}
	reached := c.current
	if cmd.last != nil {
		reached = cmd.last.Value
	}
	c.finishLocked(cmd, reached, reason)
	return *cmd.result, true
}

// StopChannel returns a channel closed when the command must abort (Stop,
// Cancel, or supersession). Nil if no command is installed.
func (c *Controller) StopChannel(runID string) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd == nil || c.cmd.runID != runID {
		return nil
	}
	return c.cmd.stopCh
}

// Active reports whether the run currently owns a live pressurization command.
func (c *Controller) Active(runID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cmd != nil && c.cmd.active && c.cmd.runID == runID
}

// LastResult returns the terminal report left by a command, if any.
func (c *Controller) LastResult(runID string) (Report, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd == nil || c.cmd.runID != runID || c.cmd.result == nil {
		return Report{}, false
	}
	return *c.cmd.result, true
}

// Target returns the commanded target of the run's command.
func (c *Controller) Target(runID string) (float64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd == nil || c.cmd.runID != runID {
		return 0, false
	}
	return c.cmd.target, true
}

// finishLocked finalizes a command. Must be called with c.mu held. It records
// the reached pressure and reason and wakes anyone waiting on the stop channel.
func (c *Controller) finishLocked(cmd *command, reached float64, reason StopReason) {
	if !cmd.active {
		return
	}
	cmd.active = false
	cmd.result = &Report{
		RunID:    cmd.runID,
		Reached:  reached,
		Target:   cmd.target,
		Reason:   reason,
		At:       time.Now().UTC(),
		Complete: reason == ReasonCompleted,
	}
	if !cmd.stopOnce {
		close(cmd.stopCh)
		cmd.stopOnce = true
	}
}

// closeCommandLocked closes whatever command is installed. Must be called with
// c.mu held.
func (c *Controller) closeCommandLocked(reason StopReason) {
	if c.cmd == nil {
		return
	}
	c.finishLocked(c.cmd, c.current, reason)
}

func toleranceSlack(limit float64) float64 {
	// Tiny epsilon to absorb float rounding against a hard limit.
	return math.Max(1e-9, limit*1e-9)
}
