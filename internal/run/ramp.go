package run

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

// PlanRamp 在升压开始前登记目标值与斜率（每拍增量）。
func (s *Store) PlanRamp(id string, target, rate float64) error {
	if target <= 0 {
		return fmt.Errorf("ramp target must be positive")
	}
	if rate <= 0 {
		return fmt.Errorf("ramp rate must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return fmt.Errorf("run %s not found", id)
	}
	r.TargetPressure = target
	r.RampRate = rate
	r.UpdatedAt = time.Now().UTC()
	s.runs[id] = r
	return nil
}

// RecordOutcome 记录一次升压链的最终结果：实际达到的压力与停机原因。
// 结果只能落定一次，后续旧命令不能覆盖，这是“后续不能继续旧命令”的留痕保证。
func (s *Store) RecordOutcome(id string, achieved float64, reason model.StopReason) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return fmt.Errorf("run %s not found", id)
	}
	if r.StopReason.Terminal() {
		return fmt.Errorf("run %s already stopped: %s", id, r.StopReason)
	}
	r.AchievedPressure = achieved
	r.StopReason = reason
	r.UpdatedAt = time.Now().UTC()
	s.runs[id] = r
	return nil
}
