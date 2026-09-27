package model

import "time"

type Run struct {
	ID      string
	Station string
	State   RunState
	Epoch   int64
	// TargetPressure/RampRate 是本次升压的目标值与每拍增量；
	// AchievedPressure/StopReason 在升压链结束（到达或中断）时落定，
	// 保证任何中断都能查到实际达到的压力和停机原因。
	TargetPressure   float64    `json:"target_pressure,omitempty"`
	RampRate         float64    `json:"ramp_rate,omitempty"`
	AchievedPressure float64    `json:"achieved_pressure,omitempty"`
	StopReason       StopReason `json:"stop_reason,omitempty"`
	CreatedAt        time.Time
	UpdatedAt        time.Time
}
type Event struct {
	ID      string
	RunID   string
	Kind    string
	Epoch   int64
	Payload string
	At      time.Time
	Durable bool
}
type PassageEvidence struct {
	RunID      string
	Station    string
	SourceID   string
	Generation int64
	Position   float64
	At         time.Time
	Valid      bool
}
type RouteLease struct {
	Route      string
	RunID      string
	Epoch      int64
	AcquiredAt time.Time
}
type CommandReceipt struct {
	RunID    string
	Command  string
	Epoch    int64
	Accepted bool
	At       time.Time
}
type Equipment struct {
	Name      string
	Kind      string
	State     string
	UpdatedAt time.Time
}
type Interlock struct {
	Name   string
	Ready  bool
	Reason string
}
type Incident struct {
	ID       string
	RunID    string
	Severity string
	Message  string
	At       time.Time
}
