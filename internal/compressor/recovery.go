package compressor

import "github.com/wyw14/solo-036/internal/pressure"

func Restore(p *pressure.Controller, value float64) { p.Recover(value) }
func Healthy(w *Worker) bool {
	return w != nil && w.pressure != nil && w.runs != nil && w.sink != nil && w.active != nil
}
