package run

import "github.com/wyw14/solo-036/internal/model"

// Start moves a freshly loaded run into pressurizing. The run stays in this
// state while the compressor approaches the target along its slope, and is
// only promoted to launched once the ramp completes.
func Start(store *Store, id string) error {
	return store.Transition(id, model.StatePressurizing)
}

// Launch marks a pressurizing run launched after target pressure is confirmed.
func Launch(store *Store, id string) error {
	return store.Transition(id, model.StateLaunched)
}

func Track(store *Store, id string) error { return store.Transition(id, model.StateTracking) }
