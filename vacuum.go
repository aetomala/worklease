package worklease

import (
	"context"
	"errors"
	"time"

	"github.com/aetomala/worklease/backend"
)

// Error message constant for Vacuum.Sweep validation.
const msgRetentionRequired = "worklease: retention must be greater than zero"

// ErrRetentionRequired is returned by Vacuum.Sweep when opts.Retention <= 0.
var ErrRetentionRequired = errors.New(msgRetentionRequired)

// SweepOptions configures a Vacuum.Sweep call. It is a type alias for
// backend.SweepOptions — the canonical definition lives in package backend to
// avoid an import cycle (package backend cannot import package worklease).
type SweepOptions = backend.SweepOptions

// VacuumConfig configures a Vacuum.
type VacuumConfig struct {
	// Observer receives the outcome of every Sweep that reaches the backend.
	// Optional; nil installs a no-op observer.
	Observer VacuumObserver
}

// VacuumObserver receives a callback after each Vacuum.Sweep backend call.
// It is called synchronously. Implementations must not block or panic.
type VacuumObserver interface {
	// OnSweep is called after every Sweep backend call, successful or not. It
	// is not called when Sweep returns ErrNilBackend or ErrRetentionRequired,
	// since those return before the backend is called.
	OnSweep(ctx context.Context, e SweepEvent)
}

// SweepEvent carries the result of one Sweep backend call. Options holds the
// options passed to Sweep, Deleted the number of rows the backend reported
// deleted, and Duration the duration of the backend call.
type SweepEvent struct {
	Options  SweepOptions
	Deleted  int64
	Duration time.Duration
	Err      error
}

// The noopVacuumObserver type discards sweep events. NewVacuum substitutes it
// when VacuumConfig.Observer is nil.
type noopVacuumObserver struct{}

func (noopVacuumObserver) OnSweep(_ context.Context, _ SweepEvent) {}

// Vacuum performs age-based bulk cleanup of lease rows. Sweep deletes rows
// released with ExitRetired once they are older than SweepOptions.Retention
// and, with SweepOptions.IncludeExpired, rows whose lease expired with no
// declared exit. Rows released with ExitFinished or ExitAbandoned, and rows
// currently held, are never deleted. A deleted row's checkpoint is gone: the
// next holder of that work ID sees ExitNone and empty state, and a holder
// whose row was deleted gets ErrFenced. Build a Vacuum with NewVacuum.
type Vacuum struct {
	b   backend.Backend
	obs VacuumObserver
}

// NewVacuum returns a Vacuum backed by b. Returns ErrNilBackend if b is nil.
// The cfg argument may be the zero value.
func NewVacuum(b backend.Backend, cfg VacuumConfig) (*Vacuum, error) {
	// ===== STEP 1: Validate Required Fields =====
	if b == nil {
		return nil, ErrNilBackend
	}

	// ===== STEP 2: Initialize and Return =====
	obs := cfg.Observer
	if obs == nil {
		obs = noopVacuumObserver{}
	}
	return &Vacuum{b: b, obs: obs}, nil
}

// Sweep returns ErrNilBackend, without calling the backend or the observer, on
// a Vacuum that NewVacuum did not build, including a nil *Vacuum. It returns
// ErrRetentionRequired, also without either call, if opts.Retention <= 0.
// Otherwise it calls the backend, reports the outcome to the configured
// VacuumObserver, and returns the backend's result unchanged.
func (v *Vacuum) Sweep(ctx context.Context, opts SweepOptions) (int64, error) {
	// ===== STEP 1: Validate the Vacuum and the Options =====
	if v == nil || v.b == nil {
		return 0, ErrNilBackend
	}
	if opts.Retention <= 0 {
		return 0, ErrRetentionRequired
	}

	// ===== STEP 2: Delegate to Backend =====
	start := time.Now()
	n, err := v.b.Sweep(ctx, opts)

	// ===== STEP 3: Observe and Return =====
	v.obs.OnSweep(ctx, SweepEvent{Options: opts, Deleted: n, Duration: time.Since(start), Err: err})
	return n, err
}
