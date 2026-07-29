package worklease

import (
	"context"
	"errors"

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

// Vacuum performs age-based bulk cleanup of terminal lease rows.
type Vacuum struct {
	b backend.Backend
}

// NewVacuum returns a Vacuum backed by b.
// NewVacuum does not validate b — a nil backend will panic on first Sweep call.
func NewVacuum(b backend.Backend) *Vacuum {
	return &Vacuum{b: b}
}

// Sweep validates opts.Retention and delegates to the backend. Returns
// ErrRetentionRequired without calling the backend if opts.Retention <= 0.
func (v *Vacuum) Sweep(ctx context.Context, opts SweepOptions) (int64, error) {
	if opts.Retention <= 0 {
		return 0, ErrRetentionRequired
	}
	return v.b.Sweep(ctx, opts)
}
