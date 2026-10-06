package backend

import (
	"context"
	"strconv"
	"time"
)

// ExitMode records how a lease holder left a lease. The zero value is ExitNone.
// Callers must treat any value they do not recognize as ExitExpired, because
// modes may be added in later releases.
type ExitMode uint8

const (
	// ExitNone means there was no previous holder: the work ID was never
	// acquired, or its row was removed by Forget or Sweep. Inferred; Release
	// never accepts it.
	ExitNone ExitMode = iota

	// ExitFinished means the run completed and the checkpoint is final state
	// for that run. The work ID may be acquired again. Sweep never deletes a
	// row whose last exit is ExitFinished, so release one-shot work with
	// ExitRetired instead.
	ExitFinished

	// ExitAbandoned means the holder stopped deliberately without completing,
	// on error, cancellation, or shutdown. The checkpoint is partial state.
	ExitAbandoned

	// ExitRetired means the work ID is complete permanently. A successor
	// should not redo the work. The row is eligible for Sweep.
	ExitRetired

	// ExitExpired means the lease expired with no recorded exit: a crash, a
	// partition, or a renewal window that ran out. The checkpoint is partial
	// state, and external effects may have happened after it. Inferred;
	// Release never accepts it.
	ExitExpired
)

// SQL text forms of each ExitMode, as stored by the PostgreSQL backend.
const (
	exitTextNone      = "none"
	exitTextFinished  = "finished"
	exitTextAbandoned = "abandoned"
	exitTextRetired   = "retired"
	exitTextExpired   = "expired"
)

// String returns the SQL text form of m: "none", "finished", "abandoned",
// "retired", or "expired". Any other value formats as "ExitMode(<n>)".
func (m ExitMode) String() string {
	switch m {
	case ExitNone:
		return exitTextNone
	case ExitFinished:
		return exitTextFinished
	case ExitAbandoned:
		return exitTextAbandoned
	case ExitRetired:
		return exitTextRetired
	case ExitExpired:
		return exitTextExpired
	default:
		return "ExitMode(" + strconv.Itoa(int(m)) + ")"
	}
}

// Checkpoint is what a lease holder reads at the start of its lease: the last
// checkpointed state and how the immediately previous holder exited. Fields
// may be added in later releases.
type Checkpoint struct {
	// State is the last checkpointed state; nil if none was ever written.
	State []byte

	// PrevExit is how the immediately previous holder exited.
	PrevExit ExitMode

	// PrevHolderID is the previous holder's ID; empty when PrevExit is ExitNone.
	PrevHolderID string
}

// Backend defines the contract for worklease storage backends. All methods are
// single-attempt — retry policy is the caller's responsibility.
type Backend interface {
	// Acquire attempts to acquire a lease for the given work. Returns ErrLeaseHeld
	// if the lease for this workID is held and has not expired.
	// On reacquisition of an expired row, Acquire records the row's declared
	// exit (ExitExpired if none was declared) and holder ID as the new lease's
	// previous exit and previous holder, and clears the declared exit, in the
	// same atomic step that issues the fencing token. A new row records ExitNone.
	Acquire(ctx context.Context, workID, holderID string, ttl time.Duration) (LeaseRecord, error)

	// Checkpoint persists state associated with the current lease. The caller must
	// pass a valid LeaseRecord obtained from Acquire. Returns ErrFenced
	// if the record's holder ID or fencing token no longer matches the stored lease.
	// Checkpoint never writes an exit field. Once the holder has declared an
	// exit with Release, Checkpoint returns ErrLeaseExpired without writing. A
	// lease that lapsed with no declared exit and no successor can still be
	// revived by Checkpoint.
	Checkpoint(ctx context.Context, record LeaseRecord, state []byte, ttl time.Duration) error

	// Renew extends the lease expiration time. Returns ErrFenced if the record's
	// holder ID or fencing token no longer matches the stored lease, or
	// ErrLeaseExpired if the lease has already expired.
	Renew(ctx context.Context, record LeaseRecord, ttl time.Duration) error

	// Release records mode as the holder's declared exit and expires the lease
	// immediately by setting expires_at to a value strictly less than NOW(), so
	// a successor can acquire without waiting for the TTL. Mode must be
	// ExitFinished, ExitAbandoned, or ExitRetired; any other value returns
	// ErrInvalidExitMode before any other check and without side effects.
	// Returns ErrFenced if WorkID, HolderID, or FencingToken does not match the
	// stored lease, or if no row exists. Returns ErrLeaseExpired if the lease
	// matches but has already expired, even when no successor has acquired it.
	// Fencing is checked before expiry.
	Release(ctx context.Context, record LeaseRecord, mode ExitMode) error

	// ReadCheckpoint returns the checkpoint state and the immediately previous
	// holder's exit for the lease identified by record. The returned State is a
	// fresh allocation owned by the caller. Returns ErrFenced if no row exists
	// for record.WorkID or if record.FencingToken does not match the stored lease.
	ReadCheckpoint(ctx context.Context, record LeaseRecord) (Checkpoint, error)

	// Forget permanently deletes the row identified by record. Returns ErrFenced
	// if record.HolderID or record.FencingToken no longer matches the stored
	// lease, or if no row exists for record.WorkID.
	Forget(ctx context.Context, record LeaseRecord) error

	// Sweep deletes rows that are not currently held, were last updated more
	// than opts.Retention ago, and either were released with ExitRetired or,
	// when opts.IncludeExpired is set, have no declared exit. Rows released with
	// ExitFinished or ExitAbandoned are never deleted. Returns the number of
	// rows deleted. Does not validate opts.Retention; Vacuum.Sweep in package
	// worklease validates before calling this.
	Sweep(ctx context.Context, opts SweepOptions) (int64, error)
}

// LeaseRecord represents a currently held lease. It is returned by Acquire and
// must be passed back to Checkpoint, Renew, Release, ReadCheckpoint, and Forget.
// All fields are read-only.
type LeaseRecord struct {
	// WorkID is the identifier for the unit of work being leased. Immutable.
	WorkID string

	// HolderID is the identifier of the entity holding the lease. Immutable.
	HolderID string

	// FencingToken is a monotonically increasing token that prevents stale
	// operations on the lease. If the lease is reassigned, the token changes.
	FencingToken uint64

	// ExpiresAt is the wall-clock time at which the lease expires.
	ExpiresAt time.Time
}

// SweepOptions configures a Sweep call. The canonical definition lives here in
// package backend. Package worklease re-exports it as a type alias
// (worklease.SweepOptions) because package backend cannot import package
// worklease.
type SweepOptions struct {
	// Retention is how long a retired or expired row is kept after its last
	// update. It is not tied to the TTL; held rows are never eligible.
	// Backend.Sweep does not validate it. Vacuum.Sweep in package worklease
	// returns ErrRetentionRequired if it is not positive.
	Retention time.Duration

	// IncludeExpired, if true, also deletes rows with no declared exit: the
	// lease expired after a crash, a partition, or an exhausted renewal window.
	// Default false. Enabling it discards the partial state a successor would
	// have recovered from.
	IncludeExpired bool
}
