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
	Acquire(ctx context.Context, workID, holderID string, ttl time.Duration) (LeaseRecord, error)

	// Checkpoint persists state associated with the current lease. The caller must
	// pass a valid LeaseRecord obtained from Acquire. Returns ErrFenced
	// if the record's holder ID or fencing token no longer matches the stored lease.
	Checkpoint(ctx context.Context, record LeaseRecord, state []byte, ttl time.Duration) error

	// Renew extends the lease expiration time. Returns ErrFenced if the record's
	// holder ID or fencing token no longer matches the stored lease, or
	// ErrLeaseExpired if the lease has already expired.
	Renew(ctx context.Context, record LeaseRecord, ttl time.Duration) error

	// Release surrenders the lease. Returns ErrFenced if the record's holder ID
	// or fencing token no longer matches the stored lease.
	// Implementations must set expires_at to a value strictly less than NOW() so
	// that a successor's immediately following Acquire call satisfies the expiry
	// condition. A one-millisecond past offset satisfies this for any backend with
	// at least millisecond clock resolution.
	Release(ctx context.Context, record LeaseRecord) error

	// ReadCheckpoint retrieves persisted state and the clean handoff flag for the
	// given lease. The caller must pass a valid LeaseRecord. Returns ErrFenced if
	// the record's fencing token no longer matches the stored lease.
	ReadCheckpoint(ctx context.Context, record LeaseRecord) (state []byte, cleanHandoff bool, err error)

	// Forget permanently deletes the row identified by record. Returns ErrFenced
	// if record.HolderID or record.FencingToken no longer matches the stored
	// lease, or if no row exists for record.WorkID.
	Forget(ctx context.Context, record LeaseRecord) error

	// Sweep deletes rows older than opts.Retention that are not currently held,
	// returning the number of rows deleted. Does not validate opts.Retention —
	// worklease.Vacuum.Sweep validates before calling this.
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
// (worklease.SweepOptions) so callers never need to import package backend
// directly for this type. It lives here rather than in worklease because
// package backend cannot import package worklease (worklease already imports
// backend) — defining it in worklease and having backend reference it would be
// an import cycle.
type SweepOptions struct {
	// Retention is the minimum age since a lease row was last updated before
	// it becomes eligible for deletion. Backend.Sweep does not validate this —
	// worklease.Vacuum.Sweep validates Retention > 0 before calling Backend.Sweep.
	// Retention must exceed the maximum TTL configured across all Lease clients
	// sharing this backend — this is a caller responsibility, not enforced here.
	Retention time.Duration

	// IncludeCrashed, if true, also sweeps rows where the previous holder's
	// lease expired without an explicit Release (clean handoff false). Default
	// false — only cleanly-released rows are swept. Enabling this permanently
	// discards crash-recovery checkpoint data for swept rows.
	IncludeCrashed bool
}
