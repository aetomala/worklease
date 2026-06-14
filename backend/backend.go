package backend

import (
	"context"
	"time"
)

// Backend defines the contract for worklease storage backends. All methods are
// single-attempt — retry policy is the caller's responsibility.
type Backend interface {
	// Acquire attempts to acquire a lease for the given work. Returns ErrLeaseHeld
	// if a lease already exists for this workID.
	Acquire(ctx context.Context, workID, holderID string, ttl time.Duration) (LeaseRecord, error)

	// Checkpoint persists state associated with the current lease. The caller must
	// pass a valid LeaseRecord obtained from Acquire or Renew. Returns ErrFenced
	// if the record's fencing token no longer matches the stored lease.
	Checkpoint(ctx context.Context, record LeaseRecord, state []byte, ttl time.Duration) error

	// Renew extends the lease expiration time. Returns ErrFenced if the record's
	// fencing token no longer matches the stored lease, or ErrLeaseExpired if the
	// lease has already expired.
	Renew(ctx context.Context, record LeaseRecord, ttl time.Duration) error

	// Release surrenders the lease. Returns ErrFenced if the record's fencing
	// token no longer matches the stored lease.
	// Implementations must set expires_at to a value strictly less than NOW() so
	// that a successor's immediately following Acquire call satisfies the expiry
	// condition. A one-millisecond past offset satisfies this for any backend with
	// at least millisecond clock resolution.
	Release(ctx context.Context, record LeaseRecord) error

	// ReadCheckpoint retrieves persisted state and the clean handoff flag for the
	// given lease. The caller must pass a valid LeaseRecord. Returns ErrFenced if
	// the record's fencing token no longer matches the stored lease.
	ReadCheckpoint(ctx context.Context, record LeaseRecord) (state []byte, cleanHandoff bool, err error)
}

// LeaseRecord represents a currently held lease. It is returned by Acquire and
// must be passed back to Checkpoint, Renew, Release, and ReadCheckpoint.
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
