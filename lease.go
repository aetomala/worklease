package worklease

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Error message constants for lease operations.
const (
	msgFenced       = "worklease: fenced — lease acquired by another holder"
	msgLeaseHeld    = "worklease: lease is currently held"
	msgLeaseExpired = "worklease: lease has expired"
)

// Sentinel errors for Lease operations.
var (
	ErrFenced       = errors.New(msgFenced)
	ErrLeaseHeld    = errors.New(msgLeaseHeld)
	ErrLeaseExpired = errors.New(msgLeaseExpired)
)

// Lease defines the contract for acquiring, managing, and renewing leases on
// distributed work. Implementations are responsible for handling backend
// storage, fencing, and expiration logic. All methods are safe for concurrent use.
type Lease interface {
	// Acquire attempts to acquire a lease for the given workID. Returns ErrLeaseHeld
	// if a lease already exists for this workID. If WithWaitForLease is set, blocks
	// until the lease is available, polling at the configured interval.
	Acquire(ctx context.Context, workID string, opts ...AcquireOption) (Token, error)

	// Checkpoint persists state associated with the current lease. The caller must
	// pass a valid Token obtained from Acquire or Renew. Returns ErrFenced if the
	// token's fencing token no longer matches the stored lease.
	Checkpoint(ctx context.Context, token Token, state []byte) error

	// Renew extends the lease expiration time. Returns ErrFenced if the token's
	// fencing token no longer matches the stored lease, or ErrLeaseExpired if the
	// lease has already expired.
	Renew(ctx context.Context, token Token) error

	// Release surrenders the lease and expires it immediately, making the work item
	// available for acquisition by a successor without waiting for the TTL. Sets
	// clean_handoff so the successor knows the previous owner finished intentionally.
	// Returns ErrFenced if the fencing token no longer matches the stored lease.
	Release(ctx context.Context, token Token) error

	// ReadCheckpoint retrieves persisted state and the clean handoff flag for the
	// given lease. The caller must pass a valid Token. Returns ErrFenced if the
	// token's fencing token no longer matches the stored lease.
	ReadCheckpoint(ctx context.Context, token Token) (state []byte, cleanHandoff bool, err error)

	// StartRenewal begins automatic renewal of the lease at regular intervals. Returns
	// a derived context and a stop function. Calling stop cancels the renewal context
	// and terminates the renewal loop. The renewal context is cancelled if the underlying
	// context is cancelled or if the lease is lost.
	StartRenewal(ctx context.Context, token Token, opts ...RenewalOption) (renewCtx context.Context, stopRenewal func())
}

// Token represents a currently held lease. It is returned by Acquire and Renew
// and must be passed back to Checkpoint, Renew, Release, ReadCheckpoint, and
// StartRenewal. All fields are unexported; use accessor methods to read them.
type Token struct {
	workID       string
	holderID     string
	fencingToken uint64
	expiresAt    time.Time
}

// WorkID returns the identifier for the unit of work being leased.
func (t Token) WorkID() string {
	return t.workID
}

// HolderID returns the identifier of the entity holding the lease.
func (t Token) HolderID() string {
	return t.holderID
}

// FencingToken returns the monotonically increasing token that prevents stale
// operations on the lease. If the lease is reassigned, the token changes.
func (t Token) FencingToken() uint64 {
	return t.fencingToken
}

// ExpiresAt returns the wall-clock time at which the lease expires.
func (t Token) ExpiresAt() time.Time {
	return t.expiresAt
}

// String returns a string representation of the Token.
func (t Token) String() string {
	return fmt.Sprintf("worklease.Token{workID=%q holderID=%q fencingToken=%d expiresAt=%s}",
		t.workID, t.holderID, t.fencingToken, t.expiresAt.Format(time.RFC3339))
}

// AcquireOption is a functional option for Acquire.
type AcquireOption func(*acquireConfig)

// acquireConfig holds configuration for Acquire options.
type acquireConfig struct {
	waitForLease bool
	pollInterval time.Duration
}

// WithWaitForLease configures Acquire to block and retry until a lease becomes
// available, rather than returning ErrLeaseHeld immediately.
func WithWaitForLease() AcquireOption {
	return func(c *acquireConfig) {
		c.waitForLease = true
	}
}

// WithPollInterval sets the interval at which Acquire polls when WithWaitForLease
// is active. If d is zero or negative, the default (2 * time.Second) is preserved.
func WithPollInterval(d time.Duration) AcquireOption {
	return func(c *acquireConfig) {
		if d > 0 {
			c.pollInterval = d
		}
	}
}

// HasWaitForLease reports whether opts includes WithWaitForLease.
// Pool.New uses this to enforce that WithWaitForLease is not passed in
// Config.AcquireOptions at construction time.
func HasWaitForLease(opts []AcquireOption) bool {
	cfg := acquireConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg.waitForLease
}

// RenewalOption is a functional option for StartRenewal.
type RenewalOption func(*renewalConfig)

// renewalConfig holds configuration for Renewal options.
type renewalConfig struct {
	renewalInterval time.Duration
}

// WithRenewalInterval sets the interval at which StartRenewal renews the lease.
// If d is zero or negative, the default is preserved.
func WithRenewalInterval(d time.Duration) RenewalOption {
	return func(c *renewalConfig) {
		if d > 0 {
			c.renewalInterval = d
		}
	}
}

// LeaseObserver is called synchronously after each Lease operation.
// All methods are safe for concurrent use — the caller guarantees the
// implementation is goroutine-safe.
type LeaseObserver interface {
	// OnAcquire is called after Acquire returns, whether successful or not.
	// token is the zero Token when err is non-nil.
	OnAcquire(ctx context.Context, workID string, token Token, err error)

	// OnCheckpoint is called after Checkpoint returns.
	// size is the length in bytes of the state argument.
	OnCheckpoint(ctx context.Context, token Token, size int, err error)

	// OnRenew is called after Renew returns.
	OnRenew(ctx context.Context, token Token, err error)

	// OnRelease is called after Release returns.
	OnRelease(ctx context.Context, token Token, err error)

	// OnFenced is called after OnCheckpoint or OnRenew when the returned
	// error wraps ErrFenced — the lease has been superseded.
	OnFenced(ctx context.Context, token Token)
}

// noopObserver is a LeaseObserver that discards all events.
type noopObserver struct{}

func (noopObserver) OnAcquire(_ context.Context, _ string, _ Token, _ error)    {}
func (noopObserver) OnCheckpoint(_ context.Context, _ Token, _ int, _ error)    {}
func (noopObserver) OnRenew(_ context.Context, _ Token, _ error)                {}
func (noopObserver) OnRelease(_ context.Context, _ Token, _ error)              {}
func (noopObserver) OnFenced(_ context.Context, _ Token)                        {}
