package worklease

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Error message constants for lease operations.
const (
	msgFenced               = "worklease: fenced — lease acquired by another holder"
	msgLeaseHeld            = "worklease: lease is currently held"
	msgLeaseExpired         = "worklease: lease has expired"
	msgLeaseWindowExhausted = "worklease: lease window exhausted before renewal succeeded"
	msgAcquireCancelled     = "worklease: acquire cancelled"
)

// Sentinel errors for Lease operations.
var (
	ErrFenced       = errors.New(msgFenced)
	ErrLeaseHeld    = errors.New(msgLeaseHeld)
	ErrLeaseExpired = errors.New(msgLeaseExpired)

	// ErrLeaseWindowExhausted is set as the cancel cause of the renewal context
	// when the renewal goroutine exhausts the remaining lease window without a
	// successful renewal (goroutine lifecycle path 3).
	// Inspect via context.Cause(renewCtx) — not returned directly from any method.
	ErrLeaseWindowExhausted = errors.New(msgLeaseWindowExhausted)
)

// Default backoff parameters for the renewal goroutine retry policy.
const (
	defaultBackoffInitial = 100 * time.Millisecond
	defaultBackoffMax     = 5 * time.Second
	defaultBackoffJitter  = 0.20
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

// renewalConfig holds resolved options for a StartRenewal call.
// Unexported — the library resolves options internally.
type renewalConfig struct {
	// renewalInterval is the time between renewal attempts. Default: TTL/2.
	renewalInterval time.Duration
	// backoffInitial is the first retry interval after a non-fencing Renew error. Default: 100ms.
	backoffInitial time.Duration
	// backoffMax caps the retry interval after exponential growth. Default: 5s.
	backoffMax time.Duration
	// backoffJitter is the additive jitter fraction in [0, 1]. Default: 0.20.
	backoffJitter float64
}

// WithRenewalInterval sets the time between renewal attempts.
// Default: TTL/2. Zero or negative values are ignored — the default is used silently.
func WithRenewalInterval(d time.Duration) RenewalOption {
	return func(c *renewalConfig) {
		if d > 0 {
			c.renewalInterval = d
		}
	}
}

// WithRenewalBackoff configures the exponential backoff policy used by the
// renewal goroutine when Renew returns a non-fencing, non-nil error.
// The initial parameter is the first retry interval; maxInterval caps the
// interval after growth. The jitter parameter is a fraction in [0, 1]; the
// actual wait is drawn from [base, base+jitter*base].
// Defaults: initial=100ms, max=5s, jitter=0.20.
// Clamping (in order): initial floors to 1ms if <= 0; max floors to 1ms if <= 0;
// jitter clamps to [0, 1]; finally initial caps at max if initial > max.
func WithRenewalBackoff(initial, maxInterval time.Duration, jitter float64) RenewalOption {
	return func(c *renewalConfig) {
		if initial <= 0 {
			initial = time.Millisecond
		}
		if maxInterval <= 0 {
			maxInterval = time.Millisecond
		}
		if jitter < 0 {
			jitter = 0
		}
		if jitter > 1 {
			jitter = 1
		}
		if initial > maxInterval {
			initial = maxInterval
		}
		c.backoffInitial = initial
		c.backoffMax = maxInterval
		c.backoffJitter = jitter
	}
}

// LeaseObserver receives callbacks after each Lease operation.
// All methods are called synchronously. Implementations must not block or panic.
// The zero value of Config.Observer is nil; the library substitutes a no-op observer.
type LeaseObserver interface {
	// OnAcquire is called after every Acquire attempt, successful or not.
	// e.Duration is the duration of the final backend call only — not the wait loop.
	// e.Token is the zero value of Token if e.Err is non-nil.
	OnAcquire(ctx context.Context, e AcquireEvent)

	// OnCheckpoint is called after every Checkpoint attempt.
	// If e.Err is ErrFenced, OnFenced is also called after this method returns.
	OnCheckpoint(ctx context.Context, e CheckpointEvent)

	// OnRenew is called after every Renew attempt.
	// If e.Err is ErrFenced, OnFenced is also called after this method returns.
	OnRenew(ctx context.Context, e RenewEvent)

	// OnRelease is called after every Release attempt.
	// If e.Err is ErrFenced, OnFenced is also called after this method returns.
	OnRelease(ctx context.Context, e ReleaseEvent)

	// OnReadCheckpoint is called after every ReadCheckpoint attempt.
	// OnFenced is NOT called on ReadCheckpoint — ErrFenced surfaces via e.Err only.
	OnReadCheckpoint(ctx context.Context, e ReadCheckpointEvent)

	// OnFenced is called when Checkpoint, Renew, or Release returns ErrFenced.
	// Called in addition to the operation-specific callback — not instead of.
	// e.Operation identifies which operation triggered the fencing event.
	OnFenced(ctx context.Context, e FencedEvent)
}

// Operation identifies the Lease operation that triggered a fencing event.
type Operation uint8

// Operation values for fencing events.
const (
	OperationCheckpoint Operation = iota
	OperationRenew
	OperationRelease
)

// AcquireEvent carries the result of an Acquire call.
// Duration is the duration of the final backend call only — not the wait loop.
// Token is the zero value of Token if Err is non-nil.
type AcquireEvent struct {
	WorkID   string
	Token    Token
	Duration time.Duration
	Err      error
}

// CheckpointEvent carries the result of a Checkpoint call.
// Size is len(state) from the call site.
type CheckpointEvent struct {
	Token    Token
	Size     int
	Duration time.Duration
	Err      error
}

// RenewEvent carries the result of a Renew call.
// Attempt is 1 on the first attempt. The renewal goroutine increments Attempt
// on each retry attempt within the backoff loop. Direct calls to Renew from
// caller code always produce Attempt: 1.
type RenewEvent struct {
	Token    Token
	Duration time.Duration
	Attempt  int
	Err      error
}

// ReleaseEvent carries the result of a Release call.
type ReleaseEvent struct {
	Token    Token
	Duration time.Duration
	Err      error
}

// ReadCheckpointEvent carries the result of a ReadCheckpoint call.
// Size is len of the returned state slice; 0 if nil.
type ReadCheckpointEvent struct {
	Token        Token
	Duration     time.Duration
	CleanHandoff bool
	Size         int
	Err          error
}

// FencedEvent carries the context of a fencing event.
// Called in addition to the operation-specific event — not instead of.
type FencedEvent struct {
	Token     Token
	Operation Operation
}

// noopObserver is a LeaseObserver that discards all events.
// Substituted when Config.Observer is nil.
type noopObserver struct{}

func (noopObserver) OnAcquire(_ context.Context, _ AcquireEvent)               {}
func (noopObserver) OnCheckpoint(_ context.Context, _ CheckpointEvent)         {}
func (noopObserver) OnRenew(_ context.Context, _ RenewEvent)                   {}
func (noopObserver) OnRelease(_ context.Context, _ ReleaseEvent)               {}
func (noopObserver) OnReadCheckpoint(_ context.Context, _ ReadCheckpointEvent) {}
func (noopObserver) OnFenced(_ context.Context, _ FencedEvent)                 {}
