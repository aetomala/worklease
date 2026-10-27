package worklease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aetomala/worklease/backend"
)

// Error message constants for lease operations.
const (
	msgLeaseWindowExhausted = "worklease: lease window exhausted before renewal succeeded"
	msgAcquireCancelled     = "worklease: acquire cancelled"
	msgNilBackend           = "worklease: backend is required"
)

// Sentinel errors for Lease operations. ErrFenced, ErrLeaseHeld, and
// ErrLeaseExpired are the identical values of the sentinels of the same name in
// package backend, so errors.Is matches either name.
var (
	// ErrFenced is returned by Checkpoint, Renew, Release, ReadCheckpoint, and
	// Forget when the token no longer matches the stored lease or when no record
	// exists for the work ID. It is the same value as backend.ErrFenced.
	ErrFenced = backend.ErrFenced

	// ErrLeaseHeld is returned by Acquire when the lease for the work ID is held
	// and has not expired. It is the same value as backend.ErrLeaseHeld.
	ErrLeaseHeld = backend.ErrLeaseHeld

	// ErrLeaseExpired is returned by Renew when the lease has already expired, by
	// Release when the lease has expired or this holder already declared an exit,
	// and by Checkpoint only once this holder has declared an exit with Release.
	// It is the same value as backend.ErrLeaseExpired.
	ErrLeaseExpired = backend.ErrLeaseExpired

	// ErrLeaseWindowExhausted is set as the cancel cause of the renewal context
	// when the renewal goroutine exhausts the remaining lease window without a
	// successful renewal (goroutine lifecycle path 3).
	// Inspect via context.Cause(renewCtx) — not returned directly from any method.
	ErrLeaseWindowExhausted = errors.New(msgLeaseWindowExhausted)

	// ErrConfigInvalid is the parent sentinel for configuration errors: every
	// error worklease.New returns, ErrNilBackend wherever it is returned, and the
	// configuration sentinels of the pool, worker, and leader packages. Errors
	// from other packages, such as the nil database check of postgres.New, are not
	// members. Match it with errors.Is.
	ErrConfigInvalid = errors.New("worklease: invalid configuration")

	// ErrNilBackend is returned by New and NewVacuum when the backend is nil,
	// and by Vacuum.Sweep on a Vacuum that NewVacuum did not build. It matches
	// ErrConfigInvalid.
	ErrNilBackend error = &configError{msg: msgNilBackend}
)

// The configError type is a configuration error whose message is exactly msg
// and which matches ErrConfigInvalid with errors.Is.
type configError struct {
	msg string
}

func (e *configError) Error() string { return e.msg }

func (e *configError) Unwrap() error { return ErrConfigInvalid }

// ExitMode records how a lease holder left a lease. It is an alias for
// backend.ExitMode; the canonical definition lives in package backend because
// package backend cannot import package worklease.
type ExitMode = backend.ExitMode

// Exit modes. Callers must treat any value they do not recognize as ExitExpired, because modes may be added in later releases.
const (
	// ExitNone means there was no previous holder: the work ID was never
	// acquired, or its row was removed by Forget or Sweep. Inferred; Release
	// never accepts it.
	ExitNone = backend.ExitNone

	// ExitFinished means the run completed and the checkpoint is final state
	// for that run. The work ID may be acquired again. Sweep never deletes a
	// row whose last exit is ExitFinished, so release one-shot work with
	// ExitRetired instead.
	ExitFinished = backend.ExitFinished

	// ExitAbandoned means the holder stopped deliberately without completing,
	// on error, cancellation, or shutdown. The checkpoint is partial state.
	ExitAbandoned = backend.ExitAbandoned

	// ExitRetired means the work ID is complete permanently. A successor
	// should not redo the work. The row is eligible for Sweep.
	ExitRetired = backend.ExitRetired

	// ExitExpired means the lease expired with no recorded exit: a crash, a
	// partition, or a renewal window that ran out. The checkpoint is partial
	// state, and external effects may have happened after it. Inferred;
	// Release never accepts it.
	ExitExpired = backend.ExitExpired
)

// Checkpoint is the last checkpointed state plus how the immediately previous
// holder exited. It is an alias for backend.Checkpoint.
type Checkpoint = backend.Checkpoint

// Error message constant for the retire sentinel.
const (
	msgRetire = "worklease: retire work ID"
)

var (
	// ErrInvalidExitMode is returned by Release when mode is not ExitFinished,
	// ExitAbandoned, or ExitRetired. It is the same value as
	// backend.ErrInvalidExitMode.
	ErrInvalidExitMode = backend.ErrInvalidExitMode

	// ErrRetire is returned, or wrapped, by a work function to declare that its
	// work ID is complete permanently. Runner.Run in package worker, Elect in
	// package leader, and Pool in package pool release the lease with
	// ExitRetired and report success. Pool also stops the slot.
	ErrRetire = errors.New(msgRetire)
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
//
// Lease is not a stable target for implementations outside this module: a minor
// release may add a method to it. A wrapper or fake that must satisfy Lease
// embeds the Lease interface so that it keeps compiling.
type Lease interface {
	// Acquire attempts to acquire a lease for the given workID. Returns ErrLeaseHeld
	// if the lease for this workID is held and has not expired. If WithWaitForLease
	// is set, blocks until the lease is available, polling at the configured interval.
	Acquire(ctx context.Context, workID string, opts ...AcquireOption) (Token, error)

	// Checkpoint persists state associated with the current lease. The caller must
	// pass a valid Token obtained from Acquire. Returns ErrFenced if the
	// token's holder ID or fencing token no longer matches the stored lease, including when the
	// lease lapsed and another holder has since acquired it.
	// Checkpoint returns ErrLeaseExpired, without writing, only once this holder has
	// declared an exit with Release; a declared exit cannot be undone by a late
	// write. Stop any background checkpoint loop when it sees ErrLeaseExpired.
	// A lease whose TTL lapsed with no declared exit and no successor is not an
	// error: the write succeeds and revives the lease.
	// A nil or empty state clears the stored state: ReadCheckpoint then returns a
	// nil State on every backend.
	Checkpoint(ctx context.Context, token Token, state []byte) error

	// Renew extends the lease expiration time. Returns ErrFenced if the token's
	// holder ID or fencing token no longer matches the stored lease, or ErrLeaseExpired if the
	// lease has already expired.
	Renew(ctx context.Context, token Token) error

	// Release records mode as this holder's exit and expires the lease
	// immediately, so a successor can acquire without waiting for the TTL. The
	// successor reads mode as Checkpoint.PrevExit. Mode must be ExitFinished,
	// ExitAbandoned, or ExitRetired; any other value returns ErrInvalidExitMode
	// without calling the backend or the observer. Returns ErrFenced if the
	// lease was acquired by another holder or its record was removed, and
	// ErrLeaseExpired if the lease has expired or this holder already declared an
	// exit, even when no successor has acquired it. A lapsed holder cannot
	// record an exit, so its successor sees ExitExpired. Call Release with the
	// original ctx or a cleanup context, never the renewal context.
	Release(ctx context.Context, token Token, mode ExitMode) error

	// ReadCheckpoint returns the last checkpointed state and how the
	// immediately previous holder exited. On a first acquisition State is nil
	// and PrevExit is ExitNone. State is also nil after a Checkpoint with a nil
	// or empty state, and is never a non-nil empty slice. PrevExit and
	// PrevHolderID are fixed for the life of the lease. Returns ErrFenced if the
	// token no longer matches the stored lease or if no row exists for the work
	// ID, for example after Forget or Sweep.
	ReadCheckpoint(ctx context.Context, token Token) (Checkpoint, error)

	// StartRenewal begins automatic renewal of the lease at regular intervals.
	// Returns a derived context and a stop function. The renewal context is
	// cancelled if the parent context is cancelled, if the lease is fenced, or
	// if the lease window closes before a renewal succeeds. Calling the stop
	// function ends renewal without cancelling the renewal context, and it
	// returns without waiting for an in-flight Renew call to finish.
	// If Renew returns ErrLeaseExpired, storage has found the lease lapsed:
	// renewal stops at once without retrying, and the renewal context's cause
	// matches both ErrLeaseWindowExhausted and ErrLeaseExpired.
	// LeaseWindow reports the current lease window from the renewal context.
	StartRenewal(ctx context.Context, token Token, opts ...RenewalOption) (renewCtx context.Context, stopRenewal func())

	// Forget permanently deletes the lease record for token's workID. Returns
	// ErrFenced if the holder ID or fencing token no longer matches the stored lease, or if no
	// record exists for token's workID. Unlike Release, the record is not left
	// behind for a future ReadCheckpoint — it is gone. Use only when a work ID is
	// permanently retired. Every call is reported to LeaseObserver.OnForget, and
	// a fenced call also to OnFenced.
	Forget(ctx context.Context, token Token) error
}

// Token represents a currently held lease. It is returned by Acquire and must be
// passed back to Checkpoint, Renew, Release, ReadCheckpoint, Forget, and
// StartRenewal. Renew and Checkpoint do not return a new Token — the same Token
// stays valid for the life of the lease. All fields are unexported; use accessor
// methods to read them. A Token is a value and is safe for concurrent use.
type Token struct {
	workID       string
	holderID     string
	fencingToken uint64
	expiresAt    time.Time
	deadline     time.Time // Local monotonic bound — Acquire start plus TTL; zero if unknown
}

// NewToken returns a Token holding the given fields, for tests and fakes that
// need a Token value without acquiring a lease. NewToken does not call a backend
// and does not validate its arguments. A Token it returns has no local lease
// window, so the window that StartRenewal bounds renewal by is already closed.
// Use Acquire to obtain a Token for a real lease.
func NewToken(workID, holderID string, fencingToken uint64, expiresAt time.Time) Token {
	return Token{
		workID:       workID,
		holderID:     holderID,
		fencingToken: fencingToken,
		expiresAt:    expiresAt,
	}
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

// ExpiresAt returns the lease expiry recorded at acquisition, as reported by the
// backend's clock. It is not updated by Renew or Checkpoint. For a bound that
// reflects renewals, call LeaseWindow with the renewal context.
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
//
// A minor release may add a method to LeaseObserver. Embed NoopObserver in an observer type so that it keeps compiling.
type LeaseObserver interface {
	// OnAcquire is called after every backend acquire attempt, successful or not —
	// once per poll when WithWaitForLease is set. e.Duration is the duration of
	// that attempt's backend call only — not the wait loop.
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

	// OnForget is called after every Forget attempt.
	// If e.Err is ErrFenced, OnFenced is also called after this method returns.
	OnForget(ctx context.Context, e ForgetEvent)

	// OnFenced is called when Checkpoint, Renew, Release, or Forget returns
	// ErrFenced — not for ReadCheckpoint. Called in addition to the
	// operation-specific callback — not instead of.
	// e.Operation identifies which operation triggered the fencing event.
	OnFenced(ctx context.Context, e FencedEvent)
}

// Operation identifies the Lease operation that triggered a fencing event. The
// zero value is not a valid Operation.
type Operation uint8

// Operation values for fencing events. The values start at 1 so that the zero
// value is not a valid Operation.
const (
	// OperationCheckpoint identifies a Checkpoint call.
	OperationCheckpoint Operation = iota + 1

	// OperationRenew identifies a Renew call.
	OperationRenew

	// OperationRelease identifies a Release call.
	OperationRelease

	// OperationForget identifies a Forget call.
	OperationForget
)

// Text forms of each Operation, as returned by Operation.String.
const (
	operationTextCheckpoint = "Checkpoint"
	operationTextRenew      = "Renew"
	operationTextRelease    = "Release"
	operationTextForget     = "Forget"
)

// String returns "Checkpoint", "Renew", "Release", or "Forget". Any other value
// formats as "Operation(<n>)".
func (o Operation) String() string {
	switch o {
	case OperationCheckpoint:
		return operationTextCheckpoint
	case OperationRenew:
		return operationTextRenew
	case OperationRelease:
		return operationTextRelease
	case OperationForget:
		return operationTextForget
	default:
		return fmt.Sprintf("Operation(%d)", uint8(o))
	}
}

// AcquireEvent carries the result of one backend acquire attempt.
// Duration is the duration of that backend call only — not the wait loop.
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
// caller code always produce Attempt: 1. When the renewal goroutine abandons an
// in-flight attempt, Err is the error of the abandonment: context.Canceled on
// stopRenewal, the parent context's error on its cancellation, and
// context.DeadlineExceeded when the lease window closes. Duration then runs to
// the moment of abandonment.
type RenewEvent struct {
	Token    Token
	Duration time.Duration
	Attempt  int
	Err      error
}

// ReleaseEvent carries the result of a Release call. Mode is the exit the
// caller declared. OnRelease is not called when Release rejects the mode with
// ErrInvalidExitMode.
type ReleaseEvent struct {
	Token    Token
	Duration time.Duration
	Mode     ExitMode
	Err      error
}

// ReadCheckpointEvent carries the result of a ReadCheckpoint call. Size is the
// length of the returned state; 0 if nil. PrevExit is ExitNone and
// PrevHolderID is empty when Err is non-nil.
type ReadCheckpointEvent struct {
	Token        Token
	Duration     time.Duration
	PrevExit     ExitMode
	PrevHolderID string
	Size         int
	Err          error
}

// ForgetEvent carries the result of a Forget call. Duration is the duration of
// the backend call.
type ForgetEvent struct {
	Token    Token
	Duration time.Duration
	Err      error
}

// FencedEvent carries the context of a fencing event.
// Called in addition to the operation-specific event — not instead of.
type FencedEvent struct {
	Token     Token
	Operation Operation
}

// NoopObserver is a LeaseObserver that discards every event. The library
// substitutes it when Config.Observer is nil. Embed it in an observer type to
// implement only the methods of interest; the embedding type keeps compiling
// when a later release adds a method to LeaseObserver.
type NoopObserver struct{}

// The compile-time assertion below keeps NoopObserver in step with LeaseObserver.
var _ LeaseObserver = NoopObserver{}

// OnAcquire discards the event.
func (NoopObserver) OnAcquire(_ context.Context, _ AcquireEvent) {}

// OnCheckpoint discards the event.
func (NoopObserver) OnCheckpoint(_ context.Context, _ CheckpointEvent) {}

// OnRenew discards the event.
func (NoopObserver) OnRenew(_ context.Context, _ RenewEvent) {}

// OnRelease discards the event.
func (NoopObserver) OnRelease(_ context.Context, _ ReleaseEvent) {}

// OnReadCheckpoint discards the event.
func (NoopObserver) OnReadCheckpoint(_ context.Context, _ ReadCheckpointEvent) {}

// OnForget discards the event.
func (NoopObserver) OnForget(_ context.Context, _ ForgetEvent) {}

// OnFenced discards the event.
func (NoopObserver) OnFenced(_ context.Context, _ FencedEvent) {}
