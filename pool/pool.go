package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/worker"
)

// Sentinel errors for pool operations.
var (
	// ErrConfigInvalid is the parent sentinel for all pool configuration errors.
	ErrConfigInvalid = errors.New("pool: invalid configuration")

	// ErrNilLease is returned by New when lease is nil.
	ErrNilLease = fmt.Errorf("pool: lease is required: %w", ErrConfigInvalid)

	// ErrEmptyWorkIDs is returned by New when cfg.WorkIDs is empty.
	ErrEmptyWorkIDs = fmt.Errorf("pool: WorkIDs must not be empty: %w", ErrConfigInvalid)

	// ErrWithWaitForLeaseProhibited is returned by New when cfg.AcquireOptions includes WithWaitForLease.
	ErrWithWaitForLeaseProhibited = fmt.Errorf("pool: WithWaitForLease is prohibited in AcquireOptions: %w", ErrConfigInvalid)

	// ErrAllSlotsDead is returned by Run when every slot exited via PermanentError.
	ErrAllSlotsDead = errors.New("pool: all slots permanently dead")
)

// PermanentError is implemented by errors returned from WorkFn that should
// suppress slot reacquisition. Pool checks errors.As(err, &pe) after each
// runner.Run call. A permanent error causes the slot goroutine to exit without
// reacquiring. Implement this interface on a custom error type; pool provides
// no concrete implementation.
type PermanentError interface {
	error
	Permanent() bool
}

// Permanent wraps err in a value that satisfies PermanentError with Permanent() == true.
// Use when a WorkFn wants to drop its slot without defining a custom error type.
func Permanent(err error) error { return &permanentErr{cause: err} }

// permanentErr is the concrete type returned by Permanent.
type permanentErr struct{ cause error }

func (e *permanentErr) Error() string   { return e.cause.Error() }
func (e *permanentErr) Permanent() bool { return true }
func (e *permanentErr) Unwrap() error   { return e.cause }

// Observer receives callbacks on slot lifecycle transitions.
// All methods are called synchronously. Implementations must not block or panic.
// The zero value of Config.Observer is nil; the library substitutes a no-op.
type Observer interface {
	// OnSlotAcquired is called when a slot's Runner.Run begins executing WorkFn.
	OnSlotAcquired(ctx context.Context, e SlotAcquiredEvent)

	// OnSlotLost is called when a slot loses its lease via fencing.
	// The slot attempts reacquisition at the next loop iteration.
	OnSlotLost(ctx context.Context, e SlotLostEvent)

	// OnSlotBackoff is called when a slot waits BackoffInterval after a
	// non-permanent error other than ErrFenced and ErrLeaseHeld. The
	// e.Duration field is the effective BackoffInterval.
	OnSlotBackoff(ctx context.Context, e SlotBackoffEvent)

	// OnSlotDead is called when a slot exits permanently via PermanentError.
	// The slot does not reacquire.
	OnSlotDead(ctx context.Context, e SlotDeadEvent)

	// OnSlotRetired is called when a slot's WorkFn retires its work ID with
	// worklease.ErrRetire and the lease is released with ExitRetired. The slot
	// does not reacquire.
	OnSlotRetired(ctx context.Context, e SlotRetiredEvent)
}

// SlotAcquiredEvent carries the context of a slot acquisition.
type SlotAcquiredEvent struct{ WorkID string }

// SlotLostEvent carries the context of a slot fencing loss.
type SlotLostEvent struct{ WorkID string }

// SlotBackoffEvent carries the context of a slot entering backoff.
type SlotBackoffEvent struct {
	WorkID   string
	Err      error
	Duration time.Duration
}

// SlotDeadEvent carries the context of a slot exiting permanently.
type SlotDeadEvent struct {
	WorkID string
	Err    error
}

// SlotRetiredEvent carries the context of a slot whose work ID was retired.
type SlotRetiredEvent struct {
	WorkID string
}

// noopPoolObserver is a pool.Observer that discards all slot lifecycle events.
// Substituted when Config.Observer is nil.
type noopPoolObserver struct{}

func (noopPoolObserver) OnSlotAcquired(_ context.Context, _ SlotAcquiredEvent) {}
func (noopPoolObserver) OnSlotLost(_ context.Context, _ SlotLostEvent)         {}
func (noopPoolObserver) OnSlotBackoff(_ context.Context, _ SlotBackoffEvent)   {}
func (noopPoolObserver) OnSlotDead(_ context.Context, _ SlotDeadEvent)         {}
func (noopPoolObserver) OnSlotRetired(_ context.Context, _ SlotRetiredEvent)   {}

// WorkFn is the work function executed per slot.
// The workID argument identifies the slot. The token argument is the current
// lease token; use it for mid-work Checkpoint calls. The prior argument
// carries the last checkpointed state and how the immediately previous holder
// exited. Return final state to checkpoint, or nil to skip it. Return nil to
// release with ExitFinished and run again after RerunInterval, an error wrapping
// worklease.ErrRetire to release with ExitRetired and stop the slot, a
// PermanentError to release with ExitAbandoned and drop the slot, or any other
// error to release with ExitAbandoned and back off.
type WorkFn func(ctx context.Context, workID string, token worklease.Token, prior worklease.Checkpoint) ([]byte, error)

// Pacing defaults applied by New when the matching Config field is zero or
// negative, and the jitter fraction for IdleInterval and RerunInterval waits.
const (
	defaultIdleInterval    = time.Second
	defaultRerunInterval   = time.Second
	defaultBackoffInterval = time.Second
	idleJitter             = 0.20
)

// Config holds construction parameters for a Pool.
type Config struct {
	// WorkIDs is the fixed set of work IDs this pool competes for.
	// Required; an empty slice returns ErrEmptyWorkIDs from New.
	WorkIDs []string

	// AcquireOptions are passed to each slot's internal Runner. Passing
	// worklease.WithWaitForLease returns ErrWithWaitForLeaseProhibited from New.
	AcquireOptions []worklease.AcquireOption

	// RenewalOptions are passed to each slot's internal Runner. Optional.
	RenewalOptions []worklease.RenewalOption

	// IdleInterval is the wait before retrying a slot whose work ID another
	// holder holds, meaning Runner.Run returned ErrLeaseHeld. Zero or negative
	// means 1s. Each wait is drawn uniformly from
	// [IdleInterval, 1.2*IdleInterval). OnSlotBackoff is not called for these
	// waits.
	IdleInterval time.Duration

	// RerunInterval is the wait before reacquiring a slot after its WorkFn
	// returned nil and the lease was released with ExitFinished. Zero or
	// negative means 1s. Each wait is drawn uniformly from
	// [RerunInterval, 1.2*RerunInterval), which also gives peer processes a
	// chance to acquire the work ID. OnSlotBackoff is not called for these
	// waits.
	RerunInterval time.Duration

	// BackoffInterval is the wait before reacquiring a slot after Runner.Run
	// returns a non-permanent error other than ErrFenced and ErrLeaseHeld.
	// Zero or negative means 1s. No jitter.
	BackoffInterval time.Duration

	// CleanupTimeout is passed to each slot's Runner. It bounds the final
	// Checkpoint and Release, which survive cancellation of Run's context.
	// Zero or negative means 5s.
	CleanupTimeout time.Duration

	// Observer receives callbacks on slot lifecycle transitions.
	// Optional; nil installs a no-op observer.
	Observer Observer
}

// Pool distributes a fixed set of work IDs across competing processes.
// Multiple Pool instances — one per process — share a backend and collectively
// cover the work ID set. Rebalancing is emergent from lease acquisition races.
// Pool uses one worker.Runner per active slot internally.
type Pool struct {
	lease  worklease.Lease
	cfg    Config
	fn     WorkFn
	obs    Observer // never nil after New
	mu     sync.Mutex
	active map[string]struct{}
}

// New constructs a Pool. Does not start slot acquisition — call Run.
// Returns ErrNilLease if lease is nil, ErrEmptyWorkIDs if cfg.WorkIDs is empty,
// ErrWithWaitForLeaseProhibited if cfg.AcquireOptions includes WithWaitForLease.
// All three satisfy errors.Is(err, ErrConfigInvalid).
func New(lease worklease.Lease, cfg Config, fn WorkFn) (*Pool, error) {
	// ===== STEP 1: Nil lease check =====
	if lease == nil {
		return nil, ErrNilLease
	}

	// ===== STEP 2: Empty WorkIDs check =====
	if len(cfg.WorkIDs) == 0 {
		return nil, ErrEmptyWorkIDs
	}

	// ===== STEP 3: WithWaitForLease check =====
	if worklease.HasWaitForLease(cfg.AcquireOptions) {
		return nil, ErrWithWaitForLeaseProhibited
	}

	// ===== STEP 4: Apply pacing defaults for zero or negative values =====
	if cfg.IdleInterval <= 0 {
		cfg.IdleInterval = defaultIdleInterval
	}
	if cfg.RerunInterval <= 0 {
		cfg.RerunInterval = defaultRerunInterval
	}
	if cfg.BackoffInterval <= 0 {
		cfg.BackoffInterval = defaultBackoffInterval
	}

	// ===== STEP 5: Inject NoOp observer when nil =====
	obs := cfg.Observer
	if obs == nil {
		obs = noopPoolObserver{}
	}

	return &Pool{
		lease:  lease,
		cfg:    cfg,
		fn:     fn,
		obs:    obs,
		active: make(map[string]struct{}),
	}, nil
}

// Run starts acquisition loops for all configured work IDs and blocks until
// ctx is cancelled or every slot has exited. A slot exits when its WorkFn
// retires the work ID with worklease.ErrRetire or returns a PermanentError.
// Run returns ErrAllSlotsDead only if every slot exited through a
// PermanentError. It returns nil if ctx was cancelled or at least one slot
// retired. Every active slot completes its final Checkpoint and Release,
// bounded by CleanupTimeout, before Run returns. Run is not safe to call
// concurrently on the same Pool.
func (p *Pool) Run(ctx context.Context) error {
	// Internal context so the last dying slot can unblock idle siblings.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var dead atomic.Int64
	total := int64(len(p.cfg.WorkIDs))

	var wg sync.WaitGroup
	wg.Add(len(p.cfg.WorkIDs))

	for _, id := range p.cfg.WorkIDs {
		workID := id // capture loop variable

		go func() {
			defer wg.Done()

			for {
				// ===== Construct runner — new per iteration =====
				// Active marking and OnSlotAcquired fire at WorkFn ENTRY, inside
				// the adapter — not around r.Run, which also spans acquisition.
				retired := false
				slotFn := func(wfCtx context.Context, token worklease.Token, prior worklease.Checkpoint) ([]byte, error) {
					p.obs.OnSlotAcquired(wfCtx, SlotAcquiredEvent{WorkID: workID})
					p.mu.Lock()
					p.active[workID] = struct{}{}
					p.mu.Unlock()
					defer func() {
						p.mu.Lock()
						delete(p.active, workID)
						p.mu.Unlock()
					}()
					state, err := p.fn(wfCtx, workID, token, prior)
					retired = errors.Is(err, worklease.ErrRetire)
					return state, err
				}
				r, err := worker.NewRunner(worker.RunnerConfig{
					Lease:          p.lease,
					WorkFn:         slotFn,
					AcquireOptions: p.cfg.AcquireOptions,
					RenewalOptions: p.cfg.RenewalOptions,
					CleanupTimeout: p.cfg.CleanupTimeout,
				})
				if err != nil {
					// NewRunner only fails on nil Lease or nil WorkFn — both guaranteed non-nil here.
					return
				}

				// ===== Execute =====
				runErr := r.Run(runCtx, workID)

				// ===== Decide next action — first match wins =====
				var pe PermanentError
				switch {
				case runErr == nil && retired:
					p.obs.OnSlotRetired(runCtx, SlotRetiredEvent{WorkID: workID})
					return // retired — exit goroutine; does not count toward dead
				case runCtx.Err() != nil:
					return // ctx cancelled (external or all-dead) — exit goroutine
				case runErr == nil:
					if !sleep(runCtx, jitterWait(p.cfg.RerunInterval)) {
						return
					}
				case errors.Is(runErr, worklease.ErrFenced):
					p.obs.OnSlotLost(runCtx, SlotLostEvent{WorkID: workID})
					// reacquire immediately, no wait
				case errors.As(runErr, &pe) && pe.Permanent():
					p.obs.OnSlotDead(runCtx, SlotDeadEvent{WorkID: workID, Err: runErr})
					if dead.Add(1) == total {
						cancel() // last slot dead — unblock idle siblings
					}
					return // exit goroutine permanently
				case errors.Is(runErr, worklease.ErrLeaseHeld):
					if !sleep(runCtx, jitterWait(p.cfg.IdleInterval)) {
						return
					}
				default:
					p.obs.OnSlotBackoff(runCtx, SlotBackoffEvent{WorkID: workID, Err: runErr, Duration: p.cfg.BackoffInterval})
					if !sleep(runCtx, p.cfg.BackoffInterval) {
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	if dead.Load() == total {
		return ErrAllSlotsDead
	}
	return nil
}

// jitterWait returns base plus additive jitter drawn uniformly from [0, idleJitter*base).
func jitterWait(base time.Duration) time.Duration {
	return base + time.Duration(rand.Float64()*idleJitter*float64(base))
}

// sleep waits for d or until ctx is done. It returns false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ActiveSlots returns the work IDs currently held by this Pool instance.
// Safe for concurrent use.
func (p *Pool) ActiveSlots() []string {
	p.mu.Lock()
	result := make([]string, 0, len(p.active))
	for id := range p.active {
		result = append(result, id)
	}
	p.mu.Unlock()
	return result
}
