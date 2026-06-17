package pool

import (
	"context"
	"errors"
	"fmt"
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

// Observer receives callbacks on slot lifecycle transitions. All methods are
// called synchronously. Implementations must not block or panic. The zero value
// of Config.Observer is nil; the library substitutes a no-op observer.
type Observer interface {
	// OnSlotAcquired is called when a slot's Runner.Run begins executing WorkFn.
	OnSlotAcquired(ctx context.Context, e SlotAcquiredEvent)
	// OnSlotLost is called when a slot loses its lease via fencing.
	OnSlotLost(ctx context.Context, e SlotLostEvent)
	// OnSlotBackoff is called when a slot enters backoff after a non-permanent, non-fencing error.
	OnSlotBackoff(ctx context.Context, e SlotBackoffEvent)
	// OnSlotDead is called when a slot exits permanently via PermanentError.
	OnSlotDead(ctx context.Context, e SlotDeadEvent)
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

// noopPoolObserver is a pool.Observer that discards all slot lifecycle events.
// Substituted when Config.Observer is nil.
type noopPoolObserver struct{}

func (noopPoolObserver) OnSlotAcquired(_ context.Context, _ SlotAcquiredEvent) {}
func (noopPoolObserver) OnSlotLost(_ context.Context, _ SlotLostEvent)         {}
func (noopPoolObserver) OnSlotBackoff(_ context.Context, _ SlotBackoffEvent)   {}
func (noopPoolObserver) OnSlotDead(_ context.Context, _ SlotDeadEvent)         {}

// WorkFn is the work function executed per slot.
// The ctx argument is the renewal context — cancelled on fencing or renewal failure.
// The workID argument identifies which slot is executing.
// The token argument is the current lease token — use it for mid-work Checkpoint calls.
// The prior argument contains the last checkpointed state from the previous holder, or nil.
// The cleanHandoff argument is true if the previous holder released explicitly.
// Return (checkpoint []byte, error): checkpoint is written as final state if
// non-nil and error is not ErrFenced. Return a PermanentError to drop the slot
// without reacquisition.
type WorkFn func(ctx context.Context, workID string, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error)

// Config holds construction parameters for a Pool.
type Config struct {
	// WorkIDs is the fixed set of work IDs this pool competes for.
	// Required — empty slice returns ErrEmptyWorkIDs from New.
	WorkIDs []string

	// AcquireOptions are passed to each slot's internal Runner. Optional; nil
	// passes no options to each slot's Runner.
	// Must not include worklease.WithWaitForLease — pool manages its own
	// acquisition loop; blocking inside Runner prevents clean ctx cancellation.
	// Passing WithWaitForLease returns ErrWithWaitForLeaseProhibited from New.
	AcquireOptions []worklease.AcquireOption

	// RenewalOptions are passed to each slot's internal Runner. Optional; nil
	// uses the Runner default renewal interval (TTL/2).
	RenewalOptions []worklease.RenewalOption

	// BackoffInterval is the wait before reacquiring a slot after WorkFn
	// returns a non-permanent, non-fencing error. Zero means immediate retry.
	BackoffInterval time.Duration

	// Observer receives callbacks on slot lifecycle transitions.
	// Optional — zero value (nil) installs a no-op observer. Never panics on nil.
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

	// ===== STEP 4: Inject NoOp observer when nil =====
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
// ctx is cancelled or all slots are permanently dead. Returns ErrAllSlotsDead if
// every slot exited via PermanentError before ctx was cancelled; returns nil on
// clean shutdown. All active slots complete or release before Run returns.
// Run is not safe to call concurrently on the same Pool.
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
				slotFn := func(wfCtx context.Context, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
					p.obs.OnSlotAcquired(wfCtx, SlotAcquiredEvent{WorkID: workID})
					p.mu.Lock()
					p.active[workID] = struct{}{}
					p.mu.Unlock()
					defer func() {
						p.mu.Lock()
						delete(p.active, workID)
						p.mu.Unlock()
					}()
					return p.fn(wfCtx, workID, token, prior, cleanHandoff)
				}
				r, err := worker.NewRunner(worker.RunnerConfig{
					Lease:          p.lease,
					WorkFn:         slotFn,
					AcquireOptions: p.cfg.AcquireOptions,
					RenewalOptions: p.cfg.RenewalOptions,
				})
				if err != nil {
					// NewRunner only fails on nil Lease or nil WorkFn — both guaranteed non-nil here.
					return
				}

				// ===== Execute =====
				runErr := r.Run(runCtx, workID)

				// ===== Decide next action =====
				if runCtx.Err() != nil {
					return // ctx cancelled (external or all-dead) — exit goroutine
				}
				if errors.Is(runErr, worklease.ErrFenced) {
					p.obs.OnSlotLost(runCtx, SlotLostEvent{WorkID: workID})
					continue // reacquire immediately, no backoff
				}
				var pe PermanentError
				if errors.As(runErr, &pe) && pe.Permanent() {
					p.obs.OnSlotDead(runCtx, SlotDeadEvent{WorkID: workID, Err: runErr})
					if dead.Add(1) == total {
						cancel() // last slot dead — unblock idle siblings
					}
					return // exit goroutine permanently
				}
				if runErr != nil {
					// non-permanent error — wait BackoffInterval then retry
					p.obs.OnSlotBackoff(runCtx, SlotBackoffEvent{WorkID: workID, Err: runErr, Duration: p.cfg.BackoffInterval})
					if p.cfg.BackoffInterval > 0 {
						select {
						case <-runCtx.Done():
							return
						case <-time.After(p.cfg.BackoffInterval):
						}
					}
					continue
				}
				// nil return — reacquire immediately
			}
		}()
	}

	wg.Wait()
	if dead.Load() == total {
		return ErrAllSlotsDead
	}
	return nil
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
