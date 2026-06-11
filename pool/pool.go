package pool

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/worker"
)

// Sentinel errors for pool operations.
var (
	// ErrConfigInvalid is returned by New when cfg.WorkIDs is empty, lease is nil,
	// or cfg.AcquireOptions includes WithWaitForLease.
	ErrConfigInvalid = errors.New("pool: invalid config — WorkIDs and lease are required, WithWaitForLease is prohibited")
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
	// Required — empty slice returns ErrConfigInvalid from New.
	WorkIDs []string

	// AcquireOptions are passed to each slot's internal Runner. Optional; nil
	// passes no options to each slot's Runner.
	// Must not include worklease.WithWaitForLease — pool manages its own
	// acquisition loop; blocking inside Runner prevents clean ctx cancellation.
	AcquireOptions []worklease.AcquireOption

	// RenewalOptions are passed to each slot's internal Runner. Optional; nil
	// uses the Runner default renewal interval (TTL/2).
	RenewalOptions []worklease.RenewalOption

	// BackoffInterval is the wait before reacquiring a slot after WorkFn
	// returns a non-permanent, non-fencing error. Zero means immediate retry.
	BackoffInterval time.Duration
}

// Pool distributes a fixed set of work IDs across competing processes.
// Multiple Pool instances — one per process — share a backend and collectively
// cover the work ID set. Rebalancing is emergent from lease acquisition races.
// Pool uses one worker.Runner per active slot internally.
type Pool struct {
	lease  worklease.Lease
	cfg    Config
	fn     WorkFn
	mu     sync.Mutex
	active map[string]struct{}
}

// New constructs a Pool. Does not start slot acquisition — call Run.
// Returns ErrConfigInvalid if cfg.WorkIDs is empty, lease is nil, or
// cfg.AcquireOptions includes WithWaitForLease.
func New(lease worklease.Lease, cfg Config, fn WorkFn) (*Pool, error) {
	// ===== STEP 1: Nil lease check =====
	if lease == nil {
		return nil, ErrConfigInvalid
	}

	// ===== STEP 2: Empty WorkIDs check =====
	if len(cfg.WorkIDs) == 0 {
		return nil, ErrConfigInvalid
	}

	// ===== STEP 3: WithWaitForLease check =====
	if worklease.HasWaitForLease(cfg.AcquireOptions) {
		return nil, ErrConfigInvalid
	}

	return &Pool{
		lease:  lease,
		cfg:    cfg,
		fn:     fn,
		active: make(map[string]struct{}),
	}, nil
}

// Run starts acquisition loops for all configured work IDs and blocks until
// ctx is cancelled. All active slots complete or release before Run returns.
// Run is not safe to call concurrently on the same Pool.
func (p *Pool) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(len(p.cfg.WorkIDs))

	for _, id := range p.cfg.WorkIDs {
		workID := id // capture loop variable

		go func() {
			defer wg.Done()

			for {
				// ===== Construct runner — new per iteration =====
				slotFn := func(runCtx context.Context, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
					return p.fn(runCtx, workID, token, prior, cleanHandoff)
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

				// ===== Mark active immediately before r.Run =====
				p.mu.Lock()
				p.active[workID] = struct{}{}
				p.mu.Unlock()

				// ===== Execute =====
				runErr := r.Run(ctx, workID)

				// ===== Unmark active immediately after r.Run returns =====
				p.mu.Lock()
				delete(p.active, workID)
				p.mu.Unlock()

				// ===== Decide next action =====
				if ctx.Err() != nil {
					return // ctx cancelled — exit goroutine
				}
				if errors.Is(runErr, worklease.ErrFenced) {
					continue // reacquire immediately, no backoff
				}
				var pe PermanentError
				if errors.As(runErr, &pe) && pe.Permanent() {
					return // exit goroutine permanently
				}
				if runErr != nil {
					// non-permanent error — wait BackoffInterval then retry
					if p.cfg.BackoffInterval > 0 {
						select {
						case <-ctx.Done():
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
