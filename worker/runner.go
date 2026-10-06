package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aetomala/worklease"
)

// Sentinel errors for Runner operations.
var (
	ErrLeaseRequired  = errors.New("worker: lease is required")
	ErrWorkFnRequired = errors.New("worker: work function is required")
)

// WorkFn is the work function signature accepted by Runner.Run.
// The ctx argument is the renewal context; it is cancelled on fencing, when the
// lease window is exhausted, or when the caller's context is cancelled. The
// prior argument carries the last checkpointed state and how the immediately
// previous holder exited; PrevExit is ExitNone on a first run. Return final
// state to checkpoint, or nil to skip the final checkpoint. Return nil to
// release with ExitFinished, an error wrapping worklease.ErrRetire to release
// with ExitRetired, or any other error to release with ExitAbandoned.
type WorkFn func(ctx context.Context, token worklease.Token, prior worklease.Checkpoint) ([]byte, error)

// defaultCleanupTimeout bounds the final Checkpoint and Release when
// RunnerConfig.CleanupTimeout is zero or negative.
const defaultCleanupTimeout = 5 * time.Second

// RunnerConfig holds configuration for a Runner instance.
type RunnerConfig struct {
	// Lease is the lease client. Required; nil returns ErrLeaseRequired from
	// NewRunner.
	Lease worklease.Lease

	// WorkFn is the work function. Required; nil returns ErrWorkFnRequired from
	// NewRunner.
	WorkFn WorkFn

	// AcquireOptions are passed to Lease.Acquire. Optional.
	AcquireOptions []worklease.AcquireOption

	// RenewalOptions are passed to Lease.StartRenewal. Optional.
	RenewalOptions []worklease.RenewalOption

	// CleanupTimeout bounds the final Checkpoint and Release, which run on a
	// context that survives cancellation of the caller's context. Zero or
	// negative means 5s.
	CleanupTimeout time.Duration
}

// Runner manages the acquire/checkpoint/release lifecycle for a WorkFn.
// Callers construct a Runner once and call Run for each unit of work. All
// methods are safe for concurrent use.
type Runner struct {
	lease          worklease.Lease
	fn             WorkFn
	acquireOptions []worklease.AcquireOption
	renewalOptions []worklease.RenewalOption
	cleanupTimeout time.Duration // Always positive after NewRunner
}

// NewRunner returns a new Runner. Returns ErrLeaseRequired if cfg.Lease is nil,
// ErrWorkFnRequired if cfg.WorkFn is nil.
func NewRunner(cfg RunnerConfig) (*Runner, error) {
	// ===== STEP 1: Validate Required Fields =====
	if cfg.Lease == nil {
		return nil, ErrLeaseRequired
	}
	if cfg.WorkFn == nil {
		return nil, ErrWorkFnRequired
	}

	// ===== STEP 2: Apply Defaults for Zero Values =====
	cleanupTimeout := cfg.CleanupTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = defaultCleanupTimeout
	}

	// ===== STEP 3: Initialize and Return =====
	return &Runner{
		lease:          cfg.Lease,
		fn:             cfg.WorkFn,
		acquireOptions: cfg.AcquireOptions,
		renewalOptions: cfg.RenewalOptions,
		cleanupTimeout: cleanupTimeout,
	}, nil
}

// Run acquires the lease for workID, reads the prior Checkpoint, starts
// automatic renewal, calls WorkFn with the renewal context, checkpoints any
// returned final state, and releases the lease with the exit mode that
// matches WorkFn's outcome. The final Checkpoint and Release run on a context
// that survives cancellation of ctx, bounded by CleanupTimeout. Run returns
// nil when WorkFn returns nil or an error wrapping worklease.ErrRetire and the
// exit is recorded. Run returns worklease.ErrFenced, without releasing, if a
// successor holds the lease. Run does not release when the lease window was
// exhausted; the successor sees ExitExpired. If WorkFn panics, renewal is
// stopped, the lease is not released, and the panic propagates.
// If ReadCheckpoint fails, Run returns the error without calling WorkFn and
// without releasing; the lease expires after its TTL and the successor sees
// ExitExpired. If the final Checkpoint fails with a non-fencing error, Run
// releases with ExitAbandoned and returns the checkpoint error, or WorkFn's
// error if WorkFn failed. If Release fails with a non-fencing error, the exit
// may not be recorded; after ErrLeaseExpired it is not, and the successor sees
// ExitExpired. Run then returns the release error if WorkFn succeeded, or
// WorkFn's error otherwise.
func (r *Runner) Run(ctx context.Context, workID string) error {
	// ===== STEP 1: Acquire =====
	token, err := r.lease.Acquire(ctx, workID, r.acquireOptions...)
	if err != nil {
		return fmt.Errorf("worker: acquire: %w", err)
	}

	// ===== STEP 2: Read Prior Checkpoint =====
	// No Release on failure: the lease expires after its TTL and the successor
	// sees ExitExpired with this holder as PrevHolderID.
	cp, err := r.lease.ReadCheckpoint(ctx, token)
	if err != nil {
		if errors.Is(err, worklease.ErrFenced) {
			return worklease.ErrFenced
		}
		return fmt.Errorf("worker: read checkpoint: %w", err)
	}

	// ===== STEP 3: Start Renewal =====
	renewCtx, stopRenewal := r.lease.StartRenewal(ctx, token, r.renewalOptions...)
	defer stopRenewal() // panic-safety net; stopRenewal is idempotent

	// ===== STEP 4: Call WorkFn =====
	finalState, workErr := r.fn(renewCtx, token, cp)

	// ===== STEP 5: Stop Renewal and Read Its Cause =====
	stopRenewal()
	cause := context.Cause(renewCtx)

	// ===== STEP 6: Classify the Outcome =====
	if errors.Is(workErr, worklease.ErrFenced) || errors.Is(cause, worklease.ErrFenced) {
		return worklease.ErrFenced
	}
	if errors.Is(cause, worklease.ErrLeaseWindowExhausted) {
		if workErr != nil {
			return workErr
		}
		return fmt.Errorf("worker: %w", cause)
	}
	mode, result := worklease.ExitAbandoned, workErr
	switch {
	case errors.Is(workErr, worklease.ErrRetire):
		mode, result = worklease.ExitRetired, nil
	case workErr == nil:
		mode = worklease.ExitFinished
	}
	successClass := result == nil

	// ===== STEP 7: Derive the Cleanup Context =====
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cleanupTimeout)
	defer cancel()

	// ===== STEP 8: Checkpoint Final State =====
	if finalState != nil {
		if cpErr := r.lease.Checkpoint(cleanupCtx, token, finalState); cpErr != nil {
			if errors.Is(cpErr, worklease.ErrFenced) {
				return worklease.ErrFenced
			}
			mode = worklease.ExitAbandoned
			if successClass {
				result = fmt.Errorf("worker: checkpoint: %w", cpErr)
			}
		}
	}

	// ===== STEP 9: Release With the Exit Mode =====
	if relErr := r.lease.Release(cleanupCtx, token, mode); relErr != nil {
		if errors.Is(relErr, worklease.ErrFenced) {
			return worklease.ErrFenced
		}
		if result == nil {
			return fmt.Errorf("worker: release: %w", relErr)
		}
	}

	return result
}
