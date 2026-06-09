package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/aetomala/worklease"
)

// Sentinel errors for Runner operations.
var (
	ErrLeaseRequired  = errors.New("lease is required")
	ErrWorkFnRequired = errors.New("work function is required")
)

// WorkFn is the work function signature accepted by Runner.Run. ctx is the
// renewal context — it is cancelled when the lease is fenced or lost, so
// fencing propagates into the work automatically. prior contains the last
// checkpointed state from the previous holder (nil on first run). cleanHandoff
// is true when the previous holder released intentionally. Return the final
// state to checkpoint after the work completes (nil skips the final checkpoint)
// and any error.
type WorkFn func(ctx context.Context, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error)

// RunnerConfig holds configuration for a Runner instance.
type RunnerConfig struct {
	Lease          worklease.Lease           // Required.
	WorkFn         WorkFn                    // Required.
	AcquireOptions []worklease.AcquireOption // Optional; nil uses defaults.
	RenewalOptions []worklease.RenewalOption // Optional; nil uses defaults.
}

// Runner manages the acquire/checkpoint/release lifecycle for a WorkFn.
// Callers construct a Runner once and call Run for each unit of work. All
// methods are safe for concurrent use.
type Runner struct {
	lease          worklease.Lease
	fn             WorkFn
	acquireOptions []worklease.AcquireOption
	renewalOptions []worklease.RenewalOption
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

	// ===== STEP 2: Initialize and Return =====
	return &Runner{
		lease:          cfg.Lease,
		fn:             cfg.WorkFn,
		acquireOptions: cfg.AcquireOptions,
		renewalOptions: cfg.RenewalOptions,
	}, nil
}

// Run acquires the lease for workID, reads prior checkpoint state, starts
// automatic renewal, calls the WorkFn with the renewal context, checkpoints
// any returned final state, stops renewal, and releases the lease. Returns
// worklease.ErrFenced if the lease is superseded at any point — in that case
// Release is not called. Returns the WorkFn error on non-fencing work failure
// after checkpointing any partial state and releasing the lease. Returns
// worklease.ErrLeaseHeld if the lease is already held and WithWaitForLease was
// not configured.
func (r *Runner) Run(ctx context.Context, workID string) error {
	// ===== STEP 1: Acquire =====
	token, err := r.lease.Acquire(ctx, workID, r.acquireOptions...)
	if err != nil {
		return fmt.Errorf("worker: acquire: %w", err)
	}

	// ===== STEP 2: Read Prior Checkpoint =====
	prior, cleanHandoff, err := r.lease.ReadCheckpoint(ctx, token)
	if err != nil {
		if errors.Is(err, worklease.ErrFenced) {
			return worklease.ErrFenced
		}
		_ = r.lease.Release(ctx, token)
		return fmt.Errorf("worker: read checkpoint: %w", err)
	}

	// ===== STEP 3: Start Renewal =====
	renewCtx, stopRenewal := r.lease.StartRenewal(ctx, token, r.renewalOptions...)

	// ===== STEP 4: Call WorkFn =====
	finalState, workErr := r.fn(renewCtx, token, prior, cleanHandoff)

	// ===== STEP 5: Checkpoint Final State =====
	if finalState != nil && !errors.Is(workErr, worklease.ErrFenced) {
		if cpErr := r.lease.Checkpoint(ctx, token, finalState); cpErr != nil {
			stopRenewal()
			if errors.Is(cpErr, worklease.ErrFenced) {
				return worklease.ErrFenced
			}
			_ = r.lease.Release(ctx, token)
			return fmt.Errorf("worker: checkpoint: %w", cpErr)
		}
	}

	// ===== STEP 6: Stop Renewal =====
	stopRenewal()

	// ===== STEP 7: Release (unless fenced) =====
	if errors.Is(workErr, worklease.ErrFenced) {
		return worklease.ErrFenced
	}
	if relErr := r.lease.Release(ctx, token); relErr != nil {
		if errors.Is(relErr, worklease.ErrFenced) {
			return worklease.ErrFenced
		}
		return fmt.Errorf("worker: release: %w", relErr)
	}

	return workErr
}
