package leader

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aetomala/worklease"
)

// Sentinel errors for leader operations.
var (
	// ErrLeaseRequired is returned by Elect when lease is nil.
	ErrLeaseRequired = errors.New("leader: lease is required")
)

// defaultCleanupTimeout bounds Release when Config.CleanupTimeout is zero or
// negative.
const defaultCleanupTimeout = 5 * time.Second

// Config holds options for Elect.
// TTL and HolderID are already embedded in the worklease.Lease passed to Elect.
type Config struct {
	// AcquireOptions are passed to Lease.Acquire. Optional; nil uses defaults.
	// Pass worklease.WithWaitForLease() here to block until leadership is
	// available.
	AcquireOptions []worklease.AcquireOption

	// RenewalOptions are passed to Lease.StartRenewal. Optional; nil uses
	// defaults.
	RenewalOptions []worklease.RenewalOption

	// BackoffInterval is the duration Elect sleeps before returning on every
	// non-fencing path, including ExitAbandoned. Zero means no sleep. Fencing
	// paths bypass the sleep.
	BackoffInterval time.Duration

	// CleanupTimeout bounds Release, which runs on a context that survives
	// cancellation of the context passed to Elect. Zero or negative means 5s.
	CleanupTimeout time.Duration

	// OnElected is called after Acquire succeeds, before fn is invoked.
	// Optional; nil is a no-op.
	OnElected func(ctx context.Context, token worklease.Token)

	// OnLost is called when the renewal context is cancelled by fencing or by
	// an exhausted lease window, and the parent context is not cancelled.
	// Optional; nil is a no-op.
	OnLost func(ctx context.Context, token worklease.Token)

	// OnRelinquished is called after Release succeeds with ExitFinished or
	// ExitRetired. It is not called after ExitAbandoned. Optional; nil is a
	// no-op.
	OnRelinquished func(ctx context.Context, token worklease.Token)
}

// Elect acquires workID and calls fn under a managed renewal context.
// The fn argument receives a context that is cancelled if the lease is fenced,
// if the lease window is exhausted, or if ctx is cancelled; fn must respect it.
// Elect records fn's outcome as the exit mode: nil releases with ExitFinished,
// an error wrapping worklease.ErrRetire releases with ExitRetired and Elect
// returns nil, and any other error releases with ExitAbandoned. Elect does not
// release when fenced or when the lease window was exhausted; the successor
// sees ExitExpired. Release runs on a context that survives cancellation of
// ctx, bounded by Config.CleanupTimeout. Elect surfaces worklease.ErrLeaseHeld,
// worklease.ErrFenced, and context errors from the underlying Lease unchanged.
// Elect does not force blocking acquisition; pass worklease.WithWaitForLease()
// in cfg.AcquireOptions to block until leadership is available.
func Elect(ctx context.Context, lease worklease.Lease, workID string, cfg Config, fn func(ctx context.Context) error) error {
	// ===== STEP 1: Nil check =====
	if lease == nil {
		return ErrLeaseRequired
	}

	// backoff sleeps BackoffInterval on non-fencing paths, cut short by ctx.
	backoff := func() {
		if cfg.BackoffInterval > 0 {
			select {
			case <-time.After(cfg.BackoffInterval):
			case <-ctx.Done():
			}
		}
	}

	// ===== STEP 2: Acquire =====
	token, err := lease.Acquire(ctx, workID, cfg.AcquireOptions...)
	if err != nil {
		return err
	}

	// ===== STEP 3: OnElected (use ctx, not renewCtx) =====
	if cfg.OnElected != nil {
		cfg.OnElected(ctx, token)
	}

	// ===== STEP 4: StartRenewal =====
	renewCtx, stopRenewal := lease.StartRenewal(ctx, token, cfg.RenewalOptions...)
	defer stopRenewal() // panic-safety net; stopRenewal is idempotent

	// ===== STEP 5: Call fn =====
	fnErr := fn(renewCtx)

	// ===== STEP 6: OnLost if renewal context was cancelled before fn returned =====
	// Only the renewal goroutine cancels renewCtx while ctx is still live, so a
	// cancelled parent — caller shutdown — is not reported as a lost lease.
	if renewCtx.Err() != nil && ctx.Err() == nil && cfg.OnLost != nil {
		cfg.OnLost(ctx, token)
	}

	// ===== STEP 7: Stop Renewal and Read Its Cause =====
	stopRenewal()
	cause := context.Cause(renewCtx)

	// ===== STEP 8: Classify the Outcome =====
	if errors.Is(fnErr, worklease.ErrFenced) || errors.Is(cause, worklease.ErrFenced) {
		return worklease.ErrFenced
	}
	if errors.Is(cause, worklease.ErrLeaseWindowExhausted) {
		result := fnErr
		if result == nil {
			result = fmt.Errorf("leader: %w", cause)
		}
		backoff()
		return result
	}
	mode, result := worklease.ExitAbandoned, fnErr
	switch {
	case errors.Is(fnErr, worklease.ErrRetire):
		mode, result = worklease.ExitRetired, nil
	case fnErr == nil:
		mode = worklease.ExitFinished
	}

	// ===== STEP 9: Release on the Cleanup Context =====
	cleanupTimeout := cfg.CleanupTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = defaultCleanupTimeout
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	relErr := lease.Release(cleanupCtx, token, mode)

	// ===== STEP 10: OnRelinquished only after a recorded Finished or Retired exit =====
	if relErr == nil && mode != worklease.ExitAbandoned && cfg.OnRelinquished != nil {
		cfg.OnRelinquished(ctx, token)
	}

	// ===== STEP 11: Handle a Release Error =====
	if errors.Is(relErr, worklease.ErrFenced) {
		return worklease.ErrFenced
	}
	if relErr != nil && result == nil {
		result = fmt.Errorf("leader: release: %w", relErr)
	}

	// ===== STEP 12: BackoffInterval sleep (non-fencing paths only) =====
	backoff()

	// ===== STEP 13: Return =====
	return result
}
