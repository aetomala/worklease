package leader

import (
	"context"
	"errors"
	"time"

	"github.com/aetomala/worklease"
)

// Sentinel errors for leader operations.
var (
	// ErrLeaseRequired is returned by Elect when lease is nil.
	ErrLeaseRequired = errors.New("leader: lease is required")
)

// Config holds options for Elect. TTL and HolderID are already embedded in
// the worklease.Lease passed to Elect — they are not duplicated here.
type Config struct {
	// AcquireOptions are passed to Lease.Acquire. Optional; nil passes no
	// options to Acquire and uses the default fail-fast behavior.
	// Pass worklease.WithWaitForLease() here to block until leadership is
	// available.
	AcquireOptions []worklease.AcquireOption

	// RenewalOptions are passed to Lease.StartRenewal. Optional; nil uses
	// the default renewal interval (TTL/2).
	RenewalOptions []worklease.RenewalOption

	// BackoffInterval is the duration Elect sleeps before returning on
	// non-fencing paths. Zero means no sleep. Fencing paths (ErrFenced from
	// fn or Release) bypass the sleep — callers must react to fencing
	// immediately.
	//
	// Set this when wrapping Elect in a retry loop. Because Release expires
	// the lease immediately (ADR-0012), a fast-returning fn and an immediate
	// re-call of Elect will produce rapid acquire/release/reacquire cycling.
	// BackoffInterval throttles that cycling without requiring the caller to
	// manage its own sleep. pool.Pool callers are unaffected — Pool already
	// governs reacquisition delay via Config.BackoffInterval.
	BackoffInterval time.Duration

	// OnElected is called after Acquire succeeds, before fn is invoked.
	// Optional — nil is a no-op.
	OnElected func(ctx context.Context, token worklease.Token)

	// OnLost is called when the renewal context is cancelled due to fencing
	// or renewal failure, before fn returns.
	// Optional — nil is a no-op.
	OnLost func(ctx context.Context, token worklease.Token)

	// OnRelinquished is called after Release succeeds on a clean exit.
	// Optional — nil is a no-op.
	OnRelinquished func(ctx context.Context, token worklease.Token)
}

// Elect acquires workID and calls fn under a managed renewal context.
// The fn argument receives a context cancelled if the lease is fenced or renewal fails.
// Callers must respect context cancellation — fencing propagates via context.
// Elect calls Release before returning in all non-fencing paths. Because Release expires
// the lease immediately, the work item is available to a successor as soon as Elect returns.
// Elect surfaces worklease.ErrLeaseHeld, worklease.ErrFenced, and context errors
// from the underlying Lease unchanged.
// Elect does not force blocking acquisition — pass worklease.WithWaitForLease()
// in cfg.AcquireOptions to block until leadership is available.
// If cfg.BackoffInterval is positive, Elect sleeps for that duration before returning
// on non-fencing paths — throttling retry loops without requiring callers to manage
// their own sleep. Fencing paths bypass the sleep.
func Elect(ctx context.Context, lease worklease.Lease, workID string, cfg Config, fn func(ctx context.Context) error) error {
	// ===== STEP 1: Nil check =====
	if lease == nil {
		return ErrLeaseRequired
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

	// ===== STEP 5: Defer stopRenewal (panic-safety net) =====
	defer stopRenewal()

	// ===== STEP 6: Call fn =====
	fnErr := fn(renewCtx)

	// ===== STEP 7: OnLost if renewal context was cancelled before fn returned =====
	if renewCtx.Err() != nil && cfg.OnLost != nil {
		cfg.OnLost(ctx, token)
	}

	// ===== STEP 8: stopRenewal (explicit, before Release) =====
	stopRenewal()

	// ===== STEP 9: Check for fencing — do not Release or call OnRelinquished =====
	if errors.Is(fnErr, worklease.ErrFenced) {
		return worklease.ErrFenced
	}

	// ===== STEP 10: Release =====
	releaseErr := lease.Release(ctx, token)
	if errors.Is(releaseErr, worklease.ErrFenced) {
		return worklease.ErrFenced
	}

	// ===== STEP 11: OnRelinquished only if Release succeeded =====
	if releaseErr == nil && cfg.OnRelinquished != nil {
		cfg.OnRelinquished(ctx, token)
	}

	// ===== STEP 12: BackoffInterval sleep (non-fencing paths only) =====
	if cfg.BackoffInterval > 0 {
		select {
		case <-time.After(cfg.BackoffInterval):
		case <-ctx.Done():
		}
	}

	// ===== STEP 13: Return fn error if present, else release error =====
	if fnErr != nil {
		return fnErr
	}
	return releaseErr
}
