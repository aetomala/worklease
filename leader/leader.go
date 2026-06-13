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

	// ===== STEP 3: StartRenewal =====
	renewCtx, stopRenewal := lease.StartRenewal(ctx, token, cfg.RenewalOptions...)

	// ===== STEP 4: Defer stopRenewal =====
	defer stopRenewal()

	// ===== STEP 5: Call fn =====
	fnErr := fn(renewCtx)

	// ===== STEP 6: stopRenewal (explicit) =====
	stopRenewal()

	// ===== STEP 7: Check for fencing =====
	if errors.Is(fnErr, worklease.ErrFenced) {
		return worklease.ErrFenced
	}

	// ===== STEP 8: Release =====
	releaseErr := lease.Release(ctx, token)
	if errors.Is(releaseErr, worklease.ErrFenced) {
		return worklease.ErrFenced
	}

	// ===== STEP 9: BackoffInterval sleep (non-fencing paths only) =====
	if cfg.BackoffInterval > 0 {
		select {
		case <-time.After(cfg.BackoffInterval):
		case <-ctx.Done():
		}
	}

	// ===== STEP 10: Return fn error if present, else release error =====
	if fnErr != nil {
		return fnErr
	}
	return releaseErr
}
