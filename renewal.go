package worklease

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// StartRenewal begins a managed renewal goroutine that renews the lease until it
// is stopped, fenced, or the lease window is exhausted. It returns a derived
// context (renewCtx) and a stop function (stopRenewal).
//
// The renewal goroutine has four lifecycle paths:
//   - Normal stop: the caller invokes stopRenewal(). The goroutine exits without
//     cancelling renewCtx; context.Cause(renewCtx) returns nil.
//   - Fencing: Renew returns ErrFenced. The goroutine cancels renewCtx with cause
//     ErrFenced and exits, after emitting OnRenew then OnFenced. No retry.
//   - Lease window exhausted: a non-fencing Renew error is retried with exponential
//     backoff until time.Now() >= token.ExpiresAt(). The goroutine then cancels
//     renewCtx with cause ErrLeaseWindowExhausted and exits.
//   - Parent context cancelled: ctx.Done() fires. The goroutine exits; renewCtx
//     auto-cancels as a child of ctx and the goroutine sets no cause.
//
// stopRenewal blocks until the goroutine has fully exited and is idempotent.
// Callers must invoke stopRenewal before Release, and must use the original ctx
// (not renewCtx) for Release.
func (c *leaseClient) StartRenewal(ctx context.Context, token Token, opts ...RenewalOption) (context.Context, func()) {
	// ===== STEP 1: Resolve options =====
	rcfg := renewalConfig{
		renewalInterval: c.cfg.TTL / 2,
		backoffInitial:  defaultBackoffInitial,
		backoffMax:      defaultBackoffMax,
		backoffJitter:   defaultBackoffJitter,
	}
	for _, o := range opts {
		o(&rcfg)
	}

	// ===== STEP 2: Derive cancel-cause context =====
	renewCtx, cancelCause := context.WithCancelCause(ctx)

	// ===== STEP 3: Prepare stop channel and wait group =====
	stopCh := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup

	// ===== STEP 4: Start goroutine =====
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(rcfg.renewalInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return // Path 1: normal stop — do NOT cancel
			case <-ctx.Done():
				return // Path 4: parent cancelled — renewCtx auto-cancels
			case <-ticker.C:
				if !c.renewCycle(ctx, token, rcfg, cancelCause, stopCh) {
					return // Path 2 or 3 — cause already set inside renewCycle
				}
			}
		}
	}()

	// ===== STEP 5: Build stop function =====
	stopRenewal := func() {
		once.Do(func() { close(stopCh) })
		wg.Wait()
	}

	return renewCtx, stopRenewal
}

// renewCycle runs one renewal cycle: an initial Renew attempt followed, on a
// non-fencing error, by exponential-backoff retries bounded by the lease window.
// It returns true when the lease was renewed (await the next tick) and false when
// the goroutine must exit. On a false return for a fencing or window-exhausted
// outcome, renewCycle has already called cancelCause; on stop/parent it has not.
func (c *leaseClient) renewCycle(ctx context.Context, token Token, rcfg renewalConfig, cancelCause context.CancelCauseFunc, stopCh <-chan struct{}) bool {
	attempt := 1
	base := rcfg.backoffInitial
	for {
		start := time.Now()
		err := c.b.Renew(ctx, toRecord(token), c.cfg.TTL)
		dur := time.Since(start)
		c.obs.OnRenew(ctx, RenewEvent{Token: token, Duration: dur, Attempt: attempt, Err: err})

		switch {
		case errors.Is(err, ErrFenced):
			c.obs.OnFenced(ctx, FencedEvent{Token: token, Operation: OperationRenew})
			cancelCause(ErrFenced) // Path 2
			return false
		case err == nil:
			return true // renewed — await next tick
		}

		// ===== Non-fencing error: retry with backoff, bounded by the lease window =====
		if !time.Now().Before(token.ExpiresAt()) {
			cancelCause(ErrLeaseWindowExhausted) // Path 3 (before sleep)
			return false
		}
		timer := time.NewTimer(backoffWait(base, rcfg.backoffJitter))
		select {
		case <-stopCh:
			timer.Stop()
			return false // Path 1
		case <-ctx.Done():
			timer.Stop()
			return false // Path 4
		case <-timer.C:
		}
		if !time.Now().Before(token.ExpiresAt()) {
			cancelCause(ErrLeaseWindowExhausted) // Path 3 (after sleep)
			return false
		}
		base = nextBackoff(base, rcfg.backoffMax)
		attempt++
	}
}

// backoffWait returns base plus additive jitter drawn from [0, jitter*base).
// The result is never below base.
func backoffWait(base time.Duration, jitter float64) time.Duration {
	if jitter <= 0 {
		return base
	}
	return base + time.Duration(jitter*float64(base)*rand.Float64())
}

// nextBackoff doubles base, capped at maxInterval.
func nextBackoff(base, maxInterval time.Duration) time.Duration {
	next := base * 2
	if next > maxInterval {
		return maxInterval
	}
	return next
}
