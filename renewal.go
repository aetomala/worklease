package worklease

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
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
//     backoff until the lease window closes. The goroutine then cancels renewCtx
//     with cause ErrLeaseWindowExhausted and exits. It cancels when the window
//     closes, whether it is waiting for the next tick, for a backoff, or for an
//     in-flight Renew call. Exception: ErrLeaseExpired from Renew cancels at
//     once, without backoff, with cause
//     errors.Join(ErrLeaseWindowExhausted, ErrLeaseExpired).
//   - Parent context cancelled: ctx.Done() fires. The goroutine exits; renewCtx
//     auto-cancels as a child of ctx and the goroutine sets no cause.
//
// The lease window starts as the local start time of the acquiring backend
// call plus TTL; token.ExpiresAt(), stamped by the backend's clock, does not
// affect it. After every successful renewal it advances to the local start
// time of that Renew call plus TTL. Because a backend computes the stored
// expiry from a reading of its own clock taken during the call, which is never
// before the local start, a constant offset between the local and backend clocks
// cannot make these bounds exceed the true expiry. A backend clock that steps
// forward or runs fast during the lease can still expire it before the window
// closes; fencing then rejects the old holder's writes. LeaseWindow reads the
// current window from renewCtx.
//
// Each Renew call runs on its own goroutine, with a context derived from ctx
// whose deadline is the lease window at the time of the call. The renewal
// goroutine never waits on that call alone: on stopRenewal, on cancellation of
// ctx, or when the window closes, it cancels the call's context and moves on,
// and the call's eventual result is discarded. Every attempt is reported to
// OnRenew exactly once.
//
// The stopRenewal function returns once the renewal goroutine has exited,
// without waiting for an in-flight Renew call, and it is idempotent. Callers
// must invoke stopRenewal before Release, and must use the original ctx (not
// renewCtx) for Release.
func (c *leaseClient) StartRenewal(ctx context.Context, token Token, opts ...RenewalOption) (context.Context, func()) {
	// ===== STEP 1: Resolve options =====
	rcfg := renewalConfig{
		renewalInterval: defaultRenewalInterval(c.cfg.TTL),
		backoffInitial:  defaultBackoffInitial,
		backoffMax:      defaultBackoffMax,
		backoffJitter:   defaultBackoffJitter,
	}
	for _, o := range opts {
		o(&rcfg)
	}

	// ===== STEP 2: Derive cancel-cause context and tracked window =====
	cancelCtx, cancelCause := context.WithCancelCause(ctx)
	first := initialWindow(token)
	tw := &trackedWindow{done: cancelCtx.Done()}
	tw.end.Store(&first)
	window := first // owned by the renewal goroutine; tw.end only ever holds copies
	renewCtx := context.WithValue(cancelCtx, leaseWindowKey{}, tw)

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
			windowTimer := time.NewTimer(time.Until(window))
			select {
			case <-stopCh:
				windowTimer.Stop()
				return // Path 1: normal stop — do NOT cancel
			case <-ctx.Done():
				windowTimer.Stop()
				return // Path 4: parent cancelled — renewCtx auto-cancels
			case <-windowTimer.C:
				cancelCause(ErrLeaseWindowExhausted) // Path 3: window closed between ticks
				return
			case <-ticker.C:
				windowTimer.Stop()
			}
			if !time.Now().Before(window) {
				cancelCause(ErrLeaseWindowExhausted) // Path 3: window closed as the tick fired
				return
			}
			if !c.renewCycle(ctx, token, rcfg, cancelCause, stopCh, &window) {
				return // Path 1, 2, 3, or 4 — any cause is already set inside renewCycle
			}
			next := window
			tw.end.Store(&next)
		}
	}()

	// ===== STEP 5: Build stop function =====
	stopRenewal := func() {
		once.Do(func() { close(stopCh) })
		wg.Wait()
	}

	return renewCtx, stopRenewal
}

// LeaseWindow returns the end of the lease window tracked by the renewal
// goroutine whose renewal context is ctx or the nearest ancestor of ctx. The
// window starts as the local start of the acquiring backend call plus TTL, and
// advances to the local start of each successful renewal plus TTL. It is
// measured on the local monotonic clock, so under a constant offset between the
// local and backend clocks it never exceeds the true expiry. The ok result is
// false when ctx does not descend from a renewal context, and once that renewal
// context is done: after fencing, an exhausted window, or cancellation of its
// parent. After stopRenewal the window stays valid and stops advancing.
func LeaseWindow(ctx context.Context) (deadline time.Time, ok bool) {
	// ===== STEP 1: Find the tracked window =====
	tw, _ := ctx.Value(leaseWindowKey{}).(*trackedWindow)
	if tw == nil {
		return time.Time{}, false
	}

	// ===== STEP 2: Report nothing once the renewal context is done =====
	select {
	case <-tw.done:
		return time.Time{}, false
	default:
	}

	// ===== STEP 3: Return the current window =====
	return *tw.end.Load(), true
}

// The leaseWindowKey type is the context key under which StartRenewal stores
// the tracked lease window.
type leaseWindowKey struct{}

// The trackedWindow type is the value stored under leaseWindowKey. The renewal
// goroutine writes end; LeaseWindow reads it and reports false once done is
// closed.
type trackedWindow struct {
	end  atomic.Pointer[time.Time]
	done <-chan struct{}
}

// The initialWindow function returns the first lease window for token: the
// local start of the acquiring backend call plus TTL. A token not produced by
// Acquire, such as Token{}, has a zero deadline, so its window is already
// closed.
func initialWindow(token Token) time.Time {
	return token.deadline
}

// renewCycle runs one renewal cycle: an initial Renew attempt followed, on a
// non-fencing error, by exponential-backoff retries bounded by the lease window.
// It returns true when the lease was renewed (await the next tick) and false when
// the goroutine must exit. On a false return for a fencing or window-exhausted
// outcome, renewCycle has already called cancelCause; on stop/parent it has not.
// On success, window is advanced to the local start of the Renew call plus TTL.
func (c *leaseClient) renewCycle(ctx context.Context, token Token, rcfg renewalConfig, cancelCause context.CancelCauseFunc, stopCh <-chan struct{}, window *time.Time) bool {
	attempt := 1
	base := rcfg.backoffInitial
	for {
		start, end, err := c.renewAttempt(ctx, token, attempt, stopCh, *window)
		switch end {
		case attemptStopped, attemptParentDone:
			return false // Path 1 or Path 4 — do NOT cancel
		case attemptWindowClosed:
			cancelCause(ErrLeaseWindowExhausted) // Path 3: window closed during the attempt
			return false
		case attemptReturned:
		}

		switch {
		case errors.Is(err, ErrFenced):
			c.obs.OnFenced(ctx, FencedEvent{Token: token, Operation: OperationRenew})
			cancelCause(ErrFenced) // Path 2
			return false
		case errors.Is(err, ErrLeaseExpired):
			// Storage has found the lease lapsed; retrying cannot help (#84).
			cancelCause(errors.Join(ErrLeaseWindowExhausted, ErrLeaseExpired)) // Path 3, terminal
			return false
		case err == nil:
			*window = start.Add(c.cfg.TTL)
			return true // renewed — await next tick
		}

		// ===== Non-fencing error: retry with backoff, bounded by the lease window =====
		if !time.Now().Before(*window) {
			cancelCause(ErrLeaseWindowExhausted) // Path 3 (before sleep)
			return false
		}
		backoffTimer := time.NewTimer(backoffWait(base, rcfg.backoffJitter))
		windowTimer := time.NewTimer(time.Until(*window))
		select {
		case <-stopCh:
			backoffTimer.Stop()
			windowTimer.Stop()
			return false // Path 1
		case <-ctx.Done():
			backoffTimer.Stop()
			windowTimer.Stop()
			return false // Path 4
		case <-windowTimer.C:
			backoffTimer.Stop()
			cancelCause(ErrLeaseWindowExhausted) // Path 3 (during sleep)
			return false
		case <-backoffTimer.C:
			windowTimer.Stop()
		}
		if !time.Now().Before(*window) {
			cancelCause(ErrLeaseWindowExhausted) // Path 3 (after sleep)
			return false
		}
		base = nextBackoff(base, rcfg.backoffMax)
		attempt++
	}
}

// The attemptEnd type records why the renewal goroutine stopped waiting on one
// Renew attempt.
type attemptEnd uint8

// The attemptEnd values: the call returned, or the wait ended on W1, W2, or W3.
const (
	attemptReturned     attemptEnd = iota // The Renew call returned; its error is the attempt's result
	attemptStopped                        // W1: stopRenewal was called
	attemptParentDone                     // W2: the parent context was cancelled
	attemptWindowClosed                   // W3: the lease window closed
)

// The renewResult type carries the outcome of one Renew call from its goroutine.
type renewResult struct {
	err error
	dur time.Duration
}

// renewAttempt runs one Renew call on its own goroutine, with a context derived
// from ctx whose deadline is window, and waits for the first of: the call's
// result, stopCh, cancellation of ctx, or the window closing. It reports the
// attempt to OnRenew exactly once — with the call's error when the result
// arrives, and otherwise with context.Canceled, ctx.Err(), or
// context.DeadlineExceeded. An abandoned call's context is cancelled and its
// late result is discarded; the result channel is buffered so the call's
// goroutine never blocks. Returns the local start time of the call, why the
// wait ended, and the call's error when it returned.
func (c *leaseClient) renewAttempt(ctx context.Context, token Token, attempt int, stopCh <-chan struct{}, window time.Time) (time.Time, attemptEnd, error) {
	// ===== STEP 1: Launch the call =====
	attemptCtx, cancelAttempt := context.WithDeadline(ctx, window)
	results := make(chan renewResult, 1)
	start := time.Now()
	go func() {
		err := c.b.Renew(attemptCtx, toRecord(token), c.cfg.TTL)
		results <- renewResult{err: err, dur: time.Since(start)}
	}()

	// ===== STEP 2: Wait for the result, a stop, the parent, or the window =====
	windowTimer := time.NewTimer(time.Until(window))
	defer windowTimer.Stop()
	var (
		end attemptEnd
		res renewResult
	)
	select {
	case res = <-results:
		end = attemptReturned
	case <-stopCh:
		end, res = attemptStopped, renewResult{err: context.Canceled, dur: time.Since(start)}
	case <-ctx.Done():
		end, res = attemptParentDone, renewResult{err: ctx.Err(), dur: time.Since(start)}
	case <-windowTimer.C:
		end, res = attemptWindowClosed, renewResult{err: context.DeadlineExceeded, dur: time.Since(start)}
	}

	// ===== STEP 3: Cancel the call's context, then report the attempt =====
	cancelAttempt()
	c.obs.OnRenew(ctx, RenewEvent{Token: token, Duration: res.dur, Attempt: attempt, Err: res.err})
	return start, end, res.err
}

// defaultRenewalInterval returns TTL/2, floored at one nanosecond so that
// time.NewTicker never receives a non-positive interval.
func defaultRenewalInterval(ttl time.Duration) time.Duration {
	return max(ttl/2, time.Nanosecond)
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
