// Command renewal-backoff demonstrates two v0.5 additions to the lease lifecycle:
// WithWaitForLease context cancellation and bounded renewal retry via WithRenewalBackoff.
package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend/memory"
)

func scenario1WaitDeadline(ctx context.Context) {
	log.Println("=== Scenario 1: WithWaitForLease + Deadline Exceeded ===")

	b := memory.New()

	// Worker A acquires and holds the lease for the duration of this scenario.
	leaseA, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "worker-A"})
	tokenA, _ := leaseA.Acquire(ctx, "task:work-item")
	log.Println("  worker-A: lease acquired — holding for 30s (TTL)")

	// Worker B tries to acquire the same work ID with a 500ms deadline.
	// In v0.5, a cancelled or deadline-exceeded wait returns an error wrapping
	// ctx.Err() — not ErrLeaseHeld.
	waitCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	leaseB, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "worker-B"})
	_, err := leaseB.Acquire(waitCtx, "task:work-item",
		worklease.WithWaitForLease(),
		worklease.WithPollInterval(100*time.Millisecond),
	)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		log.Println("  worker-B: deadline exceeded — stopped waiting for the lease")
		log.Println("  worker-B: errors.Is(err, context.DeadlineExceeded) = true (v0.5 behaviour)")
	case errors.Is(err, worklease.ErrLeaseHeld):
		log.Println("  worker-B: ErrLeaseHeld (pre-v0.5 behaviour — should not reach here)")
	default:
		log.Printf("  worker-B: unexpected error: %v", err)
	}

	_ = leaseA.Release(ctx, tokenA, worklease.ExitFinished)
	log.Println()
}

func scenario2RenewalWindowExhaustion(ctx context.Context) {
	log.Println("=== Scenario 2: WithRenewalBackoff + ErrLeaseWindowExhausted ===")

	clk := &fakeClock{now: time.Now()}
	b := memory.New(memory.WithClock(clk))
	lease, _ := worklease.New(b, worklease.Config{TTL: 200 * time.Millisecond, HolderID: "worker-C"})
	token, _ := lease.Acquire(ctx, "task:work-item")
	log.Printf("  worker-C: lease acquired (expires in 200ms, fencing token %d)", token.FencingToken())

	// A fake clock lets storage find the lease lapsed while the local lease
	// window is still open. Advancing it past the TTL right after StartRenewal
	// makes the first Renew, at the default interval (TTL/2), return
	// ErrLeaseExpired. Renewal then stops at once, without backoff or retry,
	// and the renewal context's cause is
	// errors.Join(ErrLeaseWindowExhausted, ErrLeaseExpired), so errors.Is
	// matches both sentinels. A renewal interval longer than the TTL no longer
	// reaches this path: since v0.7 the renewal context is cancelled when the
	// local window closes, before a late tick could call Renew.
	renewCtx, stopRenewal := lease.StartRenewal(ctx, token,
		worklease.WithRenewalBackoff(50*time.Millisecond, 200*time.Millisecond, 0),
	)
	defer stopRenewal()
	clk.advance(time.Second)

	// Block until the renewal goroutine exits due to window exhaustion.
	<-renewCtx.Done()
	stopRenewal() // idempotent; satisfies the stopRenewal-before-Release contract

	if errors.Is(context.Cause(renewCtx), worklease.ErrLeaseWindowExhausted) {
		log.Println("  worker-C: ErrLeaseWindowExhausted — lease window closed before renewal succeeded")
		if errors.Is(context.Cause(renewCtx), worklease.ErrLeaseExpired) {
			log.Println("  worker-C: cause also matches ErrLeaseExpired — terminal, renewal did not retry")
		}
		log.Println("  worker-C: work abandoned; a successor worker may re-acquire this lease")
	}
	log.Println()
}

// fakeClock is a memory.Clock that moves only when advance is called.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// Now returns the clock's current time.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// advance moves the clock forward by d.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func main() {
	ctx := context.Background()
	scenario1WaitDeadline(ctx)
	scenario2RenewalWindowExhaustion(ctx)
}
