// Command renewal-backoff demonstrates two v0.5 additions to the lease lifecycle:
// WithWaitForLease context cancellation and bounded renewal retry via WithRenewalBackoff.
package main

import (
	"context"
	"errors"
	"log"
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

	_ = leaseA.Release(ctx, tokenA)
	log.Println()
}

func scenario2RenewalWindowExhaustion(ctx context.Context) {
	log.Println("=== Scenario 2: WithRenewalBackoff + ErrLeaseWindowExhausted ===")

	b := memory.New()
	lease, _ := worklease.New(b, worklease.Config{TTL: 200 * time.Millisecond, HolderID: "worker-C"})
	token, _ := lease.Acquire(ctx, "task:work-item")
	log.Printf("  worker-C: lease acquired (expires in 200ms, fencing token %d)", token.FencingToken())

	// Set the renewal interval longer than the TTL so the first renewal fires after
	// the lease has already expired. This is a deliberate misconfiguration that
	// forces the window-exhausted path; in production keep the interval below the
	// TTL (the default is TTL/2). The first Renew returns ErrLeaseExpired, which the
	// goroutine treats as a non-fencing error.
	//
	// WithRenewalBackoff configures the retry policy for non-fencing errors (e.g.,
	// Postgres connection drops in production). initial=50ms, max=200ms, no jitter.
	//
	// Because the lease window has already closed when that error arrives, no
	// backoff retry runs: the window check cancels renewCtx with
	// ErrLeaseWindowExhausted immediately.
	renewCtx, stopRenewal := lease.StartRenewal(ctx, token,
		worklease.WithRenewalInterval(400*time.Millisecond),
		worklease.WithRenewalBackoff(50*time.Millisecond, 200*time.Millisecond, 0),
	)
	defer stopRenewal()

	// Block until the renewal goroutine exits due to window exhaustion.
	<-renewCtx.Done()
	stopRenewal() // idempotent; satisfies the stopRenewal-before-Release contract

	if errors.Is(context.Cause(renewCtx), worklease.ErrLeaseWindowExhausted) {
		log.Println("  worker-C: ErrLeaseWindowExhausted — lease window closed before renewal succeeded")
		log.Println("  worker-C: work abandoned; a successor worker may re-acquire this lease")
	}
	log.Println()
}

func main() {
	ctx := context.Background()
	scenario1WaitDeadline(ctx)
	scenario2RenewalWindowExhaustion(ctx)
}
