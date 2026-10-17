package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/memory"
	"github.com/aetomala/worklease/leader"
)

// runScheduler runs N aggregation cycles (200ms each).
// Returns ctx.Err() if the context is cancelled before all cycles complete.
func runScheduler(ctx context.Context, node string, cycles int) error {
	for i := 1; i <= cycles; i++ {
		select {
		case <-ctx.Done():
			log.Printf("  %s: interrupted at cycle %d", node, i)
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			log.Printf("  %s: aggregated metrics (cycle %d/%d)", node, i, cycles)
		}
	}
	return nil
}

func scenario1HappyPath(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 1: Happy Path — Single-Node Election ===")

	leaseA, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-A"})
	leaseB, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-B"})

	// node-B starts after node-A's first cycle so A is guaranteed to hold the lease.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(300 * time.Millisecond)
		err := leader.Elect(ctx, leaseB, "scheduler:primary", leader.Config{}, func(renewCtx context.Context) error {
			return runScheduler(renewCtx, "node-B", 3)
		})
		if errors.Is(err, worklease.ErrLeaseHeld) {
			log.Println("  node-B: ErrLeaseHeld — another node is leader, skipping this cycle")
		}
	}()

	// OnRelinquished fires only after Release succeeds with ExitFinished or ExitRetired —
	// never after a failed term, which releases with ExitAbandoned.
	cfgA := leader.Config{
		OnRelinquished: func(_ context.Context, t worklease.Token) {
			log.Printf("  node-A: OnRelinquished — term released with ExitFinished (fencing token %d)", t.FencingToken())
		},
	}
	err := leader.Elect(ctx, leaseA, "scheduler:primary", cfgA, func(renewCtx context.Context) error {
		return runScheduler(renewCtx, "node-A", 3)
	})
	if err != nil {
		log.Printf("  node-A: leader.Elect failed: %v", err)
	} else {
		log.Println("  node-A: leadership term complete — lease released")
	}

	wg.Wait()
	log.Println()
}

func scenario2StandbyFailover(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 2: Standby Failover with WithWaitForLease ===")

	// node-C: acquires manually with a short TTL, runs 2 cycles, then crashes (no Release).
	{
		leaseC, _ := worklease.New(b, worklease.Config{TTL: 3 * time.Second, HolderID: "node-C"})
		_, _ = leaseC.Acquire(ctx, "scheduler:primary")
		log.Println("  node-C: aggregated metrics (cycle 1)")
		time.Sleep(200 * time.Millisecond)
		log.Println("  node-C: aggregated metrics (cycle 2)")
		time.Sleep(200 * time.Millisecond)
		log.Println("  node-C: crashed — lease expires in 3s")
	}

	// node-D: WithWaitForLease polls until node-C's 3s lease expires, then takes over.
	log.Println("  node-D: waiting to become leader (WithWaitForLease)...")
	leaseD, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-D"})
	err := leader.Elect(ctx, leaseD, "scheduler:primary", leader.Config{
		AcquireOptions: []worklease.AcquireOption{
			worklease.WithWaitForLease(),
			worklease.WithPollInterval(500 * time.Millisecond),
		},
	}, func(renewCtx context.Context) error {
		log.Println("  node-D: acquired leadership — running scheduler")
		return runScheduler(renewCtx, "node-D", 3)
	})
	if err != nil {
		log.Printf("  node-D: leader.Elect failed: %v", err)
	} else {
		log.Println("  node-D: leadership term complete — lease released")
	}
	log.Println()
}

// stalledRenew wraps a Backend so that every Renew waits until its context
// ends, simulating a database that stopped answering.
type stalledRenew struct {
	backend.Backend
}

// Renew waits for ctx to end and returns ctx.Err().
func (stalledRenew) Renew(ctx context.Context, _ backend.LeaseRecord, _ time.Duration) error {
	<-ctx.Done()
	return ctx.Err()
}

func scenario3StalledLeader(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 3: Stalled Renewal Stops the Leader When Its Lease Window Closes ===")

	// node-E: every Renew stalls, so no renewal ever succeeds. The first Renew
	// starts at the default interval (TTL/2 = 1s) and never returns; when the
	// 2s lease window closes, the renewal context is cancelled anyway.
	leaseE, _ := worklease.New(stalledRenew{Backend: b}, worklease.Config{TTL: 2 * time.Second, HolderID: "node-E"})
	_ = leader.Elect(ctx, leaseE, "scheduler:primary", leader.Config{
		OnLost: func(_ context.Context, _ worklease.Token) {
			log.Println("  node-E: OnLost — lease window closed, stopping work")
		},
	}, func(renewCtx context.Context) error {
		err := runScheduler(renewCtx, "node-E", 20)
		log.Printf("  node-E: renewal context cancelled (%v)", context.Cause(renewCtx))
		return err
	})
	log.Println("  node-E: leader.Elect returned without releasing — the successor will see ExitExpired")

	// node-F: waits for node-E's lease to expire in storage, then takes over.
	leaseF, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-F"})
	tokenF, _ := leaseF.Acquire(ctx, "scheduler:primary",
		worklease.WithWaitForLease(),
		worklease.WithPollInterval(100*time.Millisecond),
	)
	cp, _ := leaseF.ReadCheckpoint(ctx, tokenF)
	log.Printf("  node-F: acquired leadership (fencing token %d); previous leader's exit: %s", tokenF.FencingToken(), cp.PrevExit)
	_ = leaseF.Release(ctx, tokenF, worklease.ExitFinished)
	log.Println("  node-F: released leadership")
	log.Println()
}

func scenario4PersistentLeaderWithBackoff(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 4: Persistent Leader with BackoffInterval ===")
	log.Println("  Two nodes compete in retry loops. BackoffInterval=300ms throttles")
	log.Println("  reacquisition after Release — which expires the lease immediately.")

	// runNode wraps Elect in a retry loop, executing up to maxTerms leadership terms.
	// BackoffInterval prevents rapid acquire/release/reacquire cycling: without it,
	// Release expiring the lease immediately (ADR-0012) means the losing node could
	// reacquire before the winner has done meaningful work.
	runNode := func(name string, lease worklease.Lease, maxTerms int, done chan<- string) {
		term := 0
		for term < maxTerms {
			err := leader.Elect(ctx, lease, "scheduler:primary", leader.Config{
				BackoffInterval: 300 * time.Millisecond,
			}, func(renewCtx context.Context) error {
				term++
				log.Printf("  %s: leading (term %d/%d)", name, term, maxTerms)
				return runScheduler(renewCtx, name, 1)
			})
			if errors.Is(err, worklease.ErrFenced) || ctx.Err() != nil {
				break
			}
			// BackoffInterval applies only after a successful Acquire. ErrLeaseHeld
			// (another node is leading) returns at once, so wait before polling again.
			if errors.Is(err, worklease.ErrLeaseHeld) {
				time.Sleep(100 * time.Millisecond)
			}
		}
		log.Printf("  %s: exiting after %d term(s)", name, term)
		done <- name
	}

	leaseG, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-G"})
	leaseH, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-H"})

	done := make(chan string, 2)
	go runNode("node-G", leaseG, 2, done)
	go runNode("node-H", leaseH, 2, done)
	<-done
	<-done
	log.Println()
}

func main() {
	ctx := context.Background()

	scenario1HappyPath(ctx, memory.New())
	scenario2StandbyFailover(ctx, memory.New())
	scenario3StalledLeader(ctx, memory.New())
	scenario4PersistentLeaderWithBackoff(ctx, memory.New())
}
