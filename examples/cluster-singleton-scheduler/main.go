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

	err := leader.Elect(ctx, leaseA, "scheduler:primary", leader.Config{}, func(renewCtx context.Context) error {
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

func scenario3FencedLeader(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 3: Fenced Leader Exits via Context Cancellation ===")

	// node-E: TTL=2s, renewal interval=4s — the lease expires before the first renewal fires.
	leaseE, _ := worklease.New(b, worklease.Config{TTL: 2 * time.Second, HolderID: "node-E"})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := leader.Elect(ctx, leaseE, "scheduler:primary", leader.Config{
			RenewalOptions: []worklease.RenewalOption{worklease.WithRenewalInterval(4 * time.Second)},
		}, func(renewCtx context.Context) error {
			if err := runScheduler(renewCtx, "node-E", 2); err != nil {
				return err
			}
			log.Println("  node-E: stalled — awaiting context cancellation...")
			<-renewCtx.Done()
			log.Printf("  node-E: context cancelled (%v)", renewCtx.Err())
			return renewCtx.Err()
		})
		if errors.Is(err, worklease.ErrFenced) {
			log.Println("  node-E: leader.Elect returned ErrFenced — another node took leadership")
		}
	}()

	// Give node-E time to acquire and stall, then wait for its 2s lease to expire.
	time.Sleep(3 * time.Second)

	leaseF, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-F"})
	tokenF, _ := leaseF.Acquire(ctx, "scheduler:primary")
	log.Printf("  node-F: acquired leadership (fencing token %d)", tokenF.FencingToken())

	// Wait for node-E's renewal goroutine to fire at t=4s and detect ErrFenced.
	time.Sleep(2 * time.Second)

	wg.Wait()
	_ = leaseF.Release(ctx, tokenF)
	log.Println("  node-F: released leadership")
	log.Println()
}

func main() {
	ctx := context.Background()

	scenario1HappyPath(ctx, memory.New())
	scenario2StandbyFailover(ctx, memory.New())
	scenario3FencedLeader(ctx, memory.New())
}
