package main

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/memory"
	"github.com/aetomala/worklease/checkpoint"
	"github.com/aetomala/worklease/pool"
)

// PartitionProgress is the cursor checkpoint — the last processed event offset.
type PartitionProgress struct {
	LastOffset int `json:"last_offset"`
}

// slotDone is a PermanentError that tells the pool not to reacquire this slot.
type slotDone struct{ reason string }

func (e *slotDone) Error() string   { return e.reason }
func (e *slotDone) Permanent() bool { return true }

func scenario1Distribution(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 1: Pool Distributes Work Across Partitions ===")

	workIDs := []string{"part-0", "part-1", "part-2", "part-3", "part-4", "part-5"}
	lease, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-A"})

	// Each slot processes once and exits permanently so the pool completes cleanly.
	fn := func(ctx context.Context, workID string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
		time.Sleep(200 * time.Millisecond)
		log.Printf("  [pool-A] processing %s", workID)
		return nil, &slotDone{"done"}
	}

	p, _ := pool.New(lease, pool.Config{WorkIDs: workIDs}, fn)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Run(ctx)
	}()

	// All 6 goroutines acquire immediately and are active while their WorkFn runs.
	time.Sleep(50 * time.Millisecond)
	active := p.ActiveSlots()
	sort.Strings(active)
	log.Printf("  pool-A active slots: %v", active)

	wg.Wait()
	log.Println()
}

func scenario2CheckpointResume(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 2: Checkpoint Resume on Clean Handoff ===")

	workIDs := []string{"shard-0", "shard-1", "shard-2"}

	// pool-A: TTL=1s; each slot processes one batch, checkpoints, then exits permanently.
	// The short TTL means the leases expire quickly so pool-B can acquire.
	leaseA, _ := worklease.New(b, worklease.Config{TTL: 1 * time.Second, HolderID: "pool-A"})

	aFn := func(ctx context.Context, workID string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
		time.Sleep(100 * time.Millisecond)
		progress := PartitionProgress{LastOffset: 100}
		data, err := checkpoint.Encode(checkpoint.JSON(), &progress)
		if err != nil {
			return nil, err
		}
		log.Printf("  [pool-A] %s: processed events 0–99 (offset %d)", workID, progress.LastOffset)
		return data, &slotDone{"batch complete"}
	}

	poolA, _ := pool.New(leaseA, pool.Config{WorkIDs: workIDs}, aFn)
	poolA.Run(ctx) // blocks until all 3 slots exit via PermanentError

	// The Runner calls Release before returning the PermanentError, setting
	// cleanHandoff=true. However, the TTL clock started at acquisition time, so pool-B
	// must wait for the 1s TTL to elapse before it can acquire the leases.
	log.Println("  [waiting 1s for pool-A leases to expire...]")
	time.Sleep(1100 * time.Millisecond)

	leaseB, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-B"})

	bFn := func(ctx context.Context, workID string, _ worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
		progress, err := checkpoint.Decode[PartitionProgress](checkpoint.JSON(), prior)
		if err != nil {
			return nil, err
		}
		if cleanHandoff && progress.LastOffset > 0 {
			log.Printf("  [pool-B] %s: resuming from offset %d (clean handoff)", workID, progress.LastOffset)
		}
		time.Sleep(100 * time.Millisecond)
		start := progress.LastOffset
		progress.LastOffset += 100
		data, err := checkpoint.Encode(checkpoint.JSON(), &progress)
		if err != nil {
			return nil, err
		}
		log.Printf("  [pool-B] %s: processed events %d–%d (offset %d)", workID, start, progress.LastOffset-1, progress.LastOffset)
		return data, &slotDone{"batch complete"}
	}

	poolB, _ := pool.New(leaseB, pool.Config{WorkIDs: workIDs, BackoffInterval: 50 * time.Millisecond}, bFn)
	poolB.Run(ctx)

	log.Println()
}

func scenario3PermanentError(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 3: PermanentError Drops a Decommissioned Slot ===")

	workIDs := []string{"queue-0", "queue-1", "queue-2"}
	lease, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-C"})

	var (
		mu      sync.Mutex
		offsets = map[string]int{"queue-0": 0, "queue-1": 0}
	)

	fn := func(ctx context.Context, workID string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
		if workID == "queue-2" {
			log.Printf("  [pool-C] %s: decommissioned — dropping slot permanently", workID)
			return nil, &slotDone{"decommissioned"}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}

		mu.Lock()
		offsets[workID] += 50
		offset := offsets[workID]
		mu.Unlock()

		log.Printf("  [pool-C] %s: processed batch (offset %d)", workID, offset)
		progress := PartitionProgress{LastOffset: offset}
		data, err := checkpoint.Encode(checkpoint.JSON(), &progress)
		if err != nil {
			return nil, err
		}
		return data, nil
	}

	p, _ := pool.New(lease, pool.Config{WorkIDs: workIDs, BackoffInterval: 50 * time.Millisecond}, fn)

	runCtx, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
	defer cancel()

	// At 50ms, queue-2 has already exited via PermanentError while queue-0 and
	// queue-1 are still mid-batch — showing the eviction without a false empty.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		active := p.ActiveSlots()
		sort.Strings(active)
		log.Printf("  pool-C active slots after queue-2 eviction: %v", active)
	}()

	p.Run(runCtx)
	wg.Wait()
	log.Println()
}

func main() {
	ctx := context.Background()

	scenario1Distribution(ctx, memory.New())
	scenario2CheckpointResume(ctx, memory.New())
	scenario3PermanentError(ctx, memory.New())
}
