package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/memory"
	"github.com/aetomala/worklease/checkpoint"
	"github.com/aetomala/worklease/worker"
)

// MigrationProgress is the cursor checkpoint: an append-only list of migrated tenant IDs.
type MigrationProgress struct {
	MigratedTenants []string `json:"migrated_tenants"`
}

func migrateTenant(ctx context.Context, tenantID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

// migrateTenants iterates tenants, skipping already-migrated ones, and checkpoints after each new migration.
// The renewCtx parameter is passed to migrateTenant so fencing propagates into the migration call.
// Checkpoint uses context.Background() so it completes even if renewCtx is about to cancel.
func migrateTenants(renewCtx context.Context, lease worklease.Lease, token worklease.Token, tenants []string, progress *MigrationProgress) error {
	migratedSet := make(map[string]bool, len(progress.MigratedTenants))
	for _, id := range progress.MigratedTenants {
		migratedSet[id] = true
	}

	for _, tenantID := range tenants {
		if migratedSet[tenantID] {
			log.Printf("  %s: skipping %s — already migrated", token.HolderID(), tenantID)
			continue
		}

		if err := migrateTenant(renewCtx, tenantID); err != nil {
			return fmt.Errorf("migrate %s: %w", tenantID, err)
		}

		progress.MigratedTenants = append(progress.MigratedTenants, tenantID)
		data, err := checkpoint.Encode(checkpoint.JSON(), progress)
		if err != nil {
			return fmt.Errorf("encode checkpoint: %w", err)
		}
		if err := lease.Checkpoint(context.Background(), token, data); err != nil {
			return fmt.Errorf("checkpoint after %s: %w", tenantID, err)
		}

		log.Printf("  %s: migrated %s (%d/%d)", token.HolderID(), tenantID, len(progress.MigratedTenants), len(tenants))
	}
	return nil
}

// cleanupTimeout bounds each Runner's final Checkpoint and Release, which run on a
// context that survives cancellation of the caller's ctx.
const cleanupTimeout = 10 * time.Second

// newMigrationRunner returns a Runner whose work function resumes from the cursor
// and retires the migration once every tenant is migrated. A migration is one-shot
// work: returning worklease.ErrRetire releases with ExitRetired and Run returns nil.
// onStart, if non-nil, runs first inside the work function while the lease is held.
func newMigrationRunner(lease worklease.Lease, tenants []string, onStart func(worklease.Token)) *worker.Runner {
	r, _ := worker.NewRunner(worker.RunnerConfig{
		Lease:          lease,
		CleanupTimeout: cleanupTimeout,
		WorkFn: func(renewCtx context.Context, token worklease.Token, cp worklease.Checkpoint) ([]byte, error) {
			if onStart != nil {
				onStart(token)
			}
			holder := token.HolderID()
			log.Printf("  %s: acquired lease (fencing token %d) — PrevExit=%s", holder, token.FencingToken(), cp.PrevExit)

			var progress MigrationProgress
			switch cp.PrevExit {
			case worklease.ExitRetired:
				log.Printf("  %s: migration already complete — keeping it retired", holder)
				return cp.State, worklease.ErrRetire
			case worklease.ExitExpired, worklease.ExitAbandoned:
				p, err := checkpoint.Decode[MigrationProgress](checkpoint.JSON(), cp.State)
				if err != nil {
					return nil, err
				}
				progress = p
				log.Printf("  %s: previous coordinator %s did not finish — %d tenants already migrated, resuming",
					holder, cp.PrevHolderID, len(progress.MigratedTenants))
			}

			if err := migrateTenants(renewCtx, lease, token, tenants, &progress); err != nil {
				return nil, err // ExitAbandoned: the next coordinator resumes from the cursor
			}
			data, err := checkpoint.Encode(checkpoint.JSON(), &progress)
			if err != nil {
				return nil, err
			}
			return data, worklease.ErrRetire
		},
	})
	return r
}

func scenario1HappyPath(ctx context.Context, b backend.Backend, tenants []string) {
	log.Println("=== Scenario 1: Happy Path ===")

	lease, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "coordinator-A"})
	if err := newMigrationRunner(lease, tenants, nil).Run(ctx, "migration:schema-v2"); err != nil {
		log.Printf("  coordinator-A: migration failed: %v", err)
		return
	}
	log.Printf("  coordinator-A: all %d tenants migrated — released with ExitRetired", len(tenants))

	// A later coordinator acquires the same work ID and sees the retirement.
	leaseA2, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "coordinator-A2"})
	if err := newMigrationRunner(leaseA2, tenants, nil).Run(ctx, "migration:schema-v2"); err != nil {
		log.Printf("  coordinator-A2: run failed: %v", err)
		return
	}
	log.Println()
}

func scenario2CrashRecovery(ctx context.Context, b backend.Backend, tenants []string) {
	log.Println("=== Scenario 2: Crash Recovery ===")

	// Coordinator B: acquires, migrates first 3 tenants, then crashes (no Release).
	{
		leaseB, _ := worklease.New(b, worklease.Config{TTL: 3 * time.Second, HolderID: "coordinator-B"})
		tokenB, _ := leaseB.Acquire(ctx, "migration:schema-v2")

		var progress MigrationProgress
		for _, tenantID := range tenants[:3] {
			if err := migrateTenant(ctx, tenantID); err != nil {
				log.Printf("  coordinator-B: migration failed: %v", err)
				return
			}
			progress.MigratedTenants = append(progress.MigratedTenants, tenantID)
			data, _ := checkpoint.Encode(checkpoint.JSON(), &progress)
			if err := leaseB.Checkpoint(ctx, tokenB, data); err != nil {
				log.Printf("  coordinator-B: checkpoint failed: %v", err)
				return
			}
			log.Printf("  coordinator-B: migrated %s (%d/%d)", tenantID, len(progress.MigratedTenants), len(tenants))
		}
		log.Printf("  coordinator-B: crashed after %d tenants — lease expires in 3s", len(progress.MigratedTenants))
	}

	log.Println("  [waiting 4s for lease to expire...]")
	time.Sleep(4 * time.Second)

	// Coordinator C: acquires after expiry, reads checkpoint, resumes from tenant 4.
	leaseC, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "coordinator-C"})
	if err := newMigrationRunner(leaseC, tenants, nil).Run(ctx, "migration:schema-v2"); err != nil {
		log.Printf("  coordinator-C: migration failed: %v", err)
		return
	}
	log.Printf("  coordinator-C: all %d tenants migrated — released with ExitRetired", len(tenants))
	log.Println()
}

func scenario3ZombieFencing(ctx context.Context, b backend.Backend, tenants []string) {
	log.Println("=== Scenario 3: Zombie Fencing ===")

	var zombieLease worklease.Lease
	var zombieToken worklease.Token
	var zombieProgress MigrationProgress

	// Coordinator D: acquires with a short TTL, migrates 2 tenants, then gets stuck.
	{
		leaseD, _ := worklease.New(b, worklease.Config{TTL: 3 * time.Second, HolderID: "coordinator-D"})
		tokenD, _ := leaseD.Acquire(ctx, "migration:schema-v2")
		zombieLease, zombieToken = leaseD, tokenD

		for _, tenantID := range tenants[:2] {
			if err := migrateTenant(ctx, tenantID); err != nil {
				log.Printf("  coordinator-D: migration failed: %v", err)
				return
			}
			zombieProgress.MigratedTenants = append(zombieProgress.MigratedTenants, tenantID)
			data, _ := checkpoint.Encode(checkpoint.JSON(), &zombieProgress)
			if err := leaseD.Checkpoint(ctx, tokenD, data); err != nil {
				log.Printf("  coordinator-D: checkpoint failed: %v", err)
				return
			}
		}
		log.Printf("  coordinator-D: acquired (token %d), migrated 2 tenants, now stuck...", tokenD.FencingToken())
	}

	log.Println("  [waiting 4s for lease to expire...]")
	time.Sleep(4 * time.Second)
	log.Println("  [lease expired]")

	// Coordinator E: acquires after expiry — higher fencing token.
	// Coordinator D wakes up and tries to checkpoint tenant 3 — rejected, because
	// coordinator-E acquired the expired lease with a higher fencing token.
	leaseE, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "coordinator-E"})
	zombieCheck := func(eToken worklease.Token) {
		zombieProgress.MigratedTenants = append(zombieProgress.MigratedTenants, tenants[2])
		dData, _ := checkpoint.Encode(checkpoint.JSON(), &zombieProgress)
		if err := zombieLease.Checkpoint(ctx, zombieToken, dData); errors.Is(err, worklease.ErrFenced) {
			log.Printf("  coordinator-D: ErrFenced — token %d rejected; coordinator-E holds token %d — zombie stopped",
				zombieToken.FencingToken(), eToken.FencingToken())
		}
	}
	if err := newMigrationRunner(leaseE, tenants, zombieCheck).Run(ctx, "migration:schema-v2"); err != nil {
		log.Printf("  coordinator-E: migration failed: %v", err)
		return
	}
	log.Printf("  coordinator-E: all %d tenants migrated — released with ExitRetired", len(tenants))
	log.Println()
}

func main() {
	tenants := []string{
		"tenant-001", "tenant-002", "tenant-003", "tenant-004",
		"tenant-005", "tenant-006", "tenant-007", "tenant-008",
	}
	ctx := context.Background()

	scenario1HappyPath(ctx, memory.New(), tenants)
	scenario2CrashRecovery(ctx, memory.New(), tenants)
	scenario3ZombieFencing(ctx, memory.New(), tenants)
}
