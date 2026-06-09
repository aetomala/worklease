package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/memory"
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
// renewCtx is passed to migrateTenant so fencing propagates into the migration call.
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
		data, _ := json.Marshal(progress)
		if err := lease.Checkpoint(context.Background(), token, data); err != nil {
			return fmt.Errorf("checkpoint after %s: %w", tenantID, err)
		}

		log.Printf("  %s: migrated %s (%d/%d)", token.HolderID(), tenantID, len(progress.MigratedTenants), len(tenants))
	}
	return nil
}

func scenario1HappyPath(ctx context.Context, b backend.Backend, tenants []string) {
	log.Println("=== Scenario 1: Happy Path ===")

	lease, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "coordinator-A"})
	token, _ := lease.Acquire(ctx, "migration:schema-v2")

	renewCtx, stopRenewal := lease.StartRenewal(ctx, token)
	defer stopRenewal()

	if err := migrateTenants(renewCtx, lease, token, tenants, &MigrationProgress{}); err != nil {
		log.Printf("  coordinator-A: migration failed: %v", err)
		return
	}

	stopRenewal()
	if err := lease.Release(ctx, token); err != nil {
		log.Printf("  coordinator-A: release failed: %v", err)
		return
	}
	log.Printf("  coordinator-A: all %d tenants migrated, lease released cleanly", len(tenants))
	log.Println()
}

func scenario2CrashRecovery(ctx context.Context, b backend.Backend, tenants []string) {
	log.Println("=== Scenario 2: Crash Recovery ===")

	// Worker B: acquires, migrates first 3 tenants, then crashes (no Release).
	{
		lease, _ := worklease.New(b, worklease.Config{TTL: 3 * time.Second, HolderID: "coordinator-B"})
		token, _ := lease.Acquire(ctx, "migration:schema-v2")

		var progress MigrationProgress
		for _, tenantID := range tenants[:3] {
			if err := migrateTenant(ctx, tenantID); err != nil {
				log.Printf("  coordinator-B: migration failed: %v", err)
				return
			}
			progress.MigratedTenants = append(progress.MigratedTenants, tenantID)
			data, _ := json.Marshal(progress)
			if err := lease.Checkpoint(ctx, token, data); err != nil {
				log.Printf("  coordinator-B: checkpoint failed: %v", err)
				return
			}
			log.Printf("  coordinator-B: migrated %s (%d/%d)", tenantID, len(progress.MigratedTenants), len(tenants))
		}
		log.Printf("  coordinator-B: crashed after %d tenants — lease expires in 3s", len(progress.MigratedTenants))
	}

	log.Println("  [waiting 4s for lease to expire...]")
	time.Sleep(4 * time.Second)

	// Worker C: acquires after expiry, reads checkpoint, resumes from tenant 4.
	lease, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "coordinator-C"})
	token, _ := lease.Acquire(ctx, "migration:schema-v2")
	log.Printf("  coordinator-C: acquired lease (fencing token %d)", token.FencingToken())

	state, cleanHandoff, _ := lease.ReadCheckpoint(ctx, token)
	var progress MigrationProgress
	if state != nil {
		_ = json.Unmarshal(state, &progress)
		if !cleanHandoff {
			log.Printf("  coordinator-C: cleanHandoff=false — %d tenants already migrated, resuming",
				len(progress.MigratedTenants))
		}
	}

	renewCtx, stopRenewal := lease.StartRenewal(ctx, token)
	defer stopRenewal()

	if err := migrateTenants(renewCtx, lease, token, tenants, &progress); err != nil {
		log.Printf("  coordinator-C: migration failed: %v", err)
		return
	}

	stopRenewal()
	if err := lease.Release(ctx, token); err != nil {
		log.Printf("  coordinator-C: release failed: %v", err)
		return
	}
	log.Printf("  coordinator-C: all %d tenants migrated, lease released cleanly", len(tenants))
	log.Println()
}

func scenario3ZombieFencing(ctx context.Context, b backend.Backend, tenants []string) {
	log.Println("=== Scenario 3: Zombie Fencing ===")

	var zombieLease worklease.Lease
	var zombieToken worklease.Token
	var zombieProgress MigrationProgress

	// Worker D: acquires with a short TTL, migrates 2 tenants, then gets stuck.
	{
		lease, _ := worklease.New(b, worklease.Config{TTL: 3 * time.Second, HolderID: "coordinator-D"})
		token, _ := lease.Acquire(ctx, "migration:schema-v2")
		zombieLease, zombieToken = lease, token

		for _, tenantID := range tenants[:2] {
			if err := migrateTenant(ctx, tenantID); err != nil {
				log.Printf("  coordinator-D: migration failed: %v", err)
				return
			}
			zombieProgress.MigratedTenants = append(zombieProgress.MigratedTenants, tenantID)
			data, _ := json.Marshal(zombieProgress)
			if err := lease.Checkpoint(ctx, token, data); err != nil {
				log.Printf("  coordinator-D: checkpoint failed: %v", err)
				return
			}
		}
		log.Printf("  coordinator-D: acquired (token %d), migrated 2 tenants, now stuck...", token.FencingToken())
	}

	log.Println("  [waiting 4s for lease to expire...]")
	time.Sleep(4 * time.Second)
	log.Println("  [lease expired]")

	// Worker E: acquires after expiry — higher fencing token.
	leaseE, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "coordinator-E"})
	tokenE, _ := leaseE.Acquire(ctx, "migration:schema-v2")
	log.Printf("  coordinator-E: acquired (token %d)", tokenE.FencingToken())

	// Worker D wakes up and tries to checkpoint tenant 3 — rejected.
	zombieProgress.MigratedTenants = append(zombieProgress.MigratedTenants, tenants[2])
	data, _ := json.Marshal(zombieProgress)
	err := zombieLease.Checkpoint(ctx, zombieToken, data)
	if errors.Is(err, worklease.ErrFenced) {
		log.Printf("  coordinator-D: ErrFenced — token %d rejected; coordinator-E holds token %d — zombie stopped",
			zombieToken.FencingToken(), tokenE.FencingToken())
	}

	// Worker E reads checkpoint (2 tenants done) and completes the batch.
	eState, cleanHandoff, _ := leaseE.ReadCheckpoint(ctx, tokenE)
	var progress MigrationProgress
	if eState != nil {
		_ = json.Unmarshal(eState, &progress)
		if !cleanHandoff {
			log.Printf("  coordinator-E: resuming from checkpoint (%d tenants already migrated)", len(progress.MigratedTenants))
		}
	}

	renewCtx, stopRenewal := leaseE.StartRenewal(ctx, tokenE)
	defer stopRenewal()

	if err := migrateTenants(renewCtx, leaseE, tokenE, tenants, &progress); err != nil {
		log.Printf("  coordinator-E: migration failed: %v", err)
		return
	}

	stopRenewal()
	if err := leaseE.Release(ctx, tokenE); err != nil {
		log.Printf("  coordinator-E: release failed: %v", err)
		return
	}
	log.Printf("  coordinator-E: all %d tenants migrated, lease released cleanly", len(tenants))
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
