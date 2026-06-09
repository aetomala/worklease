package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/memory"
)

// CancellationProgress tracks which steps of a subscription cancellation have completed.
type CancellationProgress struct {
	BillingCancelled   bool `json:"billing_cancelled"`
	ResourcesScheduled bool `json:"resources_scheduled"`
	DataArchived       bool `json:"data_archived"`
	EmailSent          bool `json:"email_sent"`
}

func cancelBilling(tenantID string) {
	time.Sleep(100 * time.Millisecond)
	log.Printf("    cancel billing [%s] — ok", tenantID)
}

func scheduleDeprovisioning(tenantID string) {
	time.Sleep(100 * time.Millisecond)
	log.Printf("    schedule deprovisioning [%s] — ok", tenantID)
}

func archiveData(tenantID string) {
	time.Sleep(100 * time.Millisecond)
	log.Printf("    archive data [%s] — ok", tenantID)
}

func sendEmail(tenantID string) {
	time.Sleep(100 * time.Millisecond)
	log.Printf("    send email [%s] — ok", tenantID)
}

// runStep executes effect, records completion in flag, checkpoints progress.
// Effect fires before Checkpoint — if the worker crashes between effect and checkpoint,
// the successor re-executes this step. That is the at-least-once window.
func runStep(
	ctx context.Context,
	lease worklease.Lease,
	token worklease.Token,
	name string,
	effect func(),
	flag *bool,
	progress *CancellationProgress,
) error {
	log.Printf("  %s: %s", token.HolderID(), name)
	effect()
	*flag = true
	data, _ := json.Marshal(progress)
	if err := lease.Checkpoint(ctx, token, data); err != nil {
		return err
	}
	return nil
}

func scenario1HappyPath(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 1: Happy Path ===")

	lease, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "worker-A"})
	token, _ := lease.Acquire(ctx, "cancel:tenant-alpha")

	renewCtx, stopRenewal := lease.StartRenewal(ctx, token)
	defer stopRenewal()

	var progress CancellationProgress

	if err := runStep(renewCtx, lease, token, "cancel billing", func() { cancelBilling("tenant-alpha") }, &progress.BillingCancelled, &progress); err != nil {
		log.Printf("worker-A: step failed: %v", err)
		return
	}
	if err := runStep(renewCtx, lease, token, "schedule deprovisioning", func() { scheduleDeprovisioning("tenant-alpha") }, &progress.ResourcesScheduled, &progress); err != nil {
		log.Printf("worker-A: step failed: %v", err)
		return
	}
	if err := runStep(renewCtx, lease, token, "archive data", func() { archiveData("tenant-alpha") }, &progress.DataArchived, &progress); err != nil {
		log.Printf("worker-A: step failed: %v", err)
		return
	}
	if err := runStep(renewCtx, lease, token, "send email", func() { sendEmail("tenant-alpha") }, &progress.EmailSent, &progress); err != nil {
		log.Printf("worker-A: step failed: %v", err)
		return
	}

	stopRenewal()
	// Release uses the original ctx — not renewCtx — because renewCtx may be cancelled
	// by stopRenewal before Release completes.
	if err := lease.Release(ctx, token); err != nil {
		log.Printf("worker-A: release failed: %v", err)
		return
	}
	log.Println("  worker-A: lease released cleanly (cleanHandoff=true)")
	log.Println()
}

func scenario2CrashRecovery(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 2: Crash Recovery ===")

	// Worker B: acquires, checkpoints billing, then crashes (no Release).
	{
		lease, _ := worklease.New(b, worklease.Config{TTL: 3 * time.Second, HolderID: "worker-B"})
		token, _ := lease.Acquire(ctx, "cancel:tenant-beta")

		cancelBilling("tenant-beta")
		progress := CancellationProgress{BillingCancelled: true}
		data, _ := json.Marshal(progress)
		if err := lease.Checkpoint(ctx, token, data); err != nil {
			log.Printf("  worker-B: checkpoint failed: %v", err)
			return
		}
		log.Println("  worker-B: crashed after billing — lease expires in 3s")
	}

	log.Println("  [waiting 4s for lease to expire...]")
	time.Sleep(4 * time.Second)

	// Worker C: acquires after expiry, reads checkpoint, resumes from step 2.
	lease, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "worker-C"})
	token, _ := lease.Acquire(ctx, "cancel:tenant-beta")
	log.Printf("  worker-C: acquired lease (fencing token %d)", token.FencingToken())

	state, cleanHandoff, _ := lease.ReadCheckpoint(ctx, token)
	var progress CancellationProgress
	if state != nil {
		_ = json.Unmarshal(state, &progress)
		if !cleanHandoff {
			log.Println("  worker-C: cleanHandoff=false — previous worker crashed, validating partial state")
		}
	}

	if progress.BillingCancelled {
		log.Println("  worker-C: billing already cancelled by previous worker — skipping")
	} else {
		if err := runStep(ctx, lease, token, "cancel billing", func() { cancelBilling("tenant-beta") }, &progress.BillingCancelled, &progress); err != nil {
			log.Printf("  worker-C: step failed: %v", err)
			return
		}
	}
	if err := runStep(ctx, lease, token, "schedule deprovisioning", func() { scheduleDeprovisioning("tenant-beta") }, &progress.ResourcesScheduled, &progress); err != nil {
		log.Printf("  worker-C: step failed: %v", err)
		return
	}
	if err := runStep(ctx, lease, token, "archive data", func() { archiveData("tenant-beta") }, &progress.DataArchived, &progress); err != nil {
		log.Printf("  worker-C: step failed: %v", err)
		return
	}
	if err := runStep(ctx, lease, token, "send email", func() { sendEmail("tenant-beta") }, &progress.EmailSent, &progress); err != nil {
		log.Printf("  worker-C: step failed: %v", err)
		return
	}

	if err := lease.Release(ctx, token); err != nil {
		log.Printf("  worker-C: release failed: %v", err)
		return
	}
	log.Println("  worker-C: lease released cleanly (cleanHandoff=true)")
	log.Println()
}

func scenario3ZombieFencing(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 3: Zombie Fencing ===")

	var zombieLease worklease.Lease
	var zombieToken worklease.Token

	// Worker D: acquires with a short TTL, then gets stuck (no renewal).
	{
		lease, _ := worklease.New(b, worklease.Config{TTL: 3 * time.Second, HolderID: "worker-D"})
		token, _ := lease.Acquire(ctx, "cancel:tenant-gamma")
		zombieLease, zombieToken = lease, token
		log.Printf("  worker-D: acquired lease (fencing token %d), now stuck for 4s...", token.FencingToken())
	}

	log.Println("  [waiting 4s for lease to expire...]")
	time.Sleep(4 * time.Second)
	log.Println("  [lease expired]")

	// Worker E: acquires after expiry — receives a higher fencing token.
	leaseE, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "worker-E"})
	tokenE, _ := leaseE.Acquire(ctx, "cancel:tenant-gamma")
	log.Printf("  worker-E: acquired lease (fencing token %d)", tokenE.FencingToken())

	// Worker D wakes up and tries to checkpoint — rejected.
	progress := CancellationProgress{BillingCancelled: true}
	data, _ := json.Marshal(progress)
	err := zombieLease.Checkpoint(ctx, zombieToken, data)
	if errors.Is(err, worklease.ErrFenced) {
		log.Printf("  worker-D: ErrFenced — token %d rejected; worker-E holds token %d — zombie stopped",
			zombieToken.FencingToken(), tokenE.FencingToken())
	}

	// Worker E completes the cancellation normally.
	var progressE CancellationProgress
	if err := runStep(ctx, leaseE, tokenE, "cancel billing", func() { cancelBilling("tenant-gamma") }, &progressE.BillingCancelled, &progressE); err != nil {
		log.Printf("  worker-E: step failed: %v", err)
		return
	}
	if err := runStep(ctx, leaseE, tokenE, "schedule deprovisioning", func() { scheduleDeprovisioning("tenant-gamma") }, &progressE.ResourcesScheduled, &progressE); err != nil {
		log.Printf("  worker-E: step failed: %v", err)
		return
	}
	if err := runStep(ctx, leaseE, tokenE, "archive data", func() { archiveData("tenant-gamma") }, &progressE.DataArchived, &progressE); err != nil {
		log.Printf("  worker-E: step failed: %v", err)
		return
	}
	if err := runStep(ctx, leaseE, tokenE, "send email", func() { sendEmail("tenant-gamma") }, &progressE.EmailSent, &progressE); err != nil {
		log.Printf("  worker-E: step failed: %v", err)
		return
	}
	if err := leaseE.Release(ctx, tokenE); err != nil {
		log.Printf("  worker-E: release failed: %v", err)
		return
	}
	log.Println("  worker-E: cancellation complete")
	log.Println()
}

func main() {
	ctx := context.Background()
	b := memory.New()

	scenario1HappyPath(ctx, b)
	scenario2CrashRecovery(ctx, b)
	scenario3ZombieFencing(ctx, b)
}
