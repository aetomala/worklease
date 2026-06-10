package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/memory"
	"github.com/aetomala/worklease/checkpoint"
	"github.com/aetomala/worklease/worker"
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
	data, err := checkpoint.Encode(checkpoint.JSON(), progress)
	if err != nil {
		return err
	}
	return lease.Checkpoint(ctx, token, data)
}

func scenario1HappyPath(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 1: Happy Path ===")

	lease, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "worker-A"})
	r, _ := worker.NewRunner(worker.RunnerConfig{
		Lease: lease,
		WorkFn: func(renewCtx context.Context, token worklease.Token, _ []byte, _ bool) ([]byte, error) {
			progress := CancellationProgress{}

			if err := runStep(renewCtx, lease, token, "cancel billing", func() { cancelBilling("tenant-alpha") }, &progress.BillingCancelled, &progress); err != nil {
				return nil, err
			}
			if err := runStep(renewCtx, lease, token, "schedule deprovisioning", func() { scheduleDeprovisioning("tenant-alpha") }, &progress.ResourcesScheduled, &progress); err != nil {
				return nil, err
			}
			if err := runStep(renewCtx, lease, token, "archive data", func() { archiveData("tenant-alpha") }, &progress.DataArchived, &progress); err != nil {
				return nil, err
			}
			if err := runStep(renewCtx, lease, token, "send email", func() { sendEmail("tenant-alpha") }, &progress.EmailSent, &progress); err != nil {
				return nil, err
			}
			return nil, nil
		},
	})

	if err := r.Run(ctx, "cancel:tenant-alpha"); err != nil {
		log.Printf("worker-A: run failed: %v", err)
		return
	}
	log.Println("  worker-A: lease released cleanly (cleanHandoff=true)")
	log.Println()
}

func scenario2CrashRecovery(ctx context.Context, b backend.Backend) {
	log.Println("=== Scenario 2: Crash Recovery ===")

	// Worker B: acquires, checkpoints billing, then crashes (no Release).
	{
		leaseB, _ := worklease.New(b, worklease.Config{TTL: 3 * time.Second, HolderID: "worker-B"})
		token, _ := leaseB.Acquire(ctx, "cancel:tenant-beta")

		cancelBilling("tenant-beta")
		progress := CancellationProgress{BillingCancelled: true}
		data, _ := checkpoint.Encode(checkpoint.JSON(), &progress)
		if err := leaseB.Checkpoint(ctx, token, data); err != nil {
			log.Printf("  worker-B: checkpoint failed: %v", err)
			return
		}
		log.Println("  worker-B: crashed after billing — lease expires in 3s")
	}

	log.Println("  [waiting 4s for lease to expire...]")
	time.Sleep(4 * time.Second)

	// Worker C: acquires after expiry, reads checkpoint, resumes from step 2.
	leaseC, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "worker-C"})
	r, _ := worker.NewRunner(worker.RunnerConfig{
		Lease: leaseC,
		WorkFn: func(renewCtx context.Context, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
			progress, err := checkpoint.Decode[CancellationProgress](checkpoint.JSON(), prior)
			if err != nil {
				return nil, err
			}
			log.Printf("  worker-C: acquired lease (fencing token %d)", token.FencingToken())
			if !cleanHandoff {
				log.Println("  worker-C: cleanHandoff=false — previous worker crashed, validating partial state")
			}

			if progress.BillingCancelled {
				log.Println("  worker-C: billing already cancelled by previous worker — skipping")
			} else {
				if err := runStep(renewCtx, leaseC, token, "cancel billing", func() { cancelBilling("tenant-beta") }, &progress.BillingCancelled, &progress); err != nil {
					return nil, err
				}
			}
			if err := runStep(renewCtx, leaseC, token, "schedule deprovisioning", func() { scheduleDeprovisioning("tenant-beta") }, &progress.ResourcesScheduled, &progress); err != nil {
				return nil, err
			}
			if err := runStep(renewCtx, leaseC, token, "archive data", func() { archiveData("tenant-beta") }, &progress.DataArchived, &progress); err != nil {
				return nil, err
			}
			if err := runStep(renewCtx, leaseC, token, "send email", func() { sendEmail("tenant-beta") }, &progress.EmailSent, &progress); err != nil {
				return nil, err
			}
			return nil, nil
		},
	})

	if err := r.Run(ctx, "cancel:tenant-beta"); err != nil {
		log.Printf("  worker-C: run failed: %v", err)
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

	// Worker E: acquires after expiry — higher fencing token.
	leaseE, _ := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "worker-E"})
	rE, _ := worker.NewRunner(worker.RunnerConfig{
		Lease: leaseE,
		WorkFn: func(renewCtx context.Context, token worklease.Token, _ []byte, _ bool) ([]byte, error) {
			log.Printf("  worker-E: acquired lease (fencing token %d)", token.FencingToken())

			// Worker D wakes up and tries to checkpoint — rejected.
			dProgress := CancellationProgress{BillingCancelled: true}
			dData, _ := checkpoint.Encode(checkpoint.JSON(), &dProgress)
			if err := zombieLease.Checkpoint(ctx, zombieToken, dData); errors.Is(err, worklease.ErrFenced) {
				log.Printf("  worker-D: ErrFenced — token %d rejected; worker-E holds token %d — zombie stopped",
					zombieToken.FencingToken(), token.FencingToken())
			}

			// Worker E completes the cancellation normally.
			var progressE CancellationProgress
			if err := runStep(renewCtx, leaseE, token, "cancel billing", func() { cancelBilling("tenant-gamma") }, &progressE.BillingCancelled, &progressE); err != nil {
				return nil, err
			}
			if err := runStep(renewCtx, leaseE, token, "schedule deprovisioning", func() { scheduleDeprovisioning("tenant-gamma") }, &progressE.ResourcesScheduled, &progressE); err != nil {
				return nil, err
			}
			if err := runStep(renewCtx, leaseE, token, "archive data", func() { archiveData("tenant-gamma") }, &progressE.DataArchived, &progressE); err != nil {
				return nil, err
			}
			if err := runStep(renewCtx, leaseE, token, "send email", func() { sendEmail("tenant-gamma") }, &progressE.EmailSent, &progressE); err != nil {
				return nil, err
			}
			return nil, nil
		},
	})

	if err := rE.Run(ctx, "cancel:tenant-gamma"); err != nil {
		log.Printf("  worker-E: run failed: %v", err)
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
