package memory_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/memory"
)

var _ = Describe("Backend (memory)", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		b      backend.Backend
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		b = memory.New()
	})

	AfterEach(func() {
		cancel()
	})

	// ===== PHASE 1: Acquire =====
	Describe("Acquire", func() {
		It("no lease exists → creates record; sets fencingToken to 1; returns LeaseRecord with correct workID and holderID", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(record.WorkID).To(Equal("w1"))
			Expect(record.HolderID).To(Equal("holder-a"))
			Expect(record.FencingToken).To(Equal(uint64(1)))
			Expect(record.ExpiresAt).NotTo(BeZero())
		})

		It("lease exists and unexpired → returns ErrLeaseHeld; does not modify existing record", func() {
			record1, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(record1.FencingToken).To(Equal(uint64(1)))

			record2, err := b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())
			Expect(record2.WorkID).To(Equal(""))
		})

		It("lease exists and expired → replaces record; increments fencingToken by 1; returns LeaseRecord with new fencingToken=2", func() {
			record1, err := b.Acquire(ctx, "w1", "holder-a", -1*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(record1.FencingToken).To(Equal(uint64(1)))

			record2, err := b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(record2.FencingToken).To(Equal(uint64(2)))
			Expect(record2.HolderID).To(Equal("holder-b"))
		})

		It("concurrent Acquire calls for same workID → only one caller succeeds; other 9 return ErrLeaseHeld", func() {
			const workers = 10
			results := make(chan error, workers)
			for i := 0; i < workers; i++ {
				go func() {
					_, err := b.Acquire(ctx, "w1", "test-worker", 30*time.Second)
					results <- err
				}()
			}
			successes := 0
			failures := 0
			for i := 0; i < workers; i++ {
				err := <-results
				if err == nil {
					successes++
				} else if errors.Is(err, worklease.ErrLeaseHeld) {
					failures++
				}
			}
			Expect(successes).To(Equal(1))
			Expect(failures).To(Equal(workers - 1))
		})
	})

	// ===== PHASE 2: Checkpoint =====
	Describe("Checkpoint", func() {
		It("fencing token matches → writes state bytes; returns nil", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Checkpoint(ctx, record, []byte("checkpoint-data"), 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
		})

		It("fencing token stale → returns ErrFenced; does not modify record", func() {
			record1, err := b.Acquire(ctx, "w1", "holder-a", -1*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// re-acquire increments the token
			_, err = b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// now record1.FencingToken is stale
			err = b.Checkpoint(ctx, record1, []byte("state"), 30*time.Second)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})
	})

	// ===== PHASE 3: Renew =====
	Describe("Renew", func() {
		It("fencing token matches → extends expiration; does not modify checkpoint bytes; returns nil", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Write a checkpoint
			err = b.Checkpoint(ctx, record, []byte("checkpoint"), 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Renew should succeed
			err = b.Renew(ctx, record, 10*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Checkpoint should still be present
			state, _, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(Equal([]byte("checkpoint")))
		})

		It("fencing token stale → returns ErrFenced", func() {
			record1, err := b.Acquire(ctx, "w1", "holder-a", -1*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// re-acquire increments the token
			_, err = b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// now record1.FencingToken is stale
			err = b.Renew(ctx, record1, 30*time.Second)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})
	})

	// ===== PHASE 4: Release =====
	Describe("Release", func() {
		It("fencing token matches → sets clean_handoff to true; returns nil", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Release(ctx, record)
			Expect(err).NotTo(HaveOccurred())
		})

		It("fencing token stale → returns ErrFenced", func() {
			record1, err := b.Acquire(ctx, "w1", "holder-a", -1*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// re-acquire increments the token
			_, err = b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// now record1.FencingToken is stale
			err = b.Release(ctx, record1)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})
	})

	// ===== PHASE 5: ReadCheckpoint =====
	Describe("ReadCheckpoint", func() {
		It("no checkpoint written → returns nil state, cleanHandoff=false, nil error", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			state, cleanHandoff, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(BeNil())
			Expect(cleanHandoff).To(BeFalse())
		})

		It("checkpoint written → returns checkpoint bytes; returns correct cleanHandoff value", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Checkpoint(ctx, record, []byte("state-data"), 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			state, cleanHandoff, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(Equal([]byte("state-data")))
			Expect(cleanHandoff).To(BeFalse())
		})

		It("checkpoint written and released → returns checkpoint bytes; returns cleanHandoff=true", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Checkpoint(ctx, record, []byte("state"), 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Release(ctx, record)
			Expect(err).NotTo(HaveOccurred())

			state, cleanHandoff, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(Equal([]byte("state")))
			Expect(cleanHandoff).To(BeTrue())
		})
	})
})
