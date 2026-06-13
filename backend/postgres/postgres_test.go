package postgres_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	wlpostgres "github.com/aetomala/worklease/backend/postgres"
)

var _ = Describe("Backend (postgres)", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		b      backend.Backend
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		var err error
		b, err = wlpostgres.New(db)
		Expect(err).NotTo(HaveOccurred())
		// Clean table before each spec
		_, err = db.ExecContext(ctx, "DELETE FROM worklease_leases")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Acquire", func() {
		It("no lease exists → inserts record with fencingToken=1; returns LeaseRecord with correct fields", func() {
			record, err := b.Acquire(ctx, "w1", "holder-1", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(record.WorkID).To(Equal("w1"))
			Expect(record.HolderID).To(Equal("holder-1"))
			Expect(record.FencingToken).To(Equal(uint64(1)))
			Expect(record.ExpiresAt).NotTo(BeZero())
		})

		It("lease held and unexpired → returns ErrLeaseHeld", func() {
			// First acquire succeeds
			_, err := b.Acquire(ctx, "w2", "holder-1", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Second acquire with different holder should fail
			_, err = b.Acquire(ctx, "w2", "holder-2", 30*time.Second)
			Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())
		})

		It("lease exists and expired → updates record and increments fencingToken; preserves previous checkpoint bytes", func() {
			// Insert an expired lease with prior checkpoint
			_, err := db.ExecContext(ctx,
				`INSERT INTO worklease_leases (work_id, holder_id, fencing_token, expires_at, checkpoint, clean_handoff)
				 VALUES ($1, $2, 1, NOW() - INTERVAL '1 second', $3, FALSE)`,
				"w3", "old-holder", []byte("prior-state"),
			)
			Expect(err).NotTo(HaveOccurred())

			// Acquire with new holder should succeed
			record, err := b.Acquire(ctx, "w3", "new-holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(record.WorkID).To(Equal("w3"))
			Expect(record.HolderID).To(Equal("new-holder"))
			Expect(record.FencingToken).To(Equal(uint64(2)))

			// Verify previous checkpoint is preserved
			checkpoint, cleanHandoff, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(checkpoint).To(Equal([]byte("prior-state")))
			Expect(cleanHandoff).To(BeFalse())
		})
	})

	Describe("Checkpoint", func() {
		It("fencing token matches → writes state, extends TTL, returns nil", func() {
			record, err := b.Acquire(ctx, "w4", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Checkpoint(ctx, record, []byte("new-state"), 45*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Verify state was written
			checkpoint, cleanHandoff, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(checkpoint).To(Equal([]byte("new-state")))
			Expect(cleanHandoff).To(BeFalse())
		})

		It("fencing token stale → returns ErrFenced", func() {
			record, err := b.Acquire(ctx, "w5", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Simulate a higher token being issued
			_, err = db.ExecContext(ctx, "UPDATE worklease_leases SET fencing_token = fencing_token + 1 WHERE work_id = $1", "w5")
			Expect(err).NotTo(HaveOccurred())

			// Now the original record's token is stale
			err = b.Checkpoint(ctx, record, []byte("state"), 30*time.Second)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})
	})

	Describe("Renew", func() {
		It("fencing token matches → extends TTL, returns nil", func() {
			record, err := b.Acquire(ctx, "w6", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Renew(ctx, record, 60*time.Second)
			Expect(err).NotTo(HaveOccurred())
		})

		It("fencing token stale → returns ErrFenced", func() {
			record, err := b.Acquire(ctx, "w7", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Simulate a higher token being issued
			_, err = db.ExecContext(ctx, "UPDATE worklease_leases SET fencing_token = fencing_token + 1 WHERE work_id = $1", "w7")
			Expect(err).NotTo(HaveOccurred())

			// Now the original record's token is stale
			err = b.Renew(ctx, record, 60*time.Second)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})

		It("lease expired → returns ErrLeaseExpired", func() {
			record, err := b.Acquire(ctx, "w13", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Simulate the lease expiring without a competitor re-acquiring.
			_, err = db.ExecContext(ctx,
				"UPDATE worklease_leases SET expires_at = NOW() - INTERVAL '1 second' WHERE work_id = $1", "w13")
			Expect(err).NotTo(HaveOccurred())

			err = b.Renew(ctx, record, 60*time.Second)
			Expect(errors.Is(err, worklease.ErrLeaseExpired)).To(BeTrue())
		})
	})

	Describe("Release", func() {
		It("fencing token matches → sets clean_handoff=true, expires lease immediately, returns nil", func() {
			record, err := b.Acquire(ctx, "w8", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Release(ctx, record)
			Expect(err).NotTo(HaveOccurred())

			// Verify clean_handoff was set and expires_at is in the past.
			var cleanHandoff bool
			var expiresAt time.Time
			err = db.QueryRowContext(ctx,
				"SELECT clean_handoff, expires_at FROM worklease_leases WHERE work_id = $1", "w8",
			).Scan(&cleanHandoff, &expiresAt)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanHandoff).To(BeTrue())
			Expect(expiresAt).To(BeTemporally("<", time.Now()))
		})

		It("fencing token stale → returns ErrFenced", func() {
			record, err := b.Acquire(ctx, "w9", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Simulate a higher token being issued
			_, err = db.ExecContext(ctx, "UPDATE worklease_leases SET fencing_token = fencing_token + 1 WHERE work_id = $1", "w9")
			Expect(err).NotTo(HaveOccurred())

			// Now the original record's token is stale
			err = b.Release(ctx, record)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})
	})

	Describe("ReadCheckpoint", func() {
		It("no checkpoint exists → returns nil state, false, nil", func() {
			// Acquire a lease without checkpoint
			record, err := b.Acquire(ctx, "w10", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			checkpoint, cleanHandoff, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(checkpoint).To(BeNil())
			Expect(cleanHandoff).To(BeFalse())
		})

		It("fencing token stale → returns ErrFenced", func() {
			record, err := b.Acquire(ctx, "w12", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Simulate a successor acquiring the lease (higher fencing token).
			_, err = db.ExecContext(ctx,
				"UPDATE worklease_leases SET fencing_token = fencing_token + 1 WHERE work_id = $1", "w12")
			Expect(err).NotTo(HaveOccurred())

			_, _, err = b.ReadCheckpoint(ctx, record)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})

		It("checkpoint exists → returns correct bytes and cleanHandoff value", func() {
			// Acquire a lease and checkpoint it
			record, err := b.Acquire(ctx, "w11", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Checkpoint(ctx, record, []byte("saved-state"), 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Release to set clean_handoff=true
			err = b.Release(ctx, record)
			Expect(err).NotTo(HaveOccurred())

			checkpoint, cleanHandoff, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(checkpoint).To(Equal([]byte("saved-state")))
			Expect(cleanHandoff).To(BeTrue())
		})
	})
})
