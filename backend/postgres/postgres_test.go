package postgres_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/conformance"
	wlpostgres "github.com/aetomala/worklease/backend/postgres"
)

var _ = Describe("conformance", conformance.RunSuite(func() backend.Backend {
	_, err := db.Exec("DELETE FROM worklease_leases")
	Expect(err).NotTo(HaveOccurred())
	b, err := wlpostgres.New(db)
	Expect(err).NotTo(HaveOccurred())
	return b
}))

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
		It("no lease exists → inserts record with a positive fencingToken from the global sequence; returns LeaseRecord with correct fields", func() {
			record, err := b.Acquire(ctx, "w1", "holder-1", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(record.WorkID).To(Equal("w1"))
			Expect(record.HolderID).To(Equal("holder-1"))
			// Fencing tokens now come from worklease_fencing_seq, which is not reset
			// between specs — assert a positive token rather than an absolute value.
			Expect(record.FencingToken).To(BeNumerically(">", 0))
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

		It("lease exists and expired → reacquire issues a strictly greater fencing token from the global sequence and preserves previous checkpoint bytes", func() {
			// Acquire a baseline lease and checkpoint it.
			rec1, err := b.Acquire(ctx, "w3", "old-holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Checkpoint(ctx, rec1, []byte("prior-state"), 30*time.Second)).To(Succeed())

			// Expire the lease so it can be reacquired.
			_, err = db.ExecContext(ctx,
				"UPDATE worklease_leases SET expires_at = NOW() - INTERVAL '1 second' WHERE work_id = $1", "w3")
			Expect(err).NotTo(HaveOccurred())

			// Reacquire with a new holder should succeed and issue a strictly greater token.
			rec2, err := b.Acquire(ctx, "w3", "new-holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(rec2.WorkID).To(Equal("w3"))
			Expect(rec2.HolderID).To(Equal("new-holder"))
			Expect(rec2.FencingToken).To(BeNumerically(">", rec1.FencingToken))

			// Verify previous checkpoint is preserved across the reacquire.
			checkpoint, cleanHandoff, err := b.ReadCheckpoint(ctx, rec2)
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
