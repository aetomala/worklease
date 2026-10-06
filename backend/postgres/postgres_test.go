package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/conformance"
	wlpostgres "github.com/aetomala/worklease/backend/postgres"
	"github.com/aetomala/worklease/leader"
	"github.com/aetomala/worklease/worker"
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
			cp, err := b.ReadCheckpoint(ctx, rec2)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(Equal([]byte("prior-state")))
			Expect(cp.PrevExit).To(Equal(backend.ExitExpired))
			Expect(cp.PrevHolderID).To(Equal("old-holder"))
		})
	})

	Describe("Checkpoint", func() {
		It("fencing token matches → writes state, extends TTL, returns nil", func() {
			record, err := b.Acquire(ctx, "w4", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Checkpoint(ctx, record, []byte("new-state"), 45*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Verify state was written
			cp, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(Equal([]byte("new-state")))
			Expect(cp.PrevExit).To(Equal(backend.ExitNone))
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
		It("Release sets exit_mode to the mode text and expires_at below NOW()", func() {
			record, err := b.Acquire(ctx, "w8", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Release(ctx, record, backend.ExitAbandoned)
			Expect(err).NotTo(HaveOccurred())

			// The expiry comparison uses the database clock — comparing against the
			// local clock fails whenever the database runs more than 1ms ahead.
			var exitMode string
			var expired bool
			err = db.QueryRowContext(ctx,
				"SELECT exit_mode, expires_at < NOW() FROM worklease_leases WHERE work_id = $1", "w8",
			).Scan(&exitMode, &expired)
			Expect(err).NotTo(HaveOccurred())
			Expect(exitMode).To(Equal("abandoned"))
			Expect(expired).To(BeTrue())
		})

		It("Checkpoint after Release returns ErrLeaseExpired and leaves exit_mode and expires_at unchanged", func() {
			record, err := b.Acquire(ctx, "w8b", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Release(ctx, record, backend.ExitFinished)).To(Succeed())

			Expect(b.Checkpoint(ctx, record, []byte("late"), 30*time.Second)).To(MatchError(worklease.ErrLeaseExpired))

			var exitMode string
			var expired bool
			var checkpoint []byte
			err = db.QueryRowContext(ctx,
				"SELECT exit_mode, expires_at < NOW(), checkpoint FROM worklease_leases WHERE work_id = $1", "w8b",
			).Scan(&exitMode, &expired, &checkpoint)
			Expect(err).NotTo(HaveOccurred())
			Expect(exitMode).To(Equal("finished"))
			Expect(expired).To(BeTrue())
			Expect(checkpoint).To(BeNil())
		})

		It("fencing token stale → returns ErrFenced", func() {
			record, err := b.Acquire(ctx, "w9", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Simulate a higher token being issued
			_, err = db.ExecContext(ctx, "UPDATE worklease_leases SET fencing_token = fencing_token + 1 WHERE work_id = $1", "w9")
			Expect(err).NotTo(HaveOccurred())

			// Now the original record's token is stale
			err = b.Release(ctx, record, backend.ExitFinished)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})
	})

	Describe("ReadCheckpoint", func() {
		It("no checkpoint exists → returns nil state, false, nil", func() {
			// Acquire a lease without checkpoint
			record, err := b.Acquire(ctx, "w10", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			cp, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(BeNil())
			Expect(cp.PrevExit).To(Equal(backend.ExitNone))
		})

		It("fencing token stale → returns ErrFenced", func() {
			record, err := b.Acquire(ctx, "w12", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Simulate a successor acquiring the lease (higher fencing token).
			_, err = db.ExecContext(ctx,
				"UPDATE worklease_leases SET fencing_token = fencing_token + 1 WHERE work_id = $1", "w12")
			Expect(err).NotTo(HaveOccurred())

			_, err = b.ReadCheckpoint(ctx, record)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})

		It("ReadCheckpoint returns ErrFenced when no row exists", func() {
			_, err := b.ReadCheckpoint(ctx, backend.LeaseRecord{WorkID: "w-missing", HolderID: "holder", FencingToken: 1})
			Expect(err).To(MatchError(worklease.ErrFenced))
		})

		It("ReadCheckpoint returns the checkpoint bytes and PrevExit after release and reacquire", func() {
			record, err := b.Acquire(ctx, "w11", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Checkpoint(ctx, record, []byte("saved-state"), 30*time.Second)).To(Succeed())
			Expect(b.Release(ctx, record, backend.ExitFinished)).To(Succeed())

			rec2, err := b.Acquire(ctx, "w11", "holder-2", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			cp, err := b.ReadCheckpoint(ctx, rec2)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(Equal([]byte("saved-state")))
			Expect(cp.PrevExit).To(Equal(backend.ExitFinished))
			Expect(cp.PrevHolderID).To(Equal("holder"))
		})
	})

	Describe("exit columns", func() {
		It("Acquire on a released row sets prev_exit_mode and prev_holder_id and clears exit_mode", func() {
			record, err := b.Acquire(ctx, "x1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Release(ctx, record, backend.ExitRetired)).To(Succeed())
			_, err = b.Acquire(ctx, "x1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			var exitMode sql.NullString
			var prevExit string
			var prevHolder sql.NullString
			err = db.QueryRowContext(ctx,
				"SELECT exit_mode, prev_exit_mode, prev_holder_id FROM worklease_leases WHERE work_id = $1", "x1",
			).Scan(&exitMode, &prevExit, &prevHolder)
			Expect(err).NotTo(HaveOccurred())
			Expect(exitMode.Valid).To(BeFalse())
			Expect(prevExit).To(Equal("retired"))
			Expect(prevHolder.String).To(Equal("holder-a"))
		})

		It("Acquire never writes clean_handoff: the column keeps its value across Acquire, Checkpoint, and Release", func() {
			record, err := b.Acquire(ctx, "x2", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.ExecContext(ctx, "UPDATE worklease_leases SET clean_handoff = TRUE WHERE work_id = $1", "x2")
			Expect(err).NotTo(HaveOccurred())

			Expect(b.Checkpoint(ctx, record, []byte("s"), 30*time.Second)).To(Succeed())
			Expect(b.Release(ctx, record, backend.ExitAbandoned)).To(Succeed())
			_, err = b.Acquire(ctx, "x2", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			var cleanHandoff bool
			err = db.QueryRowContext(ctx, "SELECT clean_handoff FROM worklease_leases WHERE work_id = $1", "x2").Scan(&cleanHandoff)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanHandoff).To(BeTrue())
		})

		It("schema rejects exit_mode = 'bogus' with a check violation", func() {
			_, err := b.Acquire(ctx, "x3", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.ExecContext(ctx, "UPDATE worklease_leases SET exit_mode = 'bogus' WHERE work_id = $1", "x3")
			Expect(err).To(MatchError(ContainSubstring("worklease_leases_exit_mode_check")))
		})

		It("schema rejects prev_exit_mode = 'bogus' with a check violation", func() {
			_, err := b.Acquire(ctx, "x4", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.ExecContext(ctx, "UPDATE worklease_leases SET prev_exit_mode = 'bogus' WHERE work_id = $1", "x4")
			Expect(err).To(MatchError(ContainSubstring("worklease_leases_prev_exit_mode_check")))
		})
	})

	Describe("worker.Runner on PostgreSQL (#80; Group B)", func() {
		// runCancelledMidWork runs holder-a's Runner on "w-run", cancels the parent
		// context while WorkFn is running, and returns Run's error. WorkFn
		// returns its final state together with the context error.
		runCancelledMidWork := func() error {
			leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "holder-a"})
			Expect(err).NotTo(HaveOccurred())
			parent, cancelParent := context.WithCancel(ctx)
			defer cancelParent()
			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: leaseA,
				WorkFn: func(wctx context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
					cancelParent()
					<-wctx.Done()
					return []byte("final-a"), wctx.Err()
				},
			})
			Expect(err).NotTo(HaveOccurred())
			return r.Run(parent, "w-run")
		}

		It("when the parent context is cancelled mid-WorkFn, Run returns the WorkFn error, the final state is stored, and a successor acquires without waiting for the TTL", func() {
			Expect(runCancelledMidWork()).To(MatchError(context.Canceled))

			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "holder-b"})
			Expect(err).NotTo(HaveOccurred())
			tokenB, err := leaseB.Acquire(ctx, "w-run") // fail-fast: ErrLeaseHeld if A did not release
			Expect(err).NotTo(HaveOccurred())
			cp, err := leaseB.ReadCheckpoint(ctx, tokenB)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(Equal([]byte("final-a")))
		})

		It("the successor reads PrevExit == ExitAbandoned, the cancelled holder's ID, and its final state", func() {
			Expect(runCancelledMidWork()).To(HaveOccurred())

			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "holder-b"})
			Expect(err).NotTo(HaveOccurred())
			tokenB, err := leaseB.Acquire(ctx, "w-run")
			Expect(err).NotTo(HaveOccurred())
			cp, err := leaseB.ReadCheckpoint(ctx, tokenB)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp).To(Equal(worklease.Checkpoint{State: []byte("final-a"), PrevExit: worklease.ExitAbandoned, PrevHolderID: "holder-a"}))
		})
	})

	Describe("leader.Elect on PostgreSQL (#80; Group B)", func() {
		It("when the parent context is cancelled mid-fn, the lease is released and a successor acquires without waiting for the TTL and reads ExitAbandoned", func() {
			leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "leader-a"})
			Expect(err).NotTo(HaveOccurred())
			parent, cancelParent := context.WithCancel(ctx)
			defer cancelParent()
			err = leader.Elect(parent, leaseA, "w-elect", leader.Config{}, func(c context.Context) error {
				cancelParent()
				<-c.Done()
				return c.Err()
			})
			Expect(err).To(MatchError(context.Canceled))

			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "leader-b"})
			Expect(err).NotTo(HaveOccurred())
			tokenB, err := leaseB.Acquire(ctx, "w-elect") // fail-fast: ErrLeaseHeld if A did not release
			Expect(err).NotTo(HaveOccurred())
			cp, err := leaseB.ReadCheckpoint(ctx, tokenB)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.PrevExit).To(Equal(worklease.ExitAbandoned))
			Expect(cp.PrevHolderID).To(Equal("leader-a"))
		})
	})
})
