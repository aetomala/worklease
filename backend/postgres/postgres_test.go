package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/conformance"
	wlpostgres "github.com/aetomala/worklease/backend/postgres"
	"github.com/aetomala/worklease/leader"
	"github.com/aetomala/worklease/pool"
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
		It("no checkpoint exists → returns nil State and PrevExit ExitNone", func() {
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

		It("stores NULL for a nil or empty checkpoint state", func() {
			record, err := b.Acquire(ctx, "w-cp-null", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			for _, empty := range [][]byte{nil, {}} {
				Expect(b.Checkpoint(ctx, record, []byte("payload"), 30*time.Second)).To(Succeed())
				Expect(b.Checkpoint(ctx, record, empty, 30*time.Second)).To(Succeed())
				var isNull bool
				Expect(db.QueryRowContext(ctx,
					"SELECT checkpoint IS NULL FROM worklease_leases WHERE work_id = $1", "w-cp-null").Scan(&isNull)).To(Succeed())
				Expect(isNull).To(BeTrue())
			}
		})

		It("reads an empty stored checkpoint back as nil", func() {
			record, err := b.Acquire(ctx, "w-cp-empty", "holder", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// The row shape written before this change: an empty bytea, not NULL.
			_, err = db.ExecContext(ctx,
				`UPDATE worklease_leases SET checkpoint = '\x'::bytea WHERE work_id = $1`, "w-cp-empty")
			Expect(err).NotTo(HaveOccurred())
			var isNull bool
			Expect(db.QueryRowContext(ctx,
				"SELECT checkpoint IS NULL FROM worklease_leases WHERE work_id = $1", "w-cp-empty").Scan(&isNull)).To(Succeed())
			Expect(isNull).To(BeFalse())

			cp, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(BeNil())
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

		It("schema has no clean_handoff column", func() {
			var n int
			err := db.QueryRowContext(ctx,
				"SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'worklease_leases' AND column_name = 'clean_handoff'",
			).Scan(&n)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(BeZero())
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

	Describe("isolation level (#89)", func() {
		// contend starts 20 Acquire calls on workID together behind a channel and
		// returns how many won and how many got ErrLeaseHeld. Any other error
		// fails the spec.
		contend := func(workID string) (won, held int) {
			const callers = 20
			startCh := make(chan struct{})
			results := make(chan error, callers)
			for i := range callers {
				go func() {
					<-startCh
					_, err := b.Acquire(ctx, workID, fmt.Sprintf("holder-%d", i), 30*time.Second)
					results <- err
				}()
			}
			close(startCh)
			for range callers {
				err := <-results
				Expect(err == nil || errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue(), "unexpected Acquire error: %v", err)
				if err == nil {
					won++
				} else {
					held++
				}
			}
			return won, held
		}

		It("the test database defaults to READ COMMITTED", func() {
			var level string
			Expect(db.QueryRowContext(ctx, "SHOW default_transaction_isolation").Scan(&level)).To(Succeed())
			Expect(level).To(Equal("read committed"))
		})

		It("contended Acquire on a work ID with no row: one of 20 concurrent callers wins and the other 19 get ErrLeaseHeld, in each of 20 rounds", func() {
			for round := range 20 {
				won, held := contend(fmt.Sprintf("iso-new-%d", round))
				Expect(won).To(Equal(1), "round %d", round)
				Expect(held).To(Equal(19), "round %d", round)
			}
		})

		It("contended Acquire on an expired row: one of 20 concurrent callers wins and the other 19 get ErrLeaseHeld, in each of 20 rounds", func() {
			for round := range 20 {
				workID := fmt.Sprintf("iso-expired-%d", round)
				_, err := b.Acquire(ctx, workID, "seed", -time.Second)
				Expect(err).NotTo(HaveOccurred())
				won, held := contend(workID)
				Expect(won).To(Equal(1), "round %d", round)
				Expect(held).To(Equal(19), "round %d", round)
			}
		})
	})

	Describe("worker.Runner on PostgreSQL (#80)", func() {
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

	Describe("pool.Pool on PostgreSQL (#80)", func() {
		It("when Run's context is cancelled mid-WorkFn, each slot's final state is stored and a successor acquires without waiting for the TTL and reads ExitAbandoned", func() {
			ids := []string{"p-0", "p-1"}
			leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-a"})
			Expect(err).NotTo(HaveOccurred())

			started := make(chan string, len(ids))
			p, err := pool.New(leaseA, pool.Config{
				WorkIDs:         ids,
				IdleInterval:    10 * time.Millisecond,
				RerunInterval:   10 * time.Millisecond,
				BackoffInterval: 10 * time.Millisecond,
			}, func(wctx context.Context, workID string, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
				started <- workID
				<-wctx.Done()
				return []byte("final-" + workID), wctx.Err()
			})
			Expect(err).NotTo(HaveOccurred())

			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()
			runDone := make(chan error, 1)
			go func() { runDone <- p.Run(runCtx) }()
			for range ids {
				Eventually(started, "2s").Should(Receive())
			}

			runCancel()
			Eventually(runDone, "2s").Should(Receive(BeNil()))

			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-b"})
			Expect(err).NotTo(HaveOccurred())
			for _, id := range ids {
				tokenB, err := leaseB.Acquire(ctx, id) // fail-fast: ErrLeaseHeld if the slot did not release
				Expect(err).NotTo(HaveOccurred(), "expected %q to be released", id)
				cp, err := leaseB.ReadCheckpoint(ctx, tokenB)
				Expect(err).NotTo(HaveOccurred())
				Expect(cp).To(Equal(worklease.Checkpoint{State: []byte("final-" + id), PrevExit: worklease.ExitAbandoned, PrevHolderID: "pool-a"}))
			}
		})
	})

	Describe("leader.Elect on PostgreSQL (#80)", func() {
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

	// storedSpan returns expires_at - acquired_at for workID, measured in SQL so
	// only the server clock is involved.
	storedSpan := func(workID string) time.Duration {
		var us float64
		Expect(db.QueryRowContext(ctx,
			"SELECT EXTRACT(EPOCH FROM (expires_at - acquired_at)) * 1000000 FROM worklease_leases WHERE work_id = $1",
			workID).Scan(&us)).To(Succeed())
		return time.Duration(us) * time.Microsecond
	}
	stamps := func(workID string) (acquiredAt, updatedAt time.Time) {
		Expect(db.QueryRowContext(ctx,
			"SELECT acquired_at, updated_at FROM worklease_leases WHERE work_id = $1",
			workID).Scan(&acquiredAt, &updatedAt)).To(Succeed())
		return acquiredAt, updatedAt
	}

	It("Acquire stores an expiry at least ttl after the server's acquisition time, rounding 1500ns up", func() {
		_, err := b.Acquire(ctx, "ttl-1", "holder-a", 1500*time.Nanosecond)
		Expect(err).NotTo(HaveOccurred())
		Expect(storedSpan("ttl-1")).To(BeNumerically(">=", 1500*time.Nanosecond))

		time.Sleep(time.Millisecond) // the 1500ns lease has expired; reacquire it
		_, err = b.Acquire(ctx, "ttl-1", "holder-b", 30*time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(storedSpan("ttl-1")).To(BeNumerically(">=", 30*time.Second))
	})

	It("Acquire on reacquisition sets acquired_at to the reacquisition time", func() {
		rec, err := b.Acquire(ctx, "acq-1", "holder-a", 30*time.Second)
		Expect(err).NotTo(HaveOccurred())
		first, _ := stamps("acq-1")

		Expect(b.Renew(ctx, rec, 30*time.Second)).To(Succeed())
		afterRenew, _ := stamps("acq-1")
		Expect(afterRenew.Equal(first)).To(BeTrue(), "Renew changed acquired_at")

		Expect(b.Release(ctx, rec, backend.ExitFinished)).To(Succeed())
		time.Sleep(10 * time.Millisecond)
		_, err = b.Acquire(ctx, "acq-1", "holder-b", 30*time.Second)
		Expect(err).NotTo(HaveOccurred())
		acquiredAt, updatedAt := stamps("acq-1")
		Expect(acquiredAt).To(BeTemporally(">", first))
		Expect(acquiredAt.Equal(updatedAt)).To(BeTrue(), "acquired_at %v, updated_at %v", acquiredAt, updatedAt)
	})

	Context("when a statement is cancelled mid-flight (#113)", func() {
		// holdLock runs query on a second connection inside an open transaction,
		// so a backend statement on the locked row or table blocks inside
		// PostgreSQL until the transaction ends. It is rolled back in DeferCleanup.
		holdLock := func(query string, args ...any) {
			tx, err := db.BeginTx(ctx, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = tx.Rollback() })
			_, err = tx.ExecContext(ctx, query, args...)
			Expect(err).NotTo(HaveOccurred())
		}
		lockRow := func(workID string) {
			holdLock("SELECT 1 FROM worklease_leases WHERE work_id = $1 FOR UPDATE", workID)
		}
		// cancelled returns a context that is cancelled after 300ms, and expired
		// one whose deadline passes after 300ms, both while the statement waits.
		cancelled := func() context.Context {
			c, cancelC := context.WithCancel(ctx)
			time.AfterFunc(300*time.Millisecond, cancelC)
			DeferCleanup(cancelC)
			return c
		}
		expired := func() context.Context {
			c, cancelC := context.WithTimeout(ctx, 300*time.Millisecond)
			DeferCleanup(cancelC)
			return c
		}
		// releasedRow leaves an expired row for workID, so Acquire's upsert takes
		// the ON CONFLICT path and waits on the row lock.
		releasedRow := func(workID string) {
			rec, err := b.Acquire(ctx, workID, "holder-old", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Release(ctx, rec, backend.ExitFinished)).To(Succeed())
		}
		heldLease := func(workID string) backend.LeaseRecord {
			rec, err := b.Acquire(ctx, workID, "holder-1", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			return rec
		}
		// expectDriverCancel checks the driver error the backend wrapped beside the
		// context error. lib/pq sends a cancel request, so the chain holds the
		// server's error with SQLSTATE 57014 (*pq.Error). pgx v5 times out the
		// connection instead and returns its own error, so no SQLSTATE error is in
		// the chain. Both error types implement SQLState.
		expectDriverCancel := func(err error) {
			var stateErr interface{ SQLState() string }
			found := errors.As(err, &stateErr)
			if postgresDriver() == "pgx" {
				Expect(found).To(BeFalse(), "pgx returned a SQLSTATE error: %v", err)
				return
			}
			Expect(found).To(BeTrue(), "no driver error with a SQLSTATE in %v", err)
			Expect(stateErr.SQLState()).To(Equal("57014"))
		}
		newLease := func() worklease.Lease {
			l, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "holder-2"})
			Expect(err).NotTo(HaveOccurred())
			return l
		}
		noFn := func(context.Context) error {
			Fail("fn must not run: Acquire was cancelled")
			return nil
		}
		newRunner := func(l worklease.Lease) *worker.Runner {
			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: l,
				WorkFn: func(context.Context, worklease.Token, worklease.Checkpoint) ([]byte, error) {
					Fail("WorkFn must not run: Acquire was cancelled")
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())
			return r
		}

		It("Acquire: cancellation → errors.Is context.Canceled, and the driver error is still wrapped", func() {
			releasedRow("mid-1")
			lockRow("mid-1")
			_, err := b.Acquire(cancelled(), "mid-1", "holder-2", 30*time.Second)
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
			expectDriverCancel(err)
		})

		It("Acquire: deadline → errors.Is context.DeadlineExceeded, and the driver error is still wrapped", func() {
			releasedRow("mid-2")
			lockRow("mid-2")
			_, err := b.Acquire(expired(), "mid-2", "holder-2", 30*time.Second)
			Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue(), "%v", err)
			expectDriverCancel(err)
		})

		It("Checkpoint: cancellation → errors.Is context.Canceled", func() {
			rec := heldLease("mid-3")
			lockRow("mid-3")
			err := b.Checkpoint(cancelled(), rec, []byte("state"), 30*time.Second)
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
		})

		It("Renew: cancellation → errors.Is context.Canceled", func() {
			rec := heldLease("mid-4")
			lockRow("mid-4")
			err := b.Renew(cancelled(), rec, 30*time.Second)
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
		})

		It("Release: cancellation → errors.Is context.Canceled", func() {
			rec := heldLease("mid-5")
			lockRow("mid-5")
			err := b.Release(cancelled(), rec, backend.ExitFinished)
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
		})

		It("Forget: cancellation → errors.Is context.Canceled", func() {
			rec := heldLease("mid-6")
			lockRow("mid-6")
			err := b.Forget(cancelled(), rec)
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
		})

		It("Sweep: cancellation → errors.Is context.Canceled", func() {
			rec := heldLease("mid-7")
			Expect(b.Release(ctx, rec, backend.ExitRetired)).To(Succeed())
			time.Sleep(20 * time.Millisecond) // older than the 1ms retention below
			lockRow("mid-7")
			_, err := b.Sweep(cancelled(), backend.SweepOptions{Retention: time.Millisecond})
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
		})

		It("ReadCheckpoint: cancellation → errors.Is context.Canceled", func() {
			rec := heldLease("mid-8")
			holdLock("LOCK TABLE worklease_leases IN ACCESS EXCLUSIVE MODE")
			_, err := b.ReadCheckpoint(cancelled(), rec)
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
		})

		It("Lease.Acquire with WithWaitForLease: cancellation → errors.Is context.Canceled", func() {
			releasedRow("mid-9")
			lockRow("mid-9")
			_, err := newLease().Acquire(cancelled(), "mid-9", worklease.WithWaitForLease())
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
		})

		It("Lease.Acquire with WithWaitForLease: deadline → errors.Is context.DeadlineExceeded", func() {
			releasedRow("mid-10")
			lockRow("mid-10")
			_, err := newLease().Acquire(expired(), "mid-10", worklease.WithWaitForLease())
			Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue(), "%v", err)
		})

		It("leader.Elect: cancellation → errors.Is context.Canceled", func() {
			releasedRow("mid-11")
			lockRow("mid-11")
			err := leader.Elect(cancelled(), newLease(), "mid-11", leader.Config{}, noFn)
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
		})

		It("leader.Elect: deadline → errors.Is context.DeadlineExceeded", func() {
			releasedRow("mid-12")
			lockRow("mid-12")
			err := leader.Elect(expired(), newLease(), "mid-12", leader.Config{}, noFn)
			Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue(), "%v", err)
		})

		It("worker.Runner.Run: cancellation → errors.Is context.Canceled", func() {
			releasedRow("mid-13")
			lockRow("mid-13")
			err := newRunner(newLease()).Run(cancelled(), "mid-13")
			Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "%v", err)
		})

		It("worker.Runner.Run: deadline → errors.Is context.DeadlineExceeded", func() {
			releasedRow("mid-14")
			lockRow("mid-14")
			err := newRunner(newLease()).Run(expired(), "mid-14")
			Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue(), "%v", err)
		})
	})
})
