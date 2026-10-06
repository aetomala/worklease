package memory_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/backend/conformance"
	"github.com/aetomala/worklease/backend/memory"
)

var _ = Describe("conformance", conformance.RunSuite(func() backend.Backend {
	return memory.New()
}))

type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }

func (f *fakeClock) Advance(d time.Duration) { f.now = f.now.Add(d) }

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

		It("same work ID reacquired after expiry → strictly greater fencing token from the per-instance sequence", func() {
			rec1, err := b.Acquire(ctx, "w1", "holder-a", -1*time.Second)
			Expect(err).NotTo(HaveOccurred())
			rec2, err := b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(rec2.FencingToken).To(BeNumerically(">", rec1.FencingToken))
		})

		It("per-instance counter → two independent New() instances issue from independent counters", func() {
			b1 := memory.New()
			b2 := memory.New()
			r1, err := b1.Acquire(ctx, "w1", "holder-1", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			r2, err := b2.Acquire(ctx, "w1", "holder-1", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			// Each instance issues a positive token from its own counter; no ordering
			// relationship is required across independent instances.
			Expect(r1.FencingToken).To(BeNumerically(">", 0))
			Expect(r2.FencingToken).To(BeNumerically(">", 0))
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

		It("Checkpoint leaves the previous exit and previous holder unchanged", func() {
			rec1, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Release(ctx, rec1, backend.ExitFinished)).To(Succeed())

			rec2, err := b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Checkpoint(ctx, rec2, []byte("partial"), 30*time.Second)).To(Succeed())

			cp, err := b.ReadCheckpoint(ctx, rec2)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(Equal([]byte("partial")))
			Expect(cp.PrevExit).To(Equal(backend.ExitFinished))
			Expect(cp.PrevHolderID).To(Equal("holder-a"))
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
			cp, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(Equal([]byte("checkpoint")))
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
		It("Release stores the mode on the existing record and does not delete it", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Checkpoint(ctx, record, []byte("kept"), 30*time.Second)).To(Succeed())

			Expect(b.Release(ctx, record, backend.ExitAbandoned)).To(Succeed())

			// Not deleted: the releasing record still reads its own row.
			cp, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(Equal([]byte("kept")))

			// Stored: the successor reads the declared mode.
			rec2, err := b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			cp2, err := b.ReadCheckpoint(ctx, rec2)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp2.PrevExit).To(Equal(backend.ExitAbandoned))
		})

		It("fencing token stale → returns ErrFenced", func() {
			record1, err := b.Acquire(ctx, "w1", "holder-a", -1*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// re-acquire increments the token
			_, err = b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// now record1.FencingToken is stale
			err = b.Release(ctx, record1, backend.ExitFinished)
			Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
		})
	})

	// ===== PHASE 5: ReadCheckpoint =====
	Describe("ReadCheckpoint", func() {
		It("Acquire on a fresh work ID stores ExitNone as the previous exit; no checkpoint → nil State", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			cp, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(BeNil())
			Expect(cp.PrevExit).To(Equal(backend.ExitNone))
			Expect(cp.PrevHolderID).To(BeEmpty())
		})

		It("checkpoint written → returns checkpoint bytes and the lease's previous exit", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			err = b.Checkpoint(ctx, record, []byte("state-data"), 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			cp, err := b.ReadCheckpoint(ctx, record)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(Equal([]byte("state-data")))
			Expect(cp.PrevExit).To(Equal(backend.ExitNone))
		})

		It("Acquire on a released record records the declared exit and the old holder, and clears the current exit", func() {
			record, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Checkpoint(ctx, record, []byte("state"), 30*time.Second)).To(Succeed())
			Expect(b.Release(ctx, record, backend.ExitRetired)).To(Succeed())

			rec2, err := b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())
			cp, err := b.ReadCheckpoint(ctx, rec2)
			Expect(err).NotTo(HaveOccurred())
			Expect(cp.State).To(Equal([]byte("state")))
			Expect(cp.PrevExit).To(Equal(backend.ExitRetired))
			Expect(cp.PrevHolderID).To(Equal("holder-a"))

			// The current exit was cleared at Acquire: the new holder may checkpoint.
			Expect(b.Checkpoint(ctx, rec2, []byte("next"), 30*time.Second)).To(Succeed())
		})
	})

	// ===== PHASE 6: Clock Injection =====
	Describe("Clock injection", func() {
		var fc *fakeClock

		BeforeEach(func() {
			fc = &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			b = memory.New(memory.WithClock(fc))
		})

		Describe("New", func() {
			Context("with no options", func() {
				It("returns a backend that uses the real clock", func() {
					realB := memory.New()
					_, err := realB.Acquire(ctx, "w1", "h1", 10*time.Second)
					Expect(err).NotTo(HaveOccurred())

					// Immediately try to acquire again — must get ErrLeaseHeld
					// because real clock has not advanced 10 seconds
					_, err = realB.Acquire(ctx, "w1", "h2", 10*time.Second)
					Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())
				})
			})

			Context("with WithClock", func() {
				It("returns a backend that uses the injected clock for expiry checks", func() {
					// Acquire with fake clock at T=0; TTL=5s → expires at T=5
					_, err := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					// Advance fake clock to T=6 — lease is now expired
					fc.Advance(6 * time.Second)

					// A new acquire must succeed because the injected clock shows expiry
					_, err = b.Acquire(ctx, "w1", "h2", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		Describe("Backend.Acquire", func() {
			Context("when the lease exists and has not expired per the injected clock", func() {
				It("returns ErrLeaseHeld", func() {
					_, err := b.Acquire(ctx, "w1", "h1", 10*time.Second)
					Expect(err).NotTo(HaveOccurred())

					// Clock not advanced — lease still valid
					_, err = b.Acquire(ctx, "w1", "h2", 10*time.Second)
					Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())
				})
			})

			Context("when the lease exists and has expired per the injected clock", func() {
				It("acquires the lease and increments the fencing token", func() {
					rec, err := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())
					Expect(rec.FencingToken).To(Equal(uint64(1)))

					// Advance past TTL
					fc.Advance(6 * time.Second)

					rec2, err := b.Acquire(ctx, "w1", "h2", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())
					Expect(rec2.FencingToken).To(Equal(uint64(2)))
				})
			})

			Context("when no lease exists", func() {
				It("creates the lease with fencing token 1", func() {
					rec, err := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())
					Expect(rec.FencingToken).To(Equal(uint64(1)))
				})
			})

			Context("when the expired lease had a checkpoint but no Release (crash)", func() {
				It("Acquire on an expired record with no declared exit records ExitExpired and the old holder", func() {
					rec1, err := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					err = b.Checkpoint(ctx, rec1, []byte("crash-state"), 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					fc.Advance(6 * time.Second)

					rec2, err := b.Acquire(ctx, "w1", "h2", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())
					Expect(rec2.FencingToken).To(Equal(uint64(2)))

					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.State).To(Equal([]byte("crash-state")))
					Expect(cp.PrevExit).To(Equal(backend.ExitExpired))
					Expect(cp.PrevHolderID).To(Equal("h1"))
				})
			})

			Context("when the lease had a checkpoint and a clean Release", func() {
				It("re-acquires immediately after Release; successor reads previous checkpoint bytes and ExitFinished", func() {
					rec1, err := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					err = b.Checkpoint(ctx, rec1, []byte("clean-state"), 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					err = b.Release(ctx, rec1, backend.ExitFinished)
					Expect(err).NotTo(HaveOccurred())

					// No clock advance needed: Release sets expiresAt to the past,
					// making the record immediately acquirable.
					rec2, err := b.Acquire(ctx, "w1", "h2", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.State).To(Equal([]byte("clean-state")))
					Expect(cp.PrevExit).To(Equal(backend.ExitFinished))
					Expect(cp.PrevHolderID).To(Equal("h1"))
				})
			})
		})

		Describe("Backend.Renew", func() {
			Context("when the holder's fencing token matches", func() {
				It("extends the expiry using the injected clock's current time", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)

					// Advance clock — new expiry should be based on new clock position
					fc.Advance(2 * time.Second)
					err := b.Renew(ctx, backend.LeaseRecord{
						WorkID: rec.WorkID, HolderID: rec.HolderID, FencingToken: rec.FencingToken, ExpiresAt: rec.ExpiresAt,
					}, 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					// Advance to T=8 (2+5=7 from clock, lease expires at clock T=7)
					// Clock is at T=2 after Renew; we advance 5 more seconds → T=7
					// Renew set expiresAt = clock(T=2) + 5s = T=7
					// Advance one more second → T=8, past expiry
					fc.Advance(6 * time.Second)
					_, err = b.Acquire(ctx, "w1", "h2", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("when the fencing token is stale", func() {
				It("returns ErrFenced", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					staleRecord := backend.LeaseRecord{
						WorkID: rec.WorkID, HolderID: rec.HolderID, FencingToken: 999, ExpiresAt: rec.ExpiresAt,
					}
					err := b.Renew(ctx, staleRecord, 5*time.Second)
					Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				})
			})

			Context("when the injected clock equals the lease expiry exactly", func() {
				It("returns ErrLeaseExpired — matches postgres expires_at > NOW()", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					fc.Advance(5 * time.Second)
					err := b.Renew(ctx, rec, 5*time.Second)
					Expect(errors.Is(err, worklease.ErrLeaseExpired)).To(BeTrue())
				})
			})
		})

		Describe("Backend.Sweep", func() {
			Context("when a released row's age equals Retention exactly", func() {
				It("keeps the row — matches postgres updated_at < NOW() - retention", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					Expect(b.Release(ctx, rec, backend.ExitRetired)).To(Succeed())
					fc.Advance(time.Hour)
					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Hour})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())

					fc.Advance(time.Nanosecond)
					n, err = b.Sweep(ctx, backend.SweepOptions{Retention: time.Hour})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(Equal(int64(1)))
				})
			})

			Context("when rows were released with different exit modes", func() {
				It("deletes a retired record and keeps finished and abandoned records", func() {
					for _, wm := range []struct {
						id   string
						mode backend.ExitMode
					}{{"w-ret", backend.ExitRetired}, {"w-fin", backend.ExitFinished}, {"w-abn", backend.ExitAbandoned}} {
						rec, err := b.Acquire(ctx, wm.id, "h1", 5*time.Second)
						Expect(err).NotTo(HaveOccurred())
						Expect(b.Release(ctx, rec, wm.mode)).To(Succeed())
					}
					fc.Advance(2 * time.Hour)

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Hour, IncludeExpired: true})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(Equal(int64(1)))

					for id, want := range map[string]backend.ExitMode{"w-ret": backend.ExitNone, "w-fin": backend.ExitFinished, "w-abn": backend.ExitAbandoned} {
						rec, err := b.Acquire(ctx, id, "h2", 5*time.Second)
						Expect(err).NotTo(HaveOccurred())
						cp, err := b.ReadCheckpoint(ctx, rec)
						Expect(err).NotTo(HaveOccurred())
						Expect(cp.PrevExit).To(Equal(want), id)
					}
				})
			})
		})

		Describe("Backend.Release", func() {
			Context("at the lease expiry boundary (Rule 26)", func() {
				It("returns ErrLeaseExpired when the clock equals expiresAt", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					fc.Advance(5 * time.Second)
					Expect(b.Release(ctx, rec, backend.ExitFinished)).To(MatchError(worklease.ErrLeaseExpired))
				})

				It("returns ErrLeaseExpired one nanosecond after expiresAt and succeeds one nanosecond before it", func() {
					after, _ := b.Acquire(ctx, "w-after", "h1", 5*time.Second)
					before, _ := b.Acquire(ctx, "w-before", "h1", 5*time.Second)

					fc.Advance(5*time.Second - time.Nanosecond)
					Expect(b.Release(ctx, before, backend.ExitFinished)).To(Succeed())

					fc.Advance(2 * time.Nanosecond)
					Expect(b.Release(ctx, after, backend.ExitFinished)).To(MatchError(worklease.ErrLeaseExpired))
				})
			})

			Context("with an invalid mode and a cancelled context", func() {
				It("validates the mode before checking ctx: returns ErrInvalidExitMode", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					cancelled, cancelNow := context.WithCancel(ctx)
					cancelNow()
					Expect(b.Release(cancelled, rec, backend.ExitNone)).To(MatchError(worklease.ErrInvalidExitMode))
				})
			})
		})

		Describe("Backend.Checkpoint exit guard", func() {
			Context("after the holder released", func() {
				It("returns ErrLeaseExpired, keeps the declared exit, and keeps the lease expired", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					Expect(b.Release(ctx, rec, backend.ExitRetired)).To(Succeed())

					Expect(b.Checkpoint(ctx, rec, []byte("late"), 5*time.Second)).To(MatchError(worklease.ErrLeaseExpired))

					rec2, err := b.Acquire(ctx, "w1", "h2", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitRetired))
					Expect(cp.State).To(BeNil())
				})
			})

			Context("after the lease lapsed with no declared exit and no successor", func() {
				It("revives the lease (no expiry check)", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					fc.Advance(6 * time.Second)

					Expect(b.Checkpoint(ctx, rec, []byte("revived"), 5*time.Second)).To(Succeed())

					_, err := b.Acquire(ctx, "w1", "h2", 5*time.Second)
					Expect(err).To(MatchError(worklease.ErrLeaseHeld))
				})
			})
		})

		// ===== ADR-0014: slice ownership =====
		Describe("memoryBackend slice ownership", func() {
			Context("Checkpoint", func() {
				It("does not reflect mutations to the state slice made after Checkpoint returns", func() {
					rec, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
					Expect(err).NotTo(HaveOccurred())
					state := []byte("abc")
					Expect(b.Checkpoint(ctx, rec, state, 30*time.Second)).To(Succeed())
					state[0] = 'X' // mutate after Checkpoint returns
					got, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(got.State).To(Equal([]byte("abc")))
				})
			})
			Context("ReadCheckpoint", func() {
				It("returns a slice that is independent of the stored state — mutations do not affect storage", func() {
					rec, err := b.Acquire(ctx, "w1", "holder-a", 30*time.Second)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec, []byte("abc"), 30*time.Second)).To(Succeed())
					got, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					got.State[0] = 'X' // mutate the returned slice
					again, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(again.State).To(Equal([]byte("abc")))
				})
			})
		})
	})
})
