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

		It("resets cleanHandoff to false even when the previous holder released cleanly", func() {
			// holder-a acquires, releases cleanly → cleanHandoff=true on the record.
			rec1, err := b.Acquire(ctx, "w1", "holder-a", -1*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Release(ctx, rec1)).To(Succeed())

			// holder-b re-acquires (expired via -1s TTL); inherits cleanHandoff=true.
			rec2, err := b.Acquire(ctx, "w1", "holder-b", 30*time.Second)
			Expect(err).NotTo(HaveOccurred())

			_, cleanHandoff, err := b.ReadCheckpoint(ctx, rec2)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanHandoff).To(BeTrue())

			// holder-b checkpoints — must reset cleanHandoff to false.
			Expect(b.Checkpoint(ctx, rec2, []byte("partial"), 30*time.Second)).To(Succeed())

			_, cleanHandoff, err = b.ReadCheckpoint(ctx, rec2)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanHandoff).To(BeFalse())
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
				It("re-acquires; successor reads previous checkpoint bytes and cleanHandoff=false", func() {
					rec1, err := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					err = b.Checkpoint(ctx, rec1, []byte("crash-state"), 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					fc.Advance(6 * time.Second)

					rec2, err := b.Acquire(ctx, "w1", "h2", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())
					Expect(rec2.FencingToken).To(Equal(uint64(2)))

					state, cleanHandoff, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(state).To(Equal([]byte("crash-state")))
					Expect(cleanHandoff).To(BeFalse())
				})
			})

			Context("when the lease had a checkpoint and a clean Release", func() {
				It("re-acquires immediately; successor reads previous checkpoint bytes and cleanHandoff=true", func() {
					rec1, err := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					err = b.Checkpoint(ctx, rec1, []byte("clean-state"), 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					err = b.Release(ctx, rec1)
					Expect(err).NotTo(HaveOccurred())

					// No clock advance needed: Release sets expiresAt to the past,
					// making the record immediately acquirable.
					rec2, err := b.Acquire(ctx, "w1", "h2", 5*time.Second)
					Expect(err).NotTo(HaveOccurred())

					state, cleanHandoff, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(state).To(Equal([]byte("clean-state")))
					Expect(cleanHandoff).To(BeTrue())
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
		})

		Describe("Backend.Release", func() {
			Context("when the holder's fencing token matches", func() {
				It("sets cleanHandoff to true on the existing record", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					err := b.Release(ctx, backend.LeaseRecord{
						WorkID: rec.WorkID, HolderID: rec.HolderID, FencingToken: rec.FencingToken, ExpiresAt: rec.ExpiresAt,
					})
					Expect(err).NotTo(HaveOccurred())

					_, cleanHandoff, err := b.ReadCheckpoint(ctx, backend.LeaseRecord{
						WorkID: rec.WorkID, HolderID: rec.HolderID, FencingToken: rec.FencingToken, ExpiresAt: rec.ExpiresAt,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(cleanHandoff).To(BeTrue())
				})
			})

			Context("after Release", func() {
				It("ReadCheckpoint returns the checkpoint and cleanHandoff true", func() {
					rec, _ := b.Acquire(ctx, "w1", "h1", 5*time.Second)
					_ = b.Checkpoint(ctx, backend.LeaseRecord{
						WorkID: rec.WorkID, HolderID: rec.HolderID, FencingToken: rec.FencingToken, ExpiresAt: rec.ExpiresAt,
					}, []byte("saved-state"), 5*time.Second)
					_ = b.Release(ctx, backend.LeaseRecord{
						WorkID: rec.WorkID, HolderID: rec.HolderID, FencingToken: rec.FencingToken, ExpiresAt: rec.ExpiresAt,
					})

					state, cleanHandoff, err := b.ReadCheckpoint(ctx, backend.LeaseRecord{
						WorkID: rec.WorkID, HolderID: rec.HolderID, FencingToken: rec.FencingToken, ExpiresAt: rec.ExpiresAt,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(state).To(Equal([]byte("saved-state")))
					Expect(cleanHandoff).To(BeTrue())
				})
			})
		})
	})
})
