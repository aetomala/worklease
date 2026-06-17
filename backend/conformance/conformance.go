// Package conformance provides a backend-agnostic Ginkgo specification that any
// worklease backend must satisfy. It enforces memory-vs-Postgres parity
// structurally rather than discovering drift one bug at a time (ADR-0015).
package conformance

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
)

// Lease TTLs used by the suite: a positive normalTTL keeps a lease live for a
// spec, while a non-positive expiredTTL produces an already-expired record
// without sleeping, so expiry is exercised identically across the in-memory and
// PostgreSQL backends.
const (
	normalTTL  = 30 * time.Second
	expiredTTL = -time.Second
)

// RunSuite returns a Ginkgo spec tree asserting that newBackend produces a
// Backend conforming to the worklease backend contract. Use inside a Describe:
//
//	var _ = Describe("memory backend", conformance.RunSuite(func() backend.Backend {
//	    return memory.New()
//	}))
//
// newBackend is called once per spec in a BeforeEach to obtain a clean backend.
// Expiry is exercised via non-positive TTL — no clock injection required. This
// package imports only backend and worklease — never memory or postgres directly.
func RunSuite(newBackend func() backend.Backend) func() {
	return func() {
		var (
			ctx context.Context
			b   backend.Backend
		)

		BeforeEach(func() {
			ctx = context.Background()
			b = newBackend()
		})

		stale := func(rec backend.LeaseRecord) backend.LeaseRecord {
			rec.FencingToken++
			return rec
		}

		neverAcquired := backend.LeaseRecord{WorkID: "never-acquired", HolderID: "h", FencingToken: 1}

		Describe("Backend conformance", func() {
			Context("Acquire", func() {
				It("succeeds on a fresh work ID and returns a LeaseRecord with FencingToken > 0", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(rec.FencingToken).To(BeNumerically(">", uint64(0)))
				})

				It("returns ErrLeaseHeld when the lease is held and unexpired", func() {
					_, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					_, err = b.Acquire(ctx, "w1", "h2", normalTTL)
					Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())
				})

				It("succeeds after expiry via non-positive TTL and returns a strictly greater FencingToken", func() {
					rec1, err := b.Acquire(ctx, "w1", "h", expiredTTL)
					Expect(err).NotTo(HaveOccurred())
					rec2, err := b.Acquire(ctx, "w1", "h2", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(rec2.FencingToken).To(BeNumerically(">", rec1.FencingToken))
				})

				It("preserves checkpoint bytes from the expired record on reacquisition", func() {
					rec1, err := b.Acquire(ctx, "w1", "h", expiredTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec1, []byte("data"), expiredTTL)).To(Succeed())
					rec2, err := b.Acquire(ctx, "w1", "h2", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					state, _, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(state).To(Equal([]byte("data")))
				})

				It("preserves cleanHandoff from the expired record on reacquisition", func() {
					rec1, err := b.Acquire(ctx, "w1", "h", expiredTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec1)).To(Succeed())
					rec2, err := b.Acquire(ctx, "w1", "h2", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					_, cleanHandoff, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cleanHandoff).To(BeTrue())
				})
			})

			Context("Checkpoint", func() {
				It("succeeds with a valid LeaseRecord and persists the state", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec, []byte("payload"), normalTTL)).To(Succeed())
					state, _, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(state).To(Equal([]byte("payload")))
				})

				It("returns ErrFenced when FencingToken does not match the stored token", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					err = b.Checkpoint(ctx, stale(rec), []byte("x"), normalTTL)
					Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				})

				It("returns ErrFenced on a work ID that was never acquired", func() {
					err := b.Checkpoint(ctx, neverAcquired, []byte("x"), normalTTL)
					Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				})

				It("sets cleanHandoff to false after a successful checkpoint", func() {
					rec1, err := b.Acquire(ctx, "w1", "h", expiredTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec1)).To(Succeed()) // cleanHandoff = true
					rec2, err := b.Acquire(ctx, "w1", "h2", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec2, []byte("x"), normalTTL)).To(Succeed())
					_, cleanHandoff, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cleanHandoff).To(BeFalse())
				})
			})

			Context("Renew", func() {
				It("succeeds with a valid LeaseRecord", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Renew(ctx, rec, normalTTL)).To(Succeed())
				})

				It("returns ErrFenced when FencingToken does not match the stored token", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					err = b.Renew(ctx, stale(rec), normalTTL)
					Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				})

				It("returns ErrFenced on a work ID that was never acquired", func() {
					err := b.Renew(ctx, neverAcquired, normalTTL)
					Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				})

				It("returns ErrLeaseExpired when the lease has expired — non-positive TTL at acquire time", func() {
					rec, err := b.Acquire(ctx, "w1", "h", expiredTTL)
					Expect(err).NotTo(HaveOccurred())
					err = b.Renew(ctx, rec, normalTTL)
					Expect(errors.Is(err, worklease.ErrLeaseExpired)).To(BeTrue())
				})
			})

			Context("Release", func() {
				It("succeeds with a valid LeaseRecord and sets cleanHandoff to true", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec)).To(Succeed())
					_, cleanHandoff, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(cleanHandoff).To(BeTrue())
				})

				It("makes the work ID immediately acquirable after Release — no TTL wait required", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec)).To(Succeed())
					_, err = b.Acquire(ctx, "w1", "h2", normalTTL)
					Expect(err).NotTo(HaveOccurred())
				})

				It("returns ErrFenced when FencingToken does not match the stored token", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					err = b.Release(ctx, stale(rec))
					Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				})
			})

			Context("ReadCheckpoint", func() {
				It("returns cleanHandoff false after a crash — no Release was called", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec, []byte("partial"), normalTTL)).To(Succeed())
					_, cleanHandoff, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(cleanHandoff).To(BeFalse())
				})

				It("returns cleanHandoff true after an explicit Release", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec)).To(Succeed())
					_, cleanHandoff, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(cleanHandoff).To(BeTrue())
				})

				It("returns nil state when no checkpoint has been written", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					state, _, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(state).To(BeNil())
				})

				It("returns ErrFenced when FencingToken does not match the stored token", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					_, _, err = b.ReadCheckpoint(ctx, stale(rec))
					Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				})
			})

			Context("slice aliasing invariants", func() {
				It("does not expose stored state via ReadCheckpoint — mutation of returned slice does not affect storage", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec, []byte("abc"), normalTTL)).To(Succeed())
					got, _, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					got[0] = 'X'
					again, _, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(again).To(Equal([]byte("abc")))
				})

				It("does not retain caller's slice after Checkpoint — mutation of state after call does not affect stored checkpoint", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					state := []byte("abc")
					Expect(b.Checkpoint(ctx, rec, state, normalTTL)).To(Succeed())
					state[0] = 'X'
					got, _, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(got).To(Equal([]byte("abc")))
				})
			})
		})
	}
}
