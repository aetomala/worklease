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

				It("issues globally increasing fencing tokens across distinct work IDs on the same backend", func() {
					r1, err := b.Acquire(ctx, "work-a", "holder-1", time.Minute)
					Expect(err).NotTo(HaveOccurred())
					r2, err := b.Acquire(ctx, "work-b", "holder-1", time.Minute)
					Expect(err).NotTo(HaveOccurred())
					Expect(r2.FencingToken).To(BeNumerically(">", r1.FencingToken))
				})

				It("preserves checkpoint bytes from the expired record on reacquisition", func() {
					rec1, err := b.Acquire(ctx, "w1", "h", expiredTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec1, []byte("data"), expiredTTL)).To(Succeed())
					rec2, err := b.Acquire(ctx, "w1", "h2", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp2, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp2.State).To(Equal([]byte("data")))
				})

				It("captures the previous holder's declared exit and holder ID on reacquisition", func() {
					rec1, err := b.Acquire(ctx, "acq-prev-1", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec1, backend.ExitAbandoned)).To(Succeed())

					rec2, err := b.Acquire(ctx, "acq-prev-1", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())

					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitAbandoned))
					Expect(cp.PrevHolderID).To(Equal("holder-a"))
				})
			})

			Context("Checkpoint", func() {
				It("succeeds with a valid LeaseRecord and persists the state", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec, []byte("payload"), normalTTL)).To(Succeed())
					cp, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.State).To(Equal([]byte("payload")))
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

				It("does not change PrevExit or PrevHolderID", func() {
					rec1, err := b.Acquire(ctx, "cp-prev-1", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec1, backend.ExitFinished)).To(Succeed())

					rec2, err := b.Acquire(ctx, "cp-prev-1", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec2, []byte("x"), normalTTL)).To(Succeed())

					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.State).To(Equal([]byte("x")))
					Expect(cp.PrevExit).To(Equal(backend.ExitFinished))
					Expect(cp.PrevHolderID).To(Equal("holder-a"))
				})

				It("returns ErrLeaseExpired after the holder released, and the successor acquires at once and reads the declared mode", func() {
					rec, err := b.Acquire(ctx, "cp-after-release", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec, []byte("final"), normalTTL)).To(Succeed())
					Expect(b.Release(ctx, rec, backend.ExitFinished)).To(Succeed())

					Expect(b.Checkpoint(ctx, rec, []byte("late"), normalTTL)).To(MatchError(worklease.ErrLeaseExpired))

					rec2, err := b.Acquire(ctx, "cp-after-release", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.State).To(Equal([]byte("final")))
					Expect(cp.PrevExit).To(Equal(backend.ExitFinished))
					Expect(cp.PrevHolderID).To(Equal("holder-a"))
				})

				It("revives a lease that lapsed with no declared exit and no successor", func() {
					rec, err := b.Acquire(ctx, "cp-revive", "holder-a", expiredTTL)
					Expect(err).NotTo(HaveOccurred())

					Expect(b.Checkpoint(ctx, rec, []byte("revived"), normalTTL)).To(Succeed())

					_, err = b.Acquire(ctx, "cp-revive", "holder-b", normalTTL)
					Expect(err).To(MatchError(worklease.ErrLeaseHeld))
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
				It("succeeds with a valid LeaseRecord and records the declared exit for the successor", func() {
					rec, err := b.Acquire(ctx, "rel-1", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec, backend.ExitFinished)).To(Succeed())

					rec2, err := b.Acquire(ctx, "rel-1", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())

					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitFinished))
					Expect(cp.PrevHolderID).To(Equal("holder-a"))
				})

				It("makes the work ID immediately acquirable after Release — no TTL wait required", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec, backend.ExitFinished)).To(Succeed())
					_, err = b.Acquire(ctx, "w1", "h2", normalTTL)
					Expect(err).NotTo(HaveOccurred())
				})

				It("returns ErrFenced when FencingToken does not match the stored token", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					err = b.Release(ctx, stale(rec), backend.ExitFinished)
					Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				})

				It("returns ErrLeaseExpired in every declared mode once the lease has expired, even with no successor, and records nothing", func() {
					for _, mode := range []backend.ExitMode{backend.ExitFinished, backend.ExitAbandoned, backend.ExitRetired} {
						workID := "rel-exp-" + mode.String()
						rec, err := b.Acquire(ctx, workID, "holder-a", expiredTTL)
						Expect(err).NotTo(HaveOccurred())

						Expect(b.Release(ctx, rec, mode)).To(MatchError(worklease.ErrLeaseExpired))

						rec2, err := b.Acquire(ctx, workID, "holder-b", normalTTL)
						Expect(err).NotTo(HaveOccurred())
						cp, err := b.ReadCheckpoint(ctx, rec2)
						Expect(err).NotTo(HaveOccurred())
						Expect(cp.PrevExit).To(Equal(backend.ExitExpired))
						Expect(cp.PrevHolderID).To(Equal("holder-a"))
					}
				})

				It("checks fencing before expiry: a superseded expired record returns ErrFenced", func() {
					rec, err := b.Acquire(ctx, "rel-order", "holder-a", expiredTTL)
					Expect(err).NotTo(HaveOccurred())
					_, err = b.Acquire(ctx, "rel-order", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())

					Expect(b.Release(ctx, rec, backend.ExitFinished)).To(MatchError(worklease.ErrFenced))
				})

				It("returns ErrInvalidExitMode for a mode Release does not accept and leaves the lease held", func() {
					rec, err := b.Acquire(ctx, "rel-invalid", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())

					for _, mode := range []backend.ExitMode{backend.ExitNone, backend.ExitExpired, backend.ExitMode(99)} {
						Expect(b.Release(ctx, rec, mode)).To(MatchError(worklease.ErrInvalidExitMode))
					}

					_, err = b.Acquire(ctx, "rel-invalid", "holder-b", normalTTL)
					Expect(err).To(MatchError(worklease.ErrLeaseHeld))
				})

				It("returns ErrLeaseExpired on a second Release, and the successor reads the first declared mode", func() {
					rec, err := b.Acquire(ctx, "rel-twice", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec, backend.ExitRetired)).To(Succeed())

					Expect(b.Release(ctx, rec, backend.ExitAbandoned)).To(MatchError(worklease.ErrLeaseExpired))

					rec2, err := b.Acquire(ctx, "rel-twice", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitRetired))
				})
			})

			Context("ReadCheckpoint", func() {
				It("returns ExitNone, an empty PrevHolderID, and nil State on a first acquisition", func() {
					rec, err := b.Acquire(ctx, "rc-first", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())

					cp, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.State).To(BeNil())
					Expect(cp.PrevExit).To(Equal(backend.ExitNone))
					Expect(cp.PrevHolderID).To(BeEmpty())
				})

				It("returns ExitExpired, the crashed holder's ID, and its last checkpoint after a crash", func() {
					rec1, err := b.Acquire(ctx, "rc-crash", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					// Checkpoint with a non-positive TTL, then no Release: the lease lapses with no declared exit.
					Expect(b.Checkpoint(ctx, rec1, []byte("partial"), expiredTTL)).To(Succeed())

					rec2, err := b.Acquire(ctx, "rc-crash", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.State).To(Equal([]byte("partial")))
					Expect(cp.PrevExit).To(Equal(backend.ExitExpired))
					Expect(cp.PrevHolderID).To(Equal("holder-a"))
				})

				It("returns the same PrevExit and PrevHolderID on every read for the life of the lease", func() {
					rec1, err := b.Acquire(ctx, "rc-stable", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec1, backend.ExitRetired)).To(Succeed())
					rec2, err := b.Acquire(ctx, "rc-stable", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())

					first, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Renew(ctx, rec2, normalTTL)).To(Succeed())
					second, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(second.PrevExit).To(Equal(first.PrevExit))
					Expect(second.PrevHolderID).To(Equal(first.PrevHolderID))
					Expect(first.PrevExit).To(Equal(backend.ExitRetired))
				})

				It("returns ErrFenced for a work ID that was never acquired", func() {
					_, err := b.ReadCheckpoint(ctx, neverAcquired)
					Expect(err).To(MatchError(worklease.ErrFenced))
				})

				It("returns ErrFenced after Forget removed the row", func() {
					rec, err := b.Acquire(ctx, "rc-forget", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Forget(ctx, rec)).To(Succeed())

					_, err = b.ReadCheckpoint(ctx, rec)
					Expect(err).To(MatchError(worklease.ErrFenced))
				})

				It("returns ErrFenced after Sweep removed the row", func() {
					rec, err := b.Acquire(ctx, "rc-sweep", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec, backend.ExitRetired)).To(Succeed())
					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeNumerically(">=", 1))

					_, err = b.ReadCheckpoint(ctx, rec)
					Expect(err).To(MatchError(worklease.ErrFenced))
				})

				It("returns ExitNone after Forget and a fresh Acquire", func() {
					rec, err := b.Acquire(ctx, "rc-forget-new", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Forget(ctx, rec)).To(Succeed())

					rec2, err := b.Acquire(ctx, "rc-forget-new", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitNone))
					Expect(cp.PrevHolderID).To(BeEmpty())
				})

				It("returns nil state when no checkpoint has been written", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.State).To(BeNil())
				})

				It("returns ErrFenced when FencingToken does not match the stored token", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					_, err = b.ReadCheckpoint(ctx, stale(rec))
					Expect(errors.Is(err, worklease.ErrFenced)).To(BeTrue())
				})
			})

			Context("cancelled context", func() {
				It("returns the context error without side effects from every method", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec, []byte("before"), normalTTL)).To(Succeed())

					cancelled, cancel := context.WithCancel(ctx)
					cancel()

					_, err = b.Acquire(cancelled, "w-new", "h", normalTTL)
					Expect(err).To(MatchError(context.Canceled))
					Expect(b.Checkpoint(cancelled, rec, []byte("after"), normalTTL)).To(MatchError(context.Canceled))
					Expect(b.Renew(cancelled, rec, normalTTL)).To(MatchError(context.Canceled))
					Expect(b.Release(cancelled, rec, backend.ExitFinished)).To(MatchError(context.Canceled))
					_, err = b.ReadCheckpoint(cancelled, rec)
					Expect(err).To(MatchError(context.Canceled))
					Expect(b.Forget(cancelled, rec)).To(MatchError(context.Canceled))
					_, err = b.Sweep(cancelled, backend.SweepOptions{Retention: time.Nanosecond, IncludeExpired: true})
					Expect(err).To(MatchError(context.Canceled))

					// Nothing changed: no lease was created for w-new, and w1 is still
					// held with its original checkpoint and no declared exit.
					_, err = b.Acquire(ctx, "w-new", "h2", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.State).To(Equal([]byte("before")))
					Expect(cp.PrevExit).To(Equal(backend.ExitNone))
					_, err = b.Acquire(ctx, rec.WorkID, "other-holder", normalTTL)
					Expect(err).To(MatchError(worklease.ErrLeaseHeld))
				})
			})

			Context("holder mismatch", func() {
				It("returns ErrFenced from Checkpoint, Renew, Release, and Forget when HolderID does not match the stored lease", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					other := rec
					other.HolderID = "h-other"

					Expect(b.Checkpoint(ctx, other, []byte("x"), normalTTL)).To(MatchError(worklease.ErrFenced))
					Expect(b.Renew(ctx, other, normalTTL)).To(MatchError(worklease.ErrFenced))
					Expect(b.Release(ctx, other, backend.ExitFinished)).To(MatchError(worklease.ErrFenced))
					Expect(b.Forget(ctx, other)).To(MatchError(worklease.ErrFenced))

					// The rightful holder's lease is untouched.
					cp, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitNone))
					_, err = b.Acquire(ctx, rec.WorkID, "third-holder", normalTTL)
					Expect(err).To(MatchError(worklease.ErrLeaseHeld))
					Expect(b.Renew(ctx, rec, normalTTL)).To(Succeed())
				})
			})

			Context("slice aliasing invariants", func() {
				It("does not expose stored state via ReadCheckpoint — mutation of returned slice does not affect storage", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, rec, []byte("abc"), normalTTL)).To(Succeed())
					got, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					got.State[0] = 'X'
					again, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(again.State).To(Equal([]byte("abc")))
				})

				It("does not retain caller's slice after Checkpoint — mutation of state after call does not affect stored checkpoint", func() {
					rec, err := b.Acquire(ctx, "w1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					state := []byte("abc")
					Expect(b.Checkpoint(ctx, rec, state, normalTTL)).To(Succeed())
					state[0] = 'X'
					got, err := b.ReadCheckpoint(ctx, rec)
					Expect(err).NotTo(HaveOccurred())
					Expect(got.State).To(Equal([]byte("abc")))
				})
			})

			Context("Forget", func() {
				It("permanently deletes the row so a subsequent Acquire issues a strictly greater fencing token", func() {
					rec, err := b.Acquire(ctx, "forget-1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())

					Expect(b.Forget(ctx, rec)).To(Succeed())

					rec2, err := b.Acquire(ctx, "forget-1", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(rec2.FencingToken).To(BeNumerically(">", rec.FencingToken))
				})

				It("returns ErrFenced on a stale fencing token and does not delete the row", func() {
					rec, err := b.Acquire(ctx, "forget-2", "h", normalTTL)
					Expect(err).NotTo(HaveOccurred())

					Expect(b.Forget(ctx, stale(rec))).To(MatchError(worklease.ErrFenced))

					Expect(b.Checkpoint(ctx, rec, []byte("still here"), normalTTL)).To(Succeed())
				})

				It("returns ErrFenced when no record exists for the work ID", func() {
					Expect(b.Forget(ctx, neverAcquired)).To(MatchError(worklease.ErrFenced))
				})

				It("succeeds after the holder released, and the next Acquire reports ExitNone", func() {
					rec, err := b.Acquire(ctx, "forget-after-release", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec, backend.ExitRetired)).To(Succeed())

					Expect(b.Forget(ctx, rec)).To(Succeed())

					rec2, err := b.Acquire(ctx, "forget-after-release", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitNone))
				})
			})

			Context("Vacuum.Sweep (via Backend.Sweep)", func() {
				It("deletes a retired row once Retention has elapsed and a fresh Acquire sees ExitNone", func() {
					rec, err := b.Acquire(ctx, "sweep-retired", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec, backend.ExitRetired)).To(Succeed())

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeNumerically(">=", 1))

					rec2, err := b.Acquire(ctx, "sweep-retired", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(rec2.FencingToken).To(BeNumerically(">", rec.FencingToken))
					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitNone))
				})

				It("never deletes a finished or abandoned row, even with IncludeExpired", func() {
					recF, err := b.Acquire(ctx, "sweep-finished", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, recF, backend.ExitFinished)).To(Succeed())
					recA, err := b.Acquire(ctx, "sweep-abandoned", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, recA, backend.ExitAbandoned)).To(Succeed())

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond, IncludeExpired: true})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())
				})

				It("deletes an expired row with no declared exit only when IncludeExpired is set", func() {
					_, err := b.Acquire(ctx, "sweep-expired", "holder-a", expiredTTL)
					Expect(err).NotTo(HaveOccurred())

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())

					n, err = b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond, IncludeExpired: true})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeNumerically(">=", 1))
				})

				It("does not delete a retired row still within the retention window, and reacquiring it reports ExitRetired", func() {
					rec, err := b.Acquire(ctx, "sweep-window", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, rec, backend.ExitRetired)).To(Succeed())

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Hour})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())

					rec2, err := b.Acquire(ctx, "sweep-window", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, rec2)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitRetired))
					Expect(cp.PrevHolderID).To(Equal("holder-a"))
				})

				It("does not delete a row that is currently held, regardless of IncludeExpired", func() {
					_, err := b.Acquire(ctx, "sweep-held", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond, IncludeExpired: true})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())
				})
			})

			Context("handoff across three holders", func() {
				It("release then crash: C observes B's expiry, not A's release, and default Sweep keeps the row", func() {
					recA, err := b.Acquire(ctx, "seq-1", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, recA, []byte("a-state"), normalTTL)).To(Succeed())
					Expect(b.Release(ctx, recA, backend.ExitFinished)).To(Succeed())

					// B acquires and crashes before its first checkpoint: its lease is already expired.
					_, err = b.Acquire(ctx, "seq-1", "holder-b", expiredTTL)
					Expect(err).NotTo(HaveOccurred())

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())

					recC, err := b.Acquire(ctx, "seq-1", "holder-c", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, recC)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitExpired))
					Expect(cp.PrevHolderID).To(Equal("holder-b"))
					Expect(cp.State).To(Equal([]byte("a-state")))
				})

				It("#74 reproduction: after a release then a crash, default Sweep keeps the row and IncludeExpired deletes it", func() {
					recA, err := b.Acquire(ctx, "seq-74", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Checkpoint(ctx, recA, []byte("a-state"), normalTTL)).To(Succeed())
					Expect(b.Release(ctx, recA, backend.ExitFinished)).To(Succeed())

					_, err = b.Acquire(ctx, "seq-74", "holder-b", expiredTTL)
					Expect(err).NotTo(HaveOccurred())

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())

					n, err = b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond, IncludeExpired: true})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeNumerically(">=", 1))
				})

				It("crash then release: C observes B's declared exit, and Sweep keeps the abandoned row", func() {
					_, err := b.Acquire(ctx, "seq-2", "holder-a", expiredTTL)
					Expect(err).NotTo(HaveOccurred())

					recB, err := b.Acquire(ctx, "seq-2", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cpB, err := b.ReadCheckpoint(ctx, recB)
					Expect(err).NotTo(HaveOccurred())
					Expect(cpB.PrevExit).To(Equal(backend.ExitExpired))
					Expect(cpB.PrevHolderID).To(Equal("holder-a"))
					Expect(b.Release(ctx, recB, backend.ExitAbandoned)).To(Succeed())

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond, IncludeExpired: true})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())

					recC, err := b.Acquire(ctx, "seq-2", "holder-c", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, recC)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitAbandoned))
					Expect(cp.PrevHolderID).To(Equal("holder-b"))
				})

				It("release then release: C observes B's exit, and A's retirement does not reach C or Sweep", func() {
					recA, err := b.Acquire(ctx, "seq-3", "holder-a", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					Expect(b.Release(ctx, recA, backend.ExitRetired)).To(Succeed())

					recB, err := b.Acquire(ctx, "seq-3", "holder-b", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cpB, err := b.ReadCheckpoint(ctx, recB)
					Expect(err).NotTo(HaveOccurred())
					Expect(cpB.PrevExit).To(Equal(backend.ExitRetired))
					Expect(cpB.PrevHolderID).To(Equal("holder-a"))
					Expect(b.Release(ctx, recB, backend.ExitFinished)).To(Succeed())

					n, err := b.Sweep(ctx, backend.SweepOptions{Retention: time.Nanosecond})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())

					recC, err := b.Acquire(ctx, "seq-3", "holder-c", normalTTL)
					Expect(err).NotTo(HaveOccurred())
					cp, err := b.ReadCheckpoint(ctx, recC)
					Expect(err).NotTo(HaveOccurred())
					Expect(cp.PrevExit).To(Equal(backend.ExitFinished))
					Expect(cp.PrevHolderID).To(Equal("holder-b"))
				})
			})
		})
	}
}
