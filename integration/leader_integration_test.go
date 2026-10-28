package integration_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/leader"
)

func leaderSpecs(h harness) {
	Describe("leader.Elect", func() {
		var (
			ctx    context.Context
			cancel context.CancelFunc
		)

		BeforeEach(func() {
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		})

		AfterEach(func() {
			cancel()
		})

		// ===== PHASE 1: Single Leader =====
		Describe("Phase 1: Single Leader (fail-fast default)", func() {
			It("Node A runs; Node B gets ErrLeaseHeld", func() {
				env := h.newEnv()
				b := env.backend

				leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
				Expect(err).NotTo(HaveOccurred())
				leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-b"})
				Expect(err).NotTo(HaveOccurred())

				aStarted := make(chan struct{})
				aResult := make(chan error, 1)

				// Node A holds the lease while its fn is running.
				go func() {
					aResult <- leader.Elect(ctx, leaseA, "leader-work", leader.Config{}, func(ctx context.Context) error {
						close(aStarted)
						<-ctx.Done()
						return ctx.Err()
					})
				}()

				Eventually(aStarted).Should(BeClosed())

				// Node B attempts fail-fast while A holds the lease.
				err = leader.Elect(ctx, leaseB, "leader-work", leader.Config{IdleInterval: 10 * time.Millisecond}, func(_ context.Context) error { return nil })
				Expect(errors.Is(err, worklease.ErrLeaseHeld)).To(BeTrue())

				// Clean up: cancel lets Node A exit.
				cancel()
				Eventually(aResult, "1s").Should(Receive())
			})
		})

		// ===== PHASE 2: WithWaitForLease Standby Failover =====
		Describe("Phase 2: WithWaitForLease Standby Failover", func() {
			It("Node D acquires after Node C's TTL expires", func() {
				env := h.newEnv()
				b := env.backend

				leaseC, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-c"})
				Expect(err).NotTo(HaveOccurred())
				leaseD, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-d"})
				Expect(err).NotTo(HaveOccurred())

				// Node C holds without renewal — simulates a crashed leader.
				_, err = leaseC.Acquire(ctx, "leader-work")
				Expect(err).NotTo(HaveOccurred())

				// Start Node D in background; it blocks until the lease is free.
				dResult := make(chan error, 1)
				go func() {
					dResult <- leader.Elect(ctx, leaseD, "leader-work", leader.Config{
						AcquireOptions: []worklease.AcquireOption{
							worklease.WithWaitForLease(),
							worklease.WithPollInterval(10 * time.Millisecond),
						},
					}, func(_ context.Context) error { return nil })
				}()

				// Expire Node C's lease — Node D's next poll succeeds.
				env.expire("leader-work")

				Eventually(dResult, "1s").Should(Receive(BeNil()))
			})
		})

		// ===== PHASE 3: Fencing Propagates via Context Cancellation =====
		Describe("Phase 3: Fencing Propagates via Context Cancellation", func() {
			It("Elect returns ErrFenced when another node steals the lease", func() {
				env := h.newEnv()
				b := env.backend
				gate := &renewalGate{Backend: b}

				leaseE, err := worklease.New(gate, worklease.Config{TTL: 30 * time.Second, HolderID: "node-e"})
				Expect(err).NotTo(HaveOccurred())
				leaseF, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-f"})
				Expect(err).NotTo(HaveOccurred())

				eStarted := make(chan struct{})
				eResult := make(chan error, 1)

				// Node E holds the lease; fn blocks on its renewal context.
				go func() {
					eResult <- leader.Elect(ctx, leaseE, "leader-work", leader.Config{
						RenewalOptions: []worklease.RenewalOption{
							worklease.WithRenewalInterval(50 * time.Millisecond),
						},
					}, func(ctx context.Context) error {
						close(eStarted)
						<-ctx.Done()
						return ctx.Err()
					})
				}()

				Eventually(eStarted).Should(BeClosed())

				// Hold Node E's renewals at the gate, expire its lease, and let Node F
				// acquire and bump the fencing token. Without the gate, a renewal could find
				// the lease expired before Node F acquires and report window exhaustion.
				gate.pause()
				env.expire("leader-work")

				tokenF, err := leaseF.Acquire(ctx, "leader-work")
				Expect(err).NotTo(HaveOccurred())
				defer leaseF.Release(ctx, tokenF, worklease.ExitFinished) //nolint:errcheck
				gate.resume()

				// Node E's next renewal (fires within 50ms real time) passes the gate and
				// sees a fencing token mismatch, so Elect returns ErrFenced.
				var electErr error
				Eventually(eResult, "1s").Should(Receive(&electErr))
				Expect(errors.Is(electErr, worklease.ErrFenced)).To(BeTrue())
			})
		})

		// ===== PHASE 4: Cancellation =====
		Describe("Phase 4: Cancellation", func() {
			It("when the parent context is cancelled mid-fn, the lease is released and the next leader acquires immediately and sees ExitAbandoned", func() {
				env := h.newEnv()
				b := env.backend

				leaseG, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-g"})
				Expect(err).NotTo(HaveOccurred())
				parent, cancelParent := context.WithCancel(ctx)
				defer cancelParent()
				err = leader.Elect(parent, leaseG, "leader-work", leader.Config{}, func(c context.Context) error {
					cancelParent()
					<-c.Done()
					return c.Err()
				})
				Expect(err).To(MatchError(context.Canceled))

				// No clock advance: Elect released on its cleanup context.
				leaseH, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-h"})
				Expect(err).NotTo(HaveOccurred())
				tokenH, err := leaseH.Acquire(ctx, "leader-work")
				Expect(err).NotTo(HaveOccurred())
				cp, err := leaseH.ReadCheckpoint(ctx, tokenH)
				Expect(err).NotTo(HaveOccurred())
				Expect(cp.PrevExit).To(Equal(worklease.ExitAbandoned))
				Expect(cp.PrevHolderID).To(Equal("node-g"))
			})
		})
	})
}
