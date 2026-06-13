package integration_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend/memory"
	"github.com/aetomala/worklease/worker"
)

var _ = Describe("worker.Runner", func() {
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

	// ===== PHASE 1: First Acquisition =====
	Describe("Phase 1: First Acquisition", func() {
		It("WorkFn receives nil prior and cleanHandoff=false on first run", func() {
			b := memory.New()
			lease, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
			Expect(err).NotTo(HaveOccurred())

			var capturedPrior []byte
			var capturedCleanHandoff bool

			r, err := worker.NewRunner(worker.RunnerConfig{
				Lease: lease,
				WorkFn: func(_ context.Context, _ worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
					capturedPrior = prior
					capturedCleanHandoff = cleanHandoff
					return []byte("state-a"), nil
				},
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(r.Run(ctx, "work-1")).To(Succeed())
			Expect(capturedPrior).To(BeNil())
			Expect(capturedCleanHandoff).To(BeFalse())
		})
	})

	// ===== PHASE 2: Clean Handoff =====
	Describe("Phase 2: Clean Handoff", func() {
		It("Worker B receives prior state and cleanHandoff=true after Worker A releases", func() {
			fc := &fakeClock{now: time.Now()}
			b := memory.New(memory.WithClock(fc))
			stateA := []byte("checkpoint-from-a")

			// Worker A: runs, checkpoints stateA, releases cleanly.
			leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
			Expect(err).NotTo(HaveOccurred())
			rA, err := worker.NewRunner(worker.RunnerConfig{
				Lease: leaseA,
				WorkFn: func(_ context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					return stateA, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(rA.Run(ctx, "work-1")).To(Succeed())

			// Advance the fake clock past TTL — the released record appears expired,
			// allowing Worker B to acquire (cleanHandoff carries through).
			fc.Advance(31 * time.Second)

			// Worker B: same backend, different holder.
			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-b"})
			Expect(err).NotTo(HaveOccurred())

			var capturedPrior []byte
			var capturedCleanHandoff bool

			rB, err := worker.NewRunner(worker.RunnerConfig{
				Lease: leaseB,
				WorkFn: func(_ context.Context, _ worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
					capturedPrior = prior
					capturedCleanHandoff = cleanHandoff
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(rB.Run(ctx, "work-1")).To(Succeed())

			Expect(capturedPrior).To(Equal(stateA))
			Expect(capturedCleanHandoff).To(BeTrue())
		})
	})

	// ===== PHASE 3: Crash Recovery =====
	Describe("Phase 3: Crash Recovery", func() {
		It("Worker B receives prior state and cleanHandoff=false after Worker A crashes", func() {
			fc := &fakeClock{now: time.Now()}
			b := memory.New(memory.WithClock(fc))

			stateA := []byte("checkpoint-before-crash")

			// Worker A acquires and checkpoints — no release (crash simulation).
			leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
			Expect(err).NotTo(HaveOccurred())
			tokenA, err := leaseA.Acquire(ctx, "work-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(leaseA.Checkpoint(ctx, tokenA, stateA)).To(Succeed())

			// Advance fake clock past TTL so the lease appears expired.
			fc.Advance(31 * time.Second)

			// Worker B runner: should see stateA and cleanHandoff=false.
			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-b"})
			Expect(err).NotTo(HaveOccurred())

			var capturedPrior []byte
			var capturedCleanHandoff bool

			rB, err := worker.NewRunner(worker.RunnerConfig{
				Lease: leaseB,
				WorkFn: func(_ context.Context, _ worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
					capturedPrior = prior
					capturedCleanHandoff = cleanHandoff
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(rB.Run(ctx, "work-1")).To(Succeed())

			Expect(capturedPrior).To(Equal(stateA))
			Expect(capturedCleanHandoff).To(BeFalse())
		})
	})

	// ===== PHASE 4: Fencing Propagation =====
	Describe("Phase 4: Fencing Propagation", func() {
		It("runner.Run returns ErrFenced when another holder acquires the lease", func() {
			fc := &fakeClock{now: time.Now()}
			b := memory.New(memory.WithClock(fc))

			leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
			Expect(err).NotTo(HaveOccurred())
			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-b"})
			Expect(err).NotTo(HaveOccurred())

			started := make(chan struct{})
			runErr := make(chan error, 1)

			// Worker A blocks inside WorkFn waiting for fencing.
			rA, err := worker.NewRunner(worker.RunnerConfig{
				Lease: leaseA,
				WorkFn: func(ctx context.Context, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
					close(started)
					<-ctx.Done()
					return nil, ctx.Err()
				},
				RenewalOptions: []worklease.RenewalOption{
					worklease.WithRenewalInterval(50 * time.Millisecond),
				},
			})
			Expect(err).NotTo(HaveOccurred())

			go func() { runErr <- rA.Run(ctx, "work-1") }()

			// Wait until Worker A's WorkFn is running (lease acquired and renewal started).
			Eventually(started).Should(BeClosed())

			// Advance fake clock past Worker A's TTL so the record appears expired.
			// Worker B can then acquire, bumping the fencing token.
			fc.Advance(31 * time.Second)

			tokenB, err := leaseB.Acquire(ctx, "work-1")
			Expect(err).NotTo(HaveOccurred())
			defer leaseB.Release(ctx, tokenB) //nolint:errcheck

			// Worker A's next renewal (fires within 50ms real time) sees a fencing
			// token mismatch and cancels the renewal context. WorkFn unblocks, and
			// Release also detects the mismatch, so Run returns ErrFenced.
			Eventually(runErr, "1s").Should(Receive(MatchError(worklease.ErrFenced)))
		})
	})
})
