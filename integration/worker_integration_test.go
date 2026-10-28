package integration_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
	"github.com/aetomala/worklease/worker"
)

// failFirstReadCheckpoint fails the first ReadCheckpoint with a non-fencing
// error and delegates every other call to the wrapped backend.
type failFirstReadCheckpoint struct {
	backend.Backend
	failed atomic.Bool
}

func (f *failFirstReadCheckpoint) ReadCheckpoint(ctx context.Context, record backend.LeaseRecord) (backend.Checkpoint, error) {
	if f.failed.CompareAndSwap(false, true) {
		return backend.Checkpoint{}, errors.New("storage unavailable")
	}
	return f.Backend.ReadCheckpoint(ctx, record)
}

// failingRenew returns a transient error from every Renew and delegates every
// other call to the wrapped backend.
type failingRenew struct {
	backend.Backend
}

func (failingRenew) Renew(_ context.Context, _ backend.LeaseRecord, _ time.Duration) error {
	return errors.New("connection reset")
}

// The renewalGate type wraps a backend and holds Renew calls while paused.
// Every other method delegates to the wrapped backend.
type renewalGate struct {
	backend.Backend
	mu       sync.Mutex
	paused   bool
	inFlight int
	resumeCh chan struct{}
}

func (g *renewalGate) pause() {
	g.mu.Lock()
	g.paused = true
	g.resumeCh = make(chan struct{})
	g.mu.Unlock()
	for {
		g.mu.Lock()
		n := g.inFlight
		g.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (g *renewalGate) resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.paused = false
	close(g.resumeCh)
}

func (g *renewalGate) Renew(ctx context.Context, record backend.LeaseRecord, ttl time.Duration) error {
	g.mu.Lock()
	for g.paused {
		ch := g.resumeCh
		g.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		g.mu.Lock()
	}
	g.inFlight++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.inFlight--
		g.mu.Unlock()
	}()
	return g.Backend.Renew(ctx, record, ttl)
}

func workerSpecs(h harness) {
	Describe("worker.Runner", func() {
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

		// successorSees runs holder node-b's Runner on work-1 and returns the
		// Checkpoint its WorkFn received.
		successorSees := func(b backend.Backend) worklease.Checkpoint {
			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-b"})
			Expect(err).NotTo(HaveOccurred())
			var got worklease.Checkpoint
			rB, err := worker.NewRunner(worker.RunnerConfig{
				Lease: leaseB,
				WorkFn: func(_ context.Context, _ worklease.Token, prior worklease.Checkpoint) ([]byte, error) {
					got = prior
					return nil, nil
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(rB.Run(ctx, "work-1")).To(Succeed())
			return got
		}

		// ===== PHASE 1: First Acquisition =====
		Describe("Phase 1: First Acquisition", func() {
			It("first run sees ExitNone and nil state", func() {
				env := h.newEnv()
				b := env.backend
				lease, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
				Expect(err).NotTo(HaveOccurred())

				var got worklease.Checkpoint
				r, err := worker.NewRunner(worker.RunnerConfig{
					Lease: lease,
					WorkFn: func(_ context.Context, _ worklease.Token, prior worklease.Checkpoint) ([]byte, error) {
						got = prior
						return []byte("state-a"), nil
					},
				})
				Expect(err).NotTo(HaveOccurred())

				Expect(r.Run(ctx, "work-1")).To(Succeed())
				Expect(got.State).To(BeNil())
				Expect(got.PrevExit).To(Equal(worklease.ExitNone))
				Expect(got.PrevHolderID).To(BeEmpty())
			})
		})

		// ===== PHASE 2: Handoff After a Declared Exit =====
		Describe("Phase 2: Handoff After a Declared Exit", func() {
			It("after A's WorkFn returns nil, B sees ExitFinished, A's holder ID, and A's state", func() {
				env := h.newEnv()
				b := env.backend
				stateA := []byte("checkpoint-from-a")

				leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
				Expect(err).NotTo(HaveOccurred())
				rA, err := worker.NewRunner(worker.RunnerConfig{
					Lease: leaseA,
					WorkFn: func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
						return stateA, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(rA.Run(ctx, "work-1")).To(Succeed())

				// No clock advance needed: Release sets expiresAt to the past, so
				// Worker B can acquire immediately without waiting for the TTL.
				Expect(successorSees(b)).To(Equal(worklease.Checkpoint{State: stateA, PrevExit: worklease.ExitFinished, PrevHolderID: "node-a"}))
			})

			It("after A's WorkFn fails, B sees ExitAbandoned and A's final state", func() {
				env := h.newEnv()
				b := env.backend

				leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
				Expect(err).NotTo(HaveOccurred())
				workErr := errors.New("batch failed")
				rA, err := worker.NewRunner(worker.RunnerConfig{
					Lease: leaseA,
					WorkFn: func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
						return []byte("final-a"), workErr
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(rA.Run(ctx, "work-1")).To(MatchError(workErr))

				Expect(successorSees(b)).To(Equal(worklease.Checkpoint{State: []byte("final-a"), PrevExit: worklease.ExitAbandoned, PrevHolderID: "node-a"}))
			})
		})

		// ===== PHASE 3: Recovery After No Declared Exit =====
		Describe("Phase 3: Recovery After No Declared Exit", func() {
			It("after A crashes, B sees ExitExpired and A's last checkpoint", func() {
				env := h.newEnv()
				b := env.backend

				stateA := []byte("checkpoint-before-crash")

				// Worker A acquires and checkpoints — no release (crash simulation).
				leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
				Expect(err).NotTo(HaveOccurred())
				tokenA, err := leaseA.Acquire(ctx, "work-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(leaseA.Checkpoint(ctx, tokenA, stateA)).To(Succeed())

				// Expire A's lease, as if its TTL had passed.
				env.expire("work-1")

				Expect(successorSees(b)).To(Equal(worklease.Checkpoint{State: stateA, PrevExit: worklease.ExitExpired, PrevHolderID: "node-a"}))
			})

			It("after A's ReadCheckpoint fails, A does not release and B sees ExitExpired with A as the previous holder (#79 reproduction 2)", func() {
				env := h.newEnv()
				b := env.backend

				leaseA, err := worklease.New(&failFirstReadCheckpoint{Backend: b}, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
				Expect(err).NotTo(HaveOccurred())
				fnCalled := false
				rA, err := worker.NewRunner(worker.RunnerConfig{
					Lease: leaseA,
					WorkFn: func(_ context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
						fnCalled = true
						return nil, nil
					},
				})
				Expect(err).NotTo(HaveOccurred())
				err = rA.Run(ctx, "work-1")
				Expect(err).To(MatchError(ContainSubstring("storage unavailable")))
				Expect(fnCalled).To(BeFalse())

				// A did not release, so the lease is still held until its TTL passes.
				leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-b"})
				Expect(err).NotTo(HaveOccurred())
				_, err = leaseB.Acquire(ctx, "work-1")
				Expect(err).To(MatchError(worklease.ErrLeaseHeld))

				env.expire("work-1")

				got := successorSees(b)
				Expect(got.PrevExit).To(Equal(worklease.ExitExpired))
				Expect(got.PrevHolderID).To(Equal("node-a"))
			})

			It("after A's lease window is exhausted, A does not release and B sees ExitExpired (#79 reproduction 3)", func() {
				env := h.newEnv()
				b := env.backend

				// Every Renew fails transiently, so the 200ms window runs out in real time.
				leaseA, err := worklease.New(failingRenew{Backend: b}, worklease.Config{TTL: 200 * time.Millisecond, HolderID: "node-a"})
				Expect(err).NotTo(HaveOccurred())
				rA, err := worker.NewRunner(worker.RunnerConfig{
					Lease: leaseA,
					WorkFn: func(wctx context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
						<-wctx.Done()
						return []byte("unsaved"), nil
					},
					RenewalOptions: []worklease.RenewalOption{worklease.WithRenewalInterval(20 * time.Millisecond)},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(rA.Run(ctx, "work-1")).To(MatchError(worklease.ErrLeaseWindowExhausted))

				// PrevExit == ExitExpired below proves A did not release: Release records a declared exit.
				env.expire("work-1")

				got := successorSees(b)
				Expect(got.PrevExit).To(Equal(worklease.ExitExpired))
				Expect(got.PrevHolderID).To(Equal("node-a"))
				Expect(got.State).To(BeNil()) // the final state was not checkpointed
			})
		})

		// ===== PHASE 4: Fencing Propagation =====
		Describe("Phase 4: Fencing Propagation", func() {
			It("runner.Run returns ErrFenced when another holder acquires the lease", func() {
				env := h.newEnv()
				b := env.backend
				gate := &renewalGate{Backend: b}

				leaseA, err := worklease.New(gate, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
				Expect(err).NotTo(HaveOccurred())
				leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-b"})
				Expect(err).NotTo(HaveOccurred())

				started := make(chan struct{})
				runErr := make(chan error, 1)

				// Worker A blocks inside WorkFn waiting for fencing.
				rA, err := worker.NewRunner(worker.RunnerConfig{
					Lease: leaseA,
					WorkFn: func(ctx context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
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

				// Hold Worker A's renewals at the gate, expire its lease, and let Worker B
				// acquire and bump the fencing token.
				gate.pause()
				env.expire("work-1")

				tokenB, err := leaseB.Acquire(ctx, "work-1")
				Expect(err).NotTo(HaveOccurred())
				defer leaseB.Release(ctx, tokenB, worklease.ExitFinished) //nolint:errcheck
				gate.resume()

				// Worker A's next renewal (fires within 50ms real time) passes the gate, sees
				// a fencing token mismatch, and cancels the renewal context with cause
				// ErrFenced, so Run returns ErrFenced without releasing.
				Eventually(runErr, "1s").Should(Receive(MatchError(worklease.ErrFenced)))
			})
		})

		// ===== PHASE 5: Cancellation =====
		Describe("Phase 5: Cancellation", func() {
			It("when A's parent context is cancelled mid-WorkFn, A's final state is stored and B sees ExitAbandoned", func() {
				env := h.newEnv()
				b := env.backend

				leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "node-a"})
				Expect(err).NotTo(HaveOccurred())
				parent, cancelParent := context.WithCancel(ctx)
				defer cancelParent()
				rA, err := worker.NewRunner(worker.RunnerConfig{
					Lease: leaseA,
					WorkFn: func(wctx context.Context, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
						cancelParent()
						<-wctx.Done()
						return []byte("final-a"), wctx.Err()
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(rA.Run(parent, "work-1")).To(MatchError(context.Canceled))

				// No clock advance: the cancelled Runner released on its cleanup context.
				Expect(successorSees(b)).To(Equal(worklease.Checkpoint{State: []byte("final-a"), PrevExit: worklease.ExitAbandoned, PrevHolderID: "node-a"}))
			})
		})
	})
}
