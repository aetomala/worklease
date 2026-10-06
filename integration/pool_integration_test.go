package integration_test

import (
	"context"
	"sort"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend/memory"
	"github.com/aetomala/worklease/checkpoint"
	"github.com/aetomala/worklease/pool"
)

// permDone signals that a slot should exit permanently without reacquisition.
type permDone struct{}

func (permDone) Error() string   { return "done" }
func (permDone) Permanent() bool { return true }

var _ = Describe("pool.Pool", func() {
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

	// ===== PHASE 1: All Work IDs Eventually Acquired =====
	Describe("Phase 1: All Work IDs Eventually Acquired", func() {
		It("all three work IDs are processed before Run returns", func() {
			b := memory.New()
			lease, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-a"})
			Expect(err).NotTo(HaveOccurred())

			var acquired sync.Map

			p, err := pool.New(lease, pool.Config{
				WorkIDs:         []string{"q-0", "q-1", "q-2"},
				IdleInterval:    10 * time.Millisecond,
				RerunInterval:   10 * time.Millisecond,
				BackoffInterval: 10 * time.Millisecond,
			}, func(_ context.Context, workID string, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
				acquired.Store(workID, true)
				return nil, permDone{}
			})
			Expect(err).NotTo(HaveOccurred())

			// Every slot exits via PermanentError, so Run reports ErrAllSlotsDead.
			Expect(p.Run(ctx)).To(MatchError(pool.ErrAllSlotsDead))

			for _, id := range []string{"q-0", "q-1", "q-2"} {
				_, ok := acquired.Load(id)
				Expect(ok).To(BeTrue(), "expected work ID %q to be acquired", id)
			}
		})
	})

	// ===== PHASE 2: ActiveSlots Observability =====
	Describe("Phase 2: ActiveSlots Observability", func() {
		It("ActiveSlots returns all three work IDs while WorkFns are in-flight", func() {
			b := memory.New()
			lease, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-a"})
			Expect(err).NotTo(HaveOccurred())

			started := make(chan string, 3)

			p, err := pool.New(lease, pool.Config{
				WorkIDs:         []string{"q-0", "q-1", "q-2"},
				IdleInterval:    10 * time.Millisecond,
				RerunInterval:   10 * time.Millisecond,
				BackoffInterval: 10 * time.Millisecond,
			}, func(ctx context.Context, workID string, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
				started <- workID
				<-ctx.Done()
				return nil, ctx.Err()
			})
			Expect(err).NotTo(HaveOccurred())

			runDone := make(chan error, 1)
			go func() { runDone <- p.Run(ctx) }()

			// Wait until all three WorkFns have started.
			seen := make(map[string]bool, 3)
			for i := 0; i < 3; i++ {
				id := <-started
				seen[id] = true
			}
			Expect(seen).To(HaveLen(3))

			active := p.ActiveSlots()
			sort.Strings(active)
			Expect(active).To(Equal([]string{"q-0", "q-1", "q-2"}))

			cancel()
			Eventually(runDone, "1s").Should(Receive())
		})
	})

	// ===== PHASE 3: PermanentError Drops a Slot =====
	Describe("Phase 3: PermanentError Drops a Slot", func() {
		It("decommissioned slot exits permanently; remaining slots stay active", func() {
			b := memory.New()
			lease, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-a"})
			Expect(err).NotTo(HaveOccurred())

			started := make(chan string, 2)

			p, err := pool.New(lease, pool.Config{
				WorkIDs:         []string{"q-0", "q-1", "q-decommissioned"},
				IdleInterval:    10 * time.Millisecond,
				RerunInterval:   10 * time.Millisecond,
				BackoffInterval: 10 * time.Millisecond,
			}, func(ctx context.Context, workID string, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
				if workID == "q-decommissioned" {
					return nil, permDone{}
				}
				started <- workID
				<-ctx.Done()
				return nil, ctx.Err()
			})
			Expect(err).NotTo(HaveOccurred())

			runDone := make(chan error, 1)
			go func() { runDone <- p.Run(ctx) }()

			// Wait until q-0 and q-1 are both in their blocking WorkFns.
			seen := make(map[string]bool, 2)
			for i := 0; i < 2; i++ {
				id := <-started
				seen[id] = true
			}
			Expect(seen).To(HaveKey("q-0"))
			Expect(seen).To(HaveKey("q-1"))

			// q-decommissioned exited permanently — wait until it drops from active.
			Eventually(func() []string { return p.ActiveSlots() }, "1s").
				ShouldNot(ContainElement("q-decommissioned"))
			Expect(p.ActiveSlots()).To(ContainElement("q-0"))
			Expect(p.ActiveSlots()).To(ContainElement("q-1"))

			cancel()
			Eventually(runDone, "1s").Should(Receive())
		})
	})

	// ===== PHASE 4: Checkpoint-as-Cursor Resume =====
	Describe("Phase 4: Checkpoint-as-Cursor Resume", func() {
		It("Pool B sees ExitAbandoned and Pool A's offsets for every slot (D1)", func() {
			b := memory.New()

			codec := checkpoint.JSON()
			type cursor struct{ Offset int }

			// Pool A: checkpoint {Offset: 100} per slot then exit permanently.
			leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-a"})
			Expect(err).NotTo(HaveOccurred())

			pA, err := pool.New(leaseA, pool.Config{
				WorkIDs:         []string{"q-0", "q-1", "q-2"},
				IdleInterval:    10 * time.Millisecond,
				RerunInterval:   10 * time.Millisecond,
				BackoffInterval: 10 * time.Millisecond,
			}, func(_ context.Context, _ string, _ worklease.Token, _ worklease.Checkpoint) ([]byte, error) {
				state, encErr := checkpoint.Encode[cursor](codec, cursor{Offset: 100})
				Expect(encErr).NotTo(HaveOccurred())
				return state, permDone{}
			})
			Expect(err).NotTo(HaveOccurred())
			// Pool A's slots all exit via PermanentError, so Run reports ErrAllSlotsDead.
			Expect(pA.Run(ctx)).To(MatchError(pool.ErrAllSlotsDead))

			// No clock advance needed: Pool A's slots are released with ExitAbandoned,
			// and Release now sets expiresAt to the past — Pool B can acquire immediately.

			// Pool B: capture the previous exit and offset per slot.
			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-b"})
			Expect(err).NotTo(HaveOccurred())

			type slotResult struct {
				prevExit worklease.ExitMode
				offset   int
			}
			results := make(map[string]slotResult)
			var resultsMu sync.Mutex

			pB, err := pool.New(leaseB, pool.Config{
				WorkIDs:         []string{"q-0", "q-1", "q-2"},
				IdleInterval:    10 * time.Millisecond,
				RerunInterval:   10 * time.Millisecond,
				BackoffInterval: 10 * time.Millisecond,
			}, func(_ context.Context, workID string, _ worklease.Token, prior worklease.Checkpoint) ([]byte, error) {
				c, decErr := checkpoint.Decode[cursor](codec, prior.State)
				Expect(decErr).NotTo(HaveOccurred())
				resultsMu.Lock()
				results[workID] = slotResult{prevExit: prior.PrevExit, offset: c.Offset}
				resultsMu.Unlock()
				return nil, permDone{}
			})
			Expect(err).NotTo(HaveOccurred())
			// Pool B's slots all exit via PermanentError, so Run reports ErrAllSlotsDead.
			Expect(pB.Run(ctx)).To(MatchError(pool.ErrAllSlotsDead))

			resultsMu.Lock()
			defer resultsMu.Unlock()
			for _, id := range []string{"q-0", "q-1", "q-2"} {
				r := results[id]
				// Pool A's slots exited through PermanentError, which releases with
				// ExitAbandoned (D1); the offsets are still recovered.
				Expect(r.prevExit).To(Equal(worklease.ExitAbandoned), "expected ExitAbandoned for %q", id)
				Expect(r.offset).To(Equal(100), "expected offset 100 for %q", id)
			}
		})
	})

	// ===== PHASE 5: Cancellation =====
	Describe("Phase 5: Cancellation", func() {
		It("when Run's context is cancelled mid-WorkFn, each slot's final state is stored and a successor sees ExitAbandoned (Rule 34)", func() {
			b := memory.New()
			ids := []string{"q-0", "q-1", "q-2"}

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
				Eventually(started, "1s").Should(Receive())
			}

			runCancel()
			Eventually(runDone, "1s").Should(Receive(BeNil()))

			// No clock advance: every slot released on its cleanup context.
			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-b"})
			Expect(err).NotTo(HaveOccurred())
			for _, id := range ids {
				tokenB, err := leaseB.Acquire(ctx, id)
				Expect(err).NotTo(HaveOccurred(), "expected %q to be released", id)
				cp, err := leaseB.ReadCheckpoint(ctx, tokenB)
				Expect(err).NotTo(HaveOccurred())
				Expect(cp).To(Equal(worklease.Checkpoint{State: []byte("final-" + id), PrevExit: worklease.ExitAbandoned, PrevHolderID: "pool-a"}))
			}
		})
	})
})
