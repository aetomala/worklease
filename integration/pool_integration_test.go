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
				BackoffInterval: 10 * time.Millisecond,
			}, func(_ context.Context, workID string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
				acquired.Store(workID, true)
				return nil, permDone{}
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(p.Run(ctx)).To(Succeed())

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
				WorkIDs: []string{"q-0", "q-1", "q-2"},
			}, func(ctx context.Context, workID string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
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
				WorkIDs: []string{"q-0", "q-1", "q-decommissioned"},
			}, func(ctx context.Context, workID string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
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
		It("Pool B reads clean-handoff checkpoints left by Pool A", func() {
			b := memory.New()

			codec := checkpoint.JSON()
			type cursor struct{ Offset int }

			// Pool A: checkpoint {Offset: 100} per slot then exit permanently.
			leaseA, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-a"})
			Expect(err).NotTo(HaveOccurred())

			pA, err := pool.New(leaseA, pool.Config{
				WorkIDs: []string{"q-0", "q-1", "q-2"},
			}, func(_ context.Context, _ string, _ worklease.Token, _ []byte, _ bool) ([]byte, error) {
				state, encErr := checkpoint.Encode[cursor](codec, cursor{Offset: 100})
				Expect(encErr).NotTo(HaveOccurred())
				return state, permDone{}
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(pA.Run(ctx)).To(Succeed())

			// No clock advance needed: Pool A's slots are released via permDone exit,
			// and Release now sets expiresAt to the past — Pool B can acquire immediately.

			// Pool B: capture prior and cleanHandoff per slot.
			leaseB, err := worklease.New(b, worklease.Config{TTL: 30 * time.Second, HolderID: "pool-b"})
			Expect(err).NotTo(HaveOccurred())

			type slotResult struct {
				cleanHandoff bool
				offset       int
			}
			results := make(map[string]slotResult)
			var resultsMu sync.Mutex

			pB, err := pool.New(leaseB, pool.Config{
				WorkIDs: []string{"q-0", "q-1", "q-2"},
			}, func(_ context.Context, workID string, _ worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
				c, decErr := checkpoint.Decode[cursor](codec, prior)
				Expect(decErr).NotTo(HaveOccurred())
				resultsMu.Lock()
				results[workID] = slotResult{cleanHandoff: cleanHandoff, offset: c.Offset}
				resultsMu.Unlock()
				return nil, permDone{}
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(pB.Run(ctx)).To(Succeed())

			resultsMu.Lock()
			defer resultsMu.Unlock()
			for _, id := range []string{"q-0", "q-1", "q-2"} {
				r := results[id]
				Expect(r.cleanHandoff).To(BeTrue(), "expected cleanHandoff for %q", id)
				Expect(r.offset).To(Equal(100), "expected offset 100 for %q", id)
			}
		})
	})
})
