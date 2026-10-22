package worklease_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend/memory"
)

// The acquireCounter type counts Acquire attempts and ignores every other event.
// Embedding worklease.NoopObserver supplies the other LeaseObserver methods.
type acquireCounter struct {
	worklease.NoopObserver
	n atomic.Int64
}

func (c *acquireCounter) OnAcquire(_ context.Context, _ worklease.AcquireEvent) {
	c.n.Add(1)
}

func ExampleNoopObserver() {
	obs := &acquireCounter{}
	lease, err := worklease.New(memory.New(), worklease.Config{
		TTL:      30 * time.Second,
		HolderID: "worker-1",
		Observer: obs,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	if _, err := lease.Acquire(context.Background(), "work-1"); err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("acquires:", obs.n.Load())
	// Output: acquires: 1
}
