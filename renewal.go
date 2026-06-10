package worklease

import (
	"context"
	"errors"
	"sync"
	"time"
)

// StartRenewal begins a managed renewal goroutine that automatically renews the lease at regular intervals.
// It returns a derived context (renewCtx) and a stop function (stopRenewal).
//
// The renewal goroutine has four lifecycle paths:
//   - Normal stop: caller invokes stopRenewal(). The goroutine exits cleanly without cancelling renewCtx.
//   - Fencing: c.b.Renew returns ErrFenced. The goroutine cancels renewCtx and exits.
//   - Non-fencing error: c.b.Renew returns a non-ErrFenced error. The goroutine cancels renewCtx and exits.
//   - Parent context cancelled: ctx.Done() fires. The goroutine exits cleanly; renewCtx auto-cancels as a child context.
//
// stopRenewal blocks until the goroutine has fully exited before returning.
// It is idempotent — calling it multiple times is safe.
//
// Callers must invoke stopRenewal before calling Release, and must use the original ctx (not renewCtx) for Release.
func (c *leaseClient) StartRenewal(ctx context.Context, token Token, opts ...RenewalOption) (context.Context, func()) {
	// ===== STEP 1: Resolve options =====
	rcfg := renewalConfig{renewalInterval: c.cfg.TTL / 2}
	for _, o := range opts {
		o(&rcfg)
	}

	// ===== STEP 2: Derive cancellable context =====
	renewCtx, cancel := context.WithCancel(ctx)

	// ===== STEP 3: Prepare stop channel and wait group =====
	stopCh := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup

	// ===== STEP 4: Start goroutine =====
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(rcfg.renewalInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return // Path 1: normal stop — do NOT call cancel()
			case <-ctx.Done():
				return // Path 4: parent cancelled — renewCtx auto-cancels
			case <-ticker.C:
				err := c.b.Renew(ctx, toRecord(token), c.cfg.TTL)
				c.obs.OnRenew(ctx, token, err)
				if errors.Is(err, ErrFenced) {
					c.obs.OnFenced(ctx, token)
					cancel()
					return
				}
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	// ===== STEP 5: Build stop function =====
	stopRenewal := func() {
		once.Do(func() { close(stopCh) })
		wg.Wait()
	}

	return renewCtx, stopRenewal
}
