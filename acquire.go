package worklease

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Acquire attempts to acquire a lease for the given workID. Returns ErrLeaseHeld
// if a lease already exists for this workID. If WithWaitForLease is set, blocks
// until the lease is available, polling at the configured interval. Returns
// ErrLeaseHeld on context cancellation while waiting.
func (c *leaseClient) Acquire(ctx context.Context, workID string, opts ...AcquireOption) (Token, error) {
	// ===== STEP 1: Validate Inputs =====
	if workID == "" {
		return Token{}, fmt.Errorf("worklease: Acquire: workID is required")
	}

	// ===== STEP 2: Resolve Options =====
	cfg := acquireConfig{
		pollInterval: 2 * time.Second,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	// ===== STEP 3: Single-Attempt Path =====
	if !cfg.waitForLease {
		record, err := c.b.Acquire(ctx, workID, c.cfg.HolderID, c.cfg.TTL)
		if err != nil {
			return Token{}, fmt.Errorf("worklease: Acquire: %w", err)
		}
		return newToken(record), nil
	}

	// ===== STEP 4: Wait+Retry Loop =====
	for {
		record, err := c.b.Acquire(ctx, workID, c.cfg.HolderID, c.cfg.TTL)
		if err == nil {
			return newToken(record), nil
		}

		if !errors.Is(err, ErrLeaseHeld) {
			return Token{}, fmt.Errorf("worklease: Acquire: %w", err)
		}

		// ===== STEP 5: Wait for Next Attempt =====
		timer := time.NewTimer(cfg.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Token{}, ErrLeaseHeld
		case <-timer.C:
			// Retry
		}
	}
}
