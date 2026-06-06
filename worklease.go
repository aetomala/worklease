package worklease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aetomala/worklease/backend"
)

// Config holds configuration for a Lease instance.
type Config struct {
	// TTL is the time-to-live for acquired leases. Required; zero returns an error.
	TTL time.Duration

	// HolderID is the identifier of the entity that will hold leases. Required; empty returns an error.
	HolderID string
}

// leaseClient implements the Lease interface. It wraps a Backend and delegates
// all operations to it, translating between the exported Token API and the
// backend's LeaseRecord API.
type leaseClient struct {
	b   backend.Backend
	cfg Config
}

// New returns a new Lease instance backed by the provided Backend. Returns an error
// if the backend is nil, TTL is zero, or HolderID is empty.
func New(b backend.Backend, cfg Config) (Lease, error) {
	// ===== STEP 1: Validate Required Fields =====
	if b == nil {
		return nil, fmt.Errorf("worklease: New: backend is required")
	}

	if cfg.TTL == 0 {
		return nil, fmt.Errorf("worklease: New: TTL is required")
	}

	if cfg.HolderID == "" {
		return nil, fmt.Errorf("worklease: New: HolderID is required")
	}

	// ===== STEP 2: Initialize and Return =====
	return &leaseClient{b: b, cfg: cfg}, nil
}

// newToken converts a backend LeaseRecord to an exported Token.
func newToken(r backend.LeaseRecord) Token {
	return Token{
		workID:       r.WorkID,
		holderID:     r.HolderID,
		fencingToken: r.FencingToken,
		expiresAt:    r.ExpiresAt,
	}
}

// toRecord converts an exported Token to a backend LeaseRecord.
func toRecord(t Token) backend.LeaseRecord {
	return backend.LeaseRecord{
		WorkID:       t.workID,
		HolderID:     t.holderID,
		FencingToken: t.fencingToken,
		ExpiresAt:    t.expiresAt,
	}
}

// Checkpoint persists state associated with the current lease. The caller must
// pass a valid Token obtained from Acquire or Renew. Returns ErrFenced if the
// token's fencing token no longer matches the stored lease.
func (c *leaseClient) Checkpoint(ctx context.Context, token Token, state []byte) error {
	// ===== Validate and Delegate =====
	record := toRecord(token)
	err := c.b.Checkpoint(ctx, record, state, c.cfg.TTL)

	if errors.Is(err, ErrFenced) {
		return fmt.Errorf("worklease: Checkpoint: workID=%q holderID=%q: %w", token.WorkID(), token.HolderID(), ErrFenced)
	}

	if err != nil {
		return fmt.Errorf("worklease: Checkpoint: %w", err)
	}

	return nil
}

// Renew extends the lease expiration time. Returns ErrFenced if the token's
// fencing token no longer matches the stored lease, or ErrLeaseExpired if the
// lease has already expired.
func (c *leaseClient) Renew(ctx context.Context, token Token) error {
	// ===== Validate and Delegate =====
	record := toRecord(token)
	err := c.b.Renew(ctx, record, c.cfg.TTL)

	if errors.Is(err, ErrFenced) {
		return fmt.Errorf("worklease: Renew: workID=%q holderID=%q: %w", token.WorkID(), token.HolderID(), ErrFenced)
	}

	if err != nil {
		return fmt.Errorf("worklease: Renew: %w", err)
	}

	return nil
}

// Release surrenders the lease. Returns ErrFenced if the token's fencing token
// no longer matches the stored lease.
func (c *leaseClient) Release(ctx context.Context, token Token) error {
	// ===== Validate and Delegate =====
	record := toRecord(token)
	err := c.b.Release(ctx, record)

	if errors.Is(err, ErrFenced) {
		return fmt.Errorf("worklease: Release: workID=%q holderID=%q: %w", token.WorkID(), token.HolderID(), ErrFenced)
	}

	if err != nil {
		return fmt.Errorf("worklease: Release: %w", err)
	}

	return nil
}

// ReadCheckpoint retrieves persisted state and the clean handoff flag for the
// given lease. The caller must pass a valid Token. Returns ErrFenced if the
// token's fencing token no longer matches the stored lease.
func (c *leaseClient) ReadCheckpoint(ctx context.Context, token Token) ([]byte, bool, error) {
	// ===== Delegate to Backend =====
	record := toRecord(token)
	return c.b.ReadCheckpoint(ctx, record)
}
