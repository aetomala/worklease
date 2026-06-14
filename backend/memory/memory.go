package memory

import (
	"context"
	"sync"
	"time"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
)

// record represents a single lease in memory.
type record struct {
	holderID     string
	fencingToken uint64
	expiresAt    time.Time
	checkpoint   []byte
	cleanHandoff bool
}

// Clock provides the current time. Exported — allows test packages outside
// backend/memory to implement fake clocks.
type Clock interface {
	Now() time.Time
}

// realClock is the default Clock implementation. Unexported.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Option configures the in-memory backend.
type Option func(*memoryConfig)

// memoryConfig holds resolved configuration for the in-memory backend.
type memoryConfig struct {
	clock Clock
}

// WithClock overrides the clock used for all time.Now() calls inside the
// memory backend. The default clock uses time.Now().
// Use WithClock to inject a fake clock in tests.
func WithClock(c Clock) Option {
	return func(cfg *memoryConfig) {
		cfg.clock = c
	}
}

// releaseGracePeriod is the offset applied to the current time when Release expires the
// lease immediately. It must satisfy: clock.Now() + releaseGracePeriod < clock.Now() at
// the moment of the next Acquire call — one millisecond in the past is sufficient for
// any clock with at least millisecond resolution.
const releaseGracePeriod = -time.Millisecond

// memoryBackend is an in-memory implementation of the Backend interface.
type memoryBackend struct {
	mu    sync.Mutex
	clock Clock // never nil after New()
	records map[string]*record
}

// New returns an in-memory Backend. Safe for concurrent use within a single process.
// Not safe for use across processes. No Close method — no cleanup required.
// Pass WithClock to inject a fake clock for deterministic expiry tests.
func New(opts ...Option) backend.Backend {
	cfg := memoryConfig{clock: realClock{}}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &memoryBackend{
		records: make(map[string]*record),
		clock:   cfg.clock,
	}
}

// Acquire attempts to acquire a lease for the given work. If no lease exists or
// the lease has expired, a new lease is created with an incremented fencing token.
// If a valid lease already exists, ErrLeaseHeld is returned without modification.
func (mb *memoryBackend) Acquire(ctx context.Context, workID, holderID string, ttl time.Duration) (backend.LeaseRecord, error) {
	// ===== STEP 1: Acquire Lock =====
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// ===== STEP 2: Check Existing Record =====
	r, exists := mb.records[workID]

	// ===== STEP 3: Evaluate Expiry =====
	if exists && !mb.clock.Now().After(r.expiresAt) {
		// Lease is held and not expired
		return backend.LeaseRecord{}, worklease.ErrLeaseHeld
	}

	// ===== STEP 4: Determine New Fencing Token and Preserve Checkpoint =====
	// Mirrors the PostgreSQL backend's ON CONFLICT DO UPDATE: checkpoint and
	// cleanHandoff are carried over from the expired record so that a successor
	// can read what the previous owner left behind — the same semantics as the
	// postgres backend's `checkpoint = worklease_leases.checkpoint` clause.
	var newToken uint64 = 1
	var prevCheckpoint []byte
	var prevCleanHandoff bool
	if exists {
		newToken = r.fencingToken + 1
		prevCheckpoint = r.checkpoint
		prevCleanHandoff = r.cleanHandoff
	}

	// ===== STEP 5: Create New Record =====
	newRecord := &record{
		holderID:     holderID,
		fencingToken: newToken,
		expiresAt:    mb.clock.Now().Add(ttl),
		checkpoint:   prevCheckpoint,
		cleanHandoff: prevCleanHandoff,
	}

	// ===== STEP 6: Store and Return =====
	mb.records[workID] = newRecord

	return backend.LeaseRecord{
		WorkID:       workID,
		HolderID:     holderID,
		FencingToken: newToken,
		ExpiresAt:    newRecord.expiresAt,
	}, nil
}

// Checkpoint persists state associated with the current lease. If the fencing
// token does not match, ErrFenced is returned without modification.
func (mb *memoryBackend) Checkpoint(ctx context.Context, record backend.LeaseRecord, state []byte, ttl time.Duration) error {
	// ===== STEP 1: Acquire Lock =====
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// ===== STEP 2: Look Up Record =====
	r, exists := mb.records[record.WorkID]

	// ===== STEP 3: Check Fencing Token =====
	if !exists || r.fencingToken != record.FencingToken {
		return worklease.ErrFenced
	}

	// ===== STEP 4: Update Checkpoint =====
	r.checkpoint = state
	r.expiresAt = mb.clock.Now().Add(ttl)
	r.cleanHandoff = false

	return nil
}

// Renew extends the lease expiration time. If the fencing token does not match,
// ErrFenced is returned. If the lease has already expired, ErrLeaseExpired is returned.
func (mb *memoryBackend) Renew(ctx context.Context, record backend.LeaseRecord, ttl time.Duration) error {
	// ===== STEP 1: Acquire Lock =====
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// ===== STEP 2: Look Up Record =====
	r, exists := mb.records[record.WorkID]

	// ===== STEP 3: Check Fencing Token =====
	if !exists || r.fencingToken != record.FencingToken {
		return worklease.ErrFenced
	}

	// ===== STEP 4: Check Expiry =====
	if mb.clock.Now().After(r.expiresAt) {
		return worklease.ErrLeaseExpired
	}

	// ===== STEP 5: Extend Expiration =====
	r.expiresAt = mb.clock.Now().Add(ttl)

	return nil
}

// Release surrenders the lease and expires it immediately by setting expiresAt to
// the past, so a successor can acquire without waiting for the TTL. If the fencing
// token does not match, ErrFenced is returned without modification.
func (mb *memoryBackend) Release(ctx context.Context, record backend.LeaseRecord) error {
	// ===== STEP 1: Acquire Lock =====
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// ===== STEP 2: Look Up Record =====
	r, exists := mb.records[record.WorkID]

	// ===== STEP 3: Check Fencing Token =====
	if !exists || r.fencingToken != record.FencingToken {
		return worklease.ErrFenced
	}

	// ===== STEP 4: Mark clean and expire immediately =====
	// Setting expiresAt to the past makes the record immediately acquirable
	// by a successor — the TTL governs crash detection, not clean-handoff latency.
	r.cleanHandoff = true
	r.expiresAt = mb.clock.Now().Add(releaseGracePeriod)

	return nil
}

// ReadCheckpoint retrieves persisted state and the clean handoff flag for the
// given lease. If the fencing token does not match, ErrFenced is returned.
// If the record has no checkpoint, nil and false are returned without error.
func (mb *memoryBackend) ReadCheckpoint(ctx context.Context, record backend.LeaseRecord) ([]byte, bool, error) {
	// ===== STEP 1: Acquire Lock =====
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// ===== STEP 2: Look Up Record =====
	r, exists := mb.records[record.WorkID]

	// ===== STEP 3: Check Fencing Token =====
	if !exists || r.fencingToken != record.FencingToken {
		return nil, false, worklease.ErrFenced
	}

	// ===== STEP 4: Return Checkpoint and Clean Handoff Flag =====
	return r.checkpoint, r.cleanHandoff, nil
}
