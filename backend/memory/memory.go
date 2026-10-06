package memory

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	exitMode     backend.ExitMode // Current holder's declared exit; ExitNone encodes SQL NULL (no declared exit).
	prevExit     backend.ExitMode // Immediately previous holder's exit, captured at Acquire.
	prevHolderID string           // Immediately previous holder's ID; empty when prevExit is ExitNone.
	updatedAt    time.Time
}

// heldBy reports whether rec identifies the stored lease — both the holder ID
// and the fencing token match. Mirrors the postgres WHERE clause on writes.
func (r *record) heldBy(rec backend.LeaseRecord) bool {
	return r.holderID == rec.HolderID && r.fencingToken == rec.FencingToken
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
// The seq field is the per-instance monotonic fencing counter, shared across all
// work IDs in this instance and never reset for the life of the instance. Two
// independent New() calls produce independent counters — this mirrors one Postgres
// sequence per database, not a process-global counter.
type memoryBackend struct {
	mu      sync.Mutex
	clock   Clock // never nil after New()
	records map[string]*record
	seq     atomic.Uint64
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
	// ===== Check Context =====
	if err := ctx.Err(); err != nil {
		return backend.LeaseRecord{}, fmt.Errorf("memory: Acquire: %w", err)
	}

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

	// ===== STEP 4: Determine New Fencing Token, Preserve Checkpoint, Capture Previous Exit =====
	// Fencing token comes from the per-instance global sequence — strictly
	// increasing across all work IDs, never reset. The checkpoint carries over
	// from an expired record so a successor can read what the prior owner left
	// behind, and the prior owner's declared exit (ExitExpired if none) and
	// holder ID are captured as the new lease's previous exit — all under the
	// same lock that issues the token (mirrors the postgres ON CONFLICT clause).
	newToken := mb.seq.Add(1)
	var prevCheckpoint []byte
	prevExit := backend.ExitNone
	var prevHolderID string
	if exists {
		prevCheckpoint = r.checkpoint
		prevExit = r.exitMode
		if prevExit == backend.ExitNone {
			prevExit = backend.ExitExpired
		}
		prevHolderID = r.holderID
	}

	// ===== STEP 5: Create New Record =====
	newRecord := &record{
		holderID:     holderID,
		fencingToken: newToken,
		expiresAt:    mb.clock.Now().Add(ttl),
		checkpoint:   prevCheckpoint,
		exitMode:     backend.ExitNone,
		prevExit:     prevExit,
		prevHolderID: prevHolderID,
		updatedAt:    mb.clock.Now(),
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

// Checkpoint persists state associated with the current lease. If the holder ID
// or fencing token does not match, ErrFenced is returned without modification.
// Once the holder has declared an exit with Release, ErrLeaseExpired is returned
// without writing. Checkpoint never writes an exit field.
func (mb *memoryBackend) Checkpoint(ctx context.Context, record backend.LeaseRecord, state []byte, ttl time.Duration) error {
	// ===== Check Context =====
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memory: Checkpoint: %w", err)
	}

	// ===== STEP 1: Acquire Lock =====
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// ===== STEP 2: Look Up Record =====
	r, exists := mb.records[record.WorkID]

	// ===== STEP 3: Check Fencing Token =====
	if !exists || !r.heldBy(record) {
		return worklease.ErrFenced
	}

	// ===== STEP 4: Refuse After a Declared Exit =====
	// A declared exit cannot be undone by a late write (D12). A lapsed lease
	// with no declared exit can still be revived: no expiry check here.
	if r.exitMode != backend.ExitNone {
		return worklease.ErrLeaseExpired
	}

	// ===== STEP 5: Update Checkpoint =====
	// Defensive copy (ADR-0014): do not alias the caller's slice — the caller
	// may mutate state after Checkpoint returns.
	if state == nil {
		r.checkpoint = nil
	} else {
		stored := make([]byte, len(state))
		copy(stored, state)
		r.checkpoint = stored
	}
	r.expiresAt = mb.clock.Now().Add(ttl)
	r.updatedAt = mb.clock.Now()

	return nil
}

// Renew extends the lease expiration time. If the holder ID or fencing token does
// not match, ErrFenced is returned. If the lease has already expired, ErrLeaseExpired is returned.
func (mb *memoryBackend) Renew(ctx context.Context, record backend.LeaseRecord, ttl time.Duration) error {
	// ===== Check Context =====
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memory: Renew: %w", err)
	}

	// ===== STEP 1: Acquire Lock =====
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// ===== STEP 2: Look Up Record =====
	r, exists := mb.records[record.WorkID]

	// ===== STEP 3: Check Fencing Token =====
	if !exists || !r.heldBy(record) {
		return worklease.ErrFenced
	}

	// ===== STEP 4: Check Expiry =====
	// A lease is renewable only while now < expiresAt — mirrors postgres
	// expires_at > NOW(), so both backends refuse renewal at the boundary.
	if !mb.clock.Now().Before(r.expiresAt) {
		return worklease.ErrLeaseExpired
	}

	// ===== STEP 5: Extend Expiration =====
	r.expiresAt = mb.clock.Now().Add(ttl)
	r.updatedAt = mb.clock.Now()

	return nil
}

// Release records mode as the holder's declared exit and expires the lease
// immediately by setting expiresAt to the past, so a successor can acquire
// without waiting for the TTL. Returns ErrInvalidExitMode for an undeclarable
// mode before any other check, ErrFenced if the holder ID or fencing token does
// not match, and ErrLeaseExpired if the lease has expired.
func (mb *memoryBackend) Release(ctx context.Context, record backend.LeaseRecord, mode backend.ExitMode) error {
	// ===== Validate Mode =====
	if mode != backend.ExitFinished && mode != backend.ExitAbandoned && mode != backend.ExitRetired {
		return worklease.ErrInvalidExitMode
	}

	// ===== Check Context =====
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memory: Release: %w", err)
	}

	// ===== STEP 1: Acquire Lock =====
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// ===== STEP 2: Look Up Record =====
	r, exists := mb.records[record.WorkID]

	// ===== STEP 3: Check Fencing Token =====
	if !exists || !r.heldBy(record) {
		return worklease.ErrFenced
	}

	// ===== STEP 4: Check Expiry =====
	// Live only while now < expiresAt — mirrors postgres expires_at > NOW(). A
	// second Release after a declared exit lands here too (D17).
	if !r.expiresAt.After(mb.clock.Now()) {
		return worklease.ErrLeaseExpired
	}

	// ===== STEP 5: Record Exit and Expire Immediately =====
	// Setting expiresAt to the past makes the record immediately acquirable
	// by a successor — the TTL governs crash detection, not handoff latency.
	r.exitMode = mode
	r.expiresAt = mb.clock.Now().Add(releaseGracePeriod)
	r.updatedAt = mb.clock.Now()

	return nil
}

// ReadCheckpoint returns the checkpoint state and the immediately previous
// holder's exit. If no record exists or the fencing token does not match,
// ErrFenced is returned. If the record has no checkpoint, State is nil.
func (mb *memoryBackend) ReadCheckpoint(ctx context.Context, record backend.LeaseRecord) (backend.Checkpoint, error) {
	// ===== Check Context =====
	if err := ctx.Err(); err != nil {
		return backend.Checkpoint{}, fmt.Errorf("memory: ReadCheckpoint: %w", err)
	}

	// ===== STEP 1: Acquire Lock =====
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// ===== STEP 2: Look Up Record =====
	r, exists := mb.records[record.WorkID]

	// ===== STEP 3: Check Fencing Token =====
	if !exists || r.fencingToken != record.FencingToken {
		return backend.Checkpoint{}, worklease.ErrFenced
	}

	// ===== STEP 4: Return Checkpoint and Previous Exit =====
	// Defensive copy (ADR-0014): return a fresh slice, never the stored backing
	// array — mutation of the result must not affect stored state.
	if r.checkpoint == nil {
		return backend.Checkpoint{PrevExit: r.prevExit, PrevHolderID: r.prevHolderID}, nil
	}
	out := make([]byte, len(r.checkpoint))
	copy(out, r.checkpoint)
	return backend.Checkpoint{State: out, PrevExit: r.prevExit, PrevHolderID: r.prevHolderID}, nil
}

// Forget permanently deletes the record identified by record.WorkID. Returns
// ErrFenced if no record exists or the holder ID or fencing token does not match.
func (mb *memoryBackend) Forget(ctx context.Context, record backend.LeaseRecord) error {
	// ===== Check Context =====
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memory: Forget: %w", err)
	}

	mb.mu.Lock()
	defer mb.mu.Unlock()

	r, exists := mb.records[record.WorkID]
	if !exists || !r.heldBy(record) {
		return worklease.ErrFenced
	}

	delete(mb.records, record.WorkID)

	return nil
}

// Sweep deletes records older than opts.Retention that are not currently held,
// returning the number deleted. Does not validate opts.Retention — callers use
// worklease.Vacuum.Sweep, which validates before calling this.
func (mb *memoryBackend) Sweep(ctx context.Context, opts backend.SweepOptions) (int64, error) {
	// ===== Check Context =====
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("memory: Sweep: %w", err)
	}

	mb.mu.Lock()
	defer mb.mu.Unlock()

	now := mb.clock.Now()
	var deleted int64
	for workID, r := range mb.records {
		// Strictly older than Retention — mirrors postgres updated_at < NOW() - retention.
		if now.Sub(r.updatedAt) <= opts.Retention {
			continue
		}
		if !now.After(r.expiresAt) {
			continue
		}
		if r.exitMode != backend.ExitRetired && !(opts.IncludeExpired && r.exitMode == backend.ExitNone) {
			continue
		}
		delete(mb.records, workID)
		deleted++
	}

	return deleted, nil
}
