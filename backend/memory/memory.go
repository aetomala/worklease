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

// memoryBackend is an in-memory implementation of the Backend interface.
type memoryBackend struct {
	mu      sync.Mutex
	records map[string]*record
}

// New returns an in-memory Backend safe for concurrent use within a single process.
// No Close method is required — in-memory storage has no resources to release.
func New() backend.Backend {
	return &memoryBackend{
		records: make(map[string]*record),
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
	if exists && !time.Now().After(r.expiresAt) {
		// Lease is held and not expired
		return backend.LeaseRecord{}, worklease.ErrLeaseHeld
	}

	// ===== STEP 4: Determine New Fencing Token =====
	var newToken uint64 = 1
	if exists {
		newToken = r.fencingToken + 1
	}

	// ===== STEP 5: Create New Record =====
	newRecord := &record{
		holderID:     holderID,
		fencingToken: newToken,
		expiresAt:    time.Now().Add(ttl),
		checkpoint:   nil,
		cleanHandoff: false,
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
	r.expiresAt = time.Now().Add(ttl)

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
	if time.Now().After(r.expiresAt) {
		return worklease.ErrLeaseExpired
	}

	// ===== STEP 5: Extend Expiration =====
	r.expiresAt = time.Now().Add(ttl)

	return nil
}

// Release surrenders the lease. If the fencing token does not match, ErrFenced
// is returned without modification.
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

	// ===== STEP 4: Delete Record =====
	delete(mb.records, record.WorkID)

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
