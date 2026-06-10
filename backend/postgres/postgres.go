package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"github.com/aetomala/worklease"
	"github.com/aetomala/worklease/backend"
)

const (
	queryAcquire = `
INSERT INTO worklease_leases (work_id, holder_id, fencing_token, expires_at, checkpoint, clean_handoff)
VALUES ($1, $2, 1, NOW() + $3, NULL, FALSE)
ON CONFLICT (work_id) DO UPDATE
SET holder_id     = EXCLUDED.holder_id,
    fencing_token = worklease_leases.fencing_token + 1,
    expires_at    = EXCLUDED.expires_at,
    checkpoint    = worklease_leases.checkpoint,
    clean_handoff = worklease_leases.clean_handoff,
    updated_at    = NOW()
WHERE worklease_leases.expires_at < NOW()`

	queryAcquireRead = `
SELECT fencing_token, expires_at
FROM worklease_leases
WHERE work_id = $1`

	queryCheckpoint = `
UPDATE worklease_leases
SET checkpoint    = $1,
    expires_at    = NOW() + $2,
    clean_handoff = FALSE,
    updated_at    = NOW()
WHERE work_id       = $3
  AND holder_id     = $4
  AND fencing_token = $5`

	queryRenew = `
UPDATE worklease_leases
SET expires_at  = NOW() + $1,
    updated_at  = NOW()
WHERE work_id       = $2
  AND holder_id     = $3
  AND fencing_token = $4
  AND expires_at    > NOW()`

	queryRenewCheck = `
SELECT expires_at
FROM worklease_leases
WHERE work_id       = $1
  AND fencing_token = $2`

	queryRelease = `
UPDATE worklease_leases
SET clean_handoff = TRUE,
    updated_at    = NOW()
WHERE work_id       = $1
  AND holder_id     = $2
  AND fencing_token = $3`

	queryReadCheckpoint = `
SELECT fencing_token, checkpoint, clean_handoff
FROM worklease_leases
WHERE work_id = $1`
)

// postgresBackend implements the Backend interface using PostgreSQL.
type postgresBackend struct {
	db *sql.DB
}

// New returns a PostgreSQL-backed Backend. Does not take ownership of db — caller
// is responsible for db.Close(). Returns an error if db is nil.
func New(db *sql.DB) (backend.Backend, error) {
	// ===== STEP 1: Validate Required Fields =====
	if db == nil {
		return nil, errors.New("postgres: db is required")
	}

	// ===== STEP 2: Initialize and Return =====
	return &postgresBackend{db: db}, nil
}

// Acquire attempts to acquire a lease for the given work. Returns ErrLeaseHeld
// if a lease already exists for this workID. Returns a LeaseRecord with the newly
// acquired lease details on success.
func (p *postgresBackend) Acquire(ctx context.Context, workID, holderID string, ttl time.Duration) (backend.LeaseRecord, error) {
	// ===== STEP 1: Execute INSERT/UPDATE =====
	ttlStr := fmt.Sprintf("%.6f seconds", ttl.Seconds())
	result, err := p.db.ExecContext(ctx, queryAcquire, workID, holderID, ttlStr)
	if err != nil {
		return backend.LeaseRecord{}, fmt.Errorf("postgres: Acquire: exec failed: %w", err)
	}

	// ===== STEP 2: Check Rows Affected =====
	// If no rows were affected, the lease is held by another (non-expired) holder
	n, err := result.RowsAffected()
	if err != nil {
		return backend.LeaseRecord{}, fmt.Errorf("postgres: Acquire: %w", err)
	}
	if n == 0 {
		return backend.LeaseRecord{}, worklease.ErrLeaseHeld
	}

	// ===== STEP 3: Read Back the Newly Acquired Lease =====
	var fencingToken uint64
	var expiresAt time.Time
	err = p.db.QueryRowContext(ctx, queryAcquireRead, workID).Scan(&fencingToken, &expiresAt)
	if err != nil {
		return backend.LeaseRecord{}, fmt.Errorf("postgres: Acquire: read failed: %w", err)
	}

	// ===== STEP 4: Return LeaseRecord =====
	return backend.LeaseRecord{
		WorkID:       workID,
		HolderID:     holderID,
		FencingToken: fencingToken,
		ExpiresAt:    expiresAt,
	}, nil
}

// Checkpoint persists state associated with the current lease. Returns ErrFenced
// if the record's fencing token no longer matches the stored lease.
func (p *postgresBackend) Checkpoint(ctx context.Context, record backend.LeaseRecord, state []byte, ttl time.Duration) error {
	// ===== STEP 1: Execute UPDATE =====
	ttlStr := fmt.Sprintf("%.6f seconds", ttl.Seconds())
	result, err := p.db.ExecContext(ctx, queryCheckpoint, state, ttlStr, record.WorkID, record.HolderID, record.FencingToken)
	if err != nil {
		return fmt.Errorf("postgres: Checkpoint: %w", err)
	}

	// ===== STEP 2: Check Rows Affected =====
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: Checkpoint: %w", err)
	}
	if n == 0 {
		return worklease.ErrFenced
	}

	return nil
}

// Renew extends the lease expiration time. Returns ErrFenced if the record's
// fencing token no longer matches the stored lease. Returns ErrLeaseExpired if
// the lease has already expired.
func (p *postgresBackend) Renew(ctx context.Context, record backend.LeaseRecord, ttl time.Duration) error {
	// ===== STEP 1: Execute UPDATE =====
	ttlStr := fmt.Sprintf("%.6f seconds", ttl.Seconds())
	result, err := p.db.ExecContext(ctx, queryRenew, ttlStr, record.WorkID, record.HolderID, record.FencingToken)
	if err != nil {
		return fmt.Errorf("postgres: Renew: %w", err)
	}

	// ===== STEP 2: Check Rows Affected =====
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: Renew: %w", err)
	}
	if n == 0 {
		// ===== STEP 3: Distinguish Fenced vs Expired =====
		var expiresAt time.Time
		err := p.db.QueryRowContext(ctx, queryRenewCheck, record.WorkID, record.FencingToken).Scan(&expiresAt)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return worklease.ErrFenced
			}
			return fmt.Errorf("postgres: Renew: %w", err)
		}
		return worklease.ErrLeaseExpired
	}

	return nil
}

// Release surrenders the lease. Returns ErrFenced if the record's fencing token
// no longer matches the stored lease.
func (p *postgresBackend) Release(ctx context.Context, record backend.LeaseRecord) error {
	// ===== STEP 1: Execute UPDATE =====
	result, err := p.db.ExecContext(ctx, queryRelease, record.WorkID, record.HolderID, record.FencingToken)
	if err != nil {
		return fmt.Errorf("postgres: Release: %w", err)
	}

	// ===== STEP 2: Check Rows Affected =====
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: Release: %w", err)
	}
	if n == 0 {
		return worklease.ErrFenced
	}

	return nil
}

// ReadCheckpoint retrieves persisted state and the clean handoff flag for the
// given lease. Returns nil, false, nil if no record exists for the workID.
// Returns ErrFenced if the record's fencing token no longer matches the stored lease.
func (p *postgresBackend) ReadCheckpoint(ctx context.Context, record backend.LeaseRecord) ([]byte, bool, error) {
	// ===== STEP 1: Query by work_id =====
	var storedToken uint64
	var state []byte
	var cleanHandoff bool
	err := p.db.QueryRowContext(ctx, queryReadCheckpoint, record.WorkID).Scan(&storedToken, &state, &cleanHandoff)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("postgres: ReadCheckpoint: %w", err)
	}

	// ===== STEP 2: Check Fencing Token =====
	if storedToken != record.FencingToken {
		return nil, false, worklease.ErrFenced
	}

	return state, cleanHandoff, nil
}
