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
INSERT INTO worklease_leases (work_id, holder_id, fencing_token, expires_at, checkpoint, exit_mode, prev_exit_mode, prev_holder_id)
VALUES ($1, $2, nextval('worklease_fencing_seq'), NOW() + $3, NULL, NULL, 'none', NULL)
ON CONFLICT (work_id) DO UPDATE
SET holder_id      = EXCLUDED.holder_id,
    fencing_token  = nextval('worklease_fencing_seq'),
    expires_at     = EXCLUDED.expires_at,
    checkpoint     = worklease_leases.checkpoint,
    prev_exit_mode = COALESCE(worklease_leases.exit_mode, 'expired'),
    prev_holder_id = worklease_leases.holder_id,
    exit_mode      = NULL,
    updated_at     = NOW()
WHERE worklease_leases.expires_at < NOW()
RETURNING fencing_token, expires_at`

	queryCheckpoint = `
UPDATE worklease_leases
SET checkpoint = $1,
    expires_at = NOW() + $2,
    updated_at = NOW()
WHERE work_id       = $3
  AND holder_id     = $4
  AND fencing_token = $5
  AND exit_mode IS NULL`

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
  AND holder_id     = $2
  AND fencing_token = $3`

	queryRelease = `
UPDATE worklease_leases
SET exit_mode  = $4,
    expires_at = NOW() - INTERVAL '1 millisecond',
    updated_at = NOW()
WHERE work_id       = $1
  AND holder_id     = $2
  AND fencing_token = $3
  AND expires_at    > NOW()`

	// The queryHolderCheck query classifies a zero-row Checkpoint or Release: no
	// row means fenced; a row means the holder declared an exit or the lease expired.
	queryHolderCheck = `
SELECT 1
FROM worklease_leases
WHERE work_id       = $1
  AND holder_id     = $2
  AND fencing_token = $3`

	queryReadCheckpoint = `
SELECT fencing_token, checkpoint, prev_exit_mode, prev_holder_id
FROM worklease_leases
WHERE work_id = $1`

	queryForget = `
DELETE FROM worklease_leases
WHERE work_id       = $1
  AND holder_id     = $2
  AND fencing_token = $3`

	queryVacuumSweep = `
DELETE FROM worklease_leases
WHERE updated_at < NOW() - $1::interval
  AND expires_at < NOW()
  AND (exit_mode = 'retired' OR ($2 AND exit_mode IS NULL))`
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
// if the lease for this workID is held and has not expired. Returns a LeaseRecord with the newly
// acquired lease details on success.
func (p *postgresBackend) Acquire(ctx context.Context, workID, holderID string, ttl time.Duration) (backend.LeaseRecord, error) {
	// ===== STEP 1: Execute INSERT/UPDATE with RETURNING =====
	ttlStr := fmt.Sprintf("%.6f seconds", ttl.Seconds())
	var fencingToken uint64
	var expiresAt time.Time
	err := p.db.QueryRowContext(ctx, queryAcquire, workID, holderID, ttlStr).Scan(&fencingToken, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		// No row returned — the lease is held by another, unexpired holder.
		return backend.LeaseRecord{}, worklease.ErrLeaseHeld
	}
	if err != nil {
		return backend.LeaseRecord{}, fmt.Errorf("postgres: Acquire: exec failed: %w", err)
	}

	// ===== STEP 2: Return LeaseRecord =====
	return backend.LeaseRecord{
		WorkID:       workID,
		HolderID:     holderID,
		FencingToken: fencingToken,
		ExpiresAt:    expiresAt,
	}, nil
}

// Checkpoint persists state and extends the lease. Returns ErrFenced if the
// record no longer matches the stored lease, and ErrLeaseExpired, without
// writing, if the holder has already declared an exit with Release.
func (p *postgresBackend) Checkpoint(ctx context.Context, record backend.LeaseRecord, state []byte, ttl time.Duration) error {
	// ===== STEP 1: Write State and Extend the Lease =====
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
	if n > 0 {
		return nil
	}

	// ===== STEP 3: Classify Zero Rows — Fenced or Exit Declared =====
	var one int
	err = p.db.QueryRowContext(ctx, queryHolderCheck, record.WorkID, record.HolderID, record.FencingToken).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return worklease.ErrFenced
	}
	if err != nil {
		return fmt.Errorf("postgres: Checkpoint: %w", err)
	}

	return worklease.ErrLeaseExpired
}

// Renew extends the lease expiration time. Returns ErrFenced if the record's
// holder ID or fencing token no longer matches the stored lease. Returns ErrLeaseExpired if
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
		err := p.db.QueryRowContext(ctx, queryRenewCheck, record.WorkID, record.HolderID, record.FencingToken).Scan(&expiresAt)
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

// Release records mode as the holder's declared exit and expires the lease
// immediately. Returns ErrInvalidExitMode for an undeclarable mode, ErrFenced
// if the record no longer matches the stored lease, and ErrLeaseExpired if it
// matches but has expired.
func (p *postgresBackend) Release(ctx context.Context, record backend.LeaseRecord, mode backend.ExitMode) error {
	// ===== STEP 1: Validate Mode =====
	if mode != backend.ExitFinished && mode != backend.ExitAbandoned && mode != backend.ExitRetired {
		return worklease.ErrInvalidExitMode
	}

	// ===== STEP 2: Record Exit and Expire Immediately =====
	result, err := p.db.ExecContext(ctx, queryRelease, record.WorkID, record.HolderID, record.FencingToken, mode.String())
	if err != nil {
		return fmt.Errorf("postgres: Release: %w", err)
	}

	// ===== STEP 3: Check Rows Affected =====
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: Release: %w", err)
	}
	if n > 0 {
		return nil
	}

	// ===== STEP 4: Classify Zero Rows — Fenced or Expired =====
	var one int
	err = p.db.QueryRowContext(ctx, queryHolderCheck, record.WorkID, record.HolderID, record.FencingToken).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return worklease.ErrFenced
	}
	if err != nil {
		return fmt.Errorf("postgres: Release: %w", err)
	}

	return worklease.ErrLeaseExpired
}

// ReadCheckpoint returns the checkpoint state and the immediately previous
// holder's exit. Returns ErrFenced if no row exists for record.WorkID or if
// record.FencingToken does not match the stored lease.
func (p *postgresBackend) ReadCheckpoint(ctx context.Context, record backend.LeaseRecord) (backend.Checkpoint, error) {
	// ===== STEP 1: Read the Row =====
	var (
		storedToken  uint64
		state        []byte
		prevExitText string
		prevHolderID sql.NullString
	)
	err := p.db.QueryRowContext(ctx, queryReadCheckpoint, record.WorkID).Scan(&storedToken, &state, &prevExitText, &prevHolderID)
	if errors.Is(err, sql.ErrNoRows) {
		return backend.Checkpoint{}, worklease.ErrFenced
	}
	if err != nil {
		return backend.Checkpoint{}, fmt.Errorf("postgres: ReadCheckpoint: %w", err)
	}

	// ===== STEP 2: Fence on Token =====
	if storedToken != record.FencingToken {
		return backend.Checkpoint{}, worklease.ErrFenced
	}

	// ===== STEP 3: Return Checkpoint =====
	return backend.Checkpoint{
		State:        state,
		PrevExit:     exitModeFromSQL(prevExitText),
		PrevHolderID: prevHolderID.String,
	}, nil
}

// exitModeFromSQL maps a stored prev_exit_mode value to an ExitMode. Unknown
// values map to ExitExpired, the conservative interpretation.
func exitModeFromSQL(s string) backend.ExitMode {
	switch s {
	case backend.ExitNone.String():
		return backend.ExitNone
	case backend.ExitFinished.String():
		return backend.ExitFinished
	case backend.ExitAbandoned.String():
		return backend.ExitAbandoned
	case backend.ExitRetired.String():
		return backend.ExitRetired
	default:
		return backend.ExitExpired
	}
}

// Forget permanently deletes the row identified by record. Returns ErrFenced
// if record.HolderID or record.FencingToken no longer matches the stored
// lease, or if no row exists for record.WorkID.
func (p *postgresBackend) Forget(ctx context.Context, record backend.LeaseRecord) error {
	result, err := p.db.ExecContext(ctx, queryForget, record.WorkID, record.HolderID, record.FencingToken)
	if err != nil {
		return fmt.Errorf("postgres: Forget: %w", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: Forget: %w", err)
	}
	if n == 0 {
		return worklease.ErrFenced
	}

	return nil
}

// Sweep deletes rows older than opts.Retention that are not currently held,
// returning the number of rows deleted. Does not validate opts.Retention —
// callers use worklease.Vacuum.Sweep, which validates before calling this.
func (p *postgresBackend) Sweep(ctx context.Context, opts backend.SweepOptions) (int64, error) {
	retentionStr := fmt.Sprintf("%.6f seconds", opts.Retention.Seconds())
	result, err := p.db.ExecContext(ctx, queryVacuumSweep, retentionStr, opts.IncludeExpired)
	if err != nil {
		return 0, fmt.Errorf("postgres: Sweep: %w", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("postgres: Sweep: %w", err)
	}

	return n, nil
}
