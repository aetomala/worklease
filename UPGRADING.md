# Upgrading worklease

## v0.5.x → v0.6.0

### Breaking Changes

- **`Lease` interface gains `Forget`.** Any custom implementation of `worklease.Lease` (rare — the library's own `New()` is the only implementation most callers need) must add:

  ```go
  Forget(ctx context.Context, token Token) error
  ```

- **`backend.Backend` interface gains `Forget` and `Sweep`.** Any custom `backend.Backend` implementation (e.g. a third-party Redis or etcd backend) must add:

  ```go
  Forget(ctx context.Context, record LeaseRecord) error
  Sweep(ctx context.Context, opts SweepOptions) (int64, error)
  ```

  See "Migrate the PostgreSQL schema" below.

### 1. Migrate the PostgreSQL schema before deploying

Run this idempotent SQL against every database that holds `worklease_leases`. It is safe to run more than once.

```sql
ALTER TABLE worklease_leases ADD COLUMN IF NOT EXISTS exit_mode      TEXT;
ALTER TABLE worklease_leases ADD COLUMN IF NOT EXISTS prev_exit_mode TEXT NOT NULL DEFAULT 'expired';
ALTER TABLE worklease_leases ADD COLUMN IF NOT EXISTS prev_holder_id TEXT;
ALTER TABLE worklease_leases ALTER COLUMN prev_exit_mode SET DEFAULT 'none';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'worklease_leases_exit_mode_check'
          AND conrelid = 'worklease_leases'::regclass
    ) THEN
        ALTER TABLE worklease_leases
            ADD CONSTRAINT worklease_leases_exit_mode_check
            CHECK (exit_mode IS NULL OR exit_mode IN ('finished', 'abandoned', 'retired'))
            NOT VALID;
    END IF;
END $$;
ALTER TABLE worklease_leases VALIDATE CONSTRAINT worklease_leases_exit_mode_check;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'worklease_leases_prev_exit_mode_check'
          AND conrelid = 'worklease_leases'::regclass
    ) THEN
        ALTER TABLE worklease_leases
            ADD CONSTRAINT worklease_leases_prev_exit_mode_check
            CHECK (prev_exit_mode IN ('none', 'expired', 'finished', 'abandoned', 'retired'))
            NOT VALID;
    END IF;
END $$;
ALTER TABLE worklease_leases VALIDATE CONSTRAINT worklease_leases_prev_exit_mode_check;
```

Existing rows get `exit_mode = NULL` and `prev_exit_mode = 'expired'`. The v0.5 `clean_handoff` value may be stale, so it is not trusted: the next holder of each existing row sees `ExitExpired` once and re-validates its partial state. That costs at most some unnecessary work, never an incorrect resume. Because existing rows have no declared exit, default `Vacuum.Sweep` never removes a row last released before v0.6; only `IncludeExpired: true` does, and it also removes rows whose holder genuinely crashed. Without this migration, every v0.6 `Acquire` fails with a `column … does not exist` error.

`clean_handoff` stays in the table, unread and unwritten by v0.6, so a rollback to v0.5 still works. A later release drops it with its own step here.

### 2. Rolling upgrades

Fencing is unaffected while v0.5 and v0.6 processes share the table: both draw tokens from `worklease_fencing_seq` and fence on holder ID and token. Handoff information is degraded until the rollout finishes:

- A v0.6 successor of a v0.5 holder sees `ExitExpired`, because v0.5 never writes `exit_mode`. This is conservative.
- A v0.5 `Acquire` does not capture `prev_exit_mode` or clear `exit_mode`, so a later v0.6 successor can see the exit of the holder before the v0.5 one. This is the same class of error as in v0.5.
- A v0.5 successor of a v0.6 holder reads a frozen `clean_handoff`.
- **Do not run `Vacuum.Sweep` until no v0.5 process remains.** A v0.5 holder can reacquire a row whose `exit_mode` is still `'retired'`.

Finish the rollout promptly. To keep exact handoff information throughout, drain every v0.5 process before starting v0.6.

### New in v0.6.0

- `Lease.Forget(ctx, token) error` — permanently deletes a lease row. Returns `ErrFenced` if the token no longer matches the stored lease or if no row exists for the work ID. Unlike `Release`, the row is not left behind for a future `ReadCheckpoint`.
- `worklease.Vacuum` and `worklease.SweepOptions` — age-based bulk cleanup. `NewVacuum(b backend.Backend) *Vacuum`, then `v.Sweep(ctx, SweepOptions{Retention: ..., IncludeCrashed: ...})` deletes rows older than `Retention` that are not currently held. `Retention` must exceed the maximum TTL configured across all `Lease` clients sharing the backend — `Sweep` returns `ErrRetentionRequired` if `Retention <= 0`.
- `ErrRetentionRequired` — new sentinel, returned by `Vacuum.Sweep`.
- ADR-0016's retention component (`Forget` / `Vacuum.Sweep`) is now Accepted.
- ADR-0017 — schema migration remains caller-owned; ships as of this release.

Neither `Forget` nor `Sweep` invoke `LeaseObserver` — the observer's method set is unchanged in v0.6.0.

## v0.4.x → v0.5.0

### Breaking Changes

- **`Acquire` with `WithWaitForLease` returns `ctx.Err()` on cancellation/deadline,
  not `ErrLeaseHeld`.** Previously, when the wait loop observed `ctx.Done()` it
  returned the bare `ErrLeaseHeld` sentinel. It now returns an error wrapping
  `ctx.Err()` — `fmt.Errorf("worklease: acquire cancelled: %w", ctx.Err())` — which
  satisfies `errors.Is(err, context.Canceled)` or `errors.Is(err, context.DeadlineExceeded)`
  but **no longer** satisfies `errors.Is(err, worklease.ErrLeaseHeld)`.

  This is a **runtime** break — it is not caught by the compiler. Callers who used
  `WithWaitForLease` and treated `errors.Is(err, ErrLeaseHeld)` as their sole
  loop-termination or timeout signal must now also check `context.Canceled` and
  `context.DeadlineExceeded`:

  ```go
  _, err := lease.Acquire(ctx, workID, worklease.WithWaitForLease())
  switch {
  case err == nil:
      // acquired
  case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
      // wait was cancelled or timed out — previously surfaced as ErrLeaseHeld
  default:
      // other backend error
  }
  ```

  The synchronous, no-wait path (without `WithWaitForLease`) is unchanged — it still
  surfaces the backend `ErrLeaseHeld` directly.

### Breaking — Schema Migration Required

v0.5 sources fencing tokens from a global `worklease_fencing_seq` SEQUENCE. Both
`Acquire` paths call `nextval('worklease_fencing_seq')` directly — there is no
fallback. A database created under v0.4 has `fencing_token BIGINT NOT NULL DEFAULT 1`
and no sequence. The first `Acquire` after deploying v0.5 fails at runtime with:

```
pq: relation "worklease_fencing_seq" does not exist
```

Apply the following migration **before** deploying v0.5. All four statements are
idempotent and safe to run against a live v0.4 database:

```sql
CREATE SEQUENCE IF NOT EXISTS worklease_fencing_seq;
SELECT setval('worklease_fencing_seq', (SELECT COALESCE(MAX(fencing_token),0)+1 FROM worklease_leases));
ALTER TABLE worklease_leases ALTER COLUMN fencing_token SET DEFAULT nextval('worklease_fencing_seq');
CREATE INDEX IF NOT EXISTS idx_worklease_leases_updated_at ON worklease_leases (updated_at);
```

The `setval` step seeds the sequence above the highest token already in the table.
Without it the sequence starts at 1 and can re-issue token values ≤ existing per-row
tokens, silently defeating fencing for any rows that were active before the migration.

**Verify the migration:** after applying the SQL, run one `Acquire` for any work ID
and confirm the returned fencing token is strictly greater than the value returned by
`SELECT MAX(fencing_token) FROM worklease_leases` taken immediately before the
migration. If the table was empty before the migration, the first token issued will be
1 — this is correct.

### Behavioral Changes in v0.5.0

- **The renewal goroutine now retries transient errors instead of giving up.**
  Previously, a single non-fencing error from `Renew` cancelled the renewal context
  and stopped renewal. The goroutine now retries with exponential backoff (plus
  additive jitter), bounded strictly by the lease window: it stops retrying once
  `token.ExpiresAt()` is reached and cancels the renewal context with cause
  `ErrLeaseWindowExhausted` (inspect via `context.Cause(renewCtx)`). A fencing error
  is still never retried — it cancels immediately with cause `ErrFenced`. Configure
  the policy with `WithRenewalBackoff(initial, max, jitter)`; the defaults are
  100ms / 5s / 0.20.

### New in v0.5.0

- `worklease.WithRenewalBackoff(initial, max time.Duration, jitter float64)` — configures
  the renewal goroutine's bounded-retry backoff policy.
- `worklease.ErrLeaseWindowExhausted` — the cancel cause set on the renewal context when
  the lease window closes before a renewal succeeds; inspect via `context.Cause(renewCtx)`.
- `RenewEvent.Attempt` — the 1-based attempt counter delivered to `OnRenew`, incremented on
  each retry within the renewal goroutine. Direct `Renew` calls always report `Attempt: 1`.

## v0.3.x → v0.4.0

### Breaking Changes

- **`LeaseObserver` redesign — flat parameters replaced by event structs.** The five
  flat-parameter methods are replaced by six event-struct methods. Implementers must
  convert their method signatures:

  ```go
  // Before (v0.3)
  OnAcquire(ctx context.Context, workID string, token Token, err error)
  OnCheckpoint(ctx context.Context, token Token, size int, err error)
  OnRenew(ctx context.Context, token Token, err error)
  OnRelease(ctx context.Context, token Token, err error)
  OnFenced(ctx context.Context, token Token)

  // After (v0.4)
  OnAcquire(ctx context.Context, e AcquireEvent)
  OnCheckpoint(ctx context.Context, e CheckpointEvent)
  OnRenew(ctx context.Context, e RenewEvent)
  OnRelease(ctx context.Context, e ReleaseEvent)
  OnReadCheckpoint(ctx context.Context, e ReadCheckpointEvent) // new method
  OnFenced(ctx context.Context, e FencedEvent)
  ```

  The previous fields are now event-struct fields (e.g. `e.WorkID`, `e.Token`, `e.Size`,
  `e.Err`). Three behavioral changes accompany the redesign:
  - `OnReadCheckpoint` is new — it fires after every `ReadCheckpoint` attempt.
  - `OnFenced` now also fires on the `Release` path; previously it fired only after
    Checkpoint and Renew. `FencedEvent.Operation` identifies which operation triggered it.
  - Every operation event carries a `Duration` measuring the final backend call only —
    not the wait loop. For `Acquire` with `WithWaitForLease`, record your own timestamp if
    you need the cumulative wait duration.

  Callers who pass `nil` for `Config.Observer` are unaffected — the no-op substitution is
  unchanged.

- **`pool.New` returns distinct config sentinels.** The single `pool.ErrConfigInvalid`
  (whose message also changed to `"pool: invalid configuration"`) is replaced at the call
  site by `ErrNilLease`, `ErrEmptyWorkIDs`, and `ErrWithWaitForLeaseProhibited`. All three
  satisfy `errors.Is(err, pool.ErrConfigInvalid)`, so switch any exact-string or `==`
  checks to `errors.Is`.

- **`pool.Pool.Run` returns `ErrAllSlotsDead`.** When every slot exits via a
  `PermanentError`, `Run` now returns `pool.ErrAllSlotsDead` instead of `nil`. Callers that
  treated a `nil` return as "pool drained cleanly" should distinguish the two: `nil` means
  the context was cancelled; `ErrAllSlotsDead` means all slots died permanently.

### New in v0.4.0

- `pool.Permanent(err error) error` — wraps an error so it satisfies `PermanentError`
  without defining a custom type.
- `pool.Observer` — slot lifecycle callbacks (`OnSlotAcquired`, `OnSlotLost`,
  `OnSlotBackoff`, `OnSlotDead`), injected via `pool.Config.Observer`; nil is a no-op.
- `leader.Config` callbacks — `OnElected`, `OnLost`, `OnRelinquished`; all optional, nil is
  a no-op.
- `backend/conformance` — `RunSuite` enforces memory-vs-Postgres parity; relevant only if
  you implement a custom `backend.Backend`.
- `examples/observability` — a stdlib-only `LeaseObserver` reference implementation.

## v0.1.x → v0.2.0

No breaking changes. All existing code compiles without modification.

### New in v0.2.0

- `worker.Runner` — optional lifecycle manager; no changes to the `Lease` interface required
- `checkpoint` subpackage — optional encoding helpers; no changes to `Checkpoint`/`ReadCheckpoint` signatures
- `LeaseObserver` — wired via `Config.Observer`; zero value (nil) is a silent no-op
- `memory.WithClock` — injectable clock for the in-memory backend; `memory.New()` with no args is unchanged

## v0.2.x → v0.3.0

### Breaking Changes

- **`checkpoint.Codec` method rename** — `Encode` → `Marshal`, `Decode` → `Unmarshal`.
  Callers who implement `Codec` directly must rename their method implementations. Callers
  using only `checkpoint.JSON()` are unaffected — `JSONCodec` is updated. The package-level
  generic helpers `Encode[T]` and `Decode[T]` are unchanged.

### New in v0.3.0

- `worklease.HasWaitForLease` — reports whether a `[]AcquireOption` slice includes `WithWaitForLease`
- `leader` package — `leader.Elect` runs a function under managed lease acquisition and renewal
- `pool` package — `pool.Pool` distributes a fixed set of work IDs across competing processes
- `leader.Config.BackoffInterval` — optional duration `Elect` sleeps before returning on
  non-fencing paths; set this when wrapping `Elect` in a retry loop to prevent rapid
  acquire/release/reacquire cycling

### Behavioral Changes in v0.3.0

- **`Release` expires the lease immediately.** Previously, a cleanly-released lease remained
  inaccessible to successors until the full TTL elapsed. Now `Release` sets `expires_at` to
  one millisecond in the past, allowing an immediate successor `Acquire`. Callers with retry
  loops that call `Acquire` immediately after `Release` may now see the lease acquired before
  the retry loop fires — this is the intended behavior. Callers who depended on the TTL gap
  as an incidental rate limiter should add an explicit backoff (see `leader.Config.BackoffInterval`).
