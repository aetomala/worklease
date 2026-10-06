# Upgrading worklease

## v0.5.x → v0.6.0

v0.6 replaces the `clean_handoff` flag with declared exit modes (ADR-0018). It needs a schema migration and code changes in every caller of `Release`, `ReadCheckpoint`, and the `worker`/`pool` work functions.

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

### 3. Update `Release` calls

`Release` takes a required exit mode. Choose the one that tells the next holder what happened:

| Mode | Use when | The next holder should |
|---|---|---|
| `worklease.ExitFinished` | The run completed; the checkpoint is final state for this run, and the work ID will be acquired again (a shard, a recurring job) | Continue from the final state |
| `worklease.ExitAbandoned` | You stopped deliberately without completing: an error, a cancellation, a shutdown | Validate the partial state, then resume |
| `worklease.ExitRetired` | The work ID is done for good | Not redo the work |

```go
// Before
defer lease.Release(ctx, token)

// After: choose the mode from the outcome.
mode := worklease.ExitFinished
if workErr != nil {
    mode = worklease.ExitAbandoned
}
err := lease.Release(cleanupCtx, token, mode)
```

Rows released with `ExitFinished` are kept as resume points and are never swept. Release one-shot work with `ExitRetired`, or the table grows without bound.

`Release` now returns `ErrLeaseExpired` once the lease has expired, even when no successor has acquired it, and records nothing; the next holder sees `ExitExpired`. Treat it as "my exit was not recorded". A second `Release` with the same token also returns `ErrLeaseExpired`; the first declared mode stands. After `Release`, `Checkpoint` returns `ErrLeaseExpired` without writing, so stop any background checkpoint loop when it sees that error.

### 4. Update `ReadCheckpoint` calls

```go
// Before
state, cleanHandoff, err := lease.ReadCheckpoint(ctx, token)

// After
prior, err := lease.ReadCheckpoint(ctx, token)
state := prior.State
switch prior.PrevExit {
case worklease.ExitNone:
    // No previous holder: start fresh.
case worklease.ExitFinished:
    // Continue from the previous run's final state.
case worklease.ExitRetired:
    // Done for good: do not redo the work.
default:
    // ExitAbandoned, ExitExpired, or a mode this code does not know: validate partial state.
}
```

Treat any mode your code does not recognize as `ExitExpired`; later releases may add modes. `ReadCheckpoint` now returns `ErrFenced` on PostgreSQL when the row does not exist (for example after `Forget` or `Sweep`), matching the memory backend.

### 5. Update work functions

```go
// worker: before
WorkFn: func(ctx context.Context, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error)
// worker: after
WorkFn: func(ctx context.Context, token worklease.Token, prior worklease.Checkpoint) ([]byte, error)

// pool: before
func(ctx context.Context, workID string, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error)
// pool: after
func(ctx context.Context, workID string, token worklease.Token, prior worklease.Checkpoint) ([]byte, error)
```

`worker.Runner`, `leader.Elect`, and `pool` choose the exit mode from the work function's result: `nil` → `ExitFinished`; an error wrapping `worklease.ErrRetire` → `ExitRetired` and success; any other error → `ExitAbandoned`. They do not release when fenced, when the lease window ran out, or, for `worker.Runner` and `pool`, when `ReadCheckpoint` failed. A `pool.PermanentError` releases with `ExitAbandoned`. The final `Checkpoint` and `Release` now run on a context that survives cancellation of yours, bounded by the new `CleanupTimeout` field (default 5s).

### 6. Other changes

- `leader.Config.OnRelinquished` fires only after a successful release with `ExitFinished` or `ExitRetired`. When `fn` succeeded, `Elect` wraps a non-fenced release error as `leader: release: …`; when `fn` failed, it returns `fn`'s error.
- `pool.Config.BackoffInterval` zero now means 1s, not immediate retry; set a small positive value if you relied on immediate retry. A slot whose work ID a peer holds waits the new `IdleInterval` (default 1s, up to 20% jitter) and no longer triggers `OnSlotBackoff`. A slot whose work function returned `nil` waits the new `RerunInterval` (default 1s, up to 20% jitter) before running again; set it lower for back-to-back batches, or loop inside the work function. Custom `pool.Observer` implementations must add `OnSlotRetired`. `pool.Run` returns `nil` when at least one slot retired.
- `ErrLeaseExpired` from `Renew` ends renewal immediately. `context.Cause(renewCtx)` matches both `ErrLeaseWindowExhausted` and `ErrLeaseExpired`.
- Observers: `ReleaseEvent.Mode` is new; `ReadCheckpointEvent.CleanHandoff` is replaced by `PrevExit` and `PrevHolderID`.
- Custom `backend.Backend` implementations: change `Release` and `ReadCheckpoint` as above, capture the previous exit and holder in `Acquire` atomically with token issuance, and pass `conformance.RunSuite`.

### 7. New in v0.6.0

- `Lease.Forget(ctx, token) error` — permanently deletes a lease row. Returns `ErrFenced` if the token no longer matches the stored lease or if no row exists for the work ID.
- `worklease.Vacuum` and `worklease.SweepOptions` — `NewVacuum(b).Sweep(ctx, SweepOptions{Retention: …, IncludeExpired: …})` deletes rows released with `ExitRetired`, and with `IncludeExpired` also rows whose lease expired with no declared exit, once they are older than `Retention`. Held rows are never deleted. `Sweep` returns `ErrRetentionRequired` if `Retention <= 0`.
- The `Lease` and `backend.Backend` interfaces gain `Forget` (and `Backend` gains `Sweep`); custom implementations must add them:

  ```go
  Forget(ctx context.Context, token Token) error              // Lease
  Forget(ctx context.Context, record LeaseRecord) error       // Backend
  Sweep(ctx context.Context, opts SweepOptions) (int64, error) // Backend
  ```

- Neither `Forget` nor `Sweep` invokes `LeaseObserver`.
- ADR-0016's retention component and ADR-0017 (schema migration remains caller-owned) are Accepted; ADR-0018 (explicit exit modes) is Accepted.

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
