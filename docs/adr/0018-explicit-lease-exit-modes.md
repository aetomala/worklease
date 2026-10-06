# ADR-0018: Lease holders record an explicit exit mode; successors read the previous holder's exit

**Status:** Accepted
**Date:** 2026-10-02
**Amends:** ADR-0012 (Release semantics), ADR-0016 (retention component)
**Affects:** ADR-0007 (observer events), ADR-0010 / ADR-0011 (work function signatures), ADR-0015 (conformance), ADR-0017 (schema migration)
**Issues:** #73, #74, #75, #76, #79, #80, #81, #84

## Context

Through v0.6, a successor learns how the previous holder left through one row flag, `clean_handoff`. `Release` sets it to `TRUE`; `Checkpoint` resets it to `FALSE`. `Acquire` carries it forward unchanged. ARCHITECTURE promises successors that `true` means "the checkpoint contains the final state of completed work".

The v0.6.0 pre-release audit found that this model cannot keep that promise:

- **The flag is not about the immediately previous holder (#73).** Suppose A releases (`TRUE`), then B acquires and crashes before checkpointing. C, the next holder, reads `TRUE`. The flag reports the last holder that released or checkpointed, not the one C actually replaced.
- **Retention deletes crash data (#74).** Default `Vacuum.Sweep` keeps rows whose flag is `FALSE`. Because the flag carries over, B's crashed row in the sequence above is swept.
- **Release is not completion (#79, #80).** `worker.Runner`, `leader.Elect`, and the README quickstart call `Release` on failure paths, after the renewal window is exhausted, and on shutdown. Each of these hands off as "clean", with partial or no work done.
- **Released is not retired (#76).** `pool`, `worker`, and `leader` call `Release` at the end of every cycle, and successors resume from the released checkpoint. Default `Sweep` deletes those resume points.
- **A boolean cannot express what callers need.** "No previous holder" and "previous holder crashed before its first checkpoint" both read as `state=nil, cleanHandoff=false`. A crash and a deliberate abandon look the same. Nothing can say "this work ID is done for good".

worklease makes no API-stability promise before 1.0. This is the last window in which the handoff model can change without a major version.

## Decision

### 1. Holders declare how they exit

`Release` takes a required exit mode:

```go
// package worklease
Release(ctx context.Context, token Token, mode ExitMode) error
```

| Mode | Declared by | Meaning to the successor |
|---|---|---|
| `ExitFinished` | `Release(…, ExitFinished)` | The run completed. The checkpoint is final state for that run, and the work ID may be acquired again (a pool shard, a recurring job). |
| `ExitAbandoned` | `Release(…, ExitAbandoned)` | The holder stopped deliberately without completing — on error, cancellation, or shutdown. The checkpoint is partial state; validate it before resuming. |
| `ExitRetired` | `Release(…, ExitRetired)` | The work ID is complete permanently. A successor should not redo the work. The row is eligible for default `Sweep`. |
| `ExitExpired` | *inferred* | The lease expired with no recorded exit — a crash, a partition, or a renewal window that ran out. The checkpoint is partial state, and external effects may have fired after it. |
| `ExitNone` | *inferred* | There was no previous holder: the work ID was never acquired, or its row was removed by `Forget` or `Sweep`. |

`ExitMode` is a `uint8` with a `String()` method. Its zero value is `ExitNone`.

**Unknown modes are treated as `ExitExpired`.** This is a documented, permanent rule: any mode a caller does not recognize must be handled as expired, which is the conservative interpretation. It lets a mode be added after 1.0 without silently changing the meaning of existing caller code.

`Release` accepts only `ExitFinished`, `ExitAbandoned`, or `ExitRetired`. Any other value returns `ErrInvalidExitMode` without calling the backend. Backends apply the same check first, without side effects, so a direct `Backend.Release` call cannot store an undeclarable mode.

Every mode keeps ADR-0012's behavior: `Release` expires the lease immediately, so a successor acquires without waiting for the TTL. A retired work ID can still be acquired. The new holder sees `PrevExit == ExitRetired` and decides what to do.

`ExitAbandoned` releases immediately, the same as every other mode. A work function that fails persistently is therefore retried as fast as the next acquirer polls. This release does not add an exit-specific delay. For `pool`, the bound is `IdleInterval`, `RerunInterval`, and `BackoffInterval`, each defaulting to 1s. `leader.Elect` callers are paced by `leader.Config.BackoffInterval`, which applies on every non-fencing return after a successful `Acquire`, including `ExitAbandoned`. Direct `Lease` and `worker.Runner` callers own their retry pacing, as described in the ARCHITECTURE residual risk "leader.Elect retry loops have no built-in backoff".

### 2. A lapsed holder cannot record an exit

`Release`, in any mode, returns `ErrLeaseExpired` once the lease has expired, even if no successor has acquired it yet. A holder that lost its lease cannot vouch for how the work ended, so the successor sees `ExitExpired`. The checks run in order: fencing first (`ErrFenced`), then expiry (`ErrLeaseExpired`). This matches `Renew`.

`Checkpoint` returns `ErrLeaseExpired`, without writing, once the holder has declared an exit with `Release`; a declared exit cannot be undone by a late write. A slow but live holder whose lease lapsed with no declared exit and no successor may still revive it by checkpointing. After any declared exit, every further write from that holder — `Checkpoint`, `Release`, `Renew` — returns `ErrLeaseExpired`. `Forget` is unchanged.

### 3. The previous holder's exit is captured at Acquire

The row stores the current holder's exit separately from the previous holder's. `clean_handoff` is no longer read or written.

| Column | Type | Set by | Meaning |
|---|---|---|---|
| `exit_mode` | `TEXT NULL`, `CHECK IN ('finished','abandoned','retired')` | `Release` | The current or most recent holder's declared exit; `NULL` while held or if none was declared |
| `prev_exit_mode` | `TEXT NOT NULL DEFAULT 'none'`, `CHECK IN ('none','expired','finished','abandoned','retired')` | `Acquire` | How the immediately previous holder exited |
| `prev_holder_id` | `TEXT NULL` | `Acquire` | The immediately previous holder's ID; `NULL` when `prev_exit_mode = 'none'` |

`Acquire`, on the conflict path where an existing expired row is reacquired, captures the previous holder and resets the current exit atomically, in the same statement that draws the fencing token:

```sql
prev_exit_mode = COALESCE(worklease_leases.exit_mode, 'expired'),
prev_holder_id = worklease_leases.holder_id,
exit_mode      = NULL
```

On the insert path, a new row gets `prev_exit_mode = 'none'` and `prev_holder_id = NULL`.

`Checkpoint` never writes an exit column; it reads `exit_mode` only to refuse writes after a declared exit. The old "Checkpoint resets the flag" rule existed only because one flag was shared between the current and the previous holder.

The values are stored as text for operator readability. Other backends — memory now, Redis or etcd later — must perform the same copy-and-reset atomically with token issuance, using a Lua script or a transaction. Conformance enforces this.

### 4. Successors read a `Checkpoint` value

```go
// package backend (canonical); package worklease re-exports it as a type alias
type Checkpoint struct {
	State        []byte   // Last checkpointed state; nil if none was ever written.
	PrevExit     ExitMode // How the immediately previous holder exited.
	PrevHolderID string   // Previous holder's ID; empty when PrevExit is ExitNone.
}

ReadCheckpoint(ctx context.Context, token Token) (Checkpoint, error) // Lease
ReadCheckpoint(ctx context.Context, record LeaseRecord) (Checkpoint, error) // Backend
```

`PrevExit` and `PrevHolderID` are stable for the whole lease: repeated reads return the same values. `State` reflects the holder's own checkpoints once it writes any. Fields may be added to `Checkpoint` later without breaking callers.

`ExitMode` and `Checkpoint` are defined in `package backend` and re-exported from `package worklease` as type aliases, with the constants re-declared. This follows the precedent of `SweepOptions`: `backend` cannot import `worklease`.

`ExitMode.String()` returns the SQL text value — `"none"`, `"finished"`, `"abandoned"`, `"retired"`, `"expired"` — and formats any other value as `ExitMode(<n>)`. The memory backend stores a SQL `NULL` (no declared exit) as `ExitNone` in its current-exit field. `ExitNone` is never declarable, so the encoding is unambiguous.

### 5. Work functions receive the `Checkpoint`; `ErrRetire` retires the work ID

The `worker.WorkFn` and `pool.WorkFn` signatures replace `(prior []byte, cleanHandoff bool)` with `(prior worklease.Checkpoint)`. Their return type stays `([]byte, error)`.

A new root sentinel, `worklease.ErrRetire`, lets a work function declare permanent completion. It is defined in the root package so that `worker`, `pool`, and `leader` all honor it without importing one another.

`worker.Runner.Run` maps outcomes to exits:

| Outcome | Final state | Exit recorded | `Run` returns |
|---|---|---|---|
| `WorkFn` returns `nil` | checkpointed | `ExitFinished` | `nil` |
| `WorkFn` returns an error wrapping `ErrRetire` | checkpointed | `ExitRetired` | `nil` |
| `WorkFn` returns another non-fencing error, including after parent-context cancellation | checkpointed | `ExitAbandoned` | the error |
| `ReadCheckpoint` fails with a non-fencing error | — | none (the work function never saw the state; the successor sees `ExitExpired` after the TTL) | the wrapped error |
| Fenced, from `WorkFn`, `Checkpoint`, or renewal | — | none (a successor already holds the lease) | `ErrFenced` |
| `renewCtx` cancelled with `ErrLeaseWindowExhausted` (including the #84 join with `ErrLeaseExpired`) | — | none (lease lapsed; successor sees `ExitExpired`) | the `WorkFn` error, or the wrapped cause if `WorkFn` returned `nil` |
| `WorkFn` panics | — | none (successor sees `ExitExpired` after the TTL) | the panic propagates |
| The final `Checkpoint` fails with a non-fencing error | not stored | `ExitAbandoned` | `worker: checkpoint: …` if `WorkFn` succeeded, else the `WorkFn` error |
| `Release` returns `ErrLeaseExpired` after `WorkFn` returned `nil` or `ErrRetire` | checkpointed | none (successor sees `ExitExpired`) | `worker: release: …` wrapping `ErrLeaseExpired` |
| `Release` returns `ErrLeaseExpired` after `WorkFn` returned another error | checkpointed | none (successor sees `ExitExpired`) | the `WorkFn` error |
| `Release` returns `ErrFenced` | checkpointed | none (a successor holds the lease) | `ErrFenced` |

The final `Checkpoint` and the `Release` on these paths run under a cleanup context, `context.WithTimeout(context.WithoutCancel(ctx), CleanupTimeout)`. It survives cancellation of the caller's context but cannot hang shutdown. `CleanupTimeout` is a new field on `worker.RunnerConfig`, `leader.Config`, and `pool.Config`; zero means 5s (#80).

`leader.Elect` applies the same mapping to `fn`'s return value. Precedence is fenced, then lease window exhausted, then `ErrRetire`, then `nil`, then any other error. `OnRelinquished` fires after a successful `Release` with `ExitFinished` or `ExitRetired`; it does not fire for `ExitAbandoned`.

`pool` treats `ErrRetire` as successful, permanent completion of a slot:
- The slot goroutine exits without reacquiring and without backoff.
- `pool.Observer` gains `OnSlotRetired(ctx, SlotRetiredEvent{WorkID})`. `OnSlotDead` stays reserved for `PermanentError`.
- A slot whose work function returned `nil` waits `RerunInterval` (default 1s, up to 20% jitter) before reacquiring, so a finished slot neither spins nor starves peer processes.
- `Run` returns once every slot has exited, whether retired or dead, or when `ctx` is cancelled.
- `Run` returns `ErrAllSlotsDead` only if every slot exited through `PermanentError`. If at least one slot retired, it returns `nil`.

### 6. Retention keys on the declared exit

`Backend.Sweep` deletes rows that are expired, older than `Retention`, and either:
- have `exit_mode = 'retired'` — always; or
- have `exit_mode IS NULL`, meaning no declared exit (expired) — only when `IncludeExpired` is set.

`SweepOptions.IncludeCrashed` is renamed `IncludeExpired`. `Sweep` has not shipped in a tag, so the rename breaks no released API.

`ExitFinished` and `ExitAbandoned` rows are never deleted by `Sweep`. They are the resume points that successors read.

Rows released with `ExitFinished` are kept indefinitely because they are resume points for recurring work (pool shards, scheduled jobs). Callers running one-shot work that should not be redone must release with `ExitRetired`; otherwise the row is never eligible for `Sweep` and the table grows without bound. The README quickstart and the `ExitFinished` godoc must state this.

`Retention` is how long a retired or expired row is kept after its last update: an idempotency window against re-running retired work, an audit window, and a grace period in which expired work can be recovered. It is not tied to the TTL. Held rows are never eligible, whatever `Retention` is.

### 7. Observer events

- `ReleaseEvent` gains `Mode ExitMode`.
- `ReadCheckpointEvent` replaces `CleanHandoff bool` with `PrevExit ExitMode` and `PrevHolderID string`.

## Migration (ADR-0017)

Idempotent SQL to apply before deploying v0.6:

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

The whole block was run twice in a row against PostgreSQL 16, and the second run succeeded.

**Existing rows map conservatively.** Under v0.5, `clean_handoff = TRUE` may be stale (#73), so it is not trusted. Every existing row starts with `exit_mode = NULL` and `prev_exit_mode = 'expired'`. The next successor of each row re-validates its partial state once. That is unnecessary work at most, never an incorrect resume.

The `clean_handoff` column is left in place, unread and unwritten by v0.6, so that a rollback to v0.5 does not fail. It is dropped in a later release, with its own UPGRADING step.

**Rolling upgrades are supported, with degraded handoff accuracy during the window.** While v0.5 and v0.6 processes share the table:

- **Fencing is unaffected.** Both versions draw tokens from `worklease_fencing_seq` and fence on holder ID and token.
- **A v0.6 successor of a v0.5 holder** sees `ExitExpired`, because v0.5's `Release` never writes `exit_mode`. This is conservative.
- **A v0.5 `Acquire`** does not reset `exit_mode` or capture `prev_*`. A later v0.6 successor of that v0.5 holder can therefore inherit the exit declared by the v0.6 holder *before* it. That is the same class of error as #73, so the window is no worse than v0.5.
- **A v0.5 successor of a v0.6 holder** reads the frozen `clean_handoff` value. Again, no worse than v0.5.
- **`Sweep` must not run until no v0.5 process remains.** A v0.5 holder can reacquire a row whose `exit_mode` is still `'retired'`.

`UPGRADING.md` documents this window and recommends completing the rollout promptly. Operators who need exact handoff information throughout can drain all v0.5 processes before starting v0.6.

Every schema copy must be updated in the same change: `schema.sql`, the README quickstart DDL, the ARCHITECTURE schema block, and the test bootstrap. The CHANGELOG records this under `### Breaking`, with the failure mode if the migration is skipped. Because `Acquire` writes `prev_exit_mode`, every `Acquire` fails with `column "prev_exit_mode" … does not exist`.

## Rationale

**An explicit, required exit mode replaces an implicit flag.** The defects behind #73 and #79 came from `Release` meaning one thing to the code (lease surrendered) and another to successors (work completed). Requiring `mode` at every call site makes `defer lease.Release(ctx, token)` impossible to write without choosing, in line with ARCHITECTURE's "Explicit over implicit" principle. A variadic default of `ExitFinished` was rejected because it recreates the README-quickstart bug.

**Five modes cover every distinction a successor can act on.** Each mode leads to a different successor action:

| Mode | Successor action |
|---|---|
| `None` | Start fresh |
| `Finished` | Continue from final state |
| `Abandoned` | Validate partial state; the holder stopped cleanly |
| `Expired` | Validate partial state; effects may have fired after the checkpoint |
| `Retired` | Do not redo the work |

The audit's reproductions are all expressible: a crash before the first checkpoint is `Expired` with nil state, no longer confusable with `None`. Abandoned and expired are distinct so that operators can separate deliberate-failure rates from crash and lost-lease rates.

**Capturing at `Acquire` is the only place that knows the immediately previous holder.** The row is the one place every process can read. Snapshotting `exit_mode` into `prev_exit_mode` in the token-issuing statement makes the capture atomic with ownership transfer. Because `RETURNING` sees the new `prev_*` values, a later version can also return them from `Acquire` without a schema change or Postgres 18's `RETURNING OLD`.

**Retention follows declared intent, not lease mechanics.** `Release` is the routine handoff for `pool`, `worker`, and `leader`, so it cannot also mean "delete me". `ExitRetired` lets callers state the one fact `Sweep` needs. It also gives `Forget` a delayed, idempotency-preserving counterpart: after `Forget`, a duplicate job sees `None` and re-runs; after `Retire`, it sees `Retired`.

**A lapsed holder's word is not trusted.** Once expired, the holder cannot know whether its last step completed relative to any successor. Refusing its exit keeps the record at `Expired`, which is the conservative answer.

**`Checkpoint` never writes an exit.** Separating the current exit from the previous exit removes the need for `Checkpoint` to reset a flag. `Checkpoint` means "state plus lease extension, atomically", and it refuses once the holder has declared an exit, so the successor reads the exit that was actually declared. Clearing the exit on a late checkpoint was rejected: it would revive a released lease, delay the successor by a TTL, and downgrade a declared `ExitFinished` to `ExitExpired`.

## Consequences

**Positive:**
- `PrevExit` truthfully describes the immediately previous holder on every path the audit reproduced (#73, #79, #80).
- Default `Sweep` cannot delete crash-recovery data or resume points (#74, #76). `Retention` gets a definition that matches its use.
- Successors can distinguish a first acquisition from a crash before the first checkpoint.
- Future fields on `Checkpoint` are additive. A future mode is safe through the unknown-means-expired rule.

**Negative:**
- This is a breaking change across the public surface:
  - `Lease.Release` and `Lease.ReadCheckpoint`
  - `Backend.Release` and `Backend.ReadCheckpoint`
  - `worker.WorkFn` and `pool.WorkFn`
  - `ReleaseEvent` and `ReadCheckpointEvent`
  - `SweepOptions.IncludeCrashed`, renamed `IncludeExpired`
  - `pool.Observer`, which gains `OnSlotRetired`
  - every conformance spec that touches handoff
- A schema migration is required, handoff information is degraded during a rolling upgrade, and `clean_handoff` lingers until a later release drops it.
- v0.6.0's tag is delayed until this lands.
- `ErrRetire` signals success through the error channel. This matches the existing `pool.PermanentError` convention, and was chosen over a result struct to keep the work-function return type unchanged.
- Prose and earlier docs speak of "crash recovery"; the identifier is `ExitExpired` because the backend can only observe expiry. A crash, a partition, and a live holder whose renewal window ran out are indistinguishable to it. Docs keep "crash recovery" as the user-facing term and define it as `ExitExpired`.

## Alternatives Considered

- **Keep the boolean; add `Abandon`.** This fixes #73 and #79 with less change. It still cannot distinguish a first acquisition from an early crash, cannot express retirement (so #76 would need a second schema change), and freezes a `bool` into the 1.0 API.
- **Keep the boolean; never release on failure.** Every failed run would block its work ID for a full TTL, and none of the expressiveness gaps would be fixed.
- **Return the previous exit from `Acquire` instead of storing it.** Only the acquiring process could ever see it. It needs `RETURNING OLD` or a CTE on Postgres, and it changes `Backend.Acquire`. The stored column makes this available later, for free.
- **A `Release` mode that defaults to `ExitFinished`.** This recreates the bug that `defer Release` marks failures as completed work.
- **Retire through a work-function result struct.** It is more explicit, but changes the return type of every work function. The sentinel was chosen.

## Interactions

- **#75 (missing-row `ReadCheckpoint`).** With `ExitNone` as an explicit value for "no previous holder", "missing row means fresh start" is no longer needed as an interpretation. That strengthens the case for `ErrFenced`, but #75 is decided separately.
- **#80 (shutdown).** A successor sees `ExitAbandoned` after a graceful shutdown. Cleanup is bounded by `CleanupTimeout`, which defaults to 5s.
- **#77 (documentation drift).** ARCHITECTURE §Handoff vs Crash Recovery, §Release, §PostgreSQL Backend, and the schema block are rewritten for this model when it ships.

## References

- `backend/postgres/postgres.go` — `queryAcquire`, `queryRelease`, `queryReadCheckpoint`, `queryVacuumSweep`
- `backend/memory/memory.go` — `record`, `Acquire`, `Release`, `ReadCheckpoint`, `Sweep`
- `backend/conformance/conformance.go` — handoff and `Sweep` specs to be replaced
- `worker/runner.go`, `leader/leader.go`, `pool/pool.go` — the exit mapping
- `REVIEW-v0.6.0.md` §5.4.1 and §5.4.9 — the audit discussion this ADR resolves
