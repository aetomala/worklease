# worklease — Architecture

This document explains the design decisions, concepts, and internal mechanics of the `worklease`
library. It is the starting point for contributors and for users who want to understand what the
library does and why it is built the way it is.

## Table of Contents

- [Overview](#overview)
- [The Problem in Depth](#the-problem-in-depth)
- [What worklease Does Not Solve](#what-worklease-does-not-solve)
- [Core Concepts](#core-concepts)
- [Design Principles](#design-principles)
- [Project Structure](#project-structure)
- [API Architecture](#api-architecture)
- [PostgreSQL Backend](#postgresql-backend)
- [In-Memory Backend](#in-memory-backend)
- [Renewal Loop](#renewal-loop)
- [Acquire Flow](#acquire-flow)
- [Observability — LeaseObserver](#observability--leaseobserver)
- [worker.Runner — Lifecycle Management](#workerrunner--lifecycle-management)
- [checkpoint — Typed Encoding](#checkpoint--typed-encoding)
- [leader — Simplified Leader Election (v0.3)](#leader--simplified-leader-election-v03)
- [pool — Work Distribution (v0.3)](#pool--work-distribution-v03)
- [Testing Approach](#testing-approach)
- [Residual Risks](#residual-risks)
- [Roadmap](#roadmap)
- [Architecture Decision Records](#architecture-decision-records)

---

## Overview

`worklease` is a Go library for **lease-based work coordination** in distributed systems.

It is not a distributed lock library.

Every Go distributed locking library (`distlock`, `pglock`, `dynamodb-lock-go`, etcd leases,
`client-go/leaderelection`) solves the same problem: *who owns this resource right now?* They
provide mutual exclusion, TTL expiry, and heartbeat renewal. None of them answer the question
that comes immediately after ownership changes: *what does the new owner need to continue the
work the previous owner started?*

`worklease` fills that gap. It provides checkpointed lease handoff with fencing — the pattern
that makes it possible for a new worker to resume where the previous worker left off, with the
last known checkpoint intact and zombie writes rejected.

### Prior Art

This pattern is not new. AWS's Kinesis Client Library has implemented it since 2013 via its
`LeaseRefresher`. KCL maintains a DynamoDB table with an explicit `checkpoint` column and a
`leaseCounter` for fencing. A worker atomically renews its lease while writing its progress
checkpoint. When a worker's lease expires, the successor reads the checkpoint and resumes from
exactly that point. Fencing via conditional writes on `leaseCounter` is structurally identical
to `worklease`'s `fencing_token`.

The pattern is proven at AWS scale for over a decade. `worklease` extracts the same semantics
into a general-purpose Go primitive — no AWS dependency, no Kinesis assumption, pluggable
backends.

---

## The Problem in Depth

Consider a worker that processes a long-running job — provider onboarding, a multi-step batch
transformation, an async lifecycle operation. The worker acquires a lease, begins work, and
checkpoints progress at each step. Now it crashes.

Without `worklease`:

- Worker B acquires the lease. It has no idea what Worker A completed. It starts from
  the beginning. If the operation is not idempotent, you now have duplicate writes to the
  checkpoint store.
- If Worker A was slow (not dead) and its lease simply expired, it may still be writing.
  Worker B's writes and Worker A's writes interleave. State is corrupted.

With `worklease`:

- Worker A writes progress state atomically with every lease renewal. The last checkpoint
  is guaranteed to be written before any renewal succeeds.
- Worker B acquires the lease, reads the checkpoint, and resumes from the last known safe
  state. It also knows how Worker A left: a declared exit through `Release` (finished,
  abandoned, or retired) or a lease that simply expired (crash recovery). These are different
  situations requiring different handling.
- If Worker A is a zombie (slow, not dead), its next write to the lease store will be
  rejected — Worker B was issued a higher fencing token, and Worker A's writes with a stale
  token fail with `ErrFenced`.

### When worklease is the right fit

`worklease` is designed for a specific shape of work: stateful, multi-step operations where
restarting from scratch is expensive or incorrect, and where the team is not willing to adopt
a full workflow engine.

- Go workers with PostgreSQL already in the stack
- Multi-step jobs with meaningful intermediate state (provider onboarding, async lifecycle
  management, batch processing with partial results)
- Teams who have outgrown "just use a lock" but do not want to rewrite their workers as
  Temporal Workflows or adopt a managed platform

If your jobs are small enough to restart cheaply, idempotency and a job queue are the right
tools. If your work is complex enough to need replay semantics and activity scheduling,
Temporal is the right tool. `worklease` is the primitive for the space in between.

### worklease vs job queues

A natural comparison is to job queues — River, Asynq, `FOR UPDATE SKIP LOCKED` on a Postgres
jobs table. These are excellent tools and the right choice for most work assignment problems.
The distinction is what happens when a worker dies mid-job:

- A job queue re-enqueues the job. The next worker starts from the beginning. This is correct
  when jobs are idempotent and restart is cheap.
- `worklease` hands the checkpoint to the next worker. The next worker resumes from the last
  known state. This is necessary when restart is expensive or when intermediate state must
  survive the handoff.

If your workers can restart from scratch without correctness or cost concerns, use a job queue.
`worklease` exists for the cases where they cannot.

---

## What worklease Does Not Solve

These are not gaps or omissions. They are intentional scope boundaries. Understanding them
before adopting the library avoids incorrect assumptions about what the library guarantees.

### External side effects are not fenced

`worklease` fences writes to the lease store. It does not fence writes to external systems.

If Worker A calls Stripe, sends an email, or writes to S3 during a step, and then crashes
before checkpointing, Worker B will re-execute that step from the last checkpoint. The external
mutation may fire twice. `ErrFenced` stops stale checkpoint writes to PostgreSQL — it does not
stop a zombie worker from making an external API call between lease expiry and its next
`Checkpoint` or `Renew` call.

This is an inherent property of any coordination primitive — Temporal, KCL, and Chubby all
share it. The solution is at the application layer: make every external mutation idempotent,
use an outbox pattern, or enforce idempotency keys at each downstream system.

`worklease` provides the coordination plumbing. Callers own external idempotency.

### Checkpoint granularity is a caller decision

The library does not prescribe how often to checkpoint. Checkpointing after every atomic unit
of work provides the finest recovery granularity but may be expensive. Checkpointing less
frequently reduces overhead but increases the work a successor must repeat.

The right checkpoint interval depends on the cost of the work unit and the cost of the
checkpoint write. `worklease` provides the mechanism; callers decide the frequency.

### The window between checkpoints is at-least-once

Between two checkpoints, work is at-least-once. If Worker A executes a step, crashes before
checkpointing, and Worker B resumes from the previous checkpoint, that step will be re-executed.
`worklease` does not provide exactly-once semantics. It provides a resumable progress marker —
the guarantee is that the successor starts from the last checkpointed state, not from scratch.

Exactly-once execution of individual steps requires idempotent steps or external coordination
beyond what this library provides.

### Release expires the lease immediately

`Release(ctx, token, mode)` records the exit mode the holder declares — `ExitFinished`,
`ExitAbandoned`, or `ExitRetired` — and sets `expires_at` to a past timestamp, making the work
item immediately acquirable by a successor. A successor using `WithWaitForLease` will acquire
on its next poll; a fail-fast successor can call `Acquire` immediately after. Every mode expires
the lease immediately (ADR-0012, amended by ADR-0018).

The exit mode tells the successor how the previous holder left. The TTL governs crash detection
only — when a holder crashes without calling `Release`, the successor must wait for the TTL to
elapse before the record appears expired, and then sees `ExitExpired`. A `Release` bypasses
that wait.

### Recovery logic lives in caller code

`worklease` delivers the checkpoint bytes and how the previous holder exited (`PrevExit`). What those bytes mean,
whether the partial state is valid, how to roll back a half-finished step, and how to reconcile
external effects that happened before the crash — these are application concerns.

The library provides the coordination layer. Callers own the recovery semantics.

---

## Core Concepts

### Lease

A time-limited claim on a named unit of work, identified by a `workID` string. The lease has a
TTL; if it is not renewed before expiry, another worker can acquire it.

The library does not define what a `workID` represents. It can be a tenant ID, a job ID, a
shard number, or any string the caller uses to name a unit of work.

### Fencing Token

A monotonically incrementing integer issued every time a lease is acquired. The fencing token
is stored in the backend alongside the lease, and every write operation (checkpoint, renew,
release) is conditional on the token still being the current one.

If Worker A holds fencing token 4 and its lease expires, Worker B acquires the lease and
receives fencing token 5. Any write Worker A attempts with token 4 is now rejected — the
backend holds token 5. This prevents zombie writes to the checkpoint store.

The library owns fencing logic entirely. Callers cannot read or construct fencing tokens
directly — they receive a `Token` value and pass it back to operations. The library validates
the token on every write. See [ADR-0002](adr/0002-token-inspectable-via-methods.md).

Tokens are monotonic but not contiguous. Every `Acquire` attempt draws from the sequence
regardless of outcome — including attempts that find the lease held and return `ErrLeaseHeld`.
Gaps in the token sequence are expected and carry no operational meaning; callers must not
assume consecutive token values.

### Checkpoint

Progress state written atomically with lease renewal in a single backend operation. The
checkpoint is a raw `[]byte` blob — the library stores it and hands it to the next owner;
it has no opinion on encoding. See [ADR-0003](adr/0003-checkpoint-serialization-raw-bytes.md).

`Checkpoint` and `Renew` are distinct operations. `Checkpoint` writes state and extends the
lease TTL atomically — either both succeed or neither does. `Renew` extends the TTL without
writing new state, preserving the last checkpoint as-is. The lease can be extended without
updating the checkpoint (via `Renew`), but a checkpoint cannot be written without extending
the lease (via `Checkpoint`).

### Handoff vs Crash Recovery

When a new owner acquires a lease, it calls `ReadCheckpoint` to get the previous owner's
state. The call returns a `Checkpoint` value: `State` (the last checkpointed bytes),
`PrevExit` (how the immediately previous holder exited), and `PrevHolderID` (who it was).
`Acquire` captures the previous holder's exit atomically with token issuance, so the values
describe the immediately previous holder and are the same on every read during the lease
(ADR-0018).

| `PrevExit` | How it is set | What the new owner should do |
|---|---|---|
| `ExitNone` | Inferred: the work ID was never acquired, or its row was removed by `Forget` or `Sweep` | Start fresh |
| `ExitFinished` | `Release(…, ExitFinished)` | Continue from the final state |
| `ExitAbandoned` | `Release(…, ExitAbandoned)` — an error, a cancellation, a shutdown | Validate the partial state, then resume; the holder stopped cleanly |
| `ExitExpired` | Inferred: the lease expired with no recorded exit — a crash, a partition, or a renewal window that ran out | Validate the partial state; external effects may have fired after the last checkpoint |
| `ExitRetired` | `Release(…, ExitRetired)` | Do not redo the work |

Unknown modes must be treated as `ExitExpired`, the conservative interpretation. These are
semantically different situations; the library makes them explicit and forces callers to
handle each. Treating crash recovery the same as a completed run is a common source of subtle
bugs in distributed systems.

---

## Design Principles

**1. Coordination semantics over convenience.** The library does one thing correctly:
checkpointed lease handoff with fencing. It has no serialization opinions, no framework
requirements, and no platform dependencies.

**2. Explicit over implicit.** `Release` takes a required exit mode, so a holder must declare
whether it finished, abandoned, or retired the work, and successors handle each `PrevExit`
differently. `nil` state on fresh acquisition is explicit, not an error.
`ErrFenced` is returned, not silently swallowed.

**3. Library owns fencing, callers own state.** The library unconditionally enforces fencing
on every write to the lease store. Callers own checkpoint serialization, claim structure,
recovery logic, and external idempotency. This boundary is intentional — the library cannot
fence what it does not own.

**4. Backend is single-attempt; core owns retry.** Backend methods map to single storage
operations. Retry policy lives in the library core and is consistent across all backends. See
[ADR-0006](adr/0006-backend-acquire-single-attempt.md).

**5. No resource ownership surprise.** `postgres.New(db)` does not close `db`. The caller
constructs; the caller closes. See [ADR-0001](adr/0001-backend-interface-no-close.md).

**6. Observability via LeaseObserver.** `Token` implements `fmt.Stringer`. `LeaseObserver`
is the injection seam for metrics, structured logs, and traces — injected via `Config.Observer`,
defaulting to a no-op. No observability framework is required or assumed. Its methods take
per-operation event structs so new fields can be added without breaking implementers.
See [ADR-0007](adr/0007-observer-config-field.md).

**7. The code is the documentation.** Exported identifiers are documented, packages have doc
comments, comments explain why not what, errors carry context, interfaces specify contracts.

**8. Test at the interface boundary.** The `Backend` interface makes both the PostgreSQL and
in-memory backends fully testable in isolation. Integration tests for PostgreSQL require a real
database. Unit tests for the library core use the in-memory backend or mockgen-generated mocks.

**9. Higher-level packages are additive, not modifying.** `worker.Runner`, `leader`, and `pool`
build on `worklease.Lease` without modifying the core API surface. Each package addresses a
specific use-case shape; callers use only what they need.

---

## Project Structure

| Package | Purpose | Since |
|---------|---------|-------|
| `github.com/aetomala/worklease` | `Lease` interface, `Token`, options, errors, `New` constructor | v0.1 |
| `github.com/aetomala/worklease/backend` | Internal `Backend` interface and `LeaseRecord` type | v0.1 |
| `github.com/aetomala/worklease/backend/postgres` | PostgreSQL-backed production backend | v0.1 |
| `github.com/aetomala/worklease/backend/memory` | In-memory backend for testing; supports clock injection | v0.1 |
| `github.com/aetomala/worklease/backend/conformance` | Backend-agnostic `RunSuite` enforcing memory/Postgres parity | v0.4 |
| `github.com/aetomala/worklease/worker` | `Runner` — manages acquire/checkpoint/release lifecycle | v0.2 |
| `github.com/aetomala/worklease/checkpoint` | `Codec` interface and typed `Encode[T]`/`Decode[T]` helpers | v0.2 |
| `github.com/aetomala/worklease/leader` | `Elect` — simplified leader election without checkpoint state | v0.3 |
| `github.com/aetomala/worklease/pool` | `Pool` — distributes work IDs across competing processes | v0.3 |

### File layout

```
worklease/
├── doc.go              # Package-level documentation
├── lease.go            # Lease interface, Token, LeaseObserver + event structs + Operation, noopObserver, errors
├── worklease.go        # New() constructor, Config struct, leaseClient
├── acquire.go          # Acquire — wait+retry loop for WithWaitForLease
├── renewal.go          # StartRenewal — managed renewal goroutine
├── backend/
│   ├── backend.go          # Backend interface, LeaseRecord — internal to library
│   ├── conformance/        # v0.4
│   │   └── conformance.go  # RunSuite — backend-agnostic parity spec
│   ├── postgres/
│   │   ├── postgres.go     # PostgreSQL backend implementation
│   │   ├── schema.sql      # CREATE TABLE statement
│   │   └── postgres_test.go
│   └── memory/
│       ├── memory.go       # In-memory backend; Clock interface, Option, WithClock; defensive slice copies
│       └── memory_test.go
├── worker/
│   └── runner.go       # Runner, WorkFn, RunnerConfig, NewRunner
├── checkpoint/
│   └── codec.go        # Codec, JSONCodec, Encode[T], Decode[T]
├── leader/             # v0.3
│   └── leader.go       # Elect, Config (with OnElected/OnLost/OnRelinquished — v0.4)
├── pool/               # v0.3
│   └── pool.go         # Pool, WorkFn, PermanentError, Permanent, Observer, Config, New, sentinels
├── testutil/
│   ├── mock_backend.go # Generated Backend mock (mockgen)
│   └── mock_lease.go   # Generated Lease + LeaseObserver mocks (mockgen)
├── examples/
│   ├── subscription-cancellation/
│   ├── cross-tenant-migration/
│   ├── cluster-singleton-scheduler/  # v0.3
│   ├── partition-processor/          # v0.3
│   └── observability/                # v0.4
└── docs/
    ├── ARCHITECTURE.md     # This document
    └── adr/                # Architecture Decision Records
```

**Why no `internal/`?** This is a library. Everything the caller needs is exported — there is
nothing to accidentally expose. The `backend` package is public by design: it defines the
`Backend` interface that third-party backend authors implement.

**Why is `Backend` in a subpackage?** Callers use `worklease.New(backend, cfg)` — they never
construct a `Backend` directly. Keeping the interface in `backend/` makes the pluggability
contract visible in the filesystem without promoting it to the top-level API. A new Redis
backend author imports `github.com/aetomala/worklease/backend` and implements `Backend` — the
import path makes the role clear.

---

## API Architecture

### The Lease Interface

```go
type Lease interface {
    Acquire(ctx context.Context, workID string, opts ...AcquireOption) (Token, error)
    Checkpoint(ctx context.Context, token Token, state []byte) error
    Renew(ctx context.Context, token Token) error
    Release(ctx context.Context, token Token, mode ExitMode) error
    ReadCheckpoint(ctx context.Context, token Token) (Checkpoint, error)
    StartRenewal(ctx context.Context, token Token, opts ...RenewalOption) (renewCtx context.Context, stopRenewal func())
    Forget(ctx context.Context, token Token) error
}
```

`Checkpoint` and `Renew` are separate operations because they mean different things. `Renew`
says "I'm alive." `Checkpoint` says "I'm alive and here is where I am." A worker that is
making progress but has not reached a safe checkpointable boundary should call `Renew`. A
worker that has reached a meaningful state boundary should call `Checkpoint`.

### Token

`Token` is a value type with unexported fields. Callers receive a `Token` from `Acquire` and
pass it to all subsequent operations. The library validates the fencing token on every write.
Callers cannot construct or mutate tokens — fencing is unconditionally library-enforced.

```go
type Token struct {
    workID       string
    holderID     string
    fencingToken uint64
    expiresAt    time.Time
    deadline     time.Time // Local monotonic bound — Acquire start plus TTL; zero if unknown
}

func (t Token) WorkID() string       { return t.workID }
func (t Token) HolderID() string     { return t.holderID }
func (t Token) FencingToken() uint64 { return t.fencingToken }
func (t Token) ExpiresAt() time.Time { return t.expiresAt }
func (t Token) String() string       { ... }
```

`fmt.Stringer` is implemented so that `log.Printf("acquired %v", token)` works without caller
formatting. Structured loggers can use the individual accessors or `token.String()`.

See [ADR-0002](adr/0002-token-inspectable-via-methods.md) for the rationale behind unexported
fields.

### Error Sentinels

```go
var (
    // ErrFenced is returned by Checkpoint, Renew, Release, ReadCheckpoint, or Forget when a
    // higher fencing token has been issued, or the row no longer exists. This worker has been
    // superseded. Stop immediately.
    ErrFenced = errors.New("worklease: fenced — lease acquired by another holder")

    // ErrLeaseHeld is returned by Acquire when the lease is currently held and
    // WithWaitForLease was not passed, or the context expired while waiting.
    ErrLeaseHeld = errors.New("worklease: lease is currently held")

    // ErrLeaseExpired is returned when the lease expired before the operation completed,
    // or when the holder writes after declaring an exit with Release.
    ErrLeaseExpired = errors.New("worklease: lease has expired")

    // ErrLeaseWindowExhausted is the cancel cause of the renewal context when renewal fails
    // for the whole lease window. Inspect it with context.Cause(renewCtx).
    ErrLeaseWindowExhausted = errors.New("worklease: lease window exhausted before renewal succeeded")

    // ErrInvalidExitMode is returned by Release for any mode other than ExitFinished,
    // ExitAbandoned, or ExitRetired.
    ErrInvalidExitMode = errors.New("worklease: invalid exit mode")

    // ErrRetire is returned, or wrapped, by a work function to release with ExitRetired.
    ErrRetire = errors.New("worklease: retire work ID")
)
```

The block is abridged; `vacuum.go` also declares `ErrRetentionRequired`.

`ErrFenced` is the critical sentinel. When `Checkpoint` or `Renew` returns `ErrFenced`, it
means a successor has already acquired the lease and this worker is a zombie. The correct
response is to stop immediately — any further writes to the lease store will be rejected.
Note that `ErrFenced` does not and cannot stop in-flight external calls (Stripe, S3, domain
tables) that the worker may have already initiated. See
[What worklease Does Not Solve](#what-worklease-does-not-solve).

### Config and Constructor

```go
type Config struct {
    TTL      time.Duration  // Lease TTL; required.
    HolderID string         // Unique identity for this worker; required.
    Observer LeaseObserver  // Optional; nil installs a no-op observer. Never panics on nil.
}

func New(b backend.Backend, cfg Config) (Lease, error)
```

`HolderID` should uniquely identify the worker process. A hostname, a Kubernetes Pod name,
or a UUID generated at startup are all good choices. It appears in `Token.HolderID()` and
in the `holder_id` column in the backend, making it possible to track which worker holds a
lease at any point in time.

`Observer` is the injection point for metrics, logs, and traces. See
[Observability — LeaseObserver](#observability--leaseobserver).

### AcquireOption and RenewalOption

```go
// WithWaitForLease instructs Acquire to block until the lease becomes available
// or the context deadline is reached, rather than returning ErrLeaseHeld immediately.
func WithWaitForLease() AcquireOption

// WithPollInterval sets the interval between Acquire attempts when
// WithWaitForLease is active. Defaults to 2 seconds.
func WithPollInterval(d time.Duration) AcquireOption

// WithRenewalInterval overrides the default renewal interval (TTL/2).
func WithRenewalInterval(d time.Duration) RenewalOption
```

### The Backend Interface

`Backend` is the storage contract. It is an internal interface — callers never implement it
directly unless they are writing a new backend. The interface is defined in
`github.com/aetomala/worklease/backend`.

```go
type Backend interface {
    Acquire(ctx context.Context, workID, holderID string, ttl time.Duration) (LeaseRecord, error)
    Checkpoint(ctx context.Context, record LeaseRecord, state []byte, ttl time.Duration) error
    Renew(ctx context.Context, record LeaseRecord, ttl time.Duration) error
    Release(ctx context.Context, record LeaseRecord, mode ExitMode) error
    ReadCheckpoint(ctx context.Context, record LeaseRecord) (Checkpoint, error)
    Forget(ctx context.Context, record LeaseRecord) error
    Sweep(ctx context.Context, opts SweepOptions) (int64, error)
}
```

All backend methods are single-attempt. The backend never retries. This is a hard contract —
see [ADR-0006](adr/0006-backend-acquire-single-attempt.md).

`Backend` does not include `Close`. See [ADR-0001](adr/0001-backend-interface-no-close.md).

**Slice ownership.** `Checkpoint` must not retain a reference to the caller's `state` slice
after returning, and `ReadCheckpoint` must return a fresh allocation — never the stored backing
array. This gives checkpoint state value semantics regardless of backend: the caller owns the
slices it passes and receives, the backend owns its stored copy. The PostgreSQL backend is
compliant by `BYTEA` serialization; the in-memory backend makes explicit `copy()` calls. See
[ADR-0014](adr/0014-backend-slice-ownership-contract.md).

**Non-positive TTL.** `Acquire` with a non-positive `ttl` must produce an already-expired
record. This is a test-only affordance used by the conformance suite to exercise expiry without
clock manipulation — production never passes a non-positive TTL because `worklease.New`
validates `Config.TTL > 0`. See [ADR-0015](adr/0015-backend-conformance-suite.md).

`LeaseRecord` is the currency between the library core and the backend. It mirrors `Token`
but is the backend's internal representation — callers never see it. The library wraps
`LeaseRecord` into `Token` before returning to callers.

---

## PostgreSQL Backend

### Schema

```sql
CREATE SEQUENCE IF NOT EXISTS worklease_fencing_seq;

CREATE TABLE IF NOT EXISTS worklease_leases (
    work_id         TEXT        PRIMARY KEY,
    holder_id       TEXT        NOT NULL,
    fencing_token   BIGINT      NOT NULL DEFAULT nextval('worklease_fencing_seq'),
    expires_at      TIMESTAMPTZ NOT NULL,
    checkpoint      BYTEA,
    -- Deprecated: unread and unwritten since v0.6.0 (ADR-0018). Kept so a rollback to v0.5 does not fail; dropped in a later release (#86).
    clean_handoff   BOOLEAN     NOT NULL DEFAULT FALSE,
    exit_mode       TEXT,
    prev_exit_mode  TEXT        NOT NULL DEFAULT 'none',
    prev_holder_id  TEXT,
    acquired_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT worklease_leases_exit_mode_check
        CHECK (exit_mode IS NULL OR exit_mode IN ('finished', 'abandoned', 'retired')),
    CONSTRAINT worklease_leases_prev_exit_mode_check
        CHECK (prev_exit_mode IN ('none', 'expired', 'finished', 'abandoned', 'retired'))
);

CREATE INDEX IF NOT EXISTS idx_worklease_leases_updated_at
    ON worklease_leases (updated_at);
```

As of v0.5, `fencing_token` is sourced from a single global `worklease_fencing_seq` SEQUENCE
rather than a per-row `+ 1` (ADR-0016). Tokens are therefore strictly increasing across **all**
work IDs, not just within a single row's history, and they survive row deletion — the property
that makes v0.6 retention (`Forget`, `Vacuum.Sweep`) safe. `checkpoint` is nullable — `NULL` on first
acquisition means no prior state. `exit_mode` holds the current holder's declared exit (`NULL`
until it calls `Release`); `prev_exit_mode` and `prev_holder_id` hold the immediately previous
holder's exit and ID, captured by `Acquire` (ADR-0018). `clean_handoff` is deprecated: it is
never read or written since v0.6 and stays only so a rollback to v0.5 does not fail (#86).

### Acquire — Single-Statement Upsert with RETURNING

```sql
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
RETURNING fencing_token, expires_at
```

The `WHERE` clause is the fencing gate. If the lease is held and unexpired, the condition is
false — zero rows are updated, the statement returns no row, and the backend maps the resulting
`sql.ErrNoRows` to `ErrLeaseHeld`. If the lease is absent or expired, the upsert succeeds, draws
a fresh token from the sequence, and `RETURNING` yields the token and expiry in the same
statement. In the same statement it captures the previous holder's exit into `prev_exit_mode`
(`'expired'` when that holder declared none) and its ID into `prev_holder_id`, then clears
`exit_mode` for the new holder. A first insert records `prev_exit_mode = 'none'`. As of v0.5 this replaces the prior two-step `ExecContext` + read-back `SELECT`,
closing the read-back race (R8/F4).

**Sequence consumption on failed acquires**: `nextval('worklease_fencing_seq')` in the
`VALUES` clause is evaluated before conflict resolution. An `Acquire` that returns
`ErrLeaseHeld` (the `WHERE` clause is false — lease held, not expired) still consumes a
sequence value. This is the mechanism behind fencing tokens being monotonic but not
contiguous; see the Fencing Token section in Core Concepts.

**Clock note**: `NOW()` is the PostgreSQL server's clock — not the worker's clock. Expiry
decisions are made by the database, not by the client. This is intentional; see
[Residual Risks — R1](#residual-risks).

### Checkpoint — Conditional Update

```sql
UPDATE worklease_leases
SET checkpoint = $1,
    expires_at = NOW() + $2,
    updated_at = NOW()
WHERE work_id       = $3
  AND holder_id     = $4
  AND fencing_token = $5
  AND exit_mode IS NULL
```

Zero rows updated means either that the worker has been superseded or that it has already
declared an exit. A follow-up `queryHolderCheck` (shown under Release) tells them apart: no
matching row returns `ErrFenced`; a matching row returns `ErrLeaseExpired`, because a holder
cannot write after `Release` (ADR-0018). `Checkpoint` never writes an exit column. A lease that
lapsed with no declared exit can still be revived by its holder's `Checkpoint` if no successor
has acquired it.

The `AND fencing_token = $5` clause is the fencing enforcement. It is evaluated atomically by
PostgreSQL. There is no Lua script, no optimistic retry, no distributed clock dependency.

### Renew — Same Pattern, No Checkpoint Write

```sql
UPDATE worklease_leases
SET expires_at  = NOW() + $1,
    updated_at  = NOW()
WHERE work_id       = $2
  AND holder_id     = $3
  AND fencing_token = $4
  AND expires_at    > NOW()
```

Same fencing gate, plus an expiry guard. When zero rows are updated, a follow-up read
classifies the result: `ErrFenced` if the holder or token no longer match, `ErrLeaseExpired` if
they match but the lease has expired. No bare `time.Now()` — the database's `NOW()` is
authoritative. `ErrLeaseExpired` is terminal for the renewal goroutine: it cancels `renewCtx`
at once, without backoff or further attempts, with the cause
`errors.Join(ErrLeaseWindowExhausted, ErrLeaseExpired)` (#84; see Renewal Loop).

### Release — Record the exit and expire immediately

```sql
UPDATE worklease_leases
SET exit_mode  = $4,
    expires_at = NOW() - INTERVAL '1 millisecond',
    updated_at = NOW()
WHERE work_id       = $1
  AND holder_id     = $2
  AND fencing_token = $3
  AND expires_at    > NOW()
```

`Release` first validates the mode: only `ExitFinished`, `ExitAbandoned`, and `ExitRetired` are
accepted, and anything else returns `ErrInvalidExitMode` without touching the database. The
update records the declared mode in `exit_mode` and sets `expires_at` to a past timestamp, so
the record satisfies the `Acquire` condition (`expires_at < NOW()`) immediately and the
successor acquires without waiting for the TTL (ADR-0012). The checkpoint column is not
touched: the final checkpoint written by the last `Checkpoint` call survives, and the successor
reads it with `PrevExit` set to the declared mode.

When zero rows are updated, the shared holder check classifies the result — fencing first,
then expiry:

```sql
SELECT 1
FROM worklease_leases
WHERE work_id       = $1
  AND holder_id     = $2
  AND fencing_token = $3
```

No matching row returns `ErrFenced`. A matching row returns `ErrLeaseExpired`: the lease has
expired, or this holder already declared an exit, so nothing is recorded and a successor sees
`ExitExpired` (or the first declared mode). A second `Release` cannot overwrite the first.

**Operational note on `expires_at` ambiguity:** a row with `expires_at < NOW()` may be a
released lease or an expired one. `exit_mode` disambiguates: a declared mode means `Release`,
`NULL` means the holder never declared an exit. `expires_at > NOW()` continues to reliably
identify a currently held lease.

### Sweep — Retention keyed on declared exits

```sql
DELETE FROM worklease_leases
WHERE updated_at < NOW() - $1::interval
  AND expires_at < NOW()
  AND (exit_mode = 'retired' OR ($2 AND exit_mode IS NULL))
```

`Vacuum.Sweep` deletes expired rows last updated more than `Retention` ago that were released
with `ExitRetired`, plus, with `SweepOptions.IncludeExpired`, rows whose lease expired with no
declared exit. Rows released with `ExitFinished` or `ExitAbandoned`, and held rows, are never
deleted (ADR-0016, amended by ADR-0018).

---

## In-Memory Backend

The in-memory backend implements the same fencing token semantics as the PostgreSQL backend.
It is intended for unit tests within a single process — it is not safe for use across
processes.

```go
import "github.com/aetomala/worklease/backend/memory"

b := memory.New()
```

No `Close` method — no cleanup required.

### Global Fencing Sequence (v0.5)

As of v0.5, fencing tokens come from a per-instance monotonic counter — a `seq atomic.Uint64`
field advanced by `seq.Add(1)` on every successful acquire — rather than a per-row `+ 1`
(ADR-0016). Tokens are strictly increasing across all work IDs on the instance and are never
reset for its lifetime. The counter is per `memory.New()` instance: two independent instances
have independent counters, mirroring one Postgres sequence per database rather than a
process-global counter. The conformance suite asserts this global monotonicity on both backends.

### Slice Ownership

`Checkpoint` copies the incoming `state` into storage rather than aliasing the caller's slice,
and `ReadCheckpoint` returns a fresh copy of the stored bytes. Without these copies, mutating a
slice after `Checkpoint` — or mutating a `ReadCheckpoint` result — would silently corrupt stored
state. The PostgreSQL backend is immune to this via `BYTEA` serialization, so the copies make the
two backends behave identically; the conformance suite enforces it. See
[ADR-0014](adr/0014-backend-slice-ownership-contract.md).

### Clock Injection

Time-based expiry uses an injectable `Clock` interface. The default `realClock` delegates to
`time.Now()`. Tests that exercise lease expiry inject a `fakeClock` instead of using real
sleep.

```go
type Clock interface {
    Now() time.Time
}

// WithClock overrides the clock used for all time operations inside the memory backend.
func WithClock(c Clock) Option

b := memory.New(memory.WithClock(fakeClock))
```

The `Clock` interface is exported so test packages outside `backend/memory` can implement fake
clocks without importing an internal type. See [ADR-0008](adr/0008-clock-interface-memory-backend.md).

---

## Renewal Loop

`StartRenewal` starts a managed renewal goroutine and returns a derived context and a stop
function. This is the recommended way to hold a lease for the duration of a long-running
operation.

```go
renewCtx, stopRenewal := lease.StartRenewal(ctx, token)
defer stopRenewal()  // panic-safety net

// All downstream work uses renewCtx so that fencing propagates automatically.
if err := doWork(renewCtx, ...); err != nil {
    return err
}

stopRenewal()  // explicit call — stops renewal before Release
// Use the original ctx for Release — renewCtx may be cancelled by the time work finishes.
return lease.Release(ctx, token, worklease.ExitFinished)
```

### Context Lifecycle

As of v0.5, `renewCtx` is derived via `context.WithCancelCause`, so the **reason** for
cancellation is inspectable with `context.Cause(renewCtx)`:

| Event | `context.Cause(renewCtx)` |
|-------|---------------------------|
| Renewal receives `ErrFenced` | `ErrFenced` — this worker is a zombie, stop all work (never retried) |
| Lease window exhausted after retries | `ErrLeaseWindowExhausted` — renewal failed for the whole window |
| Renewal receives `ErrLeaseExpired` | `errors.Join(ErrLeaseWindowExhausted, ErrLeaseExpired)` — storage found the lease lapsed; cancelled at once, never retried (#84). `errors.Is` matches both |
| Parent `ctx` is cancelled | the parent's cause — propagated from parent |
| `stopRenewal()` is called | `nil` — `renewCtx` is **not** cancelled, clean shutdown |

The distinction between `stopRenewal()` (clean, cause `nil`) and context cancellation (fencing
or window exhaustion) is intentional. Downstream code can distinguish "work is done" from "we
were fenced" from "the lease window ran out" by inspecting `context.Cause(renewCtx)`.

### Bounded Retry (v0.5)

A non-fencing error from `Backend.Renew` no longer cancels `renewCtx` on the first failure.
The goroutine retries with exponential backoff plus additive jitter, bounded strictly by the
lease window — it never renews past the point where it could still be the legitimate owner.
The window starts as the earlier of `token.ExpiresAt()` and the local acquire start plus TTL,
and advances to the local start of each successful `Renew` plus TTL. The local bounds use the
monotonic clock, so a backend clock running ahead of the local clock cannot extend retries
past the true expiry (v0.6, PR #83). When the window is exhausted, it cancels with cause
`ErrLeaseWindowExhausted`. A fencing error is never retried, and `ErrLeaseExpired` from `Renew`
ends renewal at once (#84). See the [ADR-0013 amendment](adr/0013-renewal-goroutine-retry-policy.md). Each attempt is reported to `OnRenew` via `RenewEvent.Attempt` (1-based,
incremented per retry). Configure the policy with `WithRenewalBackoff(initial, max, jitter)`
(defaults 100ms / 5s / 0.20).

Note that `renewCtx` cancellation propagates fencing into downstream work — it does not fence
external systems. An in-flight HTTP call or database write to an external system initiated
before `renewCtx` is cancelled will still complete. See
[What worklease Does Not Solve](#what-worklease-does-not-solve).

### stopRenewal — Explicit Call vs Defer

`defer stopRenewal()` is a panic-safety net registered immediately after `StartRenewal`
returns. It is not the primary stop mechanism. Before calling `Release`, always call
`stopRenewal()` explicitly — this ensures the renewal goroutine has exited cleanly before
ownership is surrendered. The `defer` then becomes a no-op.

### Default Renewal Interval

The default renewal interval is `TTL / 2`. For a 30-second TTL, the goroutine renews every
15 seconds. This is a conservative choice — it provides two full renewal windows before the
lease can expire, which tolerates transient network issues without aggressive polling.

Override via `WithRenewalInterval`:

```go
renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(10*time.Second))
```

See [ADR-0004](adr/0004-renewal-loop-managed-goroutine.md) for the full design rationale.

---

## Acquire Flow

### Default Behavior — Fail Fast

By default, `Acquire` makes one attempt. If the lease is held and unexpired, it returns
`ErrLeaseHeld` immediately.

```go
token, err := lease.Acquire(ctx, "onboarding:tenant-abc")
if errors.Is(err, worklease.ErrLeaseHeld) {
    // Lease is held — try a different work item, or return and try later.
}
```

### WithWaitForLease — Opt-In Blocking

`WithWaitForLease` enables a poll loop. The library calls `Backend.Acquire` repeatedly at
the configured poll interval (default: 2 seconds) until the lease becomes available or the
context deadline is reached.

```go
token, err := lease.Acquire(ctx, "onboarding:tenant-abc", worklease.WithWaitForLease())
```

The retry loop lives in `acquire.go` in the library core — not in the backend. `Backend.Acquire`
is always single-attempt. This keeps retry behavior consistent across all backends and keeps
backend implementations simple. See [ADR-0005](adr/0005-acquire-default-returns-err-lease-held.md)
and [ADR-0006](adr/0006-backend-acquire-single-attempt.md).

As of v0.5, when the wait loop is cancelled or its deadline is exceeded, `Acquire` returns an
error wrapping `ctx.Err()` — satisfying `errors.Is(err, context.Canceled)` or
`errors.Is(err, context.DeadlineExceeded)` — rather than the bare `ErrLeaseHeld` it returned
through v0.4. The synchronous no-wait path above still surfaces `ErrLeaseHeld` directly. This is
a runtime break for callers that treated `ErrLeaseHeld` as their sole wait-loop termination
signal; see `UPGRADING.md` and the [ADR-0005](adr/0005-acquire-default-returns-err-lease-held.md)
v0.5 amendment.

### Why Fail-Fast is the Default

Blocking by default would hide contention. A worker that expects to immediately acquire a
lease would silently wait instead of failing visibly. Fail-fast makes contention observable.
`WithWaitForLease()` at the call site makes the intent clear: this caller expects to queue
behind the current holder.

---

## Observability — LeaseObserver

`LeaseObserver` is a six-method hook interface injected via `Config.Observer`. The library
calls it synchronously after every lease operation. Implementations must not block or panic.
When `Config.Observer` is nil, the library installs a no-op observer — no nil check is ever
required at call sites.

Each method takes a per-operation event struct. The event-struct shape (introduced in v0.4,
replacing the earlier flat-parameter signatures) lets new fields be added without breaking
implementers.

```go
type LeaseObserver interface {
    OnAcquire(ctx context.Context, e AcquireEvent)
    OnCheckpoint(ctx context.Context, e CheckpointEvent)
    OnRenew(ctx context.Context, e RenewEvent)
    OnRelease(ctx context.Context, e ReleaseEvent)
    OnReadCheckpoint(ctx context.Context, e ReadCheckpointEvent)
    OnFenced(ctx context.Context, e FencedEvent)
}

// Operation identifies the Lease operation that triggered a fencing event.
type Operation uint8
const (
    OperationCheckpoint Operation = iota
    OperationRenew
    OperationRelease
)
```

Every event carries the `Token` (zero value when the operation errored before acquiring one)
and the operation's `Err`. Operation events also carry a `Duration`; `CheckpointEvent`/
`ReadCheckpointEvent` carry a `Size`; `ReleaseEvent` carries the declared `Mode`;
`ReadCheckpointEvent` carries `PrevExit` and `PrevHolderID` (zero values when the read failed);
`FencedEvent` carries the `Operation` that triggered it. An invalid exit mode is rejected
before the backend is called, so it produces no `OnRelease` event.

**Fencing order.** When a fencing event occurs, the operation-specific callback fires first
(`OnCheckpoint`, `OnRenew`, or `OnRelease`), then `OnFenced`. This order is mandatory.
`OnFenced` fires on the **Release** path as well as Checkpoint and Renew — `FencedEvent.Operation`
identifies which. `OnReadCheckpoint` is **not** a fencing trigger: a fenced `ReadCheckpoint`
surfaces only through `ReadCheckpointEvent.Err`. `ErrLeaseExpired` from `Checkpoint` or
`Release` (an exit already declared, or a lapsed lease) is reported through the operation
event's `Err` and does not fire `OnFenced`.

**Duration contract.** `Duration` measures the single final backend call only — never the
`WithWaitForLease` poll loop. A caller that needs cumulative wait-loop time records its own
timestamp around `Acquire`.

A typical use is a Prometheus implementation: each callback increments a labeled counter or
records a histogram, using `e.Token.WorkID()` / `HolderID()` / `FencingToken()` for structured
labels and `e.Duration` for latency histograms. See `examples/observability` for a stdlib-only
reference implementation.

See [ADR-0007](adr/0007-observer-config-field.md).

---

## worker.Runner — Lifecycle Management

`worker.Runner` manages the full acquire → checkpoint → release lifecycle so callers write
only the work function. Without `Runner`, every caller must write the same 30-plus-line
scaffold; `Runner` reduces that to a single `Run` call.

```go
import "github.com/aetomala/worklease/worker"

// WorkFn is the work function signature accepted by Runner.Run.
// The ctx argument is the renewal context; it is cancelled on fencing, when the
// lease window is exhausted, or when the caller's context is cancelled. The
// prior argument carries the last checkpointed state and how the immediately
// previous holder exited; PrevExit is ExitNone on a first run. Return final
// state to checkpoint, or nil to skip the final checkpoint. Return nil to
// release with ExitFinished, an error wrapping worklease.ErrRetire to release
// with ExitRetired, or any other error to release with ExitAbandoned.
type WorkFn func(ctx context.Context, token worklease.Token, prior worklease.Checkpoint) ([]byte, error)

// RunnerConfig holds configuration for a Runner instance.
type RunnerConfig struct {
	// Lease is the lease client. Required; nil returns ErrLeaseRequired from
	// NewRunner.
	Lease worklease.Lease

	// WorkFn is the work function. Required; nil returns ErrWorkFnRequired from
	// NewRunner.
	WorkFn WorkFn

	// AcquireOptions are passed to Lease.Acquire. Optional.
	AcquireOptions []worklease.AcquireOption

	// RenewalOptions are passed to Lease.StartRenewal. Optional.
	RenewalOptions []worklease.RenewalOption

	// CleanupTimeout bounds the final Checkpoint and Release, which run on a
	// context that survives cancellation of the caller's context. Zero or
	// negative means 5s.
	CleanupTimeout time.Duration
}

runner, err := worker.NewRunner(worker.RunnerConfig{
	Lease:  lease,
	WorkFn: myWorkFn,
})

err = runner.Run(ctx, "onboarding:tenant-abc")
```

`Runner.Run` performs these steps in order:

| Step | Action | Context |
|---|---|---|
| 1 | `lease.Acquire`; an error is returned wrapped as `worker: acquire: …` | `ctx` |
| 2 | `lease.ReadCheckpoint`. `ErrFenced` is returned as is; any other error is returned wrapped as `worker: read checkpoint: …`. Neither path releases: the lease expires after its TTL and the successor sees `ExitExpired` with this holder as `PrevHolderID` | `ctx` |
| 3 | `lease.StartRenewal`; `stopRenewal` is also deferred as a panic-safety net | `ctx` |
| 4 | Calls `WorkFn` with `renewCtx`, the token, and the `Checkpoint` | `renewCtx` |
| 5 | `stopRenewal()`, then reads `context.Cause(renewCtx)` | — |
| 6 | Classifies the outcome, first match wins: fenced (`WorkFn` or the cause) → return `ErrFenced`, no checkpoint, no release; lease window exhausted → no checkpoint, no release, return the `WorkFn` error or `worker: …` wrapping the cause; an error wrapping `ErrRetire` → `ExitRetired`, success; `nil` → `ExitFinished`, success; any other error → `ExitAbandoned` | — |
| 7 | Derives one cleanup context, `context.WithTimeout(context.WithoutCancel(ctx), CleanupTimeout)` | — |
| 8 | If `WorkFn` returned non-nil state: `lease.Checkpoint`. `ErrFenced` → return `ErrFenced`, no release. Any other error switches the mode to `ExitAbandoned`; the result becomes `worker: checkpoint: …` if the run was a success, else stays the `WorkFn` error | cleanup |
| 9 | `lease.Release` with the mode. `ErrFenced` → return `ErrFenced`. Any other error is returned wrapped as `worker: release: …` only if nothing failed earlier; otherwise the earlier error is returned | cleanup |

`Token` is passed to `WorkFn` so callers can make mid-work `Checkpoint` calls for fine-grained
progress recording without stepping outside the `Runner` API.

**Error contract:**

- `Run` returns `nil` when `WorkFn` returns `nil` or an error wrapping `worklease.ErrRetire`
  and the exit is recorded.
- `ErrFenced` from any step, or as the renewal cause — `Run` returns `ErrFenced` and records
  nothing; a successor already holds the lease.
- Lease window exhausted, `ReadCheckpoint` failure, or a `WorkFn` panic — no release; the
  successor sees `ExitExpired` after the TTL. A panic propagates after renewal is stopped.
- Any other `WorkFn` error — the final state is checkpointed, the lease is released with
  `ExitAbandoned`, and `Run` returns the `WorkFn` error.
- The final `Checkpoint` and `Release` survive cancellation of the caller's `ctx`, bounded by
  `CleanupTimeout` (default 5s), so a graceful shutdown still records the final state and
  releases the lease.

---

## checkpoint — Typed Encoding

The core `Lease` interface uses `[]byte` for checkpoint state (ADR-0003). The `checkpoint`
subpackage provides opt-in typed helpers that eliminate the repeated `json.Marshal` /
`json.Unmarshal` + nil-guard boilerplate present at every call site.

```go
import "github.com/aetomala/worklease/checkpoint"

// Codec is the serialization interface for checkpoint state.
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
}

// JSON returns a Codec backed by encoding/json.
func JSON() Codec

// Encode marshals v using the given Codec.
func Encode[T any](c Codec, v T) ([]byte, error)

// Decode unmarshals data into a new T using the given Codec.
// Returns the zero value of T when data is nil — the "no prior checkpoint" invariant.
func Decode[T any](c Codec, data []byte) (T, error)
```

The `Codec` interface is the extension point. The library ships `JSON()` as the default. Callers
who use protobuf, msgpack, or any other format implement the two-method `Codec` interface once
and receive the generic helpers for free — no changes to call sites.

**The nil-bytes contract:** `Decode[T]` returns the zero value of `T` without error when `data`
is nil. This is the "no prior checkpoint" invariant: a first-time acquirer receives a usable
zero struct regardless of which codec is in use. This check happens in `Decode[T]` before
delegating to the codec, so it holds even for codecs that would panic or error on nil input.

**Why `Marshal`/`Unmarshal` on the interface, `Encode`/`Decode` on the helpers?** The interface
methods match Go's convention for value-to-bytes operations (`encoding.BinaryMarshaler`,
`encoding.TextMarshaler`). The package-level generics use `Encode`/`Decode` to signal a
different semantic: type-safe, nil-safe wrappers around the codec — not raw byte operations.

See [ADR-0009](adr/0009-checkpoint-subpackage-codec-interface.md).

---

## leader — Simplified Leader Election (v0.3)

`leader.Elect` is a simplified API for use cases where mutual exclusion is all that is needed —
one active goroutine at a time for a named work ID, with fencing propagated via context
cancellation. No checkpoint state is carried or handed off.

```go
import "github.com/aetomala/worklease/leader"

err := leader.Elect(ctx, lease, "scheduler:primary", leader.Config{
    AcquireOptions: []worklease.AcquireOption{worklease.WithWaitForLease()},
}, func(ctx context.Context) error {
    // ctx is cancelled if the lease is fenced, the lease window runs out, or ctx is cancelled.
    return runScheduler(ctx)
})
```

`Elect` manages the full lifecycle: acquire, start renewal, call `fn` with `renewCtx`, stop
renewal explicitly, and release with the exit mode that matches `fn`'s outcome. `fn` receives
no `Token` — leader election is presence-only. Callers who need to make fencing-aware writes
during work should use `worker.Runner` instead.

The mapping follows `worker.Runner`, first match wins: fenced (from `fn` or the renewal cause)
→ no release, return `ErrFenced`; lease window exhausted → no release, return `fn`'s error or
`leader: …` wrapping the cause; an error wrapping `worklease.ErrRetire` → release with
`ExitRetired` and return `nil`; `nil` → release with `ExitFinished`; any other error → release
with `ExitAbandoned` and return it. If `fn` panics, renewal is stopped, the lease is not
released, and the panic propagates; the successor sees `ExitExpired` after the TTL. `Release`
runs on
`context.WithTimeout(context.WithoutCancel(ctx), cfg.CleanupTimeout)` (default 5s), so a
cancelled `ctx` still releases. A non-fenced `Release` error is returned wrapped as
`leader: release: …` when `fn` succeeded; when `fn` failed, `Elect` returns `fn`'s error.

Blocking vs fail-fast acquire behavior is caller-controlled via `cfg.AcquireOptions` —
`Elect` does not force `WithWaitForLease`. Pass it explicitly to block until leadership is
available.

**Retry loops and backoff:** Because `Release` expires the lease immediately (ADR-0012), a
caller wrapping `Elect` in a retry loop with a fast-returning `fn` will see rapid
acquire/release/reacquire cycling without throttling. Set `cfg.BackoffInterval` to avoid
this — `Elect` sleeps for that duration before returning on every non-fencing path after a
successful `Acquire`, including `ExitAbandoned` and an exhausted lease window. Fencing paths
bypass the sleep, and an `Acquire` error (such as `ErrLeaseHeld`) is returned without it:

```go
for {
    err := leader.Elect(ctx, lease, "scheduler:primary", leader.Config{
        BackoffInterval: 500 * time.Millisecond,
    }, runScheduler)
    if errors.Is(err, worklease.ErrFenced) || ctx.Err() != nil {
        return
    }
}
```

**Lifecycle callbacks (v0.4):** `leader.Config` accepts three optional `func` fields for
observing the leadership lifecycle:

```go
err := leader.Elect(ctx, lease, "scheduler:primary", leader.Config{
    OnElected:      func(ctx context.Context, t worklease.Token) { /* became leader */ },
    OnLost:         func(ctx context.Context, t worklease.Token) { /* fenced / lease window ran out */ },
    OnRelinquished: func(ctx context.Context, t worklease.Token) { /* released with ExitFinished or ExitRetired */ },
}, runScheduler)
```

`OnElected` fires after `Acquire` succeeds, before `fn`; `OnLost` fires when the renewal context
is cancelled before `fn` returns while the parent `ctx` is not; `OnRelinquished` fires only after
`Release` succeeds with `ExitFinished` or `ExitRetired` — never after `ExitAbandoned` (v0.6,
ADR-0018; see the ADR-0010 amendment). All are
optional (nil is a no-op) and receive the original `ctx`. They are plain `func` fields rather than
a `leader.Observer` interface because the event surface is narrow — three closures are more
idiomatic than a thin interface (contrast `pool.Observer` below). They sit above the underlying
`Lease`'s own `LeaseObserver`: same events, higher altitude.

See [ADR-0010](adr/0010-leader-fn-signature-and-acquire-semantics.md) and
[ADR-0012](adr/0012-release-expires-lease-immediately.md).

---

## pool — Work Distribution (v0.3)

`pool.Pool` distributes a fixed set of work IDs across competing processes. Multiple `Pool`
instances — one per process — share a backend and collectively cover the work ID set.
Rebalancing is emergent from lease acquisition races. Each active slot is backed by a
`worker.Runner` internally.

```go
import "github.com/aetomala/worklease/pool"

p, err := pool.New(lease, pool.Config{
    WorkIDs:         []string{"shard:0", "shard:1", "shard:2", "shard:3"},
    BackoffInterval: 5 * time.Second,
}, func(ctx context.Context, workID string, token worklease.Token, prior worklease.Checkpoint) ([]byte, error) {
    return processShard(ctx, workID, prior)
})

err = p.Run(ctx)  // blocks until ctx is cancelled or every slot has exited
```

The work function and configuration:

```go
// WorkFn is the work function executed per slot.
// The workID argument identifies the slot. The token argument is the current
// lease token; use it for mid-work Checkpoint calls. The prior argument
// carries the last checkpointed state and how the immediately previous holder
// exited. Return final state to checkpoint, or nil to skip it. Return nil to
// release with ExitFinished and run again after RerunInterval, an error wrapping
// worklease.ErrRetire to release with ExitRetired and stop the slot, a
// PermanentError to release with ExitAbandoned and drop the slot, or any other
// error to release with ExitAbandoned and back off.
type WorkFn func(ctx context.Context, workID string, token worklease.Token, prior worklease.Checkpoint) ([]byte, error)

// Config holds construction parameters for a Pool.
type Config struct {
	// WorkIDs is the fixed set of work IDs this pool competes for.
	// Required; an empty slice returns ErrEmptyWorkIDs from New.
	WorkIDs []string

	// AcquireOptions are passed to each slot's internal Runner. Passing
	// worklease.WithWaitForLease returns ErrWithWaitForLeaseProhibited from New.
	AcquireOptions []worklease.AcquireOption

	// RenewalOptions are passed to each slot's internal Runner. Optional.
	RenewalOptions []worklease.RenewalOption

	// IdleInterval is the wait before retrying a slot whose work ID another
	// holder holds, meaning Runner.Run returned ErrLeaseHeld. Zero or negative
	// means 1s. Each wait is drawn uniformly from
	// [IdleInterval, 1.2*IdleInterval). OnSlotBackoff is not called for these
	// waits.
	IdleInterval time.Duration

	// RerunInterval is the wait before reacquiring a slot after its WorkFn
	// returned nil and the lease was released with ExitFinished. Zero or
	// negative means 1s. Each wait is drawn uniformly from
	// [RerunInterval, 1.2*RerunInterval), which also gives peer processes a
	// chance to acquire the work ID. OnSlotBackoff is not called for these
	// waits.
	RerunInterval time.Duration

	// BackoffInterval is the wait before reacquiring a slot after Runner.Run
	// returns a non-permanent error other than ErrFenced and ErrLeaseHeld.
	// Zero or negative means 1s. No jitter.
	BackoffInterval time.Duration

	// CleanupTimeout is passed to each slot's Runner. It bounds the final
	// Checkpoint and Release, which survive cancellation of Run's context.
	// Zero or negative means 5s.
	CleanupTimeout time.Duration

	// Observer receives callbacks on slot lifecycle transitions.
	// Optional; nil installs a no-op observer.
	Observer Observer
}
```

`Pool.Run` starts one goroutine per work ID. Each goroutine loops: acquire, run the work
function through a `worker.Runner` (so the exit-mode mapping and cleanup context above apply),
then decide what to do next. After each `Runner.Run`, the first matching row wins:

| # | Condition | Action |
|---|---|---|
| 1 | `Run` returned `nil` and the work function retired the work ID (`ErrRetire`) | `OnSlotRetired`; the slot exits and does not count as dead |
| 2 | `Run`'s internal context is cancelled | the slot exits; no observer call |
| 3 | `Run` returned `nil` | wait `RerunInterval` plus up to 20% jitter; no observer call; loop |
| 4 | `ErrFenced` | `OnSlotLost`; reacquire immediately |
| 5 | `PermanentError` | `OnSlotDead`; the slot exits |
| 6 | `ErrLeaseHeld` | wait `IdleInterval` plus up to 20% jitter; no observer call; loop |
| 7 | any other error | `OnSlotBackoff` with the effective `BackoffInterval`; wait it; loop |

Every wait ends early when `ctx` is cancelled. With the defaults (1s each), a slot whose work
ID a peer holds makes about one `Acquire` attempt per second (#81), and a finished slot pauses
before running again so peer processes can win the work ID. When `ctx` is cancelled, every
active slot completes its final `Checkpoint` and `Release`, bounded by `CleanupTimeout`, before
`Run` returns.

`WorkFn` receives the `Token` because pool targets stateful work — mid-work `Checkpoint` calls
are expected. The `workID` parameter identifies which slot is executing; callers dispatch
internally based on it.

**Permanent slot failure:** Return an error implementing `PermanentError` to drop the slot
permanently without reacquisition:

```go
type PermanentError interface {
    error
    Permanent() bool
}
```

`pool` detects it via `errors.As`, so it composes correctly with `fmt.Errorf` wrapping chains.
Implement the interface on a custom error type, or wrap an existing error with the v0.4
`pool.Permanent(err)` constructor when you do not need a named type:

```go
return nil, pool.Permanent(fmt.Errorf("partition %s decommissioned", workID))
```

A `PermanentError` is a non-fencing failure, so the slot releases with `ExitAbandoned` and a
successor resumes from its partial state. To finish a work ID for good instead, return an error
wrapping `worklease.ErrRetire`: the slot releases with `ExitRetired`, fires `OnSlotRetired`, and
stops. `ErrRetire` wins when an error satisfies both.

**Slot observability (v0.4):** `pool.Config.Observer` accepts a `pool.Observer` — an interface
with `OnSlotAcquired`, `OnSlotLost`, `OnSlotBackoff`, `OnSlotDead`, and (v0.6) `OnSlotRetired`,
each taking an event struct. nil installs a no-op. Unlike `leader`'s `func` callbacks, the pool
uses an interface because it has several distinct slot events. `OnSlotBackoff` fires only before
a `BackoffInterval` wait, never for `ErrLeaseHeld` or after a `nil` return. `OnSlotAcquired` fires when a slot enters its `WorkFn`
— the same moment the slot becomes visible in `ActiveSlots` — not when the internal `Acquire`
succeeds. `ActiveSlots()` reflects only slots currently executing `WorkFn`; slots that are
acquiring or in backoff are excluded.

**Shutdown signal (v0.4, narrowed in v0.6):** `Run` returns `ErrAllSlotsDead` only when every
slot was counted dead through a `PermanentError`. A slot that observes cancellation of `ctx`
before its `PermanentError` check exits without being counted, so if the caller's `ctx` is
cancelled at the same moment the last slot fails, `Run` may return `nil` instead of
`ErrAllSlotsDead`. A cancellation that lands after the last slot was counted does not hide it.
`Run` returns `nil` when `ctx` was cancelled or at least one slot retired. Supervisors can act on the
difference without parsing a nil return.

**`WithWaitForLease` is prohibited** in `pool.Config.AcquireOptions`. The pool manages its
own acquisition loop — if a slot goroutine blocks inside `Runner.Run` waiting for a lease, it
cannot respond to context cancellation during shutdown. `pool.New` returns
`ErrWithWaitForLeaseProhibited` if `WithWaitForLease` is detected. Construction errors use
distinct sentinels — `ErrNilLease`, `ErrEmptyWorkIDs`, `ErrWithWaitForLeaseProhibited` — each
wrapping `ErrConfigInvalid`, so `errors.Is(err, pool.ErrConfigInvalid)` still matches the broad
case.

See [ADR-0011](adr/0011-pool-scope-and-permanent-error-interface.md).

---

## Testing Approach

### Unit Tests — Ginkgo/Gomega BDD

All tests use Ginkgo/Gomega. Each test file has one outer `Describe` per component, with
nested `Describe` blocks for lifecycle phases, `Context` blocks for conditions, and `It` blocks
as outcome assertions.

```go
var _ = Describe("leaseClient", func() {
    // shared vars, BeforeEach, AfterEach at this level

    Describe("Phase 1: Constructor and Initialization", func() {
        It("returns ErrLeaseHeld when the lease is already held", func() {
            ...
        })
    })

    Describe("Phase 3: Core Operations", func() {
        It("returns ErrFenced when the fencing token is stale", func() {
            ...
        })
    })
})
```

`It` text is a complete sentence describing the expected outcome. Tests at the library core
level use the in-memory backend or mockgen-generated mocks — no database required.

### Integration Tests — PostgreSQL Backend

`backend/postgres/postgres_test.go` requires a real PostgreSQL instance. Set
`WORKLEASE_TEST_POSTGRES_DSN` to a valid DSN:

```bash
WORKLEASE_TEST_POSTGRES_DSN="postgres://user:pass@localhost/worklease_test?sslmode=disable" \
    go test ./backend/postgres/...
```

Integration tests verify the SQL operations that are not testable with the in-memory backend:
expiry semantics via `NOW()`, the `ON CONFLICT` upsert behavior, `TIMESTAMPTZ` precision, and
the two-query `ReadCheckpoint` and `Renew` disambiguation paths.

### Backend Conformance Suite

`backend/conformance` provides a single, backend-agnostic specification that both backends must
pass, so memory-vs-Postgres parity is enforced structurally rather than discovered one bug at a
time. `RunSuite` takes a factory and returns a Ginkgo spec tree; each backend's test file wires
it in:

```go
// backend/memory/memory_test.go
var _ = Describe("conformance", conformance.RunSuite(func() backend.Backend {
    return memory.New()
}))

// backend/postgres/postgres_test.go — DSN-gated, truncates per spec
var _ = Describe("conformance", conformance.RunSuite(func() backend.Backend {
    _, _ = db.Exec("DELETE FROM worklease_leases")
    b, _ := wlpostgres.New(db)
    return b
}))
```

The factory yields a clean backend per spec. Expiry is exercised with a non-positive TTL — no
clock injection — so the same specs run identically on both backends. The package imports only
`backend` and `worklease`, never a concrete backend. The suite locks the historical parity bugs
(issues #13/#14/#15) and the [ADR-0014](adr/0014-backend-slice-ownership-contract.md)
slice-aliasing invariants. A new backend must pass it before it is considered complete. See
[ADR-0015](adr/0015-backend-conformance-suite.md).

### Race Detection and CI

All tests run with `-race`. The CI pipeline runs two jobs: `lint + govulncheck` and
`test + postgres service container`. The in-memory backend and the renewal goroutine are the
areas that benefit most from race detection.

```bash
make ci   # lint, build, test (all three targets)
```

---

## Residual Risks

**R1 — Clock skew across workers.** Lease expiry is decided by the database clock: `Acquire`, `Renew`, and `Release` compare `expires_at` with the server's `NOW()`. The renewal goroutine bounds its retries with a local monotonic window (acquire start plus TTL, advanced after each successful `Renew`), so a local clock that runs slow relative to the database can let a holder believe its lease is live slightly longer than storage does. Fencing still rejects its writes once a successor acquires. The one-millisecond past offset that `Release` writes (ADR-0012) assumes at least millisecond clock resolution.

**R2 — Renewal goroutine leak on a missed `stopRenewal`.** If a caller of `StartRenewal` never calls `stopRenewal()`, the goroutine runs until the parent context is cancelled or fencing occurs. Call `defer stopRenewal()` immediately after `StartRenewal`. `worker.Runner` and `leader.Elect` do this themselves.

**R3 — In-memory backend real-time expiry.** Resolved in v0.2: `memory.WithClock` injects a clock, and every expiry path uses it.

**R4 — External side effects are not fenced.** `ErrFenced` stops stale writes to the lease store only. Make every external mutation idempotent.

**R5 — At-least-once between checkpoints.** A holder that crashes between two checkpoints causes its successor to redo the work since the last checkpoint. The successor sees `ExitExpired` and partial state. worklease provides a resumable progress marker, not exactly-once step execution.

**R6 — `pool` and `WithWaitForLease`.** Blocking acquisition inside a pool slot would stop the slot from responding to cancellation. `pool.New` returns `ErrWithWaitForLeaseProhibited` if `WithWaitForLease` is in `AcquireOptions`.

**R7 — `leader.Elect` retry loops have no built-in backoff.** `Release` expires the lease immediately (ADR-0012), in every exit mode. A caller that wraps `Elect` in a tight loop with a fast-failing `fn` cycles through acquire, release, and reacquire. Mitigated by `leader.Config.BackoffInterval`, which applies on every non-fencing return after a successful `Acquire`, including `ExitAbandoned`. `pool` waits `IdleInterval`, `RerunInterval`, or `BackoffInterval` (each defaults to 1s) on every looping path except the immediate reacquire after `ErrFenced`. Direct `Lease` and `worker.Runner` callers own their retry pacing.

**R8 — Postgres `Acquire` read-back race.** Resolved in v0.5: `Acquire` is a single `INSERT … ON CONFLICT … RETURNING` statement.

**R9 — Renewal surrenders on the first transient error.** Resolved in v0.5 (ADR-0013): retries use exponential backoff with additive jitter, bounded by the lease window. Since v0.6, `ErrLeaseExpired` from `Renew` ends renewal immediately, because storage has already found the lease lapsed.

**R10 — Checkpoint data loss when sweeping expired rows.** `Vacuum.Sweep` never deletes rows released with `ExitFinished` or `ExitAbandoned`, and deletes `ExitRetired` rows only after `Retention`. With `IncludeExpired`, it also deletes expired rows with no declared exit, which discards the partial state a successor would have recovered from. `IncludeExpired` defaults to `false`. Operators who enable it should set `Retention` longer than the time a successor needs to arrive. Rows last released before v0.6 migrate with `exit_mode = NULL`, so default `Sweep` never removes them; only `IncludeExpired` does, together with genuinely expired rows. Deletion is fencing-safe: the global sequence (ADR-0016) gives the next `Acquire` a strictly greater token, so a zombie holding the old token is fenced.

**R11 — Degraded handoff accuracy during a v0.5 → v0.6 rolling upgrade.** Fencing is unaffected. v0.5 processes neither write `exit_mode` nor capture `prev_exit_mode` and `prev_holder_id`, so while both versions share the table, handoff information is conservative or stale. Do not run `Vacuum.Sweep` until no v0.5 process remains: a v0.5 holder can reacquire a row whose `exit_mode` is still `'retired'`. See `UPGRADING.md`.

---

## Roadmap

### v0.1 — Released (v0.1.0, 2026-06-06)

- `Lease` interface: `Acquire`, `Checkpoint`, `Renew`, `Release`, `ReadCheckpoint`, `StartRenewal`
- `Token` value type — unexported fields, accessor methods, `fmt.Stringer`
- `AcquireOption`: `WithWaitForLease`, `WithPollInterval`
- `RenewalOption`: `WithRenewalInterval`
- PostgreSQL backend — fencing via conditional `UPDATE WHERE fencing_token = $n`
- In-memory backend — for unit testing
- Error sentinels: `ErrFenced`, `ErrLeaseHeld`, `ErrLeaseExpired`
- ADR-0001 through ADR-0006 ✅

### v0.2 — Released (v0.2.0, 2026-06-09)

- `LeaseObserver` — five-method hook interface; injected via `Config.Observer`; no-op default
- `memory.Clock` interface + `memory.Option` + `memory.WithClock` — deterministic expiry in tests
- `worker.Runner` — acquire/checkpoint/release lifecycle management
- `checkpoint` package — `Codec` interface, `JSON()` codec, `Encode[T]`/`Decode[T]` helpers
- Both examples rewritten to use `Runner` and `checkpoint`
- Three retroactive backend correctness fixes (issues #13, #14, #15)
- ADR-0007, ADR-0008, ADR-0009 ✅

### v0.3 — Released (v0.3.0, 2026-06-13)

- `HasWaitForLease([]AcquireOption) bool` — option inspection helper for higher-level packages
- `Codec` interface method rename: `Encode`/`Decode` → `Marshal`/`Unmarshal` (breaking change; see `UPGRADING.md`)
- `leader` package — `Elect`, `Config`
- `pool` package — `Pool`, `WorkFn`, `PermanentError`, `Config`
- `Release` semantics corrected — now expires the lease immediately in both backends, enabling instant clean handoff (issue #33)
- ADR-0010, ADR-0011, ADR-0012

### v0.4 — Released (v0.4.0, 2026-06-17)

- `LeaseObserver` redesign — six event-struct methods, new `OnReadCheckpoint`, `OnFenced` on the Release path, `Duration` on all operation events (breaking; see `UPGRADING.md`)
- Memory backend slice-ownership defensive copies (ADR-0014)
- `backend/conformance` suite — `RunSuite` enforcing memory/Postgres parity (ADR-0015)
- `pool.Observer`, `pool.Permanent`, distinct config sentinels, `ErrAllSlotsDead`
- `leader.Config` lifecycle callbacks — `OnElected`, `OnLost`, `OnRelinquished`
- `examples/observability` — stdlib-only `LeaseObserver` reference
- ADR-0014, ADR-0015

### v0.5 — Released (v0.5.0, 2026-06-29)

- Renewal goroutine bounded retry — exponential backoff plus additive jitter, bounded strictly by the lease window; `WithRenewalBackoff`, `ErrLeaseWindowExhausted` (via `context.Cause`), `RenewEvent.Attempt`; `StartRenewal` uses `context.WithCancelCause` (ADR-0013)
- Global fencing sequence on both backends — Postgres `worklease_fencing_seq` and per-instance memory `atomic.Uint64`; tokens strictly increase across all work IDs and survive row deletion (ADR-0016 fencing component)
- Single-statement `Acquire` with `RETURNING` — closes the read-back race (R8/F4)
- `Acquire` with `WithWaitForLease` returns `ctx.Err()` on wait-loop cancellation/deadline (breaking; ADR-0005 amendment, see `UPGRADING.md`)
- ADR-0013, ADR-0016 (fencing component)

### v0.6 — Unreleased

- `Lease.Forget` / `Vacuum.Sweep` — caller-governed row lifecycle and retention; `Backend` gains `Forget` and `Sweep` (ADR-0016 retention component, Accepted)
- ADR-0017 — schema migration remains caller-owned
- Explicit lease exit modes (ADR-0018) — `Release(ctx, token, mode)` with `ExitFinished`, `ExitAbandoned`, `ExitRetired`; `ReadCheckpoint` returns a `Checkpoint` with `PrevExit` and `PrevHolderID`; `worker.Runner`, `leader.Elect`, and `pool` record each run's outcome, release on a cleanup context bounded by `CleanupTimeout`, and `pool` gains `IdleInterval`, `RerunInterval`, and slot retirement via `ErrRetire`. PostgreSQL schema migration required — see `UPGRADING.md`

### v0.7 — Planned

- Drop the deprecated `clean_handoff` column (#86)
- Run the integration suite against both the memory and PostgreSQL backends (#87)
- Bound each renewal attempt so `stopRenewal` cannot hang on a stalled `Renew` (#88)
- Define PostgreSQL isolation-level behavior for contended `Acquire` (#89)
- Reject TTLs below a minimum that PostgreSQL intervals can represent (#90)
- Report a nil backend from `NewVacuum` instead of panicking in `Sweep` (#91)
- Make `Forget` and `Vacuum.Sweep` outcomes observable (#92)
- Expose the tracked lease window to work code (#93)
- Reject empty and duplicate work IDs in `pool.New` (#97)

### Unscheduled (post-1.0)

- Redis backend
- etcd backend

---

## Architecture Decision Records

All significant design decisions are captured in `docs/adr/`. Each ADR documents the context,
the decision made, and the consequences — including the alternatives that were rejected and why.

| ADR | Title | Status |
|-----|-------|--------|
| [0001](adr/0001-backend-interface-no-close.md) | Backend interface does not include Close | Accepted |
| [0002](adr/0002-token-inspectable-via-methods.md) | Token fields unexported, exposed via accessor methods and fmt.Stringer | Accepted |
| [0003](adr/0003-checkpoint-serialization-raw-bytes.md) | Checkpoint serialization is raw []byte — caller owns the format | Accepted |
| [0004](adr/0004-renewal-loop-managed-goroutine.md) | StartRenewal is a managed goroutine returning (renewCtx, stopRenewal) | Accepted |
| [0005](adr/0005-acquire-default-returns-err-lease-held.md) | Acquire returns ErrLeaseHeld immediately by default; wait+retry is opt-in | Accepted |
| [0006](adr/0006-backend-acquire-single-attempt.md) | Backend.Acquire is single-attempt; the wait+retry loop lives in library core | Accepted |
| [0007](adr/0007-observer-config-field.md) | LeaseObserver injected via Config field; nil installs no-op | Accepted |
| [0008](adr/0008-clock-interface-memory-backend.md) | Clock interface for in-memory backend testability | Accepted |
| [0009](adr/0009-checkpoint-subpackage-codec-interface.md) | checkpoint subpackage with Codec interface and generic helpers | Accepted |
| [0010](adr/0010-leader-fn-signature-and-acquire-semantics.md) | leader.Elect fn receives no Token; acquire semantics are caller-controlled (amended v0.6: `OnRelinquished` only after `ExitFinished`/`ExitRetired`) | Accepted |
| [0011](adr/0011-pool-scope-and-permanent-error-interface.md) | pool scope is cross-process; permanent slot failure signals via interface (amended v0.4: Observer, Permanent, distinct sentinels, ErrAllSlotsDead; amended v0.6: slot pacing, retirement, cleanup) | Accepted |
| [0012](adr/0012-release-expires-lease-immediately.md) | Release expires the lease immediately — successor acquires without TTL wait (amended v0.6: declared exit modes) | Accepted |
| [0013](adr/0013-renewal-goroutine-retry-policy.md) | Renewal goroutine retry policy bounded by the lease window (amended v0.6: window tracking, terminal `ErrLeaseExpired`) | Accepted |
| [0014](adr/0014-backend-slice-ownership-contract.md) | Backend slice ownership contract — defensive copies required | Accepted |
| [0015](adr/0015-backend-conformance-suite.md) | Backend conformance suite — RunSuite against all backends | Accepted |
| [0016](adr/0016-row-lifecycle-global-fencing-sequence.md) | Row lifecycle: global fencing sequence (v0.5); retention — `Forget` / `Vacuum.Sweep` (v0.6; amended v0.6: retention keys on declared exits) | Accepted |
| [0017](adr/0017-schema-migration-caller-owned.md) | Schema migration remains caller-owned | Accepted |
| [0018](adr/0018-explicit-lease-exit-modes.md) | Explicit lease exit modes — holders declare how they exit; successors read `PrevExit` | Accepted |

ADR-0007 and ADR-0010 carry v0.4 amendments (observer event-struct redesign; leader lifecycle
callbacks). ADR-0004 and ADR-0005 carry v0.5 amendments (renewal bounded retry; acquire ctx.Err()
propagation). ADR-0013 shipped in v0.5; ADR-0016's global-fencing-sequence component shipped in
v0.5, and its retention component (`Forget` / `Vacuum.Sweep`) is Accepted for v0.6. ADR-0018
(explicit exit modes) is Accepted for v0.6 and carries dated amendments to ADR-0010, ADR-0011,
ADR-0012, ADR-0013, and ADR-0016.

---

*Last updated: June 2026 — v0.5 (v0.5.0)*
