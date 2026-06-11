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
  state. It also knows whether Worker A called `Release` explicitly (clean handoff) or
  whether the lease simply expired (crash recovery). These are different situations requiring
  different handling.
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

### Release marks intent, not immediate transfer

`Release` sets `clean_handoff = TRUE` in the lease store and returns. It does not expire the
lease or immediately transfer ownership. A successor worker still cannot acquire the lease
until `expires_at < NOW()` — unless it uses `WithWaitForLease` with a short poll interval.

"Clean handoff" is a semantic flag that tells the successor the previous owner finished
intentionally. It is not a mechanism for instant ownership transfer. If the TTL is 30 seconds
and Worker A calls `Release` at second 5, Worker B cannot acquire until second 30 unless the
poll loop catches the expiry. Choose TTLs and poll intervals that match the handoff latency
your application can tolerate.

### Recovery logic lives in caller code

`worklease` delivers the checkpoint bytes and the `cleanHandoff` flag. What those bytes mean,
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
state. The call returns two values: the state bytes, and a `cleanHandoff bool`.

- `cleanHandoff == true`: the previous owner called `Release` explicitly. The checkpoint
  contains the final state of completed work. The new owner can resume cleanly.
- `cleanHandoff == false`: the previous owner's lease expired without a `Release`. The
  checkpoint contains the last known partial state. The new owner should validate that
  state before resuming — the previous owner may have been mid-step.

These are semantically different situations. The library makes the distinction explicit and
forces callers to handle both. Treating crash recovery the same as clean handoff is a common
source of subtle bugs in distributed systems.

---

## Design Principles

**1. Coordination semantics over convenience.** The library does one thing correctly:
checkpointed lease handoff with fencing. It has no serialization opinions, no framework
requirements, and no platform dependencies.

**2. Explicit over implicit.** `cleanHandoff bool` forces callers to handle crash recovery
differently from clean handoff. `nil` state on fresh acquisition is explicit, not an error.
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
defaulting to a no-op. No observability framework is required or assumed.
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
| `github.com/aetomala/worklease/worker` | `Runner` — manages acquire/checkpoint/release lifecycle | v0.2 |
| `github.com/aetomala/worklease/checkpoint` | `Codec` interface and typed `Encode[T]`/`Decode[T]` helpers | v0.2 |
| `github.com/aetomala/worklease/leader` | `Elect` — simplified leader election without checkpoint state | v0.3 |
| `github.com/aetomala/worklease/pool` | `Pool` — distributes work IDs across competing processes | v0.3 |

### File layout

```
worklease/
├── doc.go              # Package-level documentation
├── lease.go            # Lease interface, Token, LeaseObserver, AcquireOption, RenewalOption, errors
├── worklease.go        # New() constructor, Config struct, noopObserver
├── acquire.go          # Acquire — wait+retry loop for WithWaitForLease
├── renewal.go          # StartRenewal — managed renewal goroutine
├── backend/
│   ├── backend.go          # Backend interface, LeaseRecord — internal to library
│   ├── postgres/
│   │   ├── postgres.go     # PostgreSQL backend implementation
│   │   ├── schema.sql      # CREATE TABLE statement
│   │   └── postgres_test.go
│   └── memory/
│       ├── memory.go       # In-memory backend; Clock interface, Option, WithClock
│       └── memory_test.go
├── worker/
│   └── runner.go       # Runner, WorkFn, RunnerConfig, NewRunner
├── checkpoint/
│   └── codec.go        # Codec, JSONCodec, Encode[T], Decode[T]
├── leader/             # v0.3
│   └── leader.go       # Elect, Config
├── pool/               # v0.3
│   └── pool.go         # Pool, WorkFn, PermanentError, Config, New
├── testutil/
│   └── mock_backend.go # Generated Backend mock (mockgen)
├── examples/
│   ├── subscription-cancellation/
│   └── cross-tenant-migration/
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
    Release(ctx context.Context, token Token) error
    ReadCheckpoint(ctx context.Context, token Token) (state []byte, cleanHandoff bool, err error)
    StartRenewal(ctx context.Context, token Token, opts ...RenewalOption) (renewCtx context.Context, stopRenewal func())
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
    // ErrFenced is returned by Checkpoint or Renew when a higher fencing token has been
    // issued. This worker has been superseded. Stop immediately.
    ErrFenced = errors.New("worklease: fenced — lease acquired by another holder")

    // ErrLeaseHeld is returned by Acquire when the lease is currently held and
    // WithWaitForLease was not passed, or the context expired while waiting.
    ErrLeaseHeld = errors.New("worklease: lease is currently held")

    // ErrLeaseExpired is returned when the lease expired before the operation completed.
    ErrLeaseExpired = errors.New("worklease: lease has expired")
)
```

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
    Release(ctx context.Context, record LeaseRecord) error
    ReadCheckpoint(ctx context.Context, record LeaseRecord) (state []byte, cleanHandoff bool, err error)
}
```

All backend methods are single-attempt. The backend never retries. This is a hard contract —
see [ADR-0006](adr/0006-backend-acquire-single-attempt.md).

`Backend` does not include `Close`. See [ADR-0001](adr/0001-backend-interface-no-close.md).

`LeaseRecord` is the currency between the library core and the backend. It mirrors `Token`
but is the backend's internal representation — callers never see it. The library wraps
`LeaseRecord` into `Token` before returning to callers.

---

## PostgreSQL Backend

### Schema

```sql
CREATE TABLE worklease_leases (
    work_id         TEXT PRIMARY KEY,
    holder_id       TEXT NOT NULL,
    fencing_token   BIGINT NOT NULL DEFAULT 1,
    expires_at      TIMESTAMPTZ NOT NULL,
    checkpoint      BYTEA,
    clean_handoff   BOOLEAN NOT NULL DEFAULT FALSE,
    acquired_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

`fencing_token` starts at 1 and increments on every acquisition. `checkpoint` is nullable —
`NULL` on first acquisition means no prior state. `clean_handoff` is reset to `FALSE` on every
acquisition and set to `TRUE` only by an explicit `Release`.

### Acquire — Upsert with Expiry Guard

```sql
INSERT INTO worklease_leases (work_id, holder_id, fencing_token, expires_at, checkpoint, clean_handoff)
VALUES ($1, $2, 1, NOW() + $3, NULL, FALSE)
ON CONFLICT (work_id) DO UPDATE
SET holder_id     = EXCLUDED.holder_id,
    fencing_token = worklease_leases.fencing_token + 1,
    expires_at    = EXCLUDED.expires_at,
    checkpoint    = worklease_leases.checkpoint,   -- preserve last checkpoint
    clean_handoff = worklease_leases.clean_handoff, -- preserve until successor reads
    updated_at    = NOW()
WHERE worklease_leases.expires_at < NOW()           -- only if expired
```

The `WHERE` clause is the fencing gate. If the lease is held and unexpired, the condition is
false — zero rows are updated. The backend returns `ErrLeaseHeld`. If the lease is absent or
expired, the upsert succeeds and the fencing token is incremented atomically.

**Clock note**: `NOW()` is the PostgreSQL server's clock — not the worker's clock. Expiry
decisions are made by the database, not by the client. This is intentional; see
[Residual Risks — R1](#residual-risks).

### Checkpoint — Conditional Update

```sql
UPDATE worklease_leases
SET checkpoint    = $1,
    expires_at    = NOW() + $2,
    clean_handoff = FALSE,
    updated_at    = NOW()
WHERE work_id       = $3
  AND holder_id     = $4
  AND fencing_token = $5
```

Zero rows updated means the fencing token in the database is higher than the one the worker
holds — the worker has been superseded. The backend returns `ErrFenced`.

The `AND fencing_token = $5` clause is the fencing enforcement. It is evaluated atomically by
PostgreSQL. There is no Lua script, no optimistic retry, no distributed clock dependency.

### Renew — Same Pattern, No Checkpoint Write

```sql
UPDATE worklease_leases
SET expires_at = NOW() + $1,
    updated_at = NOW()
WHERE work_id       = $2
  AND holder_id     = $3
  AND fencing_token = $4
  AND expires_at    > NOW()
```

Same fencing gate, plus an expiry guard. If the lease has already expired, zero rows are
updated and `ErrLeaseExpired` is returned — the renewal goroutine cancels `renewCtx` and
signals to downstream work that the lease is lost. No bare `time.Now()` — the database's
`NOW()` is authoritative.

### Release — Set clean_handoff

```sql
UPDATE worklease_leases
SET clean_handoff = TRUE,
    updated_at    = NOW()
WHERE work_id       = $1
  AND holder_id     = $2
  AND fencing_token = $3
```

The checkpoint column is not touched. The final checkpoint written by the last `Checkpoint`
call survives. The successor will read it alongside `cleanHandoff = true`.

`Release` does not expire the lease or shorten the TTL. The successor cannot acquire until
`expires_at < NOW()`. See [What worklease Does Not Solve — Release marks intent, not immediate
transfer](#what-worklease-does-not-solve).

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
return lease.Release(ctx, token)
```

### Context Lifecycle

The `renewCtx` is cancelled when any of the following occur:

| Event | Signal |
|-------|--------|
| Renewal receives `ErrFenced` | `renewCtx` cancelled — this worker is a zombie, stop all work |
| Renewal receives `ErrLeaseExpired` | `renewCtx` cancelled — lease expired without a competitor |
| Renewal fails (non-fencing error) | `renewCtx` cancelled — lease state is uncertain |
| Parent `ctx` is cancelled | `renewCtx` cancelled — propagated from parent |
| `stopRenewal()` is called | `renewCtx` is **not** cancelled — clean shutdown |

The distinction between `stopRenewal()` (clean) and context cancellation (fencing or error)
is intentional. Downstream code can distinguish "work is done" from "we were fenced" by
checking whether `stopRenewal()` was called before context cancellation occurred.

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

### Why Fail-Fast is the Default

Blocking by default would hide contention. A worker that expects to immediately acquire a
lease would silently wait instead of failing visibly. Fail-fast makes contention observable.
`WithWaitForLease()` at the call site makes the intent clear: this caller expects to queue
behind the current holder.

---

## Observability — LeaseObserver

`LeaseObserver` is a five-method hook interface injected via `Config.Observer`. The library
calls it synchronously after every lease operation. Implementations must not block or panic.
When `Config.Observer` is nil, the library installs a no-op observer — no nil check is ever
required at call sites.

```go
type LeaseObserver interface {
    OnAcquire(ctx context.Context, workID string, token Token, err error)
    OnCheckpoint(ctx context.Context, token Token, size int, err error)
    OnRenew(ctx context.Context, token Token, err error)
    OnRelease(ctx context.Context, token Token, err error)
    OnFenced(ctx context.Context, token Token)
}
```

When a fencing event occurs, `OnCheckpoint` (or `OnRenew`) fires first, then `OnFenced`. This
order is mandatory — observers can rely on it to distinguish a fencing event from a generic
error in `OnCheckpoint`/`OnRenew`.

`OnFenced` is not called for `Release` — a fenced release is surfaced via `OnRelease` only.

A typical use is a Prometheus implementation: each callback increments a labeled counter or
records a histogram. The `Token` parameter gives access to `WorkID()`, `HolderID()`, and
`FencingToken()` for structured labelling.

See [ADR-0007](adr/0007-observer-config-field.md).

---

## worker.Runner — Lifecycle Management

`worker.Runner` manages the full acquire → checkpoint → release lifecycle so callers write
only the work function. Without `Runner`, every caller must write the same 30-plus-line
scaffold; `Runner` reduces that to a single `Run` call.

```go
import "github.com/aetomala/worklease/worker"

// WorkFn is the work function signature accepted by Runner.Run.
// ctx is the renewal context — cancelled on fencing or renewal failure.
// prior contains the last checkpointed state from the previous holder (nil on first run).
// cleanHandoff is true when the previous holder released intentionally.
// Return final state to checkpoint after work completes; return nil to skip final checkpoint.
type WorkFn func(ctx context.Context, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error)

runner, err := worker.NewRunner(worker.RunnerConfig{
    Lease:  lease,
    WorkFn: myWorkFn,
})

err = runner.Run(ctx, "onboarding:tenant-abc")
```

`Runner.Run` performs these steps in order:

1. `lease.Acquire` — obtains the lease
2. `lease.ReadCheckpoint` — reads prior state and `cleanHandoff` flag
3. `lease.StartRenewal` — starts the managed renewal goroutine
4. Calls `WorkFn` with `renewCtx`, the token, prior state, and `cleanHandoff`
5. `stopRenewal()` — explicit stop before release
6. If `WorkFn` returned non-nil state: `lease.Checkpoint` — writes final state
7. `lease.Release` — surrenders the lease (skipped on `ErrFenced`)

`Token` is passed to `WorkFn` so callers can make mid-work `Checkpoint` calls for fine-grained
progress recording without stepping outside the `Runner` API.

**Error contract:**

- `ErrFenced` from any step — `Runner` returns `ErrFenced`; `Release` is not called.
- `WorkFn` returning `ErrFenced` — same as above.
- Any other error — `Runner` calls `Release` before returning.

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
    // ctx is cancelled if the lease is fenced or renewal fails.
    return runScheduler(ctx)
})
```

`Elect` manages the full lifecycle: acquire, start renewal, call `fn` with `renewCtx`, stop
renewal explicitly, and release. `fn` receives no `Token` — leader election is presence-only.
Callers who need to make fencing-aware writes during work should use `worker.Runner` instead.

Blocking vs fail-fast acquire behavior is caller-controlled via `cfg.AcquireOptions` —
`Elect` does not force `WithWaitForLease`. Pass it explicitly to block until leadership is
available.

See [ADR-0010](adr/0010-leader-fn-signature-and-acquire-semantics.md).

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
}, func(ctx context.Context, workID string, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
    return processShard(ctx, workID, prior, cleanHandoff)
})

err = p.Run(ctx)  // blocks until ctx is cancelled
```

`Pool.Run` starts one goroutine per work ID. Each goroutine loops: acquire → work → release,
with `BackoffInterval` delay on non-fencing errors. When `ctx` is cancelled, all goroutines
exit and `Run` returns after all active slots have completed or released.

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

`pool` provides no concrete implementation — callers define their own error type. `errors.As`
unwrapping is used to detect it, so the error composes correctly with `fmt.Errorf` wrapping
chains.

**`WithWaitForLease` is prohibited** in `pool.Config.AcquireOptions`. The pool manages its
own acquisition loop — if a slot goroutine blocks inside `Runner.Run` waiting for a lease, it
cannot respond to context cancellation during shutdown. `pool.New` returns `ErrConfigInvalid`
if `WithWaitForLease` is detected.

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

### Race Detection and CI

All tests run with `-race`. The CI pipeline runs two jobs: `lint + govulncheck` and
`test + postgres service container`. The in-memory backend and the renewal goroutine are the
areas that benefit most from race detection.

```bash
make ci   # lint, build, test (all three targets)
```

---

## Residual Risks

**R1 — Clock skew across workers.** Lease expiry is determined by `expires_at TIMESTAMPTZ`
in PostgreSQL, which uses the database server's clock. A worker that uses its local clock to
decide whether to attempt `Acquire` may observe a different expiry than the database. The
library addresses this by always letting the database determine expiry — the `WHERE expires_at < NOW()`
clause in `Acquire` uses PostgreSQL's `NOW()`, not the client's clock. Worker-side clocks are
never used for expiry decisions.

**R2 — Renewal goroutine leak on missed stopRenewal.** If the caller does not call
`stopRenewal()` after completing work, the renewal goroutine runs until the parent context is
cancelled. The goroutine will eventually self-terminate when the lease expires or is acquired
by another holder (fencing). Callers should `defer stopRenewal()` immediately after
`StartRenewal` returns to prevent this. The godoc for `StartRenewal` makes this explicit.
`worker.Runner` handles this correctly by design.

**R3 — In-memory backend real-time expiry.** Resolved in v0.2. The `Clock` interface is
injected via `memory.WithClock`. All expiry paths inside `memory.go` use `mb.clock.Now()`;
no bare `time.Now()` remains. Tests exercise expiry deterministically without real sleep.

**R4 — External side effects are not fenced.** `worklease` fences writes to the lease store.
It does not fence external mutations — Stripe charges, email sends, S3 writes, or domain table
updates outside the lease schema. A zombie worker can initiate external mutations between lease
expiry and its next `Checkpoint` or `Renew` call. `renewCtx` cancellation propagates fencing
into Go context-aware code, but does not cancel in-flight network calls already dispatched.
Callers must make every external mutation idempotent, use an outbox pattern, or enforce
idempotency keys at each downstream system.

**R5 — Checkpoint ordering and external effect sequencing.** Two failure modes exist at the
boundary between checkpointing and external effects:

- Effect before checkpoint: Worker A calls an external API, then crashes before checkpointing.
  Worker B resumes from the previous checkpoint and re-executes the step. The external effect
  fires twice.
- Checkpoint before effect: Worker A checkpoints a step as complete, then crashes before the
  external effect. Worker B resumes after that checkpoint and skips the effect. The external
  effect never fires.

Neither case is a library defect — they are inherent to at-least-once execution between
checkpoint boundaries. The correct mitigation is idempotent external effects with
checkpoint-aligned boundaries: checkpoint after the effect completes successfully, not before.

**R6 — pool WithWaitForLease misuse.** Passing `worklease.WithWaitForLease()` in
`pool.Config.AcquireOptions` causes each slot goroutine to block inside `runner.Run` during
acquisition, preventing the goroutine from responding to context cancellation until the lease
becomes available. This breaks clean pool shutdown. `pool.New` returns `ErrConfigInvalid` if
`WithWaitForLease` is detected in `AcquireOptions`.

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

### v0.3 — In Design

- `HasWaitForLease([]AcquireOption) bool` — option inspection helper for higher-level packages
- `Codec` interface method rename: `Encode`/`Decode` → `Marshal`/`Unmarshal` (breaking change; see `UPGRADING.md`)
- `leader` package — `Elect`, `Config`
- `pool` package — `Pool`, `WorkFn`, `PermanentError`, `Config`
- ADR-0010, ADR-0011

### v0.4 — Planned

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
| [0010](adr/0010-leader-fn-signature-and-acquire-semantics.md) | leader.Elect fn receives no Token; acquire semantics are caller-controlled | Accepted |
| [0011](adr/0011-pool-scope-and-permanent-error-interface.md) | pool scope is cross-process; permanent slot failure signals via interface | Accepted |

---

*Last updated: June 2026 — v0.2.0 released, v0.3 in design*
