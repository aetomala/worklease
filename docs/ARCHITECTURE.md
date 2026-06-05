# worklease — Architecture

This document explains the design decisions, concepts, and internal mechanics of the `worklease`
library. It is the starting point for contributors and for users who want to understand what the
library does and why it is built the way it is.

## Table of Contents

- [Overview](#overview)
- [The Problem in Depth](#the-problem-in-depth)
- [Core Concepts](#core-concepts)
- [Design Principles](#design-principles)
- [Project Structure](#project-structure)
- [API Architecture](#api-architecture)
- [PostgreSQL Backend](#postgresql-backend)
- [In-Memory Backend](#in-memory-backend)
- [Renewal Loop](#renewal-loop)
- [Acquire Flow](#acquire-flow)
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
that makes it possible for a new worker to resume where the previous worker left off, safely,
without duplicate writes and without starting from scratch.

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
  the beginning. If the operation is not idempotent, you now have duplicate writes.
- If Worker A was slow (not dead) and its lease simply expired, it may still be writing.
  Worker B's writes and Worker A's writes interleave. State is corrupted.

With `worklease`:

- Worker A writes progress state atomically with every lease renewal. The last checkpoint
  is guaranteed to be written before any renewal succeeds.
- Worker B acquires the lease, reads the checkpoint, and resumes from exactly where Worker A
  left off. It also knows whether Worker A called `Release` explicitly (clean handoff) or
  whether the lease simply expired (crash recovery). These are different situations requiring
  different handling.
- If Worker A is a zombie (slow, not dead), its next write attempt will be rejected — Worker B
  was issued a higher fencing token, and Worker A's writes with a stale token are silently
  rejected by the backend.

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
backend holds token 5. This prevents the zombie write problem.

The library owns fencing logic entirely. Callers cannot read or construct fencing tokens
directly — they receive a `Token` value and pass it back to operations. The library validates
the token on every write. See [ADR-0002](adr/0002-token-inspectable-via-methods.md).

### Checkpoint

Progress state written atomically with lease renewal in a single backend operation. The
checkpoint is a raw `[]byte` blob — the library stores it and hands it to the next owner;
it has no opinion on encoding. See [ADR-0003](adr/0003-checkpoint-serialization-raw-bytes.md).

The atomicity guarantee is critical: either the checkpoint and the renewal both succeed, or
neither does. This means the lease cannot be extended without writing a checkpoint, and a
checkpoint cannot be written without extending the lease. Liveness and state-save are a
single operation.

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

**3. Library owns fencing, callers own state.** Callers cannot bypass fencing — it is
unconditionally enforced on every write. Callers own checkpoint serialization, claim
structure, and recovery logic.

**4. Backend is single-attempt; core owns retry.** Backend methods map to single storage
operations. Retry policy lives in the library core and is consistent across all backends. See
[ADR-0006](adr/0006-backend-acquire-single-attempt.md).

**5. No resource ownership surprise.** `postgres.New(db)` does not close `db`. The caller
constructs; the caller closes. See [ADR-0001](adr/0001-backend-interface-no-close.md).

**6. Observability without framework.** `Token` implements `fmt.Stringer`. Accessor methods
on `Token` expose all fields needed for logs, traces, and metrics. No observability interfaces
to inject in v0.1 — hooks are left clean for v0.2.

**7. The code is the documentation.** Exported identifiers are documented, packages have doc
comments, comments explain why not what, errors carry context, interfaces specify contracts.

**8. Test at the interface boundary.** The `Backend` interface makes both the PostgreSQL and
in-memory backends fully testable in isolation. Integration tests for PostgreSQL require a real
database. Unit tests for the library core use the in-memory backend.

---

## Project Structure

| Package | Purpose | Status |
|---------|---------|--------|
| `github.com/aetomala/worklease` | `Lease` interface, `Token` type, options, errors, `New` constructor | v0.1 |
| `github.com/aetomala/worklease/backend` | Internal `Backend` interface and `LeaseRecord` type | v0.1 |
| `github.com/aetomala/worklease/backend/postgres` | PostgreSQL-backed production backend | v0.1 |
| `github.com/aetomala/worklease/backend/memory` | In-memory backend for unit testing | v0.1 |

### File layout

```
worklease/
├── doc.go          # Package-level documentation
├── lease.go        # Lease interface, Token, AcquireOption, RenewalOption, error sentinels
├── worklease.go    # New() constructor, Config struct
├── acquire.go      # Acquire — wait+retry loop for WithWaitForLease
├── renewal.go      # StartRenewal — managed renewal goroutine
├── backend/
│   ├── backend.go          # Backend interface, LeaseRecord — internal to library
│   ├── postgres/
│   │   ├── postgres.go     # PostgreSQL backend implementation
│   │   ├── schema.sql      # CREATE TABLE statement
│   │   └── postgres_test.go
│   └── memory/
│       ├── memory.go       # In-memory backend implementation
│       └── memory_test.go
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
response is to stop immediately — any further writes will be rejected.

### Config and Constructor

```go
type Config struct {
    TTL      time.Duration // Lease TTL; required.
    HolderID string        // Unique identity for this worker; required.
}

func New(backend Backend, cfg Config) (Lease, error)
```

`HolderID` should uniquely identify the worker process. A hostname, a Kubernetes Pod name,
or a UUID generated at startup are all good choices. It appears in `Token.HolderID()` and
in the `holder_id` column in the backend, making it possible to track which worker holds a
lease at any point in time.

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
```

Same fencing gate. No `checkpoint` column update — the existing checkpoint is preserved.

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

---

## In-Memory Backend

The in-memory backend implements the same fencing token semantics as the PostgreSQL backend.
It is intended for unit tests within a single process — it is not safe for use across
processes.

```go
import "github.com/aetomala/worklease/backend/memory"

backend := memory.New()
```

No `Close` method — no cleanup required. Time-based expiry uses `time.Now()` from the local
clock. Tests that exercise lease expiry must use real sleep or a clock injection (v0.2 concern;
see [Residual Risks — R3](#residual-risks)).

---

## Renewal Loop

`StartRenewal` starts a managed renewal goroutine and returns a derived context and a stop
function. This is the recommended way to hold a lease for the duration of a long-running
operation.

```go
renewCtx, stopRenewal := lease.StartRenewal(ctx, token)
defer stopRenewal()

// All downstream work uses renewCtx so that fencing propagates automatically.
if err := doWork(renewCtx, ...); err != nil {
    return err
}

// Use the original ctx for Release — renewCtx may be cancelled by the time work finishes.
return lease.Release(ctx, token)
```

### Context Lifecycle

The `renewCtx` is cancelled when any of the following occur:

| Event | Signal |
|-------|--------|
| Renewal receives `ErrFenced` | `renewCtx` cancelled — this worker is a zombie, stop all work |
| Renewal fails (non-fencing error) | `renewCtx` cancelled — lease state is uncertain |
| Parent `ctx` is cancelled | `renewCtx` cancelled — propagated from parent |
| `stopRenewal()` is called | `renewCtx` is **not** cancelled — clean shutdown |

The distinction between `stopRenewal()` (clean) and context cancellation (fencing or error)
is intentional. Downstream code can distinguish "work is done" from "we were fenced" by
checking whether `stopRenewal()` was called before context cancellation occurred.

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

## Testing Approach

### Unit Tests — In-Memory Backend

Library core tests (`acquire.go`, `renewal.go`, `worklease.go`) use the in-memory backend.
No database required. All tests use table-driven format with named cases.

```go
tests := []struct {
    name    string
    workID  string
    wantErr error
}{
    {"returns ErrLeaseHeld when lease is held", "job-1", worklease.ErrLeaseHeld},
    {"returns ErrFenced when token is stale",   "job-2", worklease.ErrFenced},
}
```

Test names are sentences describing the scenario.

### Integration Tests — PostgreSQL Backend

`backend/postgres/postgres_test.go` requires a real PostgreSQL instance. Set
`WORKLEASE_TEST_DSN` to a valid DSN:

```bash
WORKLEASE_TEST_DSN="postgres://user:pass@localhost/worklease_test?sslmode=disable" \
    go test -race -tags integration ./backend/postgres/...
```

Integration tests verify the SQL operations that are not testable with the in-memory backend:
expiry semantics via `NOW()`, the `ON CONFLICT` upsert behavior, and `TIMESTAMPTZ` precision.

### Race Detection

All tests run with `-race`. Concurrent access to the in-memory backend and the renewal
goroutine are areas that benefit most from race detection.

```bash
go test -race ./...
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

**R3 — In-memory backend real-time expiry.** The in-memory backend uses `time.Now()` for
expiry checks. Tests that exercise lease expiry must either use real sleep or inject a clock.
Clock injection is a v0.2 concern for the in-memory backend.

---

## Roadmap

### v0.1 — Complete

- `Lease` interface: `Acquire`, `Checkpoint`, `Renew`, `Release`, `ReadCheckpoint`, `StartRenewal`
- `Token` value type — unexported fields, accessor methods, `fmt.Stringer`
- `AcquireOption`: `WithWaitForLease`, `WithPollInterval`
- `RenewalOption`: `WithRenewalInterval`
- PostgreSQL backend — fencing via conditional `UPDATE WHERE fencing_token = $n`
- In-memory backend — for unit testing
- Error sentinels: `ErrFenced`, `ErrLeaseHeld`, `ErrLeaseExpired`
- ADR-0001 through ADR-0006

### v0.2 — Planned

- Redis backend
- Clock injection in in-memory backend (addresses R3)
- Observability interface injection (logger, metrics)
- `Token` test constructor (addresses the table-driven test limitation from ADR-0002)

### Future

- etcd backend
- Distributed progress aggregation across workers

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

---

*Last updated: June 2026 — v0.1*
