# ADR-0005: Acquire returns ErrLeaseHeld immediately by default; wait+retry is opt-in

**Status:** Accepted
**Date:** 2026-06-05

## Context

When a caller calls `Acquire` on a lease that is currently held and unexpired, the library must
decide the default behavior. Two behaviors were considered:

1. Return `ErrLeaseHeld` immediately — fail-fast, no blocking.
2. Block until the lease becomes available or the context deadline is reached.

Both patterns are valid depending on the caller's use case:
- A worker that only processes work it can immediately claim prefers fail-fast.
- A worker that should queue up behind the current holder prefers blocking.

## Decision

The default behavior is to return `ErrLeaseHeld` immediately. Blocking behavior is opt-in via
`WithWaitForLease()`:

```go
// Default: returns ErrLeaseHeld immediately if the lease is held.
token, err := lease.Acquire(ctx, "work-item-1")

// Opt-in: blocks until the lease becomes available or ctx is cancelled.
token, err := lease.Acquire(ctx, "work-item-1", worklease.WithWaitForLease())

// Opt-in with custom poll interval:
token, err := lease.Acquire(ctx, "work-item-1",
    worklease.WithWaitForLease(),
    worklease.WithPollInterval(5*time.Second),
)
```

The retry loop for `WithWaitForLease` lives in the library core (`acquire.go`), not in the
backend. `Backend.Acquire` is always single-attempt. See ADR-0006.

## Rationale

**Fail-fast is the safer default.** A caller that acquires a lease to claim a work item should
know immediately if someone else holds it. Blocking by default would hide contention — a caller
that expects immediate acquisition would silently block instead of failing visibly.

**Explicit opt-in makes intent clear.** `WithWaitForLease()` in the call site makes it
obvious to a code reader that this acquisition is expected to queue behind the current holder.
Without the option, the intent is invisible.

**Context deadline is the natural bound for blocking.** The caller sets the deadline; the
library respects it. No library-internal timeout is needed — the wait loop exits when the
context is cancelled or reaches its deadline.

**Poll interval should be caller-configurable.** The default of 2 seconds is appropriate for
most lease TTLs (30s–5min), but a caller with a 5-second TTL may want a 1-second poll. The
option gives callers control without overloading `Acquire`'s signature.

## Consequences

**Positive:**
- Fail-fast by default — contention is visible immediately, not hidden behind blocking.
- Opt-in blocking is explicit and readable at the call site.
- Context deadline governs the wait — no additional timeout configuration required.
- Poll interval is tunable for callers with short TTLs.

**Negative:**
- Callers that always want blocking behavior must pass `WithWaitForLease()` on every call.
  This is intentional — it keeps the default safe and makes blocking opt-in.
- Poll-based waiting adds latency relative to a push-based notification model. A push model
  would require backend-specific notification mechanisms (PostgreSQL LISTEN/NOTIFY, Redis
  keyspace notifications) that cannot be expressed in the generic `Backend` interface.

## References

- `acquire.go` — wait+retry loop implementation
- `lease.go` — `AcquireOption`, `WithWaitForLease`, `WithPollInterval` definitions
- `backend/backend.go` — `Backend.Acquire` is single-attempt; see ADR-0006
