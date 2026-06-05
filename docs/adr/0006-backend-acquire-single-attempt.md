# ADR-0006: Backend.Acquire is single-attempt; the wait+retry loop lives in library core

**Status:** Accepted
**Date:** 2026-06-05

## Context

When `WithWaitForLease()` is passed to `Acquire`, the library must poll the backend until the
lease becomes available. The retry loop could live in two places:

1. Inside each `Backend` implementation — each backend owns its own retry semantics.
2. In the library core (`acquire.go`) — backends are always single-attempt; retry is a
   library-level concern above the backend.

## Decision

`Backend.Acquire` is always single-attempt. If the lease is held, it returns `ErrLeaseHeld`
immediately — it does not block or retry. The wait+retry loop lives in `acquire.go` in the
library core.

```go
// Backend.Acquire — single attempt contract:
// If the lease is absent or expired, creates/overwrites it and returns the resulting LeaseRecord.
// If the lease is held and unexpired, returns ErrLeaseHeld immediately.
// Never blocks. Never retries.
Acquire(ctx context.Context, workID, holderID string, ttl time.Duration) (LeaseRecord, error)
```

The library core in `acquire.go` calls `Backend.Acquire` in a ticker loop when
`WithWaitForLease()` is active, exiting when it succeeds or the context is cancelled.

## Rationale

**Retry policy should be consistent across all backends.** If each backend implemented its own
retry loop, subtle differences in poll interval, backoff, and context-cancellation handling
would create inconsistent behavior depending on which backend was in use. A caller using the
PostgreSQL backend and a caller using the in-memory backend should observe identical retry
semantics.

**Backend implementations are simpler without retry logic.** A backend that only maps to a
single storage operation is straightforward to implement, test, and audit. A backend that also
owns a retry loop needs to handle ticker lifecycle, context cancellation, and poll interval
configuration — all without duplicating what the library core already knows.

**Single-attempt semantics are easier to test.** A backend unit test that calls `Acquire` on a
held lease checks that it returns `ErrLeaseHeld` — done. Testing a retry loop requires time,
tickers, and context manipulation that belongs in library core tests, not backend tests.

**The retry loop is a policy decision, not a storage decision.** Whether to wait 2 seconds or
5 seconds between attempts, and for how long to wait total, is a function of the caller's TTL
and context deadline — not of the underlying storage system. Keeping it in the library core
keeps the policy decision in one place.

## Consequences

**Positive:**
- Retry behavior is identical across all backends.
- Backend implementations are smaller and easier to test in isolation.
- `WithWaitForLease` poll interval configuration applies uniformly to all backends.
- New backends do not need to implement retry logic.

**Negative:**
- Backends cannot use backend-native push mechanisms (PostgreSQL `LISTEN/NOTIFY`, Redis
  keyspace events) to reduce polling latency. Push-based optimization would require either
  a richer `Backend` interface or backend-specific `Acquire` option handling — both out of
  scope for v0.1. The poll-based approach is correct and simple; push optimization is a
  future concern.

## References

- `acquire.go` — wait+retry loop implementation
- `backend/backend.go` — `Backend.Acquire` interface contract
- ADR-0005 — `WithWaitForLease` and `WithPollInterval` option design
