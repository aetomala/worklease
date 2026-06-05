# ADR-0004: StartRenewal is a managed goroutine returning (renewCtx, stopRenewal)

**Status:** Accepted
**Date:** 2026-06-05

## Context

A worker holding a lease must renew it periodically or lose ownership. Manual renewal calls
in application code are error-prone: it is easy to forget a renewal call in a long code path,
or to call `Renew` with the wrong interval. A managed renewal loop simplifies the common case.

Two approaches were considered:

1. Caller-managed — callers call `Renew` manually in a ticker loop. The library provides no
   loop abstraction.
2. Library-managed goroutine — `StartRenewal` starts a goroutine and returns a derived context.
   Fencing propagates automatically via context cancellation.

## Decision

`StartRenewal` starts a managed renewal goroutine and returns a derived context and a stop
function:

```go
StartRenewal(ctx context.Context, token Token, opts ...RenewalOption) (renewCtx context.Context, stopRenewal func())
```

The derived `renewCtx` is cancelled when:
- The renewal goroutine receives `ErrFenced` — this worker has been superseded.
- Any renewal attempt fails after the configured retry budget.
- The parent `ctx` is cancelled.

`stopRenewal()` halts the goroutine cleanly without cancelling `renewCtx`. Callers call it
when work completes normally, before calling `Release`.

Callers must use `renewCtx` for all downstream work so that fencing propagates automatically.
The original `ctx` (not `renewCtx`) must be used when calling `Release` — `renewCtx` may
already be cancelled by the time work completes.

## Rationale

**Automatic fencing propagation is the key safety property.** Without the managed goroutine
and derived context, a fenced worker would continue writing until it happened to call `Renew`
or `Checkpoint` and observe `ErrFenced`. With `renewCtx`, a fenced worker's downstream work
is cancelled automatically at the next renewal interval — no polling required in application code.

**Manual renewal is a correctness footgun.** A caller that calls `Renew` at the end of a long
processing step may hold a lease for far longer than the configured TTL before the renewal
reaches the backend. The library defaults to TTL/2 renewal interval, a well-known safe choice
for any TTL-based lease system.

**The stop function separates clean shutdown from fencing.** `stopRenewal()` is for normal
completion; context cancellation is for fencing and error. These are different signals with
different meanings. Conflating them (e.g., cancelling `renewCtx` on clean stop) would make it
impossible for downstream code to distinguish "work done" from "we were fenced."

**`context.Context` is the standard Go cancellation propagation mechanism.** Using it means
callers get cancellation propagation to all their context-aware operations (HTTP clients, SQL
queries, gRPC calls) without any library-specific plumbing.

## Consequences

**Positive:**
- Fencing propagates automatically to all downstream context-aware operations.
- Default renewal interval (TTL/2) is safe for all configurations without tuning.
- Clean shutdown and fencing are distinguishable signals.
- Renewal goroutine lifecycle is fully library-managed — no goroutine leaks if `stopRenewal`
  is called or the parent context is cancelled.

**Negative:**
- Callers must be disciplined about which context to pass to which operations. Using `renewCtx`
  for `Release` is a common mistake — documented in godoc with the explicit guidance to use
  the original `ctx` for `Release`. See residual risk R2 in the architecture document.
- The goroutine leaks if the caller neither calls `stopRenewal` nor cancels the parent context.
  The goroutine will self-terminate when the lease expires or is acquired by another holder,
  but the delay is TTL-length. Callers should `defer stopRenewal()` immediately after
  `StartRenewal` returns.

## References

- `renewal.go` — `StartRenewal` implementation
- `lease.go` — `Lease.StartRenewal` method signature and contract
- Architecture document — Renewal Loop Design and Residual Risks R2
