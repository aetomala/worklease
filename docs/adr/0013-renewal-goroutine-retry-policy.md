# ADR-0013: Renewal goroutine retries with backoff bounded by the lease window

**Status:** Accepted
**Date:** 2026-06-25

## Context

`StartRenewal` (ADR-0004) runs a managed goroutine that renews the lease at
`TTL/2`. In the original design, a single failed renewal — any non-`nil` error
from `Backend.Renew` — cancelled the renewal context and stopped the goroutine.

That made the renewal loop brittle. A transient backend blip (a dropped TCP
connection, a one-off statement timeout, a brief Postgres failover) killed an
otherwise-healthy lease even though the lease itself had not expired and the next
attempt would very likely have succeeded. The worker then lost ownership of work
it was still entitled to hold, for no durable reason.

At the same time, retrying forever is unsafe: once the lease window has actually
elapsed, the holder is no longer the owner and must stop. Any retry policy has to
be bounded by the one fact that matters — `token.ExpiresAt()`.

## Decision

The renewal goroutine retries a non-fencing `Renew` error with exponential
backoff plus additive jitter, bounded strictly by the lease window:

- On a non-fencing, non-nil error, the goroutine waits `base` (growing by ×2 up to
  a cap) plus jitter, then retries — but only while `time.Now()` is before
  `token.ExpiresAt()`. The window is checked both before and after each backoff
  sleep, so the goroutine never renews past the point where it could still be the
  legitimate owner.
- When the window is exhausted, the goroutine cancels the renewal context with
  cause `ErrLeaseWindowExhausted`, inspectable via `context.Cause(renewCtx)`.
- A fencing error (`ErrFenced`) is **never** retried — it cancels immediately with
  cause `ErrFenced`, after emitting `OnRenew` then `OnFenced`.
- A normal `stopRenewal()` leaves the context uncancelled (`context.Cause` is
  `nil`); a parent-context cancellation propagates as the parent's cause without
  the goroutine setting one.

The policy is configured with:

```go
func WithRenewalBackoff(initial, max time.Duration, jitter float64) RenewalOption
```

Defaults are `initial=100ms`, `max=5s`, `jitter=0.20`, all clamped to safe ranges.
`RenewEvent.Attempt` (1-based, incremented per retry) is threaded to `OnRenew` so
observers can see the retry sequence; direct `Renew` calls always report
`Attempt: 1`. The four goroutine lifecycle paths (normal stop / fencing / window
exhausted / parent cancelled) each set the documented `context.Cause`.

This decision amends ADR-0004, whose "any renewal attempt fails after the
configured retry budget" clause is now realized by this bounded-retry policy, and
whose `context.WithCancel` is replaced by `context.WithCancelCause`.

## Rationale

**The lease window is the only correct retry bound.** Retrying past
`token.ExpiresAt()` would let a stale holder believe it still owns work another
process may have already acquired. Bounding strictly by the window means retries
can only ever recover a lease that is still legitimately held.

**Backoff plus jitter avoids thundering-herd reconnection.** When a shared backend
recovers, many renewal goroutines would otherwise retry in lockstep. Exponential
growth plus additive jitter spreads the load.

**Fencing is categorically different from a transient error.** A fencing error is
a definitive statement that another holder has superseded this one — there is
nothing to retry. Conflating it with a transient error (by retrying both) would
delay the fenced worker's shutdown and weaken the core safety property of ADR-0004.

**Surfacing the cause via `context.Cause` keeps the signal in the standard
mechanism.** Downstream code already selects on `renewCtx.Done()`; `context.Cause`
lets it distinguish "window exhausted" from "fenced" from "parent cancelled"
without a side channel.

## Consequences

**Positive:**
- A transient backend failure no longer drops a healthy lease.
- Retries are provably bounded by the lease window — no stale-holder window opens.
- Observers see each attempt (`RenewEvent.Attempt`) and the precise stop reason
  (`context.Cause`).

**Negative:**
- The renewal goroutine is more complex than a single-shot renewer, with four
  explicit lifecycle paths to keep correct.
- A non-fencing error that persists for the whole lease window now delays the
  observable failure until `ExpiresAt`, rather than reporting at the first error.
  This is intentional — the holder is entitled to the lease until then — but it
  changes the timing of `OnRenew(err)` callbacks for a persistently failing backend.

## References

- `renewal.go` — `StartRenewal`, `renewCycle`, `backoffWait`, `nextBackoff`
- `lease.go` — `WithRenewalBackoff`, `ErrLeaseWindowExhausted`, `RenewEvent.Attempt`,
  `renewalConfig` backoff fields
- `docs/adr/0004-renewal-loop-managed-goroutine.md` — the ADR this amends
- `UPGRADING.md` — v0.4.x → v0.5.0 renewal behavioral change
