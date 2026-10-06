# renewal-backoff

A runnable example demonstrating two v0.5 additions to the lease lifecycle:
`WithWaitForLease` context cancellation and bounded renewal retry with `WithRenewalBackoff`.

---

## Project structure

```
renewal-backoff/
├── go.mod    ← separate module; replace directive points to repo root
├── go.sum
└── main.go   ← two scenarios, no infrastructure required
```

---

## Setup

Prerequisites: Go 1.25+.

```bash
go mod tidy
```

---

## Running the example

```bash
go run .
```

Expected output:

```
=== Scenario 1: WithWaitForLease + Deadline Exceeded ===
  worker-A: lease acquired — holding for 30s (TTL)
  worker-B: deadline exceeded — stopped waiting for the lease
  worker-B: errors.Is(err, context.DeadlineExceeded) = true (v0.5 behaviour)

=== Scenario 2: WithRenewalBackoff + ErrLeaseWindowExhausted ===
  worker-C: lease acquired (expires in 200ms, fencing token 1)
  worker-C: ErrLeaseWindowExhausted — lease window closed before renewal succeeded
  worker-C: cause also matches ErrLeaseExpired — terminal, renewal did not retry
  worker-C: work abandoned; a successor worker may re-acquire this lease
```

The example runs in approximately 1 second — 500ms waiting for the deadline in Scenario 1
and 400ms for the renewal goroutine to fire in Scenario 2.

---

## Key implementation details

**`WithWaitForLease` cancellation in v0.5** — Before v0.5, `Acquire` with `WithWaitForLease`
returned `ErrLeaseHeld` when the wait loop was cancelled or its deadline exceeded. From v0.5,
it returns an error wrapping `ctx.Err()` so `errors.Is(err, context.DeadlineExceeded)` and
`errors.Is(err, context.Canceled)` work as expected. The `ErrLeaseHeld` sentinel no longer
fires on the wait path — only on the immediate fail-fast path (no `WithWaitForLease`).

**`WithRenewalBackoff` and terminal `ErrLeaseExpired`** — `StartRenewal` retries transient
non-fencing `Renew` errors with exponential backoff bounded by the lease window. The window
starts as the earlier of `token.ExpiresAt()` and the local acquire time plus TTL, and
advances after every successful renewal. In Scenario 2 the renewal interval is deliberately
set longer than the TTL, so the first `Renew` runs after the lease has expired and returns
`ErrLeaseExpired`. That error is terminal (#84): storage has found the lease lapsed, so the
goroutine cancels `renewCtx` at once, without any backoff retry. The cause is
`errors.Join(ErrLeaseWindowExhausted, ErrLeaseExpired)`, so `errors.Is` matches both
sentinels, and existing checks for `ErrLeaseWindowExhausted` keep working.

In production (Postgres), transient errors such as connection drops also enter this retry
path. `WithRenewalBackoff` controls how aggressively the goroutine retries before the
window closes.

**`stopRenewal` before abandon** — In Scenario 2 the work function blocks on
`<-renewCtx.Done()` until the window exhausts. `stopRenewal()` is called afterwards — it is
idempotent and satisfies the requirement that it be called before any `Release`. Here `Release`
is skipped because the lease is already expired and the caller is abandoning work, not
surrendering a live lease.

---

## Next steps

- [Library overview](../../README.md)
- [Architecture](../../docs/ARCHITECTURE.md)
