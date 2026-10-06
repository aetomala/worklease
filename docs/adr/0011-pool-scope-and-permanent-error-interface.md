# ADR-0011: pool scope is cross-process; permanent slot failure signals via interface

**Status:** Accepted
**Date:** 2026-06-10

## Context

The `pool` package distributes a fixed set of work IDs across workers. Two design questions
arose:

1. Should `pool` target in-process goroutine fan-out, or cross-process competition via a shared
   backend?
2. How should a work function signal that a slot should not be reacquired after failure —
   a package-level sentinel error, or a typed interface?

## Decision

`pool` targets cross-process distribution. Multiple `Pool` instances — one per process — share
a backend and collectively cover the work ID set. Rebalancing is emergent from lease acquisition
races, not from coordination between pool instances.

Permanent slot failure is signalled by returning an error that implements `PermanentError`:

```go
type PermanentError interface {
    error
    Permanent() bool
}
```

`pool` provides no concrete implementation. Callers define their own error type implementing
the interface. `pool` checks `errors.As(err, &pe)` after each `runner.Run` call.

## Rationale

### 1. Cross-process scope is where worklease adds value

An in-process pool — goroutines sharing one backend within a single process — is `errgroup`
with a semaphore. The lease primitive adds overhead without benefit: fencing tokens are
meaningful only when multiple processes compete for the same work ID. In-process goroutines
sharing a process do not need fencing to coordinate. Building `pool` on top of `worklease`
is only justified when the competing workers are independent processes that cannot coordinate
through shared memory.

### 2. PermanentError as interface, not sentinel

Two alternatives were considered:

**Sentinel error (`pool.ErrPermanent`):** The work function wraps its error with
`errors.Join(pool.ErrPermanent, cause)`. Simple. But it requires the work function to import
the `pool` package, creating a dependency from the work function's package back to `pool`. In
the common case where `WorkFn` is defined in application code that already imports `pool`, this
is not a cycle. But it is an unnecessary coupling — the work function's error type should not
depend on which coordination package happens to be running it.

**Typed interface (`PermanentError`):** The work function returns any error that implements
`Permanent() bool`. The work function's package defines its own error type; it does not import
`pool`. `pool` uses `errors.As` to check. This is the pattern the standard library uses for
retryability (`net.Error.Temporary()`, `net.Error.Timeout()`): the coordination layer checks a
behaviour interface, not a specific sentinel value. The work function and the pool are decoupled
at the type level.

The interface pattern is the correct choice. It is consistent with established Go conventions,
requires no import of `pool` in `WorkFn`, and composes naturally with `errors.As` unwrapping
chains.

### 3. WithWaitForLease is prohibited in pool.Config.AcquireOptions

`pool` manages its own acquisition loop — one goroutine per slot, calling `runner.Run` in a
loop. `runner.Run` calls `Lease.Acquire` internally. If `WithWaitForLease` is passed in
`AcquireOptions`, the slot goroutine blocks inside `runner.Run` during acquisition, preventing
it from responding to context cancellation until the lease becomes available. This defeats the
pool's ability to shut down cleanly when `ctx` is cancelled. `pool.New` returns
`ErrConfigInvalid` if `WithWaitForLease` is detected in `AcquireOptions`.

### 4. pool.WorkFn receives worklease.Token

`pool` targets long-running, stateful work — the same shape as the cross-tenant migration
example. Mid-work checkpointing requires the token. Hiding the token from `WorkFn` would force
callers who need mid-work checkpointing to step outside the pool API, closing over a token from
the outer scope — a worse design than exposing it directly. `workID` is added as the first
parameter after `ctx` to identify which slot is executing; the remaining parameters match
`worker.WorkFn`.

## Consequences

**Positive:**
- `pool` is meaningful only where `worklease` adds value: cross-process competition over a
  shared backend.
- Work functions signal permanent failure via their own error types — no import of `pool`
  required.
- `errors.As` unwrapping means `PermanentError` composes correctly with `fmt.Errorf` wrapping
  chains.
- `WithWaitForLease` prohibition is enforced at construction time, not discovered at runtime.

**Negative:**
- Callers must define their own `PermanentError` implementation. `pool` provides no concrete
  type. This is a small overhead for the decoupling benefit.
- In-process fan-out use cases are explicitly out of scope; callers who want that use
  `errgroup`.

## v0.4 Amendment (2026-06-16)

v0.4 extends `pool` along four axes; the cross-process scope and `PermanentError` interface
decisions above are unchanged.

- **`pool.Permanent(err error) error`** — a constructor returning a value that satisfies
  `PermanentError`. The original decision (interface, no concrete type) stands; `Permanent` is an
  additive convenience so a `WorkFn` can drop its slot without defining a custom type. The
  interface remains the canonical mechanism — `Permanent` simply wraps the common case.

- **`pool.Observer` interface** — `OnSlotAcquired`, `OnSlotLost`, `OnSlotBackoff`, `OnSlotDead`,
  each taking an event struct, injected via `Config.Observer`; nil installs a no-op.
  This is an **interface**, unlike `leader`'s `func` callbacks (ADR-0010), because the pool has
  four distinct slot events — enough surface to justify selective implementation via embedding a
  no-op. Event structs (not flat params) keep it consistent with `LeaseObserver` (ADR-0007) and
  forward-extensible. `OnSlotAcquired` fires at `WorkFn` entry — the same point `ActiveSlots`
  starts reporting the slot — not when the internal `Acquire` succeeds. The pool observer layers
  above each slot's `Runner → Lease` `LeaseObserver`: same underlying events, different altitude.

- **Distinct config sentinels** — `ErrNilLease`, `ErrEmptyWorkIDs`, and
  `ErrWithWaitForLeaseProhibited` replace the single `ErrConfigInvalid` at the `New` call site.
  Each wraps `ErrConfigInvalid` via `%w`, so `errors.Is(err, ErrConfigInvalid)` still holds for
  callers checking the broad case while distinct sentinels aid debugging. `ErrConfigInvalid`'s
  message changed to `"pool: invalid configuration"`.

- **`ErrAllSlotsDead`** — `Run` returns it when every slot has exited via `PermanentError`,
  distinguishing a fully-dead pool from clean shutdown (`nil` on context cancellation). The last
  slot to die cancels an internal context so idle siblings unblock.

These are breaking at the margins (the `ErrConfigInvalid` message and the `Run`-returns-
`ErrAllSlotsDead` behavior); see `UPGRADING.md` (v0.3 → v0.4).

## References

- `pool/pool.go` — `Pool`, `WorkFn`, `PermanentError`, `Permanent`, `Observer`, `Config`, `New`, sentinels
- `worker/runner.go` — `Runner.Run`, used per slot internally
- `lease.go` — `AcquireOption`, `WithWaitForLease`
- `docs/adr/0006-backend-acquire-single-attempt.md` — single-attempt backend contract
- `docs/adr/0007-observer-config-field.md` — `LeaseObserver`, the event-struct pattern `pool.Observer` mirrors
- `docs/adr/0010-leader-fn-signature-and-acquire-semantics.md` — parallel design decisions for `leader` (callbacks vs interface)

## Amendment (2026-10-06) — v0.6 slot pacing, retirement, and cleanup

- **Idle wait (#81).** `pool.Config.IdleInterval` is the wait after `Runner.Run` returns `ErrLeaseHeld`, meaning a peer holds the work ID. Zero or negative means 1s. Each wait is drawn uniformly from `[IdleInterval, 1.2·IdleInterval)`. `OnSlotBackoff` does not fire for these waits.
- **Backoff default (#81).** `BackoffInterval` keeps its role, the wait after a real non-permanent error, but its zero value changes from immediate retry to 1s. This is a breaking change.
- **Rerun wait (v0.6).** `pool.Config.RerunInterval` is the wait after a `WorkFn` returns `nil` and the slot releases with `ExitFinished`. Zero or negative means 1s, with the same 20% jitter as `IdleInterval`. A finished slot no longer reacquires immediately: without a pause, a fast `WorkFn` reran in a tight loop, and a peer process almost never won a finished work ID, which defeated rebalancing. `OnSlotBackoff` does not fire for these waits. This is a breaking change.
- **Pacing summary.** Every path that loops, except the immediate reacquire after `ErrFenced`, has a non-zero default wait: `IdleInterval` (peer holds the work ID), `RerunInterval` (work finished), `BackoffInterval` (work failed). `ErrFenced` reacquires immediately, as before.
- **Retirement (ADR-0018).** A `WorkFn` that returns an error wrapping `worklease.ErrRetire` releases with `ExitRetired`. The slot exits without reacquiring or backing off, and `Observer.OnSlotRetired` fires. `OnSlotDead` stays reserved for `PermanentError`. `ErrRetire` wins when an error satisfies both. `Run` returns `ErrAllSlotsDead` only if every slot exited through `PermanentError`; if at least one slot retired, it returns `nil`.
- **Exit modes (ADR-0018).** `pool.WorkFn` receives `prior worklease.Checkpoint`. A `PermanentError` is a non-fencing error, so its slot releases with `ExitAbandoned`, and its successor resumes from partial state.
- **Cleanup (#80).** `pool.Config.CleanupTimeout` bounds each slot's final `Checkpoint` and `Release`, which run on a context that survives cancellation of `Run`'s context. Zero or negative means 5s.
