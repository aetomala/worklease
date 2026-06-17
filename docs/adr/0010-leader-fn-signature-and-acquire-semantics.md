# ADR-0010: leader.Elect fn receives no Token; acquire semantics are caller-controlled

**Status:** Accepted
**Date:** 2026-06-10

## Context

The `leader` package provides a simplified API for pure leader-election use cases: one active
goroutine at a time for a named work ID, with fencing propagated automatically via context
cancellation. No checkpoint state is carried or handed off.

Two design questions arose during package design:

1. Should the work function `fn` receive a `worklease.Token`?
2. Should `Elect` force blocking acquisition (`WithWaitForLease`) or let the caller control it?

## Decision

`Elect` accepts a work function with signature `func(ctx context.Context) error` — no `Token`
parameter. Acquire semantics are caller-controlled via `leader.Config.AcquireOptions`; `Elect`
does not force `WithWaitForLease`.

```go
func Elect(
    ctx context.Context,
    lease worklease.Lease,
    workID string,
    cfg Config,
    fn func(ctx context.Context) error,
) error

type Config struct {
    AcquireOptions []worklease.AcquireOption
    RenewalOptions []worklease.RenewalOption
}
```

## Rationale

### 1. No Token in fn — leader is presence-only

`leader` is for callers who need exactly one guarantee: one active goroutine at a time for a
named work ID. Callers who need to write to fencing-aware storage mid-work need the token to
call `lease.Checkpoint`. If `fn` receives a `Token`, those callers are half-served —
they have the token but not the `Lease`, so they cannot checkpoint without closing over the
`Lease` from the outer scope.

The correct abstraction for callers who need fencing-aware writes is `worker.Runner`, which
provides both the token and the full checkpoint lifecycle. `leader` serves a distinct use case:
presence without state. Adding `Token` to `fn` would blur that distinction without completing
the capability gap.

### 2. Acquire semantics are caller-controlled

`Elect` is built on `worklease.Lease`, which already expresses blocking vs fail-fast via
`WithWaitForLease`. Callers who have constructed a `Lease` understand this option — it is part
of the documented `Acquire` API. Forcing `WithWaitForLease` inside `Elect` would remove a
valid use case: a caller who wants to attempt leadership and proceed to other work if the lease
is already held. That caller would pass a short-deadline context instead — a workaround, not a
design.

Callers who want to block until leadership is available pass
`worklease.WithWaitForLease()` in `cfg.AcquireOptions`. The intent remains explicit and
readable at the call site, consistent with the rest of the library.

### 3. No TTL or HolderID in leader.Config

`TTL` and `HolderID` are already embedded in the `worklease.Lease` passed to `Elect` — they
were required by `worklease.New` at construction time. Duplicating them in `leader.Config`
would allow them to diverge from the values the `Lease` was built with, creating a class of
configuration bugs with no obvious failure mode. `leader.Config` holds only the options that
are legitimately variable per `Elect` call: `AcquireOptions` and `RenewalOptions`.

## Consequences

**Positive:**
- `leader` has a clear, narrow scope: presence-only leader election with no checkpoint state.
- Callers who need fencing-aware writes are directed to `worker.Runner` — the correct tool.
- Acquire semantics (blocking vs fail-fast) remain consistent with the rest of the library;
  no hidden behavior inside `Elect`.
- `leader.Config` cannot express a TTL/HolderID mismatch with the underlying `Lease`.

**Negative:**
- Callers who want blocking leader election must pass `worklease.WithWaitForLease()` explicitly
  rather than getting it by default. This is intentional — see Rationale §2.

## v0.4 Amendment — lifecycle callbacks (2026-06-16)

v0.4 adds three optional lifecycle callbacks to `leader.Config`:

```go
type Config struct {
    // ... AcquireOptions, RenewalOptions, BackoffInterval ...
    OnElected      func(ctx context.Context, token worklease.Token) // after Acquire, before fn
    OnLost         func(ctx context.Context, token worklease.Token) // renewCtx cancelled before fn returns
    OnRelinquished func(ctx context.Context, token worklease.Token) // after a successful Release
}
```

All three are optional `func` fields, not a `leader.Observer` interface. The leadership event
surface is narrow — elected, lost, relinquished — and three optional fields are the idiomatic Go
way to express a small set of optional callbacks; a dedicated interface would force implementers
to define a type (or embed a no-op) for what is naturally three closures. This differs from
`pool.Observer` (ADR-0011), whose four slot events justify an interface.

The callbacks layer **above** the underlying `LeaseObserver` (ADR-0007): a `Lease` passed to
`Elect` may carry its own observer, which still fires per backend operation. The leader
callbacks fire at the higher leadership-lifecycle altitude — same underlying events, different
level of abstraction, not redundant. All three receive the original `ctx`, never `renewCtx`;
nil callbacks are no-ops. `OnElected` does not fire if `Acquire` fails; `OnRelinquished` does
not fire if `fn` or `Release` returns `ErrFenced`.

## References

- `leader/leader.go` — `Elect`, `Config` (incl. `OnElected`/`OnLost`/`OnRelinquished`)
- `lease.go` — `AcquireOption`, `WithWaitForLease`
- `worker/runner.go` — `Runner.Run`, the correct abstraction for fencing-aware work
- `docs/adr/0005-acquire-default-returns-err-lease-held.md` — acquire default behavior
- `docs/adr/0007-observer-config-field.md` — the `LeaseObserver` these callbacks layer above
- `docs/adr/0011-pool-scope-and-permanent-error-interface.md` — `pool.Observer` (interface, by contrast)
