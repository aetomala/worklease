# ADR-0007: Observer injection via Config field

**Status:** Accepted
**Date:** 2026-06-08

## Context

`worklease` operations — Acquire, Checkpoint, Renew, Release — produce outcomes that callers
may want to observe: metrics, structured logs, distributed traces. Three injection patterns
were considered:

1. Separate constructor parameter: `New(b backend.Backend, cfg Config, obs LeaseObserver)`
2. Variadic option: `New(b backend.Backend, cfg Config, opts ...Option)`
3. Config field: `Config.Observer LeaseObserver`

## Decision

`LeaseObserver` is injected via `Config.Observer`. The zero value (nil) silently installs a
no-op observer so existing callers are unaffected.

## Rationale

**Config is already the variadic extension point.** New optional behaviour belongs in `Config`,
not as a new parameter position that would break all existing call sites.

**A separate constructor parameter would be a breaking API change.** Existing callers would
need to pass `nil` or a no-op observer explicitly — a needless migration burden for a feature
they do not use.

**Variadic options add complexity without benefit here.** `LeaseObserver` is a single interface.
A full options pattern is warranted when there are multiple independent optional concerns; for
one interface, a named field is clearer and easier to document.

**The nil-installs-noop guarantee means zero adoption friction.** Library users who do not
care about observability never see `LeaseObserver`. Library users who do care set one field.

## Consequences

**Positive:**
- Zero breaking change — existing `New(b, cfg)` callers compile unchanged.
- Observer is available to all call sites (Acquire, Checkpoint, Renew, Release, StartRenewal
  goroutine) via the `leaseClient.obs` field — no threading needed.
- `LeaseObserver` is the seam for future metrics and tracing implementations.

**Negative:**
- `Config` grows over time; long-term it may need to be split. This is accepted given the
  library's current scope.

## v0.4 Amendment — event-struct redesign (2026-06-16)

The injection decision above is unchanged: `LeaseObserver` is still injected via
`Config.Observer`, and nil still installs a no-op. v0.4 changed the interface **shape**, not
the injection mechanism.

The five flat-parameter methods became six event-struct methods:

```go
type LeaseObserver interface {
    OnAcquire(ctx context.Context, e AcquireEvent)
    OnCheckpoint(ctx context.Context, e CheckpointEvent)
    OnRenew(ctx context.Context, e RenewEvent)
    OnRelease(ctx context.Context, e ReleaseEvent)
    OnReadCheckpoint(ctx context.Context, e ReadCheckpointEvent) // new in v0.4
    OnFenced(ctx context.Context, e FencedEvent)
}
```

Three behavioral changes accompany the new shape:

- **`OnReadCheckpoint` is new** — it fires after every `ReadCheckpoint` attempt. `OnFenced` is
  *not* called on this path; a fenced read surfaces via `ReadCheckpointEvent.Err` only.
- **`OnFenced` now also fires on the `Release` path.** Previously it fired only after Checkpoint
  and Renew. `FencedEvent.Operation` (`OperationCheckpoint` / `OperationRenew` /
  `OperationRelease`) identifies the source. The operation-specific callback still fires before
  `OnFenced`.
- **Every operation event carries `Duration`** measuring the single final backend call only —
  not the wait loop. For `Acquire` with `WithWaitForLease`, callers needing cumulative wait time
  record their own timestamp.

**Rationale:** event structs are forward-extensible — new fields can be added without breaking
existing implementers, which a growing flat parameter list cannot do (Design Principle 6). This
is a breaking change to the `LeaseObserver` contract; callers who passed `nil` are unaffected.
See `UPGRADING.md` (v0.3 → v0.4) for the migration.

## References

- `lease.go` — `LeaseObserver` interface, event structs, `Operation`, and `noopObserver`
- `worklease.go` / `acquire.go` / `renewal.go` — `leaseClient` observer call sites
- `examples/observability` — stdlib-only `LeaseObserver` reference implementation
- `worklease.go` — `Config.Observer` field; `New()` observer resolution
