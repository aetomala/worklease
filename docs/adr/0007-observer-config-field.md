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

## References

- `lease.go` — `LeaseObserver` interface and `noopObserver`
- `worklease.go` — `Config.Observer` field; `New()` observer resolution
