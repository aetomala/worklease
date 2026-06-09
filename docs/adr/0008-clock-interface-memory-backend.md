# ADR-0008: Clock interface for in-memory backend testability

**Status:** Accepted
**Date:** 2026-06-08

## Context

The in-memory backend's expiry logic depends on `time.Now()`. Tests that exercise expiry
paths have two options:

1. Sleep until the TTL elapses — slow, flaky, environment-sensitive.
2. Inject a controllable clock — fast, deterministic, no sleeps.

v0.1 used negative TTLs to force pre-expired records directly into the map, bypassing the
normal Acquire path. This works but is intrusive (tests reach into backend internals).

## Decision

The `memory` package exports a `Clock` interface with a single `Now() time.Time` method.
`memory.New` accepts variadic `MemoryOption`; `memory.WithClock(c Clock)` is the option
constructor. The default clock is an unexported `realClock` that delegates to `time.Now()`.
Existing callers of `memory.New()` with no arguments are unaffected.

## Rationale

**Exported `Clock` interface.** Test packages outside `backend/memory` — including the root
package test file — may need to implement a fake clock. An unexported interface would force
those packages to define their own; an exported one provides a single contract.

**`realClock` unexported.** Callers have no reason to reference the default implementation
directly. They either use the default (no option) or supply their own implementation.

**Variadic `MemoryOption` on `New()`.** Consistent with the existing `AcquireOption` and
`RenewalOption` patterns in the library. Zero options is a valid call — backward-compatible.

**No clock injection on the PostgreSQL backend.** The Postgres backend delegates time to the
database server via `NOW()` SQL expressions. Injecting a Go clock there would only mock the
client side, not the server — a false sense of control. Postgres expiry is tested via the
integration suite with real time.

## Consequences

**Positive:**
- All expiry branches in the in-memory backend are testable deterministically — no sleeps.
- The `Clock` interface is reusable by any future in-process backend.
- Existing test call sites (`memory.New()`) compile unchanged.

**Negative:**
- Every `time.Now()` call in `memory.go` must use `mb.clock.Now()` — a discipline requirement
  that is easy to violate on future additions. Enforced by grep in CI.

## References

- `backend/memory/memory.go` — `Clock` interface, `realClock`, `MemoryOption`, `WithClock`
- `backend/memory/memory_test.go` — `fakeClock` helper; clock injection test section
