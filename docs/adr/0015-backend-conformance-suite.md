# ADR-0015: Backend conformance suite — RunSuite against all backends

**Status:** Accepted
**Date:** 2026-06-16

## Context

`worklease` ships two `Backend` implementations — in-memory and PostgreSQL — that must be
behaviorally identical. They have repeatedly drifted apart and the drift was caught one bug
at a time: issue #13 (memory `Checkpoint` did not reset `cleanHandoff`), issue #14 (postgres
`ReadCheckpoint` returned `nil` instead of `ErrFenced` on a stale token), issue #15 (postgres
`Renew` re-extended an expired lease). The ADR-0014 slice-ownership defect is the same family.

Each was a separate regression discovered after the fact. Nothing structurally guaranteed
that both backends honor the same contract, so parity depended on every author remembering to
add matching specs to two separate test files.

## Decision

A `backend/conformance` package provides a single, backend-agnostic specification:

```go
func RunSuite(newBackend func() backend.Backend) func()
```

`RunSuite` returns a Ginkgo spec tree. Each backend's test package invokes it with a factory
that yields a clean backend per spec:

```go
// backend/memory/memory_test.go
var _ = Describe("conformance", conformance.RunSuite(func() backend.Backend {
    return memory.New()
}))

// backend/postgres/postgres_test.go (DSN-gated by the suite bootstrap)
var _ = Describe("conformance", conformance.RunSuite(func() backend.Backend {
    _, _ = db.Exec("DELETE FROM worklease_leases")
    b, _ := wlpostgres.New(db)
    return b
}))
```

Two design constraints make the suite truly backend-agnostic:

1. **`backend/conformance` imports only `backend` and `worklease`** — never `memory` or
   `postgres`. The consuming backend test packages own the factory; the suite owns the specs.
2. **Expiry is exercised via non-positive TTL, not clock injection.** `Backend.Acquire` with a
   non-positive `ttl` must produce an already-expired record. This is a test-only affordance —
   production never passes a non-positive TTL because `worklease.New` validates `Config.TTL > 0`
   — and it lets the same specs drive expiry on both backends without a `Clock` abstraction the
   PostgreSQL backend cannot honor (it uses the database server's `NOW()`).

The suite covers acquire/checkpoint/renew/release/read-checkpoint semantics, including the
specific behaviors behind the historical parity bugs: `ErrLeaseExpired` on renew-of-expired
(#15), `ErrFenced` on stale-token operations (#14), `cleanHandoff` reset after checkpoint
(#13), and the ADR-0014 slice-aliasing invariants. New backends must pass it before being
considered complete.

## Rationale

**A factory, not an instance.** Backends are stateful; each spec needs a clean one. A
`func() backend.Backend` called in `BeforeEach` gives every spec isolation — a fresh map for
memory, a truncated table for Postgres — without the suite knowing which backend it drives.

**Non-positive TTL beats clock injection for a cross-backend suite.** A `Clock` cannot be
injected into PostgreSQL, whose expiry is decided by server-side `NOW()`. Negative TTL works
identically on both backends and matches the project's "never sleep in tests" convention, so a
single set of specs runs everywhere.

**The suite lives in non-test source (`conformance.go`).** It is a reusable library consumed by
other packages' `_test.go` files, so it cannot itself be `_test.go`. It registers no suite root
of its own — the specs execute inside the memory and postgres test binaries.

**ReadCheckpoint absent-row behavior is intentionally not asserted.** The PostgreSQL backend
returns `nil, false, nil` for a never-acquired work ID by design, while the memory backend
returns `ErrFenced`; the case is unreachable in the documented `Lease` flow (a `ReadCheckpoint`
is always preceded by a successful `Acquire` for the same token). The #14 guard the suite *does*
assert is the token-mismatch case (row exists, wrong token → `ErrFenced`), which both backends
honor.

## Consequences

**Positive:**
- Backend parity is enforced structurally — a new divergence fails CI, not a future incident.
- Adding a backend (Redis, etcd) is gated on a single, authoritative spec.
- The historical #13/#14/#15 regressions now have permanent guards.

**Negative:**
- Postgres conformance runs only when `WORKLEASE_TEST_POSTGRES_DSN` is set (skipped locally,
  exercised in CI's postgres service container). A local-only run validates memory parity only.
- The non-positive-TTL affordance is a behavior the backends must support solely for the suite.
  It is documented on `Backend.Acquire` and unreachable from production code, but it is surface
  area that exists for testing.

## References

- `backend/conformance/conformance.go` — `RunSuite` and the spec tree
- `backend/memory/memory_test.go`, `backend/postgres/postgres_test.go` — wiring
- `backend/backend.go` — `Backend.Acquire` non-positive-TTL contract note
- `docs/adr/0014-backend-slice-ownership-contract.md` — a contract this suite enforces
- Issues #13, #14, #15 — the parity bugs that motivated the suite
