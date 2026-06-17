# ADR-0014: Backend slice ownership contract — defensive copies required

**Status:** Accepted
**Date:** 2026-06-16

## Context

`Backend.Checkpoint` accepts a `state []byte` and `Backend.ReadCheckpoint` returns a
`state []byte`. The original in-memory backend stored the caller's slice by reference
(`r.checkpoint = state`) and returned the stored slice directly
(`return r.checkpoint, ...`). Both share a backing array with the caller:

- After `Checkpoint`, the caller still holds a reference to the array now owned by the
  backend. Mutating it after the call silently corrupts stored state.
- `ReadCheckpoint` hands the caller the live stored array. Mutating the result corrupts
  the backend's copy.

The PostgreSQL backend is immune to both: `state` is serialized into a `BYTEA` parameter
on write and scanned into a fresh slice on read, so no aliasing is possible. This made
the defect **backend-dependent** — code that is correct against Postgres silently breaks
against the in-memory backend. That class of divergence is exactly what a single contract,
enforced by the conformance suite (ADR-0015), should prevent.

## Decision

The `Backend` interface carries an explicit slice-ownership contract:

- `Checkpoint` must not retain a reference to the `state` slice after it returns. A backend
  that stores the bytes makes a defensive copy.
- `ReadCheckpoint` must return a fresh allocation — never the underlying stored slice.

The in-memory backend satisfies this with two `copy()` calls:

```go
// Checkpoint
stored := make([]byte, len(state))
copy(stored, state)
r.checkpoint = stored

// ReadCheckpoint
out := make([]byte, len(r.checkpoint))
copy(out, r.checkpoint)
return out, r.cleanHandoff, nil
```

The PostgreSQL backend is already compliant by serialization and needs no change. A
`nil` checkpoint round-trips as `nil` (no zero-length slice substituted).

## Rationale

**The contract belongs to the interface, not to one backend.** Callers pass and receive
`[]byte` without knowing which backend is wired in. The only safe, predictable behavior is
value semantics: the backend owns its stored copy, the caller owns the slices it passes and
receives. Documenting this on `Backend` makes every implementation responsible for it.

**Defensive copies are the standard Go idiom for `[]byte` ownership at an API boundary.**
The cost is one allocation per call on the in-memory path — negligible for a backend whose
purpose is single-process testing, and irrelevant on the Postgres path where serialization
already copies.

**A copy is strictly safer than documenting "do not mutate."** A prose warning that callers
must not mutate slices fails silently when ignored. A copy makes the safe behavior the only
behavior.

## Consequences

**Positive:**
- Identical, backend-independent semantics for checkpoint state.
- Eliminates a silent corruption vector in the in-memory backend.
- The invariant is enforced structurally by the conformance suite (ADR-0015), not by
  reviewer vigilance.

**Negative:**
- One extra allocation per `Checkpoint`/`ReadCheckpoint` on the in-memory backend. Accepted:
  the in-memory backend is for tests, where clarity and correctness outweigh allocation count.

## References

- `backend/backend.go` — `Backend.Checkpoint` / `ReadCheckpoint` interface comments
- `backend/memory/memory.go` — defensive `copy()` in `Checkpoint` and `ReadCheckpoint`
- `backend/conformance/conformance.go` — slice-aliasing invariant specs
- `docs/adr/0015-backend-conformance-suite.md` — the suite that enforces this contract
- `docs/adr/0003-checkpoint-serialization-raw-bytes.md` — why checkpoint state is `[]byte`
