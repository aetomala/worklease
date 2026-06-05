# ADR-0001: Backend interface does not include Close

**Status:** Accepted
**Date:** 2026-06-05

## Context

`worklease.New` accepts a `Backend` to decouple the library from any specific storage
implementation. Every concrete backend wraps an external resource — a `*sql.DB` for PostgreSQL,
a Redis client for a future Redis backend — that requires explicit cleanup on shutdown.

Two ownership models were considered:

1. `Backend` includes `Close() error`. The library calls it when done.
2. `Backend` has no `Close`. The caller constructs and closes the underlying resource.

## Decision

`Backend` does not include `Close`. Concrete backend constructors accept the underlying
connection as a parameter and do not take ownership of it. The caller is responsible for
closing the resource.

Example:

```go
db, err := sql.Open("postgres", dsn)
// ...
backend, err := postgres.New(db)
lease, err := worklease.New(backend, cfg)
// ...
defer db.Close() // caller owns db lifecycle — not worklease
```

## Rationale

**Ownership surprise is a correctness hazard.** If the library closed the `*sql.DB`, it would
invalidate a connection pool the caller may be sharing with other subsystems. A caller that
passes a shared `db` and later tries to use it would get `sql: database is closed` errors with
no obvious cause.

**Library scope is coordination, not resource lifecycle.** `worklease` coordinates lease
handoff. It has no business owning the lifecycle of a database connection it did not create.

**Constructors signal the contract.** `postgres.New(db *sql.DB)` makes the ownership boundary
visible at the call site. The caller passed the connection in; the caller owns cleanup.

**`Backend` is an interface — `Close` would be vestigial for some implementations.** The
in-memory backend has nothing to close. Adding `Close` to the interface forces a no-op
implementation on every backend that has no cleanup to do.

## Consequences

**Positive:**
- No ownership surprise. Callers retain full control over connection lifecycle.
- Simpler `Backend` interface — no `Close` method to implement or call.
- In-memory backend needs no cleanup path.
- The library composes cleanly when `*sql.DB` is shared with other packages.

**Negative:**
- Callers must remember to close the underlying resource themselves. This is standard Go
  resource management and is documented in each backend constructor's godoc.

## References

- `backend/backend.go` — `Backend` interface definition
- `backend/postgres/postgres.go` — `postgres.New(db *sql.DB)` constructor
- `backend/memory/memory.go` — `memory.New()` — no Close required
