# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [v0.3.0] — 2026-06-11

### Breaking

- `checkpoint.Codec` interface methods renamed: `Encode` → `Marshal`, `Decode` → `Unmarshal`. Callers who implement `Codec` directly must rename their method implementations. Callers using only `checkpoint.JSON()` are unaffected — `JSONCodec` is updated transparently. The package-level generic helpers `Encode[T]` and `Decode[T]` are unchanged.

### Added

- `worklease.HasWaitForLease(opts []AcquireOption) bool` — reports whether a `[]AcquireOption` slice includes `WithWaitForLease`; used by `pool.New` to enforce that blocking acquisition is not passed through the pool
- `leader` package — `leader.Elect` acquires a work ID, starts managed lease renewal, calls `fn(renewCtx)`, stops renewal, and releases; fencing bypasses release and propagates via context cancellation
- `pool` package — `pool.Pool` distributes a fixed set of work IDs across competing processes; one slot goroutine per work ID; supports backoff on transient errors, immediate reacquisition on fencing, and `PermanentError` to drop a slot without reacquisition; `ActiveSlots()` reports currently held IDs

---

## [v0.2.0] — 2026-06-09

### Fixed

- `postgres.Backend.Renew`: now returns `ErrLeaseExpired` when the lease has expired (fencing token matches but `expires_at ≤ NOW()`), matching the Backend interface contract and the memory backend's behaviour
- `postgres.Backend.ReadCheckpoint`: now returns `ErrFenced` when called with a stale fencing token, matching the Backend interface contract and the memory backend's behavior

### Added

- `worker.Runner` — lifecycle manager that wraps acquire/ReadCheckpoint/StartRenewal/Release; callers implement only `WorkFn`
- `examples/cross-tenant-migration` — runnable example demonstrating checkpoint-as-cursor, crash recovery mid-batch, and zombie fencing
- `examples/subscription-cancellation` — runnable example demonstrating crash recovery, zombie fencing, and clean handoff semantics
- `examples/README.md` — examples landing page
- Fixed `memory.Backend.Acquire`: checkpoint and `cleanHandoff` from an expired record are now preserved for the successor, matching the PostgreSQL backend's `ON CONFLICT DO UPDATE` semantics
- `LeaseObserver` interface — five hook methods (`OnAcquire`, `OnCheckpoint`, `OnRenew`,
  `OnRelease`, `OnFenced`) called synchronously after each `Lease` operation
- `Config.Observer` field — wire a `LeaseObserver` into a `Lease` instance at construction
  time; zero value (nil) installs a silent no-op observer
- `memory.Clock` interface — injectable clock for the in-memory backend
- `memory.WithClock` option — `memory.New(memory.WithClock(fc))` for deterministic expiry
  tests without sleeping
- `memory.Option` type — functional option type for the in-memory backend constructor
- ADR-0007: observer injection via `Config` field
- ADR-0008: `Clock` interface for in-memory backend testability
- `checkpoint` subpackage — `Codec` interface, `JSONCodec` implementation, and generic `Encode[T]`/`Decode[T]` helpers; `Decode[T]` returns the zero value on nil input (no prior checkpoint)
- ADR-0009: `checkpoint` subpackage — codec interface design, generics constraint, nil-bytes contract

---

## [v0.1.0] — 2026-06-06

### Added

- `Lease` interface: `Acquire`, `Checkpoint`, `Renew`, `Release`, `ReadCheckpoint`, `StartRenewal`
- `Token` value type with unexported fields and accessor methods (`WorkID`, `HolderID`,
  `FencingToken`, `ExpiresAt`) — implements `fmt.Stringer`
- `AcquireOption`: `WithWaitForLease`, `WithPollInterval`
- `RenewalOption`: `WithRenewalInterval`
- Error sentinels: `ErrFenced`, `ErrLeaseHeld`, `ErrLeaseExpired`
- `Config` struct with `TTL` and `HolderID` fields
- `New(backend.Backend, Config) (Lease, error)` constructor
- PostgreSQL backend (`backend/postgres`) — fencing via conditional `UPDATE … WHERE fencing_token = $n`
- In-memory backend (`backend/memory`) — for unit testing within a single process
- `backend/postgres/schema.sql` — `worklease_leases` table definition
- `doc.go` — package-level documentation
- `docs/ARCHITECTURE.md` — human-readable architecture document
- Architecture Decision Records ADR-0001 through ADR-0006 in `docs/adr/`
- `CONTRIBUTING.md` — contributing guide, ADR requirement, PR and code style guidelines
- `.github/workflows/ci.yml` — CI with lint and test jobs; Postgres service container for integration tests
- `go.mod` — module `github.com/aetomala/worklease`, Go 1.26.4
- `Makefile` — `build`, `test`, `lint`, `vuln`, `ci` targets
- `.golangci.yml` — `revive` and `godot` linters
- `.gitignore` — standard Go ignores
