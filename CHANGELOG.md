# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Breaking

- `Acquire` with `WithWaitForLease` now returns an error wrapping `ctx.Err()` — `fmt.Errorf("worklease: acquire cancelled: %w", ctx.Err())` — when the wait loop is cancelled or its deadline is exceeded, instead of the bare `ErrLeaseHeld` sentinel. The returned error satisfies `errors.Is(err, context.Canceled)` / `errors.Is(err, context.DeadlineExceeded)` and no longer satisfies `errors.Is(err, ErrLeaseHeld)`. This is a runtime break (not compile-detectable); the synchronous no-wait path is unchanged. See `UPGRADING.md`.
- **Postgres schema migration required** — databases created under v0.4 have `fencing_token BIGINT NOT NULL DEFAULT 1` and no `worklease_fencing_seq` sequence. Deploying v0.5 against a v0.4 schema causes every `Acquire` to fail at runtime with `pq: relation "worklease_fencing_seq" does not exist`. Apply the idempotent migration before deploying v0.5. See `UPGRADING.md`.

### Added

- `WithRenewalBackoff(initial, max time.Duration, jitter float64)` — configures the renewal goroutine's bounded-retry backoff policy (defaults 100ms / 5s / 0.20, clamped).
- `ErrLeaseWindowExhausted` — set as the cancel cause of the renewal context when the lease window closes before a renewal succeeds; inspect via `context.Cause(renewCtx)`.
- `RenewEvent.Attempt` — 1-based attempt counter delivered to `OnRenew`, incremented on each retry within the renewal goroutine; direct `Renew` calls always report `Attempt: 1`.
- Global fencing sequence — fencing tokens now come from a single monotonic source per backend instance: a Postgres `worklease_fencing_seq` SEQUENCE and a per-instance `atomic.Uint64` counter in the memory backend. Tokens are strictly increasing across all work IDs and survive row deletion (ADR-0016). The conformance suite asserts global monotonicity across distinct work IDs on both backends.
- ADR-0013 (renewal goroutine retry policy) and ADR-0016 (row lifecycle: global fencing sequence) added; ADR-0004 and ADR-0005 amended with v0.5 sections.

### Changed

- The renewal goroutine now retries a non-fencing `Renew` error with exponential backoff plus additive jitter, bounded strictly by the lease window, instead of cancelling on the first error. When the window is exhausted it cancels the renewal context with cause `ErrLeaseWindowExhausted`; a fencing error still cancels immediately with cause `ErrFenced` and is never retried.
- `StartRenewal` now derives its renewal context via `context.WithCancelCause`; the fencing and window-exhausted causes surface through `context.Cause(renewCtx)`. A normal `stopRenewal()` leaves the context uncancelled (`context.Cause` is `nil`).
- Postgres `Acquire` is now a single `INSERT … ON CONFLICT … RETURNING` statement sourcing the token from `nextval('worklease_fencing_seq')`, replacing the prior two-step `ExecContext` + read-back `SELECT` and closing the read-back race (R8/F4). The `queryAcquireRead` query was removed; zero returned rows (`sql.ErrNoRows`) map to `ErrLeaseHeld`.

### Documentation

- Created ADR-0013 and ADR-0016; amended ADR-0004 (renewal bounded retry) and ADR-0005 (acquire ctx.Err() propagation); synced `docs/ARCHITECTURE.md` and `README.md` to the v0.5 surface (bounded renewal retry, global fencing sequence, single-statement Postgres acquire, ctx-aware acquire cancellation).
- README quickstart DDL synced to `backend/postgres/schema.sql` — adds the v0.5 `worklease_fencing_seq` sequence, `nextval` default, and `updated_at` index; adds a canonical-source pointer so README and `schema.sql` cannot drift independently.
- Corrected Go version floor in `README.md` and all five example `go.mod` files from `1.26` to `1.25`, consistent with the library `go.mod` floor and the README badge.

### Chore

- Added `build-examples` CI job — iterates `examples/*/` as independent Go modules and runs `go build ./...` in each; a broken example now fails CI.
- Updated `Prerequisites` line in all five example READMEs from `Go 1.26+` to `Go 1.25+`, consistent with the library floor and `go.mod` directives.
- Removed duplicate cross-work-ID fencing-token monotonicity spec from the memory backend test suite — the identical property is asserted by the shared conformance suite (ADR-0015).

---

## [v0.4.0] — 2026-06-17

### Breaking

- `LeaseObserver` redesigned: the five flat-parameter methods are replaced by six event-struct methods — `OnAcquire(ctx, AcquireEvent)`, `OnCheckpoint(ctx, CheckpointEvent)`, `OnRenew(ctx, RenewEvent)`, `OnRelease(ctx, ReleaseEvent)`, `OnReadCheckpoint(ctx, ReadCheckpointEvent)`, and `OnFenced(ctx, FencedEvent)`. New `OnReadCheckpoint` callback; `OnFenced` now also fires on the `Release` path (it previously fired only on Checkpoint and Renew); a `Duration` field on all operation events measures the final backend call only — not the wait loop. Implementers of `LeaseObserver` must convert to the event structs. See `UPGRADING.md`.
- `pool.ErrConfigInvalid` message changed to `"pool: invalid configuration"`, and `pool.New` now returns the distinct sentinels below instead of the single `ErrConfigInvalid`. Callers matching the exact old error string must switch to `errors.Is(err, pool.ErrConfigInvalid)` (still satisfied by all three). `pool.Pool.Run` now returns `ErrAllSlotsDead` (previously `nil`) when all slots exit via `PermanentError`.

### Added

- `pool.Observer` interface (`OnSlotAcquired`, `OnSlotLost`, `OnSlotBackoff`, `OnSlotDead`) with event structs, injected via `pool.Config.Observer`; nil installs a no-op.
- `pool.Permanent(err error) error` — constructor returning a value that satisfies `PermanentError`, so a `WorkFn` can drop its slot without defining a custom error type.
- `pool.ErrAllSlotsDead` — returned by `pool.Pool.Run` when every slot exits via `PermanentError`, distinguishing a fully-dead pool from clean shutdown.
- Distinct `pool` config sentinels — `ErrNilLease`, `ErrEmptyWorkIDs`, `ErrWithWaitForLeaseProhibited` — each wrapping `ErrConfigInvalid`.
- `leader.Config` lifecycle callbacks — `OnElected` (after acquire, before `fn`), `OnLost` (when the renewal context is cancelled before `fn` returns), and `OnRelinquished` (after a successful `Release`). All optional; nil is a no-op.
- `backend/conformance` package — `RunSuite(newBackend func() backend.Backend) func()` returns a backend-agnostic Ginkgo spec tree that enforces memory-vs-Postgres parity structurally (ADR-0015). Wired into both backend test suites; expiry is exercised via non-positive TTL with no clock injection. Covers acquire/checkpoint/renew/release/read-checkpoint semantics including `ErrLeaseExpired` on renew-of-expired, `ErrFenced` on stale and never-acquired records, and slice-ownership invariants.
- `examples/observability` — a stdlib-only `LeaseObserver` reference implementation producing per-operation counts and latency, lease hold duration (via `OnAcquire`/`OnRelease` correlation on the fencing token), and a dedicated fencing counter, with a mapping to Prometheus/OpenTelemetry instruments.

### Changed

- `pool.Pool.ActiveSlots` now reflects slots that are actively executing `WorkFn` (marking moved to WorkFn entry), excluding the acquire phase.

### Fixed

- Memory backend no longer aliases caller slices (ADR-0014). `Checkpoint` now stores a defensive copy of the incoming `state`, and `ReadCheckpoint` returns a fresh copy of the stored slice. Previously, mutating a slice after `Checkpoint` or mutating a `ReadCheckpoint` result silently corrupted stored state — a backend-dependent bug, since the Postgres backend was immune via BYTEA serialization.

### Documentation

- Synced `docs/ARCHITECTURE.md`, `README.md`, and `doc.go` to the v0.4 surface (LeaseObserver event structs, pool observer/sentinels/`ErrAllSlotsDead`, leader callbacks, backend conformance suite, slice-ownership contract).
- Added ADR-0014 (backend slice ownership contract) and ADR-0015 (backend conformance suite); amended ADR-0007, ADR-0010, and ADR-0011 with v0.4 addenda.

---

## [v0.3.0] — 2026-06-13

### Breaking

- `checkpoint.Codec` interface methods renamed: `Encode` → `Marshal`, `Decode` → `Unmarshal`. Callers who implement `Codec` directly must rename their method implementations. Callers using only `checkpoint.JSON()` are unaffected — `JSONCodec` is updated transparently. The package-level generic helpers `Encode[T]` and `Decode[T]` are unchanged.

### Added

- `worklease.HasWaitForLease(opts []AcquireOption) bool` — reports whether a `[]AcquireOption` slice includes `WithWaitForLease`; used by `pool.New` to enforce that blocking acquisition is not passed through the pool
- `leader` package — `leader.Elect` acquires a work ID, starts managed lease renewal, calls `fn(renewCtx)`, stops renewal, and releases; fencing bypasses release and propagates via context cancellation
- `pool` package — `pool.Pool` distributes a fixed set of work IDs across competing processes; one slot goroutine per work ID; supports backoff on transient errors, immediate reacquisition on fencing, and `PermanentError` to drop a slot without reacquisition; `ActiveSlots()` reports currently held IDs
- `leader.Config.BackoffInterval` — optional duration Elect sleeps before returning on
  non-fencing paths; throttles retry loops that would otherwise see rapid
  acquire/release/reacquire cycling now that Release expires the lease immediately (issue #36)
- `examples/cluster-singleton-scheduler` — demonstrates `leader.Elect` with `WithWaitForLease`
  standby failover and fencing propagation via context cancellation; Scenario 4 shows
  competing retry loops with `BackoffInterval`
- `examples/partition-processor` — demonstrates `pool.Pool` with concurrent slot acquisition,
  `ActiveSlots` observability, checkpoint-as-cursor resume on clean handoff, and `PermanentError`
  slot eviction

### Fixed

- `Release` now expires the lease immediately in both the memory and PostgreSQL backends,
  allowing a successor to acquire without waiting for the TTL to elapse. Previously a
  cleanly-released lease required the same full TTL wait as a crashed holder.

### Chore

- Add integration tests for `worker.Runner`, `leader.Elect`, and `pool.Pool` against
  the real in-memory backend; covers first acquisition, crash recovery, clean handoff,
  fencing propagation, `PermanentError` eviction, and `ActiveSlots` observability
- Named `releaseGracePeriod` constant in the memory backend and documented the
  `Release` immediate-expiry postcondition in the `Backend` interface contract (issue #37)

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
