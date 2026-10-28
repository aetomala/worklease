# ADR-0020: v1 compatibility promise

**Status:** Accepted
**Date:** 2026-10-26
**Amends:** ADR-0019 (error text, interface growth), ADR-0017 (schema migration)
**Issues:** none

## Context

v1.0.0 freezes the exported API that ADR-0019 audited. A promise also covers behavior the identifier list does not show: default values, error text, the PostgreSQL schema, the supported Go versions, and how interfaces may grow. v0.8 left these unstated or stated piecemeal (D-4, D-10, D-20, ADR-0017). This ADR states them once.

## Decision

| # | Subject | Decision |
|---|---|---|
| 1 | Semantic versioning | From v1.0.0 the module follows semantic versioning. A change to any exported identifier in a package that is not under `internal/` is breaking unless a row below says otherwise |
| 2 | Error text | Sentinels are matched with `errors.Is`. Message text, including the wrapped format `worklease: <Op>: workID=… holderID=…: <sentinel>`, is not part of the promise. A change to message text is listed under `### Changed` (D-20) |
| 3 | Interface growth | Three documented exceptions to strict semver for implementers: `Lease` and the observer interfaces (`LeaseObserver`, `VacuumObserver`, `pool.Observer`) may gain methods in a minor release, and implementers embed the matching type (`NoopObserver`, `NoopVacuumObserver`, `pool.NoopObserver`; a wrapper of `Lease` embeds the interface). The `backend.Backend` method set is fixed for all of v1; new capabilities arrive as optional interfaces found by type assertion (D-4) |
| 4 | Defaults | A default in the table below marked **tunable** may change in a minor release when the CHANGELOG lists it; callers who depend on a value set it explicitly. A default marked **frozen** is part of the API |
| 5 | Schema | A minor release may add tables, nullable columns, and indexes. It may not drop or rename a column, change a column type, or tighten a constraint; any such change is a major release. Applying and migrating the schema stays the caller's job (ADR-0017); each release that changes the schema gives the DDL in `UPGRADING.md` |
| 6 | Supported Go | The two latest Go releases. The `go` directive in `go.mod` is raised only in a minor release, when the oldest supported release leaves support, and the CHANGELOG says so. The `toolchain` line is not part of the promise. At v1.0.0 the two releases are Go 1.26 and 1.27, and the `go` directive is `go 1.26.0` (D-30, 2026-10-27) |
| 7 | Deprecation | An exported identifier is marked `Deprecated:` in godoc for at least one minor release before a major release removes it |
| 8 | Conformance suite | `backend/conformance` tightens the backend contract only in a major release; a minor release may add specs only for a new optional capability. The suite's Ginkgo and Gomega dependency is documented (D-9) |
| 9 | Database driver | `backend/postgres` registers no driver; the caller chooses one (D-19). The suites run against `github.com/lib/pq` only |
| 10 | `internal/` | Nothing under `internal/` is promised (D-1) |

### Defaults (values verified by script on the `v0.8.0` tree, `0af5b72`; re-verified on the v1.0 tip, Rule 58)

| Setting | Default | Source | Class |
|---|---|---|---|
| Renewal interval | TTL/2, floored at one nanosecond | `renewal.go`, `lease.go` | tunable |
| Renewal retry backoff: initial, maximum, jitter | 100 ms, 5 s, 20% | `lease.go` | tunable |
| `WithPollInterval` | 2 s | `lease.go`, `acquire.go` | tunable |
| `pool.Config` `IdleInterval`, `RerunInterval`, `BackoffInterval` | 1 s each | `pool/pool.go` | tunable |
| `leader.Config` `IdleInterval` | 1 s | `leader/leader.go` | tunable |
| `CleanupTimeout` (`worker`, `leader`) | 5 s | `worker/runner.go`, `leader/leader.go` | tunable |
| `leader.Config.BackoffInterval` zero value | no sleep | `leader/leader.go` | frozen (D-10) |
| `pool.Config.BackoffInterval` zero value | 1 s | `pool/pool.go` | frozen (D-10) |
| Nil observer | the matching no-op observer | `worklease.go`, `vacuum.go`, `pool/pool.go` | frozen |
| `Config.TTL` | required, at least 1 ms (`minTTL`); a smaller value is a configuration error | `worklease.go` | frozen |

## Consequences

The promise is stricter than a bare "semver" claim in three places the owner should check: rows 3 (observers and `Lease` may grow), 4 (some defaults may be retuned), and 8 (the conformance suite is versioned with the library). Row 1 is checked at each release from v1.0.0 with `gorelease` or `apidiff` against the previous tag (E-1). Rows 4 to 6 are policy only and are enforced by review.
