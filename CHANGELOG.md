# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
