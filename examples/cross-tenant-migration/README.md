# cross-tenant-migration

A runnable example showing how `worklease` coordinates a data migration across all
tenants in a multi-tenant SaaS — demonstrating checkpoint-as-cursor, crash recovery
that resumes from the last migrated tenant, and zombie fencing mid-batch.

- **Cursor checkpoint** — the checkpoint is an append-only list of migrated tenant IDs, not boolean step flags; recovery builds a set and skips any tenant already in it
- **Crash recovery** — a crashed coordinator's successor reads the cursor, skips already-migrated tenants, and resumes from the next one
- **Zombie fencing mid-batch** — a stale coordinator's `Checkpoint` call is rejected with `ErrFenced`; both fencing token values are printed

---

## Project structure

```
cross-tenant-migration/
├── go.mod    ← separate module; replace directive points to repo root
├── go.sum
└── main.go   ← three scenarios, no infrastructure required
```

---

## Setup

Prerequisites: Go 1.26+.

```bash
go mod tidy
```

---

## Running the example

```bash
go run .
```

Expected output:

```
=== Scenario 1: Happy Path ===
  coordinator-A: migrated tenant-001 (1/8)
  coordinator-A: migrated tenant-002 (2/8)
  coordinator-A: migrated tenant-003 (3/8)
  coordinator-A: migrated tenant-004 (4/8)
  coordinator-A: migrated tenant-005 (5/8)
  coordinator-A: migrated tenant-006 (6/8)
  coordinator-A: migrated tenant-007 (7/8)
  coordinator-A: migrated tenant-008 (8/8)
  coordinator-A: all 8 tenants migrated, lease released cleanly

=== Scenario 2: Crash Recovery ===
  coordinator-B: migrated tenant-001 (1/8)
  coordinator-B: migrated tenant-002 (2/8)
  coordinator-B: migrated tenant-003 (3/8)
  coordinator-B: crashed after 3 tenants — lease expires in 3s
  [waiting 4s for lease to expire...]
  coordinator-C: acquired lease (fencing token 2)
  coordinator-C: cleanHandoff=false — 3 tenants already migrated, resuming
  coordinator-C: skipping tenant-001 — already migrated
  coordinator-C: skipping tenant-002 — already migrated
  coordinator-C: skipping tenant-003 — already migrated
  coordinator-C: migrated tenant-004 (4/8)
  coordinator-C: migrated tenant-005 (5/8)
  coordinator-C: migrated tenant-006 (6/8)
  coordinator-C: migrated tenant-007 (7/8)
  coordinator-C: migrated tenant-008 (8/8)
  coordinator-C: all 8 tenants migrated, lease released cleanly

=== Scenario 3: Zombie Fencing ===
  coordinator-D: acquired (token 1), migrated 2 tenants, now stuck...
  [waiting 4s for lease to expire...]
  [lease expired]
  coordinator-E: acquired (token 2)
  coordinator-D: ErrFenced — token 1 rejected; coordinator-E holds token 2 — zombie stopped
  coordinator-E: resuming from checkpoint (2 tenants already migrated)
  coordinator-E: skipping tenant-001 — already migrated
  coordinator-E: skipping tenant-002 — already migrated
  coordinator-E: migrated tenant-003 (3/8)
  coordinator-E: migrated tenant-004 (4/8)
  coordinator-E: migrated tenant-005 (5/8)
  coordinator-E: migrated tenant-006 (6/8)
  coordinator-E: migrated tenant-007 (7/8)
  coordinator-E: migrated tenant-008 (8/8)
  coordinator-E: all 8 tenants migrated, lease released cleanly
```

The example takes approximately 9 seconds — 4 seconds in each of Scenarios 2 and 3
waiting for a 3-second lease TTL to elapse, plus step stubs.

---

## Key implementation details

**Cursor checkpoint vs. step flags** — unlike the `subscription-cancellation` example
where the checkpoint records which named steps have completed, here the checkpoint is
an append-only `[]string` of tenant IDs. Recovery builds a `map[string]bool` from
that slice and skips any tenant already present. This models progress through a
collection, not progress through a fixed set of steps.

**Context discipline in the loop** — `migrateTenant` receives `renewCtx` so that a
fencing event propagates into the migration call and fails it fast. `Checkpoint`
receives `context.Background()` — the migration effect has already fired, so the
checkpoint must complete regardless of renewal state. Using `renewCtx` for
`Checkpoint` would silently drop progress if the context cancelled between the
effect and the record.

**`cleanHandoff` distinction** — when a coordinator calls `Release`, the successor
reads `cleanHandoff = true`: the previous owner exited intentionally. When a lease
expires without `Release`, the successor reads `cleanHandoff = false`: the coordinator
crashed or was fenced. Scenario 2 demonstrates the crash case — coordinator-C logs
the distinction and skips the 3 tenants already recorded in the cursor.

---

## Next steps

- [Library overview](../../README.md)
- [Architecture](../../docs/ARCHITECTURE.md)
- [subscription-cancellation example](../subscription-cancellation/)
