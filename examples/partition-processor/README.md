# partition-processor

A runnable example showing how `pool.Pool` distributes a fixed set of named partitions
across competing processes. Demonstrates concurrent slot distribution with `ActiveSlots`
observability, checkpoint-as-cursor resume after another pool stops, `PermanentError` slot
eviction for decommissioned work IDs, and retiring a finished work ID with
`worklease.ErrRetire`.

---

## Project structure

```
partition-processor/
├── go.mod    ← separate module; replace directive points to repo root
├── go.sum
└── main.go   ← four scenarios, no infrastructure required
```

---

## Setup

Prerequisites: Go 1.25+.

```bash
go mod tidy
```

---

## Running the example

```bash
go run .
```

Expected output (log line order within each scenario is non-deterministic):

```
=== Scenario 1: Pool Distributes Work Across Partitions ===
  pool-A active slots: [part-0 part-1 part-2 part-3 part-4 part-5]
  [pool-A] processing part-2
  [pool-A] processing part-3
  [pool-A] processing part-1
  [pool-A] processing part-4
  [pool-A] processing part-0
  [pool-A] processing part-5

=== Scenario 2: Checkpoint Resume After pool-A Stops ===
  [pool-A] shard-0: processed events 0–99 (offset 100)
  [pool-A] shard-1: processed events 0–99 (offset 100)
  [pool-A] shard-2: processed events 0–99 (offset 100)
  [pool-B] shard-0: resuming from offset 100 (PrevExit=abandoned, PrevHolderID=pool-A)
  [pool-B] shard-1: resuming from offset 100 (PrevExit=abandoned, PrevHolderID=pool-A)
  [pool-B] shard-2: resuming from offset 100 (PrevExit=abandoned, PrevHolderID=pool-A)
  [pool-B] shard-2: processed events 100–199 (offset 200)
  [pool-B] shard-0: processed events 100–199 (offset 200)
  [pool-B] shard-1: processed events 100–199 (offset 200)

=== Scenario 3: PermanentError Drops a Decommissioned Slot ===
  [pool-C] queue-2: decommissioned — dropping slot permanently
  pool-C active slots after queue-2 eviction: [queue-0 queue-1]
  [pool-C] queue-0: processed batch (offset 50)
  [pool-C] queue-1: processed batch (offset 50)
  [pool-C] queue-1: processed batch (offset 100)
  [pool-C] queue-0: processed batch (offset 100)
  [pool-C] queue-0: processed batch (offset 150)
  [pool-C] queue-1: processed batch (offset 150)

=== Scenario 4: ErrRetire Retires a Finished Partition ===
  [pool-D] topic-legacy: drained — returning worklease.ErrRetire
  [pool-D] OnSlotRetired: topic-legacy — slot stopped, work ID retired
  [pool-D] topic-0: processed one batch — stopping slot
  [pool-D] topic-1: processed one batch — stopping slot
  pool-D Run returned: <nil>
```

The example takes about 2 seconds, most of it the 1.2-second context timeout in Scenario 3.

---

## Key implementation details

**`pool.Pool` vs `worker.Runner`** — `worker.Runner` manages a single work ID: one
acquire, one `WorkFn` call, one release. `pool.Pool` manages a fixed set of work IDs
concurrently. Each work ID gets its own internal `Runner`, so each run is released with the
exit mode that matches the work function's result. The pool loops per slot — when `WorkFn`
returns nil, the slot waits `RerunInterval` and runs again. This continues until the context
is cancelled, the slot returns a `PermanentError` or retires its work ID with `ErrRetire`, or
the process exits.

**`ActiveSlots` for observability** — `p.ActiveSlots()` returns the work IDs currently held
by this pool instance. It is safe to call at any time, including while the pool is running.
In a multi-process deployment this gives a live view of which partitions each process owns,
which is useful for dashboards, health checks, and diagnosing rebalancing behavior. Scenario
1 checks `ActiveSlots` at 50ms while all 6 `WorkFn` calls are in-flight, showing the full
partition set claimed immediately on startup.

**Pacing: `IdleInterval`, `RerunInterval`, `BackoffInterval`** — every looping path
waits except the immediate reacquire after `ErrFenced`. When a peer process holds the work
ID, `Acquire` returns `ErrLeaseHeld` and the slot waits `IdleInterval`. After a work
function returns `nil`, the slot waits `RerunInterval` before running again, which also
gives peers a chance to win the work ID. After any other error, it waits `BackoffInterval`
and fires `OnSlotBackoff`. All three default to 1s, and `IdleInterval` and `RerunInterval`
add up to 20% jitter. This example sets `IdleInterval` (100ms) and `RerunInterval` (300ms)
explicitly so it runs quickly.

**`PermanentError` for slot eviction** — returning a `PermanentError` from `WorkFn` causes
the slot goroutine to exit without reacquiring. The error value signals the pool with two
methods: `error` (message) and `Permanent() bool` (returns true). Any application-defined
type satisfying that interface works. Scenario 3 uses this to model a decommissioned
partition: `queue-2` returns `&slotDone{"decommissioned"}` immediately. The other two slots
continue running normally. `ActiveSlots` at 50ms shows only `[queue-0 queue-1]` — the
eviction is reflected in the active set without stopping the pool.

**Resuming after another pool stops** — Scenario 2's pool-A ends each slot with a
`PermanentError`. That is a non-fencing error, so each slot's `Runner` checkpoints the
offset and releases the lease with `ExitAbandoned`. `Release` expires the lease
immediately (ADR-0012), so pool-B acquires the work IDs at once, without waiting for the
TTL. Its work function reads `PrevExit == ExitAbandoned` and `PrevHolderID == pool-A`, and
resumes from the checkpointed offset. A cursor is safe to resume from however the previous
holder left; a work function that keeps richer state would validate it first after
`ExitAbandoned` or `ExitExpired`.

**Retiring a work ID** — Scenario 4's `topic-legacy` is fully drained, so its work function
returns `worklease.ErrRetire`. The slot releases with `ExitRetired`, `OnSlotRetired` fires on
the pool's `Observer`, and the slot stops without backoff. The other two slots stop through
`PermanentError`. Because at least one slot retired, `Run` returns `nil` rather than
`ErrAllSlotsDead`.

---

## Next steps

- [Library overview](../../README.md)
- [Architecture](../../docs/ARCHITECTURE.md)
- [Cluster singleton scheduler example](../cluster-singleton-scheduler/) — `leader.Elect`
  for single-leader patterns
