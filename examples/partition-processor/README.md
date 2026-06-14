# partition-processor

A runnable example showing how `pool.Pool` distributes a fixed set of named partitions
across competing processes. Demonstrates concurrent slot distribution with `ActiveSlots`
observability, checkpoint-as-cursor resume on clean handoff, and `PermanentError` slot
eviction for decommissioned work IDs.

---

## Project structure

```
partition-processor/
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

Expected output (log line order within each scenario is non-deterministic):

```
=== Scenario 1: Pool Distributes Work Across Partitions ===
  pool-A active slots: [part-0 part-1 part-2 part-3 part-4 part-5]
  [pool-A] processing part-4
  [pool-A] processing part-5
  [pool-A] processing part-2
  [pool-A] processing part-0
  [pool-A] processing part-1
  [pool-A] processing part-3

=== Scenario 2: Checkpoint Resume on Clean Handoff ===
  [pool-A] shard-0: processed events 0–99 (offset 100)
  [pool-A] shard-1: processed events 0–99 (offset 100)
  [pool-A] shard-2: processed events 0–99 (offset 100)
  [waiting 1s for pool-A leases to expire...]
  [pool-B] shard-0: resuming from offset 100 (clean handoff)
  [pool-B] shard-1: resuming from offset 100 (clean handoff)
  [pool-B] shard-2: resuming from offset 100 (clean handoff)
  [pool-B] shard-0: processed events 100–199 (offset 200)
  [pool-B] shard-1: processed events 100–199 (offset 200)
  [pool-B] shard-2: processed events 100–199 (offset 200)

=== Scenario 3: PermanentError Drops a Decommissioned Slot ===
  [pool-C] queue-2: decommissioned — dropping slot permanently
  pool-C active slots after queue-2 eviction: [queue-0 queue-1]
  [pool-C] queue-0: processed batch (offset 50)
  [pool-C] queue-1: processed batch (offset 50)
```

The example takes approximately 3 seconds — ~1 second in Scenario 2 waiting for pool-A's
leases to expire, and ~1.2 seconds in Scenario 3 until the context timeout.

---

## Key implementation details

**`pool.Pool` vs `worker.Runner`** — `worker.Runner` manages a single work ID: one
acquire, one `WorkFn` call, one release. `pool.Pool` manages a fixed set of work IDs
concurrently. Each work ID gets its own internal `Runner`. The pool loops indefinitely per
slot — when `WorkFn` returns nil, the slot is immediately reacquired and the function is
called again. This continues until the context is cancelled, a `PermanentError` is returned,
or the process exits.

**`ActiveSlots` for observability** — `p.ActiveSlots()` returns the work IDs currently held
by this pool instance. It is safe to call at any time, including while the pool is running.
In a multi-process deployment this gives a live view of which partitions each process owns,
which is useful for dashboards, health checks, and diagnosing rebalancing behavior. Scenario
1 checks `ActiveSlots` at 50ms while all 6 `WorkFn` calls are in-flight, showing the full
partition set claimed immediately on startup.

**`BackoffInterval` prevents busy-loop on `ErrLeaseHeld`** — when a slot is held by another
process, `Acquire` returns `ErrLeaseHeld`. Without a backoff, the acquisition loop would
spin at CPU speed retrying. `Config.BackoffInterval` inserts a wait between the error
return and the next attempt. Set it to a duration appropriate for your TTL — a value in the
range of 10–20% of the TTL is a reasonable starting point.

**`PermanentError` for slot eviction** — returning a `PermanentError` from `WorkFn` causes
the slot goroutine to exit without reacquiring. The error value signals the pool with two
methods: `error` (message) and `Permanent() bool` (returns true). Any application-defined
type satisfying that interface works. Scenario 3 uses this to model a decommissioned
partition: `queue-2` returns `&slotDone{"decommissioned"}` immediately. The other two slots
continue running normally. `ActiveSlots` at 50ms shows only `[queue-0 queue-1]` — the
eviction is reflected in the active set without stopping the pool.

**Cross-holder handoff requires waiting for TTL expiry** — Scenario 2 illustrates a
critical constraint. When pool-A's `WorkFn` returns a `PermanentError`, the `Runner` calls
`Release`, which sets `cleanHandoff=true` in the lease record. However, `Release` does not
change `expires_at`. A *different* holder — pool-B — cannot acquire the lease until
`expires_at <= NOW()`. Only the *same* holder can reacquire before TTL expiry. This is why
Scenario 2 uses a 1-second TTL for pool-A and inserts an explicit `time.Sleep(1100ms)`
after `poolA.Run` returns. In production, choose TTLs that balance failover latency against
the cost of false expiry.

---

## Next steps

- [Library overview](../../README.md)
- [Architecture](../../docs/ARCHITECTURE.md)
- [Cluster singleton scheduler example](../cluster-singleton-scheduler/) — `leader.Elect`
  for single-leader patterns
