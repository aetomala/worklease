# worklease — Examples

Runnable examples demonstrating lease-based work coordination with `worklease`.
Each example uses the in-memory backend — no infrastructure required.
Swap in the PostgreSQL backend for production use.

---

## Examples

### [Subscription Cancellation](subscription-cancellation/)

A multi-step SaaS cancellation flow that crashes mid-execution. Best for:
- Understanding what worklease solves that a distributed lock does not
- Seeing crash recovery (`cleanHandoff = false`) and zombie fencing (`ErrFenced`) side by side

**Features**:
- Three sequential scenarios: happy path, crash recovery, zombie fencing
- `StartRenewal` keeping the lease alive across a long-running flow
- Effect-before-checkpoint ordering demonstrated explicitly

**Run**:
```bash
cd subscription-cancellation
go run .
```

---

### [Cross-Tenant Migration](cross-tenant-migration/)

A coordinator that migrates tenants one at a time across a shared database. Best for:
- Understanding checkpoint-as-cursor (progress through a collection, not steps in a flow)
- Seeing how crash recovery skips already-processed items rather than re-running from scratch

**Features**:
- For-loop coordination pattern with `StartRenewal` keeping the lease alive across many iterations
- Checkpoint after each tenant — fine-grained recovery cursor
- Zombie fencing mid-batch with fencing token values in output

**Run**:
```bash
cd cross-tenant-migration
go run .
```

---

### [Cluster Singleton Scheduler](cluster-singleton-scheduler/)

A background scheduler that must run on exactly one cluster node at a time, using the
`leader` package. Best for:
- Understanding `leader.Elect` as a simplified alternative to the manual acquire/StartRenewal/Release lifecycle
- Seeing how `WithWaitForLease` turns a fail-fast election into an always-on standby
- Understanding how fencing propagates to the work function via context cancellation

**Features**:
- Scenario 1: Fail-fast election — the losing node gets `ErrLeaseHeld` and skips
- Scenario 2: Standby failover — `WithWaitForLease` blocks until the crashed leader's lease expires
- Scenario 3: Fenced leader exits cleanly — `renewCtx` cancellation reaches the work loop

**Run**:
```bash
cd cluster-singleton-scheduler
go run .
```

---

### [Partition Processor](partition-processor/)

An event pipeline with a fixed set of named partitions, using the `pool` package. Best for:
- Understanding `pool.Pool` as a concurrent alternative to running a single `worker.Runner`
- Seeing `ActiveSlots` used for runtime observability of partition ownership
- Understanding `PermanentError` for permanent slot eviction and cross-holder checkpoint resume

**Features**:
- Scenario 1: Pool acquires all 6 partitions immediately; `ActiveSlots` shows concurrent ownership
- Scenario 2: Pool-A checkpoints per-partition cursors; Pool-B resumes from those offsets on clean handoff
- Scenario 3: `PermanentError` evicts a decommissioned partition while the rest of the pool keeps running

**Run**:
```bash
cd partition-processor
go run .
```
