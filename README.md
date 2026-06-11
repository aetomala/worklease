# worklease

[![Go Reference](https://pkg.go.dev/badge/github.com/aetomala/worklease.svg)](https://pkg.go.dev/github.com/aetomala/worklease)
[![Go Report Card](https://goreportcard.com/badge/github.com/aetomala/worklease)](https://goreportcard.com/report/github.com/aetomala/worklease)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**worklease** is a Go library for lease-based work coordination in distributed systems.

It is not a distributed lock library.

Distributed locks answer: *who owns this resource right now?* `worklease` answers a different question: *when ownership changes, what does the new owner need to continue the work the previous owner started?*

---

## The Problem

Worker A acquires a lease, begins processing a multi-step job, makes partial progress, then crashes. Worker B acquires the lease — and starts from scratch.

Depending on the operation, this means duplicated writes, lost progress, or inconsistent state. Idempotency helps only if restarting from the beginning is cheap. For long-running work — provider onboarding, async lifecycle management, batch processing with intermediate state — it isn't.

The correct fix is **checkpointed lease handoff**: the outgoing owner writes progress state atomically with its last lease renewal. The incoming owner reads that state before starting work. Fencing tokens prevent zombie writes from expired owners corrupting the checkpoint after they've been superseded.

Every Go distributed locking library (`distlock`, `pglock`, `dynamodb-lock-go`, etcd leases, `client-go/leaderelection`) solves **presence** — mutual exclusion, TTL expiry, heartbeat renewal. None solve **continuity**. `worklease` fills that gap.

### Is worklease the right fit?

`worklease` is designed for a specific shape of work: stateful, multi-step operations where restarting from scratch is expensive or incorrect, and where adopting a full workflow engine is not the right trade-off.

It is a good fit if you are running Go workers with PostgreSQL already in the stack, doing long-running jobs with meaningful intermediate state, and need correct handoff semantics without restructuring your application.

It is not the right fit if your jobs are small enough to restart cheaply — a job queue (River, Asynq, `FOR UPDATE SKIP LOCKED`) is simpler and sufficient. If your work is complex enough to need replay semantics and activity scheduling, Temporal is the right tool.

---

## Prior Art

This pattern is not new. AWS's Kinesis Client Library has implemented it since 2013 via its `LeaseRefresher`. KCL maintains a DynamoDB table where each lease entry holds an explicit `checkpoint` column and a `leaseCounter` for fencing. A worker atomically renews its lease while writing its progress checkpoint. When a worker's lease expires, the successor reads the checkpoint column and resumes from exactly that point. Fencing is enforced via conditional writes on `leaseCounter` — structurally identical to `worklease`'s `fencing_token`.

The pattern is proven and has been running production Kinesis workloads at AWS scale for over a decade.

What KCL doesn't provide:

- A Go-native implementation (KCL Go is a thin wrapper over a Java MultiLangDaemon process — not idiomatic)
- General-purpose work coordination (its checkpoint is a stream sequence number; the entire model assumes Kinesis shard processing)
- Portability outside AWS (DynamoDB backend, CloudWatch metrics, IAM — AWS-only)

`worklease` extracts the same semantics into a general-purpose Go primitive — no AWS dependency, no Kinesis assumption, pluggable backends.

---

## What worklease does not solve

These are intentional scope boundaries, not gaps.

**External side effects are not fenced.** `ErrFenced` stops stale checkpoint writes to the lease store. It does not stop a zombie worker from calling Stripe, sending an email, or writing to S3 before it discovers it has been superseded. Callers must make every external mutation idempotent, use an outbox pattern, or enforce idempotency keys at downstream systems. This is an inherent property of any coordination primitive — the library fences what it owns.

**The window between checkpoints is at-least-once.** If Worker A executes a step and crashes before checkpointing, Worker B will re-execute that step from the previous checkpoint. `worklease` provides a resumable progress marker, not exactly-once step execution.

**Recovery logic is yours.** The library delivers checkpoint bytes and the `cleanHandoff` flag. What those bytes mean, how to validate partial state, and how to reconcile external effects that happened before the crash are application concerns.

**Release marks intent, not immediate transfer.** Calling `Release` sets a flag and returns — it does not expire the lease. A successor cannot acquire until the TTL elapses. Choose TTLs and poll intervals that match the handoff latency your application can tolerate.

---

## Concepts

**Lease** — A time-limited claim on a named unit of work. Expires if not renewed. When it expires, another worker can acquire it.

**Fencing token** — A monotonically incrementing integer issued on every acquisition. A worker's writes to the lease store are rejected if a higher token has been issued — preventing zombie workers from corrupting the checkpoint after their lease expires.

**Checkpoint** — Progress state written atomically with lease renewal. If the worker is making progress, it proves liveness and saves state in one operation. The last checkpoint survives to the next owner. `Checkpoint` and `Renew` are distinct: `Checkpoint` writes state and extends the TTL atomically; `Renew` extends the TTL without updating state.

**Handoff vs crash recovery** — Two distinct acquisition paths. If the previous owner called `Release` explicitly, the checkpoint contains final state and the successor knows the work was cleanly surrendered. If the lease expired without a release, the checkpoint contains the last known progress and the successor resumes from partial state. These are different situations and callers handle them differently. The library makes the distinction explicit.

---

## Usage

### Acquiring a lease and checkpointing progress

```go
backend, err := postgres.New(db)
if err != nil {
    return err
}

lease, err := worklease.New(backend, worklease.Config{
    TTL:      30 * time.Second,
    HolderID: workerID,
})
if err != nil {
    return err
}

token, err := lease.Acquire(ctx, "onboarding:tenant-abc")
if err != nil {
    return err
}
defer lease.Release(ctx, token)

// Read what the previous owner left behind
state, cleanHandoff, err := lease.ReadCheckpoint(ctx, token)
if err != nil {
    return err
}

var progress OnboardingProgress
if state != nil {
    if err := json.Unmarshal(state, &progress); err != nil {
        return err
    }
    if !cleanHandoff {
        // Previous owner crashed — validate partial state before resuming.
        // External effects from incomplete steps may have already fired.
        progress = recoverFromPartial(progress)
    }
}

// Do work, checkpointing at each step boundary.
// Checkpoint after the external effect completes — not before.
for _, step := range remainingSteps(progress) {
    if err := executeStep(ctx, step); err != nil {
        return err
    }

    progress.CompletedSteps = append(progress.CompletedSteps, step.ID)
    snapshot, _ := json.Marshal(progress)

    // Atomically saves progress and renews the lease.
    // Returns ErrFenced if this worker has been superseded.
    if err := lease.Checkpoint(ctx, token, snapshot); err != nil {
        return err
    }
}
```

### Renewing without checkpointing

If the worker is healthy but not at a checkpointable boundary yet:

```go
if err := lease.Renew(ctx, token); err != nil {
    // ErrFenced: a higher token has been issued — stop working.
    return err
}
```

`Checkpoint` and `Renew` are separate because they mean different things. `Renew` says "I'm alive." `Checkpoint` says "I'm alive and here's where I am."

### Fresh acquisition

`ReadCheckpoint` returns `nil` state (not an error) when this is the first worker to acquire this lease:

```go
state, _, err := lease.ReadCheckpoint(ctx, token)
if err != nil {
    return err
}
if state == nil {
    // First acquisition — start from the beginning.
    progress = OnboardingProgress{}
}
```

---

## Error Reference

| Error | Meaning |
|---|---|
| `ErrFenced` | A higher fencing token has been issued. This worker has been superseded. Stop writing to the lease store. |
| `ErrLeaseHeld` | The lease is currently held by another worker. |
| `ErrLeaseExpired` | The lease expired before this operation completed. |

`ErrFenced` is the critical one. When `Checkpoint` or `Renew` returns `ErrFenced`, a successor has already acquired the lease and this worker is a zombie. Stop immediately — any further writes to the lease store will be rejected. Note that `ErrFenced` does not cancel in-flight external calls the worker may have already initiated; those must be made idempotent at the application layer.

---

## Upgrading

`v0.3.0` includes a breaking change in the `checkpoint` package: `Codec.Encode` and `Codec.Decode` are renamed to `Marshal` and `Unmarshal`. Callers using `checkpoint.JSON()` are unaffected. See [`UPGRADING.md`](UPGRADING.md) for full migration instructions.

---

## Higher-level packages

### worker — Lifecycle management

`worker.Runner` handles the acquire / ReadCheckpoint / StartRenewal / WorkFn / Release lifecycle so callers implement only the work function:

```go
import "github.com/aetomala/worklease/worker"

r, err := worker.NewRunner(worker.RunnerConfig{
    Lease:  lease,
    WorkFn: func(ctx context.Context, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
        // prior is the last checkpoint from the previous holder; nil on first acquisition.
        // Return updated checkpoint bytes, or nil to leave the checkpoint unchanged.
        return processWork(ctx, prior, cleanHandoff)
    },
})
if err != nil { ... }
if err := r.Run(ctx, "onboarding:tenant-abc"); err != nil { ... }
```

### leader — Simplified leadership

`leader.Elect` acquires a work ID, starts managed lease renewal, calls `fn` under the renewal context, stops renewal, and releases. Suitable for single-leader patterns where one process should be active at a time:

```go
import "github.com/aetomala/worklease/leader"

err := leader.Elect(ctx, lease, "scheduler:primary", leader.Config{}, func(ctx context.Context) error {
    // ctx is cancelled if the lease is fenced or renewal fails.
    // Return when the work is done; leader.Elect will release.
    return runScheduler(ctx)
})
```

Pass `worklease.WithWaitForLease()` in `leader.Config.AcquireOptions` to block until leadership is available rather than returning `ErrLeaseHeld` immediately.

### pool — Distributed work distribution

`pool.Pool` distributes a fixed set of work IDs across competing processes. Multiple Pool instances — one per process — collectively cover the full work ID set. Rebalancing is emergent from lease acquisition races:

```go
import "github.com/aetomala/worklease/pool"

p, err := pool.New(lease, pool.Config{
    WorkIDs: []string{"shard-0", "shard-1", "shard-2", "shard-3"},
}, func(ctx context.Context, workID string, token worklease.Token, prior []byte, cleanHandoff bool) ([]byte, error) {
    return processShard(ctx, workID, prior)
})
if err != nil { ... }

// Blocks until ctx is cancelled; all active slots complete before Run returns.
if err := p.Run(ctx); err != nil { ... }
```

Return a `PermanentError` from the work function to drop a slot without reacquisition.

### checkpoint — Typed serialization helpers

`checkpoint.Codec` and the generic `Encode[T]` / `Decode[T]` helpers add typed serialization on top of the raw `[]byte` checkpoint layer. `checkpoint.JSON()` returns a `JSONCodec` backed by `encoding/json`.

---

## Backends

### PostgreSQL (v0.1)

```go
import "github.com/aetomala/worklease/backend/postgres"

db, _ := sql.Open("postgres", dsn)
backend, _ := postgres.New(db)

lease, _ := worklease.New(backend, worklease.Config{
    TTL:      30 * time.Second,
    HolderID: os.Getenv("WORKER_ID"),
})
```

Run the migration before first use:

```sql
CREATE TABLE worklease_leases (
    work_id         TEXT PRIMARY KEY,
    holder_id       TEXT NOT NULL,
    fencing_token   BIGINT NOT NULL DEFAULT 1,
    expires_at      TIMESTAMPTZ NOT NULL,
    checkpoint      BYTEA,
    clean_handoff   BOOLEAN NOT NULL DEFAULT FALSE,
    acquired_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

The atomic checkpoint operation is a single `UPDATE ... WHERE fencing_token = $n`. Zero rows updated means the token is stale. There is no Lua script, no optimistic retry loop, no distributed clock dependency.

### In-memory (testing)

```go
import "github.com/aetomala/worklease/backend/memory"

backend := memory.New()
lease, _ := worklease.New(backend, worklease.Config{
    TTL:      5 * time.Second,
    HolderID: "test-worker",
})
```

Suitable for unit tests within a single process. Implements the same fencing token semantics as the PostgreSQL backend. Not safe for concurrent use across processes.

---

## Comparison

| Tool | Layer | Solves | Doesn't solve |
|---|---|---|---|
| `distlock` / `pglock` | Library | Mutual exclusion with TTL | No checkpoint, no handoff semantics |
| `dynamodb-lock-go` | Library | DynamoDB-backed locking | No progress state, no write fencing |
| etcd leases | Infrastructure | Kubernetes-native presence | No work coordination layer |
| `client-go/leaderelection` | Library | Single leader election | Not per-work-item ownership |
| KCL `LeaseRefresher` | Library | Checkpointed lease handoff at AWS scale | Java only, Kinesis-specific, AWS-only |
| Temporal | Platform | Durable workflow execution with replay | Requires full application rewrite as Workflows + Activities; run a server |
| Inngest | Platform | Managed durable step execution | Requires platform adoption — not a drop-in library |
| DBOS | Framework | DB-backed durable execution | Requires full application restructure; limited Go support |
| Watermill | Library | Go message routing | Checkpointing is broker offset tracking, not general work state |
| River / Asynq / job queues | Library | Work assignment with retry | No stateful handoff — successor restarts from scratch |
| **`worklease`** | Library | Work handoff with continuity, idiomatic Go, pluggable backends | Not a general-purpose lock; does not fence external side effects |

### Why not Temporal, Inngest, or DBOS?

These tools sit at the high-guarantee, high-adoption-cost end of the spectrum. They solve the failure mode — but the adoption unit is your entire application architecture.

Temporal requires rewriting business logic as Workflows and Activities, running a Temporal server, and accepting deterministic replay constraints. Inngest is a fully managed platform with its own execution model. DBOS restructures your entire application around a framework.

Teams already running Go workers with PostgreSQL don't need to restructure their application to get correct lease handoff semantics. They need a library primitive they can drop into the system they already have.

Distributed locks sit at the opposite end — low adoption cost, wrong abstraction. They tell you who holds the resource; they don't help the new holder continue what the old holder started.

The gap between these two ends is where `worklease` lives. The only prior art in that gap is KCL, which is Java-only and Kinesis-specific.

---

## Installation

```
go get github.com/aetomala/worklease
```

Requires Go 1.26+. PostgreSQL backend requires PostgreSQL 12+.

---

## Status

v0.3.0 is the current stable release. The public API (`Lease`, `Token`, options, sentinels) is stable.

---

## Roadmap

### Released

- **v0.1.0** — Core lease primitives: `Lease`, `Backend`, PostgreSQL + in-memory backends, fencing, checkpoint
- **v0.2.0** — `worker.Runner`, `checkpoint.Codec`, `LeaseObserver`, `memory.Clock`, examples
- **v0.3.0** — `leader.Elect`, `pool.Pool`, `HasWaitForLease`, `checkpoint.Codec` method rename (breaking — see `UPGRADING.md`)

### Future

- Redis backend
- etcd backend
- `Token` test constructor — unblocks table-driven tests that construct tokens directly

---

## Examples

### Subscription cancellation with crash recovery and fencing

Demonstrates the core worklease failure mode: a worker crashes mid-cancellation
after billing has fired but before resources are deprovisioned. A successor worker
reads the checkpoint and resumes without double-billing. A zombie fencing scenario
shows `ErrFenced` rejecting a stale write with both fencing token values visible in
the output.

No infrastructure required — runs against the in-memory backend.

```bash
cd examples/subscription-cancellation
go run .
```

### Cluster singleton scheduler with standby failover and fencing

Demonstrates the `leader` package: one node acquires leadership and runs a periodic
scheduler; a standby blocks with `WithWaitForLease` until the leader crashes and its
lease expires; a third scenario shows how fencing propagates to the work function via
context cancellation when a stalled leader is superseded.

No infrastructure required — runs against the in-memory backend.

```bash
cd examples/cluster-singleton-scheduler
go run .
```

### Partition processor with checkpoint resume and slot eviction

Demonstrates the `pool` package: a pool acquires a fixed set of named partitions and
processes them concurrently; `ActiveSlots` provides live observability of partition
ownership; a second pool resumes from checkpointed offsets on clean handoff; a
decommissioned partition exits via `PermanentError` while the rest of the pool
continues running.

No infrastructure required — runs against the in-memory backend.

```bash
cd examples/partition-processor
go run .
```

Source: [`examples/`](examples/)

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
