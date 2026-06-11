# cluster-singleton-scheduler

A runnable example showing how `leader.Elect` coordinates a cluster-wide singleton — a
background scheduler that should run on exactly one node at a time. Demonstrates fail-fast
single election, standby failover with `WithWaitForLease`, and how fencing propagates to
the work function via context cancellation.

---

## Project structure

```
cluster-singleton-scheduler/
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
=== Scenario 1: Happy Path — Single-Node Election ===
  node-A: aggregated metrics (cycle 1/3)
  node-B: ErrLeaseHeld — another node is leader, skipping this cycle
  node-A: aggregated metrics (cycle 2/3)
  node-A: aggregated metrics (cycle 3/3)
  node-A: leadership term complete — lease released

=== Scenario 2: Standby Failover with WithWaitForLease ===
  node-C: aggregated metrics (cycle 1)
  node-C: aggregated metrics (cycle 2)
  node-C: crashed — lease expires in 3s
  node-D: waiting to become leader (WithWaitForLease)...
  node-D: acquired leadership — running scheduler
  node-D: aggregated metrics (cycle 1/3)
  node-D: aggregated metrics (cycle 2/3)
  node-D: aggregated metrics (cycle 3/3)
  node-D: leadership term complete — lease released

=== Scenario 3: Fenced Leader Exits via Context Cancellation ===
  node-E: aggregated metrics (cycle 1/2)
  node-E: aggregated metrics (cycle 2/2)
  node-E: stalled — awaiting context cancellation...
  node-F: acquired leadership (fencing token 2)
  node-E: context cancelled (context canceled)
  node-E: leader.Elect returned ErrFenced — another node took leadership
  node-F: released leadership
```

The example takes approximately 10 seconds — ~3 seconds in Scenario 2 waiting for node-C's
lease to expire, and ~5 seconds in Scenario 3 waiting for the lease to expire then for
node-E's renewal to detect the fence.

---

## Key implementation details

**`leader.Elect` vs the manual lifecycle** — without `leader.Elect`, single-leader code
requires five coordinated steps: `Acquire`, `StartRenewal`, work loop, `stopRenewal`,
`Release`. `leader.Elect` reduces this to one call. The caller implements only a
`func(ctx context.Context) error` that does the actual work; lifecycle management is
handled internally. The returned error is `ErrFenced` if the lease was superseded, or the
`fn` error otherwise.

**Fail-fast vs standby election** — Scenario 1 shows the default behavior: if the lease is
already held, `leader.Elect` returns `ErrLeaseHeld` immediately. The caller decides what to
do — skip this scheduling turn, log, and retry later. Scenario 2 shows the alternative:
passing `WithWaitForLease()` in `leader.Config.AcquireOptions` makes the call block until
the lease becomes available. This is the right option when you always want exactly one node
running — when the current leader disappears, the standby takes over automatically without
any external coordination.

**Fencing propagates via context — `fn` must check `ctx.Done()`** — Scenario 3 illustrates
the most important invariant. When a lease is fenced (another node acquires it), the
internal renewal goroutine detects `ErrFenced` from `Renew` and cancels `renewCtx`. The
work function receives this cancellation via its context argument. If the work function
ignores context cancellation — running a loop without a `select { case <-ctx.Done(): }`
branch — it will continue executing after the lease is gone, potentially conflicting with
the new leader. Fencing is not a signal that is delivered by magic; it only reaches the
work function if the work function is listening.

**`fn` receives `renewCtx`, not the outer `ctx`** — `leader.Elect` passes the renewal
context (not the context given to `Elect`) into `fn`. This means `fn` sees cancellation
from both fencing events and parent context cancellation. `Release` inside `Elect` uses
the original outer context so that releasing the lease succeeds even when `renewCtx` is
already cancelled.

---

## Next steps

- [Library overview](../../README.md)
- [Architecture](../../docs/ARCHITECTURE.md)
- [Partition processor example](../partition-processor/) — `pool.Pool` for distributing
  a fixed set of work IDs across competing processes
