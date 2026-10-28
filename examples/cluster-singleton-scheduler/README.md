# cluster-singleton-scheduler

A runnable example showing how `leader.Elect` coordinates a cluster-wide singleton — a
background scheduler that should run on exactly one node at a time. Demonstrates fail-fast
single election, standby failover with `WithWaitForLease`, and how losing the lease (fencing
or a closed lease window) propagates to the work function via context cancellation.

---

## Project structure

```
cluster-singleton-scheduler/
├── go.mod    ← separate module; replace directive points to repo root
├── go.sum
└── main.go   ← four scenarios, no infrastructure required
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
  node-A: aggregated metrics (cycle 2/3)
  node-A: aggregated metrics (cycle 3/3)
  node-A: OnRelinquished — term released with ExitFinished (fencing token 1)
  node-A: leadership term complete — lease released
  node-B: ErrLeaseHeld — another node is leader, skipping this cycle

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

=== Scenario 3: Stalled Renewal Stops the Leader When Its Lease Window Closes ===
  node-E: aggregated metrics (cycle 1/20)
  node-E: aggregated metrics (cycle 2/20)
  node-E: aggregated metrics (cycle 3/20)
  node-E: aggregated metrics (cycle 4/20)
  node-E: aggregated metrics (cycle 5/20)
  node-E: aggregated metrics (cycle 6/20)
  node-E: aggregated metrics (cycle 7/20)
  node-E: aggregated metrics (cycle 8/20)
  node-E: aggregated metrics (cycle 9/20)
  node-E: interrupted at cycle 10
  node-E: renewal context cancelled (worklease: lease window exhausted before renewal succeeded)
  node-E: OnLost — lease window closed, stopping work
  node-E: leader.Elect returned without releasing — the successor will see ExitExpired
  node-F: acquired leadership (fencing token 2); previous leader's exit: expired
  node-F: released leadership

=== Scenario 4: Persistent Leader with BackoffInterval ===
  Two nodes compete in retry loops. BackoffInterval=300ms paces the leader
  between terms; IdleInterval (default 1s) paces the follower, so the
  leader usually wins its next term too.
  node-H: leading (term 1/2)
  node-H: aggregated metrics (cycle 1/1)
  node-H: leading (term 2/2)
  node-H: aggregated metrics (cycle 1/1)
  node-H: exiting after 2 term(s)
  node-G: leading (term 1/2)
  node-G: aggregated metrics (cycle 1/1)
  node-G: leading (term 2/2)
  node-G: aggregated metrics (cycle 1/1)
  node-G: exiting after 2 term(s)
```

The example takes approximately 10 seconds — about 1.5 seconds in Scenario 1, where node-B
waits `IdleInterval` before it reports `ErrLeaseHeld`; about 4 seconds in Scenario 2, 3 of
them waiting for node-C's lease to expire; about 2 seconds in Scenario 3, until node-E's
lease window closes; and about 2 seconds in Scenario 4.

---

## Key implementation details

**`leader.Elect` vs the manual lifecycle** — without `leader.Elect`, single-leader code
requires five coordinated steps: `Acquire`, `StartRenewal`, work loop, `stopRenewal`,
`Release`. `leader.Elect` reduces this to one call. The caller implements only a
`func(ctx context.Context) error` that does the actual work; lifecycle management is
handled internally. The returned error is `ErrFenced` if the lease was superseded; the
`Acquire` error, after a wait, if the lease was not acquired; and otherwise the `fn` error,
or a `leader: release:` error when `fn` succeeded and `Release` failed. If the lease window
runs out, `Elect` does not release, and returns the `fn` error, or the cause wrapped as
`leader: …` when `fn` returned nil.

**Fail-fast vs standby election** — Scenario 1 shows the default behavior: if the lease is
already held, `leader.Elect` returns `ErrLeaseHeld` after waiting `IdleInterval` (default 1s). The caller decides what to
do — skip this scheduling turn, log, and retry later. Scenario 2 shows the alternative:
passing `WithWaitForLease()` in `leader.Config.AcquireOptions` makes the call block until
the lease becomes available. This is the right option when you always want exactly one node
running — when the current leader disappears, the standby takes over automatically without
any external coordination.

**Losing the lease propagates via context — `fn` must check `ctx.Done()`** — Scenario 3
illustrates the most important invariant. node-E's database stops answering renewals. The
renewal goroutine does not wait for the stalled call: when node-E's lease window closes, it
cancels `renewCtx` with cause `ErrLeaseWindowExhausted` and `OnLost` fires, so node-E's work
stops before any successor can acquire the lease. node-F then takes over and reads
`ExitExpired`, because node-E never released. The same cancellation delivers `ErrFenced` when a
successor does acquire the lease. It reaches the work function only if the work function is
listening: a loop without a `select { case <-ctx.Done(): }` branch keeps running after the
lease is gone.

**`fn` receives `renewCtx`, not the outer `ctx`** — `leader.Elect` passes the renewal
context (not the context given to `Elect`) into `fn`. This means `fn` sees cancellation
from both fencing events and parent context cancellation. `Release` inside `Elect` runs on
a context that survives cancellation of the outer `ctx`, bounded by
`leader.Config.CleanupTimeout` (default 5s), so releasing the lease succeeds even when
`renewCtx` or `ctx` is already cancelled.

**`OnRelinquished` means a clean handover** — Scenario 1 sets `leader.Config.OnRelinquished`.
It fires only after `Release` succeeds with `ExitFinished` (or `ExitRetired`, when `fn`
returns `worklease.ErrRetire`). If `fn` returns any other error, `Elect` releases with
`ExitAbandoned` and does not call it, so the callback never reports a failed term as a
handover.

**`IdleInterval` paces the follower, `BackoffInterval` the leader** — Scenario 4 runs `Elect`
in a retry loop with no sleep of its own. `BackoffInterval` throttles the loop after a term the
node actually held, including a failed one. When the other node is leading, `Elect` waits
`IdleInterval` (default 1s, up to 20% jitter) before returning `ErrLeaseHeld`, so the follower
polls `Acquire` about once a second instead of in a tight loop. Because the leader's 300ms
backoff is shorter than that wait, the node that leads first usually takes all its terms before
the other gets one. Call `Lease.Acquire` directly when you want a single immediate attempt.

---

## Next steps

- [Library overview](../../README.md)
- [Architecture](../../docs/ARCHITECTURE.md)
- [Partition processor example](../partition-processor/) — `pool.Pool` for distributing
  a fixed set of work IDs across competing processes
