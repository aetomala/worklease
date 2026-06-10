# subscription-cancellation

A runnable example showing how `worklease` handles a multi-step SaaS subscription
cancellation when workers crash mid-flow — demonstrating crash recovery
(`cleanHandoff = false`), zombie fencing (`ErrFenced`), and the clean handoff path.

---

## Project structure

```
subscription-cancellation/
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
  worker-A: cancel billing
    cancel billing [tenant-alpha] — ok
  worker-A: schedule deprovisioning
    schedule deprovisioning [tenant-alpha] — ok
  worker-A: archive data
    archive data [tenant-alpha] — ok
  worker-A: send email
    send email [tenant-alpha] — ok
  worker-A: lease released cleanly (cleanHandoff=true)

=== Scenario 2: Crash Recovery ===
    cancel billing [tenant-beta] — ok
  worker-B: crashed after billing — lease expires in 3s
  [waiting 4s for lease to expire...]
  worker-C: acquired lease (fencing token 2)
  worker-C: cleanHandoff=false — previous worker crashed, validating partial state
  worker-C: billing already cancelled by previous worker — skipping
  worker-C: schedule deprovisioning
    schedule deprovisioning [tenant-beta] — ok
  worker-C: archive data
    archive data [tenant-beta] — ok
  worker-C: send email
    send email [tenant-beta] — ok
  worker-C: lease released cleanly (cleanHandoff=true)

=== Scenario 3: Zombie Fencing ===
  worker-D: acquired lease (fencing token 1), now stuck for 4s...
  [waiting 4s for lease to expire...]
  [lease expired]
  worker-E: acquired lease (fencing token 2)
  worker-D: ErrFenced — token 1 rejected; worker-E holds token 2 — zombie stopped
  worker-E: cancel billing
    cancel billing [tenant-gamma] — ok
  worker-E: schedule deprovisioning
    schedule deprovisioning [tenant-gamma] — ok
  worker-E: archive data
    archive data [tenant-gamma] — ok
  worker-E: send email
    send email [tenant-gamma] — ok
  worker-E: cancellation complete
```

The example takes approximately 9 seconds — 4 seconds in each of Scenarios 2 and 3
waiting for a 3-second lease TTL to elapse, plus step stubs.

---

## Key implementation details

**Effect before checkpoint** — each step calls the external function first, then
checkpoints. If the worker crashes between the effect and the checkpoint, the
successor re-executes that step. This is the at-least-once window. The checkpoint
records that the step completed safely; it does not prevent the effect from having
already fired. Steps must be idempotent at the downstream system.

**`StartRenewal` and `renewCtx`** — Scenario 1 calls `StartRenewal` to keep the
lease alive across all four steps. The returned `renewCtx` is cancelled if the
renewal loop detects the lease has been fenced. All steps use `renewCtx` so that a
fencing event propagates into downstream work. `Release` uses the original `ctx`
(not `renewCtx`) — once `stopRenewal()` is called, `renewCtx` may already be
cancelled, which would cause `Release` to fail.

**`cleanHandoff` distinction** — when a worker calls `Release`, the successor reads
`cleanHandoff = true`: the previous owner exited intentionally. When a lease expires
without a `Release`, the successor reads `cleanHandoff = false`: the previous owner
crashed or was fenced. These are different situations. Scenario 2 demonstrates the
crash case — Worker C logs the distinction and skips steps that are already marked
complete in the checkpoint.

---

## Next steps

- [Library overview](../../README.md)
- [Architecture](../../docs/ARCHITECTURE.md)
