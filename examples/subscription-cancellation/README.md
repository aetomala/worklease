# subscription-cancellation

A runnable example showing how `worklease` handles a multi-step SaaS subscription
cancellation when workers crash mid-flow — demonstrating how each worker reads the previous
holder's exit (`PrevExit`, `PrevHolderID`), crash recovery (`ExitExpired`), zombie fencing
(`ErrFenced`), and a completed run (`ExitFinished`).

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

Prerequisites: Go 1.25+.

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
  worker-A: acquired lease (fencing token 1) — PrevExit=none, PrevHolderID=none
  worker-A: cancel billing
    cancel billing [tenant-alpha] — ok
  worker-A: schedule deprovisioning
    schedule deprovisioning [tenant-alpha] — ok
  worker-A: archive data
    archive data [tenant-alpha] — ok
  worker-A: send email
    send email [tenant-alpha] — ok
  worker-A: work function returned nil — released with ExitFinished

=== Scenario 2: Crash Recovery ===
    cancel billing [tenant-beta] — ok
  worker-B: crashed after billing — lease expires in 3s
  [waiting 4s for lease to expire...]
  worker-C: acquired lease (fencing token 3) — PrevExit=expired, PrevHolderID=worker-B
  worker-C: previous worker's lease expired with no declared exit — validating partial state
  worker-C: billing already cancelled by previous worker — skipping
  worker-C: schedule deprovisioning
    schedule deprovisioning [tenant-beta] — ok
  worker-C: archive data
    archive data [tenant-beta] — ok
  worker-C: send email
    send email [tenant-beta] — ok
  worker-C: work function returned nil — released with ExitFinished

=== Scenario 3: Zombie Fencing ===
  worker-D: acquired lease (fencing token 4), now stuck for 4s...
  [waiting 4s for lease to expire...]
  [lease expired]
  worker-E: acquired lease (fencing token 5) — PrevExit=expired, PrevHolderID=worker-D
  worker-D: ErrFenced — token 4 rejected; worker-E holds token 5 — zombie stopped
  worker-E: cancel billing
    cancel billing [tenant-gamma] — ok
  worker-E: schedule deprovisioning
    schedule deprovisioning [tenant-gamma] — ok
  worker-E: archive data
    archive data [tenant-gamma] — ok
  worker-E: send email
    send email [tenant-gamma] — ok
  worker-E: cancellation complete — released with ExitFinished
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

**`worker.Runner` and `renewCtx`** — each scenario runs its steps through
`worker.Runner`, which starts lease renewal for the whole run. The work function
receives `renewCtx`, which is cancelled if the renewal loop detects the lease has been
fenced or its window ran out. All steps use `renewCtx` so that a fencing event
propagates into downstream work. `Runner` releases on its own cleanup context, which
survives cancellation of the caller's `ctx` (bounded by `CleanupTimeout`, default 5s).

**Exit modes** — every worker logs `PrevExit` and `PrevHolderID` when it acquires.
A first acquisition reads `ExitNone`. When a work function returns `nil`, `Runner`
releases with `ExitFinished`, and the next holder would read `ExitFinished`. When a
lease expires with no `Release`, as with worker-B and worker-D, the successor reads
`ExitExpired`: the previous holder crashed or stalled, and external effects may have
fired after its last checkpoint. A work function that returns an error releases with
`ExitAbandoned`, which a successor also treats as partial state. Scenario 2 shows the
crash case: worker-C validates the partial state and skips steps already marked
complete in the checkpoint.

---

## Next steps

- [Library overview](../../README.md)
- [Architecture](../../docs/ARCHITECTURE.md)
