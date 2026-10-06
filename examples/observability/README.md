# observability

A runnable example showing how to implement `worklease.LeaseObserver` to produce the
signals a production deployment cares about — per-operation counts and latency, lease hold
duration, a fencing counter, and counts per exit mode — using only the standard library. It demonstrates the
patterns that are non-obvious from the interface alone, and shows where a real metrics
backend (Prometheus, OpenTelemetry) would plug in.

---

## Project structure

```
observability/
├── go.mod    ← separate module; replace directive points to repo root
├── go.sum
└── main.go   ← metricsObserver + a clean lifecycle, a fencing scenario, and an abandoned run
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

Expected output (latency values vary run to run):

```
holder-A checkpoint correctly fenced: worklease: Checkpoint: workID="report-2026-06" holderID="holder-A": worklease: fenced — lease acquired by another holder
=== lease metrics ===
acquire          calls=3 errors=0 avg_latency=4.014µs
checkpoint       calls=3 errors=1 avg_latency=97ns
renew            calls=1 errors=0 avg_latency=125ns
release          calls=2 errors=0 avg_latency=125ns
read_checkpoint  calls=3 errors=0 avg_latency=69ns
fenced           total=1
renew_retries    total=0
hold_duration    holding[0]=32.333µs
hold_duration    holding[1]=416ns
release_by_mode  mode=finished total=1
release_by_mode  mode=abandoned total=1
release_by_mode  mode=retired total=0
read_by_prev     prev_exit=none total=1
read_by_prev     prev_exit=finished total=1
read_by_prev     prev_exit=abandoned total=0
read_by_prev     prev_exit=retired total=0
read_by_prev     prev_exit=expired total=1
```

The example runs instantly — expiry is driven by an injected fake clock, so there is no
real waiting.

---

## Key implementation details

**`LeaseObserver` is the single observability seam** — set `worklease.Config.Observer` to
any `LeaseObserver` and the library calls it synchronously after every operation. nil
installs a no-op, so observability is fully opt-in and call sites never nil-check. Because
the methods are called synchronously, an in-memory aggregator needs only a mutex; a real
implementation forwards to instruments that are already concurrency-safe.

**Per-operation latency comes from the event, not a wrapper** — every operation event
carries a `Duration` measuring the single final backend call (the `WithWaitForLease` poll
loop is excluded). The example sums it per operation and reports an average; a real backend
records it into a histogram. No `Lease` wrapping is required.

**Lease hold duration requires correlation on the fencing token** — hold duration spans two
callbacks, `OnAcquire` and `OnRelease`. The fencing token (`token.FencingToken()`) is the
stable identity of a single holding, so the example stores the acquire time keyed on it in
`OnAcquire` and computes the delta in `OnRelease`. A fenced holder may never reach a clean
`Release`, so an open correlation entry is expected — a real implementation would surface it
as an in-flight gauge rather than a completed observation.

**`OnFenced` enables a dedicated fencing counter** — when an operation is fenced, `OnFenced`
fires *in addition to* the operation-specific callback (`OnCheckpoint`/`OnRenew`/`OnRelease`),
and `FencedEvent.Operation` identifies which. Incrementing one counter in `OnFenced` gives an
accurate fencing rate without inspecting error values in every operation callback. The example
triggers a real fencing event: holder A's lease expires (fake clock advance), holder B
acquires the same work ID — bumping the fencing token — and holder A's next `Checkpoint` is
fenced.

**Counting exit modes** — since v0.6 every `ReleaseEvent` carries the declared `Mode`, and
every `ReadCheckpointEvent` carries `PrevExit`, how the previous holder left. The example
counts successful releases per `e.Mode.String()` and successful reads per
`e.PrevExit.String()`. Holder B reads `expired` because holder A never released, holder B
then releases with `finished`, and holder C reads `finished` and releases with `abandoned`.
In production, the share of `abandoned` releases is a failure rate, and reads that see
`expired` count crashes and lost leases. An invalid mode is rejected before the backend is
called, so it never produces a `ReleaseEvent`.

**Mapping to a real metrics backend** — the `metricsObserver` fields map directly to standard
instruments (see the comment block at the top of `main.go`): call/error counts → a labeled
`Counter`, latency and hold duration → `Histogram`s, fencing → a `Counter`, exit modes → a
`Counter` labelled by mode. The callback
bodies and correlation logic are identical whether the sink is an in-memory map, a Prometheus
`CounterVec`/`HistogramVec`, or OpenTelemetry instruments.

---

## Next steps

- [Library overview](../../README.md)
- [Architecture: Observability — LeaseObserver](../../docs/ARCHITECTURE.md)
- [ADR-0007: Observer injection and the v0.4 event-struct redesign](../../docs/adr/0007-observer-config-field.md)
