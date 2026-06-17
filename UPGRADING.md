# Upgrading worklease

## v0.3.x → v0.4.0

### Breaking Changes

- **`LeaseObserver` redesign — flat parameters replaced by event structs.** The five
  flat-parameter methods are replaced by six event-struct methods. Implementers must
  convert their method signatures:

  ```go
  // Before (v0.3)
  OnAcquire(ctx context.Context, workID string, token Token, err error)
  OnCheckpoint(ctx context.Context, token Token, size int, err error)
  OnRenew(ctx context.Context, token Token, err error)
  OnRelease(ctx context.Context, token Token, err error)
  OnFenced(ctx context.Context, token Token)

  // After (v0.4)
  OnAcquire(ctx context.Context, e AcquireEvent)
  OnCheckpoint(ctx context.Context, e CheckpointEvent)
  OnRenew(ctx context.Context, e RenewEvent)
  OnRelease(ctx context.Context, e ReleaseEvent)
  OnReadCheckpoint(ctx context.Context, e ReadCheckpointEvent) // new method
  OnFenced(ctx context.Context, e FencedEvent)
  ```

  The previous fields are now event-struct fields (e.g. `e.WorkID`, `e.Token`, `e.Size`,
  `e.Err`). Three behavioral changes accompany the redesign:
  - `OnReadCheckpoint` is new — it fires after every `ReadCheckpoint` attempt.
  - `OnFenced` now also fires on the `Release` path; previously it fired only after
    Checkpoint and Renew. `FencedEvent.Operation` identifies which operation triggered it.
  - Every operation event carries a `Duration` measuring the final backend call only —
    not the wait loop. For `Acquire` with `WithWaitForLease`, record your own timestamp if
    you need the cumulative wait duration.

  Callers who pass `nil` for `Config.Observer` are unaffected — the no-op substitution is
  unchanged.

- **`pool.New` returns distinct config sentinels.** The single `pool.ErrConfigInvalid`
  (whose message also changed to `"pool: invalid configuration"`) is replaced at the call
  site by `ErrNilLease`, `ErrEmptyWorkIDs`, and `ErrWithWaitForLeaseProhibited`. All three
  satisfy `errors.Is(err, pool.ErrConfigInvalid)`, so switch any exact-string or `==`
  checks to `errors.Is`.

- **`pool.Pool.Run` returns `ErrAllSlotsDead`.** When every slot exits via a
  `PermanentError`, `Run` now returns `pool.ErrAllSlotsDead` instead of `nil`. Callers that
  treated a `nil` return as "pool drained cleanly" should distinguish the two: `nil` means
  the context was cancelled; `ErrAllSlotsDead` means all slots died permanently.

### New in v0.4.0

- `pool.Permanent(err error) error` — wraps an error so it satisfies `PermanentError`
  without defining a custom type.
- `pool.Observer` — slot lifecycle callbacks (`OnSlotAcquired`, `OnSlotLost`,
  `OnSlotBackoff`, `OnSlotDead`), injected via `pool.Config.Observer`; nil is a no-op.
- `leader.Config` callbacks — `OnElected`, `OnLost`, `OnRelinquished`; all optional, nil is
  a no-op.
- `backend/conformance` — `RunSuite` enforces memory-vs-Postgres parity; relevant only if
  you implement a custom `backend.Backend`.
- `examples/observability` — a stdlib-only `LeaseObserver` reference implementation.

## v0.1.x → v0.2.0

No breaking changes. All existing code compiles without modification.

### New in v0.2.0

- `worker.Runner` — optional lifecycle manager; no changes to the `Lease` interface required
- `checkpoint` subpackage — optional encoding helpers; no changes to `Checkpoint`/`ReadCheckpoint` signatures
- `LeaseObserver` — wired via `Config.Observer`; zero value (nil) is a silent no-op
- `memory.WithClock` — injectable clock for the in-memory backend; `memory.New()` with no args is unchanged

## v0.2.x → v0.3.0

### Breaking Changes

- **`checkpoint.Codec` method rename** — `Encode` → `Marshal`, `Decode` → `Unmarshal`.
  Callers who implement `Codec` directly must rename their method implementations. Callers
  using only `checkpoint.JSON()` are unaffected — `JSONCodec` is updated. The package-level
  generic helpers `Encode[T]` and `Decode[T]` are unchanged.

### New in v0.3.0

- `worklease.HasWaitForLease` — reports whether a `[]AcquireOption` slice includes `WithWaitForLease`
- `leader` package — `leader.Elect` runs a function under managed lease acquisition and renewal
- `pool` package — `pool.Pool` distributes a fixed set of work IDs across competing processes
- `leader.Config.BackoffInterval` — optional duration `Elect` sleeps before returning on
  non-fencing paths; set this when wrapping `Elect` in a retry loop to prevent rapid
  acquire/release/reacquire cycling

### Behavioral Changes in v0.3.0

- **`Release` expires the lease immediately.** Previously, a cleanly-released lease remained
  inaccessible to successors until the full TTL elapsed. Now `Release` sets `expires_at` to
  one millisecond in the past, allowing an immediate successor `Acquire`. Callers with retry
  loops that call `Acquire` immediately after `Release` may now see the lease acquired before
  the retry loop fires — this is the intended behavior. Callers who depended on the TTL gap
  as an incidental rate limiter should add an explicit backoff (see `leader.Config.BackoffInterval`).
