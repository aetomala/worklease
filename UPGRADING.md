# Upgrading worklease

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
