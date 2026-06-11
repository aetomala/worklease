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
