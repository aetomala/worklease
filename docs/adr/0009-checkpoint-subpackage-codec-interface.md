# ADR-0009: checkpoint subpackage with Codec interface and generic helpers

**Status:** Accepted
**Date:** 2026-06-09

## Context

`Lease.Checkpoint` and `Lease.ReadCheckpoint` operate on `[]byte` (ADR-0003). Every caller
must marshal their progress type to bytes before calling `Checkpoint` and unmarshal after
calling `ReadCheckpoint`. Both existing examples repeat the same pattern: `json.Marshal` before
each checkpoint write and `json.Unmarshal` + nil guard after each read. There is no provided
abstraction, so each caller reimplements the pattern and a serialization format change requires
updating every call site.

ADR-0003 intentionally leaves serialization to the caller. This ADR adds an _optional_
`checkpoint` subpackage that standardises the pattern without modifying any existing interface.

## Decision

A `checkpoint` subpackage exports:

- A `Codec` interface with two methods: `Encode(v any) ([]byte, error)` and
  `Decode(data []byte, v any) error`.
- A `JSONCodec` concrete type (constructed via `checkpoint.JSON()`) implementing `Codec`
  using `encoding/json`.
- Two generic top-level helper functions: `Encode[T](codec Codec, v T) ([]byte, error)` and
  `Decode[T](codec Codec, data []byte) (T, error)`.

`Decode[T]` returns the zero value of T when `data` is nil, without error.

## Rationale

### 1. Optional subpackage, not a Lease change

ADR-0003 holds — the core `Lease` interface stays `[]byte`. The `checkpoint` subpackage is an
opt-in convenience layer. Callers using protobuf, msgpack, or hand-rolled binary pass through
the `Codec` interface or bypass the subpackage entirely; neither the library nor the backend
interface is touched.

### 2. Codec interface over format-specific helpers

`checkpoint.Encode[T](codec, v)` rather than `checkpoint.EncodeJSON[T](v)`. The codec is
provided at construction time, not baked into the helper name. Consequence: a call site does
not change when a caller swaps serialization format — only the codec argument changes. Callers
who implement `Codec` for protobuf or msgpack receive the generic helpers for free without any
additional library support.

### 3. Generic functions, not interface methods

Go does not permit type parameters on interface methods — `Codec.Encode[T]` is a compile
error. The generic helpers are top-level functions that accept a `Codec` and delegate to its
two unparameterised methods. This constraint is recorded explicitly so future maintainers do
not attempt to move the generics onto the interface.

### 4. nil-bytes → zero value, not error

`Decode[T]` checks for nil _before_ delegating to the codec. A first-time acquirer whose
`ReadCheckpoint` returns nil receives a usable zero struct regardless of which codec is in
use. The nil-safe guarantee is therefore independent of any specific `Codec` implementation —
a custom codec that panics or errors on nil is still safe to use through the generic helper.

## Consequences

**Positive:**
- Callers eliminate repeated `json.Marshal` / `json.Unmarshal` + nil guard boilerplate.
- Swapping serialization format is a one-line change at construction time.
- The nil → zero value contract is enforced in one place rather than at every call site.
- No dependency is added to the core library — `checkpoint` depends only on `encoding/json`.

**Negative:**
- The `Codec` interface adds a thin delegation layer; callers who use `encoding/json` directly
  can already achieve the same result without the subpackage.

## References

- `checkpoint/codec.go` — `Codec` interface, `JSONCodec`, `Encode[T]`, `Decode[T]`
- `docs/adr/0003-checkpoint-serialization-raw-bytes.md` — original decision to keep `[]byte`
- `examples/subscription-cancellation/main.go` — example of repeated marshal/unmarshal pattern
- `examples/cross-tenant-migration/main.go` — example of repeated marshal/unmarshal pattern
