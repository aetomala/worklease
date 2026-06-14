# ADR-0009: checkpoint subpackage with Codec interface and generic helpers

**Status:** Accepted (amended in v0.3 — see below)
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
  *Note: v0.2 shipped with these names. See **v0.3 Amendment** below — v0.3 renames these
  methods to `Marshal` and `Unmarshal` as a breaking change.*
- A `JSONCodec` concrete type (constructed via `checkpoint.JSON()`) implementing `Codec`
  using `encoding/json`.
- Two generic top-level helper functions: `Encode[T](codec Codec, v T) ([]byte, error)` and
  `Decode[T](codec Codec, data []byte) (T, error)`.  
  *These names are unchanged in v0.3 — they are generic wrappers at a different abstraction
  level than the interface methods.*

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

---

## v0.3 Amendment — Codec method rename: Encode/Decode → Marshal/Unmarshal

**Effective:** v0.3 (breaking change)

### Decision

The `Codec` interface method names are changed from `Encode`/`Decode` to `Marshal`/`Unmarshal`:

```go
// v0.2 (shipped)
type Codec interface {
    Encode(v any) ([]byte, error)
    Decode(data []byte, v any) error
}

// v0.3 (breaking rename)
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
}
```

The generic helpers retain their names — `Encode[T]` and `Decode[T]` remain unchanged.
They operate at a different abstraction level: they are type-safe, nil-safe wrappers around
the codec, not raw byte operations.

Callers who implemented `Codec` in v0.2 must rename their method implementations. The `UPGRADING.md`
document in the repository root covers this migration.

### Rationale

The v0.2 method names — `Encode`/`Decode` — conflict with the established Go convention for
value-to-bytes interfaces. In the standard library, operations that produce or consume a byte
representation of a value are named `Marshal`/`Unmarshal`:

- `encoding.BinaryMarshaler` — `MarshalBinary() ([]byte, error)`
- `encoding.TextMarshaler` — `MarshalText() ([]byte, error)`
- `encoding/json.Marshaler` — `MarshalJSON() ([]byte, error)`
- `encoding/xml.Marshaler` — `MarshalXML(...) error`

`Encode`/`Decode` in Go convention denotes stream-oriented, stateful operations:
`encoding/json.Encoder.Encode` writes to an `io.Writer`; `encoding/gob.Encoder.Encode` does
the same. The `Codec` interface is value-to-bytes — it takes a value in, returns bytes out,
with no stream or cursor state. `Marshal`/`Unmarshal` correctly describes this contract.

Using the convention-correct names removes the ambiguity (callers cannot confuse a Codec with
a streaming encoder) and aligns the interface with the pattern a Go developer expects when
seeing an interface with a `Marshal` method.

The generic helpers are exempt from this rename because `Encode[T]` and `Decode[T]` already
carry different semantics: they are caller-facing type-safe wrappers, not raw codec operations.
The distinction is intentional — the helpers encode *for* the caller (type-erasing, nil-guarding)
while the interface methods encode *as* the codec (raw bytes, no nil handling, no type parameter).

## References

- `checkpoint/codec.go` — `Codec` interface, `JSONCodec`, `Encode[T]`, `Decode[T]`
- `UPGRADING.md` — v0.3 migration guide including `Codec` method rename
- `docs/adr/0003-checkpoint-serialization-raw-bytes.md` — original decision to keep `[]byte`
- `examples/subscription-cancellation/main.go` — example of repeated marshal/unmarshal pattern
- `examples/cross-tenant-migration/main.go` — example of repeated marshal/unmarshal pattern
