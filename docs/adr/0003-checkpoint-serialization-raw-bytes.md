# ADR-0003: Checkpoint serialization is raw []byte — caller owns the format

**Status:** Accepted
**Date:** 2026-06-05

## Context

`Checkpoint` writes progress state atomically with lease renewal. `ReadCheckpoint` returns that
state to the next owner. The library must decide what type the state parameter is and whether it
owns serialization.

Options considered:

1. `[]byte` — library stores a blob, caller serializes.
2. `any` — library serializes via `encoding/json` or another codec.
3. `proto.Message` — library serializes via protobuf.
4. Generic `Checkpoint[T any]` — library serializes via a codec interface.

## Decision

Checkpoint state is `[]byte`. The library stores it, hands it back, and makes no assumptions
about its contents.

```go
// Checkpoint atomically writes state and extends the lease TTL.
Checkpoint(ctx context.Context, token Token, state []byte) error

// ReadCheckpoint returns the last checkpointed state from the previous owner.
ReadCheckpoint(ctx context.Context, token Token) (state []byte, cleanHandoff bool, err error)
```

## Rationale

**Serialization is not part of coordination semantics.** The library's job is to store the blob
atomically with lease renewal and hand it to the next owner. How the blob is encoded is the
caller's decision — JSON, protobuf, msgpack, a single int64, an encoded struct. The library does
not need to know.

**Picking a format creates unwanted opinions.** If the library encoded via `encoding/json`,
callers with protobuf-native state would pay a JSON round-trip for no reason. If the library used
protobuf, it would pull in the protobuf dependency for every caller regardless of their own
serialization choice.

**Schema evolution belongs to the caller.** JSON versioning fields, protobuf reserved tags,
forward-compatible encoding — these are application-level concerns. The library owns none of
them.

**`[]byte` composes with everything.** A caller using JSON marshals to `[]byte` and passes it.
A caller using protobuf calls `proto.Marshal` and passes the result. A caller whose checkpoint
is a single uint64 encodes it with `binary.BigEndian.PutUint64`. No intermediate layer.

**The PostgreSQL backend stores `BYTEA`.** The column type is a natural fit for arbitrary bytes.
No additional transformation is required between the library and the storage layer.

## Consequences

**Positive:**
- No serialization dependency in `worklease`.
- Callers retain full control over checkpoint encoding, versioning, and schema evolution.
- Works with any encoding — JSON, protobuf, msgpack, raw binary.
- `nil` state on first acquisition is unambiguous: no prior checkpoint exists.

**Negative:**
- Callers own the full serialization/deserialization path. This is standard Go practice for
  storage interfaces (e.g., `http.ResponseWriter` writes bytes, `database/sql` Scan takes
  pointers to any) but may surprise callers expecting an opinionated codec.

## References

- `lease.go` — `Lease.Checkpoint` and `Lease.ReadCheckpoint` method signatures
- `backend/postgres/schema.sql` — `checkpoint BYTEA` column
- `backend/backend.go` — `Backend.Checkpoint` and `Backend.ReadCheckpoint` signatures
