# ADR-0002: Token fields are unexported, exposed via accessor methods and fmt.Stringer

**Status:** Accepted
**Date:** 2026-06-05

## Context

`Acquire` returns a `Token` that is passed to all subsequent operations — `Checkpoint`, `Renew`,
`Release`, `ReadCheckpoint`, and `StartRenewal`. The token carries the fencing token value,
work ID, holder ID, and expiry time.

Three design options were considered:

1. Exported struct fields — callers can read and construct tokens freely.
2. Unexported fields with accessor methods — callers can read but cannot construct or mutate.
3. Opaque interface — callers can pass but cannot inspect at all.

## Decision

`Token` is a struct with unexported fields. All fields are exposed via accessor methods. `Token`
implements `fmt.Stringer`.

```go
type Token struct {
    workID       string
    holderID     string
    fencingToken uint64
    expiresAt    time.Time
}

func (t Token) WorkID() string       { return t.workID }
func (t Token) HolderID() string     { return t.holderID }
func (t Token) FencingToken() uint64 { return t.fencingToken }
func (t Token) ExpiresAt() time.Time { return t.expiresAt }
func (t Token) String() string       { /* formatted: workID/holderID fencingToken exp:expiresAt */ }
```

## Rationale

**Callers must not construct tokens.** If `Token` had exported fields, a caller could
construct `Token{FencingToken: 999}` and pass it to `Checkpoint` to bypass fencing. The library
owns fencing logic entirely — tokens must only be issued by `Acquire`.

**Callers must not mutate tokens.** A caller that increments its own `fencingToken` could
attempt writes that should be rejected. Unexported fields prevent this class of bug.

**Observability requires inspection.** Logs, traces, and metrics need the work ID, holder ID,
and fencing token. An opaque interface would force callers to maintain a parallel map from token
to metadata — unnecessary complexity. Accessors expose exactly what observability needs.

**`fmt.Stringer` eliminates formatting boilerplate.** `log.Printf("acquired %v", token)` works
without any caller-side format string. Structured loggers can call `token.String()` or use the
individual accessors — both paths are open.

## Consequences

**Positive:**
- Callers cannot forge or mutate tokens — fencing is unconditionally library-enforced.
- All token fields are available for logs, traces, and metrics via accessor methods.
- `log.Printf("acquired %v", token)` works out of the box.
- Token comparison is safe via `==` (value type, all fields comparable).

**Negative:**
- Tests that need to construct tokens for table-driven cases cannot do so directly. Tests must
  go through `Acquire` to obtain a real token, or the library must expose a test-only
  constructor (noted as a v0.2 concern).

## References

- `lease.go` — `Token` type definition and accessor methods
- `worklease.go` — `Acquire` implementation — the only place tokens are issued
