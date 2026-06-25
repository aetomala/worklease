# ADR-0016: Fencing tokens come from a single global monotonic sequence

**Status:** Accepted (fencing sequence). Retention component (`Forget` / `Vacuum.Sweep`) Proposed — deferred to v0.6.
**Date:** 2026-06-25

## Context

Fencing tokens are the core safety primitive of worklease: a holder stamps its
writes with a token, and a backend rejects any operation whose token is not the
current one. For this to be sound, tokens must be strictly monotonic — a later
acquisition must always produce a higher token than any earlier one.

Through v0.4 both backends derived the token **per row**: a fresh acquire used `1`
and a reacquire used `previous_token + 1`. In Postgres the column defaulted to
`1` and the `ON CONFLICT` update computed `worklease_leases.fencing_token + 1`; the
memory backend mirrored this with `r.fencingToken + 1`. Acquire also ran as two
statements — an `INSERT … ON CONFLICT` via `ExecContext`, then a separate
`SELECT` to read the token back (R8/F4: a read-back race window between the two).

Per-row derivation is fragile. If a row is ever deleted and recreated (the planned
`Forget` / retention work would do exactly this), its token resets to `1`, and a
late writer holding an old, higher token from a previous incarnation of that work
ID could be wrongly accepted — a split-brain admission. Per-row counters are
monotonic only as long as the row is never removed, which is a constraint the
retention roadmap is about to violate.

## Decision

Fencing tokens come from a single global monotonic source per backend instance,
independent of any individual row:

- **Postgres:** a database `worklease_fencing_seq` `SEQUENCE`. Every acquire —
  both the `INSERT` and the `ON CONFLICT DO UPDATE` path — sources its token from
  `nextval('worklease_fencing_seq')`. `Acquire` becomes a single
  `INSERT … ON CONFLICT … RETURNING fencing_token, expires_at` statement, removing
  the separate read-back query and its race (R8/F4). Zero rows returned
  (`sql.ErrNoRows`) maps to `ErrLeaseHeld`.
- **Memory:** a per-instance `atomic.Uint64` advanced by `seq.Add(1)` on every
  successful acquire. It is per `memory.New()` instance — two instances have
  independent counters — mirroring one Postgres sequence per database, not a
  process-global counter. It is never reset for the life of the instance.

Tokens are therefore strictly increasing across **all** work IDs on a backend, not
just within a single row's history, and survive row deletion. The conformance
suite (ADR-0015) gains a spec asserting globally increasing tokens across distinct
work IDs, enforced on both backends.

This decision also amends ADR-0005: the `WithWaitForLease` wait loop now returns an
error wrapping `ctx.Err()` on cancellation/deadline instead of bare `ErrLeaseHeld`,
so a cancelled wait is distinguishable from a held lease.

The **retention** component originally scoped under this ADR — `Forget` and
`Vacuum.Sweep` for reclaiming terminal rows — remains **Proposed and deferred to
v0.6**. The global sequence is the prerequisite that makes row deletion safe; the
deletion APIs themselves are not part of v0.5.

## Rationale

**A global sequence is monotonic by construction, even across deletion.** Because
the token source is decoupled from the row, deleting and recreating a row cannot
reset or rewind it. This is precisely the property the retention work needs and the
property per-row counters cannot provide.

**`RETURNING` collapses acquire to one atomic statement.** Sourcing the token and
reading it back in a single statement eliminates the read-back race (R8/F4) and one
network round-trip per acquire.

**Per-instance (not process-global) memory counter matches the Postgres model.**
One sequence per database corresponds to one counter per backend instance. Making
the memory counter a struct field rather than a package global keeps independent
backends genuinely independent, which the conformance factory relies on.

**Consuming sequence values on the conflict path is acceptable.** A contended
acquire that loses the `WHERE expires_at < NOW()` race still calls `nextval` and
discards the value, so the sequence has gaps. Gaps are harmless — only monotonicity
matters, not density.

## Consequences

**Positive:**
- Tokens are globally monotonic and survive row deletion, unblocking safe retention
  in v0.6.
- `Acquire` is a single statement on Postgres — the read-back race (R8/F4) is gone.
- Conformance enforces global monotonicity on every backend.

**Negative:**
- The Postgres sequence is non-transactional: a rolled-back or contended acquire
  still consumes a value, leaving gaps. This is expected and harmless but can
  surprise an operator reading raw token values.
- The memory counter's per-instance semantics must be understood: tokens are not
  comparable across two independent `memory.New()` instances. In production a single
  process shares one backend instance, so this matches the Postgres single-sequence
  model.
- Schema gains a sequence and an `updated_at` index; `schema.sql` and the test
  bootstrap must create the sequence before the table's column default can resolve.

## References

- `backend/postgres/postgres.go` — `queryAcquire` (`nextval` + `RETURNING`), single-statement `Acquire`
- `backend/postgres/schema.sql` — `worklease_fencing_seq`, column default, `updated_at` index
- `backend/memory/memory.go` — per-instance `seq atomic.Uint64`
- `backend/conformance/conformance.go` — global-monotonicity spec across distinct work IDs
- `docs/adr/0005-acquire-default-returns-err-lease-held.md` — the ADR this amends (ctx.Err() propagation)
- `UPGRADING.md` — v0.4.x → v0.5.0 acquire breaking change
