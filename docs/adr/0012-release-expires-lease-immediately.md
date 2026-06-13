# ADR-0012: Release expires the lease immediately

**Status:** Accepted
**Date:** 2026-06-13

## Context

The original `Release` implementation set `clean_handoff = TRUE` without modifying `expires_at`.
The `Acquire` condition is `expires_at < NOW()`, so a cleanly-released lease remained
inaccessible to successors until the full TTL elapsed — the same wait required after a crash
where no `Release` was called.

PR #32 (integration tests) made this observable: two specs had to advance a fake clock past the
full TTL even though the previous holder called `Release` cleanly. The `cleanHandoff` flag was
informational for the successor but provided no timing benefit.

## Decision

`Release` sets `expires_at` to a past timestamp (one millisecond before `NOW()`) in addition to
setting `clean_handoff = TRUE`. This makes the record immediately satisfy the `Acquire`
condition, so a successor can acquire without waiting for the original TTL.

Memory backend:
```go
r.cleanHandoff = true
r.expiresAt = mb.clock.Now().Add(-time.Millisecond)
```

PostgreSQL backend:
```sql
SET clean_handoff = TRUE,
    expires_at    = NOW() - INTERVAL '1 millisecond',
    updated_at    = NOW()
```

## Rationale

The TTL's purpose is crash detection: when a holder disappears without calling `Release`, the
TTL is the only mechanism the system has to make the work item available again. For a holder that
calls `Release` explicitly, the TTL serves no purpose — the holder has already declared it is
done. Requiring the successor to wait for the TTL in the clean-handoff case is an accidental
penalty, not an intentional design property.

Setting `expires_at` to a past timestamp is a one-line addition in each backend. It does not
change the API, the fencing token semantics, or any other behavior. The `Acquire` logic is
unchanged — it already handles any record where `expires_at < NOW()`.

One millisecond in the past is sufficient to satisfy `expires_at < NOW()` on any subsequent
call. A larger value would work equally well; one millisecond is the smallest unambiguous past
offset.

## Consequences

**Positive:**
- Clean handoffs no longer penalize successors with the full TTL wait.
- `leader.Elect` and `pool.Pool` support fast leadership transfer after a controlled shutdown.
- Integration tests for clean-handoff scenarios no longer need fake clock setup.
- The TTL can be sized purely for crash detection, not for handoff latency.

**Negative:**
- None identified. The behavior change is backward-compatible: no API changes, no sentinel
  changes, and no caller relied on a lease being "held" after `Release` returned.

## References

- `backend/memory/memory.go` — `Release` implementation
- `backend/postgres/postgres.go` — `queryRelease` SQL
- `lease.go` — `Lease.Release` interface comment
- `docs/adr/0008-clock-interface-memory-backend.md` — fake clock pattern used in tests
- PR #32 — integration tests that first revealed the gap
- Issue #33 — bug report with observable evidence
