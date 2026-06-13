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
- **Silent behavioral change to a documented guarantee.** "Release marks intent, not immediate
  transfer" was stated in ARCHITECTURE.md and the public documentation. A caller that relied on
  this — for example, calling `Release` as an early informational signal while continuing
  post-release cleanup under the assumption that no successor could acquire until TTL — now has
  a race it did not have before. No compiler error or sentinel surfaces this; the failure mode
  is silent. This is acceptable because the library is pre-1.0 and the old behavior was never a
  deliberate safety property, but characterizing the change as zero-impact is incorrect.

- **`expires_at` alone is no longer sufficient for ad-hoc "is this item held?" queries.**
  Before this change, `expires_at > NOW()` reliably identified a live, held lease. After this
  change, a row with `expires_at < NOW()` is ambiguous: it may mean a clean release just
  happened, a crash occurred and the TTL elapsed, or the row is stale. The `clean_handoff`
  column disambiguates intent, but only if the reader checks it. Operational queries against
  `worklease_leases` — dashboards, runbooks, incident investigation queries — that relied on
  `expires_at` as a proxy for "currently held" will silently become incorrect for the
  clean-release case. Any operational runbook for the PostgreSQL backend should be updated to
  note that "currently held" requires `expires_at > NOW()`, not the absence of
  `expires_at < NOW()`. (Note: `expires_at > NOW()` for "is it actively held right now"
  continues to work correctly.)

- **Removes an incidental rate limiter on `leader.Elect` retry loops.** `pool.Pool` is
  unaffected — `Config.BackoffInterval` already governs reacquisition delay after any
  non-permanent error. `leader.Elect` has no built-in backoff: a caller that wraps `Elect` in
  its own retry loop with a fast-failing `fn` will now see rapid acquire/release/reacquire
  cycling across competing processes (leader flapping) that the old TTL-bound delay incidentally
  throttled. Callers building retry loops around `leader.Elect` are responsible for their own
  backoff; this responsibility must be documented in `leader.Elect`'s godoc.

- **The 1ms offset is an implicit cross-backend clock-precision contract.** The contract
  is that `expires_at` written by `Release` must satisfy `expires_at < NOW()` on any subsequent
  `Acquire` call. For the PostgreSQL backend, `NOW()` is server-side with microsecond precision
  and is consistent within a transaction — the 1ms offset is safe. For the in-memory backend,
  both `Release` and `Acquire` read the same `Clock` instance; a fake clock that advances in
  integer-second ticks will nonetheless treat `now - 1ms < now` correctly because time
  comparisons use `time.Time` precision regardless of how the clock is advanced. This assumption
  holds for all known clock implementations but is not stress-tested against clocks with
  millisecond or coarser resolution. A future backend with a coarser clock must document this
  requirement explicitly.

## References

- `backend/memory/memory.go` — `Release` implementation
- `backend/postgres/postgres.go` — `queryRelease` SQL
- `lease.go` — `Lease.Release` interface comment
- `docs/adr/0008-clock-interface-memory-backend.md` — fake clock pattern used in tests
- PR #32 — integration tests that first revealed the gap
- Issue #33 — bug report with observable evidence
