# ADR-0017: Schema Migration Remains Caller-Owned

**Status:** Accepted

## Context

v0.5 added `worklease_fencing_seq` to `schema.sql`. Because `postgres.New` does not migrate, the change required a hand-authored `UPGRADING.md` section (sequence creation, `setval` seeding, `ALTER COLUMN DEFAULT`, index) and was caught missing at release-audit time as a Blocker (issue #53). This raised the question of whether `worklease` should ship a `Migrate(db)` helper, embedded migration files, or a schema-version table to narrow caller responsibility.

## Decision

Caller-owns-schema remains the model. No `Migrate` helper, no embedded migration files, no schema-version table. Instead, every `schema.sql` change is required to ship with:

1. A migration section in `UPGRADING.md` — idempotent SQL that brings an existing database from the prior schema to the new one, including any data-safety step needed to avoid corrupting live data (e.g. seeding a new sequence from existing column values before it's relied upon)
2. A `CHANGELOG` entry under a `### Breaking` heading naming the schema change and the runtime failure mode it causes if skipped
3. Confirmation that every place the schema is duplicated — the canonical `schema.sql`, the README quickstart DDL, any schema block in reference docs, and the test-suite bootstrap — has been updated to match, since a stale copy in any of these breaks new users or integration tests even when the canonical file is correct

These three requirements are now a mandatory gate on any future schema-affecting change, not a best-effort step — the same gap that let this slip through in v0.5 must not be able to recur silently.

## Consequences

- No new library surface area, no new failure modes from a migration subsystem
- The underlying risk (a schema change reaching users without a corresponding migration path) is not eliminated by tooling — it's bounded by making the migration documentation and schema-copy sync mandatory, checked deliverables rather than something that can be forgotten
- `Forget` and `Vacuum.Sweep` (ADR-0016 retention component) ship as library methods operating against the existing schema — they require no new schema beyond what v0.5 already added (`worklease_fencing_seq`, `idx_worklease_leases_updated_at` are both already live)

## Alternatives Considered

`Migrate(db)` helper / embedded migration files / schema-version table — rejected for v0.6. Revisit only if a future schema change proves the documentation-and-sync requirement insufficient in practice (i.e. a schema change reaches users broken despite following the process).
