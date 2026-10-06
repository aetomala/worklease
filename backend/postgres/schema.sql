CREATE SEQUENCE IF NOT EXISTS worklease_fencing_seq;

CREATE TABLE IF NOT EXISTS worklease_leases (
    work_id         TEXT        PRIMARY KEY,
    holder_id       TEXT        NOT NULL,
    fencing_token   BIGINT      NOT NULL DEFAULT nextval('worklease_fencing_seq'),
    expires_at      TIMESTAMPTZ NOT NULL,
    checkpoint      BYTEA,
    -- Deprecated: unread and unwritten since v0.6.0 (ADR-0018). Kept so a rollback to v0.5 does not fail; dropped in a later release (#86).
    clean_handoff   BOOLEAN     NOT NULL DEFAULT FALSE,
    exit_mode       TEXT,
    prev_exit_mode  TEXT        NOT NULL DEFAULT 'none',
    prev_holder_id  TEXT,
    acquired_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT worklease_leases_exit_mode_check
        CHECK (exit_mode IS NULL OR exit_mode IN ('finished', 'abandoned', 'retired')),
    CONSTRAINT worklease_leases_prev_exit_mode_check
        CHECK (prev_exit_mode IN ('none', 'expired', 'finished', 'abandoned', 'retired'))
);

CREATE INDEX IF NOT EXISTS idx_worklease_leases_updated_at
    ON worklease_leases (updated_at);
