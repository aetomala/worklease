CREATE SEQUENCE IF NOT EXISTS worklease_fencing_seq;

CREATE TABLE IF NOT EXISTS worklease_leases (
    work_id         TEXT PRIMARY KEY,
    holder_id       TEXT        NOT NULL,
    fencing_token   BIGINT      NOT NULL DEFAULT nextval('worklease_fencing_seq'),
    expires_at      TIMESTAMPTZ NOT NULL,
    checkpoint      BYTEA,
    clean_handoff   BOOLEAN     NOT NULL DEFAULT FALSE,
    acquired_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worklease_leases_updated_at
    ON worklease_leases (updated_at);
