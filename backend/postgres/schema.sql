CREATE TABLE IF NOT EXISTS worklease_leases (
    work_id         TEXT PRIMARY KEY,
    holder_id       TEXT        NOT NULL,
    fencing_token   BIGINT      NOT NULL DEFAULT 1,
    expires_at      TIMESTAMPTZ NOT NULL,
    checkpoint      BYTEA,
    clean_handoff   BOOLEAN     NOT NULL DEFAULT FALSE,
    acquired_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
