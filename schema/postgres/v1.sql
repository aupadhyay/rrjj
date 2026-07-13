-- rrjj Postgres/CockroachDB storage schema v1.
-- Applications may rename the three tables and index, then pass those table
-- names to rrjj. In validate mode rrjj executes no DDL.

CREATE TABLE rrjj_sessions (
    session_id TEXT PRIMARY KEY,
    format JSONB NOT NULL,
    manifest JSONB NOT NULL,
    durable_seq INT8 NOT NULL CHECK (durable_seq >= 0),
    durable_op TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE rrjj_events (
    session_id TEXT NOT NULL,
    seq INT8 NOT NULL CHECK (seq >= 0),
    timestamp TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    event JSONB NOT NULL,
    PRIMARY KEY (session_id, seq)
);

CREATE INDEX rrjj_events_session_timestamp_idx
    ON rrjj_events (session_id, timestamp);

CREATE TABLE rrjj_objects (
    session_id TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size INT8 NOT NULL CHECK (size >= 0),
    inline_bytes BYTEA,
    storage JSONB,
    PRIMARY KEY (session_id, path),
    CHECK ((inline_bytes IS NOT NULL) <> (storage IS NOT NULL))
);
