CREATE TABLE IF NOT EXISTS __RRJJ_SESSIONS_TABLE__ (
    session_id TEXT PRIMARY KEY,
    format JSONB NOT NULL,
    manifest JSONB NOT NULL,
    durable_seq INT8 NOT NULL CHECK (durable_seq >= 0),
    durable_op TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS __RRJJ_EVENTS_TABLE__ (
    session_id TEXT NOT NULL,
    seq INT8 NOT NULL CHECK (seq >= 0),
    timestamp TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    event JSONB NOT NULL,
    PRIMARY KEY (session_id, seq)
);

CREATE INDEX IF NOT EXISTS __RRJJ_EVENTS_TIMESTAMP_INDEX__
    ON __RRJJ_EVENTS_TABLE__ (session_id, timestamp);

CREATE TABLE IF NOT EXISTS __RRJJ_OBJECTS_TABLE__ (
    session_id TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size INT8 NOT NULL CHECK (size >= 0),
    inline_bytes BYTEA,
    storage JSONB,
    PRIMARY KEY (session_id, path),
    CHECK ((inline_bytes IS NOT NULL) <> (storage IS NOT NULL))
);
