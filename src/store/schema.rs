pub(super) const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS memory_scopes (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('user','project','thread','task')),
    parent_id TEXT REFERENCES memory_scopes(id),
    name TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_streams (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL REFERENCES memory_scopes(id),
    observed_through_sequence INTEGER NOT NULL DEFAULT 0,
    next_sequence INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_events (
    id TEXT PRIMARY KEY,
    stream_id TEXT NOT NULL REFERENCES memory_streams(id),
    sequence INTEGER NOT NULL,
    scope_id TEXT NOT NULL REFERENCES memory_scopes(id),
    kind TEXT NOT NULL,
    actor_id TEXT,
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    content_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    token_count INTEGER NOT NULL CHECK (token_count >= 0),
    sensitivity TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    UNIQUE(stream_id, sequence)
);

CREATE TABLE IF NOT EXISTS observation_runs (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL REFERENCES memory_scopes(id),
    stream_id TEXT NOT NULL REFERENCES memory_streams(id),
    cursor_at_plan INTEGER NOT NULL DEFAULT 0,
    from_sequence INTEGER NOT NULL,
    to_sequence INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','committed','failed','stale')),
    source_integrity TEXT NOT NULL DEFAULT 'intact' CHECK (source_integrity IN ('intact','privacy-purged')),
    observer_model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    ambiguities_json TEXT NOT NULL DEFAULT '[]',
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS one_committed_run_per_cursor
ON observation_runs(stream_id, from_sequence) WHERE status = 'committed';

CREATE TABLE IF NOT EXISTS observations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES observation_runs(id) ON DELETE CASCADE,
    scope_id TEXT NOT NULL REFERENCES memory_scopes(id),
    kind TEXT NOT NULL,
    content TEXT NOT NULL,
    importance REAL NOT NULL CHECK (importance >= 0 AND importance <= 1),
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    event_time_from TEXT,
    event_time_to TEXT,
    source_start_sequence INTEGER NOT NULL,
    source_end_sequence INTEGER NOT NULL,
    observer_model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS observation_sources (
    observation_id TEXT NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES memory_events(id) ON DELETE CASCADE,
    PRIMARY KEY(observation_id, event_id)
);

CREATE TABLE IF NOT EXISTS claims (
    id TEXT PRIMARY KEY,
    origin_run_id TEXT REFERENCES observation_runs(id) ON DELETE SET NULL,
    scope_id TEXT NOT NULL REFERENCES memory_scopes(id),
    kind TEXT NOT NULL,
    subject TEXT NOT NULL,
    predicate TEXT NOT NULL,
    value_json TEXT NOT NULL,
    modality TEXT NOT NULL,
    status TEXT NOT NULL,
    authority TEXT NOT NULL,
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    supersedes_id TEXT REFERENCES claims(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS claims_logical_key
ON claims(scope_id, kind, subject, predicate, status);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_claim_per_logical_key
ON claims(scope_id, kind, subject, predicate) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS claim_sources (
    claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES memory_events(id) ON DELETE CASCADE,
    PRIMARY KEY(claim_id, event_id)
);

CREATE TABLE IF NOT EXISTS memory_views (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL REFERENCES memory_scopes(id),
    kind TEXT NOT NULL,
    generation INTEGER NOT NULL,
    content TEXT NOT NULL,
    source_from_sequence INTEGER NOT NULL,
    source_through_sequence INTEGER NOT NULL,
    previous_view_id TEXT REFERENCES memory_views(id) ON DELETE SET NULL,
    model TEXT,
    prompt_version TEXT,
    token_count INTEGER NOT NULL CHECK (token_count >= 0),
    created_at TEXT NOT NULL,
    UNIQUE(scope_id, kind, generation)
);

CREATE TABLE IF NOT EXISTS view_sources (
    view_id TEXT NOT NULL REFERENCES memory_views(id) ON DELETE CASCADE,
    observation_id TEXT NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
    PRIMARY KEY(view_id, observation_id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
    record_type UNINDEXED,
    record_id UNINDEXED,
    scope_id UNINDEXED,
    text
);

CREATE TABLE IF NOT EXISTS memory_operations (
    idempotency_key TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    result_json TEXT
);
"#;
