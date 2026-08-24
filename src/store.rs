use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail, ensure};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::model::*;

pub const SCHEMA_VERSION: i64 = 3;

const SCHEMA: &str = r#"
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
    idempotency_key TEXT NOT NULL UNIQUE,
    UNIQUE(stream_id, sequence)
);

CREATE TABLE IF NOT EXISTS observation_runs (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL REFERENCES memory_scopes(id),
    stream_id TEXT NOT NULL REFERENCES memory_streams(id),
    cursor_at_plan INTEGER NOT NULL DEFAULT 0,
    from_sequence INTEGER NOT NULL,
    to_sequence INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','running','committed','failed','stale')),
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
    valid_from TEXT,
    valid_to TEXT,
    expires_at TEXT,
    supersedes_id TEXT REFERENCES claims(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS claims_logical_key
ON claims(scope_id, kind, subject, predicate, status);

CREATE TABLE IF NOT EXISTS claim_sources (
    claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    observation_id TEXT REFERENCES observations(id) ON DELETE CASCADE,
    event_id TEXT REFERENCES memory_events(id) ON DELETE CASCADE,
    CHECK ((observation_id IS NULL) != (event_id IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_claim_observation_source
ON claim_sources(claim_id, observation_id) WHERE observation_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS unique_claim_event_source
ON claim_sources(claim_id, event_id) WHERE event_id IS NOT NULL;

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
    result_json TEXT NOT NULL,
    purged INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
"#;

pub struct MemoryStore {
    conn: Connection,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateView {
    pub scope_id: String,
    pub kind: ViewKind,
    pub content: String,
    pub source_from_sequence: i64,
    pub source_through_sequence: i64,
    pub source_observation_ids: Vec<String>,
    pub model: Option<String>,
    pub prompt_version: Option<String>,
    pub token_count: Option<i64>,
    pub idempotency_key: String,
}

impl MemoryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path != Path::new(":memory:")
            && let Some(parent) = path.parent()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating database directory {}", parent.display()))?;
        }
        let mut conn = Connection::open(path)
            .with_context(|| format!("opening memory database {}", path.display()))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "busy_timeout", 5_000)?;
        let schema_version: i64 =
            conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        ensure!(
            schema_version == 0 || schema_version == SCHEMA_VERSION,
            "database schema version {schema_version} is incompatible with OMK schema version {SCHEMA_VERSION}; start with a fresh database"
        );
        if schema_version == 0 {
            let has_schema_objects: bool = conn.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_schema
                    WHERE name NOT LIKE 'sqlite_%'
                )",
                [],
                |row| row.get(0),
            )?;
            ensure!(
                !has_schema_objects,
                "unversioned database is not empty; start with a fresh database"
            );
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .context("starting SQLite schema transaction")?;
            tx.execute_batch(SCHEMA).context("applying SQLite schema")?;
            tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            tx.commit()
                .context("committing SQLite schema initialization")?;
        }
        Ok(Self { conn })
    }

    pub fn create_scope(
        &mut self,
        id: &str,
        kind: ScopeKind,
        parent_id: Option<&str>,
        name: Option<&str>,
        idempotency_key: &str,
    ) -> Result<MutationResult<Scope>> {
        validate_nonempty("scope id", id)?;
        validate_nonempty("idempotency key", idempotency_key)?;
        ensure!(parent_id != Some(id), "a scope cannot be its own parent");

        let request_hash = operation_request_hash(
            "scope.create",
            &json!({"id": id, "kind": kind, "parentId": parent_id, "name": name}),
        )?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Scope>(&tx, idempotency_key, "scope.create", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        if let Some(parent) = parent_id {
            ensure_scope_exists(&tx, parent)?;
        }
        let now = now();
        tx.execute(
            "INSERT INTO memory_scopes(id,kind,parent_id,name,created_at) VALUES (?1,?2,?3,?4,?5)",
            params![id, enum_text(&kind), parent_id, name, now],
        )
        .with_context(|| format!("creating scope {id}"))?;
        let scope = Scope {
            id: id.to_owned(),
            kind,
            parent_id: parent_id.map(str::to_owned),
            name: name.map(str::to_owned),
            created_at: now,
        };
        save_operation(&tx, idempotency_key, "scope.create", &request_hash, &scope)?;
        tx.commit()?;
        Ok(MutationResult::created(scope))
    }

    pub fn list_scopes(&self) -> Result<Vec<Scope>> {
        let mut statement = self.conn.prepare(
            "SELECT id,kind,parent_id,name,created_at FROM memory_scopes ORDER BY created_at,id",
        )?;
        collect_rows(statement.query_map([], row_scope)?)
    }

    pub fn get_scope(&self, id: &str) -> Result<Scope> {
        self.conn
            .query_row(
                "SELECT id,kind,parent_id,name,created_at FROM memory_scopes WHERE id=?1",
                [id],
                row_scope,
            )
            .optional()?
            .ok_or_else(|| anyhow!("scope {id} does not exist"))
    }

    pub fn visible_scope_ids(&self, scope_id: &str) -> Result<Vec<String>> {
        visible_scope_ids(&self.conn, scope_id)
    }

    pub fn append_event(&mut self, event: NewEvent) -> Result<MutationResult<MemoryEvent>> {
        validate_nonempty("scope id", &event.scope_id)?;
        validate_nonempty("stream id", &event.stream_id)?;
        validate_nonempty("idempotency key", &event.idempotency_key)?;
        ensure!(
            event.token_count.is_none_or(|count| count >= 0),
            "token count cannot be negative"
        );
        ensure!(event.metadata.is_object(), "metadata must be a JSON object");
        if let Some(occurred_at) = &event.occurred_at {
            chrono::DateTime::parse_from_rfc3339(occurred_at)
                .context("occurred-at must be an RFC 3339 timestamp")?;
        }

        let request_hash = operation_request_hash("event.append", &event)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<MemoryEvent>(&tx, &event.idempotency_key, "event.append", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(redact_for_agent(prior)));
        }
        ensure_scope_exists(&tx, &event.scope_id)?;
        let now = now();
        tx.execute(
            "INSERT OR IGNORE INTO memory_streams(id,scope_id,created_at) VALUES (?1,?2,?3)",
            params![event.stream_id, event.scope_id, now],
        )?;
        let (stream_scope, sequence): (String, i64) = tx.query_row(
            "SELECT scope_id,next_sequence FROM memory_streams WHERE id=?1",
            [&event.stream_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            stream_scope == event.scope_id,
            "stream {} belongs to scope {}, not {}",
            event.stream_id,
            stream_scope,
            event.scope_id
        );
        tx.execute(
            "UPDATE memory_streams SET next_sequence=next_sequence+1 WHERE id=?1",
            [&event.stream_id],
        )?;
        let (content, content_hash, token_count) = if event.sensitivity == Sensitivity::DoNotStore {
            let tombstone = json!({"omitted": true, "reason": "do-not-store"});
            (
                tombstone.clone(),
                hash_json(&tombstone),
                estimate_event_tokens(&tombstone, &json!({})),
            )
        } else {
            let estimated = estimate_event_tokens(&event.content, &event.metadata);
            let count = event
                .token_count
                .map_or(estimated, |count| count.max(estimated));
            (event.content.clone(), hash_json(&event.content), count)
        };
        let stored_metadata = if event.sensitivity == Sensitivity::DoNotStore {
            json!({})
        } else {
            event.metadata
        };
        let stored = MemoryEvent {
            id: Uuid::new_v4().to_string(),
            stream_id: event.stream_id,
            sequence,
            scope_id: event.scope_id,
            kind: event.kind,
            actor_id: event.actor_id,
            occurred_at: event.occurred_at.unwrap_or_else(|| now.clone()),
            recorded_at: now,
            content,
            content_hash,
            token_count,
            sensitivity: event.sensitivity,
            metadata: stored_metadata,
        };
        tx.execute(
            "INSERT INTO memory_events(id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json,idempotency_key)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            params![
                stored.id,
                stored.stream_id,
                stored.sequence,
                stored.scope_id,
                enum_text(&stored.kind),
                stored.actor_id,
                stored.occurred_at,
                stored.recorded_at,
                stored.content.to_string(),
                stored.content_hash,
                stored.token_count,
                enum_text(&stored.sensitivity),
                stored.metadata.to_string(),
                event.idempotency_key,
            ],
        )?;
        if matches!(
            stored.sensitivity,
            Sensitivity::Normal | Sensitivity::Private
        ) {
            insert_fts(
                &tx,
                "event",
                &stored.id,
                &stored.scope_id,
                &searchable_json(&stored.content),
            )?;
        }
        save_operation(
            &tx,
            &event.idempotency_key,
            "event.append",
            &request_hash,
            &stored,
        )?;
        tx.commit()?;
        Ok(MutationResult::created(redact_for_agent(stored)))
    }

    pub fn recall_event_range(
        &self,
        stream_id: &str,
        from_sequence: i64,
        to_sequence: i64,
    ) -> Result<Vec<MemoryEvent>> {
        ensure!(from_sequence > 0, "from sequence must be positive");
        ensure!(
            to_sequence >= from_sequence,
            "to sequence must be at least from sequence"
        );
        let mut statement = self.conn.prepare(
            "SELECT id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json
             FROM memory_events WHERE stream_id=?1 AND sequence BETWEEN ?2 AND ?3 ORDER BY sequence",
        )?;
        collect_rows(
            statement.query_map(params![stream_id, from_sequence, to_sequence], row_event)?,
        )
    }

    pub fn get_event(&self, event_id: &str) -> Result<MemoryEvent> {
        self.conn
            .query_row(
                "SELECT id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json FROM memory_events WHERE id=?1",
                [event_id],
                row_event,
            )
            .optional()?
            .ok_or_else(|| anyhow!("event {event_id} does not exist"))
    }

    pub fn plan_observation(
        &mut self,
        scope_id: &str,
        stream_id: &str,
        max_tokens: i64,
        observer_model: &str,
        prompt_version: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<ObservationPlanOutcome>> {
        ensure!(max_tokens > 0, "max tokens must be positive");
        validate_nonempty("observer model", observer_model)?;
        validate_nonempty("prompt version", prompt_version)?;
        validate_nonempty("idempotency key", idempotency_key)?;
        let request_hash = operation_request_hash(
            "observation.plan",
            &json!({
                "scopeId": scope_id,
                "streamId": stream_id,
                "maxTokens": max_tokens,
                "observerModel": observer_model,
                "promptVersion": prompt_version
            }),
        )?;
        let tx = self.immediate()?;
        if let Some(prior) = prior_result::<ObservationPlanOutcome>(
            &tx,
            idempotency_key,
            "observation.plan",
            &request_hash,
        )? {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let scope = query_scope(&tx, scope_id)?;
        let (stream_scope, cursor): (String, i64) = tx
            .query_row(
                "SELECT scope_id,observed_through_sequence FROM memory_streams WHERE id=?1",
                [stream_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| anyhow!("stream {stream_id} does not exist"))?;
        ensure!(
            stream_scope == scope_id,
            "stream {stream_id} does not belong to scope {scope_id}"
        );

        let model_events: Vec<MemoryEvent> = query_events_after(&tx, stream_id, cursor)?
            .into_iter()
            .map(redact_for_agent)
            .collect();
        if let Some(first) = model_events.first() {
            ensure!(
                first.token_count <= max_tokens,
                "observation budget too small: minimumRequiredTokens={} for event {}",
                first.token_count,
                first.id
            );
        }
        let mut selected = Vec::new();
        let mut tokens = 0;
        for event in model_events {
            if !selected.is_empty() && tokens + event.token_count > max_tokens {
                break;
            }
            tokens += event.token_count;
            selected.push(event);
            if tokens >= max_tokens {
                break;
            }
        }
        if selected.is_empty() {
            let outcome = ObservationPlanOutcome::caught_up(scope_id, stream_id, cursor);
            save_operation(
                &tx,
                idempotency_key,
                "observation.plan",
                &request_hash,
                &outcome,
            )?;
            tx.commit()?;
            return Ok(MutationResult::created(outcome));
        }
        let from_sequence = selected.first().expect("not empty").sequence;
        let to_sequence = selected.last().expect("not empty").sequence;
        let run_id = Uuid::new_v4().to_string();
        let timestamp = now();
        tx.execute(
            "INSERT INTO observation_runs(id,scope_id,stream_id,cursor_at_plan,from_sequence,to_sequence,status,observer_model,prompt_version,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,'pending',?7,?8,?9,?9)",
            params![run_id, scope_id, stream_id, cursor, from_sequence, to_sequence, observer_model, prompt_version, timestamp],
        )?;
        let visible = visible_scope_ids(&tx, scope_id)?;
        let mut active_claims = query_claims_for_scopes(&tx, &visible, Some("active"))?;
        sort_claims_by_scope(&mut active_claims, &visible);
        let previous_continuation = latest_view(&tx, scope_id, "continuation")?;
        let plan = ObservationPlan {
            run_id,
            scope,
            stream_id: stream_id.to_owned(),
            from_sequence,
            to_sequence,
            events: selected,
            active_claims,
            previous_continuation,
        };
        let outcome = ObservationPlanOutcome::ready(plan);
        save_operation(
            &tx,
            idempotency_key,
            "observation.plan",
            &request_hash,
            &outcome,
        )?;
        tx.commit()?;
        Ok(MutationResult::created(outcome))
    }

    pub fn commit_observation(
        &mut self,
        run_id: &str,
        result: ObserverResult,
        idempotency_key: &str,
    ) -> Result<MutationResult<ObservationCommit>> {
        validate_nonempty("run id", run_id)?;
        validate_nonempty("idempotency key", idempotency_key)?;
        validate_observer_result(&result)?;

        let request_hash = operation_request_hash("observation.commit", &(run_id, &result))?;
        let tx = self.immediate()?;
        if let Some(prior) = prior_result::<ObservationCommit>(
            &tx,
            idempotency_key,
            "observation.commit",
            &request_hash,
        )? {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let run = query_run(&tx, run_id)?;
        ensure!(
            matches!(run.status.as_str(), "pending" | "running"),
            "observation run {run_id} is {}, not pending",
            run.status
        );
        let cursor: i64 = tx.query_row(
            "SELECT observed_through_sequence FROM memory_streams WHERE id=?1",
            [&run.stream_id],
            |row| row.get(0),
        )?;
        if cursor != run.cursor_at_plan {
            tx.execute(
                "UPDATE observation_runs SET status='stale',updated_at=?2,error='stream cursor advanced' WHERE id=?1",
                params![run_id, now()],
            )?;
            tx.commit()?;
            bail!(
                "observation run {run_id} is stale: expected cursor {}, found {cursor}",
                run.cursor_at_plan
            );
        }

        let source_events =
            query_events_range(&tx, &run.stream_id, run.from_sequence, run.to_sequence)?;
        let sources_by_id: HashMap<String, MemoryEvent> = source_events
            .into_iter()
            .map(|event| (event.id.clone(), event))
            .collect();
        validate_provenance(&result, &sources_by_id)?;
        let timestamp = now();
        tx.execute(
            "UPDATE observation_runs SET status='running',updated_at=?2 WHERE id=?1",
            params![run_id, timestamp],
        )?;

        let mut observations = Vec::with_capacity(result.observations.len());
        for draft in &result.observations {
            let mut sequences: Vec<i64> = draft
                .source_event_ids
                .iter()
                .map(|id| sources_by_id[id].sequence)
                .collect();
            sequences.sort_unstable();
            let observation = Observation {
                id: Uuid::new_v4().to_string(),
                run_id: run_id.to_owned(),
                scope_id: run.scope_id.clone(),
                kind: draft.kind.clone(),
                content: draft.content.trim().to_owned(),
                importance: draft.importance,
                confidence: draft.confidence,
                event_time_from: draft.event_time_from.clone(),
                event_time_to: draft.event_time_to.clone(),
                source_start_sequence: *sequences.first().expect("validated source"),
                source_end_sequence: *sequences.last().expect("validated source"),
                observer_model: run.observer_model.clone(),
                prompt_version: run.prompt_version.clone(),
                created_at: timestamp.clone(),
            };
            insert_observation(&tx, &observation)?;
            for event_id in &draft.source_event_ids {
                tx.execute(
                    "INSERT INTO observation_sources(observation_id,event_id) VALUES (?1,?2)",
                    params![observation.id, event_id],
                )?;
            }
            insert_fts(
                &tx,
                "observation",
                &observation.id,
                &observation.scope_id,
                &observation.content,
            )?;
            observations.push(observation);
        }

        let mut claims = Vec::with_capacity(result.claims.len());
        for draft in &result.claims {
            let claim = Claim {
                id: Uuid::new_v4().to_string(),
                scope_id: run.scope_id.clone(),
                kind: draft.kind.clone(),
                subject: draft.subject.trim().to_owned(),
                predicate: draft.predicate.trim().to_owned(),
                value: draft.value.clone(),
                modality: draft.modality.clone(),
                status: ClaimStatus::Pending,
                authority: ClaimAuthority::ModelInference,
                confidence: draft.confidence,
                valid_from: draft.valid_from.clone(),
                valid_to: draft.valid_to.clone(),
                expires_at: draft.expires_at.clone(),
                supersedes_id: None,
                created_at: timestamp.clone(),
                updated_at: timestamp.clone(),
            };
            insert_claim(&tx, &claim, Some(run_id))?;
            for event_id in &draft.source_event_ids {
                tx.execute(
                    "INSERT INTO claim_sources(claim_id,event_id) VALUES (?1,?2)",
                    params![claim.id, event_id],
                )?;
            }
            index_claim(&tx, &claim)?;
            claims.push(claim);
        }

        let preserve_continuation = observer_result_is_completely_empty(&result);
        let previous_continuation = preserve_continuation
            .then(|| latest_view(&tx, &run.scope_id, "continuation"))
            .transpose()?
            .flatten();
        let (continuation_view, continuation_action) = if let Some(previous) = previous_continuation
        {
            (previous, ContinuationAction::Preserved)
        } else {
            let continuation_content = serde_json::to_string_pretty(&result.continuation)?;
            let view = insert_next_view(
                &tx,
                &run.scope_id,
                ViewKind::Continuation,
                &continuation_content,
                run.from_sequence,
                run.to_sequence,
                Some(&run.observer_model),
                Some(&run.prompt_version),
                estimate_tokens(&continuation_content),
            )?;
            for observation in &observations {
                tx.execute(
                    "INSERT INTO view_sources(view_id,observation_id) VALUES (?1,?2)",
                    params![view.id, observation.id],
                )?;
            }
            (view, ContinuationAction::Created)
        };

        let changed = tx.execute(
            "UPDATE memory_streams SET observed_through_sequence=?2 WHERE id=?1 AND observed_through_sequence=?3",
            params![run.stream_id, run.to_sequence, run.cursor_at_plan],
        )?;
        ensure!(
            changed == 1,
            "stream cursor changed during observation commit"
        );
        let ambiguities_json = serde_json::to_string(&result.ambiguities)?;
        tx.execute(
            "UPDATE observation_runs SET status='committed',ambiguities_json=?2,updated_at=?3,error=NULL WHERE id=?1",
            params![run_id, ambiguities_json, now()],
        )?;
        let commit = ObservationCommit {
            run_id: run_id.to_owned(),
            observations,
            claims,
            continuation_view,
            continuation_action,
            ambiguities: result.ambiguities,
            next_required_action: (!result.claims.is_empty()).then(|| {
                format!(
                    "omk claim reconcile --scope {} --idempotency-key <key>",
                    run.scope_id
                )
            }),
        };
        save_operation(
            &tx,
            idempotency_key,
            "observation.commit",
            &request_hash,
            &commit,
        )?;
        tx.commit()?;
        Ok(MutationResult::created(commit))
    }

    pub fn fail_observation(
        &mut self,
        run_id: &str,
        reason: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Value>> {
        validate_nonempty("run id", run_id)?;
        validate_nonempty("failure reason", reason)?;
        validate_nonempty("idempotency key", idempotency_key)?;
        ensure!(
            reason.chars().count() <= 200,
            "failure reason must be at most 200 characters"
        );
        let request_hash = operation_request_hash("observation.fail", &(run_id, reason))?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Value>(&tx, idempotency_key, "observation.fail", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let run = query_run(&tx, run_id)?;
        ensure!(
            matches!(run.status.as_str(), "pending" | "running"),
            "observation run {run_id} is {}, not pending",
            run.status
        );
        tx.execute(
            "UPDATE observation_runs SET status='failed',error=?2,updated_at=?3 WHERE id=?1",
            params![run_id, reason, now()],
        )?;
        let result = json!({"runId": run_id, "status": "failed", "reason": reason});
        save_operation(
            &tx,
            idempotency_key,
            "observation.fail",
            &request_hash,
            &result,
        )?;
        tx.commit()?;
        Ok(MutationResult::created(result))
    }

    pub fn get_observation_run(&self, run_id: &str) -> Result<ObservationRunInfo> {
        self.conn
            .query_row(
                "SELECT id,scope_id,stream_id,cursor_at_plan,from_sequence,to_sequence,status,source_integrity,observer_model,prompt_version,ambiguities_json,error,created_at,updated_at
                 FROM observation_runs WHERE id=?1",
                [run_id],
                row_observation_run_info,
            )
            .optional()?
            .ok_or_else(|| anyhow!("observation run {run_id} does not exist"))
    }

    pub fn list_observation_runs(
        &self,
        scope_id: Option<&str>,
        stream_id: Option<&str>,
        status: Option<&str>,
    ) -> Result<Vec<ObservationRunInfo>> {
        if let Some(status) = status {
            ensure!(
                matches!(
                    status,
                    "pending" | "running" | "committed" | "failed" | "stale"
                ),
                "invalid observation run status {status}"
            );
        }
        let mut statement = self.conn.prepare(
            "SELECT id,scope_id,stream_id,cursor_at_plan,from_sequence,to_sequence,status,source_integrity,observer_model,prompt_version,ambiguities_json,error,created_at,updated_at
             FROM observation_runs
             WHERE (?1 IS NULL OR scope_id=?1)
               AND (?2 IS NULL OR stream_id=?2)
               AND (?3 IS NULL OR status=?3)
             ORDER BY created_at,id",
        )?;
        collect_rows(statement.query_map(
            params![scope_id, stream_id, status],
            row_observation_run_info,
        )?)
    }

    pub fn stream_status(&self, stream_id: &str) -> Result<StreamStatus> {
        let (scope_id, observed_through_sequence, next_sequence): (String, i64, i64) = self
            .conn
            .query_row(
                "SELECT scope_id,observed_through_sequence,next_sequence FROM memory_streams WHERE id=?1",
                [stream_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| anyhow!("stream {stream_id} does not exist"))?;
        let last_sequence = self.conn.query_row(
            "SELECT MAX(sequence) FROM memory_events WHERE stream_id=?1",
            [stream_id],
            |row| row.get(0),
        )?;
        Ok(StreamStatus {
            id: stream_id.to_owned(),
            scope_id,
            observed_through_sequence,
            next_sequence,
            last_sequence,
            runs: self.list_observation_runs(None, Some(stream_id), None)?,
        })
    }

    fn immediate(&mut self) -> Result<Transaction<'_>> {
        self.conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("starting SQLite write transaction")
    }
}

impl MemoryStore {
    #[allow(clippy::too_many_arguments)]
    pub fn remember_claim(
        &mut self,
        scope_id: &str,
        kind: ClaimKind,
        subject: &str,
        predicate: &str,
        value: Value,
        source_event_ids: &[String],
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.create_direct_claim(
            scope_id,
            kind,
            subject,
            predicate,
            value,
            ClaimModality::ExplicitAssertion,
            ClaimStatus::Active,
            ClaimAuthority::ExplicitUser,
            source_event_ids,
            idempotency_key,
            "claim.remember",
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn propose_claim(
        &mut self,
        scope_id: &str,
        kind: ClaimKind,
        subject: &str,
        predicate: &str,
        value: Value,
        source_event_ids: &[String],
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.create_direct_claim(
            scope_id,
            kind,
            subject,
            predicate,
            value,
            ClaimModality::Proposal,
            ClaimStatus::Pending,
            ClaimAuthority::ExplicitUser,
            source_event_ids,
            idempotency_key,
            "claim.propose",
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_direct_claim(
        &mut self,
        scope_id: &str,
        kind: ClaimKind,
        subject: &str,
        predicate: &str,
        value: Value,
        modality: ClaimModality,
        requested_status: ClaimStatus,
        authority: ClaimAuthority,
        source_event_ids: &[String],
        idempotency_key: &str,
        operation: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("subject", subject)?;
        validate_nonempty("predicate", predicate)?;
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({
            "scopeId": scope_id,
            "kind": kind,
            "subject": subject,
            "predicate": predicate,
            "value": value,
            "modality": modality,
            "requestedStatus": requested_status,
            "authority": authority,
            "sourceEventIds": source_event_ids
        });
        let request_hash = operation_request_hash(operation, &request)?;
        let tx = self.immediate()?;
        if let Some(prior) = prior_result::<Claim>(&tx, idempotency_key, operation, &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        ensure_scope_exists(&tx, scope_id)?;
        validate_claim_event_sources(&tx, scope_id, source_event_ids)?;
        let command_event = insert_memory_command_event(&tx, scope_id, operation, &request)?;

        let kind_text = enum_text(&kind);
        let active = query_active_logical_claim(&tx, scope_id, &kind_text, subject, predicate)?;
        if requested_status == ClaimStatus::Active
            && let Some(existing) = &active
            && existing.value == value
        {
            attach_event_sources(&tx, &existing.id, source_event_ids)?;
            attach_event_sources(&tx, &existing.id, std::slice::from_ref(&command_event.id))?;
            save_operation(&tx, idempotency_key, operation, &request_hash, existing)?;
            tx.commit()?;
            return Ok(MutationResult::created(existing.clone()));
        }
        let status = if requested_status == ClaimStatus::Active && active.is_some() {
            ClaimStatus::Disputed
        } else {
            requested_status
        };
        let timestamp = now();
        let claim = Claim {
            id: Uuid::new_v4().to_string(),
            scope_id: scope_id.to_owned(),
            kind,
            subject: subject.trim().to_owned(),
            predicate: predicate.trim().to_owned(),
            value,
            modality,
            status,
            authority,
            confidence: 1.0,
            valid_from: None,
            valid_to: None,
            expires_at: None,
            supersedes_id: None,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        insert_claim(&tx, &claim, None)?;
        attach_event_sources(&tx, &claim.id, source_event_ids)?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        index_claim(&tx, &claim)?;
        save_operation(&tx, idempotency_key, operation, &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn confirm_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({"claimId": claim_id});
        let request_hash = operation_request_hash("claim.confirm", &request)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Claim>(&tx, idempotency_key, "claim.confirm", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let mut claim = query_claim(&tx, claim_id)?;
        ensure!(
            !matches!(claim.status, ClaimStatus::Rejected | ClaimStatus::Expired),
            "cannot confirm a rejected or forgotten claim"
        );
        let command_event =
            insert_memory_command_event(&tx, &claim.scope_id, "claim.confirm", &request)?;
        supersede_other_active_claims(&tx, &claim, Some(&claim.id))?;
        claim.status = ClaimStatus::Active;
        claim.modality = ClaimModality::AcceptedDecision;
        claim.authority = ClaimAuthority::ExplicitUser;
        claim.updated_at = now();
        tx.execute(
            "UPDATE claims SET status='active',modality='accepted-decision',authority='explicit-user',updated_at=?2 WHERE id=?1",
            params![claim.id, claim.updated_at],
        )?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        save_operation(&tx, idempotency_key, "claim.confirm", &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn correct_claim(
        &mut self,
        claim_id: &str,
        value: Value,
        source_event_ids: &[String],
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({
            "claimId": claim_id,
            "value": value,
            "sourceEventIds": source_event_ids
        });
        let request_hash = operation_request_hash("claim.correct", &request)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Claim>(&tx, idempotency_key, "claim.correct", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let old = query_claim(&tx, claim_id)?;
        validate_claim_event_sources(&tx, &old.scope_id, source_event_ids)?;
        let command_event =
            insert_memory_command_event(&tx, &old.scope_id, "claim.correct", &request)?;
        let timestamp = now();
        tx.execute(
            "UPDATE claims SET status='superseded',updated_at=?1 WHERE scope_id=?2 AND kind=?3 AND subject=?4 AND predicate=?5 AND status='active'",
            params![timestamp, old.scope_id, enum_text(&old.kind), old.subject, old.predicate],
        )?;
        tx.execute(
            "UPDATE claims SET status='superseded',updated_at=?2 WHERE id=?1",
            params![old.id, timestamp],
        )?;
        let claim = Claim {
            id: Uuid::new_v4().to_string(),
            scope_id: old.scope_id,
            kind: old.kind,
            subject: old.subject,
            predicate: old.predicate,
            value,
            modality: ClaimModality::ExplicitAssertion,
            status: ClaimStatus::Active,
            authority: ClaimAuthority::ExplicitUser,
            confidence: 1.0,
            valid_from: None,
            valid_to: None,
            expires_at: None,
            supersedes_id: Some(old.id),
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        insert_claim(&tx, &claim, None)?;
        attach_event_sources(&tx, &claim.id, source_event_ids)?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        index_claim(&tx, &claim)?;
        save_operation(&tx, idempotency_key, "claim.correct", &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn rescope_claim(
        &mut self,
        claim_id: &str,
        new_scope_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({"claimId": claim_id, "newScopeId": new_scope_id});
        let request_hash = operation_request_hash("claim.rescope", &request)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Claim>(&tx, idempotency_key, "claim.rescope", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        ensure_scope_exists(&tx, new_scope_id)?;
        let old = query_claim(&tx, claim_id)?;
        validate_existing_claim_sources_visible(&tx, claim_id, new_scope_id)?;
        let command_event =
            insert_memory_command_event(&tx, new_scope_id, "claim.rescope", &request)?;
        let timestamp = now();
        tx.execute(
            "UPDATE claims SET status='superseded',updated_at=?2 WHERE id=?1",
            params![old.id, timestamp],
        )?;
        let claim = Claim {
            id: Uuid::new_v4().to_string(),
            scope_id: new_scope_id.to_owned(),
            status: if old.status == ClaimStatus::Active {
                ClaimStatus::Active
            } else {
                ClaimStatus::Pending
            },
            supersedes_id: Some(old.id.clone()),
            created_at: timestamp.clone(),
            updated_at: timestamp,
            ..old
        };
        if claim.status == ClaimStatus::Active {
            supersede_other_active_claims(&tx, &claim, None)?;
        }
        insert_claim(&tx, &claim, None)?;
        copy_claim_sources(&tx, claim_id, &claim.id)?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        index_claim(&tx, &claim)?;
        save_operation(&tx, idempotency_key, "claim.rescope", &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn reject_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.update_claim_status(
            claim_id,
            ClaimStatus::Rejected,
            idempotency_key,
            "claim.reject",
        )
    }

    pub fn forget_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.update_claim_status(
            claim_id,
            ClaimStatus::Expired,
            idempotency_key,
            "claim.forget",
        )
    }

    fn update_claim_status(
        &mut self,
        claim_id: &str,
        status: ClaimStatus,
        idempotency_key: &str,
        operation: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({"claimId": claim_id, "status": status});
        let request_hash = operation_request_hash(operation, &request)?;
        let tx = self.immediate()?;
        if let Some(prior) = prior_result::<Claim>(&tx, idempotency_key, operation, &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let mut claim = query_claim(&tx, claim_id)?;
        let command_event = insert_memory_command_event(&tx, &claim.scope_id, operation, &request)?;
        claim.status = status;
        claim.updated_at = now();
        tx.execute(
            "UPDATE claims SET status=?2,updated_at=?3 WHERE id=?1",
            params![claim.id, enum_text(&claim.status), claim.updated_at],
        )?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        save_operation(&tx, idempotency_key, operation, &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn list_claims(
        &self,
        scope_id: &str,
        include_ancestors: bool,
        status: Option<ClaimStatus>,
    ) -> Result<Vec<Claim>> {
        let scope_ids = if include_ancestors {
            visible_scope_ids(&self.conn, scope_id)?
        } else {
            ensure_scope_exists(&self.conn, scope_id)?;
            vec![scope_id.to_owned()]
        };
        let status_text = status.as_ref().map(enum_text);
        query_claims_for_scopes(&self.conn, &scope_ids, status_text.as_deref())
    }

    pub fn reconcile(
        &mut self,
        scope_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<ReconciliationSummary>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request_hash = operation_request_hash("claim.reconcile", &scope_id)?;
        let tx = self.immediate()?;
        if let Some(prior) = prior_result::<ReconciliationSummary>(
            &tx,
            idempotency_key,
            "claim.reconcile",
            &request_hash,
        )? {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        ensure_scope_exists(&tx, scope_id)?;
        let pending = query_claims_for_scopes(&tx, &[scope_id.to_owned()], Some("pending"))?;
        let mut summary = ReconciliationSummary {
            activated: Vec::new(),
            disputed: Vec::new(),
            duplicates_rejected: Vec::new(),
            left_pending: Vec::new(),
        };
        for claim in pending {
            if matches!(
                claim.modality,
                ClaimModality::Proposal | ClaimModality::Inference | ClaimModality::Observation
            ) || !claim_has_user_event_source(&tx, &claim.id)?
            {
                summary.left_pending.push(claim.id);
                continue;
            }
            let active = query_active_logical_claim(
                &tx,
                &claim.scope_id,
                &enum_text(&claim.kind),
                &claim.subject,
                &claim.predicate,
            )?;
            match active {
                None => {
                    tx.execute(
                        "UPDATE claims SET status='active',authority='trusted-source',updated_at=?2 WHERE id=?1",
                        params![claim.id, now()],
                    )?;
                    summary.activated.push(claim.id);
                }
                Some(existing) if existing.value == claim.value => {
                    copy_claim_sources(&tx, &claim.id, &existing.id)?;
                    tx.execute(
                        "UPDATE claims SET status='rejected',updated_at=?2 WHERE id=?1",
                        params![claim.id, now()],
                    )?;
                    summary.duplicates_rejected.push(claim.id);
                }
                Some(_) => {
                    tx.execute(
                        "UPDATE claims SET status='disputed',updated_at=?2 WHERE id=?1",
                        params![claim.id, now()],
                    )?;
                    summary.disputed.push(claim.id);
                }
            }
        }
        save_operation(
            &tx,
            idempotency_key,
            "claim.reconcile",
            &request_hash,
            &summary,
        )?;
        tx.commit()?;
        Ok(MutationResult::created(summary))
    }

    pub fn create_view(&mut self, input: CreateView) -> Result<MutationResult<MemoryView>> {
        ensure!(
            matches!(input.kind, ViewKind::Continuity | ViewKind::Continuation),
            "the first implementation supports only continuity and continuation views"
        );
        validate_nonempty("view content", &input.content)?;
        validate_nonempty("idempotency key", &input.idempotency_key)?;
        ensure!(
            input.source_from_sequence > 0
                && input.source_through_sequence >= input.source_from_sequence,
            "view source sequence range is invalid"
        );
        let request_hash = operation_request_hash("view.create", &input)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<MemoryView>(&tx, &input.idempotency_key, "view.create", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        ensure_scope_exists(&tx, &input.scope_id)?;
        for observation_id in &input.source_observation_ids {
            let (source_scope, source_start, source_end): (String, i64, i64) = tx
                .query_row(
                    "SELECT scope_id,source_start_sequence,source_end_sequence FROM observations WHERE id=?1",
                    [observation_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?
                .ok_or_else(|| anyhow!("observation {observation_id} does not exist"))?;
            ensure!(
                source_scope == input.scope_id,
                "observation {observation_id} belongs to scope {source_scope}, not {}",
                input.scope_id
            );
            ensure!(
                source_start >= input.source_from_sequence
                    && source_end <= input.source_through_sequence,
                "observation {observation_id} is outside the declared view source range"
            );
        }
        let estimated_token_count = estimate_tokens(&input.content);
        let token_count = input.token_count.map_or(estimated_token_count, |count| {
            count.max(estimated_token_count)
        });
        ensure!(token_count >= 0, "view token count cannot be negative");
        let view = insert_next_view(
            &tx,
            &input.scope_id,
            input.kind,
            &input.content,
            input.source_from_sequence,
            input.source_through_sequence,
            input.model.as_deref(),
            input.prompt_version.as_deref(),
            token_count,
        )?;
        for observation_id in &input.source_observation_ids {
            tx.execute(
                "INSERT INTO view_sources(view_id,observation_id) VALUES (?1,?2)",
                params![view.id, observation_id],
            )?;
        }
        save_operation(
            &tx,
            &input.idempotency_key,
            "view.create",
            &request_hash,
            &view,
        )?;
        tx.commit()?;
        Ok(MutationResult::created(view))
    }

    pub fn list_views(&self, scope_id: &str) -> Result<Vec<MemoryView>> {
        ensure_scope_exists(&self.conn, scope_id)?;
        let mut statement = self.conn.prepare(
            "SELECT id,scope_id,kind,generation,content,source_from_sequence,source_through_sequence,previous_view_id,model,prompt_version,token_count,created_at
             FROM memory_views WHERE scope_id=?1 ORDER BY kind,generation",
        )?;
        collect_rows(statement.query_map([scope_id], row_view)?)
    }

    pub fn recall_by_observation(&self, observation_id: &str) -> Result<Vec<MemoryEvent>> {
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM observations WHERE id=?1)",
            [observation_id],
            |row| row.get(0),
        )?;
        ensure!(exists, "observation {observation_id} does not exist");
        let mut statement = self.conn.prepare(
            "SELECT e.id,e.stream_id,e.sequence,e.scope_id,e.kind,e.actor_id,e.occurred_at,e.recorded_at,e.content_json,e.content_hash,e.token_count,e.sensitivity,e.metadata_json
             FROM memory_events e JOIN observation_sources s ON s.event_id=e.id
             WHERE s.observation_id=?1 ORDER BY e.stream_id,e.sequence",
        )?;
        collect_rows(statement.query_map([observation_id], row_event)?)
    }

    pub fn explain_observation(&self, observation_id: &str) -> Result<ObservationExplanation> {
        let observation = self
            .conn
            .query_row(
                "SELECT id,run_id,scope_id,kind,content,importance,confidence,event_time_from,event_time_to,source_start_sequence,source_end_sequence,observer_model,prompt_version,created_at
                 FROM observations WHERE id=?1",
                [observation_id],
                row_observation,
            )
            .optional()?
            .ok_or_else(|| anyhow!("observation {observation_id} does not exist"))?;
        Ok(ObservationExplanation {
            observation,
            source_events: self.recall_by_observation(observation_id)?,
        })
    }

    pub fn explain_claim(&self, claim_id: &str) -> Result<ClaimExplanation> {
        let claim = query_claim(&self.conn, claim_id)?;
        let mut observations_statement = self.conn.prepare(
            "SELECT o.id,o.run_id,o.scope_id,o.kind,o.content,o.importance,o.confidence,o.event_time_from,o.event_time_to,o.source_start_sequence,o.source_end_sequence,o.observer_model,o.prompt_version,o.created_at
             FROM observations o JOIN claim_sources s ON s.observation_id=o.id
             WHERE s.claim_id=?1 ORDER BY o.created_at,o.id",
        )?;
        let source_observations =
            collect_rows(observations_statement.query_map([claim_id], row_observation)?)?;
        let mut events_statement = self.conn.prepare(
            "SELECT DISTINCT e.id,e.stream_id,e.sequence,e.scope_id,e.kind,e.actor_id,e.occurred_at,e.recorded_at,e.content_json,e.content_hash,e.token_count,e.sensitivity,e.metadata_json
             FROM memory_events e
             LEFT JOIN claim_sources direct ON direct.event_id=e.id AND direct.claim_id=?1
             LEFT JOIN observation_sources os ON os.event_id=e.id
             LEFT JOIN claim_sources via_observation ON via_observation.observation_id=os.observation_id AND via_observation.claim_id=?1
             WHERE direct.claim_id IS NOT NULL OR via_observation.claim_id IS NOT NULL
             ORDER BY e.stream_id,e.sequence",
        )?;
        let source_events = collect_rows(events_statement.query_map([claim_id], row_event)?)?;
        Ok(ClaimExplanation {
            claim,
            source_observations,
            source_events,
        })
    }

    pub fn search_full_text(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        self.search_full_text_mode(scope_id, query, limit, false)
    }

    pub fn search_full_text_advanced(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        self.search_full_text_mode(scope_id, query, limit, true)
    }

    fn search_full_text_mode(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
        advanced_fts: bool,
    ) -> Result<Vec<SearchHit>> {
        validate_nonempty("search query", query)?;
        ensure!(
            limit > 0 && limit <= 1000,
            "search limit must be from 1 to 1000"
        );
        let scope_ids = retrieval_scope_ids(&self.conn, scope_id)?;
        let fts_query = if advanced_fts {
            query.to_owned()
        } else {
            literal_fts_query(query)
        };
        search_fts(&self.conn, &scope_ids, &fts_query, limit)
    }

    pub fn compose_context(
        &self,
        scope_id: &str,
        stream_id: &str,
        max_tokens: i64,
        recent_raw_tokens: i64,
        query: Option<&str>,
    ) -> Result<ContextBundle> {
        ensure!(max_tokens > 0, "max tokens must be positive");
        ensure!(
            recent_raw_tokens >= 0,
            "recent raw tokens cannot be negative"
        );
        let visible = visible_scope_ids(&self.conn, scope_id)?;
        let stream_scope: String = self
            .conn
            .query_row(
                "SELECT scope_id FROM memory_streams WHERE id=?1",
                [stream_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| anyhow!("stream {stream_id} does not exist"))?;
        ensure!(
            visible.contains(&stream_scope)
                || scope_is_ancestor(&self.conn, scope_id, &stream_scope)?,
            "stream {stream_id} is not visible from scope {scope_id}"
        );
        let mut claims = query_claims_for_scopes(&self.conn, &visible, Some("active"))?;
        sort_claims_by_scope(&mut claims, &visible);
        let mut pending_claims = query_claims_for_scopes(&self.conn, &visible, Some("pending"))?;
        pending_claims.extend(query_claims_for_scopes(
            &self.conn,
            &visible,
            Some("disputed"),
        )?);
        sort_claims_by_scope(&mut pending_claims, &visible);
        let required_tokens: i64 = claims.iter().map(estimate_claim_tokens).sum();
        ensure!(
            required_tokens <= max_tokens,
            "context budget too small: minimumRequiredTokens={required_tokens} for active claims"
        );
        let mut diagnostics = ContextDiagnostics {
            estimated_tokens: required_tokens,
            omitted_items: Vec::new(),
        };

        let mut selected_pending_claims = Vec::new();
        for claim in pending_claims {
            let cost = estimate_claim_tokens(&claim);
            if diagnostics.estimated_tokens + cost <= max_tokens {
                diagnostics.estimated_tokens += cost;
                selected_pending_claims.push(claim);
            } else {
                diagnostics.omitted_items.push(OmittedItem {
                    id: claim.id,
                    reason: "context token budget".to_owned(),
                });
            }
        }

        let mut candidate_views = Vec::new();
        for visible_scope in &visible {
            if let Some(view) = latest_view(&self.conn, visible_scope, "continuity")? {
                candidate_views.push(view);
            }
            if let Some(view) = latest_view(&self.conn, visible_scope, "continuation")? {
                candidate_views.push(view);
            }
        }

        let all_events = query_events_after(&self.conn, stream_id, 0)?;
        let mut recent_events_reversed = Vec::new();
        let raw_budget = recent_raw_tokens.min((max_tokens - diagnostics.estimated_tokens).max(0));
        let mut raw_tokens = 0;
        for event in all_events.iter().rev() {
            let safe = redact_for_agent(event.clone());
            if raw_tokens + safe.token_count > raw_budget {
                diagnostics.omitted_items.push(OmittedItem {
                    id: event.id.clone(),
                    reason: "outside recent raw token budget".to_owned(),
                });
                break;
            }
            raw_tokens += safe.token_count;
            recent_events_reversed.push(safe);
        }
        recent_events_reversed.reverse();
        let recent_events = recent_events_reversed;
        diagnostics.estimated_tokens += raw_tokens;
        let raw_start = recent_events.first().map(|event| event.sequence);
        let raw_end = recent_events.last().map(|event| event.sequence);

        let mut continuity_views = Vec::new();
        for view in candidate_views {
            if diagnostics.estimated_tokens + view.token_count <= max_tokens {
                diagnostics.estimated_tokens += view.token_count;
                continuity_views.push(view);
            } else {
                diagnostics.omitted_items.push(OmittedItem {
                    id: view.id,
                    reason: "context token budget".to_owned(),
                });
            }
        }

        let mut candidates = query_observations_for_scopes(&self.conn, &visible)?;
        candidates.sort_by(|left, right| {
            right
                .importance
                .total_cmp(&left.importance)
                .then_with(|| left.created_at.cmp(&right.created_at))
        });
        let represented_through: HashMap<&str, i64> = continuity_views
            .iter()
            .filter(|view| view.kind == ViewKind::Continuity)
            .map(|view| (view.scope_id.as_str(), view.source_through_sequence))
            .collect();
        let mut observations = Vec::new();
        for observation in candidates {
            let duplicated_by_raw = raw_start.is_some_and(|from| {
                observation.scope_id == stream_scope
                    && observation.source_start_sequence >= from
                    && observation.source_end_sequence <= raw_end.unwrap_or(from)
            });
            let represented_by_view = represented_through
                .get(observation.scope_id.as_str())
                .is_some_and(|through| observation.source_end_sequence <= *through);
            if duplicated_by_raw || represented_by_view {
                diagnostics.omitted_items.push(OmittedItem {
                    id: observation.id,
                    reason: if duplicated_by_raw {
                        "source events already present in raw tail"
                    } else {
                        "already represented by continuity view"
                    }
                    .to_owned(),
                });
                continue;
            }
            let cost = estimate_tokens(&observation.content);
            if diagnostics.estimated_tokens + cost <= max_tokens {
                diagnostics.estimated_tokens += cost;
                observations.push(observation);
            } else {
                diagnostics.omitted_items.push(OmittedItem {
                    id: observation.id,
                    reason: "context token budget".to_owned(),
                });
            }
        }
        observations.sort_by(|left, right| left.created_at.cmp(&right.created_at));

        let mut recalled_evidence = Vec::new();
        if let Some(query) = query {
            let retrieval_scopes = retrieval_scope_ids(&self.conn, scope_id)?;
            let hits = search_fts(&self.conn, &retrieval_scopes, &literal_fts_query(query), 10)?;
            let mut recalled_ids = HashSet::new();
            for hit in hits {
                let evidence = match hit.record_type.as_str() {
                    "event" => vec![self.get_event(&hit.id)?],
                    "observation" => self.recall_by_observation(&hit.id)?,
                    "claim" => self.explain_claim(&hit.id)?.source_events,
                    _ => Vec::new(),
                };
                for event in evidence {
                    if !recalled_ids.insert(event.id.clone())
                        || recent_events.iter().any(|recent| recent.id == event.id)
                    {
                        continue;
                    }
                    let safe = redact_for_agent(event);
                    if diagnostics.estimated_tokens + safe.token_count <= max_tokens {
                        diagnostics.estimated_tokens += safe.token_count;
                        recalled_evidence.push(safe);
                    } else {
                        diagnostics.omitted_items.push(OmittedItem {
                            id: safe.id,
                            reason: "context token budget".to_owned(),
                        });
                    }
                }
            }
        }
        Ok(ContextBundle {
            claims,
            pending_claims: selected_pending_claims,
            continuity_views,
            observations,
            recent_events,
            recalled_evidence,
            diagnostics,
        })
    }

    pub fn purge_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Value>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request_hash = operation_request_hash("claim.purge", &claim_id)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Value>(&tx, idempotency_key, "claim.purge", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        query_claim(&tx, claim_id)?;
        let command_event_ids = generated_command_source_ids(&tx, claim_id)?;
        tx.execute(
            "DELETE FROM memory_fts WHERE record_type='claim' AND record_id=?1",
            [claim_id],
        )?;
        tx.execute("DELETE FROM claims WHERE id=?1", [claim_id])?;
        let purged_command_events = purge_orphaned_command_events(&tx, &command_event_ids)?;
        scrub_operations_referencing(&tx, claim_id)?;
        let result = json!({
            "purged": "claim",
            "id": claim_id,
            "purgedCommandEvents": purged_command_events
        });
        save_operation(&tx, idempotency_key, "claim.purge", &request_hash, &result)?;
        tx.commit()?;
        Ok(MutationResult::created(result))
    }

    pub fn purge_event(
        &mut self,
        event_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Value>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request_hash = operation_request_hash("event.purge", &event_id)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Value>(&tx, idempotency_key, "event.purge", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let event: (String, i64, String) = tx
            .query_row(
                "SELECT stream_id,sequence,scope_id FROM memory_events WHERE id=?1",
                [event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| anyhow!("event {event_id} does not exist"))?;
        let observation_ids = query_string_column(
            &tx,
            "SELECT observation_id FROM observation_sources WHERE event_id=?1",
            event_id,
        )?;
        let mut claim_ids = query_string_column(
            &tx,
            "SELECT claim_id FROM claim_sources WHERE event_id=?1",
            event_id,
        )?;
        for observation_id in &observation_ids {
            claim_ids.extend(query_string_column(
                &tx,
                "SELECT claim_id FROM claim_sources WHERE observation_id=?1",
                observation_id,
            )?);
        }
        claim_ids.sort();
        claim_ids.dedup();
        let mut purged_view_ids = HashSet::new();
        for claim_id in &claim_ids {
            tx.execute(
                "DELETE FROM memory_fts WHERE record_type='claim' AND record_id=?1",
                [claim_id],
            )?;
            scrub_operations_referencing(&tx, claim_id)?;
            tx.execute("DELETE FROM claims WHERE id=?1", [claim_id])?;
        }
        for observation_id in &observation_ids {
            let view_ids = query_string_column(
                &tx,
                "SELECT view_id FROM view_sources WHERE observation_id=?1",
                observation_id,
            )?;
            for view_id in view_ids {
                purged_view_ids.insert(view_id.clone());
                tx.execute(
                    "DELETE FROM memory_fts WHERE record_type='view' AND record_id=?1",
                    [&view_id],
                )?;
                scrub_operations_referencing(&tx, &view_id)?;
                tx.execute("DELETE FROM memory_views WHERE id=?1", [&view_id])?;
            }
            tx.execute(
                "DELETE FROM memory_fts WHERE record_type='observation' AND record_id=?1",
                [observation_id],
            )?;
            scrub_operations_referencing(&tx, observation_id)?;
            tx.execute("DELETE FROM observations WHERE id=?1", [observation_id])?;
        }
        let range_view_ids = {
            let mut statement = tx.prepare(
                "SELECT id FROM memory_views
                 WHERE scope_id=?1 AND source_from_sequence<=?2 AND source_through_sequence>=?2",
            )?;
            collect_rows(
                statement.query_map(params![event.2, event.1], |row| row.get::<_, String>(0))?,
            )?
        };
        for view_id in range_view_ids {
            purged_view_ids.insert(view_id.clone());
            tx.execute(
                "DELETE FROM memory_fts WHERE record_type='view' AND record_id=?1",
                [&view_id],
            )?;
            scrub_operations_referencing(&tx, &view_id)?;
            tx.execute("DELETE FROM memory_views WHERE id=?1", [&view_id])?;
        }
        let affected_run_ids = {
            let mut statement = tx.prepare(
                "SELECT id FROM observation_runs
                 WHERE stream_id=?1 AND from_sequence<=?2 AND to_sequence>=?2
                 ORDER BY created_at,id",
            )?;
            collect_rows(
                statement.query_map(params![event.0, event.1], |row| row.get::<_, String>(0))?,
            )?
        };
        tx.execute(
            "UPDATE observation_runs
             SET status=CASE WHEN status IN ('pending','running') THEN 'stale' ELSE status END,
                 source_integrity='privacy-purged',
                 ambiguities_json='[]',
                 error=CASE WHEN status IN ('pending','running') THEN 'source evidence privacy-purged' ELSE error END,
                 updated_at=?1
             WHERE stream_id=?2 AND from_sequence<=?3 AND to_sequence>=?3",
            params![now(), event.0, event.1],
        )?;
        tx.execute(
            "DELETE FROM memory_fts WHERE record_type='event' AND record_id=?1",
            [event_id],
        )?;
        tx.execute("DELETE FROM memory_events WHERE id=?1", [event_id])?;
        scrub_operations_referencing(&tx, event_id)?;
        let mut dependent_view_ids: Vec<String> = purged_view_ids.into_iter().collect();
        dependent_view_ids.sort();
        let result = json!({
            "purged": "event",
            "id": event_id,
            "streamId": event.0,
            "sequence": event.1,
            "dependentObservations": observation_ids.len(),
            "dependentClaims": claim_ids.len(),
            "dependentViews": dependent_view_ids.len(),
            "dependentViewIds": dependent_view_ids,
            "affectedRunIds": affected_run_ids
        });
        save_operation(&tx, idempotency_key, "event.purge", &request_hash, &result)?;
        tx.commit()?;
        Ok(MutationResult::created(result))
    }
}

#[derive(Debug)]
struct ObservationRun {
    scope_id: String,
    stream_id: String,
    cursor_at_plan: i64,
    from_sequence: i64,
    to_sequence: i64,
    status: String,
    observer_model: String,
    prompt_version: String,
}

fn query_run(conn: &Connection, id: &str) -> Result<ObservationRun> {
    conn.query_row(
        "SELECT scope_id,stream_id,cursor_at_plan,from_sequence,to_sequence,status,observer_model,prompt_version FROM observation_runs WHERE id=?1",
        [id],
        |row| {
            Ok(ObservationRun {
                scope_id: row.get(0)?,
                stream_id: row.get(1)?,
                cursor_at_plan: row.get(2)?,
                from_sequence: row.get(3)?,
                to_sequence: row.get(4)?,
                status: row.get(5)?,
                observer_model: row.get(6)?,
                prompt_version: row.get(7)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| anyhow!("observation run {id} does not exist"))
}

fn validate_observer_result(result: &ObserverResult) -> Result<()> {
    if observer_result_is_completely_empty(result) {
        ensure!(
            result
                .empty_reason
                .as_deref()
                .is_some_and(|reason| !reason.trim().is_empty()),
            "an empty ObserverResult requires a non-empty emptyReason acknowledgement"
        );
    }
    if let Some(reason) = &result.empty_reason {
        ensure!(
            reason.chars().count() <= 500,
            "emptyReason must be at most 500 characters"
        );
    }
    for (index, observation) in result.observations.iter().enumerate() {
        ensure!(
            !observation.content.trim().is_empty(),
            "observation {index} content is empty"
        );
        validate_score("observation importance", observation.importance)?;
        validate_score("observation confidence", observation.confidence)?;
        ensure!(
            !observation.source_event_ids.is_empty(),
            "observation {index} has no source events"
        );
    }
    for (index, claim) in result.claims.iter().enumerate() {
        ensure!(
            !claim.subject.trim().is_empty(),
            "claim {index} subject is empty"
        );
        ensure!(
            !claim.predicate.trim().is_empty(),
            "claim {index} predicate is empty"
        );
        validate_score("claim confidence", claim.confidence)?;
        ensure!(
            !claim.source_event_ids.is_empty(),
            "claim {index} has no source events"
        );
    }
    for (index, ambiguity) in result.ambiguities.iter().enumerate() {
        ensure!(
            !ambiguity.description.trim().is_empty(),
            "ambiguity {index} description is empty"
        );
        ensure!(
            !ambiguity.source_event_ids.is_empty(),
            "ambiguity {index} has no source events"
        );
    }
    Ok(())
}

fn observer_result_is_completely_empty(result: &ObserverResult) -> bool {
    result.observations.is_empty()
        && result.claims.is_empty()
        && result.ambiguities.is_empty()
        && result.continuation.current_task.is_none()
        && result.continuation.completed.is_empty()
        && result.continuation.blockers.is_empty()
        && result.continuation.next_actions.is_empty()
        && result.continuation.unresolved_questions.is_empty()
}

fn validate_provenance(
    result: &ObserverResult,
    sources_by_id: &HashMap<String, MemoryEvent>,
) -> Result<()> {
    let all_ids = result
        .observations
        .iter()
        .flat_map(|item| item.source_event_ids.iter())
        .chain(
            result
                .claims
                .iter()
                .flat_map(|item| item.source_event_ids.iter()),
        )
        .chain(
            result
                .ambiguities
                .iter()
                .flat_map(|item| item.source_event_ids.iter()),
        );
    for event_id in all_ids {
        let event = sources_by_id
            .get(event_id)
            .ok_or_else(|| anyhow!("source event {event_id} is not in the observation run"))?;
        ensure!(
            matches!(
                event.sensitivity,
                Sensitivity::Normal | Sensitivity::Private
            ),
            "redacted event {event_id} cannot source derived memory"
        );
    }
    Ok(())
}

fn validate_score(name: &str, score: f64) -> Result<()> {
    ensure!(
        score.is_finite() && (0.0..=1.0).contains(&score),
        "{name} must be between 0 and 1"
    );
    Ok(())
}

fn redact_for_agent(mut event: MemoryEvent) -> MemoryEvent {
    if event.sensitivity == Sensitivity::Secret {
        event.content = json!({"redacted": true, "reason": "secret"});
        event.metadata = json!({});
        event.content_hash = hash_json(&event.content);
        event.token_count = estimate_event_tokens(&event.content, &event.metadata);
    }
    event
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn validate_nonempty(name: &str, value: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{name} cannot be empty");
    Ok(())
}

fn estimate_tokens(text: &str) -> i64 {
    ((text.chars().count() as i64 + 3) / 4).max(1)
}

fn estimate_event_tokens(content: &Value, metadata: &Value) -> i64 {
    estimate_tokens(&format!("{content} {metadata}"))
}

fn hash_json(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
}

fn searchable_json(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn ensure_scope_exists(conn: &Connection, id: &str) -> Result<()> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM memory_scopes WHERE id=?1)",
        [id],
        |row| row.get(0),
    )?;
    ensure!(exists, "scope {id} does not exist");
    Ok(())
}

fn prior_result<T: DeserializeOwned>(
    conn: &Connection,
    key: &str,
    expected_operation: &str,
    expected_request_hash: &str,
) -> Result<Option<T>> {
    let prior: Option<(String, String, String, bool)> = conn
        .query_row(
            "SELECT operation,request_hash,result_json,purged FROM memory_operations WHERE idempotency_key=?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((operation, request_hash, result_json, purged)) = prior else {
        return Ok(None);
    };
    ensure!(
        operation == expected_operation,
        "idempotency key was already used for {operation}, not {expected_operation}"
    );
    ensure!(
        !purged,
        "the prior result for this idempotency key was privacy-purged"
    );
    ensure!(
        request_hash == expected_request_hash,
        "idempotency conflict: this key was already used with different request input"
    );
    Ok(Some(serde_json::from_str(&result_json).with_context(
        || format!("reading stored result for idempotency key {key}"),
    )?))
}

fn save_operation<T: Serialize + ?Sized>(
    conn: &Connection,
    key: &str,
    operation: &str,
    request_hash: &str,
    result: &T,
) -> Result<()> {
    conn.execute(
        "INSERT INTO memory_operations(idempotency_key,operation,request_hash,result_json,created_at) VALUES (?1,?2,?3,?4,?5)",
        params![key, operation, request_hash, serde_json::to_string(result)?, now()],
    )?;
    Ok(())
}

fn operation_request_hash(operation: &str, request: &impl Serialize) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(operation.as_bytes());
    hasher.update([0]);
    hasher.update(serde_json::to_vec(request)?);
    Ok(format!("{:x}", hasher.finalize()))
}

fn scrub_operations_referencing(conn: &Connection, record_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE memory_operations SET result_json='{}',purged=1 WHERE instr(result_json,?1)>0",
        [record_id],
    )?;
    Ok(())
}

fn generated_command_source_ids(conn: &Connection, claim_id: &str) -> Result<Vec<String>> {
    let mut statement = conn.prepare(
        "SELECT e.id
         FROM memory_events e
         JOIN claim_sources source ON source.event_id=e.id
         WHERE source.claim_id=?1
           AND e.kind='memory-command'
           AND json_extract(e.metadata_json,'$.generatedBy')='omk'",
    )?;
    collect_rows(statement.query_map([claim_id], |row| row.get(0))?)
}

fn purge_orphaned_command_events(conn: &Connection, event_ids: &[String]) -> Result<usize> {
    let mut purged = 0;
    for event_id in event_ids {
        let referenced: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM claim_sources WHERE event_id=?1)
                    OR EXISTS(SELECT 1 FROM observation_sources WHERE event_id=?1)",
            [event_id],
            |row| row.get(0),
        )?;
        if referenced {
            continue;
        }
        conn.execute(
            "DELETE FROM memory_fts WHERE record_type='event' AND record_id=?1",
            [event_id],
        )?;
        conn.execute("DELETE FROM memory_events WHERE id=?1", [event_id])?;
        scrub_operations_referencing(conn, event_id)?;
        purged += 1;
    }
    Ok(purged)
}

fn insert_fts(
    conn: &Connection,
    record_type: &str,
    record_id: &str,
    scope_id: &str,
    text: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO memory_fts(record_type,record_id,scope_id,text) VALUES (?1,?2,?3,?4)",
        params![record_type, record_id, scope_id, text],
    )?;
    Ok(())
}

fn query_scope(conn: &Connection, id: &str) -> Result<Scope> {
    conn.query_row(
        "SELECT id,kind,parent_id,name,created_at FROM memory_scopes WHERE id=?1",
        [id],
        row_scope,
    )
    .optional()?
    .ok_or_else(|| anyhow!("scope {id} does not exist"))
}

fn visible_scope_ids(conn: &Connection, scope_id: &str) -> Result<Vec<String>> {
    let mut current = Some(scope_id.to_owned());
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    while let Some(id) = current {
        ensure!(
            seen.insert(id.clone()),
            "scope hierarchy contains a cycle at {id}"
        );
        let parent: Option<Option<String>> = conn
            .query_row(
                "SELECT parent_id FROM memory_scopes WHERE id=?1",
                [&id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(parent) = parent else {
            bail!("scope {id} does not exist");
        };
        result.push(id);
        current = parent;
    }
    result.reverse();
    Ok(result)
}

fn retrieval_scope_ids(conn: &Connection, scope_id: &str) -> Result<Vec<String>> {
    let mut result = visible_scope_ids(conn, scope_id)?;
    let mut statement = conn.prepare(
        "WITH RECURSIVE subtree(id) AS (
            SELECT id FROM memory_scopes WHERE id=?1
            UNION ALL
            SELECT child.id FROM memory_scopes child JOIN subtree parent ON child.parent_id=parent.id
         ) SELECT id FROM subtree",
    )?;
    for id in collect_rows(statement.query_map([scope_id], |row| row.get::<_, String>(0))?)? {
        if !result.contains(&id) {
            result.push(id);
        }
    }
    Ok(result)
}

fn scope_is_ancestor(conn: &Connection, ancestor_id: &str, descendant_id: &str) -> Result<bool> {
    Ok(visible_scope_ids(conn, descendant_id)?
        .iter()
        .any(|scope_id| scope_id == ancestor_id))
}

fn literal_fts_query(query: &str) -> String {
    format!("\"{}\"", query.replace('"', "\"\""))
}

fn query_events_after(conn: &Connection, stream_id: &str, cursor: i64) -> Result<Vec<MemoryEvent>> {
    let mut statement = conn.prepare(
        "SELECT id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json
         FROM memory_events WHERE stream_id=?1 AND sequence>?2 ORDER BY sequence",
    )?;
    collect_rows(statement.query_map(params![stream_id, cursor], row_event)?)
}

fn query_events_range(
    conn: &Connection,
    stream_id: &str,
    from_sequence: i64,
    to_sequence: i64,
) -> Result<Vec<MemoryEvent>> {
    let mut statement = conn.prepare(
        "SELECT id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json
         FROM memory_events WHERE stream_id=?1 AND sequence BETWEEN ?2 AND ?3 ORDER BY sequence",
    )?;
    collect_rows(statement.query_map(params![stream_id, from_sequence, to_sequence], row_event)?)
}

fn insert_observation(conn: &Connection, observation: &Observation) -> Result<()> {
    conn.execute(
        "INSERT INTO observations(id,run_id,scope_id,kind,content,importance,confidence,event_time_from,event_time_to,source_start_sequence,source_end_sequence,observer_model,prompt_version,created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            observation.id,
            observation.run_id,
            observation.scope_id,
            enum_text(&observation.kind),
            observation.content,
            observation.importance,
            observation.confidence,
            observation.event_time_from,
            observation.event_time_to,
            observation.source_start_sequence,
            observation.source_end_sequence,
            observation.observer_model,
            observation.prompt_version,
            observation.created_at,
        ],
    )?;
    Ok(())
}

fn insert_memory_command_event(
    conn: &Connection,
    scope_id: &str,
    operation: &str,
    request: &Value,
) -> Result<MemoryEvent> {
    let stream_id = format!("memory-commands:{scope_id}");
    let timestamp = now();
    conn.execute(
        "INSERT OR IGNORE INTO memory_streams(id,scope_id,created_at) VALUES (?1,?2,?3)",
        params![stream_id, scope_id, timestamp],
    )?;
    let sequence: i64 = conn.query_row(
        "SELECT next_sequence FROM memory_streams WHERE id=?1",
        [&stream_id],
        |row| row.get(0),
    )?;
    conn.execute(
        "UPDATE memory_streams SET next_sequence=next_sequence+1 WHERE id=?1",
        [&stream_id],
    )?;
    let content = json!({"operation": operation, "request": request});
    let event = MemoryEvent {
        id: Uuid::new_v4().to_string(),
        stream_id,
        sequence,
        scope_id: scope_id.to_owned(),
        kind: EventKind::MemoryCommand,
        actor_id: Some("explicit-user".to_owned()),
        occurred_at: timestamp.clone(),
        recorded_at: timestamp,
        content_hash: hash_json(&content),
        token_count: estimate_tokens(&content.to_string()),
        content,
        sensitivity: Sensitivity::Normal,
        metadata: json!({"generatedBy": "omk"}),
    };
    conn.execute(
        "INSERT INTO memory_events(id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json,idempotency_key)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            event.id,
            event.stream_id,
            event.sequence,
            event.scope_id,
            enum_text(&event.kind),
            event.actor_id,
            event.occurred_at,
            event.recorded_at,
            event.content.to_string(),
            event.content_hash,
            event.token_count,
            enum_text(&event.sensitivity),
            event.metadata.to_string(),
            format!("internal-memory-command:{}", event.id),
        ],
    )?;
    insert_fts(
        conn,
        "event",
        &event.id,
        &event.scope_id,
        &searchable_json(&event.content),
    )?;
    Ok(event)
}

fn insert_claim(conn: &Connection, claim: &Claim, origin_run_id: Option<&str>) -> Result<()> {
    conn.execute(
        "INSERT INTO claims(id,origin_run_id,scope_id,kind,subject,predicate,value_json,modality,status,authority,confidence,valid_from,valid_to,expires_at,supersedes_id,created_at,updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        params![
            claim.id,
            origin_run_id,
            claim.scope_id,
            enum_text(&claim.kind),
            claim.subject,
            claim.predicate,
            claim.value.to_string(),
            enum_text(&claim.modality),
            enum_text(&claim.status),
            enum_text(&claim.authority),
            claim.confidence,
            claim.valid_from,
            claim.valid_to,
            claim.expires_at,
            claim.supersedes_id,
            claim.created_at,
            claim.updated_at,
        ],
    )?;
    Ok(())
}

fn index_claim(conn: &Connection, claim: &Claim) -> Result<()> {
    insert_fts(
        conn,
        "claim",
        &claim.id,
        &claim.scope_id,
        &format!(
            "{} {} {}",
            claim.subject,
            claim.predicate,
            searchable_json(&claim.value)
        ),
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_next_view(
    conn: &Connection,
    scope_id: &str,
    kind: ViewKind,
    content: &str,
    source_from_sequence: i64,
    source_through_sequence: i64,
    model: Option<&str>,
    prompt_version: Option<&str>,
    token_count: i64,
) -> Result<MemoryView> {
    let kind_text = enum_text(&kind);
    let previous = latest_view(conn, scope_id, &kind_text)?;
    let view = MemoryView {
        id: Uuid::new_v4().to_string(),
        scope_id: scope_id.to_owned(),
        kind,
        generation: previous.as_ref().map_or(1, |view| view.generation + 1),
        content: content.to_owned(),
        source_from_sequence,
        source_through_sequence,
        previous_view_id: previous.map(|view| view.id),
        model: model.map(str::to_owned),
        prompt_version: prompt_version.map(str::to_owned),
        token_count,
        created_at: now(),
    };
    conn.execute(
        "INSERT INTO memory_views(id,scope_id,kind,generation,content,source_from_sequence,source_through_sequence,previous_view_id,model,prompt_version,token_count,created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        params![
            view.id,
            view.scope_id,
            kind_text,
            view.generation,
            view.content,
            view.source_from_sequence,
            view.source_through_sequence,
            view.previous_view_id,
            view.model,
            view.prompt_version,
            view.token_count,
            view.created_at,
        ],
    )?;
    insert_fts(conn, "view", &view.id, &view.scope_id, &view.content)?;
    Ok(view)
}

fn latest_view(conn: &Connection, scope_id: &str, kind: &str) -> Result<Option<MemoryView>> {
    Ok(conn
        .query_row(
            "SELECT id,scope_id,kind,generation,content,source_from_sequence,source_through_sequence,previous_view_id,model,prompt_version,token_count,created_at
             FROM memory_views WHERE scope_id=?1 AND kind=?2 ORDER BY generation DESC LIMIT 1",
            params![scope_id, kind],
            row_view,
        )
        .optional()?)
}

fn query_claims_for_scopes(
    conn: &Connection,
    scope_ids: &[String],
    status: Option<&str>,
) -> Result<Vec<Claim>> {
    if scope_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = std::iter::repeat_n("?", scope_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let mut sql = format!(
        "SELECT id,scope_id,kind,subject,predicate,value_json,modality,status,authority,confidence,valid_from,valid_to,expires_at,supersedes_id,created_at,updated_at FROM claims WHERE scope_id IN ({placeholders})"
    );
    let mut values = scope_ids.to_vec();
    if let Some(status) = status {
        sql.push_str(" AND status=?");
        values.push(status.to_owned());
    }
    sql.push_str(" ORDER BY created_at,id");
    let mut statement = conn.prepare(&sql)?;
    collect_rows(statement.query_map(rusqlite::params_from_iter(values), row_claim)?)
}

fn query_claim(conn: &Connection, id: &str) -> Result<Claim> {
    conn.query_row(
        "SELECT id,scope_id,kind,subject,predicate,value_json,modality,status,authority,confidence,valid_from,valid_to,expires_at,supersedes_id,created_at,updated_at FROM claims WHERE id=?1",
        [id],
        row_claim,
    )
    .optional()?
    .ok_or_else(|| anyhow!("claim {id} does not exist"))
}

fn query_active_logical_claim(
    conn: &Connection,
    scope_id: &str,
    kind: &str,
    subject: &str,
    predicate: &str,
) -> Result<Option<Claim>> {
    Ok(conn
        .query_row(
            "SELECT id,scope_id,kind,subject,predicate,value_json,modality,status,authority,confidence,valid_from,valid_to,expires_at,supersedes_id,created_at,updated_at
             FROM claims WHERE scope_id=?1 AND kind=?2 AND subject=?3 AND predicate=?4 AND status='active'
             ORDER BY updated_at DESC LIMIT 1",
            params![scope_id, kind, subject, predicate],
            row_claim,
        )
        .optional()?)
}

fn supersede_other_active_claims(
    conn: &Connection,
    claim: &Claim,
    excluded_id: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE claims SET status='superseded',updated_at=?1
         WHERE scope_id=?2 AND kind=?3 AND subject=?4 AND predicate=?5 AND status='active'
           AND (?6 IS NULL OR id != ?6)",
        params![
            now(),
            claim.scope_id,
            enum_text(&claim.kind),
            claim.subject,
            claim.predicate,
            excluded_id,
        ],
    )?;
    Ok(())
}

fn validate_claim_event_sources(
    conn: &Connection,
    scope_id: &str,
    event_ids: &[String],
) -> Result<()> {
    let visible = retrieval_scope_ids(conn, scope_id)?;
    for event_id in event_ids {
        let (event_scope, sensitivity): (String, String) = conn
            .query_row(
                "SELECT scope_id,sensitivity FROM memory_events WHERE id=?1",
                [event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| anyhow!("source event {event_id} does not exist"))?;
        ensure!(
            visible.contains(&event_scope),
            "source event {event_id} from scope {event_scope} is not visible to scope {scope_id}"
        );
        ensure!(
            matches!(sensitivity.as_str(), "normal" | "private"),
            "redacted event {event_id} cannot source a claim"
        );
    }
    Ok(())
}

fn validate_existing_claim_sources_visible(
    conn: &Connection,
    claim_id: &str,
    new_scope_id: &str,
) -> Result<()> {
    let visible = retrieval_scope_ids(conn, new_scope_id)?;
    let source_scopes = {
        let mut statement = conn.prepare(
            "SELECT e.scope_id
             FROM claim_sources source
             JOIN memory_events e ON e.id=source.event_id
             WHERE source.claim_id=?1
             UNION
             SELECT observation.scope_id
             FROM claim_sources source
             JOIN observations observation ON observation.id=source.observation_id
             WHERE source.claim_id=?1",
        )?;
        collect_rows(statement.query_map([claim_id], |row| row.get::<_, String>(0))?)?
    };
    for source_scope in source_scopes {
        ensure!(
            visible.contains(&source_scope),
            "claim {claim_id} has source evidence in scope {source_scope}, which is not visible from scope {new_scope_id}"
        );
    }
    Ok(())
}

fn attach_event_sources(conn: &Connection, claim_id: &str, event_ids: &[String]) -> Result<()> {
    for event_id in event_ids {
        conn.execute(
            "INSERT OR IGNORE INTO claim_sources(claim_id,event_id) VALUES (?1,?2)",
            params![claim_id, event_id],
        )?;
    }
    Ok(())
}

fn copy_claim_sources(conn: &Connection, from_claim_id: &str, to_claim_id: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO claim_sources(claim_id,observation_id,event_id)
         SELECT ?2,observation_id,event_id FROM claim_sources WHERE claim_id=?1",
        params![from_claim_id, to_claim_id],
    )?;
    Ok(())
}

fn claim_has_user_event_source(conn: &Connection, claim_id: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM claim_sources s
            JOIN memory_events e ON e.id=s.event_id
            WHERE s.claim_id=?1 AND e.kind='user-message'
              AND e.sensitivity IN ('normal','private')
        )",
        [claim_id],
        |row| row.get(0),
    )?)
}

fn query_observations_for_scopes(
    conn: &Connection,
    scope_ids: &[String],
) -> Result<Vec<Observation>> {
    if scope_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = std::iter::repeat_n("?", scope_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT id,run_id,scope_id,kind,content,importance,confidence,event_time_from,event_time_to,source_start_sequence,source_end_sequence,observer_model,prompt_version,created_at
         FROM observations WHERE scope_id IN ({placeholders}) ORDER BY created_at,id"
    );
    let mut statement = conn.prepare(&sql)?;
    collect_rows(statement.query_map(rusqlite::params_from_iter(scope_ids), row_observation)?)
}

fn estimate_claim_tokens(claim: &Claim) -> i64 {
    estimate_tokens(&format!(
        "{} {} {}",
        claim.subject, claim.predicate, claim.value
    ))
}

fn sort_claims_by_scope(claims: &mut [Claim], scope_order: &[String]) {
    claims.sort_by_key(|claim| {
        scope_order
            .iter()
            .position(|scope_id| scope_id == &claim.scope_id)
            .unwrap_or(usize::MAX)
    });
}

fn search_fts(
    conn: &Connection,
    scope_ids: &[String],
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>> {
    if scope_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = std::iter::repeat_n("?", scope_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT record_type,record_id,scope_id,text,rank FROM memory_fts
         WHERE memory_fts MATCH ? AND scope_id IN ({placeholders})
         ORDER BY rank LIMIT ?"
    );
    let mut values = Vec::with_capacity(scope_ids.len() + 2);
    values.push(rusqlite::types::Value::Text(query.to_owned()));
    values.extend(scope_ids.iter().cloned().map(rusqlite::types::Value::Text));
    values.push(rusqlite::types::Value::Integer(limit as i64));
    let mut statement = conn.prepare(&sql)?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(values), |row| {
            Ok(SearchHit {
                record_type: row.get(0)?,
                id: row.get(1)?,
                scope_id: row.get(2)?,
                text: row.get(3)?,
                rank: row.get(4)?,
            })
        })
        .with_context(|| format!("running SQLite FTS query {query:?}"))?;
    collect_rows(rows)
}

fn query_string_column(conn: &Connection, sql: &str, value: &str) -> Result<Vec<String>> {
    let mut statement = conn.prepare(sql)?;
    collect_rows(statement.query_map([value], |row| row.get(0))?)
}

fn row_scope(row: &Row<'_>) -> rusqlite::Result<Scope> {
    Ok(Scope {
        id: row.get(0)?,
        kind: parse_enum(&row.get::<_, String>(1)?)?,
        parent_id: row.get(2)?,
        name: row.get(3)?,
        created_at: row.get(4)?,
    })
}

fn row_event(row: &Row<'_>) -> rusqlite::Result<MemoryEvent> {
    let content = parse_json_column(row, 8)?;
    let content_hash: String = row.get(9)?;
    if hash_json(&content) != content_hash {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            9,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "event content hash mismatch",
            )),
        ));
    }
    Ok(MemoryEvent {
        id: row.get(0)?,
        stream_id: row.get(1)?,
        sequence: row.get(2)?,
        scope_id: row.get(3)?,
        kind: parse_enum(&row.get::<_, String>(4)?)?,
        actor_id: row.get(5)?,
        occurred_at: row.get(6)?,
        recorded_at: row.get(7)?,
        content,
        content_hash,
        token_count: row.get(10)?,
        sensitivity: parse_enum(&row.get::<_, String>(11)?)?,
        metadata: parse_json_column(row, 12)?,
    })
}

fn row_observation(row: &Row<'_>) -> rusqlite::Result<Observation> {
    Ok(Observation {
        id: row.get(0)?,
        run_id: row.get(1)?,
        scope_id: row.get(2)?,
        kind: parse_enum(&row.get::<_, String>(3)?)?,
        content: row.get(4)?,
        importance: row.get(5)?,
        confidence: row.get(6)?,
        event_time_from: row.get(7)?,
        event_time_to: row.get(8)?,
        source_start_sequence: row.get(9)?,
        source_end_sequence: row.get(10)?,
        observer_model: row.get(11)?,
        prompt_version: row.get(12)?,
        created_at: row.get(13)?,
    })
}

fn row_claim(row: &Row<'_>) -> rusqlite::Result<Claim> {
    Ok(Claim {
        id: row.get(0)?,
        scope_id: row.get(1)?,
        kind: parse_enum(&row.get::<_, String>(2)?)?,
        subject: row.get(3)?,
        predicate: row.get(4)?,
        value: parse_json_column(row, 5)?,
        modality: parse_enum(&row.get::<_, String>(6)?)?,
        status: parse_enum(&row.get::<_, String>(7)?)?,
        authority: parse_enum(&row.get::<_, String>(8)?)?,
        confidence: row.get(9)?,
        valid_from: row.get(10)?,
        valid_to: row.get(11)?,
        expires_at: row.get(12)?,
        supersedes_id: row.get(13)?,
        created_at: row.get(14)?,
        updated_at: row.get(15)?,
    })
}

fn row_view(row: &Row<'_>) -> rusqlite::Result<MemoryView> {
    Ok(MemoryView {
        id: row.get(0)?,
        scope_id: row.get(1)?,
        kind: parse_enum(&row.get::<_, String>(2)?)?,
        generation: row.get(3)?,
        content: row.get(4)?,
        source_from_sequence: row.get(5)?,
        source_through_sequence: row.get(6)?,
        previous_view_id: row.get(7)?,
        model: row.get(8)?,
        prompt_version: row.get(9)?,
        token_count: row.get(10)?,
        created_at: row.get(11)?,
    })
}

fn row_observation_run_info(row: &Row<'_>) -> rusqlite::Result<ObservationRunInfo> {
    let ambiguities_raw: String = row.get(10)?;
    let ambiguities = serde_json::from_str(&ambiguities_raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            ambiguities_raw.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;
    let status: String = row.get(6)?;
    let source_integrity: SourceIntegrity = parse_enum(&row.get::<_, String>(7)?)?;
    let error: Option<String> = row.get(11)?;
    let next_action = match (&source_integrity, status.as_str()) {
        (SourceIntegrity::PrivacyPurged, "committed") => Some(
            "derived records depending on purged evidence were removed; append replacement evidence if needed"
                .to_owned(),
        ),
        (SourceIntegrity::PrivacyPurged, _) | (_, "stale") => {
            Some("request a new observation plan".to_owned())
        }
        (_, "failed") => Some("inspect the error and request a new observation plan".to_owned()),
        _ => None,
    };
    Ok(ObservationRunInfo {
        id: row.get(0)?,
        scope_id: row.get(1)?,
        stream_id: row.get(2)?,
        cursor_at_plan: row.get(3)?,
        from_sequence: row.get(4)?,
        to_sequence: row.get(5)?,
        status,
        source_integrity,
        observer_model: row.get(8)?,
        prompt_version: row.get(9)?,
        ambiguities,
        error,
        next_action,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

fn parse_json_column(row: &Row<'_>, index: usize) -> rusqlite::Result<Value> {
    let raw: String = row.get(index)?;
    serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            raw.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>> {
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}
