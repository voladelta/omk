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

pub const SCHEMA_VERSION: i64 = 5;

mod claim;
mod context;
mod observation;
mod privacy;
mod rows;
mod schema;
mod support;

use rows::*;
use schema::SCHEMA;
use support::*;
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
        query_scope(&self.conn, id)
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
            "INSERT INTO memory_events(id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
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
            ],
        )?;
        if stored.sensitivity == Sensitivity::Normal {
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
        query_events_range(&self.conn, stream_id, from_sequence, to_sequence)
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

    fn immediate(&mut self) -> Result<Transaction<'_>> {
        self.conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("starting SQLite write transaction")
    }
}
