use super::*;

#[derive(Debug)]
pub(super) struct ObservationRun {
    pub(super) scope_id: String,
    pub(super) stream_id: String,
    pub(super) cursor_at_plan: i64,
    pub(super) from_sequence: i64,
    pub(super) to_sequence: i64,
    pub(super) status: RunStatus,
    pub(super) observer_model: String,
    pub(super) prompt_version: String,
    pub(super) truncated_event_ids: Vec<String>,
    pub(super) source_integrity: SourceIntegrity,
}

pub(super) fn query_run(conn: &Connection, id: &str) -> Result<ObservationRun> {
    conn.query_row(
        "SELECT scope_id,stream_id,cursor_at_plan,from_sequence,to_sequence,status,observer_model,prompt_version,truncated_event_ids_json,source_integrity FROM observation_runs WHERE id=?1",
        [id],
        |row| {
            Ok(ObservationRun {
                scope_id: row.get(0)?,
                stream_id: row.get(1)?,
                cursor_at_plan: row.get(2)?,
                from_sequence: row.get(3)?,
                to_sequence: row.get(4)?,
                status: parse_enum(&row.get::<_, String>(5)?)?,
                observer_model: row.get(6)?,
                prompt_version: row.get(7)?,
                truncated_event_ids: parse_json_column(row, 8)?,
                source_integrity: parse_enum(&row.get::<_, String>(9)?)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| KernelError::not_found(format!("observation run {id} does not exist")).into())
}

pub(super) fn ensure_run_pending(run: &ObservationRun, id: &str) -> Result<()> {
    match run.status {
        RunStatus::Pending => Ok(()),
        RunStatus::Stale => bail!(KernelError::stale_observation_run(format!(
            "observation run {id} is stale, not pending"
        ))),
        status => bail!(KernelError::invalid_input(format!(
            "observation run {id} is {}, not pending",
            enum_text(&status)
        ))),
    }
}

pub(super) fn redact_for_agent(mut event: MemoryEvent) -> MemoryEvent {
    if event.sensitivity == Sensitivity::Secret {
        event.content = json!({"redacted": true, "reason": "secret"});
        event.metadata = json!({});
        event.content_hash = hash_json(&event.content);
        event.token_count = estimate_event_tokens(&event.content, &event.metadata);
    }
    event
}

/// Replace an event that cannot fit the observation budget with a stub that
/// holds none of its content: an observer shown part of an event acts on half
/// of it. `eventTokens` is the whole event's cost, so a caller knows the budget
/// that would include it.
pub(super) fn stub_event_for_budget(event: MemoryEvent) -> MemoryEvent {
    let content = json!({
        "truncated": true,
        "reason": "exceeds observation budget",
        "eventTokens": serialized_item_tokens(&event).saturating_add(event_hint_extra(&event)),
    });
    let metadata = json!({});
    MemoryEvent {
        content_hash: hash_json(&content),
        token_count: estimate_event_tokens(&content, &metadata),
        content,
        metadata,
        ..event
    }
}

pub(super) struct ResolvedReadAccess<'a> {
    anchor_scope_id: &'a str,
    visible: HashSet<String>,
    reveal_secrets: bool,
}

impl<'a> ResolvedReadAccess<'a> {
    pub(super) fn resolve(conn: &Connection, access: &'a ReadAccess) -> Result<Self> {
        Ok(Self {
            anchor_scope_id: &access.anchor_scope_id,
            visible: retrieval_scope_ids(conn, &access.anchor_scope_id)?
                .into_iter()
                .collect(),
            reveal_secrets: access.reveal_secrets,
        })
    }

    pub(super) fn ensure_scope(&self, scope_id: &str) -> Result<()> {
        ensure!(
            self.visible.contains(scope_id),
            KernelError::scope_violation(format!(
                "record is not visible from scope {}",
                self.anchor_scope_id
            ))
        );
        Ok(())
    }

    pub(super) fn apply(&self, event: MemoryEvent) -> Result<MemoryEvent> {
        self.ensure_scope(&event.scope_id)?;
        Ok(if self.reveal_secrets {
            event
        } else {
            redact_for_agent(event)
        })
    }
}

pub(super) fn ensure_read_scope(
    conn: &Connection,
    access: &ReadAccess,
    record_scope_id: &str,
) -> Result<()> {
    ResolvedReadAccess::resolve(conn, access)?.ensure_scope(record_scope_id)
}

pub(super) fn now() -> String {
    Utc::now().to_rfc3339()
}

pub(super) fn validate_nonempty(name: &str, value: &str) -> Result<()> {
    ensure!(
        !value.trim().is_empty(),
        KernelError::invalid_input(format!("{name} cannot be empty"))
    );
    Ok(())
}

pub(super) fn estimate_tokens(text: &str) -> i64 {
    ((text.chars().count() as i64 + 3) / 4).max(1)
}

pub(super) fn estimate_event_tokens(content: &Value, metadata: &Value) -> i64 {
    estimate_tokens(&format!("{content} {metadata}"))
}

pub(super) fn hash_json(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(super) fn searchable_json(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

pub(super) fn ensure_scope_exists(conn: &Connection, id: &str) -> Result<()> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM memory_scopes WHERE id=?1)",
        [id],
        |row| row.get(0),
    )?;
    ensure!(
        exists,
        KernelError::not_found(format!("scope {id} does not exist"))
    );
    Ok(())
}

/// Saved results older than this are compacted to their key and request hash.
pub(super) const RESULT_RETENTION_DAYS: i64 = 30;
/// Most expired results one write compacts, which bounds the work per write.
const COMPACTION_BATCH: i64 = 64;

pub(super) fn prior_result<T: DeserializeOwned>(
    conn: &Connection,
    key: &str,
    expected_operation: &str,
    expected_request_hash: &str,
) -> Result<Option<T>> {
    let prior: Option<(String, Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT operation.operation,operation.request_hash,result.result_json
             FROM memory_operations operation
             LEFT JOIN memory_operation_results result
               ON result.idempotency_key=operation.idempotency_key
             WHERE operation.idempotency_key=?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((operation, request_hash, result_json)) = prior else {
        return Ok(None);
    };
    ensure!(
        operation == expected_operation,
        KernelError::idempotency_conflict(format!(
            "idempotency key was already used for {operation}, not {expected_operation}"
        ))
    );
    let request_hash = request_hash.ok_or_else(|| {
        KernelError::privacy_purged("the prior request for this idempotency key was privacy-purged")
    })?;
    ensure!(
        request_hash == expected_request_hash,
        KernelError::idempotency_conflict(
            "idempotency conflict: this key was already used with different request input",
        )
    );
    // A compacted operation keeps its request hash, so the key still blocks
    // a duplicate write even though the result can no longer be replayed.
    let result_json = result_json.ok_or_else(|| {
        KernelError::operation_expired(format!(
            "the result for this idempotency key is older than {RESULT_RETENTION_DAYS} days and was compacted"
        ))
    })?;
    Ok(Some(serde_json::from_str(&result_json).with_context(
        || format!("reading stored result for idempotency key {key}"),
    )?))
}

pub(super) fn save_operation<T: Serialize + ?Sized>(
    conn: &Connection,
    key: &str,
    operation: &str,
    request_hash: &str,
    result: &T,
) -> Result<()> {
    let result_json = serde_json::to_string(result)?;
    let timestamp = Utc::now();
    conn.execute(
        "INSERT INTO memory_operations(idempotency_key,operation,request_hash) VALUES (?1,?2,?3)",
        params![key, operation, request_hash],
    )?;
    conn.execute(
        "INSERT INTO memory_operation_results(idempotency_key,result_json,created_at) VALUES (?1,?2,?3)",
        params![key, result_json, timestamp.to_rfc3339()],
    )?;
    // Compact on every write: look only at the oldest batch of results (they
    // come first in commit order) and delete those past retention. The log
    // keeps keys and request hashes but sheds old result bodies.
    conn.execute(
        "DELETE FROM memory_operation_results
         WHERE id IN (SELECT id FROM memory_operation_results ORDER BY id LIMIT ?2)
           AND created_at < ?1",
        params![
            (timestamp - chrono::Duration::days(RESULT_RETENTION_DAYS)).to_rfc3339(),
            COMPACTION_BATCH
        ],
    )?;
    // Index every record ID in the result so a purge can find the operations
    // to tombstone without scanning them all.
    let mut record_ids = HashSet::new();
    collect_uuid_strings(&serde_json::from_str(&result_json)?, &mut record_ids);
    let mut insert = conn.prepare_cached(
        "INSERT OR IGNORE INTO memory_operation_refs(record_id,idempotency_key) VALUES (?1,?2)",
    )?;
    for record_id in record_ids {
        insert.execute(params![record_id, key])?;
    }
    Ok(())
}

fn collect_uuid_strings(value: &Value, found: &mut HashSet<String>) {
    match value {
        Value::String(text) => {
            if Uuid::parse_str(text).is_ok() {
                found.insert(text.clone());
            }
        }
        Value::Array(items) => items
            .iter()
            .for_each(|item| collect_uuid_strings(item, found)),
        Value::Object(fields) => fields
            .values()
            .for_each(|item| collect_uuid_strings(item, found)),
        _ => {}
    }
}

pub(super) fn operation_request_hash(operation: &str, request: &impl Serialize) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(operation.as_bytes());
    hasher.update([0]);
    hasher.update(serde_json::to_vec(request)?);
    Ok(format!("{:x}", hasher.finalize()))
}

pub(super) fn scrub_operations_referencing(conn: &Connection, record_ids: &[&str]) -> Result<()> {
    if record_ids.is_empty() {
        return Ok(());
    }
    let record_ids = serde_json::to_string(record_ids)?;
    conn.execute(
        "UPDATE memory_operations SET request_hash=NULL
         WHERE idempotency_key IN (
             SELECT idempotency_key FROM memory_operation_refs
             WHERE record_id IN (SELECT value FROM json_each(?1))
         )",
        [&record_ids],
    )?;
    conn.execute(
        "DELETE FROM memory_operation_results
         WHERE idempotency_key IN (
             SELECT idempotency_key FROM memory_operation_refs
             WHERE record_id IN (SELECT value FROM json_each(?1))
         )",
        [&record_ids],
    )?;
    // A tombstone keeps no result, so it keeps no record IDs either.
    conn.execute(
        "DELETE FROM memory_operation_refs
         WHERE idempotency_key IN (
             SELECT idempotency_key FROM memory_operation_refs
             WHERE record_id IN (SELECT value FROM json_each(?1))
         )",
        [&record_ids],
    )?;
    Ok(())
}

pub(super) fn view_successor_ids(conn: &Connection, view_ids: &[String]) -> Result<Vec<String>> {
    if view_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = placeholders(view_ids.len());
    let sql = format!(
        "WITH RECURSIVE successors(id) AS (
            SELECT id FROM memory_views WHERE id IN ({placeholders})
            UNION
            SELECT view.id
            FROM memory_views view
            JOIN successors parent ON view.previous_view_id=parent.id
         ) SELECT id FROM successors"
    );
    let mut statement = conn.prepare(&sql)?;
    collect_rows(statement.query_map(rusqlite::params_from_iter(view_ids), |row| row.get(0))?)
}

pub(super) fn generated_command_source_ids(
    conn: &Connection,
    claim_id: &str,
) -> Result<Vec<String>> {
    let mut statement = conn.prepare(
        "SELECT e.id
         FROM memory_events e
         JOIN claim_sources source ON source.event_id=e.id
         WHERE source.claim_id=?1
           AND e.kind='memory-command'
           AND json_extract(e.metadata_json,'$.generatedBy')='omk'
           AND json_extract(e.metadata_json,'$.ownerClaimId')=?1",
    )?;
    collect_rows(statement.query_map([claim_id], |row| row.get(0))?)
}

/// Make a generated command event a source of the claim it wrote, and mark
/// that claim as its owner so purging the claim purges the event too.
pub(super) fn attach_command_event(
    conn: &Connection,
    command: &MemoryEvent,
    claim_id: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE memory_events
         SET metadata_json=json_set(metadata_json,'$.ownerClaimId',?2)
         WHERE id=?1",
        params![command.id, claim_id],
    )?;
    attach_event_sources(conn, claim_id, std::slice::from_ref(&command.id))
}

pub(super) fn query_scope(conn: &Connection, id: &str) -> Result<Scope> {
    conn.query_row(
        "SELECT id,kind,parent_id,name,created_at FROM memory_scopes WHERE id=?1",
        [id],
        row_scope,
    )
    .optional()?
    .ok_or_else(|| KernelError::not_found(format!("scope {id} does not exist")).into())
}

pub(super) fn visible_scope_ids(conn: &Connection, scope_id: &str) -> Result<Vec<String>> {
    let mut current = Some(scope_id.to_owned());
    let mut result = Vec::new();
    while let Some(id) = current {
        let parent: Option<Option<String>> = conn
            .query_row(
                "SELECT parent_id FROM memory_scopes WHERE id=?1",
                [&id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(parent) = parent else {
            bail!(KernelError::not_found(format!("scope {id} does not exist")));
        };
        result.push(id);
        current = parent;
    }
    result.reverse();
    Ok(result)
}

pub(super) fn retrieval_scope_ids(conn: &Connection, scope_id: &str) -> Result<Vec<String>> {
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

pub(super) fn literal_fts_query(query: &str) -> String {
    format!("\"{}\"", query.replace('"', "\"\""))
}

pub(super) fn query_event_page(
    conn: &Connection,
    stream_id: &str,
    cursor: i64,
    reverse: bool,
) -> Result<Vec<MemoryEvent>> {
    let (comparison, order) = if reverse { ("<", "DESC") } else { (">", "ASC") };
    let mut statement = conn.prepare(&format!(
        "SELECT {EVENT_COLUMNS}
         FROM memory_events WHERE stream_id=?1 AND sequence{comparison}?2 ORDER BY sequence {order} LIMIT 32"
    ))?;
    collect_rows(statement.query_map(params![stream_id, cursor], row_event)?)
}

pub(super) fn query_events_range(
    conn: &Connection,
    stream_id: &str,
    from_sequence: i64,
    to_sequence: i64,
) -> Result<Vec<MemoryEvent>> {
    let mut statement = conn.prepare(&format!(
        "SELECT {EVENT_COLUMNS}
         FROM memory_events WHERE stream_id=?1 AND sequence BETWEEN ?2 AND ?3 ORDER BY sequence"
    ))?;
    collect_rows(statement.query_map(params![stream_id, from_sequence, to_sequence], row_event)?)
}

pub(super) fn insert_observation(conn: &Connection, observation: &Observation) -> Result<()> {
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

pub(super) fn insert_memory_command_event(
    conn: &Connection,
    scope_id: &str,
    operation: &str,
    request: &Value,
) -> Result<MemoryEvent> {
    let stream_id = format!("memory-commands:{scope_id}");
    let content = json!({"operation": operation, "request": request});
    let token_count = estimate_tokens(&content.to_string());
    insert_event(
        conn,
        EventInsert {
            scope_id: scope_id.to_owned(),
            stream_id,
            kind: EventKind::MemoryCommand,
            actor_id: Some("explicit-user".to_owned()),
            occurred_at: None,
            content,
            token_count,
            sensitivity: Sensitivity::Normal,
            metadata: json!({"generatedBy": "omk"}),
        },
    )
}

pub(super) fn insert_claim(conn: &Connection, claim: &Claim) -> Result<()> {
    conn.execute(
        "INSERT INTO claims(id,origin_run_id,scope_id,kind,subject,predicate,cardinality,value_json,value_hash,modality,status,confidence,supersedes_id,created_at,updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            claim.id,
            claim.origin_run_id,
            claim.scope_id,
            enum_text(&claim.kind),
            claim.subject,
            claim.predicate,
            enum_text(&claim.cardinality),
            claim.value.to_string(),
            claim.value_hash,
            enum_text(&claim.modality),
            enum_text(&claim.status),
            claim.confidence,
            claim.supersedes_id,
            claim.created_at,
            claim.updated_at,
        ],
    )?;
    Ok(())
}

pub(super) fn index_claim(conn: &Connection, claim: &Claim) -> Result<()> {
    let value = searchable_json(&claim.value);
    insert_fts(
        conn,
        &FtsRow {
            record_type: RecordType::Claim,
            record_id: &claim.id,
            scope_id: &claim.scope_id,
            kind: &enum_text(&claim.kind),
            text: &format!("{} {} {}", claim.subject, claim.predicate, value),
            subject: &claim.subject,
            predicate: &claim.predicate,
            value: &value,
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn insert_next_view(
    conn: &Connection,
    scope_id: &str,
    stream_id: &str,
    kind: ViewKind,
    content: &str,
    source_from_sequence: i64,
    source_through_sequence: i64,
    model: Option<&str>,
    prompt_version: Option<&str>,
    token_count: i64,
) -> Result<MemoryView> {
    let previous = latest_view(conn, stream_id, kind)?;
    let view = MemoryView {
        id: Uuid::new_v4().to_string(),
        scope_id: scope_id.to_owned(),
        stream_id: stream_id.to_owned(),
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
        "INSERT INTO memory_views(id,scope_id,stream_id,kind,generation,content,source_from_sequence,source_through_sequence,previous_view_id,model,prompt_version,token_count,created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            view.id,
            view.scope_id,
            view.stream_id,
            enum_text(&kind),
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
    Ok(view)
}

pub(super) fn latest_view(
    conn: &Connection,
    stream_id: &str,
    kind: ViewKind,
) -> Result<Option<MemoryView>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {VIEW_COLUMNS}
                 FROM memory_views WHERE stream_id=?1 AND kind=?2 ORDER BY generation DESC LIMIT 1"
            ),
            params![stream_id, enum_text(&kind)],
            row_view,
        )
        .optional()?)
}

pub(super) fn query_claims_for_scopes(
    conn: &Connection,
    scope_ids: &[String],
    status: Option<&str>,
    limit: Option<usize>,
) -> Result<Vec<Claim>> {
    if scope_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = placeholders(scope_ids.len());
    let mut sql = format!("SELECT {CLAIM_COLUMNS} FROM claims WHERE scope_id IN ({placeholders})");
    let mut values = scope_ids.to_vec();
    if let Some(status) = status {
        sql.push_str(" AND status=?");
        values.push(status.to_owned());
    }
    // A limited read keeps the newest candidates; callers reorder for display.
    if let Some(limit) = limit {
        sql.push_str(&format!(" ORDER BY created_at DESC,id LIMIT {limit}"));
    } else {
        sql.push_str(" ORDER BY created_at,id");
    }
    let mut statement = conn.prepare(&sql)?;
    collect_rows(statement.query_map(rusqlite::params_from_iter(values), row_claim)?)
}

pub(super) fn query_claim(conn: &Connection, id: &str) -> Result<Claim> {
    conn.query_row(
        &format!("SELECT {CLAIM_COLUMNS} FROM claims WHERE id=?1"),
        [id],
        row_claim,
    )
    .optional()?
    .ok_or_else(|| KernelError::not_found(format!("claim {id} does not exist")).into())
}

/// The active claim in `claim`'s slot: any value for `single`, the same value
/// for `set`.
pub(super) fn query_active_claim_member(conn: &Connection, claim: &Claim) -> Result<Option<Claim>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {CLAIM_COLUMNS}
                 FROM claims WHERE scope_id=?1 AND subject=?2 AND predicate=?3
                   AND cardinality=?4 AND (cardinality='single' OR value_hash=?5) AND status='active'
                 ORDER BY updated_at DESC LIMIT 1"
            ),
            params![
                claim.scope_id,
                claim.subject,
                claim.predicate,
                enum_text(&claim.cardinality),
                claim.value_hash
            ],
            row_claim,
        )
        .optional()?)
}

pub(super) fn supersede_other_active_claims(
    conn: &Connection,
    claim: &Claim,
    excluded_id: Option<&str>,
) -> Result<()> {
    if claim.cardinality == ClaimCardinality::Set {
        return Ok(());
    }
    conn.execute(
        "UPDATE claims SET status='superseded',updated_at=?1
         WHERE scope_id=?2 AND subject=?3 AND predicate=?4 AND status='active'
           AND (?5 IS NULL OR id != ?5)",
        params![
            now(),
            claim.scope_id,
            claim.subject,
            claim.predicate,
            excluded_id,
        ],
    )?;
    Ok(())
}

pub(super) fn ensure_claim_slot(conn: &Connection, claim: &Claim) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO claim_slots(scope_id,subject,predicate,cardinality)
         VALUES (?1,?2,?3,?4)",
        params![
            claim.scope_id,
            claim.subject,
            claim.predicate,
            enum_text(&claim.cardinality),
        ],
    )?;
    let cardinality: String = conn.query_row(
        "SELECT cardinality FROM claim_slots
         WHERE scope_id=?1 AND subject=?2 AND predicate=?3",
        params![claim.scope_id, claim.subject, claim.predicate,],
        |row| row.get(0),
    )?;
    ensure!(
        cardinality == enum_text(&claim.cardinality),
        KernelError::invalid_input(format!("claim slot already uses {cardinality} cardinality"))
    );
    Ok(())
}

pub(super) fn validate_claim_event_sources(
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
            .ok_or_else(|| {
                KernelError::not_found(format!("source event {event_id} does not exist"))
            })?;
        ensure!(
            visible.contains(&event_scope),
            KernelError::scope_violation(format!(
                "source event {event_id} from scope {event_scope} is not visible to scope {scope_id}"
            ))
        );
        ensure!(
            sensitivity == "normal",
            KernelError::invalid_input(format!("redacted event {event_id} cannot source a claim"))
        );
    }
    Ok(())
}

pub(super) fn validate_existing_claim_sources_visible(
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
             WHERE source.claim_id=?1",
        )?;
        collect_rows(statement.query_map([claim_id], |row| row.get::<_, String>(0))?)?
    };
    for source_scope in source_scopes {
        ensure!(
            visible.contains(&source_scope),
            KernelError::scope_violation(format!(
                "claim {claim_id} has source evidence in scope {source_scope}, which is not visible from scope {new_scope_id}"
            ))
        );
    }
    Ok(())
}

pub(super) fn attach_event_sources(
    conn: &Connection,
    claim_id: &str,
    event_ids: &[String],
) -> Result<()> {
    for event_id in event_ids {
        conn.execute(
            "INSERT OR IGNORE INTO claim_sources(claim_id,event_id) VALUES (?1,?2)",
            params![claim_id, event_id],
        )?;
    }
    Ok(())
}

pub(super) fn copy_claim_sources(
    conn: &Connection,
    from_claim_id: &str,
    to_claim_id: &str,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO claim_sources(claim_id,event_id)
         SELECT ?2,event_id FROM claim_sources WHERE claim_id=?1",
        params![from_claim_id, to_claim_id],
    )?;
    Ok(())
}

pub(super) fn query_observations_for_scopes(
    conn: &Connection,
    scope_ids: &[String],
    represented_view_ids: &[String],
) -> Result<Vec<Observation>> {
    if scope_ids.is_empty() {
        return Ok(Vec::new());
    }
    let scope_placeholders = placeholders(scope_ids.len());
    let view_placeholders = placeholders(represented_view_ids.len());
    // Observations represented by the selected view chain are excluded before
    // LIMIT so reflected history cannot crowd out newer observations.
    let sql = format!(
        "WITH RECURSIVE view_chain(id) AS (
            SELECT id FROM memory_views WHERE id IN ({view_placeholders})
            UNION
            SELECT view.previous_view_id
            FROM memory_views view
            JOIN view_chain current ON current.id=view.id
            WHERE view.previous_view_id IS NOT NULL
         )
         SELECT {OBSERVATION_COLUMNS}
         FROM observations
         WHERE scope_id IN ({scope_placeholders})
           AND NOT EXISTS (
               SELECT 1 FROM view_sources source
               JOIN view_chain ON view_chain.id=source.view_id
               WHERE source.observation_id=observations.id
           )
         ORDER BY created_at DESC,id LIMIT 257"
    );
    let mut statement = conn.prepare(&sql)?;
    collect_rows(statement.query_map(
        rusqlite::params_from_iter(represented_view_ids.iter().chain(scope_ids)),
        row_observation,
    )?)
}

pub(super) fn observation_has_events(
    conn: &Connection,
    observation_id: &str,
    event_ids_json: &str,
) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM json_each(?2) event
         JOIN observation_sources source ON source.event_id=event.value
         WHERE source.observation_id=?1)",
        params![observation_id, event_ids_json],
        |row| row.get(0),
    )?)
}

pub(super) fn context_source_ids(
    conn: &Connection,
    record_type: RecordType,
    id: &str,
) -> Result<Vec<String>> {
    let (table, column) = match record_type {
        RecordType::Observation => ("observation_sources", "observation_id"),
        RecordType::Claim => ("claim_sources", "claim_id"),
        RecordType::Event => return Ok(vec![id.to_owned()]),
    };
    query_strings(
        conn,
        &format!("SELECT event_id FROM {table} WHERE {column}=?1 ORDER BY event_id LIMIT 257"),
        [id],
    )
}

pub(super) fn query_strings(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Vec<String>> {
    let mut statement = conn.prepare(sql)?;
    collect_rows(statement.query_map(params, |row| row.get(0))?)
}

pub(super) fn placeholders(count: usize) -> String {
    vec!["?"; count].join(",")
}

pub(super) fn query_stream_scope(conn: &Connection, stream_id: &str) -> Result<String> {
    conn.query_row(
        "SELECT scope_id FROM memory_streams WHERE id=?1",
        [stream_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| KernelError::not_found(format!("stream {stream_id} does not exist")).into())
}
