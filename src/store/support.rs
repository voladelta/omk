use super::*;

#[derive(Debug)]
pub(super) struct ObservationRun {
    pub(super) scope_id: String,
    pub(super) stream_id: String,
    pub(super) cursor_at_plan: i64,
    pub(super) from_sequence: i64,
    pub(super) to_sequence: i64,
    pub(super) status: String,
    pub(super) observer_model: String,
    pub(super) prompt_version: String,
    pub(super) truncated_event_ids: Vec<String>,
    pub(super) source_integrity: String,
}

pub(super) fn query_run(conn: &Connection, id: &str) -> Result<ObservationRun> {
    conn.query_row(
        "SELECT scope_id,stream_id,cursor_at_plan,from_sequence,to_sequence,status,observer_model,prompt_version,truncated_event_ids_json,source_integrity FROM observation_runs WHERE id=?1",
        [id],
        |row| {
            let truncated_raw: String = row.get(8)?;
            Ok(ObservationRun {
                scope_id: row.get(0)?,
                stream_id: row.get(1)?,
                cursor_at_plan: row.get(2)?,
                from_sequence: row.get(3)?,
                to_sequence: row.get(4)?,
                status: row.get(5)?,
                observer_model: row.get(6)?,
                prompt_version: row.get(7)?,
                truncated_event_ids: serde_json::from_str(&truncated_raw).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        truncated_raw.len(),
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
                source_integrity: row.get(9)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| KernelError::not_found(format!("observation run {id} does not exist")).into())
}

pub(super) fn ensure_run_pending(run: &ObservationRun, id: &str) -> Result<()> {
    if run.status == "pending" {
        return Ok(());
    }
    if run.status == "stale" {
        bail!(KernelError::stale_observation_run(format!(
            "observation run {id} is stale, not pending"
        ),));
    }
    bail!(KernelError::invalid_input(format!(
        "observation run {id} is {}, not pending",
        run.status
    )))
}

pub(super) fn validate_observer_size(result: &ObserverResult) -> Result<()> {
    ensure!(
        serde_json::to_vec(result)?.len() <= MAX_OBSERVER_BYTES,
        KernelError::invalid_input("ObserverResult exceeds 1048576 serialized bytes")
    );
    Ok(())
}

pub(super) fn validate_observer_result(result: &ObserverResult) -> Result<()> {
    let continuation = &result.continuation;
    let items = result.observations.len()
        + result.claims.len()
        + result.ambiguities.len()
        + continuation.completed.len()
        + continuation.blockers.len()
        + continuation.next_actions.len()
        + continuation.unresolved_questions.len();
    ensure!(
        items <= MAX_OBSERVER_ITEMS,
        KernelError::invalid_input("ObserverResult exceeds 256 items")
    );
    for sources in result
        .observations
        .iter()
        .map(|item| &item.source_event_ids)
        .chain(result.claims.iter().map(|item| &item.source_event_ids))
        .chain(result.ambiguities.iter().map(|item| &item.source_event_ids))
    {
        ensure!(
            sources.len() <= MAX_SOURCE_IDS,
            KernelError::invalid_input("ObserverResult item exceeds 256 source IDs")
        );
    }
    if observer_result_is_completely_empty(result) {
        ensure!(
            result
                .empty_reason
                .as_deref()
                .is_some_and(|reason| !reason.trim().is_empty()),
            KernelError::invalid_input(
                "an empty ObserverResult requires a non-empty emptyReason acknowledgement",
            )
        );
    }
    if let Some(reason) = &result.empty_reason {
        ensure!(
            reason.chars().count() <= 500,
            KernelError::invalid_input("emptyReason must be at most 500 characters")
        );
    }
    for (index, observation) in result.observations.iter().enumerate() {
        ensure!(
            !observation.content.trim().is_empty(),
            KernelError::invalid_input(format!("observation {index} content is empty"))
        );
        validate_score("observation importance", observation.importance)?;
        validate_score("observation confidence", observation.confidence)?;
        ensure!(
            !observation.source_event_ids.is_empty(),
            KernelError::invalid_input(format!("observation {index} has no source events"))
        );
    }
    for (index, claim) in result.claims.iter().enumerate() {
        ensure!(
            !claim.subject.trim().is_empty(),
            KernelError::invalid_input(format!("claim {index} subject is empty"))
        );
        ensure!(
            !claim.predicate.trim().is_empty(),
            KernelError::invalid_input(format!("claim {index} predicate is empty"))
        );
        validate_score("claim confidence", claim.confidence)?;
        ensure!(
            !claim.source_event_ids.is_empty(),
            KernelError::invalid_input(format!("claim {index} has no source events"))
        );
    }
    for (index, ambiguity) in result.ambiguities.iter().enumerate() {
        ensure!(
            !ambiguity.description.trim().is_empty(),
            KernelError::invalid_input(format!("ambiguity {index} description is empty"))
        );
        ensure!(
            !ambiguity.source_event_ids.is_empty(),
            KernelError::invalid_input(format!("ambiguity {index} has no source events"))
        );
    }
    Ok(())
}

pub(super) fn observer_result_is_completely_empty(result: &ObserverResult) -> bool {
    result.observations.is_empty()
        && result.claims.is_empty()
        && result.ambiguities.is_empty()
        && result.continuation.current_task.is_none()
        && result.continuation.completed.is_empty()
        && result.continuation.blockers.is_empty()
        && result.continuation.next_actions.is_empty()
        && result.continuation.unresolved_questions.is_empty()
}

pub(super) fn validate_provenance(
    result: &ObserverResult,
    sources_by_id: &HashMap<String, MemoryEvent>,
    truncated_event_ids: &[String],
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
        let event = sources_by_id.get(event_id).ok_or_else(|| {
            KernelError::invalid_input(format!(
                "source event {event_id} is not in the observation run"
            ))
        })?;
        ensure!(
            event.sensitivity == Sensitivity::Normal,
            KernelError::invalid_input(format!(
                "redacted event {event_id} cannot source derived memory"
            ))
        );
        ensure!(
            !truncated_event_ids.contains(event_id),
            KernelError::invalid_input(format!(
                "truncated event {event_id} cannot source derived memory"
            ))
        );
    }
    Ok(())
}

/// Reject drafts that could never be confirmed because their slot already
/// uses another cardinality. Pending claims do not create slots.
pub(super) fn validate_claim_cardinalities(
    conn: &Connection,
    scope_id: &str,
    drafts: &[ClaimDraft],
) -> Result<()> {
    let mut batch: HashMap<(String, String, String), String> = HashMap::new();
    for (index, draft) in drafts.iter().enumerate() {
        let kind = enum_text(&draft.kind);
        let subject = draft.subject.trim();
        let predicate = draft.predicate.trim();
        let wanted = enum_text(&draft.cardinality);
        let existing: Option<String> = conn
            .query_row(
                "SELECT cardinality FROM claim_slots
                 WHERE scope_id=?1 AND kind=?2 AND subject=?3 AND predicate=?4",
                params![scope_id, kind, subject, predicate],
                |row| row.get(0),
            )
            .optional()?;
        // Drafts in one result must also agree with each other.
        let existing = existing.or_else(|| {
            batch.insert(
                (kind, subject.to_owned(), predicate.to_owned()),
                wanted.clone(),
            )
        });
        if let Some(existing) = existing.filter(|existing| existing != &wanted) {
            bail!(KernelError::invalid_input(format!(
                "claim {index} uses {wanted} cardinality but its slot already uses {existing} cardinality"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_score(name: &str, score: f64) -> Result<()> {
    ensure!(
        score.is_finite() && (0.0..=1.0).contains(&score),
        KernelError::invalid_input(format!("{name} must be between 0 and 1"))
    );
    Ok(())
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

/// Replace an event that cannot fit the observation budget with a stub whose
/// preview keeps as much serialized content as `available_tokens` allows.
/// Returns the cost of an empty stub when even that does not fit.
pub(super) fn truncate_event_for_budget(
    event: MemoryEvent,
    available_tokens: i64,
) -> std::result::Result<MemoryEvent, i64> {
    let serialized = event.content.to_string();
    let stub = |chars: usize| {
        let content = json!({
            "truncated": true,
            "reason": "exceeds observation budget",
            "preview": serialized.chars().take(chars).collect::<String>(),
        });
        let metadata = json!({});
        MemoryEvent {
            content_hash: hash_json(&content),
            token_count: estimate_event_tokens(&content, &metadata),
            content,
            metadata,
            ..event.clone()
        }
    };
    let cost = |chars: usize| serialized_item_tokens(&stub(chars));
    let empty_cost = cost(0);
    if empty_cost > available_tokens {
        return Err(empty_cost);
    }
    // Four characters per token bounds the preview, and cost grows with length.
    let mut low = 0;
    let mut high = serialized.chars().count().min(
        usize::try_from(available_tokens)
            .unwrap_or(usize::MAX / 4)
            .saturating_mul(4),
    );
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if cost(middle) <= available_tokens {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(stub(low))
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
            "SELECT operation,request_hash,result_json FROM memory_operations WHERE idempotency_key=?1",
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
        ),)
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
        "INSERT INTO memory_operations(idempotency_key,operation,request_hash,result_json,created_at) VALUES (?1,?2,?3,?4,?5)",
        params![key, operation, request_hash, result_json, timestamp.to_rfc3339()],
    )?;
    // Compact a bounded batch of expired results on every write, so the log
    // keeps keys and request hashes but sheds old result bodies.
    conn.execute(
        "UPDATE memory_operations SET result_json=NULL
         WHERE idempotency_key IN (
             SELECT idempotency_key FROM memory_operations
             WHERE result_json IS NOT NULL AND created_at < ?1
             ORDER BY created_at LIMIT ?2
         )",
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
        "UPDATE memory_operations SET request_hash=NULL,result_json=NULL
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
    let placeholders = std::iter::repeat_n("?", view_ids.len())
        .collect::<Vec<_>>()
        .join(",");
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

pub(super) fn set_command_event_owner(
    conn: &Connection,
    event_id: &str,
    claim_id: &str,
) -> Result<()> {
    let changed = conn.execute(
        "UPDATE memory_events
         SET metadata_json=json_set(metadata_json,'$.ownerClaimId',?2)
         WHERE id=?1 AND kind='memory-command'
           AND json_extract(metadata_json,'$.generatedBy')='omk'",
        params![event_id, claim_id],
    )?;
    ensure!(changed == 1, "generated command event owner update failed");
    Ok(())
}

pub(super) fn insert_fts(
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
    // record_id is UNINDEXED, so remember the rowid for deletes by key.
    conn.execute(
        "INSERT INTO memory_fts_refs(record_type,record_id,fts_rowid) VALUES (?1,?2,?3)",
        params![record_type, record_id, conn.last_insert_rowid()],
    )?;
    Ok(())
}

pub(super) fn delete_fts(conn: &Connection, record_type: &str, record_id: &str) -> Result<()> {
    let rowid: Option<i64> = conn
        .query_row(
            "SELECT fts_rowid FROM memory_fts_refs WHERE record_type=?1 AND record_id=?2",
            params![record_type, record_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(rowid) = rowid {
        conn.execute("DELETE FROM memory_fts WHERE rowid=?1", [rowid])?;
        conn.execute(
            "DELETE FROM memory_fts_refs WHERE record_type=?1 AND record_id=?2",
            params![record_type, record_id],
        )?;
    }
    Ok(())
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

pub(super) fn scope_is_ancestor(
    conn: &Connection,
    ancestor_id: &str,
    descendant_id: &str,
) -> Result<bool> {
    Ok(visible_scope_ids(conn, descendant_id)?
        .iter()
        .any(|scope_id| scope_id == ancestor_id))
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
        "SELECT id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json
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
    let mut statement = conn.prepare(
        "SELECT id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json
         FROM memory_events WHERE stream_id=?1 AND sequence BETWEEN ?2 AND ?3 ORDER BY sequence",
    )?;
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
        "INSERT INTO claims(id,origin_run_id,scope_id,kind,subject,predicate,cardinality,value_json,value_hash,modality,status,authority,confidence,supersedes_id,created_at,updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
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
            enum_text(&claim.authority),
            claim.confidence,
            claim.supersedes_id,
            claim.created_at,
            claim.updated_at,
        ],
    )?;
    Ok(())
}

pub(super) fn index_claim(conn: &Connection, claim: &Claim) -> Result<()> {
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
    let kind_text = enum_text(&kind);
    let previous = latest_view(conn, stream_id, &kind_text)?;
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
    Ok(view)
}

pub(super) fn latest_view(
    conn: &Connection,
    stream_id: &str,
    kind: &str,
) -> Result<Option<MemoryView>> {
    Ok(conn
        .query_row(
            "SELECT id,scope_id,stream_id,kind,generation,content,source_from_sequence,source_through_sequence,previous_view_id,model,prompt_version,token_count,created_at
             FROM memory_views WHERE stream_id=?1 AND kind=?2 ORDER BY generation DESC LIMIT 1",
            params![stream_id, kind],
            row_view,
        )
        .optional()?)
}

pub(super) fn query_claims_for_scopes(
    conn: &Connection,
    scope_ids: &[String],
    status: Option<&str>,
) -> Result<Vec<Claim>> {
    query_claim_candidates(conn, scope_ids, status, None)
}

pub(super) fn query_claim_candidates(
    conn: &Connection,
    scope_ids: &[String],
    status: Option<&str>,
    limit: Option<usize>,
) -> Result<Vec<Claim>> {
    if scope_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = std::iter::repeat_n("?", scope_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let mut sql = format!(
        "SELECT id,origin_run_id,scope_id,kind,subject,predicate,cardinality,value_json,value_hash,modality,status,authority,confidence,supersedes_id,created_at,updated_at FROM claims WHERE scope_id IN ({placeholders})"
    );
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
        "SELECT id,origin_run_id,scope_id,kind,subject,predicate,cardinality,value_json,value_hash,modality,status,authority,confidence,supersedes_id,created_at,updated_at FROM claims WHERE id=?1",
        [id],
        row_claim,
    )
    .optional()?
    .ok_or_else(|| KernelError::not_found(format!("claim {id} does not exist")).into())
}

pub(super) fn query_active_claim_member(
    conn: &Connection,
    scope_id: &str,
    kind: &str,
    subject: &str,
    predicate: &str,
    cardinality: &ClaimCardinality,
    value_hash: &str,
) -> Result<Option<Claim>> {
    Ok(conn
        .query_row(
            "SELECT id,origin_run_id,scope_id,kind,subject,predicate,cardinality,value_json,value_hash,modality,status,authority,confidence,supersedes_id,created_at,updated_at
             FROM claims WHERE scope_id=?1 AND kind=?2 AND subject=?3 AND predicate=?4
               AND cardinality=?5 AND (cardinality='single' OR value_hash=?6) AND status='active'
             ORDER BY updated_at DESC LIMIT 1",
            params![scope_id, kind, subject, predicate, enum_text(cardinality), value_hash],
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

pub(super) fn ensure_claim_slot(conn: &Connection, claim: &Claim) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO claim_slots(scope_id,kind,subject,predicate,cardinality)
         VALUES (?1,?2,?3,?4,?5)",
        params![
            claim.scope_id,
            enum_text(&claim.kind),
            claim.subject,
            claim.predicate,
            enum_text(&claim.cardinality),
        ],
    )?;
    let cardinality: String = conn.query_row(
        "SELECT cardinality FROM claim_slots
         WHERE scope_id=?1 AND kind=?2 AND subject=?3 AND predicate=?4",
        params![
            claim.scope_id,
            enum_text(&claim.kind),
            claim.subject,
            claim.predicate,
        ],
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
    let scope_placeholders = std::iter::repeat_n("?", scope_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let view_placeholders = std::iter::repeat_n("?", represented_view_ids.len())
        .collect::<Vec<_>>()
        .join(",");
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
         SELECT id,run_id,scope_id,kind,content,importance,confidence,event_time_from,event_time_to,source_start_sequence,source_end_sequence,observer_model,prompt_version,created_at
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
    record_type: &str,
    id: &str,
) -> Result<Vec<String>> {
    let (table, column) = match record_type {
        "observation" => ("observation_sources", "observation_id"),
        "claim" => ("claim_sources", "claim_id"),
        _ => bail!("unsupported full-text record type"),
    };
    query_string_column(
        conn,
        &format!("SELECT event_id FROM {table} WHERE {column}=?1 ORDER BY event_id LIMIT 257"),
        id,
    )
}

pub(super) fn estimate_claim_tokens(claim: &Claim) -> i64 {
    serialized_item_tokens(claim)
}

// One extra character covers the array separator; rounding per item is conservative.
pub(super) fn serialized_item_tokens(value: &impl Serialize) -> i64 {
    estimate_tokens(&(serde_json::to_string(value).expect("model value serializes") + ","))
}

pub(super) fn event_hint_extra(event: &MemoryEvent) -> i64 {
    (event.token_count - estimate_event_tokens(&event.content, &event.metadata)).max(0)
}

pub(super) fn view_hint_extra(view: &MemoryView) -> i64 {
    (view.token_count - estimate_tokens(&view.content)).max(0)
}

pub(super) fn sort_claims_by_scope(claims: &mut [Claim], scope_order: &[String]) {
    claims.sort_by_key(|claim| {
        scope_order
            .iter()
            .position(|scope_id| scope_id == &claim.scope_id)
            .unwrap_or(usize::MAX)
    });
}

/// Split active claims into those in force and those shadowed by a deeper
/// scope. A `single` claim loses to any claim for the same logical key in a
/// descendant scope; `set` claims are unioned and never shadowed.
pub(super) fn split_shadowed_claims(
    claims: Vec<Claim>,
    scope_order: &[String],
) -> (Vec<Claim>, Vec<Claim>) {
    let depth = |claim: &Claim| {
        scope_order
            .iter()
            .position(|scope_id| scope_id == &claim.scope_id)
            .unwrap_or(0)
    };
    let key = |claim: &Claim| {
        (
            enum_text(&claim.kind),
            claim.subject.clone(),
            claim.predicate.clone(),
        )
    };
    let mut deepest: HashMap<(String, String, String), usize> = HashMap::new();
    for claim in &claims {
        let entry = deepest.entry(key(claim)).or_insert(0);
        *entry = (*entry).max(depth(claim));
    }
    claims.into_iter().partition(|claim| {
        claim.cardinality != ClaimCardinality::Single || depth(claim) >= deepest[&key(claim)]
    })
}

/// Active claims may use at most this share of a context or plan budget.
pub(super) const CLAIM_BUDGET_PERCENT: i64 = 50;
/// User-scope claims are pinned first, up to this share of the claim budget.
pub(super) const USER_CLAIM_PIN_PERCENT: i64 = 40;

pub(super) fn percent_of(total: i64, percent: i64) -> i64 {
    (i128::from(total) * i128::from(percent) / 100) as i64
}

/// Choose the active claims that fit `budget` tokens. User-scope claims go
/// first, newest update first, up to the pinned share; every other claim and
/// any user claim left over then fill the rest, newest update first. Both
/// returned lists keep the input order.
pub(super) fn budget_claims(
    conn: &Connection,
    claims: Vec<Claim>,
    budget: i64,
    cost: impl Fn(&Claim) -> i64,
) -> Result<(Vec<Claim>, Vec<Claim>)> {
    let scope_ids: Vec<&str> = claims.iter().map(|claim| claim.scope_id.as_str()).collect();
    let user_scopes: HashSet<String> = {
        let mut statement = conn.prepare(
            "SELECT id FROM memory_scopes WHERE kind='user' AND id IN (SELECT value FROM json_each(?1))",
        )?;
        collect_rows(statement.query_map([serde_json::to_string(&scope_ids)?], |row| row.get(0))?)?
            .into_iter()
            .collect()
    };
    let costs: Vec<i64> = claims.iter().map(&cost).collect();
    let mut newest_first: Vec<usize> = (0..claims.len()).collect();
    newest_first.sort_by(|&left, &right| {
        claims[right]
            .updated_at
            .cmp(&claims[left].updated_at)
            .then_with(|| claims[left].id.cmp(&claims[right].id))
    });
    let mut kept = vec![false; claims.len()];
    let mut used = 0;
    let pin_budget = percent_of(budget, USER_CLAIM_PIN_PERCENT);
    for &index in &newest_first {
        if user_scopes.contains(&claims[index].scope_id) && costs[index] <= pin_budget - used {
            used += costs[index];
            kept[index] = true;
        }
    }
    for &index in &newest_first {
        if !kept[index] && costs[index] <= budget - used {
            used += costs[index];
            kept[index] = true;
        }
    }
    let (kept_claims, omitted): (Vec<_>, Vec<_>) =
        claims.into_iter().zip(kept).partition(|(_, kept)| *kept);
    Ok((
        kept_claims.into_iter().map(|(claim, _)| claim).collect(),
        omitted.into_iter().map(|(claim, _)| claim).collect(),
    ))
}

pub(super) fn search_fts(
    conn: &Connection,
    scope_ids: &[String],
    query: &str,
    limit: usize,
    current_only: bool,
) -> Result<Vec<SearchHit>> {
    if scope_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = std::iter::repeat_n("?", scope_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT record_type,record_id,memory_fts.scope_id,substr(text,1,512),rank,claims.status FROM memory_fts
         LEFT JOIN claims ON record_type='claim' AND claims.id=record_id
         WHERE memory_fts MATCH ? AND memory_fts.scope_id IN ({placeholders})
           AND (?=0 OR record_type!='claim' OR claims.status='active')
         ORDER BY rank,record_type,record_id LIMIT ?"
    );
    let mut values = Vec::with_capacity(scope_ids.len() + 3);
    values.push(rusqlite::types::Value::Text(query.to_owned()));
    values.extend(scope_ids.iter().cloned().map(rusqlite::types::Value::Text));
    values.push(rusqlite::types::Value::Integer(i64::from(current_only)));
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
                claim_status: row
                    .get::<_, Option<String>>(5)?
                    .map(|value| parse_enum(&value))
                    .transpose()?,
            })
        })
        .map_err(|error| {
            KernelError::invalid_search_query(format!(
                "running SQLite FTS query {query:?}: {error}"
            ))
        })?;
    collect_rows(rows)
}

pub(super) fn bounded_fts_query(query: &str, mode: SearchMode) -> Result<String> {
    ensure!(
        !query.trim().is_empty() && query.len() <= 4096 && query.split_whitespace().count() <= 64,
        KernelError::invalid_search_query(
            "search query must contain 1 to 4096 bytes and at most 64 whitespace terms"
        )
    );
    Ok(match mode {
        SearchMode::Phrase => literal_fts_query(query),
        SearchMode::Terms => query
            .split_whitespace()
            .map(literal_fts_query)
            .collect::<Vec<_>>()
            .join(" AND "),
        SearchMode::Advanced => query.to_owned(),
    })
}

pub(super) fn query_string_column(
    conn: &Connection,
    sql: &str,
    value: &str,
) -> Result<Vec<String>> {
    let mut statement = conn.prepare(sql)?;
    collect_rows(statement.query_map([value], |row| row.get(0))?)
}
