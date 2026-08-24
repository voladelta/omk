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
}

pub(super) fn query_run(conn: &Connection, id: &str) -> Result<ObservationRun> {
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

pub(super) fn validate_observer_result(result: &ObserverResult) -> Result<()> {
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
            event.sensitivity == Sensitivity::Normal,
            "redacted event {event_id} cannot source derived memory"
        );
    }
    Ok(())
}

pub(super) fn validate_score(name: &str, score: f64) -> Result<()> {
    ensure!(
        score.is_finite() && (0.0..=1.0).contains(&score),
        "{name} must be between 0 and 1"
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

pub(super) fn apply_read_access(
    conn: &Connection,
    access: &ReadAccess,
    event: MemoryEvent,
) -> Result<MemoryEvent> {
    let visible = retrieval_scope_ids(conn, &access.anchor_scope_id)?;
    ensure!(
        visible.contains(&event.scope_id),
        "record is not visible from scope {}",
        access.anchor_scope_id
    );
    Ok(if access.reveal_secrets {
        event
    } else {
        redact_for_agent(event)
    })
}

pub(super) fn ensure_read_scope(
    conn: &Connection,
    access: &ReadAccess,
    record_scope_id: &str,
) -> Result<()> {
    ensure!(
        retrieval_scope_ids(conn, &access.anchor_scope_id)?.contains(&record_scope_id.to_owned()),
        "record is not visible from scope {}",
        access.anchor_scope_id
    );
    Ok(())
}

pub(super) fn now() -> String {
    Utc::now().to_rfc3339()
}

pub(super) fn validate_nonempty(name: &str, value: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{name} cannot be empty");
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
    ensure!(exists, "scope {id} does not exist");
    Ok(())
}

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
        "idempotency key was already used for {operation}, not {expected_operation}"
    );
    let result_json = result_json
        .ok_or_else(|| anyhow!("the prior result for this idempotency key was privacy-purged"))?;
    let request_hash = request_hash
        .ok_or_else(|| anyhow!("the prior request for this idempotency key was privacy-purged"))?;
    ensure!(
        request_hash == expected_request_hash,
        "idempotency conflict: this key was already used with different request input"
    );
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
    conn.execute(
        "INSERT INTO memory_operations(idempotency_key,operation,request_hash,result_json) VALUES (?1,?2,?3,?4)",
        params![key, operation, request_hash, serde_json::to_string(result)?],
    )?;
    Ok(())
}

pub(super) fn operation_request_hash(operation: &str, request: &impl Serialize) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(operation.as_bytes());
    hasher.update([0]);
    hasher.update(serde_json::to_vec(request)?);
    Ok(format!("{:x}", hasher.finalize()))
}

pub(super) fn scrub_operations_referencing(conn: &Connection, record_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE memory_operations SET request_hash=NULL,result_json=NULL WHERE instr(result_json,?1)>0",
        [record_id],
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
    Ok(())
}

pub(super) fn query_scope(conn: &Connection, id: &str) -> Result<Scope> {
    conn.query_row(
        "SELECT id,kind,parent_id,name,created_at FROM memory_scopes WHERE id=?1",
        [id],
        row_scope,
    )
    .optional()?
    .ok_or_else(|| anyhow!("scope {id} does not exist"))
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
            bail!("scope {id} does not exist");
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

pub(super) fn query_events_after(
    conn: &Connection,
    stream_id: &str,
    cursor: i64,
) -> Result<Vec<MemoryEvent>> {
    let mut statement = conn.prepare(
        "SELECT id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json
         FROM memory_events WHERE stream_id=?1 AND sequence>?2 ORDER BY sequence",
    )?;
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
        "INSERT INTO memory_events(id,stream_id,sequence,scope_id,kind,actor_id,occurred_at,recorded_at,content_json,content_hash,token_count,sensitivity,metadata_json)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
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
    sql.push_str(" ORDER BY created_at,id");
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
    .ok_or_else(|| anyhow!("claim {id} does not exist"))
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
        "claim slot already uses {cardinality} cardinality"
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
            .ok_or_else(|| anyhow!("source event {event_id} does not exist"))?;
        ensure!(
            visible.contains(&event_scope),
            "source event {event_id} from scope {event_scope} is not visible to scope {scope_id}"
        );
        ensure!(
            sensitivity == "normal",
            "redacted event {event_id} cannot source a claim"
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
            "claim {claim_id} has source evidence in scope {source_scope}, which is not visible from scope {new_scope_id}"
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

pub(super) fn query_observation_source_ids_for_scopes(
    conn: &Connection,
    scope_ids: &[String],
) -> Result<HashMap<String, HashSet<String>>> {
    if scope_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", scope_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT source.observation_id,source.event_id
         FROM observation_sources source
         JOIN observations observation ON observation.id=source.observation_id
         WHERE observation.scope_id IN ({placeholders})"
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(rusqlite::params_from_iter(scope_ids), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut sources = HashMap::<String, HashSet<String>>::new();
    for row in rows {
        let (observation_id, event_id) = row?;
        sources.entry(observation_id).or_default().insert(event_id);
    }
    Ok(sources)
}

pub(super) fn query_view_observation_ids(
    conn: &Connection,
    view_ids: &[String],
) -> Result<HashSet<String>> {
    if view_ids.is_empty() {
        return Ok(HashSet::new());
    }
    let placeholders = std::iter::repeat_n("?", view_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "WITH RECURSIVE view_chain(id) AS (
            SELECT id FROM memory_views WHERE id IN ({placeholders})
            UNION
            SELECT view.previous_view_id
            FROM memory_views view
            JOIN view_chain current ON current.id=view.id
            WHERE view.previous_view_id IS NOT NULL
         )
         SELECT DISTINCT source.observation_id
         FROM view_chain
         JOIN view_sources source ON source.view_id=view_chain.id"
    );
    let mut statement = conn.prepare(&sql)?;
    Ok(collect_rows(
        statement.query_map(rusqlite::params_from_iter(view_ids), |row| {
            row.get::<_, String>(0)
        })?,
    )?
    .into_iter()
    .collect())
}

pub(super) fn estimate_claim_tokens(claim: &Claim) -> i64 {
    estimate_tokens(&format!(
        "{} {} {}",
        claim.subject, claim.predicate, claim.value
    ))
}

pub(super) fn sort_claims_by_scope(claims: &mut [Claim], scope_order: &[String]) {
    claims.sort_by_key(|claim| {
        scope_order
            .iter()
            .position(|scope_id| scope_id == &claim.scope_id)
            .unwrap_or(usize::MAX)
    });
}

pub(super) fn search_fts(
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

pub(super) fn query_string_column(
    conn: &Connection,
    sql: &str,
    value: &str,
) -> Result<Vec<String>> {
    let mut statement = conn.prepare(sql)?;
    collect_rows(statement.query_map([value], |row| row.get(0))?)
}
