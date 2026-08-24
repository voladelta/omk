use super::*;

pub(super) fn row_scope(row: &Row<'_>) -> rusqlite::Result<Scope> {
    Ok(Scope {
        id: row.get(0)?,
        kind: parse_enum(&row.get::<_, String>(1)?)?,
        parent_id: row.get(2)?,
        name: row.get(3)?,
        created_at: row.get(4)?,
    })
}

pub(super) fn row_event(row: &Row<'_>) -> rusqlite::Result<MemoryEvent> {
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

pub(super) fn row_observation(row: &Row<'_>) -> rusqlite::Result<Observation> {
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

pub(super) fn row_claim(row: &Row<'_>) -> rusqlite::Result<Claim> {
    Ok(Claim {
        id: row.get(0)?,
        origin_run_id: row.get(1)?,
        scope_id: row.get(2)?,
        kind: parse_enum(&row.get::<_, String>(3)?)?,
        subject: row.get(4)?,
        predicate: row.get(5)?,
        cardinality: parse_enum(&row.get::<_, String>(6)?)?,
        value: parse_json_column(row, 7)?,
        value_hash: row.get(8)?,
        modality: parse_enum(&row.get::<_, String>(9)?)?,
        status: parse_enum(&row.get::<_, String>(10)?)?,
        authority: parse_enum(&row.get::<_, String>(11)?)?,
        confidence: row.get(12)?,
        supersedes_id: row.get(13)?,
        created_at: row.get(14)?,
        updated_at: row.get(15)?,
    })
}

pub(super) fn row_view(row: &Row<'_>) -> rusqlite::Result<MemoryView> {
    Ok(MemoryView {
        id: row.get(0)?,
        scope_id: row.get(1)?,
        stream_id: row.get(2)?,
        kind: parse_enum(&row.get::<_, String>(3)?)?,
        generation: row.get(4)?,
        content: row.get(5)?,
        source_from_sequence: row.get(6)?,
        source_through_sequence: row.get(7)?,
        previous_view_id: row.get(8)?,
        model: row.get(9)?,
        prompt_version: row.get(10)?,
        token_count: row.get(11)?,
        created_at: row.get(12)?,
    })
}

pub(super) fn row_observation_run_info(row: &Row<'_>) -> rusqlite::Result<ObservationRunInfo> {
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

pub(super) fn parse_json_column(row: &Row<'_>, index: usize) -> rusqlite::Result<Value> {
    let raw: String = row.get(index)?;
    serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            raw.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

pub(super) fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>> {
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}
