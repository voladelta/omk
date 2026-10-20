use super::*;

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
    let mut batch: HashMap<(String, String), String> = HashMap::new();
    for (index, draft) in drafts.iter().enumerate() {
        let subject = draft.subject.trim();
        let predicate = draft.predicate.trim();
        let wanted = enum_text(&draft.cardinality);
        let existing: Option<String> = conn
            .query_row(
                "SELECT cardinality FROM claim_slots
                 WHERE scope_id=?1 AND subject=?2 AND predicate=?3",
                params![scope_id, subject, predicate],
                |row| row.get(0),
            )
            .optional()?;
        // Drafts in one result must also agree with each other.
        let existing = existing
            .or_else(|| batch.insert((subject.to_owned(), predicate.to_owned()), wanted.clone()));
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
