use std::collections::VecDeque;

use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ClaimSlotKey {
    scope_id: String,
    kind: String,
    subject: String,
    predicate: String,
}

#[derive(Default)]
struct PrivacyClosure {
    events: Vec<(String, String, i64, String)>,
    generated_event_ids: HashSet<String>,
    observation_ids: HashSet<String>,
    claim_ids: HashSet<String>,
    claim_slots: HashSet<ClaimSlotKey>,
    direct_view_ids: HashSet<String>,
    view_ids: HashSet<String>,
    affected_run_ids: HashSet<String>,
}

impl MemoryStore {
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
        let closure = collect_privacy_closure(&tx, &[], &[claim_id.to_owned()])?;
        let purged_command_events = closure.events.len();
        apply_privacy_closure(&tx, &closure)?;
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
            .ok_or_else(|| {
                KernelError::new(
                    KernelErrorKind::NotFound,
                    format!("event {event_id} does not exist"),
                )
            })?;
        let closure = collect_privacy_closure(&tx, &[event_id.to_owned()], &[])?;
        let dependent_observations = closure.observation_ids.len();
        let dependent_claims = closure.claim_ids.len();
        let purged_command_events = closure.generated_event_ids.len();
        let mut dependent_view_ids: Vec<String> = closure.view_ids.iter().cloned().collect();
        dependent_view_ids.sort();
        let mut affected_run_ids: Vec<String> = closure.affected_run_ids.iter().cloned().collect();
        affected_run_ids.sort();
        apply_privacy_closure(&tx, &closure)?;
        let result = json!({
            "purged": "event",
            "id": event_id,
            "streamId": event.0,
            "sequence": event.1,
            "dependentObservations": dependent_observations,
            "dependentClaims": dependent_claims,
            "dependentViews": dependent_view_ids.len(),
            "dependentViewIds": dependent_view_ids,
            "affectedRunIds": affected_run_ids,
            "purgedCommandEvents": purged_command_events
        });
        save_operation(&tx, idempotency_key, "event.purge", &request_hash, &result)?;
        tx.commit()?;
        Ok(MutationResult::created(result))
    }
}

fn collect_privacy_closure(
    conn: &Connection,
    root_event_ids: &[String],
    root_claim_ids: &[String],
) -> Result<PrivacyClosure> {
    let mut closure = PrivacyClosure::default();
    let mut event_queue: VecDeque<String> = root_event_ids.iter().cloned().collect();
    let mut claim_queue: VecDeque<String> = root_claim_ids.iter().cloned().collect();
    let mut seen_events = HashSet::new();

    while !event_queue.is_empty() || !claim_queue.is_empty() {
        while let Some(claim_id) = claim_queue.pop_front() {
            if !closure.claim_ids.insert(claim_id.clone()) {
                continue;
            }
            let slot = conn
                .query_row(
                    "SELECT scope_id,kind,subject,predicate FROM claims WHERE id=?1",
                    [&claim_id],
                    |row| {
                        Ok(ClaimSlotKey {
                            scope_id: row.get(0)?,
                            kind: row.get(1)?,
                            subject: row.get(2)?,
                            predicate: row.get(3)?,
                        })
                    },
                )
                .optional()?;
            let Some(slot) = slot else {
                continue;
            };
            closure.claim_slots.insert(slot);
            for command_id in generated_command_source_ids(conn, &claim_id)? {
                closure.generated_event_ids.insert(command_id.clone());
                event_queue.push_back(command_id);
            }
        }

        let Some(event_id) = event_queue.pop_front() else {
            continue;
        };
        if !seen_events.insert(event_id.clone()) {
            continue;
        }
        let event = conn
            .query_row(
                "SELECT id,stream_id,sequence,scope_id FROM memory_events WHERE id=?1",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some(event) = event else {
            continue;
        };
        for observation_id in query_string_column(
            conn,
            "SELECT observation_id FROM observation_sources WHERE event_id=?1",
            &event_id,
        )? {
            closure.observation_ids.insert(observation_id);
        }
        for claim_id in query_string_column(
            conn,
            "SELECT claim_id FROM claim_sources WHERE event_id=?1",
            &event_id,
        )? {
            claim_queue.push_back(claim_id);
        }
        let range_view_ids = {
            let mut statement = conn.prepare(
                "SELECT id FROM memory_views
                 WHERE scope_id=?1 AND stream_id=?2
                   AND source_from_sequence<=?3 AND source_through_sequence>=?3",
            )?;
            collect_rows(
                statement.query_map(params![event.3, event.1, event.2], |row| {
                    row.get::<_, String>(0)
                })?,
            )?
        };
        closure.direct_view_ids.extend(range_view_ids);
        let run_ids = {
            let mut statement = conn.prepare(
                "SELECT id FROM observation_runs
                 WHERE stream_id=?1 AND from_sequence<=?2 AND to_sequence>=?2",
            )?;
            collect_rows(
                statement.query_map(params![event.1, event.2], |row| row.get::<_, String>(0))?,
            )?
        };
        closure.affected_run_ids.extend(run_ids);
        closure.events.push(event);
    }

    for observation_id in &closure.observation_ids {
        closure.direct_view_ids.extend(query_string_column(
            conn,
            "SELECT view_id FROM view_sources WHERE observation_id=?1",
            observation_id,
        )?);
    }
    let direct_view_ids: Vec<String> = closure.direct_view_ids.iter().cloned().collect();
    closure.view_ids.extend(direct_view_ids.iter().cloned());
    closure
        .view_ids
        .extend(view_successor_ids(conn, &direct_view_ids)?);
    Ok(closure)
}

fn apply_privacy_closure(conn: &Connection, closure: &PrivacyClosure) -> Result<()> {
    for claim_id in &closure.claim_ids {
        conn.execute(
            "DELETE FROM memory_fts WHERE record_type='claim' AND record_id=?1",
            [claim_id],
        )?;
        scrub_operations_referencing(conn, claim_id)?;
        conn.execute("DELETE FROM claims WHERE id=?1", [claim_id])?;
    }
    for slot in &closure.claim_slots {
        conn.execute(
            "DELETE FROM claim_slots
             WHERE scope_id=?1 AND kind=?2 AND subject=?3 AND predicate=?4
               AND NOT EXISTS (
                   SELECT 1 FROM claims
                   WHERE scope_id=?1 AND kind=?2 AND subject=?3 AND predicate=?4
               )",
            params![slot.scope_id, slot.kind, slot.subject, slot.predicate],
        )?;
    }
    for observation_id in &closure.observation_ids {
        conn.execute(
            "DELETE FROM memory_fts WHERE record_type='observation' AND record_id=?1",
            [observation_id],
        )?;
        scrub_operations_referencing(conn, observation_id)?;
        conn.execute("DELETE FROM observations WHERE id=?1", [observation_id])?;
    }
    for view_id in &closure.view_ids {
        scrub_operations_referencing(conn, view_id)?;
    }
    for view_id in &closure.direct_view_ids {
        conn.execute("DELETE FROM memory_views WHERE id=?1", [view_id])?;
    }
    for (_, stream_id, sequence, _) in &closure.events {
        conn.execute(
            "UPDATE observation_runs
             SET status=CASE WHEN status='pending' THEN 'stale' ELSE status END,
                 source_integrity='privacy-purged',
                 ambiguities_json='[]',
                 error=CASE WHEN status='pending' THEN 'source evidence privacy-purged' ELSE error END,
                 updated_at=?1
             WHERE stream_id=?2 AND from_sequence<=?3 AND to_sequence>=?3",
            params![now(), stream_id, sequence],
        )?;
    }
    for (event_id, _, _, _) in &closure.events {
        conn.execute(
            "DELETE FROM memory_fts WHERE record_type='event' AND record_id=?1",
            [event_id],
        )?;
        scrub_operations_referencing(conn, event_id)?;
        conn.execute("DELETE FROM memory_events WHERE id=?1", [event_id])?;
    }
    Ok(())
}
