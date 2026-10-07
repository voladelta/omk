use std::collections::VecDeque;

use super::*;

#[derive(Default)]
struct PrivacyClosure {
    /// (id, stream_id, sequence, scope_id)
    events: Vec<(String, String, i64, String)>,
    generated_event_ids: HashSet<String>,
    observation_ids: HashSet<String>,
    claim_ids: HashSet<String>,
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
        self.mutate("claim.purge", idempotency_key, &claim_id, |tx| {
            query_claim(tx, claim_id)?;
            let closure = collect_privacy_closure(tx, &[], &[claim_id.to_owned()])?;
            apply_privacy_closure(tx, &closure)?;
            Ok(json!({
                "purged": "claim",
                "id": claim_id,
                "purgedCommandEvents": closure.events.len()
            }))
        })
    }

    pub fn purge_event(
        &mut self,
        event_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Value>> {
        self.mutate("event.purge", idempotency_key, &event_id, |tx| {
            let (stream_id, sequence): (String, i64) = tx
                .query_row(
                    "SELECT stream_id,sequence FROM memory_events WHERE id=?1",
                    [event_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or_else(|| {
                    KernelError::not_found(format!("event {event_id} does not exist"))
                })?;
            let closure = collect_privacy_closure(tx, &[event_id.to_owned()], &[])?;
            apply_privacy_closure(tx, &closure)?;
            fn sorted(ids: &HashSet<String>) -> Vec<&String> {
                let mut ids: Vec<&String> = ids.iter().collect();
                ids.sort();
                ids
            }
            Ok(json!({
                "purged": "event",
                "id": event_id,
                "streamId": stream_id,
                "sequence": sequence,
                "dependentObservations": closure.observation_ids.len(),
                "dependentClaims": closure.claim_ids.len(),
                "dependentViews": closure.view_ids.len(),
                "dependentViewIds": sorted(&closure.view_ids),
                "affectedRunIds": sorted(&closure.affected_run_ids),
                "purgedCommandEvents": closure.generated_event_ids.len()
            }))
        })
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
        let Some(event) = conn
            .query_row(
                "SELECT id,stream_id,sequence,scope_id FROM memory_events WHERE id=?1",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?
        else {
            continue;
        };
        closure.observation_ids.extend(query_strings(
            conn,
            "SELECT observation_id FROM observation_sources WHERE event_id=?1",
            [&event_id],
        )?);
        claim_queue.extend(query_strings(
            conn,
            "SELECT claim_id FROM claim_sources WHERE event_id=?1",
            [&event_id],
        )?);
        closure.direct_view_ids.extend(query_strings(
            conn,
            "SELECT id FROM memory_views
             WHERE scope_id=?1 AND stream_id=?2
               AND source_from_sequence<=?3 AND source_through_sequence>=?3",
            params![event.3, event.1, event.2],
        )?);
        closure.affected_run_ids.extend(query_strings(
            conn,
            "SELECT id FROM observation_runs
             WHERE stream_id=?1 AND from_sequence<=?2 AND to_sequence>=?2",
            params![event.1, event.2],
        )?);
        closure.events.push(event);
    }

    for observation_id in &closure.observation_ids {
        closure.direct_view_ids.extend(query_strings(
            conn,
            "SELECT view_id FROM view_sources WHERE observation_id=?1",
            [observation_id],
        )?);
    }
    let direct_view_ids: Vec<String> = closure.direct_view_ids.iter().cloned().collect();
    closure.view_ids = view_successor_ids(conn, &direct_view_ids)?
        .into_iter()
        .collect();
    Ok(closure)
}

fn apply_privacy_closure(conn: &Connection, closure: &PrivacyClosure) -> Result<()> {
    let record_ids: Vec<&str> = closure
        .claim_ids
        .iter()
        .chain(closure.observation_ids.iter())
        .chain(closure.view_ids.iter())
        // Saved plans hold only their run ID, so a run that loses evidence
        // tombstones the plan that created it.
        .chain(closure.affected_run_ids.iter())
        .map(String::as_str)
        .chain(closure.events.iter().map(|event| event.0.as_str()))
        .collect();
    scrub_operations_referencing(conn, &record_ids)?;
    for claim_id in &closure.claim_ids {
        delete_fts(conn, "claim", claim_id)?;
        conn.execute("DELETE FROM claims WHERE id=?1", [claim_id])?;
    }
    // A slot without any claim left forgets its cardinality.
    conn.execute(
        "DELETE FROM claim_slots
         WHERE NOT EXISTS (
             SELECT 1 FROM claims
             WHERE claims.scope_id=claim_slots.scope_id AND claims.kind=claim_slots.kind
               AND claims.subject=claim_slots.subject AND claims.predicate=claim_slots.predicate
         )",
        [],
    )?;
    for observation_id in &closure.observation_ids {
        delete_fts(conn, "observation", observation_id)?;
        conn.execute("DELETE FROM observations WHERE id=?1", [observation_id])?;
    }
    for view_id in &closure.direct_view_ids {
        conn.execute("DELETE FROM memory_views WHERE id=?1", [view_id])?;
    }
    let updated_at = now();
    for run_id in &closure.affected_run_ids {
        conn.execute(
            "UPDATE observation_runs
             SET status=CASE WHEN status='pending' THEN 'stale' ELSE status END,
                 source_integrity='privacy-purged',
                 ambiguities_json='[]',
                 truncated_event_ids_json='[]',
                 error=CASE WHEN status='pending' THEN 'source evidence privacy-purged' ELSE error END,
                 updated_at=?1
             WHERE id=?2",
            params![updated_at, run_id],
        )?;
    }
    for (event_id, _, _, _) in &closure.events {
        delete_fts(conn, "event", event_id)?;
        conn.execute("DELETE FROM memory_events WHERE id=?1", [event_id])?;
    }
    Ok(())
}
