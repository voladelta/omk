use super::*;

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
        let claim_ids = query_string_column(
            &tx,
            "SELECT claim_id FROM claim_sources WHERE event_id=?1",
            event_id,
        )?;
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
             SET status=CASE WHEN status='pending' THEN 'stale' ELSE status END,
                 source_integrity='privacy-purged',
                 ambiguities_json='[]',
                 error=CASE WHEN status='pending' THEN 'source evidence privacy-purged' ELSE error END,
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
