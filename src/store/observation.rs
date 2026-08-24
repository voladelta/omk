use super::*;

impl MemoryStore {
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
        let previous_continuation = latest_view(&tx, stream_id, "continuation")?;
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
            run.status == "pending",
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
                origin_run_id: Some(run_id.to_owned()),
                scope_id: run.scope_id.clone(),
                kind: draft.kind.clone(),
                subject: draft.subject.trim().to_owned(),
                predicate: draft.predicate.trim().to_owned(),
                cardinality: draft.cardinality.clone(),
                value: draft.value.clone(),
                value_hash: hash_json(&draft.value),
                modality: draft.modality.clone(),
                status: ClaimStatus::Pending,
                authority: ClaimAuthority::ModelInference,
                confidence: draft.confidence,
                supersedes_id: None,
                created_at: timestamp.clone(),
                updated_at: timestamp.clone(),
            };
            insert_claim(&tx, &claim)?;
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
            .then(|| latest_view(&tx, &run.stream_id, "continuation"))
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
                &run.stream_id,
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
        let next_required_action = (!claims.is_empty()).then(|| {
            let claim_ids = claims
                .iter()
                .map(|claim| claim.id.as_str())
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "review observer claims [{claim_ids}]; explicitly run `omk claim confirm --id <claim-id> --idempotency-key <key>` or `omk claim reject --id <claim-id> --idempotency-key <key>` for each"
            )
        });
        let commit = ObservationCommit {
            run_id: run_id.to_owned(),
            observations,
            claims,
            continuation_view,
            continuation_action,
            ambiguities: result.ambiguities,
            next_required_action,
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
            run.status == "pending",
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

    pub fn get_observation_run(
        &self,
        access: &ReadAccess,
        run_id: &str,
    ) -> Result<ObservationRunInfo> {
        let run = self.conn
            .query_row(
                "SELECT id,scope_id,stream_id,cursor_at_plan,from_sequence,to_sequence,status,source_integrity,observer_model,prompt_version,ambiguities_json,error,created_at,updated_at
                 FROM observation_runs WHERE id=?1",
                [run_id],
                row_observation_run_info,
            )
            .optional()?
            .ok_or_else(|| anyhow!("observation run {run_id} does not exist"))?;
        ensure_read_scope(&self.conn, access, &run.scope_id)?;
        Ok(run)
    }

    pub fn list_observation_runs(
        &self,
        access: &ReadAccess,
        stream_id: Option<&str>,
        status: Option<&str>,
    ) -> Result<Vec<ObservationRunInfo>> {
        if let Some(status) = status {
            ensure!(
                matches!(status, "pending" | "committed" | "failed" | "stale"),
                "invalid observation run status {status}"
            );
        }
        let mut statement = self.conn.prepare(
            "SELECT id,scope_id,stream_id,cursor_at_plan,from_sequence,to_sequence,status,source_integrity,observer_model,prompt_version,ambiguities_json,error,created_at,updated_at
             FROM observation_runs
             WHERE (?1 IS NULL OR stream_id=?1)
               AND (?2 IS NULL OR status=?2)
             ORDER BY created_at,id",
        )?;
        let visible = retrieval_scope_ids(&self.conn, &access.anchor_scope_id)?;
        Ok(collect_rows(
            statement.query_map(params![stream_id, status], row_observation_run_info)?,
        )?
        .into_iter()
        .filter(|run| visible.contains(&run.scope_id))
        .collect())
    }

    pub fn stream_status(&self, access: &ReadAccess, stream_id: &str) -> Result<StreamStatus> {
        let (scope_id, observed_through_sequence, next_sequence): (String, i64, i64) = self
            .conn
            .query_row(
                "SELECT scope_id,observed_through_sequence,next_sequence FROM memory_streams WHERE id=?1",
                [stream_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| anyhow!("stream {stream_id} does not exist"))?;
        ensure_read_scope(&self.conn, access, &scope_id)?;
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
            runs: self.list_observation_runs(access, Some(stream_id), None)?,
        })
    }
}
