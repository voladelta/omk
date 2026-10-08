use super::*;

#[derive(Clone, Copy)]
enum ContextRendering {
    Full,
    Compact,
}

impl ContextRendering {
    fn claim_tokens(self, claim: &Claim) -> i64 {
        match self {
            Self::Full => serialized_item_tokens(claim),
            Self::Compact => serialized_item_tokens(&claim.compact_model_record()),
        }
    }

    fn event_tokens(self, event: &MemoryEvent) -> i64 {
        let serialized = match self {
            Self::Full => serialized_item_tokens(event),
            Self::Compact => serialized_item_tokens(&event.compact_model_record()),
        };
        serialized.saturating_add(event_hint_extra(event))
    }

    fn view_tokens(self, view: &MemoryView) -> i64 {
        let serialized = match self {
            Self::Full => serialized_item_tokens(view),
            Self::Compact => serialized_item_tokens(&view.compact_model_record()),
        };
        serialized.saturating_add(view_hint_extra(view))
    }

    fn observation_tokens(self, observation: &Observation) -> i64 {
        match self {
            Self::Full => serialized_item_tokens(observation),
            Self::Compact => serialized_item_tokens(&observation.compact_model_record()),
        }
    }
}

impl ContextDiagnostics {
    /// Count `cost` against `max_tokens` when it fits; otherwise record `id`
    /// as omitted for budget.
    fn admit(&mut self, max_tokens: i64, cost: i64, id: &str) -> bool {
        if cost <= max_tokens - self.estimated_tokens {
            self.estimated_tokens += cost;
            return true;
        }
        self.omitted_items.push(OmittedItem {
            id: id.to_owned(),
            reason: "context token budget".to_owned(),
        });
        false
    }
}

impl MemoryStore {
    pub fn create_view(&mut self, input: CreateView) -> Result<MutationResult<MemoryView>> {
        validate_nonempty("view content", &input.content)?;
        ensure!(
            input.kind == ViewKind::Continuity,
            KernelError::invalid_input("continuation views are created only by observation commit")
        );
        ensure!(
            input.source_from_sequence > 0
                && input.source_through_sequence >= input.source_from_sequence,
            KernelError::invalid_input("view source sequence range is invalid")
        );
        self.mutate("view.create", &input.idempotency_key, &input, |tx| {
            ensure_scope_exists(tx, &input.scope_id)?;
            let stream_scope = query_stream_scope(tx, &input.stream_id)?;
            ensure!(
                stream_scope == input.scope_id,
                KernelError::scope_violation(format!(
                    "stream {} belongs to scope {stream_scope}, not {}",
                    input.stream_id, input.scope_id
                ))
            );
            let latest = latest_view(tx, &input.stream_id, "continuity")?;
            let latest_id = latest.as_ref().map(|view| view.id.as_str());
            ensure!(
                latest_id == input.expected_previous_view_id.as_deref(),
                KernelError::stale_view(format!(
                    "view is stale: expected previous view {:?}, found {latest_id:?}",
                    input.expected_previous_view_id,
                ))
            );
            for observation_id in &input.source_observation_ids {
                let (source_scope, source_stream, source_start, source_end): (
                    String,
                    String,
                    i64,
                    i64,
                ) = tx
                    .query_row(
                        "SELECT observation.scope_id,run.stream_id,observation.source_start_sequence,observation.source_end_sequence
                         FROM observations observation
                         JOIN observation_runs run ON run.id=observation.run_id
                         WHERE observation.id=?1",
                        [observation_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .optional()?
                    .ok_or_else(|| {
                        KernelError::not_found(format!("observation {observation_id} does not exist"))
                    })?;
                ensure!(
                    source_scope == input.scope_id,
                    KernelError::scope_violation(format!(
                        "observation {observation_id} belongs to scope {source_scope}, not {}",
                        input.scope_id
                    ))
                );
                ensure!(
                    source_stream == input.stream_id,
                    KernelError::scope_violation(format!(
                        "observation {observation_id} belongs to stream {source_stream}, not {}",
                        input.stream_id
                    ))
                );
                ensure!(
                    source_start >= input.source_from_sequence
                        && source_end <= input.source_through_sequence,
                    KernelError::invalid_input(format!(
                        "observation {observation_id} is outside the declared view source range"
                    ))
                );
            }
            let estimated = estimate_tokens(&input.content);
            let view = insert_next_view(
                tx,
                &input.scope_id,
                &input.stream_id,
                input.kind.clone(),
                &input.content,
                input.source_from_sequence,
                input.source_through_sequence,
                input.model.as_deref(),
                input.prompt_version.as_deref(),
                input.token_count.map_or(estimated, |hint| hint.max(estimated)),
            )?;
            for observation_id in &input.source_observation_ids {
                tx.execute(
                    "INSERT INTO view_sources(view_id,observation_id) VALUES (?1,?2)",
                    params![view.id, observation_id],
                )?;
            }
            Ok(view)
        })
    }

    pub fn list_views(&self, scope_id: &str) -> Result<Vec<MemoryView>> {
        ensure_scope_exists(&self.conn, scope_id)?;
        let mut statement = self.conn.prepare(&format!(
            "SELECT {VIEW_COLUMNS} FROM memory_views WHERE scope_id=?1 ORDER BY stream_id,kind,generation"
        ))?;
        collect_rows(statement.query_map([scope_id], row_view)?)
    }

    pub fn recall_by_observation(
        &self,
        access: &ReadAccess,
        observation_id: &str,
    ) -> Result<Vec<MemoryEvent>> {
        Ok(self
            .explain_observation(access, observation_id)?
            .source_events)
    }

    pub fn explain_observation(
        &self,
        access: &ReadAccess,
        observation_id: &str,
    ) -> Result<ObservationExplanation> {
        let observation = self
            .conn
            .query_row(
                &format!("SELECT {OBSERVATION_COLUMNS} FROM observations WHERE id=?1"),
                [observation_id],
                row_observation,
            )
            .optional()?
            .ok_or_else(|| {
                KernelError::not_found(format!("observation {observation_id} does not exist"))
            })?;
        let source_events = self.source_events(
            access,
            &observation.scope_id,
            "SELECT event_id FROM observation_sources WHERE observation_id=?1",
            observation_id,
        )?;
        Ok(ObservationExplanation {
            observation,
            source_events,
        })
    }

    pub fn explain_claim(&self, access: &ReadAccess, claim_id: &str) -> Result<ClaimExplanation> {
        let claim = query_claim(&self.conn, claim_id)?;
        let source_events = self.source_events(
            access,
            &claim.scope_id,
            "SELECT event_id FROM claim_sources WHERE claim_id=?1",
            claim_id,
        )?;
        Ok(ClaimExplanation {
            claim,
            source_events,
        })
    }

    /// The source events of a record in `record_scope_id`, read through
    /// `access`. `sources_sql` selects the record's event IDs.
    fn source_events(
        &self,
        access: &ReadAccess,
        record_scope_id: &str,
        sources_sql: &str,
        record_id: &str,
    ) -> Result<Vec<MemoryEvent>> {
        let access = ResolvedReadAccess::resolve(&self.conn, access)?;
        access.ensure_scope(record_scope_id)?;
        let mut statement = self.conn.prepare(&format!(
            "SELECT {EVENT_COLUMNS} FROM memory_events WHERE id IN ({sources_sql})
             ORDER BY stream_id,sequence"
        ))?;
        collect_rows(statement.query_map([record_id], row_event)?)?
            .into_iter()
            .map(|event| access.apply(event))
            .collect()
    }

    pub fn search_full_text(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        self.search_with_options(scope_id, query, limit, SearchOptions::default())
    }

    pub fn search_with_options(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
        options: SearchOptions,
    ) -> Result<Vec<SearchHit>> {
        Ok(self.search(scope_id, query, limit, options, false)?.hits)
    }

    /// Search with `matched` and `searchable` counts, so an empty or full
    /// page says whether more exists.
    pub fn search_page(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
        options: SearchOptions,
    ) -> Result<SearchPage> {
        self.search(scope_id, query, limit, options, true)
    }

    fn search(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
        options: SearchOptions,
        count: bool,
    ) -> Result<SearchPage> {
        validate_nonempty("search query", query)?;
        ensure!(
            limit > 0 && limit <= 1000,
            KernelError::invalid_input("search limit must be from 1 to 1000")
        );
        let scope_ids = retrieval_scope_ids(&self.conn, scope_id)?;
        let fts_query = bounded_fts_query(query, options.mode)?;
        search_fts(&self.conn, &scope_ids, &fts_query, limit, options, count)
    }

    /// Resolve a name to the subject of active claims: exact subject or alias,
    /// then normalized name, then whole-word containment, then spelling
    /// distance. The first tier with a match decides.
    pub fn resolve_name(&self, scope_id: &str, name: &str) -> Result<Resolution> {
        let scope_ids = retrieval_scope_ids(&self.conn, scope_id)?;
        resolve_name(&self.conn, &scope_ids, name)
    }

    pub fn compose_context(
        &self,
        scope_id: &str,
        stream_id: &str,
        max_tokens: i64,
        recent_raw_tokens: i64,
        query: Option<&str>,
    ) -> Result<ContextBundle> {
        self.compose_context_with_query(
            scope_id,
            stream_id,
            max_tokens,
            recent_raw_tokens,
            query.map(|text| ContextQuery {
                text,
                options: SearchOptions::default(),
            }),
        )
    }

    /// Compose full context with explicit evidence search semantics.
    pub fn compose_context_with_query(
        &self,
        scope_id: &str,
        stream_id: &str,
        max_tokens: i64,
        recent_raw_tokens: i64,
        query: Option<ContextQuery<'_>>,
    ) -> Result<ContextBundle> {
        self.compose_context_with_rendering(
            scope_id,
            stream_id,
            max_tokens,
            recent_raw_tokens,
            query,
            ContextRendering::Full,
        )
    }

    pub fn compose_compact_context(
        &self,
        scope_id: &str,
        stream_id: &str,
        max_tokens: i64,
        recent_raw_tokens: i64,
        query: Option<&str>,
    ) -> Result<ContextBundle> {
        self.compose_compact_context_with_query(
            scope_id,
            stream_id,
            max_tokens,
            recent_raw_tokens,
            query.map(|text| ContextQuery {
                text,
                options: SearchOptions::default(),
            }),
        )
    }

    /// Compose compact context with explicit evidence search semantics.
    pub fn compose_compact_context_with_query(
        &self,
        scope_id: &str,
        stream_id: &str,
        max_tokens: i64,
        recent_raw_tokens: i64,
        query: Option<ContextQuery<'_>>,
    ) -> Result<ContextBundle> {
        self.compose_context_with_rendering(
            scope_id,
            stream_id,
            max_tokens,
            recent_raw_tokens,
            query,
            ContextRendering::Compact,
        )
    }

    fn compose_context_with_rendering(
        &self,
        scope_id: &str,
        stream_id: &str,
        max_tokens: i64,
        recent_raw_tokens: i64,
        query: Option<ContextQuery<'_>>,
        rendering: ContextRendering,
    ) -> Result<ContextBundle> {
        ensure!(
            max_tokens > 0,
            KernelError::invalid_input("max tokens must be positive")
        );
        ensure!(
            recent_raw_tokens >= 0,
            KernelError::invalid_input("recent raw tokens cannot be negative")
        );
        // Read every section from one snapshot. Dropping the transaction
        // rolls it back, which is all a read needs.
        let _snapshot = self.conn.unchecked_transaction()?;
        let visible = visible_scope_ids(&self.conn, scope_id)?;
        let stream_scope = query_stream_scope(&self.conn, stream_id)?;
        ensure!(
            retrieval_scope_ids(&self.conn, scope_id)?.contains(&stream_scope),
            KernelError::scope_violation(format!(
                "stream {stream_id} is not visible from scope {scope_id}"
            ))
        );
        let mut claims = query_claims_for_scopes(&self.conn, &visible, Some("active"), None)?;
        sort_claims_by_scope(&mut claims, &visible);
        let (claims, shadowed_claims) = split_shadowed_claims(claims, &visible);
        let mut pending_claims =
            query_claims_for_scopes(&self.conn, &visible, Some("pending"), Some(257))?;
        pending_claims.extend(query_claims_for_scopes(
            &self.conn,
            &visible,
            Some("disputed"),
            Some(257),
        )?);
        let pending_truncated = pending_claims.len() > 256;
        pending_claims.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        pending_claims.truncate(256);
        pending_claims.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        sort_claims_by_scope(&mut pending_claims, &visible);
        let empty_payload = json!({"claims": [], "pendingClaims": [], "continuation": null,
            "continuityViews": [], "observations": [], "recentEvents": [], "recalledEvidence": []});
        let overhead_tokens = estimate_tokens(&empty_payload.to_string());
        ensure!(
            overhead_tokens <= max_tokens,
            KernelError::budget_exceeded(format!(
                "context budget too small: minimumRequiredTokens={overhead_tokens} for the context structure"
            ))
        );
        let (claims, over_budget_claims) = budget_claims(
            &self.conn,
            claims,
            percent_of(max_tokens, CLAIM_BUDGET_PERCENT),
            |claim| rendering.claim_tokens(claim),
        )?;
        let required_tokens = overhead_tokens
            + claims
                .iter()
                .map(|claim| rendering.claim_tokens(claim))
                .sum::<i64>();
        let mut diagnostics = ContextDiagnostics {
            estimated_tokens: required_tokens,
            omitted_items: shadowed_claims
                .into_iter()
                .map(|claim| OmittedItem {
                    id: claim.id,
                    reason: "shadowed by descendant scope claim".to_owned(),
                })
                .chain(over_budget_claims.into_iter().map(|claim| OmittedItem {
                    id: claim.id,
                    reason: "active claim budget".to_owned(),
                }))
                .collect(),
            truncated: pending_truncated,
        };

        let mut continuity_views = Vec::new();
        let mut continuation = None;
        if let Some(view) = latest_view(&self.conn, stream_id, "continuation")? {
            let draft: ContinuationDraft = serde_json::from_str(&view.content)
                .context("reading structured continuation view")?;
            let cost = serialized_item_tokens(&draft).saturating_add(view_hint_extra(&view));
            if diagnostics.admit(max_tokens, cost, &view.id) {
                continuation = Some(draft);
            }
        }

        let mut selected_pending_claims = Vec::new();
        for claim in pending_claims {
            if diagnostics.admit(max_tokens, rendering.claim_tokens(&claim), &claim.id) {
                selected_pending_claims.push(claim);
            }
        }

        let mut recent_events_reversed: Vec<(MemoryEvent, i64)> = Vec::new();
        let raw_budget = recent_raw_tokens.min((max_tokens - diagnostics.estimated_tokens).max(0));
        let mut raw_tokens = 0;
        let mut cursor = i64::MAX;
        'raw: loop {
            if raw_budget == 0 {
                break;
            }
            let page = query_event_page(&self.conn, stream_id, cursor, true)?;
            let page_len = page.len();
            for event in page {
                cursor = event.sequence;
                let safe = redact_for_agent(event);
                let cost = rendering.event_tokens(&safe);
                if cost > raw_budget - raw_tokens {
                    let keep = aligned_tail_len(
                        &recent_events_reversed
                            .iter()
                            .map(|(event, _)| event.sequence)
                            .collect::<Vec<_>>(),
                    );
                    let dropped = recent_events_reversed.split_off(keep);
                    raw_tokens -= dropped.iter().map(|(_, cost)| cost).sum::<i64>();
                    for event in dropped.into_iter().map(|(event, _)| event).chain([safe]) {
                        diagnostics.omitted_items.push(OmittedItem {
                            id: event.id,
                            reason: "outside recent raw token budget".to_owned(),
                        });
                    }
                    diagnostics.truncated = true;
                    break 'raw;
                }
                raw_tokens += cost;
                recent_events_reversed.push((safe, cost));
            }
            if page_len < 32 {
                break;
            }
        }
        let mut recent_events: Vec<MemoryEvent> = recent_events_reversed
            .into_iter()
            .map(|(event, _)| event)
            .collect();
        recent_events.reverse();
        diagnostics.estimated_tokens += raw_tokens;
        let mut recalled_evidence = Vec::new();
        if let Some(query) = query {
            let hits = self.search_with_options(scope_id, query.text, 10, query.options)?;
            let read_access = ReadAccess::agent(scope_id);
            let access = ResolvedReadAccess::resolve(&self.conn, &read_access)?;
            let mut recalled_ids = HashSet::new();
            for hit in hits {
                let mut ids = if hit.record_type == "event" {
                    vec![hit.id]
                } else {
                    context_source_ids(&self.conn, &hit.record_type, &hit.id)?
                };
                if ids.len() > MAX_SOURCE_IDS {
                    diagnostics.truncated = true;
                    ids.truncate(MAX_SOURCE_IDS);
                }
                for id in ids {
                    if !recalled_ids.insert(id.clone()) || recent_events.iter().any(|e| e.id == id)
                    {
                        continue;
                    }
                    let event = access.apply(self.query_event(&id)?)?;
                    if diagnostics.admit(max_tokens, rendering.event_tokens(&event), &id) {
                        recalled_evidence.push(event);
                    }
                }
            }
        }
        let represented_event_ids: Vec<&str> = recent_events
            .iter()
            .chain(recalled_evidence.iter())
            .map(|event| event.id.as_str())
            .collect();
        let represented_event_ids = serde_json::to_string(&represented_event_ids)?;

        if let Some(view) = latest_view(&self.conn, stream_id, "continuity")?
            && diagnostics.admit(max_tokens, rendering.view_tokens(&view), &view.id)
        {
            continuity_views.push(view);
        }
        let continuity_ids: Vec<String> = continuity_views.iter().map(|v| v.id.clone()).collect();

        let mut observation_scopes = visible.clone();
        if !observation_scopes.contains(&stream_scope) {
            observation_scopes.push(stream_scope);
        }
        // Newest first, so a backlog of older observations cannot hide new ones.
        let mut candidates =
            query_observations_for_scopes(&self.conn, &observation_scopes, &continuity_ids)?;
        if candidates.len() > 256 {
            diagnostics.truncated = true;
            candidates.truncate(256);
        }
        let mut observations = Vec::new();
        for observation in candidates {
            if observation_has_events(&self.conn, &observation.id, &represented_event_ids)? {
                diagnostics.omitted_items.push(OmittedItem {
                    id: observation.id,
                    reason: "source events already present in raw tail".to_owned(),
                });
                continue;
            }
            let cost = rendering.observation_tokens(&observation);
            if diagnostics.admit(max_tokens, cost, &observation.id) {
                observations.push(observation);
            }
        }
        observations.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(ContextBundle {
            claims,
            pending_claims: selected_pending_claims,
            continuation,
            continuity_views,
            observations,
            recent_events,
            recalled_evidence,
            diagnostics,
        })
    }
}
