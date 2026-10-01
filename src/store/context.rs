use super::*;

#[derive(Clone, Copy)]
enum ContextRendering {
    Full,
    Compact,
}

impl ContextRendering {
    fn claim_tokens(self, claim: &Claim) -> i64 {
        match self {
            Self::Full => estimate_claim_tokens(claim),
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

impl MemoryStore {
    pub fn create_view(&mut self, input: CreateView) -> Result<MutationResult<MemoryView>> {
        validate_nonempty("view content", &input.content)?;
        validate_nonempty("idempotency key", &input.idempotency_key)?;
        ensure!(
            input.kind == ViewKind::Continuity,
            KernelError::new(
                KernelErrorKind::InvalidInput,
                "continuation views are created only by observation commit",
            )
        );
        ensure!(
            input.source_from_sequence > 0
                && input.source_through_sequence >= input.source_from_sequence,
            KernelError::new(
                KernelErrorKind::InvalidInput,
                "view source sequence range is invalid",
            )
        );
        let request_hash = operation_request_hash("view.create", &input)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<MemoryView>(&tx, &input.idempotency_key, "view.create", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        ensure_scope_exists(&tx, &input.scope_id)?;
        let stream_scope: String = tx
            .query_row(
                "SELECT scope_id FROM memory_streams WHERE id=?1",
                [&input.stream_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| {
                KernelError::new(
                    KernelErrorKind::NotFound,
                    format!("stream {} does not exist", input.stream_id),
                )
            })?;
        ensure!(
            stream_scope == input.scope_id,
            KernelError::new(
                KernelErrorKind::ScopeViolation,
                format!(
                    "stream {} belongs to scope {stream_scope}, not {}",
                    input.stream_id, input.scope_id
                ),
            )
        );
        let latest = latest_view(&tx, &input.stream_id, "continuity")?;
        ensure!(
            latest.as_ref().map(|view| view.id.as_str())
                == input.expected_previous_view_id.as_deref(),
            KernelError::new(
                KernelErrorKind::StaleView,
                format!(
                    "view is stale: expected previous view {:?}, found {:?}",
                    input.expected_previous_view_id,
                    latest.as_ref().map(|view| view.id.as_str())
                ),
            )
        );
        for observation_id in &input.source_observation_ids {
            let (source_scope, source_stream, source_start, source_end):
                (String, String, i64, i64) = tx
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
                    KernelError::new(
                        KernelErrorKind::NotFound,
                        format!("observation {observation_id} does not exist"),
                    )
                })?;
            ensure!(
                source_scope == input.scope_id,
                KernelError::new(
                    KernelErrorKind::ScopeViolation,
                    format!(
                        "observation {observation_id} belongs to scope {source_scope}, not {}",
                        input.scope_id
                    ),
                )
            );
            ensure!(
                source_stream == input.stream_id,
                KernelError::new(
                    KernelErrorKind::ScopeViolation,
                    format!(
                        "observation {observation_id} belongs to stream {source_stream}, not {}",
                        input.stream_id
                    ),
                )
            );
            ensure!(
                source_start >= input.source_from_sequence
                    && source_end <= input.source_through_sequence,
                KernelError::new(
                    KernelErrorKind::InvalidInput,
                    format!(
                        "observation {observation_id} is outside the declared view source range"
                    ),
                )
            );
        }
        let estimated_token_count = estimate_tokens(&input.content);
        let token_count = input.token_count.map_or(estimated_token_count, |count| {
            count.max(estimated_token_count)
        });
        let view = insert_next_view(
            &tx,
            &input.scope_id,
            &input.stream_id,
            input.kind,
            &input.content,
            input.source_from_sequence,
            input.source_through_sequence,
            input.model.as_deref(),
            input.prompt_version.as_deref(),
            token_count,
        )?;
        for observation_id in &input.source_observation_ids {
            tx.execute(
                "INSERT INTO view_sources(view_id,observation_id) VALUES (?1,?2)",
                params![view.id, observation_id],
            )?;
        }
        save_operation(
            &tx,
            &input.idempotency_key,
            "view.create",
            &request_hash,
            &view,
        )?;
        tx.commit()?;
        Ok(MutationResult::created(view))
    }

    pub fn list_views(&self, scope_id: &str) -> Result<Vec<MemoryView>> {
        ensure_scope_exists(&self.conn, scope_id)?;
        let mut statement = self.conn.prepare(
            "SELECT id,scope_id,stream_id,kind,generation,content,source_from_sequence,source_through_sequence,previous_view_id,model,prompt_version,token_count,created_at
             FROM memory_views WHERE scope_id=?1 ORDER BY stream_id,kind,generation",
        )?;
        collect_rows(statement.query_map([scope_id], row_view)?)
    }

    pub fn recall_by_observation(
        &self,
        access: &ReadAccess,
        observation_id: &str,
    ) -> Result<Vec<MemoryEvent>> {
        let scope_id: String = self
            .conn
            .query_row(
                "SELECT scope_id FROM observations WHERE id=?1",
                [observation_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| {
                KernelError::new(
                    KernelErrorKind::NotFound,
                    format!("observation {observation_id} does not exist"),
                )
            })?;
        let access = ResolvedReadAccess::resolve(&self.conn, access)?;
        access.ensure_scope(&scope_id)?;
        self.recall_observation_sources(&access, observation_id)
    }

    fn recall_observation_sources(
        &self,
        access: &ResolvedReadAccess<'_>,
        observation_id: &str,
    ) -> Result<Vec<MemoryEvent>> {
        let mut statement = self.conn.prepare(
            "SELECT e.id,e.stream_id,e.sequence,e.scope_id,e.kind,e.actor_id,e.occurred_at,e.recorded_at,e.content_json,e.content_hash,e.token_count,e.sensitivity,e.metadata_json
             FROM memory_events e JOIN observation_sources s ON s.event_id=e.id
             WHERE s.observation_id=?1 ORDER BY e.stream_id,e.sequence",
        )?;
        collect_rows(statement.query_map([observation_id], row_event)?)?
            .into_iter()
            .map(|event| access.apply(event))
            .collect()
    }

    pub fn explain_observation(
        &self,
        access: &ReadAccess,
        observation_id: &str,
    ) -> Result<ObservationExplanation> {
        let observation = self
            .conn
            .query_row(
                "SELECT id,run_id,scope_id,kind,content,importance,confidence,event_time_from,event_time_to,source_start_sequence,source_end_sequence,observer_model,prompt_version,created_at
                 FROM observations WHERE id=?1",
                [observation_id],
                row_observation,
            )
            .optional()?
            .ok_or_else(|| {
                KernelError::new(
                    KernelErrorKind::NotFound,
                    format!("observation {observation_id} does not exist"),
                )
            })?;
        let access = ResolvedReadAccess::resolve(&self.conn, access)?;
        access.ensure_scope(&observation.scope_id)?;
        Ok(ObservationExplanation {
            observation,
            source_events: self.recall_observation_sources(&access, observation_id)?,
        })
    }

    pub fn explain_claim(&self, access: &ReadAccess, claim_id: &str) -> Result<ClaimExplanation> {
        let claim = query_claim(&self.conn, claim_id)?;
        let access = ResolvedReadAccess::resolve(&self.conn, access)?;
        access.ensure_scope(&claim.scope_id)?;
        let mut events_statement = self.conn.prepare(
            "SELECT DISTINCT e.id,e.stream_id,e.sequence,e.scope_id,e.kind,e.actor_id,e.occurred_at,e.recorded_at,e.content_json,e.content_hash,e.token_count,e.sensitivity,e.metadata_json
             FROM memory_events e JOIN claim_sources source ON source.event_id=e.id
             WHERE source.claim_id=?1
             ORDER BY e.stream_id,e.sequence",
        )?;
        let source_events = collect_rows(events_statement.query_map([claim_id], row_event)?)?
            .into_iter()
            .map(|event| access.apply(event))
            .collect::<Result<Vec<_>>>()?;
        Ok(ClaimExplanation {
            claim,
            source_events,
        })
    }

    pub fn search_full_text(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        self.search_with_options(scope_id, query, limit, SearchOptions::default())
    }

    pub fn search_full_text_advanced(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        self.search_with_options(
            scope_id,
            query,
            limit,
            SearchOptions {
                mode: SearchMode::Advanced,
                current_only: false,
            },
        )
    }

    pub fn search_with_options(
        &self,
        scope_id: &str,
        query: &str,
        limit: usize,
        options: SearchOptions,
    ) -> Result<Vec<SearchHit>> {
        validate_nonempty("search query", query)?;
        ensure!(
            limit > 0 && limit <= 1000,
            KernelError::new(
                KernelErrorKind::InvalidInput,
                "search limit must be from 1 to 1000",
            )
        );
        let scope_ids = retrieval_scope_ids(&self.conn, scope_id)?;
        let fts_query = bounded_fts_query(query, options.mode)?;
        search_fts(
            &self.conn,
            &scope_ids,
            &fts_query,
            limit,
            options.current_only,
        )
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
            KernelError::new(KernelErrorKind::InvalidInput, "max tokens must be positive",)
        );
        ensure!(
            recent_raw_tokens >= 0,
            KernelError::new(
                KernelErrorKind::InvalidInput,
                "recent raw tokens cannot be negative",
            )
        );
        let visible = visible_scope_ids(&self.conn, scope_id)?;
        let stream_scope: String = self
            .conn
            .query_row(
                "SELECT scope_id FROM memory_streams WHERE id=?1",
                [stream_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| {
                KernelError::new(
                    KernelErrorKind::NotFound,
                    format!("stream {stream_id} does not exist"),
                )
            })?;
        ensure!(
            visible.contains(&stream_scope)
                || scope_is_ancestor(&self.conn, scope_id, &stream_scope)?,
            KernelError::new(
                KernelErrorKind::ScopeViolation,
                format!("stream {stream_id} is not visible from scope {scope_id}"),
            )
        );
        let mut claims = query_claims_for_scopes(&self.conn, &visible, Some("active"))?;
        sort_claims_by_scope(&mut claims, &visible);
        let mut pending_claims =
            query_claim_candidates(&self.conn, &visible, Some("pending"), Some(257))?;
        pending_claims.extend(query_claim_candidates(
            &self.conn,
            &visible,
            Some("disputed"),
            Some(257),
        )?);
        let pending_truncated = pending_claims.len() > 256;
        sort_claims_by_scope(&mut pending_claims, &visible);
        pending_claims.truncate(256);
        let empty_payload = json!({"claims": [], "pendingClaims": [], "continuation": null,
            "continuityViews": [], "observations": [], "recentEvents": [], "recalledEvidence": []});
        let required_tokens: i64 = estimate_tokens(&empty_payload.to_string())
            + claims
                .iter()
                .map(|claim| rendering.claim_tokens(claim))
                .sum::<i64>();
        ensure!(
            required_tokens <= max_tokens,
            KernelError::new(
                KernelErrorKind::BudgetExceeded,
                format!(
                    "context budget too small: minimumRequiredTokens={required_tokens} for active claims"
                ),
            )
        );
        let mut diagnostics = ContextDiagnostics {
            estimated_tokens: required_tokens,
            omitted_items: Vec::new(),
            truncated: pending_truncated,
        };

        let mut continuity_views = Vec::new();
        let mut continuation = None;
        if let Some(view) = latest_view(&self.conn, stream_id, "continuation")? {
            let draft: ContinuationDraft = serde_json::from_str(&view.content)
                .context("reading structured continuation view")?;
            let cost = serialized_item_tokens(&draft).saturating_add(view_hint_extra(&view));
            if cost <= max_tokens - diagnostics.estimated_tokens {
                diagnostics.estimated_tokens += cost;
                continuation = Some(draft);
            } else {
                diagnostics.omitted_items.push(OmittedItem {
                    id: view.id,
                    reason: "context token budget".to_owned(),
                });
            }
        }

        let mut selected_pending_claims = Vec::new();
        for claim in pending_claims {
            let cost = rendering.claim_tokens(&claim);
            if cost <= max_tokens - diagnostics.estimated_tokens {
                diagnostics.estimated_tokens += cost;
                selected_pending_claims.push(claim);
            } else {
                diagnostics.omitted_items.push(OmittedItem {
                    id: claim.id,
                    reason: "context token budget".to_owned(),
                });
            }
        }

        let mut recent_events_reversed = Vec::new();
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
                    diagnostics.omitted_items.push(OmittedItem {
                        id: safe.id,
                        reason: "outside recent raw token budget".to_owned(),
                    });
                    diagnostics.truncated = true;
                    break 'raw;
                }
                raw_tokens += cost;
                recent_events_reversed.push(safe);
            }
            if page_len < 32 {
                break;
            }
        }
        recent_events_reversed.reverse();
        let recent_events = recent_events_reversed;
        diagnostics.estimated_tokens += raw_tokens;
        let mut recalled_evidence = Vec::new();
        let read_access = ReadAccess::agent(scope_id);
        if let Some(query) = query {
            let hits = self.search_with_options(scope_id, query.text, 10, query.options)?;
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
                    let cost = rendering.event_tokens(&event);
                    if cost <= max_tokens - diagnostics.estimated_tokens {
                        diagnostics.estimated_tokens += cost;
                        recalled_evidence.push(event);
                    } else {
                        diagnostics.omitted_items.push(OmittedItem {
                            id,
                            reason: "context token budget".to_owned(),
                        });
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

        let mut selected_continuity_ids = Vec::new();
        if let Some(view) = latest_view(&self.conn, stream_id, "continuity")? {
            let cost = rendering.view_tokens(&view);
            if cost <= max_tokens - diagnostics.estimated_tokens {
                diagnostics.estimated_tokens += cost;
                selected_continuity_ids.push(view.id.clone());
                continuity_views.push(view);
            } else {
                diagnostics.omitted_items.push(OmittedItem {
                    id: view.id,
                    reason: "context token budget".to_owned(),
                });
            }
        }

        let mut observation_scopes = visible.clone();
        if !observation_scopes.contains(&stream_scope) {
            observation_scopes.push(stream_scope);
        }
        let mut candidates = query_observations_for_scopes(&self.conn, &observation_scopes)?;
        if candidates.len() > 256 {
            diagnostics.truncated = true;
            candidates.truncate(256);
        }
        let mut observations = Vec::new();
        let candidate_ids: Vec<&str> = candidates
            .iter()
            .map(|observation| observation.id.as_str())
            .collect();
        let represented_observations =
            observations_in_views(&self.conn, &candidate_ids, &selected_continuity_ids)?;
        for observation in candidates {
            let duplicated_by_raw =
                observation_has_events(&self.conn, &observation.id, &represented_event_ids)?;
            let represented_by_view = represented_observations.contains(&observation.id);
            if duplicated_by_raw || represented_by_view {
                diagnostics.omitted_items.push(OmittedItem {
                    id: observation.id,
                    reason: if duplicated_by_raw {
                        "source events already present in raw tail"
                    } else {
                        "already represented by continuity view"
                    }
                    .to_owned(),
                });
                continue;
            }
            let cost = rendering.observation_tokens(&observation);
            if cost <= max_tokens - diagnostics.estimated_tokens {
                diagnostics.estimated_tokens += cost;
                observations.push(observation);
            } else {
                diagnostics.omitted_items.push(OmittedItem {
                    id: observation.id,
                    reason: "context token budget".to_owned(),
                });
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
