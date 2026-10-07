use super::*;

impl MemoryStore {
    #[allow(clippy::too_many_arguments)]
    pub fn remember_claim(
        &mut self,
        scope_id: &str,
        kind: ClaimKind,
        subject: &str,
        predicate: &str,
        value: Value,
        source_event_ids: &[String],
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.remember_claim_with_cardinality(
            scope_id,
            kind,
            subject,
            predicate,
            ClaimCardinality::Single,
            value,
            source_event_ids,
            idempotency_key,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn remember_claim_with_cardinality(
        &mut self,
        scope_id: &str,
        kind: ClaimKind,
        subject: &str,
        predicate: &str,
        cardinality: ClaimCardinality,
        value: Value,
        source_event_ids: &[String],
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.create_direct_claim(
            scope_id,
            kind,
            subject,
            predicate,
            cardinality,
            value,
            ClaimModality::ExplicitAssertion,
            ClaimStatus::Active,
            ClaimAuthority::ExplicitUser,
            source_event_ids,
            idempotency_key,
            "claim.remember",
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn propose_claim(
        &mut self,
        scope_id: &str,
        kind: ClaimKind,
        subject: &str,
        predicate: &str,
        value: Value,
        source_event_ids: &[String],
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.propose_claim_with_cardinality(
            scope_id,
            kind,
            subject,
            predicate,
            ClaimCardinality::Single,
            value,
            source_event_ids,
            idempotency_key,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn propose_claim_with_cardinality(
        &mut self,
        scope_id: &str,
        kind: ClaimKind,
        subject: &str,
        predicate: &str,
        cardinality: ClaimCardinality,
        value: Value,
        source_event_ids: &[String],
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.create_direct_claim(
            scope_id,
            kind,
            subject,
            predicate,
            cardinality,
            value,
            ClaimModality::Proposal,
            ClaimStatus::Pending,
            ClaimAuthority::ExplicitUser,
            source_event_ids,
            idempotency_key,
            "claim.propose",
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_direct_claim(
        &mut self,
        scope_id: &str,
        kind: ClaimKind,
        subject: &str,
        predicate: &str,
        cardinality: ClaimCardinality,
        value: Value,
        modality: ClaimModality,
        requested_status: ClaimStatus,
        authority: ClaimAuthority,
        source_event_ids: &[String],
        idempotency_key: &str,
        operation: &str,
    ) -> Result<MutationResult<Claim>> {
        let subject = subject.trim();
        let predicate = predicate.trim();
        validate_nonempty("subject", subject)?;
        validate_nonempty("predicate", predicate)?;
        let request = json!({
            "scopeId": scope_id,
            "kind": kind,
            "subject": subject,
            "predicate": predicate,
            "cardinality": cardinality,
            "value": value,
            "modality": modality,
            "requestedStatus": requested_status,
            "authority": authority,
            "sourceEventIds": source_event_ids
        });
        self.mutate(operation, idempotency_key, &request, |tx| {
            ensure_scope_exists(tx, scope_id)?;
            validate_claim_event_sources(tx, scope_id, source_event_ids)?;
            let command_event = insert_memory_command_event(tx, scope_id, operation, &request)?;
            let mut claim = new_claim(scope_id, kind, subject, predicate, cardinality, value);
            claim.modality = modality;
            claim.status = requested_status;
            claim.authority = authority;
            if claim.status == ClaimStatus::Active
                && let Some(existing) = query_active_claim_member(tx, &claim)?
            {
                if existing.value == claim.value {
                    attach_command_event(tx, &command_event, &existing.id)?;
                    attach_event_sources(tx, &existing.id, source_event_ids)?;
                    return Ok(existing);
                }
                // A different active value is a conflict, never a silent replace.
                claim.status = ClaimStatus::Disputed;
            }
            if claim.status == ClaimStatus::Active {
                ensure_claim_slot(tx, &claim)?;
            }
            insert_claim(tx, &claim)?;
            attach_command_event(tx, &command_event, &claim.id)?;
            attach_event_sources(tx, &claim.id, source_event_ids)?;
            index_claim(tx, &claim)?;
            Ok(claim)
        })
    }

    pub fn confirm_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        let request = json!({"claimId": claim_id});
        self.mutate("claim.confirm", idempotency_key, &request, |tx| {
            let mut claim = query_claim(tx, claim_id)?;
            ensure!(
                matches!(claim.status, ClaimStatus::Pending | ClaimStatus::Disputed),
                KernelError::invalid_input("claim must be pending or disputed to confirm")
            );
            let command_event =
                insert_memory_command_event(tx, &claim.scope_id, "claim.confirm", &request)?;
            ensure_claim_slot(tx, &claim)?;
            if let Some(existing) = query_active_claim_member(tx, &claim)?
                .filter(|existing| existing.value == claim.value)
            {
                attach_command_event(tx, &command_event, &existing.id)?;
                copy_claim_sources(tx, &claim.id, &existing.id)?;
                set_claim_status(tx, &claim.id, ClaimStatus::Rejected)?;
                return Ok(existing);
            }
            supersede_other_active_claims(tx, &claim, Some(&claim.id))?;
            claim.status = ClaimStatus::Active;
            claim.modality = ClaimModality::AcceptedDecision;
            claim.authority = ClaimAuthority::ExplicitUser;
            claim.updated_at = now();
            tx.execute(
                "UPDATE claims SET status='active',modality='accepted-decision',authority='explicit-user',updated_at=?2 WHERE id=?1",
                params![claim.id, claim.updated_at],
            )?;
            attach_command_event(tx, &command_event, &claim.id)?;
            Ok(claim)
        })
    }

    pub fn correct_claim(
        &mut self,
        claim_id: &str,
        value: Value,
        source_event_ids: &[String],
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        let request = json!({
            "claimId": claim_id,
            "value": value,
            "sourceEventIds": source_event_ids
        });
        self.mutate("claim.correct", idempotency_key, &request, |tx| {
            let old = query_claim(tx, claim_id)?;
            ensure!(
                matches!(
                    old.status,
                    ClaimStatus::Active | ClaimStatus::Pending | ClaimStatus::Disputed
                ),
                KernelError::invalid_input("claim must be active, pending, or disputed to correct")
            );
            validate_claim_event_sources(tx, &old.scope_id, source_event_ids)?;
            let command_event =
                insert_memory_command_event(tx, &old.scope_id, "claim.correct", &request)?;
            supersede_other_active_claims(tx, &old, None)?;
            set_claim_status(tx, &old.id, ClaimStatus::Superseded)?;
            let mut claim = new_claim(
                &old.scope_id,
                old.kind.clone(),
                &old.subject,
                &old.predicate,
                old.cardinality.clone(),
                value,
            );
            claim.supersedes_id = Some(old.id.clone());
            ensure_claim_slot(tx, &claim)?;
            // Only a set slot can still hold an active member here.
            if let Some(existing) = query_active_claim_member(tx, &claim)? {
                attach_command_event(tx, &command_event, &existing.id)?;
                copy_claim_sources(tx, &old.id, &existing.id)?;
                attach_event_sources(tx, &existing.id, source_event_ids)?;
                return Ok(existing);
            }
            insert_claim(tx, &claim)?;
            attach_command_event(tx, &command_event, &claim.id)?;
            attach_event_sources(tx, &claim.id, source_event_ids)?;
            index_claim(tx, &claim)?;
            Ok(claim)
        })
    }

    pub fn rescope_claim(
        &mut self,
        claim_id: &str,
        new_scope_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        let request = json!({"claimId": claim_id, "newScopeId": new_scope_id});
        self.mutate("claim.rescope", idempotency_key, &request, |tx| {
            ensure_scope_exists(tx, new_scope_id)?;
            let old = query_claim(tx, claim_id)?;
            ensure!(
                visible_scope_ids(tx, &old.scope_id)?
                    .iter()
                    .any(|scope| scope == new_scope_id),
                KernelError::scope_violation(
                    "claim rescope target must be the current scope or one of its ancestors",
                )
            );
            validate_existing_claim_sources_visible(tx, claim_id, new_scope_id)?;
            let command_event =
                insert_memory_command_event(tx, new_scope_id, "claim.rescope", &request)?;
            set_claim_status(tx, &old.id, ClaimStatus::Superseded)?;
            let timestamp = now();
            let claim = Claim {
                id: Uuid::new_v4().to_string(),
                scope_id: new_scope_id.to_owned(),
                // Only active state stays active. Disputed claims stay disputed, so a
                // rescope cannot launder a conflict into something reconcile accepts.
                status: match old.status {
                    ClaimStatus::Active => ClaimStatus::Active,
                    ClaimStatus::Disputed => ClaimStatus::Disputed,
                    _ => ClaimStatus::Pending,
                },
                supersedes_id: Some(old.id.clone()),
                created_at: timestamp.clone(),
                updated_at: timestamp,
                ..old
            };
            if claim.status == ClaimStatus::Active {
                ensure_claim_slot(tx, &claim)?;
                if let Some(existing) = query_active_claim_member(tx, &claim)? {
                    ensure!(
                        existing.value == claim.value,
                        KernelError::claim_conflict(
                            "rescope destination has a different active value; confirm or correct the conflict explicitly"
                        )
                    );
                    attach_command_event(tx, &command_event, &existing.id)?;
                    copy_claim_sources(tx, claim_id, &existing.id)?;
                    return Ok(existing);
                }
            }
            insert_claim(tx, &claim)?;
            attach_command_event(tx, &command_event, &claim.id)?;
            copy_claim_sources(tx, claim_id, &claim.id)?;
            index_claim(tx, &claim)?;
            Ok(claim)
        })
    }

    pub fn reject_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.end_claim(
            "claim.reject",
            claim_id,
            ClaimStatus::Rejected,
            &[ClaimStatus::Pending, ClaimStatus::Disputed],
            "claim must be pending or disputed to reject",
            idempotency_key,
        )
    }

    pub fn forget_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        self.end_claim(
            "claim.forget",
            claim_id,
            ClaimStatus::Expired,
            &[
                ClaimStatus::Pending,
                ClaimStatus::Active,
                ClaimStatus::Disputed,
            ],
            "claim must be pending, active, or disputed to forget",
            idempotency_key,
        )
    }

    /// Move a claim to a final status, keeping its history.
    fn end_claim(
        &mut self,
        operation: &str,
        claim_id: &str,
        status: ClaimStatus,
        allowed: &[ClaimStatus],
        not_allowed: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        let request = json!({"claimId": claim_id, "status": status});
        self.mutate(operation, idempotency_key, &request, |tx| {
            let mut claim = query_claim(tx, claim_id)?;
            ensure!(
                allowed.contains(&claim.status),
                KernelError::invalid_input(not_allowed)
            );
            let command_event =
                insert_memory_command_event(tx, &claim.scope_id, operation, &request)?;
            claim.updated_at = set_claim_status(tx, &claim.id, status.clone())?;
            claim.status = status;
            attach_command_event(tx, &command_event, &claim.id)?;
            Ok(claim)
        })
    }

    pub fn list_claims(
        &self,
        scope_id: &str,
        include_ancestors: bool,
        status: Option<ClaimStatus>,
    ) -> Result<Vec<Claim>> {
        let scope_ids = if include_ancestors {
            visible_scope_ids(&self.conn, scope_id)?
        } else {
            ensure_scope_exists(&self.conn, scope_id)?;
            vec![scope_id.to_owned()]
        };
        let status_text = status.as_ref().map(enum_text);
        query_claims_for_scopes(&self.conn, &scope_ids, status_text.as_deref(), None)
    }

    pub fn reconcile(
        &mut self,
        scope_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<ReconciliationSummary>> {
        self.mutate("claim.reconcile", idempotency_key, &scope_id, |tx| {
            ensure_scope_exists(tx, scope_id)?;
            let pending =
                query_claims_for_scopes(tx, &[scope_id.to_owned()], Some("pending"), None)?;
            let mut summary = ReconciliationSummary {
                activated: Vec::new(),
                disputed: Vec::new(),
                duplicates_rejected: Vec::new(),
                left_pending: Vec::new(),
            };
            for claim in pending {
                // Only a trusted ingestion path may activate state here. Claims
                // that carry user or model authority need an explicit claim command.
                if claim.origin_run_id.is_some()
                    || matches!(
                        claim.modality,
                        ClaimModality::Proposal
                            | ClaimModality::Inference
                            | ClaimModality::Observation
                    )
                {
                    summary.left_pending.push(claim.id);
                    continue;
                }
                match query_active_claim_member(tx, &claim)? {
                    None if !matches!(claim.authority, ClaimAuthority::TrustedSource) => {
                        summary.left_pending.push(claim.id);
                    }
                    None => {
                        ensure_claim_slot(tx, &claim)?;
                        set_claim_status(tx, &claim.id, ClaimStatus::Active)?;
                        summary.activated.push(claim.id);
                    }
                    Some(existing) if existing.value == claim.value => {
                        copy_claim_sources(tx, &claim.id, &existing.id)?;
                        set_claim_status(tx, &claim.id, ClaimStatus::Rejected)?;
                        summary.duplicates_rejected.push(claim.id);
                    }
                    Some(_) => {
                        set_claim_status(tx, &claim.id, ClaimStatus::Disputed)?;
                        summary.disputed.push(claim.id);
                    }
                }
            }
            Ok(summary)
        })
    }
}

/// A new active, explicit-user claim with full confidence.
fn new_claim(
    scope_id: &str,
    kind: ClaimKind,
    subject: &str,
    predicate: &str,
    cardinality: ClaimCardinality,
    value: Value,
) -> Claim {
    let timestamp = now();
    Claim {
        id: Uuid::new_v4().to_string(),
        origin_run_id: None,
        scope_id: scope_id.to_owned(),
        kind,
        subject: subject.to_owned(),
        predicate: predicate.to_owned(),
        cardinality,
        value_hash: hash_json(&value),
        value,
        modality: ClaimModality::ExplicitAssertion,
        status: ClaimStatus::Active,
        authority: ClaimAuthority::ExplicitUser,
        confidence: 1.0,
        supersedes_id: None,
        created_at: timestamp.clone(),
        updated_at: timestamp,
    }
}

/// Set a claim's status and return the new `updated_at`.
fn set_claim_status(conn: &Connection, claim_id: &str, status: ClaimStatus) -> Result<String> {
    let updated_at = now();
    conn.execute(
        "UPDATE claims SET status=?2,updated_at=?3 WHERE id=?1",
        params![claim_id, enum_text(&status), updated_at],
    )?;
    Ok(updated_at)
}
