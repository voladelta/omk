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
        validate_nonempty("idempotency key", idempotency_key)?;
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
        let request_hash = operation_request_hash(operation, &request)?;
        let tx = self.immediate()?;
        if let Some(prior) = prior_result::<Claim>(&tx, idempotency_key, operation, &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        ensure_scope_exists(&tx, scope_id)?;
        validate_claim_event_sources(&tx, scope_id, source_event_ids)?;
        let command_event = insert_memory_command_event(&tx, scope_id, operation, &request)?;

        let value_hash = hash_json(&value);
        let kind_text = enum_text(&kind);
        let active = query_active_claim_member(
            &tx,
            scope_id,
            &kind_text,
            subject,
            predicate,
            &cardinality,
            &value_hash,
        )?;
        if requested_status == ClaimStatus::Active
            && let Some(existing) = &active
            && existing.value == value
        {
            set_command_event_owner(&tx, &command_event.id, &existing.id)?;
            attach_event_sources(&tx, &existing.id, source_event_ids)?;
            attach_event_sources(&tx, &existing.id, std::slice::from_ref(&command_event.id))?;
            save_operation(&tx, idempotency_key, operation, &request_hash, existing)?;
            tx.commit()?;
            return Ok(MutationResult::created(existing.clone()));
        }
        let status = if requested_status == ClaimStatus::Active && active.is_some() {
            ClaimStatus::Disputed
        } else {
            requested_status
        };
        let timestamp = now();
        let claim = Claim {
            id: Uuid::new_v4().to_string(),
            origin_run_id: None,
            scope_id: scope_id.to_owned(),
            kind,
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            cardinality,
            value,
            value_hash,
            modality,
            status,
            authority,
            confidence: 1.0,
            supersedes_id: None,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        if claim.status == ClaimStatus::Active {
            ensure_claim_slot(&tx, &claim)?;
        }
        set_command_event_owner(&tx, &command_event.id, &claim.id)?;
        insert_claim(&tx, &claim)?;
        attach_event_sources(&tx, &claim.id, source_event_ids)?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        index_claim(&tx, &claim)?;
        save_operation(&tx, idempotency_key, operation, &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn confirm_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({"claimId": claim_id});
        let request_hash = operation_request_hash("claim.confirm", &request)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Claim>(&tx, idempotency_key, "claim.confirm", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let mut claim = query_claim(&tx, claim_id)?;
        ensure!(
            matches!(claim.status, ClaimStatus::Pending | ClaimStatus::Disputed),
            "claim must be pending or disputed to confirm"
        );
        let command_event =
            insert_memory_command_event(&tx, &claim.scope_id, "claim.confirm", &request)?;
        ensure_claim_slot(&tx, &claim)?;
        if let Some(existing) = query_active_claim_member(
            &tx,
            &claim.scope_id,
            &enum_text(&claim.kind),
            &claim.subject,
            &claim.predicate,
            &claim.cardinality,
            &claim.value_hash,
        )?
        .filter(|existing| existing.value == claim.value)
        {
            set_command_event_owner(&tx, &command_event.id, &existing.id)?;
            copy_claim_sources(&tx, &claim.id, &existing.id)?;
            attach_event_sources(&tx, &existing.id, std::slice::from_ref(&command_event.id))?;
            tx.execute(
                "UPDATE claims SET status='rejected',updated_at=?2 WHERE id=?1",
                params![claim.id, now()],
            )?;
            save_operation(
                &tx,
                idempotency_key,
                "claim.confirm",
                &request_hash,
                &existing,
            )?;
            tx.commit()?;
            return Ok(MutationResult::created(existing));
        }
        supersede_other_active_claims(&tx, &claim, Some(&claim.id))?;
        claim.status = ClaimStatus::Active;
        claim.modality = ClaimModality::AcceptedDecision;
        claim.authority = ClaimAuthority::ExplicitUser;
        claim.updated_at = now();
        set_command_event_owner(&tx, &command_event.id, &claim.id)?;
        tx.execute(
            "UPDATE claims SET status='active',modality='accepted-decision',authority='explicit-user',updated_at=?2 WHERE id=?1",
            params![claim.id, claim.updated_at],
        )?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        save_operation(&tx, idempotency_key, "claim.confirm", &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn correct_claim(
        &mut self,
        claim_id: &str,
        value: Value,
        source_event_ids: &[String],
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({
            "claimId": claim_id,
            "value": value,
            "sourceEventIds": source_event_ids
        });
        let request_hash = operation_request_hash("claim.correct", &request)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Claim>(&tx, idempotency_key, "claim.correct", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let old = query_claim(&tx, claim_id)?;
        let old_id = old.id.clone();
        validate_claim_event_sources(&tx, &old.scope_id, source_event_ids)?;
        let command_event =
            insert_memory_command_event(&tx, &old.scope_id, "claim.correct", &request)?;
        let timestamp = now();
        if old.cardinality == ClaimCardinality::Single {
            tx.execute(
                "UPDATE claims SET status='superseded',updated_at=?1 WHERE scope_id=?2 AND kind=?3 AND subject=?4 AND predicate=?5 AND cardinality='single' AND status='active'",
                params![timestamp, old.scope_id, enum_text(&old.kind), old.subject, old.predicate],
            )?;
        }
        tx.execute(
            "UPDATE claims SET status='superseded',updated_at=?2 WHERE id=?1",
            params![old.id, timestamp],
        )?;
        let claim = Claim {
            id: Uuid::new_v4().to_string(),
            origin_run_id: None,
            scope_id: old.scope_id,
            kind: old.kind,
            subject: old.subject,
            predicate: old.predicate,
            cardinality: old.cardinality,
            value_hash: hash_json(&value),
            value,
            modality: ClaimModality::ExplicitAssertion,
            status: ClaimStatus::Active,
            authority: ClaimAuthority::ExplicitUser,
            confidence: 1.0,
            supersedes_id: Some(old_id.clone()),
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        ensure_claim_slot(&tx, &claim)?;
        if let Some(existing) = query_active_claim_member(
            &tx,
            &claim.scope_id,
            &enum_text(&claim.kind),
            &claim.subject,
            &claim.predicate,
            &claim.cardinality,
            &claim.value_hash,
        )? {
            set_command_event_owner(&tx, &command_event.id, &existing.id)?;
            copy_claim_sources(&tx, &old_id, &existing.id)?;
            attach_event_sources(&tx, &existing.id, source_event_ids)?;
            attach_event_sources(&tx, &existing.id, std::slice::from_ref(&command_event.id))?;
            save_operation(
                &tx,
                idempotency_key,
                "claim.correct",
                &request_hash,
                &existing,
            )?;
            tx.commit()?;
            return Ok(MutationResult::created(existing));
        }
        set_command_event_owner(&tx, &command_event.id, &claim.id)?;
        insert_claim(&tx, &claim)?;
        attach_event_sources(&tx, &claim.id, source_event_ids)?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        index_claim(&tx, &claim)?;
        save_operation(&tx, idempotency_key, "claim.correct", &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn rescope_claim(
        &mut self,
        claim_id: &str,
        new_scope_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({"claimId": claim_id, "newScopeId": new_scope_id});
        let request_hash = operation_request_hash("claim.rescope", &request)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Claim>(&tx, idempotency_key, "claim.rescope", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        ensure_scope_exists(&tx, new_scope_id)?;
        let old = query_claim(&tx, claim_id)?;
        ensure!(
            new_scope_id == old.scope_id || scope_is_ancestor(&tx, new_scope_id, &old.scope_id)?,
            "claim rescope target must be the current scope or one of its ancestors"
        );
        validate_existing_claim_sources_visible(&tx, claim_id, new_scope_id)?;
        let command_event =
            insert_memory_command_event(&tx, new_scope_id, "claim.rescope", &request)?;
        let timestamp = now();
        tx.execute(
            "UPDATE claims SET status='superseded',updated_at=?2 WHERE id=?1",
            params![old.id, timestamp],
        )?;
        let claim = Claim {
            id: Uuid::new_v4().to_string(),
            scope_id: new_scope_id.to_owned(),
            status: if old.status == ClaimStatus::Active {
                ClaimStatus::Active
            } else {
                ClaimStatus::Pending
            },
            supersedes_id: Some(old.id.clone()),
            created_at: timestamp.clone(),
            updated_at: timestamp,
            ..old
        };
        if claim.status == ClaimStatus::Active {
            ensure_claim_slot(&tx, &claim)?;
            if let Some(existing) = query_active_claim_member(
                &tx,
                &claim.scope_id,
                &enum_text(&claim.kind),
                &claim.subject,
                &claim.predicate,
                &claim.cardinality,
                &claim.value_hash,
            )? {
                set_command_event_owner(&tx, &command_event.id, &existing.id)?;
                copy_claim_sources(&tx, claim_id, &existing.id)?;
                attach_event_sources(&tx, &existing.id, std::slice::from_ref(&command_event.id))?;
                save_operation(
                    &tx,
                    idempotency_key,
                    "claim.rescope",
                    &request_hash,
                    &existing,
                )?;
                tx.commit()?;
                return Ok(MutationResult::created(existing));
            }
            supersede_other_active_claims(&tx, &claim, None)?;
        }
        set_command_event_owner(&tx, &command_event.id, &claim.id)?;
        insert_claim(&tx, &claim)?;
        copy_claim_sources(&tx, claim_id, &claim.id)?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        index_claim(&tx, &claim)?;
        save_operation(&tx, idempotency_key, "claim.rescope", &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn reject_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({"claimId": claim_id, "status": ClaimStatus::Rejected});
        let request_hash = operation_request_hash("claim.reject", &request)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Claim>(&tx, idempotency_key, "claim.reject", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let mut claim = query_claim(&tx, claim_id)?;
        ensure!(
            matches!(claim.status, ClaimStatus::Pending | ClaimStatus::Disputed),
            "claim must be pending or disputed to reject"
        );
        let command_event =
            insert_memory_command_event(&tx, &claim.scope_id, "claim.reject", &request)?;
        set_command_event_owner(&tx, &command_event.id, &claim.id)?;
        claim.status = ClaimStatus::Rejected;
        claim.updated_at = now();
        tx.execute(
            "UPDATE claims SET status='rejected',updated_at=?2 WHERE id=?1",
            params![claim.id, claim.updated_at],
        )?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        save_operation(&tx, idempotency_key, "claim.reject", &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
    }

    pub fn forget_claim(
        &mut self,
        claim_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<Claim>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request = json!({"claimId": claim_id, "status": ClaimStatus::Expired});
        let request_hash = operation_request_hash("claim.forget", &request)?;
        let tx = self.immediate()?;
        if let Some(prior) =
            prior_result::<Claim>(&tx, idempotency_key, "claim.forget", &request_hash)?
        {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        let mut claim = query_claim(&tx, claim_id)?;
        ensure!(
            matches!(
                claim.status,
                ClaimStatus::Pending | ClaimStatus::Active | ClaimStatus::Disputed
            ),
            "claim must be pending, active, or disputed to forget"
        );
        let command_event =
            insert_memory_command_event(&tx, &claim.scope_id, "claim.forget", &request)?;
        set_command_event_owner(&tx, &command_event.id, &claim.id)?;
        claim.status = ClaimStatus::Expired;
        claim.updated_at = now();
        tx.execute(
            "UPDATE claims SET status='expired',updated_at=?2 WHERE id=?1",
            params![claim.id, claim.updated_at],
        )?;
        attach_event_sources(&tx, &claim.id, std::slice::from_ref(&command_event.id))?;
        save_operation(&tx, idempotency_key, "claim.forget", &request_hash, &claim)?;
        tx.commit()?;
        Ok(MutationResult::created(claim))
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
        query_claims_for_scopes(&self.conn, &scope_ids, status_text.as_deref())
    }

    pub fn reconcile(
        &mut self,
        scope_id: &str,
        idempotency_key: &str,
    ) -> Result<MutationResult<ReconciliationSummary>> {
        validate_nonempty("idempotency key", idempotency_key)?;
        let request_hash = operation_request_hash("claim.reconcile", &scope_id)?;
        let tx = self.immediate()?;
        if let Some(prior) = prior_result::<ReconciliationSummary>(
            &tx,
            idempotency_key,
            "claim.reconcile",
            &request_hash,
        )? {
            tx.commit()?;
            return Ok(MutationResult::replayed(prior));
        }
        ensure_scope_exists(&tx, scope_id)?;
        let pending = query_claims_for_scopes(&tx, &[scope_id.to_owned()], Some("pending"))?;
        let mut summary = ReconciliationSummary {
            activated: Vec::new(),
            disputed: Vec::new(),
            duplicates_rejected: Vec::new(),
            left_pending: Vec::new(),
        };
        for claim in pending {
            if claim.origin_run_id.is_some()
                || matches!(
                    claim.modality,
                    ClaimModality::Proposal | ClaimModality::Inference | ClaimModality::Observation
                )
            {
                summary.left_pending.push(claim.id);
                continue;
            }
            let active = query_active_claim_member(
                &tx,
                &claim.scope_id,
                &enum_text(&claim.kind),
                &claim.subject,
                &claim.predicate,
                &claim.cardinality,
                &claim.value_hash,
            )?;
            match active {
                None => {
                    ensure_claim_slot(&tx, &claim)?;
                    tx.execute(
                        "UPDATE claims SET status='active',authority='trusted-source',updated_at=?2 WHERE id=?1",
                        params![claim.id, now()],
                    )?;
                    summary.activated.push(claim.id);
                }
                Some(existing) if existing.value == claim.value => {
                    copy_claim_sources(&tx, &claim.id, &existing.id)?;
                    tx.execute(
                        "UPDATE claims SET status='rejected',updated_at=?2 WHERE id=?1",
                        params![claim.id, now()],
                    )?;
                    summary.duplicates_rejected.push(claim.id);
                }
                Some(_) => {
                    tx.execute(
                        "UPDATE claims SET status='disputed',updated_at=?2 WHERE id=?1",
                        params![claim.id, now()],
                    )?;
                    summary.disputed.push(claim.id);
                }
            }
        }
        save_operation(
            &tx,
            idempotency_key,
            "claim.reconcile",
            &request_hash,
            &summary,
        )?;
        tx.commit()?;
        Ok(MutationResult::created(summary))
    }
}
