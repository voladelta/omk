use super::*;

#[test]
fn set_claims_keep_distinct_members_and_lock_slot_cardinality() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let first = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Constraint,
            "project",
            "requirement",
            ClaimCardinality::Set,
            json!({"name": "exact recall", "enabled": true}),
            &[],
            "set-1",
        )
        .unwrap();
    fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Constraint,
            "project",
            "requirement",
            ClaimCardinality::Set,
            json!("use v4"),
            &[],
            "set-2",
        )
        .unwrap();
    let duplicate = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Constraint,
            "project",
            "requirement",
            ClaimCardinality::Set,
            json!({"enabled": true, "name": "exact recall"}),
            &[],
            "set-3",
        )
        .unwrap();

    assert_eq!(duplicate.id, first.id);
    assert_eq!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Active))
            .unwrap()
            .len(),
        2
    );
    let mismatch = fixture.store.remember_claim(
        "user",
        ClaimKind::Constraint,
        "project",
        "requirement",
        ClaimCardinality::Single,
        json!("one value"),
        &[],
        "single-mismatch",
    );
    assert!(mismatch.unwrap_err().to_string().contains("cardinality"));
}

#[test]
fn proposals_do_not_replace_state_but_explicit_corrections_do() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let active = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "remember-1",
        )
        .unwrap();
    let proposal = fixture
        .store
        .propose_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!(["ETH", "USDG"]),
            &[],
            "proposal-1",
        )
        .unwrap();
    let summary = fixture.store.reconcile("user", "reconcile-1").unwrap();
    // A proposal that conflicts with active state is labelled, never applied.
    assert_eq!(summary.disputed, vec![proposal.id.clone()]);
    assert_eq!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Active))
            .unwrap()[0]
            .value,
        json!("ETH")
    );

    let corrected = fixture
        .store
        .correct_claim(&active.id, json!(["ETH", "USDG"]), &[], "correct-1")
        .unwrap();
    assert_eq!(corrected.status, ClaimStatus::Active);
    assert_eq!(corrected.supersedes_id.as_deref(), Some(active.id.as_str()));
    assert_eq!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Superseded))
            .unwrap()[0]
            .id,
        active.id
    );
}

#[test]
fn confirming_a_conflicting_single_claim_supersedes_active_state() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let active = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "remember",
        )
        .unwrap();
    let pending = fixture
        .store
        .propose_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("USDG"),
            &[],
            "propose",
        )
        .unwrap();

    let confirmed = fixture.store.confirm_claim(&pending.id, "confirm").unwrap();
    assert_eq!(confirmed.id, pending.id);
    assert_eq!(confirmed.status, ClaimStatus::Active);
    // Modality records what the source said; confirmation changes only status.
    assert_eq!(confirmed.modality, ClaimModality::Proposal);
    assert_eq!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Superseded))
            .unwrap()[0]
            .id,
        active.id
    );
}

#[test]
fn claim_logical_keys_are_normalized_before_lookup() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let first = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "remember-1",
        )
        .unwrap();
    let duplicate = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            " launch ",
            " asset ",
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "remember-2",
        )
        .unwrap();

    assert_eq!(duplicate.id, first.id);
    assert_eq!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Active))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn claim_commands_enforce_lifecycle_transitions() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let active = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "remember",
        )
        .unwrap();
    let proposal = fixture
        .store
        .propose_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("USDG"),
            &[],
            "proposal",
        )
        .unwrap();

    assert!(
        fixture
            .store
            .reject_claim(&active.id, "reject-active")
            .unwrap_err()
            .to_string()
            .contains("pending or disputed")
    );
    let rejected = fixture
        .store
        .reject_claim(&proposal.id, "reject-proposal")
        .unwrap();
    assert_eq!(rejected.status, ClaimStatus::Rejected);
    assert!(
        fixture
            .store
            .confirm_claim(&active.id, "confirm-active")
            .unwrap_err()
            .to_string()
            .contains("pending or disputed")
    );
    assert!(
        fixture
            .store
            .confirm_claim(&rejected.id, "confirm-rejected")
            .unwrap_err()
            .to_string()
            .contains("pending or disputed")
    );
    assert_eq!(
        fixture
            .store
            .forget_claim(&active.id, "forget-active")
            .unwrap()
            .status,
        ClaimStatus::Expired
    );
}

#[test]
fn rescoping_an_observer_claim_keeps_it_pending() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("thread", ScopeKind::Thread, Some("user"));
    let event = fixture.event(
        "thread",
        "thread-stream",
        "Use ETH",
        Sensitivity::Normal,
        "event",
    );
    let plan = fixture
        .store
        .plan_observation(
            "thread",
            "thread-stream",
            10_000,
            "fake",
            "v1",
            Some("plan"),
        )
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let commit = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&event.id, "ETH"), "commit")
        .unwrap();

    let promoted = fixture
        .store
        .rescope_claim(&commit.claims[0].id, "user", "rescope")
        .unwrap();
    assert_eq!(promoted.status, ClaimStatus::Pending);
    assert_eq!(
        promoted.origin_run_id.as_deref(),
        Some(plan.run_id.as_str())
    );
    let reconciliation = fixture.store.reconcile("user", "reconcile").unwrap();
    assert_eq!(reconciliation.left_pending, vec![promoted.id.clone()]);
}

#[test]
fn direct_claims_are_command_sourced_and_pending_state_reaches_context() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("project", ScopeKind::Project, Some("user"));
    fixture.event("project", "stream", "work", Sensitivity::Normal, "event");
    let active = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Preference,
            "user",
            "editor",
            ClaimCardinality::Single,
            json!("vim"),
            &[],
            "remember",
        )
        .unwrap();
    let proposal = fixture
        .store
        .propose_claim(
            "project",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("USDG"),
            &[],
            "proposal",
        )
        .unwrap();
    let explanation = fixture
        .store
        .explain_claim(&access("user"), &active.id)
        .unwrap();
    assert_eq!(explanation.source_events.len(), 1);
    assert_eq!(explanation.source_events[0].kind, EventKind::MemoryCommand);

    let context = fixture
        .store
        .compose_context("project", "stream", 1_000, 100, None)
        .unwrap();
    assert_eq!(context.pending_claims.len(), 1);
    assert_eq!(context.pending_claims[0].id, proposal.id);
}

#[test]
fn observer_claims_cannot_contradict_an_existing_slot_cardinality() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "remember",
        )
        .unwrap();
    let event = fixture.event(
        "user",
        "stream",
        "Launch assets",
        Sensitivity::Normal,
        "event",
    );
    let plan = plan_run(&mut fixture, "user", "stream", "plan");
    let mut result = observer_result(&event.id, "BTC");
    result.claims[0].cardinality = ClaimCardinality::Set;
    let error = fixture
        .store
        .commit_observation(&plan.run_id, result, "commit")
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().map(KernelError::kind),
        Some(KernelErrorKind::InvalidInput)
    );
    let message = error.to_string();
    assert!(message.contains("claim 0") && message.contains("single"));
    // Nothing was written, so the run and the key stay usable.
    assert!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Pending))
            .unwrap()
            .is_empty()
    );
    let commit = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&event.id, "BTC"), "commit")
        .unwrap();
    let claim = &commit.claims[0];
    let confirmed = fixture.store.confirm_claim(&claim.id, "confirm").unwrap();
    assert_eq!(confirmed.status, ClaimStatus::Active);
}

#[test]
fn rescoping_a_disputed_claim_cannot_launder_it_into_active_state() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("thread", ScopeKind::Thread, Some("user"));
    let remember = |fixture: &mut Fixture, value: &str, key: &str| {
        fixture
            .store
            .remember_claim(
                "thread",
                ClaimKind::Decision,
                "launch",
                "asset",
                ClaimCardinality::Single,
                json!(value),
                &[],
                key,
            )
            .unwrap()
            .data
    };
    remember(&mut fixture, "ETH", "remember-x");
    let disputed = remember(&mut fixture, "BTC", "remember-y");
    assert_eq!(disputed.status, ClaimStatus::Disputed);

    let rescoped = fixture
        .store
        .rescope_claim(&disputed.id, "user", "rescope")
        .unwrap();
    assert_eq!(rescoped.status, ClaimStatus::Disputed);
    fixture.store.reconcile("user", "reconcile").unwrap();
    assert!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Active))
            .unwrap()
            .is_empty()
    );

    // Ended claims cannot be rescoped back to life.
    fixture.store.reject_claim(&rescoped.id, "reject").unwrap();
    let error = fixture
        .store
        .rescope_claim(&rescoped.id, "user", "rescope-rejected")
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().map(KernelError::kind),
        Some(KernelErrorKind::InvalidInput)
    );
    assert_eq!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Pending))
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn only_live_claims_can_be_corrected() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let original = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "remember",
        )
        .unwrap();
    let corrected = fixture
        .store
        .correct_claim(&original.id, json!("BTC"), &[], "correct")
        .unwrap();
    let rejected = fixture
        .store
        .propose_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "chain",
            ClaimCardinality::Single,
            json!("L1"),
            &[],
            "propose",
        )
        .unwrap();
    fixture.store.reject_claim(&rejected.id, "reject").unwrap();
    let expired = fixture
        .store
        .propose_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "venue",
            ClaimCardinality::Single,
            json!("DEX"),
            &[],
            "propose-2",
        )
        .unwrap();
    fixture.store.forget_claim(&expired.id, "forget").unwrap();
    for (id, key) in [
        (&original.id, "stale-superseded"),
        (&rejected.id, "stale-rejected"),
        (&expired.id, "stale-expired"),
    ] {
        let error = fixture
            .store
            .correct_claim(id, json!("late"), &[], key)
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<KernelError>().map(KernelError::kind),
            Some(KernelErrorKind::InvalidInput)
        );
    }
    // Rejected corrections leave the current state alone and their keys reusable.
    let active = fixture
        .store
        .list_claims("user", false, Some(ClaimStatus::Active))
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, corrected.id);
    let pending = fixture
        .store
        .propose_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "region",
            ClaimCardinality::Single,
            json!("EU"),
            &[],
            "propose-3",
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .correct_claim(&pending.id, json!("US"), &[], "stale-superseded")
            .unwrap()
            .status,
        ClaimStatus::Active
    );
}

#[test]
fn claim_kind_is_a_label_not_part_of_the_slot() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let fact = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Fact,
            "alice",
            "employer",
            ClaimCardinality::Single,
            json!("Acme"),
            &[],
            "remember-fact",
        )
        .unwrap();
    let decision = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "alice",
            "employer",
            ClaimCardinality::Single,
            json!("Globex"),
            &[],
            "remember-decision",
        )
        .unwrap();
    assert_eq!(fact.status, ClaimStatus::Active);
    // The same slot under another kind conflicts instead of coexisting.
    assert_eq!(decision.status, ClaimStatus::Disputed);
    assert_eq!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Active))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn reconcile_absorbs_observer_duplicates_without_activating() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "Use ETH", Sensitivity::Normal, "event");
    let active = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "remember",
        )
        .unwrap();
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let commit = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&event.id, "ETH"), "commit")
        .unwrap();

    let summary = fixture.store.reconcile("user", "reconcile").unwrap();
    assert_eq!(
        summary.duplicates_rejected,
        vec![commit.claims[0].id.clone()]
    );
    let explanation = fixture
        .store
        .explain_claim(&ReadAccess::agent("user"), &active.id)
        .unwrap();
    assert!(
        explanation
            .source_events
            .iter()
            .any(|source| source.id == event.id)
    );
}
