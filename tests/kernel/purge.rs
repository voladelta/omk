use super::*;

#[test]
fn privacy_purge_removes_dependents_and_prevents_idempotent_replay() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event(
        "user",
        "stream",
        "erase this",
        Sensitivity::Normal,
        "event-1",
    );
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-1"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let commit = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&event.id, "ETH"), "commit-1")
        .unwrap();
    fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Reflection,
            content: "Derived from erase this".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![],
            expected_previous_view_id: None,
            model: Some("fake".to_owned()),
            prompt_version: Some("reflector.v1".to_owned()),
            token_count: None,
            idempotency_key: "view-1".to_owned(),
        })
        .unwrap();
    let purge = fixture.store.purge_event(&event.id, "purge-1").unwrap();

    assert!(fixture.store.get_event(&access("user"), &event.id).is_err());
    assert_eq!(purge.data["dependentViews"], 2);
    assert_eq!(purge.data["dependentViewIds"].as_array().unwrap().len(), 2);
    assert_eq!(purge.data["affectedRunIds"][0], plan.run_id);
    let invalidated_run = fixture
        .store
        .get_observation_run(&access("user"), &plan.run_id)
        .unwrap();
    assert_eq!(invalidated_run.status, RunStatus::Committed);
    assert_eq!(
        invalidated_run.source_integrity,
        SourceIntegrity::PrivacyPurged
    );
    assert!(invalidated_run.error.is_none());
    assert!(invalidated_run.next_action.is_some());
    assert!(
        fixture
            .store
            .recall_by_observation(&access("user"), &commit.observations[0].id)
            .is_err()
    );
    assert!(
        fixture
            .store
            .explain_claim(&access("user"), &commit.claims[0].id)
            .is_err()
    );
    assert!(fixture.store.list_views("user").unwrap().is_empty());
    assert!(
        fixture
            .store
            .append_event(NewEvent {
                scope_id: "user".to_owned(),
                stream_id: "stream".to_owned(),
                kind: EventKind::UserMessage,
                actor_id: None,
                occurred_at: None,
                content: json!("erase this"),
                token_count: Some(2),
                sensitivity: Sensitivity::Normal,
                metadata: json!({}),
                idempotency_key: "event-1".to_owned(),
            })
            .unwrap_err()
            .to_string()
            .contains("privacy-purged")
    );

    let replacement = fixture.event(
        "user",
        "stream",
        "new evidence",
        Sensitivity::Normal,
        "event-2",
    );
    assert_eq!(replacement.sequence, 2);
    let retry_plan = fixture
        .store
        .plan_observation(
            "user",
            "stream",
            10_000,
            "fake",
            "v1",
            Some("plan-after-purge"),
        )
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(retry_plan.from_sequence, 2);
}

#[test]
fn observation_recovery_commits_across_purged_sequence_gaps() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let removed = fixture.event(
        "user",
        "stream",
        "remove before observing",
        Sensitivity::Normal,
        "event-1",
    );
    let stale_plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-1"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    fixture.store.purge_event(&removed.id, "purge-1").unwrap();
    assert_eq!(
        fixture
            .store
            .get_observation_run(&access("user"), &stale_plan.run_id)
            .unwrap()
            .status,
        RunStatus::Stale
    );

    let replacement = fixture.event(
        "user",
        "stream",
        "safe replacement",
        Sensitivity::Normal,
        "event-2",
    );
    assert_eq!(replacement.sequence, 2);
    let recovery_plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-2"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(recovery_plan.from_sequence, 2);
    assert_eq!(
        fixture
            .store
            .get_observation_run(&access("user"), &recovery_plan.run_id)
            .unwrap()
            .cursor_at_plan,
        0
    );
    fixture
        .store
        .commit_observation(
            &recovery_plan.run_id,
            observer_result(&replacement.id, "ETH"),
            "commit-2",
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .stream_status(&access("user"), "stream")
            .unwrap()
            .observed_through_sequence,
        2
    );
}

#[test]
fn purging_a_direct_claim_removes_orphaned_command_evidence() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let claim = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Preference,
            "user",
            "editor",
            ClaimCardinality::Single,
            json!("purge-only-value"),
            &[],
            "remember",
        )
        .unwrap();
    let command_id = fixture
        .store
        .explain_claim(&access("user"), &claim.id)
        .unwrap()
        .source_events[0]
        .id
        .clone();

    let purge = fixture.store.purge_claim(&claim.id, "purge").unwrap();
    assert_eq!(purge.data["purgedCommandEvents"], 1);
    assert!(
        fixture
            .store
            .get_event(&access("user"), &command_id)
            .is_err()
    );
    assert!(
        fixture
            .store
            .search_full_text("user", "purge-only-value", 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .store
            .remember_claim(
                "user",
                ClaimKind::Preference,
                "user",
                "editor",
                ClaimCardinality::Single,
                json!("purge-only-value"),
                &[],
                "remember",
            )
            .unwrap_err()
            .to_string()
            .contains("privacy-purged")
    );
}

#[test]
fn claim_purge_removes_empty_slot_and_allows_new_cardinality() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let claim = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Preference,
            "private subject",
            "private predicate",
            ClaimCardinality::Single,
            json!("one"),
            &[],
            "remember",
        )
        .unwrap();
    fixture.store.purge_claim(&claim.id, "purge").unwrap();

    let replacement = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Preference,
            "private subject",
            "private predicate",
            ClaimCardinality::Set,
            json!("many"),
            &[],
            "replace-as-set",
        )
        .unwrap();
    assert_eq!(replacement.cardinality, ClaimCardinality::Set);
    assert_eq!(replacement.status, ClaimStatus::Active);
}

#[test]
fn claim_purge_keeps_a_shared_command_owned_by_another_claim() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let owner = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Fact,
            "owner",
            "value",
            ClaimCardinality::Single,
            json!("A"),
            &[],
            "owner",
        )
        .unwrap();
    let owner_command = fixture
        .store
        .explain_claim(&reveal("user"), &owner.id)
        .unwrap()
        .source_events
        .into_iter()
        .find(|event| event.kind == EventKind::MemoryCommand)
        .unwrap();
    let borrower = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Fact,
            "borrower",
            "value",
            ClaimCardinality::Single,
            json!("B"),
            std::slice::from_ref(&owner_command.id),
            "borrower",
        )
        .unwrap();

    let purge = fixture
        .store
        .purge_claim(&borrower.id, "purge-borrower")
        .unwrap();
    assert_eq!(purge.data["purgedCommandEvents"], 1);
    assert_eq!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Active))
            .unwrap()[0]
            .id,
        owner.id
    );
    assert_eq!(
        fixture
            .store
            .get_event(&access("user"), &owner_command.id)
            .unwrap()
            .id,
        owner_command.id
    );
}

#[test]
fn event_purge_recurses_through_observed_generated_commands() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let source = fixture.event(
        "user",
        "stream",
        "transitive-purge-canary",
        Sensitivity::Normal,
        "source",
    );
    let claim = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Fact,
            "canary",
            "value",
            ClaimCardinality::Single,
            json!("transitive-purge-canary"),
            std::slice::from_ref(&source.id),
            "remember",
        )
        .unwrap();
    let command_event = fixture
        .store
        .explain_claim(&reveal("user"), &claim.id)
        .unwrap()
        .source_events
        .into_iter()
        .find(|event| event.kind == EventKind::MemoryCommand)
        .unwrap();
    let plan = fixture
        .store
        .plan_observation(
            "user",
            "memory-commands:user",
            1_000,
            "fake",
            "v1",
            Some("plan-command"),
        )
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let observer = ObserverResult {
        observations: vec![ObservationDraft {
            kind: ObservationKind::Event,
            content: "Observed transitive-purge-canary".to_owned(),
            importance: 1.0,
            confidence: 1.0,
            source_event_ids: vec![command_event.id.clone()],
            event_time_from: None,
            event_time_to: None,
        }],
        claims: vec![],
        continuation: ContinuationDraft::default(),
        ambiguities: vec![],
        empty_reason: None,
    };
    fixture
        .store
        .commit_observation(&plan.run_id, observer, "commit-command")
        .unwrap();
    assert!(
        fixture
            .store
            .search_full_text("user", "transitive-purge-canary", 20)
            .unwrap()
            .len()
            >= 3
    );

    let purge = fixture
        .store
        .purge_event(&source.id, "purge-source")
        .unwrap();
    assert_eq!(purge.data["purgedCommandEvents"], 1);
    assert!(
        fixture
            .store
            .search_full_text("user", "transitive-purge-canary", 20)
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .store
            .get_event(&access("user"), &command_event.id)
            .is_err()
    );
    assert!(fixture.store.list_views("user").unwrap().is_empty());
}

#[test]
fn purging_a_view_source_removes_all_successor_generations() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let first_event = fixture.event("user", "stream", "first", Sensitivity::Normal, "event-1");
    let first_plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-1"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let first_commit = fixture
        .store
        .commit_observation(
            &first_plan.run_id,
            observer_result(&first_event.id, "ETH"),
            "commit-1",
        )
        .unwrap();
    let first_view = fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Reflection,
            content: "first generation".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![first_commit.observations[0].id.clone()],
            expected_previous_view_id: None,
            model: None,
            prompt_version: None,
            token_count: None,
            idempotency_key: "view-1".to_owned(),
        })
        .unwrap();

    let second_event = fixture.event("user", "stream", "second", Sensitivity::Normal, "event-2");
    let second_plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-2"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let second_commit = fixture
        .store
        .commit_observation(
            &second_plan.run_id,
            observer_result(&second_event.id, "USDG"),
            "commit-2",
        )
        .unwrap();
    fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Reflection,
            content: "second generation".to_owned(),
            source_from_sequence: 2,
            source_through_sequence: 2,
            source_observation_ids: vec![second_commit.observations[0].id.clone()],
            expected_previous_view_id: Some(first_view.id.clone()),
            model: None,
            prompt_version: None,
            token_count: None,
            idempotency_key: "view-2".to_owned(),
        })
        .unwrap();

    let purge = fixture
        .store
        .purge_event(&first_event.id, "purge-first")
        .unwrap();
    assert!(purge.data["dependentViews"].as_u64().unwrap() >= 3);
    assert!(fixture.store.list_views("user").unwrap().is_empty());
}

#[test]
fn purges_find_operations_and_search_rows_through_side_tables() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let doomed = fixture.event(
        "user",
        "stream",
        "doomed marker",
        Sensitivity::Normal,
        "doomed-event",
    );
    let kept = fixture.event(
        "user",
        "stream",
        "kept marker",
        Sensitivity::Normal,
        "kept-event",
    );
    let claim = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "launch",
            "asset",
            ClaimCardinality::Single,
            json!("ETH"),
            std::slice::from_ref(&kept.id),
            "claim",
        )
        .unwrap();
    let refs = "SELECT COUNT(*) FROM memory_operation_refs WHERE record_id=?1";
    let fts = "SELECT COUNT(*) FROM memory_fts_refs WHERE record_id=?1";
    let rows = "SELECT COUNT(*) FROM memory_fts WHERE record_id=?1";
    assert_eq!(raw_count(&fixture, refs, &doomed.id), 1);
    assert_eq!(raw_count(&fixture, fts, &doomed.id), 1);
    assert_eq!(raw_count(&fixture, rows, &doomed.id), 1);

    fixture
        .store
        .purge_event(&doomed.id, "purge-doomed")
        .unwrap();
    assert_eq!(raw_count(&fixture, fts, &doomed.id), 0);
    assert_eq!(raw_count(&fixture, rows, &doomed.id), 0);
    // The tombstoned append no longer keeps its record IDs; the purge result does.
    assert_eq!(raw_count(&fixture, refs, &doomed.id), 1);
    assert_eq!(
        raw_count(
            &fixture,
            "SELECT COUNT(*) FROM memory_operation_refs WHERE idempotency_key=?1",
            "doomed-event"
        ),
        0
    );
    assert!(matches!(
        fixture
            .store
            .append_event(NewEvent {
                scope_id: "user".to_owned(),
                stream_id: "stream".to_owned(),
                kind: EventKind::UserMessage,
                actor_id: Some("user".to_owned()),
                occurred_at: None,
                content: Value::String("doomed marker".to_owned()),
                token_count: Some(10),
                sensitivity: Sensitivity::Normal,
                metadata: json!({}),
                idempotency_key: "doomed-event".to_owned(),
            })
            .unwrap_err()
            .downcast_ref::<KernelError>()
            .map(KernelError::kind),
        Some(KernelErrorKind::PrivacyPurged)
    ));
    // Unrelated operations and search rows survive.
    assert_eq!(raw_count(&fixture, rows, &kept.id), 1);
    assert_eq!(
        fixture
            .store
            .search_full_text("user", "kept marker", 10)
            .unwrap()
            .len(),
        1
    );
    assert!(
        fixture
            .store
            .remember_claim(
                "user",
                ClaimKind::Decision,
                "launch",
                "asset",
                ClaimCardinality::Single,
                json!("ETH"),
                std::slice::from_ref(&kept.id),
                "claim",
            )
            .unwrap()
            .operation
            .replayed
    );

    fixture.store.purge_claim(&claim.id, "purge-claim").unwrap();
    assert_eq!(raw_count(&fixture, fts, &claim.id), 0);
    assert_eq!(raw_count(&fixture, rows, &claim.id), 0);
    assert_eq!(
        raw_count(
            &fixture,
            "SELECT COUNT(*) FROM memory_operation_refs WHERE idempotency_key=?1",
            "claim"
        ),
        0
    );
}
