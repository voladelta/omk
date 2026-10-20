use super::*;

#[test]
fn exact_reads_require_a_visible_scope_even_for_known_ids() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("project-a", ScopeKind::Project, Some("user"));
    fixture.scope("project-b", ScopeKind::Project, Some("user"));
    let event = fixture.event(
        "project-a",
        "stream-a",
        "private to A",
        Sensitivity::Normal,
        "event-a",
    );

    let error = fixture
        .store
        .get_event(&access("project-b"), &event.id)
        .unwrap_err();
    assert!(error.to_string().contains("not visible"));
    let range_error = fixture
        .store
        .recall_event_range(&access("project-b"), "stream-a", 1, 1)
        .unwrap_err();
    assert!(range_error.to_string().contains("not visible"));
}

#[test]
fn generated_command_events_reject_cross_scope_stream_collisions() {
    let mut fixture = Fixture::new();
    fixture.scope("project-a", ScopeKind::Project, None);
    fixture.scope("project-b", ScopeKind::Project, None);
    fixture.event(
        "project-a",
        "memory-commands:project-b",
        "occupied",
        Sensitivity::Normal,
        "occupy-command-stream",
    );

    let error = fixture
        .store
        .remember_claim(
            "project-b",
            ClaimKind::Fact,
            "private",
            "note",
            ClaimCardinality::Single,
            json!("sibling-data"),
            &[],
            "remember-project-b",
        )
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().map(KernelError::kind),
        Some(KernelErrorKind::ScopeViolation)
    );
    assert!(
        fixture
            .store
            .list_claims("project-b", false, None)
            .unwrap()
            .is_empty()
    );

    let plan = fixture
        .store
        .plan_observation(
            "project-a",
            "memory-commands:project-b",
            10_000,
            "fake",
            "v1",
            Some("plan-project-a"),
        )
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(plan.events.len(), 1);
    assert_eq!(plan.events[0].scope_id, "project-a");
    assert_eq!(plan.events[0].content, json!("occupied"));
}

#[test]
fn observation_commit_is_atomic_idempotent_and_source_backed() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event(
        "user",
        "thread",
        "Use ETH as the launch asset",
        Sensitivity::Normal,
        "event-1",
    );
    let plan = fixture
        .store
        .plan_observation("user", "thread", 10_000, "fake", "v1", Some("plan-1"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let commit = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&event.id, "ETH"), "commit-1")
        .unwrap();
    let retry = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&event.id, "ETH"), "commit-1")
        .unwrap();
    assert_eq!(commit.observations[0].id, retry.observations[0].id);
    assert_eq!(commit.claims[0].status, ClaimStatus::Pending);
    assert!(
        commit
            .next_required_action
            .as_deref()
            .is_some_and(
                |action| action.contains("claim confirm") && action.contains("claim reject")
            )
    );
    assert!(matches!(
        fixture
            .store
            .plan_observation("user", "thread", 10_000, "fake", "v1", Some("plan-2"))
            .unwrap()
            .data,
        ObservationPlanOutcome::CaughtUp { .. }
    ));

    let summary = fixture.store.reconcile("user", "reconcile-1").unwrap();
    assert_eq!(summary.left_pending, vec![commit.claims[0].id.clone()]);
    let explanation = fixture
        .store
        .explain_claim(&access("user"), &commit.claims[0].id)
        .unwrap();
    assert_eq!(explanation.source_events[0].id, event.id);
    assert!(
        serde_json::to_value(&explanation)
            .unwrap()
            .get("sourceObservations")
            .is_none()
    );
    assert_eq!(
        fixture
            .store
            .recall_by_observation(&access("user"), &commit.observations[0].id)
            .unwrap()[0]
            .id,
        event.id
    );
}

#[test]
fn empty_observer_acknowledgements_preserve_existing_continuation() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let first = fixture.event(
        "user",
        "stream",
        "Prepare the release",
        Sensitivity::Normal,
        "event-1",
    );
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
            observer_result(&first.id, "ETH"),
            "commit-1",
        )
        .unwrap();
    assert_eq!(
        first_commit.continuation_action,
        ContinuationAction::Created
    );

    fixture.event(
        "user",
        "stream",
        "Acknowledged.",
        Sensitivity::Normal,
        "event-2",
    );
    let second_plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-2"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let empty = ObserverResult {
        observations: vec![],
        claims: vec![],
        continuation: ContinuationDraft::default(),
        ambiguities: vec![],
        empty_reason: Some("Acknowledgement contains no durable memory".to_owned()),
    };
    let second_commit = fixture
        .store
        .commit_observation(&second_plan.run_id, empty, "commit-2")
        .unwrap();
    assert_eq!(
        second_commit.continuation_action,
        ContinuationAction::Preserved
    );
    assert_eq!(
        second_commit.continuation_view.id,
        first_commit.continuation_view.id
    );
    assert_eq!(second_commit.continuation_view.generation, 1);
    assert!(
        second_commit
            .continuation_view
            .content
            .contains("Prepare launch")
    );

    let caught_up = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-3"))
        .unwrap();
    assert!(matches!(
        &caught_up.data,
        ObservationPlanOutcome::CaughtUp { .. }
    ));
    let encoded = serde_json::to_value(caught_up).unwrap();
    assert_eq!(encoded["data"]["status"], "caught-up");
    assert_eq!(encoded["data"]["observedThroughSequence"], 2);
    assert!(encoded["data"]["nextAction"].is_string());
}

#[test]
fn invalid_and_stale_observation_commits_never_advance_twice() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event(
        "user",
        "stream",
        "remember me",
        Sensitivity::Normal,
        "event-1",
    );
    let first = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-1"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let stale = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-2"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();

    let invalid = observer_result("not-in-run", "ETH");
    assert!(
        fixture
            .store
            .commit_observation(&first.run_id, invalid, "invalid-commit")
            .is_err()
    );
    fixture
        .store
        .commit_observation(&first.run_id, observer_result(&event.id, "ETH"), "commit-1")
        .unwrap();
    assert!(
        fixture
            .store
            .commit_observation(&stale.run_id, observer_result(&event.id, "ETH"), "commit-2")
            .is_err()
    );
    assert!(matches!(
        fixture
            .store
            .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-3"))
            .unwrap()
            .data,
        ObservationPlanOutcome::CaughtUp { .. }
    ));
}

#[test]
fn observer_failure_is_recorded_without_advancing_the_cursor() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "retry me", Sensitivity::Normal, "event-1");
    let failed = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-1"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    fixture
        .store
        .fail_observation(&failed.run_id, "model-timeout", "fail-1")
        .unwrap();
    let retry = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-2"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(retry.from_sequence, failed.from_sequence);
    fixture
        .store
        .commit_observation(&retry.run_id, observer_result(&event.id, "ETH"), "commit-1")
        .unwrap();
}

#[test]
fn idempotency_is_request_bound_and_reports_replays() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let request = NewEvent {
        scope_id: "user".to_owned(),
        stream_id: "stream".to_owned(),
        kind: EventKind::UserMessage,
        actor_id: None,
        occurred_at: None,
        content: json!("original"),
        token_count: Some(3),
        sensitivity: Sensitivity::Normal,
        metadata: json!({}),
        idempotency_key: "event-key".to_owned(),
    };
    let created = fixture.store.append_event(request.clone()).unwrap();
    let replay = fixture.store.append_event(request).unwrap();
    assert!(!created.operation.replayed);
    assert!(replay.operation.replayed);
    assert_eq!(created.id, replay.id);

    let conflict = fixture.store.append_event(NewEvent {
        content: json!("changed"),
        scope_id: "user".to_owned(),
        stream_id: "stream".to_owned(),
        kind: EventKind::UserMessage,
        actor_id: None,
        occurred_at: None,
        token_count: Some(3),
        sensitivity: Sensitivity::Normal,
        metadata: json!({}),
        idempotency_key: "event-key".to_owned(),
    });
    assert!(
        conflict
            .unwrap_err()
            .to_string()
            .contains("idempotency conflict")
    );
}

#[test]
fn observation_and_stream_inspection_are_complete() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "inspect", Sensitivity::Normal, "event");
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
    let explanation = fixture
        .store
        .explain_observation(&access("user"), &commit.observations[0].id)
        .unwrap();
    assert_eq!(explanation.observation.id, commit.observations[0].id);
    assert_eq!(explanation.source_events[0].id, event.id);
    let status = fixture
        .store
        .stream_status(&access("user"), "stream")
        .unwrap();
    assert_eq!(status.observed_through_sequence, 1);
    assert_eq!(status.next_sequence, 2);
    assert_eq!(status.runs[0].status, RunStatus::Committed);
}

#[test]
fn reflection_view_commits_compare_and_swap_per_stream() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "source", Sensitivity::Normal, "event");
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
    let first = fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Reflection,
            content: "generation one".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![commit.observations[0].id.clone()],
            expected_previous_view_id: None,
            model: None,
            prompt_version: None,
            token_count: None,
            idempotency_key: "view-1".to_owned(),
        })
        .unwrap();
    let stale = fixture.store.create_view(CreateView {
        scope_id: "user".to_owned(),
        stream_id: "stream".to_owned(),
        kind: ViewKind::Reflection,
        content: "stale generation two".to_owned(),
        source_from_sequence: 1,
        source_through_sequence: 1,
        source_observation_ids: vec![],
        expected_previous_view_id: None,
        model: None,
        prompt_version: None,
        token_count: None,
        idempotency_key: "view-stale".to_owned(),
    });
    assert!(stale.unwrap_err().to_string().contains("view is stale"));
    assert_eq!(
        fixture
            .store
            .list_views("user")
            .unwrap()
            .iter()
            .filter(|view| { view.kind == ViewKind::Reflection })
            .count(),
        1
    );

    let second = fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Reflection,
            content: "generation two".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![],
            expected_previous_view_id: Some(first.id.clone()),
            model: None,
            prompt_version: None,
            token_count: None,
            idempotency_key: "view-2".to_owned(),
        })
        .unwrap();
    assert_eq!(second.generation, 2);

    let forbidden = fixture.store.create_view(CreateView {
        scope_id: "user".to_owned(),
        stream_id: "stream".to_owned(),
        kind: ViewKind::Continuation,
        content: "not observer-owned".to_owned(),
        source_from_sequence: 1,
        source_through_sequence: 1,
        source_observation_ids: vec![],
        expected_previous_view_id: None,
        model: None,
        prompt_version: None,
        token_count: None,
        idempotency_key: "forbidden-continuation".to_owned(),
    });
    assert!(
        forbidden
            .unwrap_err()
            .to_string()
            .contains("observation commit")
    );
}

#[test]
fn oversized_first_event_becomes_an_uncitable_stub_so_the_cursor_can_advance() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let big = fixture.event(
        "user",
        "stream",
        &"x".repeat(4_000),
        Sensitivity::Normal,
        "big",
    );
    let small = fixture.event("user", "stream", "small", Sensitivity::Normal, "small");
    let plan = fixture
        .store
        .plan_observation("user", "stream", 400, "fake", "v1", Some("plan"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let stub = &plan.events[0];
    assert_eq!(stub.id, big.id);
    assert_eq!(stub.content["truncated"], json!(true));
    assert_eq!(stub.content["reason"], json!("exceeds observation budget"));
    // No cut text: the observer sees none of the event, only what it costs.
    assert!(stub.content.get("preview").is_none());
    assert!(!stub.content.to_string().contains("xxxx"));
    assert!(stub.content["eventTokens"].as_i64().unwrap() > 1_000);
    assert_eq!(stub.metadata, json!({}));
    let replayed = fixture
        .store
        .plan_observation("user", "stream", 400, "fake", "v1", Some("plan"))
        .unwrap();
    assert!(replayed.operation.replayed);
    assert_eq!(
        replayed.data.into_plan().unwrap().events[0].content,
        stub.content
    );
    // The stub is small, so the next event fits the same plan whole.
    assert_eq!(plan.to_sequence, 2);
    assert_eq!(plan.events[1].id, small.id);
    assert_eq!(plan.events[1].content, json!("small"));

    // The stored event is intact, but the observer cannot cite the stub.
    let error = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&big.id, "ETH"), "cite-stub")
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().map(KernelError::kind),
        Some(KernelErrorKind::InvalidInput)
    );
    assert!(error.to_string().contains("truncated event"));
    fixture
        .store
        .commit_observation(
            &plan.run_id,
            observer_result(&small.id, "ETH"),
            "cite-small",
        )
        .unwrap();
    let status = fixture
        .store
        .stream_status(&access("user"), "stream")
        .unwrap();
    assert_eq!(status.observed_through_sequence, 2);
    assert!(
        fixture
            .store
            .plan_observation("user", "stream", 400, "fake", "v1", Some("next-plan"))
            .unwrap()
            .data
            .into_plan()
            .is_none()
    );
    assert_eq!(
        fixture
            .store
            .get_event(&reveal("user"), &big.id)
            .unwrap()
            .content,
        json!("x".repeat(4_000))
    );
}

#[test]
fn duplicate_scopes_and_finished_runs_fail_with_typed_errors() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let duplicate = fixture
        .store
        .create_scope("user", ScopeKind::User, None, None, "scope-again")
        .unwrap_err();
    assert_eq!(
        duplicate
            .downcast_ref::<KernelError>()
            .map(KernelError::kind),
        Some(KernelErrorKind::InvalidInput)
    );
    assert!(duplicate.to_string().contains("scope user already exists"));
    // The rejected key was not recorded, so it can create a different scope.
    fixture
        .store
        .create_scope("project", ScopeKind::Project, None, None, "scope-again")
        .unwrap();

    let event = fixture.event("user", "stream", "work", Sensitivity::Normal, "event");
    let plan = plan_run(&mut fixture, "user", "stream", "plan");
    fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&event.id, "ETH"), "commit")
        .unwrap();
    let committed = fixture
        .store
        .fail_observation(&plan.run_id, "late failure", "fail")
        .unwrap_err();
    assert_eq!(
        committed
            .downcast_ref::<KernelError>()
            .map(KernelError::kind),
        Some(KernelErrorKind::InvalidInput)
    );
    assert!(committed.to_string().contains("is committed, not pending"));
    let again = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&event.id, "BTC"), "commit-2")
        .unwrap_err();
    assert_eq!(
        again.downcast_ref::<KernelError>().map(KernelError::kind),
        Some(KernelErrorKind::InvalidInput)
    );
}

#[test]
fn saved_plans_hold_only_their_run_and_replay_from_current_state() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let before = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Preference,
            "editor",
            "value",
            ClaimCardinality::Single,
            json!("vim"),
            &[],
            "remember-before",
        )
        .unwrap()
        .data;
    let first = fixture.event("user", "stream", "first", Sensitivity::Normal, "e1");
    let second = fixture.event("user", "stream", "second", Sensitivity::Normal, "e2");
    let plan = plan_run(&mut fixture, "user", "stream", "plan");
    assert_eq!(plan.active_claims.len(), 1);

    let conn = Connection::open(fixture._directory.path().join("memory.db")).unwrap();
    let saved: String = conn
        .query_row(
            "SELECT result_json FROM memory_operation_results WHERE idempotency_key='plan'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&saved).unwrap(),
        json!({"plannedRunId": plan.run_id})
    );

    // Replay keeps the run and its exact events but shows current claims.
    let added = fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Preference,
            "shell",
            "value",
            ClaimCardinality::Single,
            json!("zsh"),
            &[],
            "remember-after",
        )
        .unwrap()
        .data;
    let replay = fixture
        .store
        .plan_observation("user", "stream", 100_000, "fake", "v1", Some("plan"))
        .unwrap();
    assert!(replay.operation.replayed);
    let replayed = replay.data.into_plan().unwrap();
    assert_eq!(replayed.run_id, plan.run_id);
    assert_eq!(
        replayed.events.iter().map(|e| &e.id).collect::<Vec<_>>(),
        [&first.id, &second.id]
    );
    assert_eq!(
        serde_json::to_value(&replayed.events).unwrap(),
        serde_json::to_value(&plan.events).unwrap()
    );
    let mut claim_ids: Vec<&str> = replayed
        .active_claims
        .iter()
        .map(|c| c.id.as_str())
        .collect();
    claim_ids.sort_unstable();
    let mut expected = vec![before.id.as_str(), added.id.as_str()];
    expected.sort_unstable();
    assert_eq!(claim_ids, expected);

    // Purging a claim the plan showed leaves the plan replayable.
    fixture
        .store
        .purge_claim(&before.id, "purge-claim")
        .unwrap();
    let after_purge = fixture
        .store
        .plan_observation("user", "stream", 100_000, "fake", "v1", Some("plan"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(after_purge.active_claims.len(), 1);

    // Purging one of the run's events tombstones the saved plan.
    fixture.store.purge_event(&first.id, "purge-event").unwrap();
    let error = fixture
        .store
        .plan_observation("user", "stream", 100_000, "fake", "v1", Some("plan"))
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().unwrap().kind(),
        KernelErrorKind::PrivacyPurged
    );
}

#[test]
fn old_results_compact_to_keys_that_still_block_duplicates() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let old = fixture.event("user", "stream", "old", Sensitivity::Normal, "old");
    let conn = Connection::open(fixture._directory.path().join("memory.db")).unwrap();
    let expired = (chrono::Utc::now() - chrono::Duration::days(31)).to_rfc3339();
    conn.execute(
        "UPDATE memory_operation_results SET created_at=?1 WHERE idempotency_key='old'",
        [&expired],
    )
    .unwrap();

    // Any later write compacts the expired result but keeps key and hash.
    fixture.event("user", "stream", "new", Sensitivity::Normal, "new");
    let (hash, result): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT request_hash,(SELECT result_json FROM memory_operation_results WHERE idempotency_key='old') FROM memory_operations WHERE idempotency_key='old'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(hash.is_some());
    assert!(result.is_none());
    let fresh: Option<String> = conn
        .query_row(
            "SELECT result_json FROM memory_operation_results WHERE idempotency_key='new'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(fresh.is_some());

    let replay = |content: &str| NewEvent {
        scope_id: "user".to_owned(),
        stream_id: "stream".to_owned(),
        kind: EventKind::UserMessage,
        actor_id: Some("user".to_owned()),
        occurred_at: None,
        content: json!(content),
        token_count: Some(10),
        sensitivity: Sensitivity::Normal,
        metadata: json!({}),
        idempotency_key: "old".to_owned(),
    };
    let error = fixture.store.append_event(replay("old")).unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().unwrap().kind(),
        KernelErrorKind::OperationExpired
    );
    let error = fixture.store.append_event(replay("changed")).unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().unwrap().kind(),
        KernelErrorKind::IdempotencyConflict
    );
    let events: i64 = conn
        .query_row("SELECT COUNT(*) FROM memory_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events, 2);

    // Refs survive compaction, so a purge still clears the request hash.
    fixture.store.purge_event(&old.id, "purge").unwrap();
    let hash: Option<String> = conn
        .query_row(
            "SELECT request_hash FROM memory_operations WHERE idempotency_key='old'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(hash.is_none());
    let error = fixture.store.append_event(replay("old")).unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().unwrap().kind(),
        KernelErrorKind::PrivacyPurged
    );
}

#[test]
fn keyless_plans_reuse_the_open_run_at_the_cursor() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let long = "Use ETH for settlement. ".repeat(20);
    fixture.event("user", "stream", &long, Sensitivity::Normal, "event-1");
    fixture.event("user", "stream", &long, Sensitivity::Normal, "event-2");
    let plan = |fixture: &mut Fixture| {
        fixture
            .store
            .plan_observation("user", "stream", 10_000, "fake", "v1", None)
            .unwrap()
    };
    let first = plan(&mut fixture);
    let again = plan(&mut fixture);
    assert!(!first.operation.replayed);
    assert!(again.operation.replayed);
    let first_id = first.data.clone().into_plan().unwrap().run_id;
    assert_eq!(again.data.clone().into_plan().unwrap().run_id, first_id);

    // A failed run is closed, so the next keyless plan retries the range.
    fixture
        .store
        .fail_observation(&first_id, "timeout", "fail")
        .unwrap();
    let retry = plan(&mut fixture).data.into_plan().unwrap();
    assert_ne!(retry.run_id, first_id);
    assert_eq!(retry.from_sequence, 1);

    // A budget too small for the open run plans afresh instead of overrunning.
    let operations_before: i64 = Connection::open(fixture._directory.path().join("memory.db"))
        .unwrap()
        .query_row("SELECT count(*) FROM memory_operations", [], |row| {
            row.get(0)
        })
        .unwrap();
    let small = fixture
        .store
        .plan_observation("user", "stream", 300, "fake", "v1", None)
        .unwrap();
    assert!(!small.operation.replayed);
    assert_ne!(small.data.into_plan().unwrap().run_id, retry.run_id);
    let operations_after: i64 = Connection::open(fixture._directory.path().join("memory.db"))
        .unwrap()
        .query_row("SELECT count(*) FROM memory_operations", [], |row| {
            row.get(0)
        })
        .unwrap();
    // Keyless plans save no replay record: nothing could ever replay it.
    assert_eq!(operations_after, operations_before);

    // A different observer does not take over another observer's open run.
    let other = fixture
        .store
        .plan_observation("user", "stream", 10_000, "other", "v1", None)
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_ne!(other.run_id, retry.run_id);
}
