use super::*;

#[test]
fn scope_inheritance_does_not_leak_between_projects() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("project-a", ScopeKind::Project, Some("user"));
    fixture.scope("project-b", ScopeKind::Project, Some("user"));
    fixture.event("project-a", "stream-a", "A", Sensitivity::Normal, "event-a");
    fixture.event("project-b", "stream-b", "B", Sensitivity::Normal, "event-b");
    let project_claim = fixture
        .store
        .remember_claim(
            "project-a",
            ClaimKind::Constraint,
            "settlement",
            "asset",
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "claim-a",
        )
        .unwrap();
    fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Preference,
            "user",
            "plan-style",
            ClaimCardinality::Single,
            json!("complete"),
            &[],
            "claim-user",
        )
        .unwrap();

    let context = fixture
        .store
        .compose_context("project-b", "stream-b", 1_000, 100, None)
        .unwrap();
    assert_eq!(context.claims.len(), 1);
    assert_eq!(context.claims[0].scope_id, "user");
    assert!(
        fixture
            .store
            .rescope_claim(&project_claim.id, "project-b", "unsafe-rescope")
            .unwrap_err()
            .to_string()
            .contains("current scope or one of its ancestors")
    );
}

#[test]
fn project_context_includes_observations_from_its_selected_descendant_stream() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("project", ScopeKind::Project, Some("user"));
    fixture.scope("thread", ScopeKind::Thread, Some("project"));
    let event = fixture.event(
        "thread",
        "thread-stream",
        "descendant evidence",
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

    let context = fixture
        .store
        .compose_context("project", "thread-stream", 1_000, 0, None)
        .unwrap();
    assert_eq!(context.observations.len(), 1);
    assert_eq!(context.observations[0].id, commit.observations[0].id);
}

#[test]
fn full_text_search_does_not_index_views() {
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
    fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Reflection,
            content: "view-only-search-marker".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![commit.observations[0].id.clone()],
            expected_previous_view_id: None,
            model: Some("fake".to_owned()),
            prompt_version: Some("reflector.v1".to_owned()),
            token_count: None,
            idempotency_key: "view".to_owned(),
        })
        .unwrap();

    assert!(
        fixture
            .store
            .search_full_text("user", "view-only-search-marker", 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn full_text_search_is_scope_aware() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("a", ScopeKind::Project, Some("user"));
    fixture.scope("b", ScopeKind::Project, Some("user"));
    fixture.event(
        "a",
        "a-stream",
        "quartz launch",
        Sensitivity::Normal,
        "a-event",
    );
    fixture.event(
        "b",
        "b-stream",
        "ordinary work",
        Sensitivity::Normal,
        "b-event",
    );
    assert_eq!(
        fixture
            .store
            .search_full_text("a", "quartz", 10)
            .unwrap()
            .len(),
        1
    );
    assert!(
        fixture
            .store
            .search_full_text("b", "quartz", 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn budgets_are_hard_and_literal_search_includes_descendants() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("project", ScopeKind::Project, Some("user"));
    fixture.scope("thread", ScopeKind::Thread, Some("project"));
    fixture
        .store
        .append_event(NewEvent {
            scope_id: "thread".to_owned(),
            stream_id: "stream".to_owned(),
            kind: EventKind::UserMessage,
            actor_id: None,
            occurred_at: None,
            content: json!("purge-derived marker"),
            token_count: Some(100),
            sensitivity: Sensitivity::Normal,
            metadata: json!({}),
            idempotency_key: "event".to_owned(),
        })
        .unwrap();
    let plan_error = fixture
        .store
        .plan_observation("thread", "stream", 1, "fake", "v1", Some("small-plan"))
        .unwrap_err();
    assert!(plan_error.to_string().contains("minimumRequiredTokens="));
    assert!(
        plan_error
            .to_string()
            .contains("required state and first event")
    );
    assert_eq!(
        fixture
            .store
            .search_full_text("project", "purge-derived marker", 10)
            .unwrap()
            .len(),
        1
    );
    let recalled = fixture
        .store
        .compose_context("project", "stream", 1_000, 0, Some("purge-derived marker"))
        .unwrap();
    assert_eq!(recalled.recalled_evidence.len(), 1);

    fixture
        .store
        .append_event(NewEvent {
            scope_id: "thread".to_owned(),
            stream_id: "understated".to_owned(),
            kind: EventKind::UserMessage,
            actor_id: None,
            occurred_at: None,
            content: json!("x".repeat(400)),
            token_count: Some(1),
            sensitivity: Sensitivity::Normal,
            metadata: json!({}),
            idempotency_key: "understated-event".to_owned(),
        })
        .unwrap();
    let understated_error = fixture
        .store
        .plan_observation(
            "thread",
            "understated",
            10,
            "fake",
            "v1",
            Some("understated-plan"),
        )
        .unwrap_err();
    assert!(
        understated_error
            .to_string()
            .contains("minimumRequiredTokens")
    );

    let view = fixture
        .store
        .create_view(CreateView {
            scope_id: "thread".to_owned(),
            stream_id: "understated".to_owned(),
            kind: ViewKind::Reflection,
            content: "v".repeat(400),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![],
            expected_previous_view_id: None,
            model: None,
            prompt_version: None,
            token_count: Some(1),
            idempotency_key: "understated-view".to_owned(),
        })
        .unwrap();
    assert!(view.token_count >= 100);

    fixture
        .store
        .remember_claim(
            "project",
            ClaimKind::Constraint,
            "release",
            "channel",
            ClaimCardinality::Single,
            json!("stable"),
            &[],
            "claim",
        )
        .unwrap();
    let context_error = fixture
        .store
        .compose_context("project", "stream", 1, 0, None)
        .unwrap_err();
    assert!(context_error.to_string().contains("minimumRequiredTokens"));
}

#[test]
fn deeper_scope_single_claims_shadow_ancestors_in_context_and_plans() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("thread", ScopeKind::Thread, Some("user"));
    let mut remember = |scope: &str, predicate: &str, cardinality, value: &str, key: &str| {
        fixture
            .store
            .remember_claim(
                scope,
                ClaimKind::Decision,
                "launch",
                predicate,
                cardinality,
                json!(value),
                &[],
                key,
            )
            .unwrap()
            .data
    };
    let shadowed = remember(
        "user",
        "asset",
        ClaimCardinality::Single,
        "ETH",
        "user-asset",
    );
    let winner = remember(
        "thread",
        "asset",
        ClaimCardinality::Single,
        "BTC",
        "thread-asset",
    );
    let user_tag = remember("user", "tag", ClaimCardinality::Set, "a", "user-tag");
    let thread_tag = remember("thread", "tag", ClaimCardinality::Set, "b", "thread-tag");
    let user_only = remember(
        "user",
        "owner",
        ClaimCardinality::Single,
        "me",
        "user-owner",
    );
    fixture.event("thread", "stream", "work", Sensitivity::Normal, "event");

    let context = fixture
        .store
        .compose_context("thread", "stream", 100_000, 0, None)
        .unwrap();
    let mut ids: Vec<&str> = context
        .claims
        .iter()
        .map(|claim| claim.id.as_str())
        .collect();
    ids.sort_unstable();
    let mut expected = vec![
        winner.id.as_str(),
        user_tag.id.as_str(),
        thread_tag.id.as_str(),
        user_only.id.as_str(),
    ];
    expected.sort_unstable();
    assert_eq!(ids, expected);
    let omitted: Vec<_> = context
        .diagnostics
        .omitted_items
        .iter()
        .filter(|item| item.reason == "shadowed by descendant scope claim")
        .map(|item| item.id.as_str())
        .collect();
    assert_eq!(omitted, vec![shadowed.id.as_str()]);
    let compact = fixture
        .store
        .compose_compact_context("thread", "stream", 100_000, 0, None)
        .unwrap();
    assert_eq!(compact.claims.len(), 4);

    // Shadowed claims do not count toward the claim budget: twice the tokens
    // the four in-force claims need gives them their whole half-share.
    let required = context.diagnostics.estimated_tokens;
    let tight = fixture
        .store
        .compose_context("thread", "stream", required * 2, 0, None)
        .unwrap();
    assert_eq!(tight.claims.len(), 4);
    assert!(
        !tight
            .diagnostics
            .omitted_items
            .iter()
            .any(|item| item.reason == "active claim budget")
    );

    let plan = plan_run(&mut fixture, "thread", "stream", "plan");
    assert!(
        plan.active_claims
            .iter()
            .all(|claim| claim.id != shadowed.id)
    );
    assert_eq!(plan.active_claims.len(), 4);

    let listed = fixture
        .store
        .list_claims("thread", true, Some(ClaimStatus::Active))
        .unwrap();
    assert!(listed.iter().any(|claim| claim.id == shadowed.id));
}
