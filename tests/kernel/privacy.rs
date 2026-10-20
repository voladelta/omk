use super::*;

#[test]
fn compact_context_keeps_claim_origin_and_recall_ids_without_secret_content() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let source = fixture.event(
        "user",
        "stream",
        "Launch asset is ETH",
        Sensitivity::Normal,
        "source",
    );
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let commit = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&source.id, "ETH"), "commit")
        .unwrap();
    let secret = fixture.event(
        "user",
        "stream",
        "private credential",
        Sensitivity::Secret,
        "secret",
    );

    let full = fixture
        .store
        .compose_context("user", "stream", 10_000, 10_000, None)
        .unwrap();
    let compact = fixture
        .store
        .compose_compact_context("user", "stream", 10_000, 10_000, None)
        .unwrap();
    let payload = compact.compact_model_payload();

    assert!(payload.to_string().len() < full.model_payload().to_string().len());
    assert_eq!(full.diagnostics.unresolved_claims, 1);
    assert_eq!(
        full.diagnostics.oldest_unresolved_claim_at.as_deref(),
        Some(commit.claims[0].created_at.as_str())
    );
    assert_eq!(payload["pendingClaims"][0]["id"], commit.claims[0].id);
    assert_eq!(payload["pendingClaims"][0]["status"], "pending");
    assert_eq!(
        payload["pendingClaims"][0]["modality"],
        "explicit-assertion"
    );
    assert_eq!(payload["pendingClaims"][0]["originRunId"], plan.run_id);
    assert_eq!(payload["pendingClaims"][0]["value"], "ETH");
    assert_eq!(payload["recentEvents"][0]["id"], source.id);
    assert_eq!(payload["recentEvents"][1]["id"], secret.id);
    assert_eq!(payload["recentEvents"][1]["content"]["redacted"], true);
    assert!(!payload.to_string().contains("private credential"));
    assert!(payload["recentEvents"][0].get("contentHash").is_none());
    assert!(payload["recentEvents"][0].get("recordedAt").is_none());

    let claim_sources = fixture
        .store
        .explain_claim(&access("user"), &commit.claims[0].id)
        .unwrap();
    assert_eq!(claim_sources.source_events[0].id, source.id);
    assert_eq!(
        fixture
            .store
            .get_event(&access("user"), &secret.id)
            .unwrap()
            .content,
        json!({"redacted": true, "reason": "secret"})
    );

    let without_raw = fixture
        .store
        .compose_compact_context("user", "stream", 10_000, 0, None)
        .unwrap()
        .compact_model_payload();
    assert_eq!(
        without_raw["observations"][0]["id"],
        commit.observations[0].id
    );
    assert_eq!(
        without_raw["observations"][0]["content"],
        "Launch asset is ETH"
    );
    let observation_sources = fixture
        .store
        .recall_by_observation(&access("user"), &commit.observations[0].id)
        .unwrap();
    assert_eq!(observation_sources[0].id, source.id);
}

#[test]
fn append_is_idempotent_and_privacy_boundaries_are_safe() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);

    let first = fixture.event("user", "thread", "hello", Sensitivity::Normal, "event-1");
    let duplicate = fixture.event("user", "thread", "hello", Sensitivity::Normal, "event-1");
    assert_eq!(first.id, duplicate.id);
    assert_eq!(first.sequence, 1);
    assert_eq!(duplicate.content, json!("hello"));
    let conflict = fixture.store.append_event(NewEvent {
        scope_id: "user".to_owned(),
        stream_id: "thread".to_owned(),
        kind: EventKind::UserMessage,
        actor_id: Some("user".to_owned()),
        occurred_at: None,
        content: json!("different"),
        token_count: Some(10),
        sensitivity: Sensitivity::Normal,
        metadata: json!({}),
        idempotency_key: "event-1".to_owned(),
    });
    assert!(
        conflict
            .unwrap_err()
            .to_string()
            .contains("idempotency conflict")
    );

    let omitted = fixture.event(
        "user",
        "thread",
        "must never persist",
        Sensitivity::DoNotStore,
        "event-2",
    );
    assert_eq!(omitted.sequence, 2);
    assert_eq!(
        omitted.content,
        json!({"omitted": true, "reason": "do-not-store"})
    );

    let secret = fixture.event(
        "user",
        "thread",
        "secret-value",
        Sensitivity::Secret,
        "event-3",
    );
    let plan = fixture
        .store
        .plan_observation("user", "thread", 10_000, "fake", "v1", Some("plan-1"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let planned_secret = plan
        .events
        .iter()
        .find(|event| event.id == secret.id)
        .unwrap();
    assert_eq!(
        planned_secret.content,
        json!({"redacted": true, "reason": "secret"})
    );
    assert_eq!(
        fixture
            .store
            .get_event(&reveal("user"), &secret.id)
            .unwrap()
            .content,
        json!("secret-value")
    );
    assert_eq!(
        fixture
            .store
            .get_event(&access("user"), &secret.id)
            .unwrap()
            .content,
        json!({"redacted": true, "reason": "secret"})
    );
}

#[test]
fn do_not_store_retries_ignore_payload_fingerprints() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let first = fixture
        .store
        .append_event(NewEvent {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: EventKind::ToolResult,
            actor_id: Some("tool".to_owned()),
            occurred_at: None,
            content: json!("first private payload"),
            token_count: Some(100),
            sensitivity: Sensitivity::DoNotStore,
            metadata: json!({"private": "first"}),
            idempotency_key: "dns-key".to_owned(),
        })
        .unwrap();
    let replay = fixture
        .store
        .append_event(NewEvent {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: EventKind::ToolResult,
            actor_id: Some("tool".to_owned()),
            occurred_at: None,
            content: json!("different private payload"),
            token_count: Some(999),
            sensitivity: Sensitivity::DoNotStore,
            metadata: json!({"private": "different"}),
            idempotency_key: "dns-key".to_owned(),
        })
        .unwrap();

    assert!(replay.operation.replayed);
    assert_eq!(replay.id, first.id);
    assert_eq!(
        replay.content,
        json!({"omitted": true, "reason": "do-not-store"})
    );
}

#[test]
fn redacted_secret_evidence_cannot_activate_a_model_claim() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event(
        "user",
        "stream",
        "api-key-value",
        Sensitivity::Secret,
        "event-1",
    );
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-1"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let error = fixture
        .store
        .commit_observation(
            &plan.run_id,
            observer_result(&event.id, "invented"),
            "commit-1",
        )
        .unwrap_err();
    assert!(error.to_string().contains("cannot source derived memory"));
    assert!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Active))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn privacy_covers_metadata_and_observer_envelopes_are_strict() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let secret = fixture
        .store
        .append_event(NewEvent {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: EventKind::ToolResult,
            actor_id: None,
            occurred_at: None,
            content: json!("secret body"),
            token_count: Some(4),
            sensitivity: Sensitivity::Secret,
            metadata: json!({"credential": "secret metadata"}),
            idempotency_key: "secret".to_owned(),
        })
        .unwrap()
        .data;
    assert_eq!(
        secret.content,
        json!({"redacted": true, "reason": "secret"})
    );
    assert_eq!(secret.metadata, json!({}));
    let omitted = fixture
        .store
        .append_event(NewEvent {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: EventKind::ToolResult,
            actor_id: None,
            occurred_at: None,
            content: json!("never store"),
            token_count: Some(4),
            sensitivity: Sensitivity::DoNotStore,
            metadata: json!({"credential": "never store metadata"}),
            idempotency_key: "dns".to_owned(),
        })
        .unwrap()
        .data;
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan"))
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert!(plan.events.iter().all(|event| event.metadata == json!({})));
    assert_eq!(
        fixture
            .store
            .get_event(&reveal("user"), &secret.id)
            .unwrap()
            .metadata["credential"],
        "secret metadata"
    );
    assert_eq!(
        fixture
            .store
            .get_event(&access("user"), &omitted.id)
            .unwrap()
            .metadata,
        json!({})
    );
    let context = fixture
        .store
        .compose_context("user", "stream", 100, 100, None)
        .unwrap();
    assert!(
        context
            .recent_events
            .iter()
            .all(|event| event.metadata == json!({}))
    );

    assert!(serde_json::from_str::<ObserverResult>("{}").is_err());
    assert!(serde_json::from_str::<ObserverResult>(
        r#"{"observations":[],"claims":[],"continuation":{"currentTask":null,"completed":[],"blockers":[],"nextActions":[],"unresolvedQuestions":[]},"ambiguities":[],"emptyReason":"nothing durable"}"#
    )
    .is_ok());
}

#[test]
fn saved_append_results_never_hold_secret_content_or_metadata() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let appended = fixture
        .store
        .append_event(NewEvent {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: EventKind::ToolResult,
            actor_id: None,
            occurred_at: None,
            content: json!("SECRET-CONTENT-7"),
            token_count: None,
            sensitivity: Sensitivity::Secret,
            metadata: json!({"token": "SECRET-METADATA-9"}),
            idempotency_key: "secret".to_owned(),
        })
        .unwrap();
    let saved: String = Connection::open(fixture._directory.path().join("memory.db"))
        .unwrap()
        .query_row(
            "SELECT result_json FROM memory_operation_results WHERE idempotency_key='secret'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!saved.contains("SECRET-CONTENT-7"));
    assert!(!saved.contains("SECRET-METADATA-9"));
    assert!(saved.contains(&appended.data.id));
    assert_eq!(
        fixture
            .store
            .get_event(&reveal("user"), &appended.data.id)
            .unwrap()
            .content,
        json!("SECRET-CONTENT-7")
    );
}
