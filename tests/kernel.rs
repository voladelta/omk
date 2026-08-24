use omk::store::CreateView;
use omk::*;
use rusqlite::Connection;
use serde_json::{Value, json};
use tempfile::TempDir;

fn table_columns(connection: &Connection, table: &str) -> Vec<(String, i64, i64)> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(1)?, row.get(3)?, row.get(5)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

struct Fixture {
    _directory: TempDir,
    store: MemoryStore,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = MemoryStore::open(directory.path().join("memory.db")).unwrap();
        Self {
            _directory: directory,
            store,
        }
    }

    fn scope(&mut self, id: &str, kind: ScopeKind, parent: Option<&str>) {
        self.store
            .create_scope(id, kind, parent, None, &format!("scope-{id}"))
            .unwrap();
    }

    fn event(
        &mut self,
        scope: &str,
        stream: &str,
        content: &str,
        sensitivity: Sensitivity,
        key: &str,
    ) -> MemoryEvent {
        self.store
            .append_event(NewEvent {
                scope_id: scope.to_owned(),
                stream_id: stream.to_owned(),
                kind: EventKind::UserMessage,
                actor_id: Some("user".to_owned()),
                occurred_at: None,
                content: Value::String(content.to_owned()),
                token_count: Some(10),
                sensitivity,
                metadata: json!({}),
                idempotency_key: key.to_owned(),
            })
            .unwrap()
            .data
    }
}

fn observer_result(event_id: &str, value: &str) -> ObserverResult {
    ObserverResult {
        observations: vec![ObservationDraft {
            kind: ObservationKind::Decision,
            content: format!("Launch asset is {value}"),
            importance: 0.9,
            confidence: 1.0,
            source_event_ids: vec![event_id.to_owned()],
            event_time_from: None,
            event_time_to: None,
        }],
        claims: vec![ClaimDraft {
            kind: ClaimKind::Decision,
            subject: "launch".to_owned(),
            predicate: "asset".to_owned(),
            value: Value::String(value.to_owned()),
            modality: ClaimModality::ExplicitAssertion,
            confidence: 1.0,
            source_event_ids: vec![event_id.to_owned()],
        }],
        continuation: ContinuationDraft {
            current_task: Some("Prepare launch".to_owned()),
            next_actions: vec!["Implement settlement".to_owned()],
            ..ContinuationDraft::default()
        },
        ambiguities: vec![],
        empty_reason: None,
    }
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
        .plan_observation("user", "thread", 100, "fake", "v1", "plan-1")
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
        fixture.store.get_event(&secret.id).unwrap().content,
        json!("secret-value")
    );
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
        .plan_observation("user", "thread", 100, "fake", "v1", "plan-1")
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
    assert!(matches!(
        fixture
            .store
            .plan_observation("user", "thread", 100, "fake", "v1", "plan-2")
            .unwrap()
            .data,
        ObservationPlanOutcome::CaughtUp { .. }
    ));

    let summary = fixture.store.reconcile("user", "reconcile-1").unwrap();
    assert_eq!(summary.activated, vec![commit.claims[0].id.clone()]);
    let explanation = fixture.store.explain_claim(&commit.claims[0].id).unwrap();
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
            .recall_by_observation(&commit.observations[0].id)
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-1")
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-2")
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-3")
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-1")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let stale = fixture
        .store
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-2")
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
            .plan_observation("user", "stream", 100, "fake", "v1", "plan-3")
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-1")
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-2")
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
            json!(["ETH", "USDG"]),
            &[],
            "proposal-1",
        )
        .unwrap();
    let summary = fixture.store.reconcile("user", "reconcile-1").unwrap();
    assert_eq!(summary.left_pending, vec![proposal.id.clone()]);
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
            .contains("not visible")
    );
}

#[test]
fn context_deduplicates_observations_and_views_never_destroy_raw_evidence() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "Use ETH", Sensitivity::Normal, "event-1");
    let plan = fixture
        .store
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-1")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let commit = fixture
        .store
        .commit_observation(&plan.run_id, observer_result(&event.id, "ETH"), "commit-1")
        .unwrap();
    let context = fixture
        .store
        .compose_context("user", "stream", 1_000, 100, None)
        .unwrap();
    assert!(context.observations.is_empty());
    assert_eq!(context.recent_events[0].id, event.id);

    let first = fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            kind: ViewKind::Continuity,
            content: "ETH is the launch asset".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![commit.observations[0].id.clone()],
            model: Some("fake".to_owned()),
            prompt_version: Some("reflector.v1".to_owned()),
            token_count: None,
            idempotency_key: "view-1".to_owned(),
        })
        .unwrap();
    let second = fixture
        .store
        .create_view(CreateView {
            idempotency_key: "view-2".to_owned(),
            content: "Launch remains ETH-only".to_owned(),
            ..CreateView {
                scope_id: "user".to_owned(),
                kind: ViewKind::Continuity,
                content: String::new(),
                source_from_sequence: 1,
                source_through_sequence: 1,
                source_observation_ids: vec![commit.observations[0].id.clone()],
                model: Some("fake".to_owned()),
                prompt_version: Some("reflector.v1".to_owned()),
                token_count: None,
                idempotency_key: String::new(),
            }
        })
        .unwrap();
    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert_eq!(second.previous_view_id.as_deref(), Some(first.id.as_str()));
    let third = fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            kind: ViewKind::Continuity,
            content: "No new reflected observations".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![],
            model: Some("fake".to_owned()),
            prompt_version: Some("reflector.v1".to_owned()),
            token_count: None,
            idempotency_key: "view-3".to_owned(),
        })
        .unwrap();
    assert_eq!(third.previous_view_id.as_deref(), Some(second.id.as_str()));
    assert!(
        fixture
            .store
            .compose_context("user", "stream", 1_000, 0, None)
            .unwrap()
            .observations
            .is_empty()
    );
    assert_eq!(
        fixture.store.recall_event_range("stream", 1, 1).unwrap()[0].id,
        event.id
    );
}

#[test]
fn context_deduplicates_only_exact_source_events() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.event(
        "user",
        "stream-a",
        "Raw event from stream A",
        Sensitivity::Normal,
        "event-a",
    );
    let event_b = fixture.event(
        "user",
        "stream-b",
        "Decision from stream B",
        Sensitivity::Normal,
        "event-b",
    );
    let plan = fixture
        .store
        .plan_observation("user", "stream-b", 100, "fake", "v1", "plan-b")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let result = ObserverResult {
        observations: vec![ObservationDraft {
            kind: ObservationKind::Decision,
            content: "Remember stream B".to_owned(),
            importance: 1.0,
            confidence: 1.0,
            source_event_ids: vec![event_b.id],
            event_time_from: None,
            event_time_to: None,
        }],
        claims: vec![],
        continuation: ContinuationDraft::default(),
        ambiguities: vec![],
        empty_reason: None,
    };
    let commit = fixture
        .store
        .commit_observation(&plan.run_id, result, "commit-b")
        .unwrap();

    let context = fixture
        .store
        .compose_context("user", "stream-a", 1_000, 100, None)
        .unwrap();
    assert_eq!(context.observations[0].id, commit.observations[0].id);
}

#[test]
fn context_prioritizes_continuation_over_pending_claims() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.event(
        "user",
        "stream",
        "Continue the release",
        Sensitivity::Normal,
        "event",
    );
    let plan = fixture
        .store
        .plan_observation("user", "stream", 100, "fake", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let result = ObserverResult {
        observations: vec![],
        claims: vec![],
        continuation: ContinuationDraft {
            current_task: Some("Ship the release".to_owned()),
            next_actions: vec!["Run final checks".to_owned()],
            ..ContinuationDraft::default()
        },
        ambiguities: vec![],
        empty_reason: None,
    };
    let commit = fixture
        .store
        .commit_observation(&plan.run_id, result, "commit")
        .unwrap();
    fixture
        .store
        .propose_claim(
            "user",
            ClaimKind::Decision,
            "p",
            "q",
            json!("v"),
            &[],
            "proposal",
        )
        .unwrap();

    let context = fixture
        .store
        .compose_context(
            "user",
            "stream",
            commit.continuation_view.token_count,
            0,
            None,
        )
        .unwrap();
    assert!(context.pending_claims.is_empty());
    assert_eq!(context.continuity_views.len(), 1);
    assert_eq!(context.continuity_views[0].kind, ViewKind::Continuation);
}

#[test]
fn full_text_search_does_not_index_views() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "source", Sensitivity::Normal, "event");
    let plan = fixture
        .store
        .plan_observation("user", "stream", 100, "fake", "v1", "plan")
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
            kind: ViewKind::Continuity,
            content: "view-only-search-marker".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![commit.observations[0].id.clone()],
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-1")
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-1")
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
            kind: ViewKind::Continuity,
            content: "Derived from erase this".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![],
            model: Some("fake".to_owned()),
            prompt_version: Some("reflector.v1".to_owned()),
            token_count: None,
            idempotency_key: "view-1".to_owned(),
        })
        .unwrap();
    let purge = fixture.store.purge_event(&event.id, "purge-1").unwrap();

    assert!(fixture.store.get_event(&event.id).is_err());
    assert_eq!(purge.data["dependentViews"], 2);
    assert_eq!(purge.data["dependentViewIds"].as_array().unwrap().len(), 2);
    assert_eq!(purge.data["affectedRunIds"][0], plan.run_id);
    let invalidated_run = fixture.store.get_observation_run(&plan.run_id).unwrap();
    assert_eq!(invalidated_run.status, "committed");
    assert_eq!(
        invalidated_run.source_integrity,
        SourceIntegrity::PrivacyPurged
    );
    assert!(invalidated_run.error.is_none());
    assert!(invalidated_run.next_action.is_some());
    assert!(
        fixture
            .store
            .recall_by_observation(&commit.observations[0].id)
            .is_err()
    );
    assert!(fixture.store.explain_claim(&commit.claims[0].id).is_err());
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-after-purge")
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-1")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    fixture.store.purge_event(&removed.id, "purge-1").unwrap();
    assert_eq!(
        fixture
            .store
            .get_observation_run(&stale_plan.run_id)
            .unwrap()
            .status,
        "stale"
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan-2")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(recovery_plan.from_sequence, 2);
    assert_eq!(
        fixture
            .store
            .get_observation_run(&recovery_plan.run_id)
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
            .stream_status("stream")
            .unwrap()
            .observed_through_sequence,
        2
    );
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
        .plan_observation("user", "stream", 100, "fake", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert!(plan.events.iter().all(|event| event.metadata == json!({})));
    assert_eq!(
        fixture.store.get_event(&secret.id).unwrap().metadata["credential"],
        "secret metadata"
    );
    assert_eq!(
        fixture.store.get_event(&omitted.id).unwrap().metadata,
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
            json!("USDG"),
            &[],
            "proposal",
        )
        .unwrap();
    let explanation = fixture.store.explain_claim(&active.id).unwrap();
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
            json!("purge-only-value"),
            &[],
            "remember",
        )
        .unwrap();
    let command_id = fixture
        .store
        .explain_claim(&claim.id)
        .unwrap()
        .source_events[0]
        .id
        .clone();

    let purge = fixture.store.purge_claim(&claim.id, "purge").unwrap();
    assert_eq!(purge.data["purgedCommandEvents"], 1);
    assert!(fixture.store.get_event(&command_id).is_err());
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
        .plan_observation("thread", "stream", 1, "fake", "v1", "small-plan")
        .unwrap_err();
    assert!(plan_error.to_string().contains("minimumRequiredTokens=100"));
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
            "understated-plan",
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
            kind: ViewKind::Continuity,
            content: "v".repeat(400),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![],
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
fn observation_and_stream_inspection_are_complete() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "inspect", Sensitivity::Normal, "event");
    let plan = fixture
        .store
        .plan_observation("user", "stream", 100, "fake", "v1", "plan")
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
        .explain_observation(&commit.observations[0].id)
        .unwrap();
    assert_eq!(explanation.observation.id, commit.observations[0].id);
    assert_eq!(explanation.source_events[0].id, event.id);
    let status = fixture.store.stream_status("stream").unwrap();
    assert_eq!(status.observed_through_sequence, 1);
    assert_eq!(status.next_sequence, 2);
    assert_eq!(status.runs[0].status, "committed");
    assert!(
        fixture
            .store
            .list_observation_runs(None, None, Some("running"))
            .is_err()
    );
}

#[test]
fn current_schema_reopens_without_rewriting_data() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("memory.db");
    let mut store = MemoryStore::open(&path).unwrap();
    store
        .create_scope("user", ScopeKind::User, None, None, "scope")
        .unwrap();
    let event = store
        .append_event(NewEvent {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: EventKind::UserMessage,
            actor_id: None,
            occurred_at: None,
            content: json!("survives reopen"),
            token_count: None,
            sensitivity: Sensitivity::Normal,
            metadata: json!({}),
            idempotency_key: "event".to_owned(),
        })
        .unwrap();
    drop(store);

    let store = MemoryStore::open(&path).unwrap();
    assert_eq!(
        store.get_event(&event.data.id).unwrap().content,
        event.data.content
    );

    let connection = Connection::open(path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, SCHEMA_VERSION);

    let event_columns = table_columns(&connection, "memory_events");
    assert!(
        !event_columns
            .iter()
            .any(|(name, _, _)| name == "idempotency_key")
    );
    let claim_columns = table_columns(&connection, "claims");
    for removed in ["valid_from", "valid_to", "expires_at"] {
        assert!(!claim_columns.iter().any(|(name, _, _)| name == removed));
    }
    let claim_source_columns = table_columns(&connection, "claim_sources");
    assert_eq!(
        claim_source_columns
            .iter()
            .map(|(name, _, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["claim_id", "event_id"]
    );
    assert!(
        claim_source_columns
            .iter()
            .all(|(_, not_null, primary_key)| *not_null == 1 && *primary_key > 0)
    );
    let active_claim_index: bool = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master
                WHERE type='index' AND name='one_active_claim_per_logical_key'
            )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(active_claim_index);
    let operation_columns = table_columns(&connection, "memory_operations");
    assert!(
        !operation_columns
            .iter()
            .any(|(name, _, _)| name == "purged")
    );
    assert!(
        !operation_columns
            .iter()
            .any(|(name, _, _)| name == "created_at")
    );
    assert_eq!(
        operation_columns
            .iter()
            .find(|(name, _, _)| name == "result_json")
            .map(|(_, not_null, _)| *not_null),
        Some(0)
    );
}

#[test]
fn incompatible_database_versions_are_rejected_without_schema_writes() {
    let directory = tempfile::tempdir().unwrap();
    for version in [1_i64, 2, 3, 4, 99] {
        let path = directory.path().join(format!("schema-{version}.db"));
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "user_version", version)
            .unwrap();
        drop(connection);

        let error = match MemoryStore::open(&path) {
            Ok(_) => panic!("schema version {version} should be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains(&format!(
            "is incompatible with OMK schema version {SCHEMA_VERSION}"
        )));
        let connection = Connection::open(path).unwrap();
        let table_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='memory_scopes'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_count, 0);
    }
}

#[test]
fn unversioned_nonempty_databases_are_not_adopted() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unversioned.db");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute("CREATE TABLE unrelated(value TEXT)", [])
        .unwrap();
    drop(connection);

    let error = match MemoryStore::open(&path) {
        Ok(_) => panic!("unversioned nonempty database should be rejected"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("unversioned database is not empty")
    );

    let connection = Connection::open(path).unwrap();
    let omk_table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='memory_scopes'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(omk_table_count, 0);
}
