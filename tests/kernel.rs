use omk::store::CreateView;
use omk::*;
use rusqlite::Connection;
use serde_json::{Value, json};
use tempfile::TempDir;

fn access(scope: &str) -> ReadAccess {
    ReadAccess::agent(scope)
}

fn reveal(scope: &str) -> ReadAccess {
    ReadAccess {
        anchor_scope_id: scope.to_owned(),
        reveal_secrets: true,
    }
}

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
            cardinality: ClaimCardinality::Single,
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
fn compact_context_keeps_claim_authority_and_recall_ids_without_secret_content() {
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan")
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
    assert_eq!(payload["pendingClaims"][0]["id"], commit.claims[0].id);
    assert_eq!(payload["pendingClaims"][0]["status"], "pending");
    assert_eq!(
        payload["pendingClaims"][0]["modality"],
        "explicit-assertion"
    );
    assert_eq!(payload["pendingClaims"][0]["authority"], "model-inference");
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
fn compact_context_budget_prices_compact_records_and_reports_claims_over_budget() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.event(
        "user",
        "stream",
        "A decision with useful context",
        Sensitivity::Normal,
        "source",
    );

    let compact_fits_before_full = (50..350).any(|budget| {
        let Ok(full) = fixture
            .store
            .compose_context("user", "stream", budget, 300, None)
        else {
            return false;
        };
        let Ok(compact) = fixture
            .store
            .compose_compact_context("user", "stream", budget, 300, None)
        else {
            return false;
        };

        compact.recent_events.len() == 1
            && full.recent_events.is_empty()
            && compact
                .compact_model_payload()
                .to_string()
                .chars()
                .count()
                .div_ceil(4) as i64
                <= compact.diagnostics.estimated_tokens
            && compact.diagnostics.estimated_tokens <= budget
    });
    assert!(compact_fits_before_full);

    fixture
        .store
        .remember_claim(
            "user",
            ClaimKind::Decision,
            "release",
            "asset",
            json!("ETH"),
            &[],
            "remember",
        )
        .unwrap();
    let empty_payload = json!({
        "claims": [],
        "pendingClaims": [],
        "continuation": null,
        "continuityViews": [],
        "observations": [],
        "recentEvents": [],
        "recalledEvidence": [],
    });
    let overhead = empty_payload.to_string().chars().count().div_ceil(4) as i64;
    let squeezed = fixture
        .store
        .compose_compact_context("user", "stream", overhead + 1, 0, None)
        .unwrap();
    assert!(squeezed.claims.is_empty());
    assert_eq!(squeezed.diagnostics.omitted_items.len(), 1);
    assert_eq!(
        squeezed.diagnostics.omitted_items[0].reason,
        "active claim budget"
    );
    let error = fixture
        .store
        .compose_compact_context("user", "stream", overhead - 1, 0, None)
        .unwrap_err();
    assert!(error.to_string().contains("minimumRequiredTokens"));
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
        .plan_observation("user", "thread", 10_000, "fake", "v1", "plan-1")
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
            "plan-project-a",
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
fn set_claims_keep_distinct_members_and_lock_slot_cardinality() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let first = fixture
        .store
        .remember_claim_with_cardinality(
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
        .remember_claim_with_cardinality(
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
        .remember_claim_with_cardinality(
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
        json!("one value"),
        &[],
        "single-mismatch",
    );
    assert!(mismatch.unwrap_err().to_string().contains("cardinality"));
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
        .plan_observation("user", "thread", 10_000, "fake", "v1", "plan-1")
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
            .plan_observation("user", "thread", 10_000, "fake", "v1", "plan-2")
            .unwrap()
            .data,
        ObservationPlanOutcome::CaughtUp { .. }
    ));

    let summary = fixture.store.reconcile("user", "reconcile-1").unwrap();
    assert!(summary.activated.is_empty());
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-1")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-2")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-3")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-1")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let stale = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-2")
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
            .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-3")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-1")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-2")
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
            json!("USDG"),
            &[],
            "propose",
        )
        .unwrap();

    let confirmed = fixture.store.confirm_claim(&pending.id, "confirm").unwrap();
    assert_eq!(confirmed.id, pending.id);
    assert_eq!(confirmed.status, ClaimStatus::Active);
    assert_eq!(confirmed.modality, ClaimModality::AcceptedDecision);
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
        .plan_observation("thread", "thread-stream", 10_000, "fake", "v1", "plan")
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
fn rescoping_an_observer_claim_does_not_grant_activation_authority() {
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
        .plan_observation("thread", "thread-stream", 10_000, "fake", "v1", "plan")
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
    assert!(reconciliation.activated.is_empty());
    assert_eq!(reconciliation.left_pending, vec![promoted.id.clone()]);
}

#[test]
fn context_deduplicates_observations_and_views_never_destroy_raw_evidence() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "Use ETH", Sensitivity::Normal, "event-1");
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-1")
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
        .compose_context("user", "stream", 1_000, 200, None)
        .unwrap();
    assert!(context.observations.is_empty());
    assert_eq!(context.recent_events[0].id, event.id);

    let first = fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Continuity,
            content: "ETH is the launch asset".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![commit.observations[0].id.clone()],
            expected_previous_view_id: None,
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
                stream_id: "stream".to_owned(),
                kind: ViewKind::Continuity,
                content: String::new(),
                source_from_sequence: 1,
                source_through_sequence: 1,
                source_observation_ids: vec![commit.observations[0].id.clone()],
                expected_previous_view_id: Some(first.id.clone()),
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
            stream_id: "stream".to_owned(),
            kind: ViewKind::Continuity,
            content: "No new reflected observations".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![],
            expected_previous_view_id: Some(second.id.clone()),
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
    // A view rejected by the budget must not suppress its inherited observations.
    fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Continuity,
            content: "large continuity text ".repeat(1000),
            source_from_sequence: 1,
            source_through_sequence: 1,
            source_observation_ids: vec![],
            expected_previous_view_id: Some(third.id.clone()),
            model: None,
            prompt_version: None,
            token_count: None,
            idempotency_key: "view-4".to_owned(),
        })
        .unwrap();
    for compact in [false, true] {
        let context = if compact {
            fixture
                .store
                .compose_compact_context("user", "stream", 1000, 0, None)
        } else {
            fixture
                .store
                .compose_context("user", "stream", 1000, 0, None)
        }
        .unwrap();
        assert!(context.continuity_views.is_empty());
        assert_eq!(context.observations[0].id, commit.observations[0].id);
    }
    assert_eq!(
        fixture
            .store
            .recall_event_range(&access("user"), "stream", 1, 1)
            .unwrap()[0]
            .id,
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
        .plan_observation("user", "stream-b", 10_000, "fake", "v1", "plan-b")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan")
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
        .compose_context("user", "stream", 100, 0, None)
        .unwrap();
    assert!(context.pending_claims.is_empty());
    assert!(context.continuation.is_some());
    assert_eq!(
        serde_json::to_value(&context.continuation).unwrap(),
        serde_json::from_str::<Value>(&commit.continuation_view.content).unwrap()
    );
    assert!(context.continuity_views.is_empty());
}

#[test]
fn full_text_search_does_not_index_views() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "source", Sensitivity::Normal, "event");
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan")
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
            kind: ViewKind::Continuity,
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-1")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-1")
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
            kind: ViewKind::Continuity,
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-after-purge")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-1")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-2")
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan")
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
            json!("one"),
            &[],
            "remember",
        )
        .unwrap();
    fixture.store.purge_claim(&claim.id, "purge").unwrap();

    let replacement = fixture
        .store
        .remember_claim_with_cardinality(
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
            "plan-command",
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
            stream_id: "understated".to_owned(),
            kind: ViewKind::Continuity,
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan")
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
    assert_eq!(status.runs[0].status, "committed");
    assert!(
        fixture
            .store
            .list_observation_runs(&access("user"), None, Some("running"))
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
        store
            .get_event(&access("user"), &event.data.id)
            .unwrap()
            .content,
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
    for required in ["cardinality", "value_hash", "origin_run_id"] {
        assert!(claim_columns.iter().any(|(name, _, _)| name == required));
    }
    for removed in ["valid_from", "valid_to", "expires_at"] {
        assert!(!claim_columns.iter().any(|(name, _, _)| name == removed));
    }
    for (table, columns) in [
        (
            "memory_operation_refs",
            vec!["record_id", "idempotency_key"],
        ),
        (
            "memory_fts_refs",
            vec!["record_type", "record_id", "fts_rowid"],
        ),
    ] {
        assert_eq!(
            table_columns(&connection, table)
                .iter()
                .map(|(name, _, _)| name.as_str())
                .collect::<Vec<_>>(),
            columns
        );
    }
    // Purge drops tombstoned refs by key; without this index it scans every ref.
    let ref_plan: Vec<String> = connection
        .prepare("EXPLAIN QUERY PLAN DELETE FROM memory_operation_refs WHERE idempotency_key='k'")
        .unwrap()
        .query_map([], |row| row.get(3))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(
        ref_plan
            .iter()
            .any(|step| step.contains("memory_operation_refs_by_key")),
        "{ref_plan:?}"
    );
    assert!(
        table_columns(&connection, "observation_runs")
            .iter()
            .any(|(name, not_null, _)| name == "truncated_event_ids_json" && *not_null == 1)
    );
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
    for index in [
        "one_active_single_claim_per_logical_key",
        "one_active_set_claim_per_value",
    ] {
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master WHERE type='index' AND name=?1
                )",
                [index],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists);
    }
    let operation_columns = table_columns(&connection, "memory_operations");
    assert!(
        !operation_columns
            .iter()
            .any(|(name, _, _)| name == "purged")
    );
    assert_eq!(
        operation_columns
            .iter()
            .map(|(name, not_null, _)| (name.as_str(), *not_null))
            .collect::<Vec<_>>(),
        [
            ("idempotency_key", 0),
            ("operation", 1),
            ("request_hash", 0)
        ]
    );
    // Result bodies live in their own table, in commit order, so compaction
    // can delete the oldest rows and free whole pages.
    assert_eq!(
        table_columns(&connection, "memory_operation_results")
            .iter()
            .map(|(name, not_null, _)| (name.as_str(), *not_null))
            .collect::<Vec<_>>(),
        [
            ("id", 0),
            ("idempotency_key", 1),
            ("result_json", 1),
            ("created_at", 1)
        ]
    );
}

#[test]
fn context_omits_an_observation_when_raw_sources_partially_overlap() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let first = fixture.event(
        "user",
        "stream",
        "first source",
        Sensitivity::Normal,
        "event-1",
    );
    let second = fixture.event(
        "user",
        "stream",
        "second source",
        Sensitivity::Normal,
        "event-2",
    );
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let result = ObserverResult {
        observations: vec![ObservationDraft {
            kind: ObservationKind::Decision,
            content: "one interpretation covering both events".to_owned(),
            importance: 1.0,
            confidence: 1.0,
            source_event_ids: vec![first.id, second.id.clone()],
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
        .commit_observation(&plan.run_id, result, "commit")
        .unwrap();

    let context = fixture
        .store
        .compose_context("user", "stream", 1_000, 140, None)
        .unwrap();
    assert_eq!(context.recent_events.len(), 1);
    assert_eq!(context.recent_events[0].id, second.id);
    assert!(context.observations.is_empty());
    assert!(context.diagnostics.omitted_items.iter().any(|item| {
        item.id == commit.observations[0].id
            && item.reason == "source events already present in raw tail"
    }));
}

#[test]
fn continuity_view_commits_compare_and_swap_per_stream() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "source", Sensitivity::Normal, "event");
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan")
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
            kind: ViewKind::Continuity,
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
        kind: ViewKind::Continuity,
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
            .filter(|view| { view.kind == ViewKind::Continuity })
            .count(),
        1
    );

    let second = fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Continuity,
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
fn purging_a_view_source_removes_all_successor_generations() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let first_event = fixture.event("user", "stream", "first", Sensitivity::Normal, "event-1");
    let first_plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-1")
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
            kind: ViewKind::Continuity,
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", "plan-2")
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
            kind: ViewKind::Continuity,
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
fn incompatible_database_versions_are_rejected_without_schema_writes() {
    let directory = tempfile::tempdir().unwrap();
    for version in [1_i64, 2, 3, 4, 6, 99] {
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
        assert_eq!(
            error.downcast_ref::<KernelError>().map(KernelError::kind),
            Some(KernelErrorKind::SchemaMismatch)
        );
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
    assert_eq!(
        error.downcast_ref::<KernelError>().map(KernelError::kind),
        Some(KernelErrorKind::SchemaMismatch)
    );
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

fn observations_only(event_id: &str, label: &str, count: usize, importance: f64) -> ObserverResult {
    ObserverResult {
        observations: (0..count)
            .map(|index| ObservationDraft {
                kind: ObservationKind::Event,
                content: format!("{label} {index}"),
                importance,
                confidence: 1.0,
                source_event_ids: vec![event_id.to_owned()],
                event_time_from: None,
                event_time_to: None,
            })
            .collect(),
        claims: vec![],
        continuation: ContinuationDraft {
            current_task: Some("Keep going".to_owned()),
            ..ContinuationDraft::default()
        },
        ambiguities: vec![],
        empty_reason: None,
    }
}

fn plan_run(fixture: &mut Fixture, scope: &str, stream: &str, key: &str) -> ObservationPlan {
    fixture
        .store
        .plan_observation(scope, stream, 100_000, "fake", "v1", key)
        .unwrap()
        .data
        .into_plan()
        .unwrap()
}

#[test]
fn reflected_observations_do_not_starve_new_unreflected_observations() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let mut reflected = Vec::new();
    for batch in 0..2 {
        let event = fixture.event(
            "user",
            "stream",
            "old work",
            Sensitivity::Normal,
            &format!("old-event-{batch}"),
        );
        let plan = plan_run(&mut fixture, "user", "stream", &format!("old-plan-{batch}"));
        let commit = fixture
            .store
            .commit_observation(
                &plan.run_id,
                observations_only(&event.id, "reflected", 150, 0.9),
                &format!("old-commit-{batch}"),
            )
            .unwrap();
        reflected.extend(commit.observations.iter().map(|item| item.id.clone()));
    }
    fixture
        .store
        .create_view(CreateView {
            scope_id: "user".to_owned(),
            stream_id: "stream".to_owned(),
            kind: ViewKind::Continuity,
            content: "everything so far".to_owned(),
            source_from_sequence: 1,
            source_through_sequence: 2,
            source_observation_ids: reflected,
            expected_previous_view_id: None,
            model: None,
            prompt_version: None,
            token_count: None,
            idempotency_key: "view".to_owned(),
        })
        .unwrap();
    let event = fixture.event(
        "user",
        "stream",
        "new work",
        Sensitivity::Normal,
        "new-event",
    );
    let plan = plan_run(&mut fixture, "user", "stream", "new-plan");
    let fresh = fixture
        .store
        .commit_observation(
            &plan.run_id,
            observations_only(&event.id, "fresh", 5, 0.5),
            "new-commit",
        )
        .unwrap();
    for compact in [false, true] {
        let context = if compact {
            fixture
                .store
                .compose_compact_context("user", "stream", 100_000, 0, None)
        } else {
            fixture
                .store
                .compose_context("user", "stream", 100_000, 0, None)
        }
        .unwrap();
        let mut shown: Vec<&str> = context
            .observations
            .iter()
            .map(|item| item.id.as_str())
            .collect();
        shown.sort_unstable();
        let mut expected: Vec<&str> = fresh
            .observations
            .iter()
            .map(|item| item.id.as_str())
            .collect();
        expected.sort_unstable();
        assert_eq!(shown, expected);
        assert!(!context.diagnostics.truncated);
    }
}

#[test]
fn pending_claim_backlog_keeps_the_newest_claims_in_context() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.event("user", "stream", "seed", Sensitivity::Normal, "seed");
    let mut ids = Vec::new();
    for index in 0..260 {
        let claim = fixture
            .store
            .propose_claim(
                "user",
                ClaimKind::Decision,
                "backlog",
                &format!("item-{index}"),
                json!(index),
                &[],
                &format!("propose-{index}"),
            )
            .unwrap()
            .data;
        ids.push(claim.id);
    }
    let context = fixture
        .store
        .compose_context("user", "stream", 1_000_000, 0, None)
        .unwrap();
    let shown: Vec<&str> = context
        .pending_claims
        .iter()
        .map(|claim| claim.id.as_str())
        .collect();
    assert_eq!(shown.len(), 256);
    assert!(context.diagnostics.truncated);
    assert!(shown.contains(&ids[259].as_str()));
    assert!(!shown.contains(&ids[0].as_str()));
    assert_eq!(shown[0], ids[4]);
    assert_eq!(shown[255], ids[259]);
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
    let summary = fixture.store.reconcile("user", "reconcile").unwrap();
    assert!(summary.activated.is_empty());
    assert!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Active))
            .unwrap()
            .is_empty()
    );

    // A rejected claim that returns as pending still needs an explicit command.
    fixture.store.reject_claim(&rescoped.id, "reject").unwrap();
    let revived = fixture
        .store
        .rescope_claim(&rescoped.id, "user", "rescope-rejected")
        .unwrap();
    assert_eq!(revived.status, ClaimStatus::Pending);
    let summary = fixture.store.reconcile("user", "reconcile-2").unwrap();
    assert!(summary.activated.is_empty());
    assert_eq!(summary.left_pending, vec![revived.id.clone()]);
    assert!(
        fixture
            .store
            .list_claims("user", false, Some(ClaimStatus::Active))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn deeper_scope_single_claims_shadow_ancestors_in_context_and_plans() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("thread", ScopeKind::Thread, Some("user"));
    let mut remember = |scope: &str, predicate: &str, cardinality, value: &str, key: &str| {
        fixture
            .store
            .remember_claim_with_cardinality(
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
        .plan_observation("user", "stream", 400, "fake", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let stub = &plan.events[0];
    assert_eq!(stub.id, big.id);
    assert_eq!(stub.content["truncated"], json!(true));
    assert_eq!(stub.content["reason"], json!("exceeds observation budget"));
    let preview = stub.content["preview"].as_str().unwrap();
    assert!(!preview.is_empty() && preview.len() < 4_000);
    assert_eq!(stub.metadata, json!({}));
    assert_eq!(plan.to_sequence, 1);

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
    let empty = ObserverResult {
        observations: vec![],
        claims: vec![],
        continuation: ContinuationDraft::default(),
        ambiguities: vec![],
        empty_reason: Some("only a truncated event".to_owned()),
    };
    fixture
        .store
        .commit_observation(&plan.run_id, empty, "commit-empty")
        .unwrap();
    let status = fixture
        .store
        .stream_status(&access("user"), "stream")
        .unwrap();
    assert_eq!(status.observed_through_sequence, 1);

    let next = plan_run(&mut fixture, "user", "stream", "next-plan");
    assert_eq!(next.events[0].id, small.id);
    assert_eq!(next.events[0].content, json!("small"));
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

fn raw_count(fixture: &Fixture, sql: &str, value: &str) -> i64 {
    Connection::open(fixture._directory.path().join("memory.db"))
        .unwrap()
        .query_row(sql, [value], |row| row.get(0))
        .unwrap()
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
fn context_reads_one_snapshot_while_another_connection_commits() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.event("user", "stream", "seed", Sensitivity::Normal, "seed");
    let path = fixture._directory.path().join("memory.db");
    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|threads| {
        let reader = threads.spawn(|| {
            let reader = MemoryStore::open(&path).unwrap();
            let mut checked = 0;
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                let context = reader
                    .compose_context("user", "stream", 1_000_000, 0, None)
                    .unwrap();
                // Each commit adds one pending claim and one observation together.
                assert_eq!(context.pending_claims.len(), context.observations.len());
                checked += 1;
            }
            checked
        });
        for index in 0..60 {
            let event = fixture.event(
                "user",
                "stream",
                "work",
                Sensitivity::Normal,
                &format!("event-{index}"),
            );
            let plan = plan_run(&mut fixture, "user", "stream", &format!("plan-{index}"));
            fixture
                .store
                .commit_observation(
                    &plan.run_id,
                    observer_result(&event.id, &format!("value-{index}")),
                    &format!("commit-{index}"),
                )
                .unwrap();
        }
        done.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(reader.join().unwrap() > 0);
    });
}

#[test]
fn claim_budget_pins_user_claims_then_keeps_the_newest_claims() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("project", ScopeKind::Project, Some("user"));
    fixture.event("project", "stream", "start", Sensitivity::Normal, "seed");
    let mut ids = std::collections::HashMap::new();
    for (scope, prefix) in [("user", "u"), ("project", "p")] {
        for index in 0..10 {
            let subject = format!("{prefix}{index}");
            let claim = fixture
                .store
                .remember_claim(
                    scope,
                    ClaimKind::Fact,
                    &subject,
                    "value",
                    json!("v".repeat(400)),
                    &[],
                    &format!("remember-{subject}"),
                )
                .unwrap()
                .data;
            ids.insert(claim.id, subject);
        }
    }
    let all = fixture
        .store
        .compose_context("project", "stream", 1_000_000, 0, None)
        .unwrap();
    assert_eq!(all.claims.len(), 20);
    let largest = all
        .claims
        .iter()
        .map(|claim| serde_json::to_string(claim).unwrap().chars().count() as i64 / 4 + 2)
        .max()
        .unwrap();
    // Room for ten claims: the pin share holds five user claims, and the
    // newest five of the rest are project claims.
    let claim_budget = largest * 10 + largest / 2;
    let context = fixture
        .store
        .compose_context("project", "stream", claim_budget * 2, 0, None)
        .unwrap();
    let mut kept: Vec<&str> = context
        .claims
        .iter()
        .map(|claim| ids[&claim.id].as_str())
        .collect();
    kept.sort_unstable();
    assert_eq!(
        kept,
        ["p5", "p6", "p7", "p8", "p9", "u5", "u6", "u7", "u8", "u9"]
    );
    let mut omitted: Vec<&str> = context
        .diagnostics
        .omitted_items
        .iter()
        .filter(|item| item.reason == "active claim budget")
        .map(|item| ids[&item.id].as_str())
        .collect();
    omitted.sort_unstable();
    assert_eq!(
        omitted,
        ["p0", "p1", "p2", "p3", "p4", "u0", "u1", "u2", "u3", "u4"]
    );

    let plan = fixture
        .store
        .plan_observation("project", "stream", claim_budget * 2, "test", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(plan.active_claims.len(), 10);
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
            json!("zsh"),
            &[],
            "remember-after",
        )
        .unwrap()
        .data;
    let replay = fixture
        .store
        .plan_observation("user", "stream", 100_000, "fake", "v1", "plan")
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
        .plan_observation("user", "stream", 100_000, "fake", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(after_purge.active_claims.len(), 1);

    // Purging one of the run's events tombstones the saved plan.
    fixture.store.purge_event(&first.id, "purge-event").unwrap();
    let error = fixture
        .store
        .plan_observation("user", "stream", 100_000, "fake", "v1", "plan")
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
