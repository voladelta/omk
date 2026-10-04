use omk::*;
use rusqlite::Connection;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::process::Command;

fn event(store: &mut MemoryStore, text: &str, key: &str) -> MemoryEvent {
    store
        .append_event(NewEvent {
            scope_id: "thread".into(),
            stream_id: "stream".into(),
            kind: EventKind::UserMessage,
            actor_id: None,
            occurred_at: None,
            content: json!(text),
            token_count: None,
            sensitivity: Sensitivity::Normal,
            metadata: json!({}),
            idempotency_key: key.into(),
        })
        .unwrap()
        .data
}

fn setup(path: &std::path::Path) -> MemoryStore {
    let mut store = MemoryStore::open(path).unwrap();
    store
        .create_scope("project", ScopeKind::Project, None, None, "project")
        .unwrap();
    store
        .create_scope("thread", ScopeKind::Thread, Some("project"), None, "thread")
        .unwrap();
    store
}

fn claim(store: &mut MemoryStore, scope: &str, value: &str, key: &str) -> Claim {
    store
        .remember_claim(
            scope,
            ClaimKind::Fact,
            "Atlas",
            "launch",
            json!(value),
            &[],
            key,
        )
        .unwrap()
        .data
}

fn empty_result() -> ObserverResult {
    ObserverResult {
        observations: vec![],
        claims: vec![],
        ambiguities: vec![],
        continuation: ContinuationDraft::default(),
        empty_reason: Some("No durable detail".into()),
    }
}

#[test]
fn run_listing_filters_scope_before_decoding_unrelated_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    event(&mut store, "visible", "visible");
    let visible = store
        .plan_observation("thread", "stream", 1000, "test", "v1", "visible-plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    store
        .create_scope("other", ScopeKind::Project, None, None, "other")
        .unwrap();
    store
        .append_event(NewEvent {
            scope_id: "other".into(),
            stream_id: "other-stream".into(),
            kind: EventKind::UserMessage,
            actor_id: None,
            occurred_at: None,
            content: json!("unrelated"),
            token_count: None,
            sensitivity: Sensitivity::Normal,
            metadata: json!({}),
            idempotency_key: "other-event".into(),
        })
        .unwrap();
    let unrelated = store
        .plan_observation("other", "other-stream", 1000, "test", "v1", "other-plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    // An unrelated row must never reach this request's JSON decoder.
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE observation_runs SET ambiguities_json='invalid JSON' WHERE id=?1",
        [&unrelated.run_id],
    )
    .unwrap();
    for scope in ["thread", "project"] {
        let runs = store
            .list_observation_runs(&ReadAccess::agent(scope), None, Some("pending"))
            .unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].id, visible.run_id);
    }
    assert!(
        store
            .list_observation_runs(&ReadAccess::agent("thread"), Some("other-stream"), None)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_observation_runs(&ReadAccess::agent("other"), None, None)
            .is_err()
    );
}

#[test]
fn resolved_recall_checks_each_source_and_refreshes_between_requests() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    let record = claim(&mut store, "thread", "first", "claim");
    let access = ReadAccess::agent("project");
    assert!(
        !store
            .explain_claim(&access, &record.id)
            .unwrap()
            .source_events
            .is_empty()
    );
    store
        .create_scope(
            "new-child",
            ScopeKind::Task,
            Some("project"),
            None,
            "new-child",
        )
        .unwrap();
    let child = store
        .append_event(NewEvent {
            scope_id: "new-child".into(),
            stream_id: "child-stream".into(),
            kind: EventKind::ToolResult,
            actor_id: None,
            occurred_at: None,
            content: json!("secret detail"),
            token_count: None,
            sensitivity: Sensitivity::Secret,
            metadata: json!({}),
            idempotency_key: "child-event".into(),
        })
        .unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO claim_sources(claim_id,event_id) VALUES (?1,?2)",
        rusqlite::params![record.id, child.id],
    )
    .unwrap();
    let safe = store.explain_claim(&access, &record.id).unwrap();
    let secret = safe
        .source_events
        .iter()
        .find(|event| event.id == child.id)
        .unwrap();
    assert_eq!(secret.content["redacted"], true);
    let revealed = store
        .explain_claim(
            &ReadAccess {
                anchor_scope_id: "project".into(),
                reveal_secrets: true,
            },
            &record.id,
        )
        .unwrap();
    assert!(
        revealed
            .source_events
            .iter()
            .any(|event| event.content == json!("secret detail"))
    );
    store
        .create_scope("other", ScopeKind::Project, None, None, "other")
        .unwrap();
    let other = store
        .append_event(NewEvent {
            scope_id: "other".into(),
            stream_id: "other-stream".into(),
            kind: EventKind::UserMessage,
            actor_id: None,
            occurred_at: None,
            content: json!("outside scope"),
            token_count: None,
            sensitivity: Sensitivity::Normal,
            metadata: json!({}),
            idempotency_key: "other-event".into(),
        })
        .unwrap();
    conn.execute(
        "INSERT INTO claim_sources(claim_id,event_id) VALUES (?1,?2)",
        rusqlite::params![record.id, other.id],
    )
    .unwrap();
    let error = store.explain_claim(&access, &record.id).unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().unwrap().kind(),
        KernelErrorKind::ScopeViolation
    );
}

#[test]
fn purge_updates_shared_runs_once_and_preserves_unrelated_replays() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    let source = event(&mut store, "source", "source");
    let unrelated = event(&mut store, "unrelated", "unrelated");
    for (subject, key) in [("one", "claim-one"), ("two", "claim-two")] {
        store
            .remember_claim(
                "thread",
                ClaimKind::Fact,
                subject,
                "value",
                json!("accepted"),
                std::slice::from_ref(&source.id),
                key,
            )
            .unwrap();
    }
    let mut plans = Vec::new();
    for key in ["committed-plan", "pending-plan", "failed-plan"] {
        plans.push(
            store
                .plan_observation("thread", "memory-commands:thread", 6000, "test", "v1", key)
                .unwrap()
                .data
                .into_plan()
                .unwrap(),
        );
    }
    assert_eq!(plans[0].events.len(), 2);
    store
        .fail_observation(&plans[2].run_id, "observer failure", "fail")
        .unwrap();
    store
        .commit_observation(&plans[0].run_id, empty_result(), "commit")
        .unwrap();
    let conn = Connection::open(&path).unwrap();
    // Installed after opening the store; these counters are fixture-only instrumentation.
    conn.execute_batch("CREATE TABLE run_updates(id TEXT);
        CREATE TRIGGER count_run_update AFTER UPDATE ON observation_runs BEGIN INSERT INTO run_updates VALUES (NEW.id); END;
        INSERT INTO memory_operations VALUES ('already-purged','event.append',NULL,NULL,'2026-01-01T00:00:00+00:00');").unwrap();
    let purge = store.purge_event(&source.id, "purge").unwrap();
    assert_eq!(purge["purgedCommandEvents"], 2);
    assert_eq!(purge["affectedRunIds"].as_array().unwrap().len(), 3);
    for (plan, status) in plans.iter().zip(["committed", "stale", "failed"]) {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_updates WHERE id=?1",
                [&plan.run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        let run = store
            .get_observation_run(&ReadAccess::agent("thread"), &plan.run_id)
            .unwrap();
        assert_eq!(run.status, status);
        assert_eq!(run.source_integrity, SourceIntegrity::PrivacyPurged);
        assert!(run.ambiguities.is_empty());
        if status == "failed" {
            assert_eq!(run.error.as_deref(), Some("observer failure"));
        }
        if status == "stale" {
            assert_eq!(run.error.as_deref(), Some("source evidence privacy-purged"));
        }
    }
    for key in [
        "source",
        "claim-one",
        "claim-two",
        "committed-plan",
        "pending-plan",
        "failed-plan",
        "commit",
        "already-purged",
    ] {
        let tombstoned: bool = conn.query_row("SELECT request_hash IS NULL AND result_json IS NULL FROM memory_operations WHERE idempotency_key=?1", [key], |row| row.get(0)).unwrap();
        assert!(tombstoned, "operation {key} retained purged evidence");
    }
    assert_eq!(event(&mut store, "unrelated", "unrelated").id, unrelated.id);
    assert!(
        store
            .purge_event(&source.id, "purge")
            .unwrap()
            .operation
            .replayed
    );
}

#[test]
fn legacy_observer_replay_precedes_new_admission_limits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    event(&mut store, "source", "source");
    let plan = store
        .plan_observation("thread", "stream", 1000, "test", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let mut legacy_result = empty_result();
    legacy_result.continuation.completed = vec!["done".into(); 257];
    let mut commit = store
        .commit_observation(&plan.run_id, empty_result(), "commit")
        .unwrap()
        .data;
    let content = serde_json::to_string_pretty(&legacy_result.continuation).unwrap();
    commit.continuation_view.content = content.clone();
    commit.continuation_view.token_count = content.chars().count().div_ceil(4) as i64;
    let mut hash = Sha256::new();
    hash.update(b"observation.commit\0");
    hash.update(serde_json::to_vec(&(&plan.run_id, &legacy_result)).unwrap());
    let conn = Connection::open(&path).unwrap();
    // Stored v6 operation fixture accepted before item limits were introduced.
    conn.execute("UPDATE memory_operations SET request_hash=?1,result_json=?2 WHERE idempotency_key='commit'",
        rusqlite::params![format!("{:x}", hash.finalize()), serde_json::to_string(&commit).unwrap()]).unwrap();
    conn.execute(
        "UPDATE memory_views SET content=?1,token_count=?2 WHERE id=?3",
        rusqlite::params![
            content,
            commit.continuation_view.token_count,
            commit.continuation_view.id
        ],
    )
    .unwrap();
    let replay = store
        .commit_observation(&plan.run_id, legacy_result.clone(), "commit")
        .unwrap();
    assert!(replay.operation.replayed);
    assert_eq!(
        serde_json::to_value(&replay.data).unwrap(),
        serde_json::to_value(&commit).unwrap()
    );
    let input = dir.path().join("legacy.json");
    std::fs::write(&input, serde_json::to_vec(&legacy_result).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omk"))
        .args([
            "--db",
            path.to_str().unwrap(),
            "observe",
            "commit",
            "--run",
            &plan.run_id,
            "--input",
            input.to_str().unwrap(),
            "--idempotency-key",
            "commit",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["operation"]["replayed"],
        true
    );
    let mut changed = legacy_result.clone();
    changed.continuation.completed[0] = "different".into();
    assert_eq!(
        store
            .commit_observation(&plan.run_id, changed, "commit")
            .unwrap_err()
            .downcast_ref::<KernelError>()
            .unwrap()
            .kind(),
        KernelErrorKind::IdempotencyConflict
    );
    conn.execute("UPDATE memory_operations SET request_hash=NULL,result_json=NULL WHERE idempotency_key='commit'", []).unwrap();
    assert_eq!(
        store
            .commit_observation(&plan.run_id, legacy_result, "commit")
            .unwrap_err()
            .downcast_ref::<KernelError>()
            .unwrap()
            .kind(),
        KernelErrorKind::PrivacyPurged
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM memory_views", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn rescope_conflict_rolls_back_and_key_can_be_reused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    let target = claim(&mut store, "project", "October 1", "target");
    let source = claim(&mut store, "thread", "October 15", "source");
    let conn = Connection::open(&path).unwrap();
    let before: i64 = conn
        .query_row("SELECT COUNT(*) FROM memory_events", [], |r| r.get(0))
        .unwrap();
    let provenance = store
        .explain_claim(&ReadAccess::agent("thread"), &target.id)
        .unwrap()
        .source_events
        .len();
    let error = store
        .rescope_claim(&source.id, "project", "rescope")
        .unwrap_err();
    let output = Command::new(env!("CARGO_BIN_EXE_omk"))
        .args([
            "--db",
            path.to_str().unwrap(),
            "claim",
            "rescope",
            "--id",
            &source.id,
            "--scope",
            "project",
            "--idempotency-key",
            "rescope",
        ])
        .output()
        .unwrap();
    let error_json: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error_json["error"]["code"], "claim_conflict");
    assert_eq!(error_json["error"]["sameKeyReusable"], true);
    assert_eq!(
        error.downcast_ref::<KernelError>().unwrap().kind(),
        KernelErrorKind::ClaimConflict
    );
    assert_eq!(
        store
            .explain_claim(&ReadAccess::agent("thread"), &source.id)
            .unwrap()
            .claim
            .status,
        ClaimStatus::Active
    );
    assert_eq!(
        store
            .explain_claim(&ReadAccess::agent("thread"), &target.id)
            .unwrap()
            .source_events
            .len(),
        provenance
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM memory_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        before
    );
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM memory_operations WHERE idempotency_key='rescope'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    let equal = claim(&mut store, "project", "October 15", "resolve");
    store.confirm_claim(&equal.id, "confirm-resolve").unwrap();
    let merged = store
        .rescope_claim(&source.id, "project", "rescope")
        .unwrap();
    assert_eq!(merged.id, equal.id);
    assert!(
        store
            .rescope_claim(&source.id, "project", "rescope")
            .unwrap()
            .operation
            .replayed
    );
}

#[test]
fn required_model_state_and_continuation_are_budgeted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    claim(&mut store, "project", &"x".repeat(2000), "large");
    event(&mut store, "small", "event");
    let error = store
        .plan_observation("thread", "stream", 100, "test", "v1", "plan")
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().unwrap().kind(),
        KernelErrorKind::BudgetExceeded
    );
    let output = Command::new(env!("CARGO_BIN_EXE_omk"))
        .args([
            "--db",
            path.to_str().unwrap(),
            "observe",
            "plan",
            "--scope",
            "thread",
            "--stream",
            "stream",
            "--max-tokens",
            "100",
            "--model",
            "test",
            "--prompt-version",
            "v1",
            "--idempotency-key",
            "plan",
        ])
        .output()
        .unwrap();
    let error_json: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error_json["error"]["code"], "budget_exceeded");
    assert_eq!(error_json["error"]["sameKeyReusable"], true);
    let plan = store
        .plan_observation("thread", "stream", 2000, "test", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert!(plan.model_payload().to_string().chars().count().div_ceil(4) <= 2000);
    let mut result = empty_result();
    result.continuation.current_task = Some("work".repeat(2000));
    store
        .commit_observation(&plan.run_id, result, "commit")
        .unwrap();
    event(&mut store, "next", "next");
    assert!(
        store
            .plan_observation("thread", "stream", 2000, "test", "v1", "next-plan")
            .is_err()
    );
    let bundle = store
        .compose_context("thread", "stream", 1000, 400, None)
        .unwrap();
    assert!(
        bundle
            .model_payload()
            .to_string()
            .chars()
            .count()
            .div_ceil(4) as i64
            <= bundle.diagnostics.estimated_tokens
    );
    assert!(bundle.diagnostics.estimated_tokens <= 1000);
    // The large claim exceeds half of this budget, so context reports it
    // instead of failing; a budget whose claim share fits it includes it.
    assert!(bundle.claims.is_empty());
    assert!(
        bundle
            .diagnostics
            .omitted_items
            .iter()
            .any(|item| item.reason == "active claim budget")
    );
    let roomier = store
        .compose_context("thread", "stream", 1400, 400, None)
        .unwrap();
    assert_eq!(roomier.claims.len(), 1);
}

#[test]
fn schema_damage_is_rejected_without_record_writes() {
    for damage in [
        "DROP INDEX one_active_single_claim_per_logical_key",
        "DROP INDEX one_active_single_claim_per_logical_key; CREATE UNIQUE INDEX one_active_single_claim_per_logical_key ON claims(scope_id,kind,subject,predicate) WHERE status='rejected' AND cardinality='single'",
        "ALTER TABLE memory_events RENAME COLUMN metadata_json TO wrong_name",
        "DROP TABLE observation_sources",
        "DROP TABLE memory_fts; CREATE VIRTUAL TABLE memory_fts USING fts5(record_type UNINDEXED,record_id UNINDEXED,scope_id UNINDEXED,text,tokenize='porter')",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory.db");
        drop(setup(&path));
        drop(MemoryStore::open(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(damage).unwrap();
        let error = MemoryStore::open(&path)
            .err()
            .expect("reject damaged schema");
        assert_eq!(
            error.downcast_ref::<KernelError>().unwrap().kind(),
            KernelErrorKind::SchemaMismatch
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM memory_operations", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
}

#[test]
fn history_pages_do_not_decode_distant_invalid_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    for index in 0..200 {
        event(&mut store, "history", &format!("event-{index}"));
    }
    let conn = Connection::open(&path).unwrap();
    // An eager full-history read fails when it parses this distant JSON value.
    conn.execute("UPDATE memory_events SET content_json='invalid-json' WHERE stream_id='stream' AND sequence=100", []).unwrap();
    let plan = store
        .plan_observation("thread", "stream", 500, "test", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert!(plan.to_sequence < 32);
    let bundle = store
        .compose_context("thread", "stream", 500, 450, None)
        .unwrap();
    assert_eq!(bundle.recent_events.last().unwrap().sequence, 200);
    assert!(bundle.recent_events.len() < 32);
    assert!(bundle.diagnostics.truncated);
    assert!(
        store
            .recall_event_range(&ReadAccess::agent("thread"), "stream", 1, 200)
            .is_err()
    );
}

#[test]
fn extreme_token_hints_fail_without_integer_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = setup(&dir.path().join("memory.db"));
    store
        .append_event(NewEvent {
            scope_id: "thread".into(),
            stream_id: "stream".into(),
            kind: EventKind::UserMessage,
            actor_id: None,
            occurred_at: None,
            content: json!("small"),
            token_count: Some(i64::MAX),
            sensitivity: Sensitivity::Normal,
            metadata: json!({}),
            idempotency_key: "event".into(),
        })
        .unwrap();
    // The inflated hint cannot fit, so the first event is planned as a stub.
    let plan = store
        .plan_observation("thread", "stream", i64::MAX, "test", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(plan.events[0].content["truncated"], json!(true));
    let error = store
        .plan_observation("thread", "stream", 1, "test", "v1", "tiny-plan")
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<KernelError>().unwrap().kind(),
        KernelErrorKind::BudgetExceeded
    );
    let context = store
        .compose_context("thread", "stream", i64::MAX, i64::MAX, None)
        .unwrap();
    assert!(context.recent_events.is_empty());
}

#[test]
fn search_modes_status_previews_and_cli_are_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    let first = claim(&mut store, "project", "October 1", "first");
    let second = claim(&mut store, "project", "October 15", "second");
    store.confirm_claim(&second.id, "confirm-second").unwrap();
    let raw = event(
        &mut store,
        &format!("rare separated identifier {}", "界".repeat(1000)),
        "raw",
    );
    assert!(
        store
            .search_full_text("thread", "rare identifier", 10)
            .unwrap()
            .is_empty()
    );
    let options = SearchOptions {
        mode: SearchMode::Terms,
        current_only: false,
    };
    let hits = store
        .search_with_options("thread", "rare identifier", 10, options)
        .unwrap();
    assert_eq!(hits[0].id, raw.id);
    assert_eq!(hits[0].text.chars().count(), 512);
    let context = store
        .compose_context(
            "thread",
            "stream",
            2000,
            0,
            Some("rare separated identifier"),
        )
        .unwrap();
    let recalled = context
        .recalled_evidence
        .iter()
        .find(|event| event.id == raw.id)
        .unwrap();
    assert_eq!(recalled.content, raw.content);
    let history = store.search_full_text("thread", "October", 20).unwrap();
    assert!(
        history
            .iter()
            .any(|h| h.id == first.id && h.claim_status == Some(ClaimStatus::Superseded))
    );
    let current = store
        .search_with_options(
            "thread",
            "October",
            20,
            SearchOptions {
                current_only: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(!current.iter().any(|h| h.id == first.id));
    assert!(current.iter().any(|h| h.id == second.id));
    assert_eq!(
        serde_json::to_value(&history).unwrap(),
        serde_json::to_value(store.search_full_text("thread", "October", 20).unwrap()).unwrap()
    );
    assert!(
        store
            .search_full_text("thread", &"x".repeat(4097), 10)
            .is_err()
    );
    assert!(
        store
            .search_with_options("thread", &"word ".repeat(65), 10, options)
            .is_err()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_omk"))
        .args([
            "--db",
            path.to_str().unwrap(),
            "recall",
            "search",
            "--scope",
            "thread",
            "--query",
            "rare identifier",
            "--terms",
            "--current-only",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json[0]["id"], raw.id);
    let conflict = Command::new(env!("CARGO_BIN_EXE_omk"))
        .args([
            "--db",
            path.to_str().unwrap(),
            "recall",
            "search",
            "--scope",
            "thread",
            "--query",
            "rare",
            "--terms",
            "--fts-query",
        ])
        .output()
        .unwrap();
    assert!(!conflict.status.success());
}

#[test]
fn observer_limits_reject_before_commit_or_json_parse() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    let source = event(&mut store, "source", "event");
    let plan = store
        .plan_observation("thread", "stream", 1000, "test", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let mut result = empty_result();
    result.continuation.completed = vec!["item".into(); MAX_OBSERVER_ITEMS + 1];
    assert!(
        store
            .commit_observation(&plan.run_id, result, "commit")
            .is_err()
    );
    let mut result = empty_result();
    result.ambiguities.push(AmbiguityDraft {
        description: "unclear".into(),
        source_event_ids: vec![source.id; MAX_SOURCE_IDS + 1],
    });
    assert!(
        store
            .commit_observation(&plan.run_id, result, "commit")
            .is_err()
    );
    let mut result = empty_result();
    result.continuation.current_task = Some("x".repeat(MAX_OBSERVER_BYTES));
    assert!(
        store
            .commit_observation(&plan.run_id, result, "commit")
            .is_err()
    );
    let oversized = dir.path().join("oversized.json");
    std::fs::write(&oversized, vec![b'x'; MAX_OBSERVER_BYTES + 1]).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_omk"))
        .args([
            "--db",
            path.to_str().unwrap(),
            "observe",
            "commit",
            "--run",
            &plan.run_id,
            "--input",
            oversized.to_str().unwrap(),
            "--idempotency-key",
            "commit",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("exceeds 1048576 bytes"));
    store
        .commit_observation(&plan.run_id, empty_result(), "commit")
        .unwrap();
}

#[test]
fn context_caps_legacy_candidates_and_sources_but_exact_recall_is_complete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    let first = event(&mut store, "source detail", "first");
    let plan = store
        .plan_observation("thread", "stream", 1000, "test", "v1", "plan")
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    let mut result = empty_result();
    result.observations.push(ObservationDraft {
        kind: ObservationKind::Event,
        content: "targetneedle".into(),
        importance: 1.0,
        confidence: 1.0,
        source_event_ids: vec![first.id],
        event_time_from: None,
        event_time_to: None,
    });
    let commit = store
        .commit_observation(&plan.run_id, result, "commit")
        .unwrap();
    for index in 1..300 {
        event(&mut store, "source detail", &format!("event-{index}"));
    }
    let conn = Connection::open(&path).unwrap();
    let id = &commit.observations[0].id;
    // Simulate a valid old database whose source fanout exceeds new write limits.
    conn.execute("INSERT OR IGNORE INTO observation_sources SELECT ?1,id FROM memory_events WHERE stream_id='stream'", [id]).unwrap();
    for index in 0..257 {
        conn.execute("INSERT INTO observations SELECT ?1,run_id,scope_id,?2,content,0,confidence,event_time_from,event_time_to,source_start_sequence,source_end_sequence,observer_model,prompt_version,created_at FROM observations WHERE id=?3",
            rusqlite::params![format!("legacy-{index:03}"), if index == 256 { "invalid-kind" } else { "event" }, id]).unwrap();
    }
    let bundle = store
        .compose_context("thread", "stream", 100_000, 0, Some("targetneedle"))
        .unwrap();
    assert_eq!(bundle.recalled_evidence.len(), MAX_SOURCE_IDS);
    assert!(bundle.diagnostics.truncated);
    assert_eq!(
        store
            .recall_by_observation(&ReadAccess::agent("thread"), id)
            .unwrap()
            .len(),
        300
    );
    // A tight budget still gives the targeted raw source room before optional summaries.
    let tight = store
        .compose_context("thread", "stream", 300, 0, Some("targetneedle"))
        .unwrap();
    assert!(!tight.recalled_evidence.is_empty());
    assert!(
        tight
            .model_payload()
            .to_string()
            .chars()
            .count()
            .div_ceil(4) as i64
            <= tight.diagnostics.estimated_tokens
    );
}
