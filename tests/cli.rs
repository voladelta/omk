use std::io::Write;
use std::process::{Command, Output, Stdio};

use serde_json::Value;

fn omk(db: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_omk"))
        .arg("--db")
        .arg(db)
        .arg("--compact")
        .args(args)
        .output()
        .unwrap()
}

fn success_json(db: &std::path::Path, args: &[&str]) -> Value {
    let output = omk(db, args);
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn omk_with_stdin(db: &std::path::Path, args: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_omk"))
        .arg("--db")
        .arg(db)
        .arg("--compact")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn cli_reports_replays_and_structured_idempotency_conflicts() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("memory.db");
    let initialized = success_json(&db, &["init"]);
    assert_eq!(initialized["data"]["ready"], true);
    assert_eq!(initialized["data"]["schemaVersion"], 3);
    assert_eq!(initialized["operation"]["replayed"], false);
    let created = success_json(
        &db,
        &[
            "scope",
            "add",
            "--id",
            "user:cli",
            "--kind",
            "user",
            "--idempotency-key",
            "scope-key",
        ],
    );
    let replay = success_json(
        &db,
        &[
            "scope",
            "add",
            "--id",
            "user:cli",
            "--kind",
            "user",
            "--idempotency-key",
            "scope-key",
        ],
    );
    assert_eq!(created["operation"]["replayed"], false);
    assert_eq!(replay["operation"]["replayed"], true);

    success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:cli",
            "--stream",
            "stream",
            "--kind",
            "user-message",
            "--content",
            "original",
            "--idempotency-key",
            "event-key",
        ],
    );
    let conflict = omk(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:cli",
            "--stream",
            "stream",
            "--kind",
            "user-message",
            "--content",
            "changed",
            "--idempotency-key",
            "event-key",
        ],
    );
    assert!(!conflict.status.success());
    let error: Value = serde_json::from_slice(&conflict.stderr).unwrap();
    assert_eq!(error["error"]["code"], "idempotency_conflict");
    assert_eq!(error["error"]["retryable"], false);
    assert_eq!(error["error"]["sameKeyReusable"], false);

    let secret = success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:cli",
            "--stream",
            "secret-stream",
            "--kind",
            "tool-result",
            "--content",
            "secret-response-marker",
            "--metadata",
            r#"{"credential":"secret-metadata-marker"}"#,
            "--sensitivity",
            "secret",
            "--idempotency-key",
            "secret-key",
        ],
    );
    assert_eq!(
        secret["data"]["content"],
        serde_json::json!({"redacted": true, "reason": "secret"})
    );
    assert_eq!(secret["data"]["metadata"], serde_json::json!({}));
    let encoded = serde_json::to_string(&secret).unwrap();
    assert!(!encoded.contains("secret-response-marker"));
    assert!(!encoded.contains("secret-metadata-marker"));
    let replayed_secret = success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:cli",
            "--stream",
            "secret-stream",
            "--kind",
            "tool-result",
            "--content",
            "secret-response-marker",
            "--metadata",
            r#"{"credential":"secret-metadata-marker"}"#,
            "--sensitivity",
            "secret",
            "--idempotency-key",
            "secret-key",
        ],
    );
    assert_eq!(replayed_secret["operation"]["replayed"], true);
    assert!(
        !serde_json::to_string(&replayed_secret)
            .unwrap()
            .contains("secret-response-marker")
    );
}

#[test]
fn cli_literal_search_and_observer_errors_are_agent_safe() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("memory.db");
    success_json(&db, &["init"]);
    success_json(
        &db,
        &[
            "scope",
            "add",
            "--id",
            "user:cli",
            "--kind",
            "user",
            "--idempotency-key",
            "scope-key",
        ],
    );
    success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:cli",
            "--stream",
            "stream",
            "--kind",
            "user-message",
            "--content",
            "purge-derived marker",
            "--idempotency-key",
            "event-key",
        ],
    );
    let hits = success_json(
        &db,
        &[
            "recall",
            "search",
            "--scope",
            "user:cli",
            "--query",
            "purge-derived marker",
        ],
    );
    assert_eq!(hits.as_array().unwrap().len(), 1);

    let plan = success_json(
        &db,
        &[
            "observe",
            "plan",
            "--scope",
            "user:cli",
            "--stream",
            "stream",
            "--model",
            "fake",
            "--idempotency-key",
            "plan-key",
        ],
    );
    assert_eq!(plan["data"]["status"], "ready");
    assert!(plan["data"]["events"][0]["id"].is_string());
    assert!(plan["data"]["nextAction"].is_string());
    let run_id = plan["data"]["runId"].as_str().unwrap();
    let output = omk_with_stdin(
        &db,
        &[
            "observe",
            "commit",
            "--run",
            run_id,
            "--idempotency-key",
            "commit-key",
        ],
        br#"{"observations":[]}"#,
    );
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "invalid_input");
    assert_eq!(error["error"]["retryable"], false);
    assert_eq!(error["error"]["sameKeyReusable"], true);

    let corrected = omk_with_stdin(
        &db,
        &[
            "observe",
            "commit",
            "--run",
            run_id,
            "--idempotency-key",
            "commit-key",
        ],
        br#"{"observations":[],"claims":[],"continuation":{"currentTask":null,"completed":[],"blockers":[],"nextActions":[],"unresolvedQuestions":[]},"ambiguities":[],"emptyReason":"nothing durable"}"#,
    );
    assert!(
        corrected.status.success(),
        "corrected commit failed: {}",
        String::from_utf8_lossy(&corrected.stderr)
    );
    let corrected: Value = serde_json::from_slice(&corrected.stdout).unwrap();
    assert_eq!(corrected["data"]["continuationAction"], "created");

    let caught_up = success_json(
        &db,
        &[
            "observe",
            "plan",
            "--scope",
            "user:cli",
            "--stream",
            "stream",
            "--model",
            "fake",
            "--idempotency-key",
            "caught-up-plan-key",
        ],
    );
    assert_eq!(caught_up["data"]["status"], "caught-up");
    assert_eq!(caught_up["data"]["observedThroughSequence"], 1);
    assert!(caught_up["data"]["nextAction"].is_string());

    let run = success_json(&db, &["observe", "get", "--run", run_id]);
    assert_eq!(run["status"], "committed");
    assert_eq!(run["sourceIntegrity"], "intact");
    let status = success_json(&db, &["observe", "status", "--stream", "stream"]);
    assert_eq!(status["observedThroughSequence"], 1);
}

#[test]
fn cli_help_exposes_agent_critical_contracts() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("memory.db");

    let purge_help = omk(&db, &["event", "purge", "--help"]);
    assert!(purge_help.status.success());
    let purge_help = String::from_utf8(purge_help.stdout).unwrap();
    assert!(purge_help.contains("Event UUID"));
    assert!(purge_help.contains("affected derived record type"));

    let plan_help = omk(&db, &["observe", "plan", "--help"]);
    assert!(plan_help.status.success());
    let plan_help = String::from_utf8(plan_help.stdout).unwrap();
    assert!(plan_help.contains(".data.events[].id"));
    assert!(plan_help.contains("caught-up"));

    let append_help = omk(&db, &["event", "append", "--help"]);
    assert!(append_help.status.success());
    let append_help = String::from_utf8(append_help.stdout).unwrap();
    assert!(append_help.contains("Storage/privacy mode"));
    assert!(append_help.contains("do-not-store"));
}
