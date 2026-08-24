use std::io::Write;
use std::process::{Command, Output, Stdio};

use omk::SCHEMA_VERSION;
use serde_json::Value;

fn omk(db: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_omk"))
        .arg("--db")
        .arg(db)
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
    assert_eq!(initialized["data"]["schemaVersion"], SCHEMA_VERSION);
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

    let inline_secret = omk(
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
            "--sensitivity",
            "secret",
            "--idempotency-key",
            "inline-secret-key",
        ],
    );
    assert!(!inline_secret.status.success());
    let inline_error: Value = serde_json::from_slice(&inline_secret.stderr).unwrap();
    assert_eq!(inline_error["error"]["code"], "invalid_input");
    assert_eq!(inline_error["error"]["sameKeyReusable"], true);
    assert!(
        inline_error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("secret content must be read from stdin or --content-file")
    );
    assert!(!String::from_utf8_lossy(&inline_secret.stderr).contains("secret-response-marker"));

    let secret_content = directory.path().join("secret-content.txt");
    let secret_metadata = directory.path().join("secret-metadata.json");
    std::fs::write(&secret_content, "secret-response-marker").unwrap();
    std::fs::write(
        &secret_metadata,
        r#"{"credential":"secret-metadata-marker"}"#,
    )
    .unwrap();
    let secret_content = secret_content.to_str().unwrap();
    let secret_metadata = secret_metadata.to_str().unwrap();
    let inline_metadata = omk(
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
            "--content-file",
            secret_content,
            "--metadata",
            r#"{"credential":"secret-metadata-marker"}"#,
            "--sensitivity",
            "secret",
            "--idempotency-key",
            "inline-secret-metadata-key",
        ],
    );
    assert!(!inline_metadata.status.success());
    let metadata_error: Value = serde_json::from_slice(&inline_metadata.stderr).unwrap();
    assert_eq!(metadata_error["error"]["code"], "invalid_input");
    assert!(
        metadata_error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("secret metadata must be read from --metadata-file")
    );
    assert!(!String::from_utf8_lossy(&inline_metadata.stderr).contains("secret-metadata-marker"));

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
            "--content-file",
            secret_content,
            "--metadata-file",
            secret_metadata,
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
            "--content-file",
            secret_content,
            "--metadata-file",
            secret_metadata,
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

    let stdin_secret = omk_with_stdin(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:cli",
            "--stream",
            "stdin-secret-stream",
            "--kind",
            "tool-result",
            "--sensitivity",
            "secret",
            "--idempotency-key",
            "stdin-secret-key",
        ],
        b"stdin-secret-marker",
    );
    assert!(stdin_secret.status.success());
    let stdin_secret: Value = serde_json::from_slice(&stdin_secret.stdout).unwrap();
    assert_eq!(
        stdin_secret["data"]["content"],
        serde_json::json!({"redacted": true, "reason": "secret"})
    );
    assert!(
        !serde_json::to_string(&stdin_secret)
            .unwrap()
            .contains("stdin-secret-marker")
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

    let run = success_json(
        &db,
        &["observe", "get", "--scope", "user:cli", "--run", run_id],
    );
    assert_eq!(run["status"], "committed");
    assert_eq!(run["sourceIntegrity"], "intact");
    let status = success_json(
        &db,
        &[
            "observe", "status", "--scope", "user:cli", "--stream", "stream",
        ],
    );
    assert_eq!(status["observedThroughSequence"], 1);
}

#[test]
fn cli_prewrite_failures_preserve_idempotency_keys() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("memory.db");
    success_json(&db, &["init"]);
    success_json(
        &db,
        &[
            "scope",
            "add",
            "--id",
            "user:recovery",
            "--kind",
            "user",
            "--idempotency-key",
            "scope-key",
        ],
    );

    let missing_file = directory.path().join("missing-content.txt");
    let missing_file = missing_file.to_str().unwrap();
    let failed_file_read = omk(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:recovery",
            "--stream",
            "stream",
            "--kind",
            "user-message",
            "--content-file",
            missing_file,
            "--idempotency-key",
            "missing-file-key",
        ],
    );
    assert!(!failed_file_read.status.success());
    let error: Value = serde_json::from_slice(&failed_file_read.stderr).unwrap();
    assert_eq!(error["error"]["code"], "invalid_input");
    assert_eq!(error["error"]["sameKeyReusable"], true);
    let corrected_file_read = success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:recovery",
            "--stream",
            "stream",
            "--kind",
            "user-message",
            "--content",
            "corrected",
            "--idempotency-key",
            "missing-file-key",
        ],
    );
    assert_eq!(corrected_file_read["operation"]["replayed"], false);

    let missing_scope = omk(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "project:later",
            "--stream",
            "later-stream",
            "--kind",
            "user-message",
            "--content",
            "waiting for scope",
            "--idempotency-key",
            "missing-scope-key",
        ],
    );
    assert!(!missing_scope.status.success());
    let error: Value = serde_json::from_slice(&missing_scope.stderr).unwrap();
    assert_eq!(error["error"]["code"], "not_found");
    assert_eq!(error["error"]["sameKeyReusable"], true);
    assert!(error["error"]["nextAction"].as_str().is_some());

    success_json(
        &db,
        &[
            "scope",
            "add",
            "--id",
            "project:later",
            "--kind",
            "project",
            "--parent",
            "user:recovery",
            "--idempotency-key",
            "later-scope-key",
        ],
    );
    let corrected_scope = success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "project:later",
            "--stream",
            "later-stream",
            "--kind",
            "user-message",
            "--content",
            "waiting for scope",
            "--idempotency-key",
            "missing-scope-key",
        ],
    );
    assert_eq!(corrected_scope["operation"]["replayed"], false);

    let wrong_stream_scope = omk(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:recovery",
            "--stream",
            "later-stream",
            "--kind",
            "user-message",
            "--content",
            "wrong owner",
            "--idempotency-key",
            "stream-scope-key",
        ],
    );
    assert!(!wrong_stream_scope.status.success());
    let error: Value = serde_json::from_slice(&wrong_stream_scope.stderr).unwrap();
    assert_eq!(error["error"]["code"], "scope_violation");
    assert_eq!(error["error"]["sameKeyReusable"], true);
    assert!(error["error"]["nextAction"].as_str().is_some());

    let corrected_stream_scope = success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:recovery",
            "--stream",
            "user-stream",
            "--kind",
            "user-message",
            "--content",
            "correct owner",
            "--idempotency-key",
            "stream-scope-key",
        ],
    );
    assert_eq!(corrected_stream_scope["operation"]["replayed"], false);

    success_json(
        &db,
        &[
            "claim",
            "remember",
            "--scope",
            "user:recovery",
            "--kind",
            "fact",
            "--subject",
            "agent",
            "--predicate",
            "mode",
            "--value",
            "one",
            "--idempotency-key",
            "single-claim-key",
        ],
    );
    let cardinality_mismatch = omk(
        &db,
        &[
            "claim",
            "remember",
            "--scope",
            "user:recovery",
            "--kind",
            "fact",
            "--cardinality",
            "set",
            "--subject",
            "agent",
            "--predicate",
            "mode",
            "--value",
            "two",
            "--idempotency-key",
            "cardinality-key",
        ],
    );
    assert!(!cardinality_mismatch.status.success());
    let error: Value = serde_json::from_slice(&cardinality_mismatch.stderr).unwrap();
    assert_eq!(error["error"]["code"], "invalid_input");
    assert_eq!(error["error"]["sameKeyReusable"], true);

    let corrected_cardinality = success_json(
        &db,
        &[
            "claim",
            "remember",
            "--scope",
            "user:recovery",
            "--kind",
            "fact",
            "--subject",
            "agent",
            "--predicate",
            "mode",
            "--value",
            "two",
            "--idempotency-key",
            "cardinality-key",
        ],
    );
    assert_eq!(corrected_cardinality["operation"]["replayed"], false);
}

#[test]
fn cli_help_exposes_agent_critical_contracts() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("memory.db");

    let no_args = Command::new(env!("CARGO_BIN_EXE_omk")).output().unwrap();
    assert!(no_args.status.success());
    assert!(no_args.stderr.is_empty());
    let no_args = String::from_utf8(no_args.stdout).unwrap();
    assert!(no_args.contains("Usage: omk"));
    assert!(no_args.contains("Examples:"));
    assert!(no_args.contains("omk observe plan"));
    assert!(!no_args.contains("--compact"));

    let event_help = omk(&db, &["event", "--help"]);
    assert!(event_help.status.success());
    let event_help = String::from_utf8(event_help.stdout).unwrap();
    assert!(!event_help.contains("  range"));

    let context_help = omk(&db, &["context", "--help"]);
    assert!(context_help.status.success());
    assert!(
        !String::from_utf8(context_help.stdout)
            .unwrap()
            .contains("--format")
    );

    let view_help = omk(&db, &["view", "create", "--help"]);
    assert!(view_help.status.success());
    let view_help = String::from_utf8(view_help.stdout).unwrap();
    assert!(!view_help.contains("project-digest"));
    assert!(!view_help.contains("decision-rationale"));
    assert!(!view_help.contains("open-loops"));

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

    let commit_help = omk(&db, &["observe", "commit", "--help"]);
    assert!(commit_help.status.success());
    let commit_help = String::from_utf8(commit_help.stdout).unwrap();
    assert!(commit_help.contains("do not include runId"));
    assert!(commit_help.contains("\"sourceEventIds\""));
    assert!(commit_help.contains("\"eventTimeFrom\": null"));
    assert!(commit_help.contains("Allowed observation kinds:"));
    assert!(commit_help.contains("Allowed claim kinds:"));
    assert!(commit_help.contains("Allowed modalities:"));
    assert!(commit_help.contains("emptyReason"));

    let append_help = omk(&db, &["event", "append", "--help"]);
    assert!(append_help.status.success());
    let append_help = String::from_utf8(append_help.stdout).unwrap();
    assert!(append_help.contains("Storage/privacy mode"));
    assert!(append_help.contains("do-not-store"));
    assert!(!append_help.contains("private"));
    assert!(append_help.contains("--metadata-file"));
    assert!(append_help.contains("Secret content must come from stdin or --content-file"));

    let explain_help = omk(&db, &["recall", "explain-claim", "--help"]);
    assert!(explain_help.status.success());
    let explain_help = String::from_utf8(explain_help.stdout).unwrap();
    assert!(explain_help.contains("exact source events"));
    assert!(!explain_help.contains("source observations"));

    let empty_stdin = omk_with_stdin(
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
            "--idempotency-key",
            "empty-stdin-key",
        ],
        b"",
    );
    assert!(!empty_stdin.status.success());
    let empty_error: Value = serde_json::from_slice(&empty_stdin.stderr).unwrap();
    assert_eq!(empty_error["error"]["code"], "missing_input");
    assert_eq!(empty_error["error"]["sameKeyReusable"], true);
    assert_eq!(
        empty_error["error"]["nextAction"],
        "pipe input on stdin or pass the command's file option"
    );
}
