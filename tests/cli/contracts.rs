use super::*;

#[test]
fn context_query_modes_recall_separated_terms_in_both_renderings() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("memory.db");
    success_json(&db, &["init"]);
    success_json(
        &db,
        &[
            "scope",
            "add",
            "--id",
            "user:query",
            "--kind",
            "user",
            "--idempotency-key",
            "scope",
        ],
    );
    let event = success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:query",
            "--stream",
            "old",
            "--kind",
            "tool-result",
            "--content",
            "rollback threshold CLOCK_SKEW_17",
            "--idempotency-key",
            "event",
        ],
    );
    let id = event["data"]["id"].as_str().unwrap();
    let secret = omk_with_stdin(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:query",
            "--stream",
            "old",
            "--kind",
            "tool-result",
            "--sensitivity",
            "secret",
            "--idempotency-key",
            "secret",
        ],
        b"rollback secret CLOCK_SKEW_17",
    );
    assert!(
        secret.status.success(),
        "{}",
        String::from_utf8_lossy(&secret.stderr)
    );
    success_json(
        &db,
        &[
            "scope",
            "add",
            "--id",
            "user:other",
            "--kind",
            "user",
            "--idempotency-key",
            "other-scope",
        ],
    );
    success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:other",
            "--stream",
            "other",
            "--kind",
            "tool-result",
            "--content",
            "rollback other CLOCK_SKEW_17",
            "--idempotency-key",
            "other-event",
        ],
    );
    for compact in [false, true] {
        let mut args = vec![
            "context",
            "--scope",
            "user:query",
            "--stream",
            "old",
            "--recent-raw-tokens",
            "0",
            "--query",
            "rollback CLOCK_SKEW_17",
        ];
        if compact {
            args.push("--compact");
        }
        let phrase = success_json(&db, &args);
        assert_eq!(phrase["recalledEvidence"].as_array().unwrap().len(), 0);
        args.push("--terms");
        let terms = success_json(&db, &args);
        assert_eq!(terms["recalledEvidence"].as_array().unwrap().len(), 1);
        assert_eq!(terms["recalledEvidence"][0]["id"], id);
        args.pop();
        args.push("--fts-query");
        let advanced = success_json(&db, &args);
        assert_eq!(advanced["recalledEvidence"].as_array().unwrap().len(), 1);
        assert_eq!(advanced["recalledEvidence"][0]["id"], id);
    }
    for flags in [
        vec!["--terms"],
        vec!["--fts-query"],
        vec!["--terms", "--fts-query", "--query", "rollback"],
    ] {
        let mut args = vec!["context", "--scope", "user:query", "--stream", "old"];
        args.extend(flags);
        let output = omk(&db, &args);
        assert_eq!(output.status.code(), Some(2));
    }
    let invalid = omk(
        &db,
        &[
            "context",
            "--scope",
            "user:query",
            "--stream",
            "old",
            "--query",
            "\"",
            "--fts-query",
        ],
    );
    assert!(!invalid.status.success());
    let error: Value = serde_json::from_slice(&invalid.stderr).unwrap();
    let recall_invalid = omk(
        &db,
        &[
            "recall",
            "search",
            "--scope",
            "user:query",
            "--query",
            "\"",
            "--fts-query",
        ],
    );
    assert!(!recall_invalid.status.success());
    let recall_error: Value = serde_json::from_slice(&recall_invalid.stderr).unwrap();
    assert_eq!(error["error"]["code"], recall_error["error"]["code"]);
}

#[test]
fn compact_context_cli_emits_recallable_model_input_and_keeps_default_output() {
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
            "scope",
        ],
    );
    let event = success_json(
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
            "Keep the release decision",
            "--idempotency-key",
            "event",
        ],
    );
    let event_id = event["data"]["id"].as_str().unwrap();

    let default = success_json(
        &db,
        &["context", "--scope", "user:cli", "--stream", "stream"],
    );
    let compact = success_json(
        &db,
        &[
            "context",
            "--scope",
            "user:cli",
            "--stream",
            "stream",
            "--compact",
        ],
    );

    assert!(default["diagnostics"]["estimatedTokens"].is_number());
    assert_eq!(default["recentEvents"][0]["id"], event_id);
    assert!(default["recentEvents"][0]["contentHash"].is_string());
    assert!(compact.get("diagnostics").is_none());
    assert_eq!(compact["recentEvents"][0]["id"], event_id);
    assert_eq!(
        compact["recentEvents"][0]["content"],
        "Keep the release decision"
    );
    assert!(compact["recentEvents"][0].get("contentHash").is_none());

    let recalled = success_json(
        &db,
        &["event", "get", "--scope", "user:cli", "--id", event_id],
    );
    assert_eq!(recalled["id"], event_id);
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
    assert_eq!(hits["hits"].as_array().unwrap().len(), 1);
    assert_eq!(hits["matched"], 1);

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
