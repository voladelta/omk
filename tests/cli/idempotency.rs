use super::*;

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
fn cli_writes_report_busy_while_another_process_holds_the_write_lock() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("memory.db");
    success_json(&db, &["init"]);
    success_json(
        &db,
        &[
            "scope",
            "add",
            "--id",
            "user:busy",
            "--kind",
            "user",
            "--idempotency-key",
            "scope",
        ],
    );
    let append = [
        "event",
        "append",
        "--scope",
        "user:busy",
        "--stream",
        "busy-stream",
        "--kind",
        "user-message",
        "--content",
        "written after contention",
        "--idempotency-key",
        "busy-append",
    ];
    success_json(
        &db,
        &[
            "event",
            "append",
            "--scope",
            "user:busy",
            "--stream",
            "busy-stream",
            "--kind",
            "user-message",
            "--content",
            "written before contention",
            "--idempotency-key",
            "busy-seed",
        ],
    );

    let holder = rusqlite::Connection::open(&db).unwrap();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();

    // WAL readers proceed while another connection holds the write lock.
    let context = success_json(
        &db,
        &[
            "context",
            "--scope",
            "user:busy",
            "--stream",
            "busy-stream",
            "--max-tokens",
            "4000",
            "--recent-raw-tokens",
            "1000",
        ],
    );
    assert_eq!(context["recentEvents"].as_array().unwrap().len(), 1);

    let started = std::time::Instant::now();
    let output = omk(&db, &append);
    let waited = started.elapsed();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "busy");
    assert_eq!(error["error"]["retryable"], true);
    assert_eq!(error["error"]["sameKeyReusable"], true);
    assert_eq!(
        error["error"]["nextAction"],
        "retry the identical request with the same key"
    );
    assert!(
        waited >= std::time::Duration::from_secs(4),
        "busy returned before the busy timeout: {waited:?}"
    );

    holder.execute_batch("ROLLBACK").unwrap();
    drop(holder);

    let retried = success_json(&db, &append);
    assert_eq!(retried["operation"]["replayed"], false);
    assert_eq!(retried["data"]["sequence"], 2);
    let replayed = success_json(&db, &append);
    assert_eq!(replayed["operation"]["replayed"], true);
    assert_eq!(replayed["data"]["id"], retried["data"]["id"]);
}
