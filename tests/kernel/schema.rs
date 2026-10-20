use super::*;

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
