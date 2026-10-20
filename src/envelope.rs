use omk::{KernelError, KernelErrorKind, SCHEMA_VERSION};
use serde_json::json;

/// SQLite reports SQLITE_BUSY or SQLITE_LOCKED when another connection holds
/// the database past the busy timeout. Nothing was written, so a retry is safe.
fn is_sqlite_contention(cause: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        cause.downcast_ref::<rusqlite::Error>(),
        Some(rusqlite::Error::SqliteFailure(failure, _))
            if matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

/// Recovery hint for a schema mismatch, naming the schema this build opens.
fn schema_mismatch_action() -> &'static str {
    static ACTION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ACTION.get_or_init(|| {
        format!(
            "restore a valid schema v{SCHEMA_VERSION} database from backup; do not delete existing data"
        )
    })
}

pub(crate) fn classify_error(
    error: &anyhow::Error,
    key_reusable_for_command: bool,
) -> (&'static str, bool, bool, Option<&'static str>) {
    if error.chain().any(is_sqlite_contention) {
        return (
            "busy",
            true,
            true,
            Some("retry the identical request with the same key"),
        );
    }
    let Some(error) = error.downcast_ref::<KernelError>() else {
        return ("kernel_error", false, false, None);
    };
    match error.kind() {
        KernelErrorKind::ClaimConflict => (
            "claim_conflict",
            false,
            true,
            Some("resolve the destination claim explicitly, then retry rescope"),
        ),
        KernelErrorKind::SchemaMismatch => (
            "schema_mismatch",
            false,
            true,
            Some(schema_mismatch_action()),
        ),
        KernelErrorKind::IdempotencyConflict => (
            "idempotency_conflict",
            false,
            false,
            Some("use a new key or retry the identical request"),
        ),
        KernelErrorKind::BudgetExceeded => (
            "budget_exceeded",
            false,
            key_reusable_for_command,
            Some(if key_reusable_for_command {
                "increase the token budget and retry with the same key"
            } else {
                "increase the token budget and retry"
            }),
        ),
        KernelErrorKind::StaleView => (
            "stale_view",
            false,
            key_reusable_for_command,
            Some(
                "read the latest reflection view, rerun reflection from it, and retry with the same key",
            ),
        ),
        KernelErrorKind::StaleObservationRun => (
            "stale_observation_run",
            false,
            false,
            Some("request a new observation plan"),
        ),
        KernelErrorKind::PrivacyPurged => (
            "privacy_purged",
            false,
            false,
            Some("use a new idempotency key without restoring purged data"),
        ),
        KernelErrorKind::OperationExpired => (
            "operation_expired",
            false,
            false,
            Some("the operation already committed; inspect the target records instead of retrying"),
        ),
        KernelErrorKind::NotFound => (
            "not_found",
            false,
            key_reusable_for_command,
            Some(if key_reusable_for_command {
                "create or target an existing resource and retry with the same key"
            } else {
                "inspect the identifier and scope"
            }),
        ),
        KernelErrorKind::ScopeViolation => (
            "scope_violation",
            false,
            key_reusable_for_command,
            Some("inspect the scope tree and target scope"),
        ),
        KernelErrorKind::InvalidSearchQuery => (
            "invalid_search_query",
            false,
            key_reusable_for_command,
            Some("use literal search or correct --fts-query syntax"),
        ),
        KernelErrorKind::MissingInput => (
            "missing_input",
            false,
            key_reusable_for_command,
            Some("pipe input on stdin or pass the command's file option"),
        ),
        KernelErrorKind::InvalidInput => (
            "invalid_input",
            false,
            key_reusable_for_command,
            Some(if key_reusable_for_command {
                "correct the input and retry with the same key"
            } else {
                "correct the input and retry"
            }),
        ),
    }
}

pub(crate) fn print_error(
    code: &str,
    message: &str,
    retryable: bool,
    same_key_reusable: bool,
    next_action: Option<&str>,
) {
    let error = json!({
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable,
            "sameKeyReusable": same_key_reusable,
            "nextAction": next_action
        }
    });
    eprintln!(
        "{}",
        serde_json::to_string(&error).expect("serializing an error envelope cannot fail")
    );
}

#[cfg(test)]
mod tests {
    use super::{KernelError, KernelErrorKind, classify_error};

    fn classified(
        kind: KernelErrorKind,
        message: &str,
        key_reusable: bool,
    ) -> (&'static str, bool, bool, Option<&'static str>) {
        classify_error(
            &anyhow::Error::new(KernelError::new(kind, message)),
            key_reusable,
        )
    }

    #[test]
    fn stale_view_errors_have_view_specific_recovery() {
        let classified = classified(
            KernelErrorKind::StaleView,
            "view is stale: expected previous view None, found Some(\"view-id\")",
            true,
        );
        assert_eq!(classified.0, "stale_view");
        assert!(!classified.1);
        assert!(classified.2);
        assert!(
            classified
                .3
                .is_some_and(|action| action.contains("reflection view"))
        );
    }

    #[test]
    fn not_found_recovery_only_reuses_keys_for_idempotent_mutations() {
        let mutation = classified(
            KernelErrorKind::NotFound,
            "scope project:later does not exist",
            true,
        );
        assert_eq!(mutation.0, "not_found");
        assert!(mutation.2);
        assert!(mutation.3.is_some_and(|action| action.contains("same key")));

        let read = classified(
            KernelErrorKind::NotFound,
            "event event-id does not exist",
            false,
        );
        assert_eq!(read.0, "not_found");
        assert!(!read.2);
        assert!(read.3.is_some_and(|action| !action.contains("key")));
    }

    #[test]
    fn input_file_errors_are_correctable_with_the_same_mutation_key() {
        let classified = classified(
            KernelErrorKind::InvalidInput,
            "reading input file /tmp/missing: No such file or directory",
            true,
        );
        assert_eq!(classified.0, "invalid_input");
        assert!(!classified.1);
        assert!(classified.2);
    }

    fn sqlite_failure(code: i32) -> anyhow::Error {
        anyhow::Error::new(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        ))
        .context("starting SQLite write transaction")
    }

    #[test]
    fn sqlite_contention_is_a_retryable_busy_error_for_the_same_key() {
        for code in [rusqlite::ffi::SQLITE_BUSY, rusqlite::ffi::SQLITE_LOCKED] {
            let classified = classify_error(&sqlite_failure(code), false);
            assert_eq!(classified.0, "busy");
            assert!(classified.1);
            assert!(classified.2);
            assert_eq!(
                classified.3,
                Some("retry the identical request with the same key")
            );
        }
        let other = classify_error(&sqlite_failure(rusqlite::ffi::SQLITE_CORRUPT), true);
        assert_eq!(other.0, "kernel_error");
        assert!(!other.1);
    }

    #[test]
    fn schema_mismatch_recovery_names_the_current_schema() {
        let classified = classified(KernelErrorKind::SchemaMismatch, "schema mismatch", false);
        assert_eq!(classified.0, "schema_mismatch");
        let schema = format!("schema v{} database", omk::SCHEMA_VERSION);
        assert!(classified.3.is_some_and(|action| action.contains(&schema)));
    }

    #[test]
    fn expired_operations_are_final_and_point_at_the_records() {
        let classified = classified(KernelErrorKind::OperationExpired, "expired", true);
        assert_eq!(classified.0, "operation_expired");
        assert!(!classified.1);
        assert!(!classified.2);
        assert!(
            classified
                .3
                .is_some_and(|action| action.contains("inspect"))
        );
    }
}
