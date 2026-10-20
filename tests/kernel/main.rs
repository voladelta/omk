use omk::store::CreateView;
use omk::*;
use rusqlite::Connection;
use serde_json::{Value, json};
use tempfile::TempDir;

mod claims;
mod context;
mod observation;
mod privacy;
mod purge;
mod schema;
mod scopes;

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
        .plan_observation(scope, stream, 100_000, "fake", "v1", Some(key))
        .unwrap()
        .data
        .into_plan()
        .unwrap()
}

fn raw_count(fixture: &Fixture, sql: &str, value: &str) -> i64 {
    Connection::open(fixture._directory.path().join("memory.db"))
        .unwrap()
        .query_row(sql, [value], |row| row.get(0))
        .unwrap()
}

/// The claim landmark rule from the store: FNV-1a of the id, one in four.
fn is_claim_landmark(id: &str) -> bool {
    let hash = id.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    hash % 4 == 0
}
