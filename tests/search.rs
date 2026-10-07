use omk::*;
use serde_json::{Value, json};
use std::process::Command;

fn setup(path: &std::path::Path) -> MemoryStore {
    let mut store = MemoryStore::open(path).unwrap();
    store
        .create_scope("user", ScopeKind::User, None, None, "user")
        .unwrap();
    store
        .create_scope("thread", ScopeKind::Thread, Some("user"), None, "thread")
        .unwrap();
    store
        .create_scope("sibling", ScopeKind::User, None, None, "sibling")
        .unwrap();
    store
}

fn event(store: &mut MemoryStore, scope: &str, text: &str, key: &str) -> MemoryEvent {
    store
        .append_event(NewEvent {
            scope_id: scope.into(),
            stream_id: format!("stream-{scope}"),
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

fn fact(store: &mut MemoryStore, subject: &str, predicate: &str, value: &str, key: &str) -> Claim {
    store
        .remember_claim(
            "user",
            ClaimKind::Fact,
            subject,
            predicate,
            json!(value),
            &[],
            key,
        )
        .unwrap()
        .data
}

fn alias(store: &mut MemoryStore, subject: &str, name: &str, key: &str) -> Claim {
    store
        .remember_claim_with_cardinality(
            "user",
            ClaimKind::EntityAlias,
            subject,
            "alias",
            ClaimCardinality::Set,
            json!(name),
            &[],
            key,
        )
        .unwrap()
        .data
}

fn kind(error: anyhow::Error) -> KernelErrorKind {
    error.downcast_ref::<KernelError>().unwrap().kind()
}

#[test]
fn search_pages_count_matches_and_tell_empty_scopes_from_misses() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = setup(&dir.path().join("memory.db"));
    for index in 0..5 {
        event(
            &mut store,
            "thread",
            &format!("orchid note {index}"),
            &format!("e{index}"),
        );
    }
    let page = store
        .search_page("user", "orchid", 2, SearchOptions::default())
        .unwrap();
    assert_eq!((page.shown, page.matched, page.searchable), (2, 5, 5));
    assert!(page.next_action.unwrap().contains("3 more matches"));

    let miss = store
        .search_page("user", "tulip", 10, SearchOptions::default())
        .unwrap();
    assert_eq!((miss.shown, miss.matched, miss.searchable), (0, 0, 5));
    assert!(miss.next_action.unwrap().contains("no match among 5"));

    // A sibling scope has nothing visible, which is not the same as no match.
    let empty = store
        .search_page("sibling", "orchid", 10, SearchOptions::default())
        .unwrap();
    assert_eq!((empty.matched, empty.searchable), (0, 0));
    assert!(empty.next_action.unwrap().contains("no searchable records"));

    let exact = store
        .search_page("user", "orchid", 5, SearchOptions::default())
        .unwrap();
    assert_eq!(exact.next_action, None);
}

#[test]
fn command_echoes_types_and_fields_filter_inside_the_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = setup(&dir.path().join("memory.db"));
    let email = fact(&mut store, "Alice Moreau", "email", "alice@acme.io", "c1");
    let nickname = alias(&mut store, "Alice Moreau", "Ali", "c2");
    let mention = event(&mut store, "thread", "lunch with alice", "e1");

    let default = store
        .search_page("user", "alice", 20, SearchOptions::default())
        .unwrap();
    let ids: Vec<&str> = default.hits.iter().map(|hit| hit.id.as_str()).collect();
    assert_eq!(default.matched, 3, "command events are excluded: {ids:?}");
    assert!(ids.contains(&mention.id.as_str()));
    let with_commands = store
        .search_page(
            "user",
            "alice",
            20,
            SearchOptions {
                include_commands: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(with_commands.matched, 5);

    let claims_only = SearchOptions {
        types: SearchTypes {
            claims: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let claims = store.search_page("user", "alice", 20, claims_only).unwrap();
    assert_eq!(claims.matched, 2);
    assert!(claims.hits.iter().all(|hit| hit.record_type == "claim"));
    assert_eq!(claims.hits[0].subject.as_deref(), Some("Alice Moreau"));
    assert_eq!(claims.searchable, 2);

    // A value search skips the subject, so only the email value matches.
    let values = store
        .search_page(
            "user",
            "alice",
            20,
            SearchOptions {
                field: SearchField::Value,
                ..claims_only
            },
        )
        .unwrap();
    assert_eq!(
        values.hits.iter().map(|hit| &hit.id).collect::<Vec<_>>(),
        vec![&email.id]
    );
    let alias_hit = store
        .search_page(
            "user",
            "Ali",
            20,
            SearchOptions {
                field: SearchField::Value,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(alias_hit.hits[0].id, nickname.id);
}

#[test]
fn current_claims_outrank_replaced_claims_and_events() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = setup(&dir.path().join("memory.db"));
    let old = fact(&mut store, "Atlas", "launch", "October", "c1");
    let new = store
        .correct_claim(&old.id, json!("November"), &[], "c2")
        .unwrap()
        .data;
    event(&mut store, "thread", "Atlas launch slips", "e1");
    let options = SearchOptions {
        mode: SearchMode::Terms,
        ..Default::default()
    };
    let hits = store
        .search_with_options("user", "Atlas launch", 10, options)
        .unwrap();
    assert_eq!(hits[0].id, new.id);
    assert_eq!(hits.last().unwrap().id, old.id);
    assert!(hits.windows(2).all(|pair| pair[0].rank <= pair[1].rank));
}

#[test]
fn search_reaches_every_scope_past_the_facet_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = setup(&dir.path().join("memory.db"));
    for index in 0..70 {
        let scope = format!("child-{index}");
        store
            .create_scope(&scope, ScopeKind::Task, Some("thread"), None, &scope)
            .unwrap();
        event(&mut store, &scope, "quartz sample", &format!("e{index}"));
    }
    event(&mut store, "sibling", "quartz sample", "outside");
    let page = store
        .search_page("user", "quartz", 1000, SearchOptions::default())
        .unwrap();
    assert_eq!((page.shown, page.matched, page.searchable), (70, 70, 70));
    let one = store
        .search_page("child-3", "quartz", 10, SearchOptions::default())
        .unwrap();
    assert_eq!(one.matched, 1);
}

#[test]
fn bad_fts_syntax_is_an_invalid_search_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = setup(&dir.path().join("memory.db"));
    for query in ["\"", "a)", "facet : x OR ("] {
        let error = store
            .search_page(
                "user",
                query,
                10,
                SearchOptions {
                    mode: SearchMode::Advanced,
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(kind(error), KernelErrorKind::InvalidSearchQuery, "{query}");
    }
}

#[test]
fn resolve_walks_exact_name_contains_and_fuzzy_tiers() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = setup(&dir.path().join("memory.db"));
    fact(&mut store, "Alice Moreau", "role", "engineer", "c1");
    fact(&mut store, "Alice Chen", "role", "designer", "c2");
    let handle = alias(&mut store, "Alice Moreau", "amoreau", "c3");
    alias(&mut store, "Alice Chen", "unknown", "c4");

    let exact = store.resolve_name("thread", "Alice Moreau").unwrap();
    assert_eq!(
        (exact.status, exact.tier),
        (ResolveStatus::Resolved, Some(ResolveTier::Exact))
    );
    let via_alias = store.resolve_name("thread", "amoreau").unwrap();
    assert_eq!(via_alias.candidates[0].subject, "Alice Moreau");
    assert_eq!(via_alias.candidates[0].matched[0].via, "alias");
    assert_eq!(
        via_alias.candidates[0].matched[0].claim_id.as_deref(),
        Some(handle.id.as_str())
    );
    let name = store.resolve_name("thread", "ALICE  moreau.").unwrap();
    assert_eq!(
        (name.status, name.tier),
        (ResolveStatus::Resolved, Some(ResolveTier::Name))
    );
    let first = store.resolve_name("thread", "Alice").unwrap();
    assert_eq!(
        (first.status, first.tier, first.candidates.len()),
        (ResolveStatus::Ambiguous, Some(ResolveTier::Contains), 2)
    );
    let titled = store.resolve_name("thread", "Dr. Alice Chen").unwrap();
    assert_eq!(
        (titled.status, titled.candidates[0].subject.as_str()),
        (ResolveStatus::Probable, "Alice Chen")
    );
    let typo = store.resolve_name("thread", "Alice Moraeu").unwrap();
    assert_eq!(
        (typo.status, typo.tier, typo.candidates[0].distance),
        (ResolveStatus::Probable, Some(ResolveTier::Fuzzy), Some(1))
    );
    let new = store.resolve_name("thread", "Zed Quinlan").unwrap();
    assert_eq!(new.status, ResolveStatus::None);
    assert_eq!((new.considered_subjects, new.considered_aliases), (2, 2));
    assert_eq!(
        kind(store.resolve_name("thread", " N/A ").unwrap_err()),
        KernelErrorKind::InvalidInput
    );
    // The placeholder alias never matches.
    assert_eq!(
        store.resolve_name("thread", "unknowns").unwrap().status,
        ResolveStatus::None
    );
    let elsewhere = store.resolve_name("sibling", "Alice Moreau").unwrap();
    assert_eq!(elsewhere.status, ResolveStatus::None);
    assert!(elsewhere.next_action.contains("no active claims"));

    store.forget_claim(&handle.id, "forget-handle").unwrap();
    let forgotten = store.resolve_name("thread", "amoreau").unwrap();
    assert_eq!(forgotten.status, ResolveStatus::Probable);
    assert_eq!(forgotten.tier, Some(ResolveTier::Fuzzy));
}

#[test]
fn cli_search_page_flags_and_resolve() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let mut store = setup(&path);
    fact(&mut store, "Alice Moreau", "email", "alice@acme.io", "c1");
    event(&mut store, "thread", "alice called", "e1");
    drop(store);
    let run = |args: &[&str]| -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_omk"))
            .arg("--db")
            .arg(&path)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    };
    let page = run(&[
        "recall",
        "search",
        "--scope",
        "user",
        "--query",
        "alice",
        "--type",
        "claim",
        "--type",
        "observation",
        "--field",
        "value",
    ]);
    assert_eq!(page["matched"], 1);
    assert_eq!(page["hits"][0]["predicate"], "email");
    let echoes = run(&[
        "recall",
        "search",
        "--scope",
        "user",
        "--query",
        "alice",
        "--include-commands",
    ]);
    assert_eq!(echoes["matched"], 3);
    let resolved = run(&[
        "recall",
        "resolve",
        "--scope",
        "thread",
        "--name",
        "alice moreau",
    ]);
    assert_eq!(resolved["status"], "resolved");
    assert_eq!(resolved["tier"], "name");
    assert_eq!(resolved["candidates"][0]["subject"], "Alice Moreau");
}
