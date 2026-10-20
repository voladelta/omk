use super::*;

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
            ClaimCardinality::Single,
            json!("ETH"),
            &[],
            "remember",
        )
        .unwrap();
    let empty_payload = json!({
        "claims": [],
        "pendingClaims": [],
        "continuation": null,
        "reflectionViews": [],
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
fn context_deduplicates_observations_and_views_never_destroy_raw_evidence() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let event = fixture.event("user", "stream", "Use ETH", Sensitivity::Normal, "event-1");
    let plan = fixture
        .store
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan-1"))
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
            kind: ViewKind::Reflection,
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
                kind: ViewKind::Reflection,
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
            kind: ViewKind::Reflection,
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
            kind: ViewKind::Reflection,
            content: "large reflection text ".repeat(1000),
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
        assert!(context.reflection_views.is_empty());
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
        .plan_observation("user", "stream-b", 10_000, "fake", "v1", Some("plan-b"))
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan"))
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
            ClaimCardinality::Single,
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
    assert!(context.reflection_views.is_empty());
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
        .plan_observation("user", "stream", 10_000, "fake", "v1", Some("plan"))
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
            kind: ViewKind::Reflection,
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
                ClaimCardinality::Single,
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
                    ClaimCardinality::Single,
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
    // Room for ten claims: the pin share fits the newest five user claims and
    // the rest goes to the newest project claims. An overflowing pass ends at
    // its oldest landmark, and pin room it leaves goes to the project pass.
    let claim_budget = largest * 10 + largest / 2;
    let context = fixture
        .store
        .compose_context("project", "stream", claim_budget * 2, 0, None)
        .unwrap();
    let kept: Vec<String> = context
        .claims
        .iter()
        .map(|claim| ids[&claim.id].clone())
        .collect();
    let subject_ids: std::collections::HashMap<&str, &str> = ids
        .iter()
        .map(|(id, subject)| (subject.as_str(), id.as_str()))
        .collect();
    for (prefix, most) in [("u", 5), ("p", 10)] {
        // Each scope keeps a newest run, from index 9 down.
        let run: Vec<usize> = (0..10)
            .rev()
            .take_while(|i| kept.contains(&format!("{prefix}{i}")))
            .collect();
        let count = kept.iter().filter(|s| s.starts_with(prefix)).count();
        assert_eq!(run.len(), count, "{prefix} claims are not a newest run");
        assert!((1..=most).contains(&count));
        if count < 10 {
            // An overflowing run ends at a landmark, or holds none.
            let landmark =
                |i: &usize| is_claim_landmark(subject_ids[format!("{prefix}{i}").as_str()]);
            assert!(landmark(run.last().unwrap()) || !run.iter().any(landmark));
        }
    }
    let mut kept = kept;
    kept.sort_unstable();
    let mut omitted: Vec<String> = context
        .diagnostics
        .omitted_items
        .iter()
        .filter(|item| item.reason == "active claim budget")
        .map(|item| ids[&item.id].clone())
        .collect();
    omitted.extend(kept);
    omitted.sort_unstable();
    let mut every: Vec<String> = ids.values().cloned().collect();
    every.sort_unstable();
    assert_eq!(omitted, every);

    let plan = fixture
        .store
        .plan_observation(
            "project",
            "stream",
            claim_budget * 2,
            "test",
            "v1",
            Some("plan"),
        )
        .unwrap()
        .data
        .into_plan()
        .unwrap();
    assert_eq!(plan.active_claims.len(), context.claims.len());
}

#[test]
fn claim_budget_cutoff_holds_across_additions() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    fixture.scope("project", ScopeKind::Project, Some("user"));
    fixture.event("project", "stream", "start", Sensitivity::Normal, "seed");
    let add = |fixture: &mut Fixture, index: usize| {
        fixture
            .store
            .remember_claim(
                "project",
                ClaimKind::Fact,
                &format!("p{index:03}"),
                "value",
                ClaimCardinality::Single,
                json!("v".repeat(400)),
                &[],
                &format!("remember-{index}"),
            )
            .unwrap()
            .data
    };
    let first = add(&mut fixture, 0);
    let cost = serde_json::to_string(&first).unwrap().chars().count() as i64 / 4 + 2;
    for index in 1..40 {
        add(&mut fixture, index);
    }
    // Room for about 24 claims; every addition pushes the window forward.
    let budget = cost * 24 * 2;
    let oldest_kept = |fixture: &Fixture| {
        let context = fixture
            .store
            .compose_context("project", "stream", budget, 0, None)
            .unwrap();
        assert!(!context.diagnostics.omitted_items.is_empty());
        context.claims.first().unwrap().id.clone()
    };
    let mut cutoff = oldest_kept(&fixture);
    let mut moves = 0;
    for index in 40..80 {
        let newest = add(&mut fixture, index);
        let next = oldest_kept(&fixture);
        assert!(
            fixture
                .store
                .compose_context("project", "stream", budget, 0, None)
                .unwrap()
                .claims
                .iter()
                .any(|claim| claim.id == newest.id)
        );
        moves += usize::from(next != cutoff);
        cutoff = next;
    }
    // Without landmarks the cutoff moves on all 40 additions; with one claim
    // in four a landmark it moves about 10 times.
    assert!(moves <= 22, "cutoff moved {moves} times in 40 additions");
}

#[test]
fn raw_tail_start_holds_across_appends() {
    let mut fixture = Fixture::new();
    fixture.scope("user", ScopeKind::User, None);
    let content = "x".repeat(400);
    let append = |fixture: &mut Fixture, index: usize| {
        fixture.event(
            "user",
            "stream",
            &content,
            Sensitivity::Normal,
            &format!("e{index}"),
        )
    };
    append(&mut fixture, 0);
    let tokens = |fixture: &Fixture, raw: i64| {
        fixture
            .store
            .compose_context("user", "stream", 100_000, raw, None)
            .unwrap()
            .diagnostics
            .estimated_tokens
    };
    let cost = tokens(&fixture, 100_000) - tokens(&fixture, 0);
    for index in 1..40 {
        append(&mut fixture, index);
    }
    // Room for 12 events; every append pushes the window forward.
    let raw = cost * 12;
    let tail = |fixture: &Fixture| {
        fixture
            .store
            .compose_context("user", "stream", 100_000, raw, None)
            .unwrap()
            .recent_events
    };
    let mut start = tail(&fixture)[0].sequence;
    let mut moves = 0;
    for index in 40..80 {
        let newest = append(&mut fixture, index);
        let events = tail(&fixture);
        assert_eq!(events.last().unwrap().id, newest.id);
        assert!(events.len() >= 6, "kept {} of 12 events", events.len());
        moves += usize::from(events[0].sequence != start);
        start = events[0].sequence;
    }
    // A tail of the newest events that fit moves its start on all 40
    // appends; an aligned start moves about once every 6.
    assert!(moves <= 10, "tail start moved {moves} times in 40 appends");
}
