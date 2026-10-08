//! Simulated agent use against the real store, for sizing budget and
//! retention policies.
//!
//! `cargo run --release --example sim -- OUT_DIR [DAYS=360] [HYGIENE=0.5]`
//!
//! Each day creates a thread per active project (project a daily, b every
//! second day, c every third), appends 30 events, runs one observer cycle,
//! confirms, rejects or leaves its claims pending, records project facts,
//! corrections, aliases and open loops, and sometimes a user preference.
//! HYGIENE is the share of open loops the agent later forgets. Writes
//! month.jsonl (growth and latency every 30 days), snap.jsonl (the active
//! claims context returns for project a each day) and ops.jsonl (operation
//! log rows). cache.jsonl records, for consecutive compact contexts in one
//! thread at CACHE_BUDGET tokens (default 16,000) with CACHE_RAW recent raw
//! tokens (default 2,000), how much of the serialized payload the second call
//! shares with the first: the part a prompt cache could reuse. It probes after
//! every appended event and after each observer commit, review and fact step,
//! and names the payload section where the shared prefix ends.
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

use omk::*;
use serde_json::{Value, json};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn f(&mut self) -> f64 {
        (self.next() % 1_000_000) as f64 / 1_000_000.0
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
    fn chance(&mut self, p: f64) -> bool {
        self.f() < p
    }
}

const WORDS: &[&str] = &[
    "deploy",
    "rollback",
    "schema",
    "latency",
    "budget",
    "settlement",
    "ledger",
    "token",
    "review",
    "customer",
    "invoice",
    "cluster",
    "migration",
    "retry",
    "cache",
    "index",
    "parser",
    "release",
    "staging",
    "contract",
    "vendor",
    "alert",
    "queue",
    "worker",
    "snapshot",
    "replica",
    "timeout",
    "quota",
    "feature",
    "flag",
    "incident",
    "postmortem",
];

fn sentence(rng: &mut Rng, lo: u64, hi: u64) -> String {
    (0..rng.range(lo, hi))
        .map(|_| WORDS[rng.range(0, WORDS.len() as u64) as usize])
        .collect::<Vec<_>>()
        .join(" ")
}

struct Sim {
    s: MemoryStore,
    rng: Rng,
    day: i64,
    seq: u64,
    // claim id -> (created day, updated day)
    claim_day: HashMap<String, (i64, i64)>,
    // slot subject -> current active claim id, per scope
    singles: HashMap<String, Vec<(String, String)>>,
    loops: Vec<(String, Option<i64>)>,
    conflicts: u64,
    // An early append, replayed at the end to check compacted keys.
    probe: Option<NewEvent>,
    cache: BufWriter<File>,
    cache_budget: i64,
    cache_raw: i64,
    // thread, serialized payload and claim ids of the last cache probe
    last_context: Option<(String, String, Vec<String>)>,
}

impl Sim {
    fn key(&mut self, what: &str) -> String {
        self.seq += 1;
        format!("d{}:{}:{}", self.day, what, self.seq)
    }

    fn event(&mut self, scope: &str, stream: &str, secret: bool) -> MemoryEvent {
        let key = self.key("event");
        let content = sentence(&mut self.rng, 40, 120);
        let request = NewEvent {
            scope_id: scope.into(),
            stream_id: stream.into(),
            kind: EventKind::UserMessage,
            actor_id: None,
            occurred_at: None,
            content: Value::String(content),
            token_count: None,
            sensitivity: if secret {
                Sensitivity::Secret
            } else {
                Sensitivity::Normal
            },
            metadata: json!({}),
            idempotency_key: key,
        };
        if self.day == 5 && self.probe.is_none() {
            self.probe = Some(request.clone());
        }
        self.s.append_event(request).unwrap().data
    }

    fn track(&mut self, id: &str) {
        let day = self.day;
        self.claim_day
            .entry(id.to_owned())
            .and_modify(|entry| entry.1 = day)
            .or_insert((day, day));
    }

    fn remember(
        &mut self,
        scope: &str,
        kind: ClaimKind,
        subject: &str,
        card: ClaimCardinality,
    ) -> Claim {
        let key = self.key("remember");
        let value = json!(sentence(&mut self.rng, 4, 14));
        let claim = self
            .s
            .remember_claim_with_cardinality(scope, kind, subject, "value", card, value, &[], &key)
            .unwrap()
            .data;
        self.track(&claim.id);
        claim
    }

    fn single_fact(&mut self, scope: &str, kind: ClaimKind, new_p: f64, prefix: &str) {
        let existing = self.singles.get(scope).map_or(0, Vec::len);
        if existing == 0 || self.rng.chance(new_p) {
            let subject = format!("{prefix}:{existing}");
            let claim = self.remember(scope, kind, &subject, ClaimCardinality::Single);
            self.singles
                .entry(scope.into())
                .or_default()
                .push((subject, claim.id));
        } else {
            let index = self.rng.range(0, existing as u64) as usize;
            let old = self.singles[scope][index].1.clone();
            let key = self.key("correct");
            let value = json!(sentence(&mut self.rng, 4, 14));
            let claim = self.s.correct_claim(&old, value, &[], &key).unwrap().data;
            self.track(&claim.id);
            self.singles.get_mut(scope).unwrap()[index].1 = claim.id;
        }
    }

    fn cache_probe(&mut self, thread: &str, stream: &str, step: &str) {
        let bundle = self
            .s
            .compose_compact_context(thread, stream, self.cache_budget, self.cache_raw, None)
            .unwrap();
        let payload = bundle.compact_model_payload();
        let claims_len = payload["claims"].to_string().len();
        let text = payload.to_string();
        let ids: Vec<String> = bundle.claims.iter().map(|c| c.id.clone()).collect();
        let omitted = bundle
            .diagnostics
            .omitted_items
            .iter()
            .filter(|item| item.reason == "active claim budget")
            .count();
        if let Some((last_thread, last_text, last_ids)) = &self.last_context
            && last_thread == thread
        {
            let shared = last_text
                .bytes()
                .zip(text.bytes())
                .take_while(|(a, b)| a == b)
                .count();
            let first_diff = last_ids
                .iter()
                .zip(&ids)
                .take_while(|(a, b)| a == b)
                .count();
            // The top-level section holding the first differing byte.
            let section = [
                "claims",
                "pendingClaims",
                "continuation",
                "continuityViews",
                "observations",
                "recentEvents",
                "recalledEvidence",
            ]
            .into_iter()
            .filter_map(|key| text.find(&format!("\"{key}\":")).map(|at| (at, key)))
            .filter(|(at, _)| *at <= shared)
            .max()
            .map_or("none", |(_, key)| key);
            let removed = last_ids.iter().filter(|id| !ids.contains(id)).count();
            let added = ids.iter().filter(|id| !last_ids.contains(id)).count();
            writeln!(
                self.cache,
                "{}",
                json!({"day": self.day, "step": step, "prev": last_text.len(), "total": text.len(),
                       "shared": shared, "claimsLen": claims_len, "claims": ids.len(),
                       "firstDiff": first_diff, "removed": removed, "added": added,
                       "omitted": omitted, "section": section,
                       "events": bundle.recent_events.len()})
            )
            .unwrap();
        }
        self.last_context = Some((thread.to_owned(), text, ids));
    }

    fn thread_day(&mut self, project: &str, hygiene: f64) -> (String, String, f64) {
        let thread = format!("thread:{project}:{}", self.day);
        let stream = format!("s:{project}:{}", self.day);
        let key = self.key("scope");
        self.s
            .create_scope(
                &thread,
                ScopeKind::Thread,
                Some(&format!("project:{project}")),
                None,
                &key,
            )
            .unwrap();
        let mut events = Vec::new();
        for i in 0..30 {
            events.push(self.event(&thread, &stream, i == 7 && self.day % 10 == 0));
            self.cache_probe(&thread, &stream, "event");
        }
        let normal: Vec<String> = events
            .iter()
            .filter(|e| e.sensitivity == Sensitivity::Normal)
            .map(|e| e.id.clone())
            .collect();

        let key = self.key("plan");
        let started = Instant::now();
        let plan = self
            .s
            .plan_observation(&thread, &stream, 1_000_000, "sim", "v1", &key)
            .unwrap()
            .data
            .into_plan()
            .unwrap();
        let plan_ms = started.elapsed().as_secs_f64() * 1000.0;
        let kinds = [
            ClaimKind::Decision,
            ClaimKind::Fact,
            ClaimKind::Preference,
            ClaimKind::OpenLoop,
            ClaimKind::Hypothesis,
        ];
        let pick = |rng: &mut Rng| normal[rng.range(0, normal.len() as u64) as usize].clone();
        let observations = (0..6)
            .map(|_| ObservationDraft {
                kind: ObservationKind::Event,
                content: sentence(&mut self.rng, 10, 30),
                importance: self.rng.f(),
                confidence: 0.8,
                source_event_ids: vec![pick(&mut self.rng)],
                event_time_from: None,
                event_time_to: None,
            })
            .collect();
        let claims = (0..3)
            .map(|_| ClaimDraft {
                kind: kinds[self.rng.range(0, 5) as usize].clone(),
                subject: format!("obs:{}", self.rng.range(0, 300)),
                predicate: "value".into(),
                cardinality: ClaimCardinality::Single,
                value: json!(sentence(&mut self.rng, 4, 14)),
                modality: ClaimModality::Inference,
                confidence: 0.7,
                source_event_ids: vec![pick(&mut self.rng)],
            })
            .collect();
        let key = self.key("commit");
        let commit = self
            .s
            .commit_observation(
                &plan.run_id,
                ObserverResult {
                    observations,
                    claims,
                    continuation: ContinuationDraft {
                        current_task: Some(sentence(&mut self.rng, 4, 10)),
                        ..ContinuationDraft::default()
                    },
                    ambiguities: vec![],
                    empty_reason: None,
                },
                &key,
            )
            .unwrap()
            .data;
        self.cache_probe(&thread, &stream, "commit");
        for claim in commit.claims {
            let roll = self.rng.f();
            if roll < 0.4 {
                let key = self.key("confirm");
                self.s.confirm_claim(&claim.id, &key).unwrap();
                self.track(&claim.id);
                if self.rng.chance(0.25) {
                    let key = self.key("rescope");
                    match self
                        .s
                        .rescope_claim(&claim.id, &format!("project:{project}"), &key)
                    {
                        Ok(moved) => self.track(&moved.data.id),
                        Err(_) => self.conflicts += 1,
                    }
                }
            } else if roll < 0.7 {
                let key = self.key("reject");
                self.s.reject_claim(&claim.id, &key).unwrap();
            }
        }

        self.cache_probe(&thread, &stream, "review");
        let scope = format!("project:{project}");
        self.single_fact(&scope, ClaimKind::Fact, 0.6, "fact");
        if self.rng.chance(0.3) {
            let entity = format!("entity:{}", self.rng.range(0, 20));
            self.remember(
                &scope,
                ClaimKind::EntityAlias,
                &entity,
                ClaimCardinality::Set,
            );
        }
        if self.rng.chance(0.6) {
            let subject = format!("loop:{}", self.seq);
            let kind = [ClaimKind::OpenLoop, ClaimKind::Goal, ClaimKind::Commitment]
                [self.rng.range(0, 3) as usize]
                .clone();
            let claim = self.remember(&scope, kind, &subject, ClaimCardinality::Single);
            let resolve = self
                .rng
                .chance(hygiene)
                .then(|| self.day + self.rng.range(1, 21) as i64);
            self.loops.push((claim.id, resolve));
        }
        self.cache_probe(&thread, &stream, "facts");
        (thread, stream, plan_ms)
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = std::path::PathBuf::from(&args[1]);
    let days: i64 = args.get(2).map_or(360, |v| v.parse().unwrap());
    let hygiene: f64 = args.get(3).map_or(0.5, |v| v.parse().unwrap());
    std::fs::create_dir_all(&out).unwrap();
    let db = out.join("memory.db");
    let _ = std::fs::remove_file(&db);
    let mut sim = Sim {
        s: MemoryStore::open(&db).unwrap(),
        rng: Rng(0x9e37_79b9_7f4a_7c15),
        day: 0,
        seq: 0,
        claim_day: HashMap::new(),
        singles: HashMap::new(),
        loops: Vec::new(),
        conflicts: 0,
        probe: None,
        cache: BufWriter::new(File::create(out.join("cache.jsonl")).unwrap()),
        cache_budget: std::env::var("CACHE_BUDGET").map_or(16_000, |v| v.parse().unwrap()),
        cache_raw: std::env::var("CACHE_RAW").map_or(2_000, |v| v.parse().unwrap()),
        last_context: None,
    };
    // Operations commit in real time, so each simulated day moves every saved
    // operation one day into the past. That lets 30-day compaction run.
    let clock = rusqlite::Connection::open(&db).unwrap();
    clock
        .busy_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    sim.s
        .create_scope("user:me", ScopeKind::User, None, None, "scope-user")
        .unwrap();
    for p in ["a", "b", "c"] {
        sim.s
            .create_scope(
                &format!("project:{p}"),
                ScopeKind::Project,
                Some("user:me"),
                None,
                &format!("scope-{p}"),
            )
            .unwrap();
    }
    let mut month = BufWriter::new(File::create(out.join("month.jsonl")).unwrap());
    let mut snap = BufWriter::new(File::create(out.join("snap.jsonl")).unwrap());
    let (mut plan_ms, mut ctx_ms, mut samples) = (0.0, 0.0, 0.0);
    for day in 0..days {
        sim.day = day;
        let due: Vec<String> = sim
            .loops
            .iter()
            .filter(|(_, resolve)| *resolve == Some(day))
            .map(|(id, _)| id.clone())
            .collect();
        for id in due {
            let key = sim.key("forget");
            sim.s.forget_claim(&id, &key).unwrap();
        }
        if sim.rng.chance(0.3) {
            sim.single_fact("user:me", ClaimKind::Preference, 0.7, "pref");
        }
        for p in ["a", "b", "c"] {
            let active = p == "a" || (p == "b" && day % 2 == 0) || (p == "c" && day % 3 == 0);
            if !active {
                continue;
            }
            let (thread, stream, ms) = sim.thread_day(p, hygiene);
            plan_ms += ms;
            let started = Instant::now();
            let bundle = sim
                .s
                .compose_compact_context(&thread, &stream, i64::MAX / 4, 2000, None)
                .unwrap();
            ctx_ms += started.elapsed().as_secs_f64() * 1000.0;
            samples += 1.0;
            if p == "a" {
                let payload = bundle.compact_model_payload();
                let claims: Vec<Value> = bundle
                    .claims
                    .iter()
                    .zip(payload["claims"].as_array().unwrap())
                    .map(|(claim, record)| {
                        let (created, updated) = sim.claim_day.get(&claim.id).copied().unwrap_or((-1, -1));
                        let depth = if claim.scope_id.starts_with("user") { 0 } else if claim.scope_id.starts_with("project") { 1 } else { 2 };
                        json!({"id": claim.id, "depth": depth, "kind": claim.kind, "created": created,
                               "updated": updated, "tok": (record.to_string().chars().count() + 4) / 4})
                    })
                    .collect();
                // What a 16,000-token context actually keeps.
                let budgeted = sim
                    .s
                    .compose_compact_context(&thread, &stream, 16_000, 2000, None);
                let (ok, kept, over_budget) = match &budgeted {
                    Ok(bundle) => (
                        true,
                        bundle
                            .claims
                            .iter()
                            .map(|c| c.id.clone())
                            .collect::<Vec<_>>(),
                        bundle
                            .diagnostics
                            .omitted_items
                            .iter()
                            .filter(|item| item.reason == "active claim budget")
                            .count(),
                    ),
                    Err(_) => (false, Vec::new(), 0),
                };
                writeln!(
                    snap,
                    "{}",
                    json!({"day": day, "claims": claims, "ok16k": ok, "kept16k": kept,
                           "overBudget16k": over_budget})
                )
                .unwrap();
            }
        }
        clock
            .execute(
                "UPDATE memory_operation_results
                 SET created_at=strftime('%Y-%m-%dT%H:%M:%f+00:00',created_at,'-1 day')",
                [],
            )
            .unwrap();
        if day % 30 == 29 {
            // Purge one old normal event from project a and time it.
            let victim: Option<String> = {
                let stream = format!("s:a:{}", day / 2);
                sim.s
                    .recall_event_range(
                        &ReadAccess::agent(format!("thread:a:{}", day / 2)),
                        &stream,
                        1,
                        30,
                    )
                    .ok()
                    .and_then(|events| {
                        events
                            .into_iter()
                            .find(|e| e.sensitivity == Sensitivity::Normal)
                            .map(|e| e.id)
                    })
            };
            let mut purge_ms = 0.0;
            if let Some(id) = victim {
                let key = sim.key("purge");
                let started = Instant::now();
                sim.s.purge_event(&id, &key).unwrap();
                purge_ms = started.elapsed().as_secs_f64() * 1000.0;
            }
            let conn = rusqlite::Connection::open(&db).unwrap();
            let q = |sql: &str| -> i64 {
                conn.query_row(sql, [], |row| row.get::<_, Option<i64>>(0))
                    .unwrap()
                    .unwrap_or(0)
            };
            let mut by_op = serde_json::Map::new();
            {
                let mut statement = conn
                    .prepare("SELECT o.operation,COUNT(*),SUM(length(r.result_json)) FROM memory_operations o LEFT JOIN memory_operation_results r ON r.idempotency_key=o.idempotency_key GROUP BY o.operation")
                    .unwrap();
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                        ))
                    })
                    .unwrap();
                for row in rows {
                    let (op, n, bytes) = row.unwrap();
                    by_op.insert(op, json!({"n": n, "bytes": bytes.unwrap_or(0)}));
                }
            }
            let file_bytes = std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0)
                + std::fs::metadata(out.join("memory.db-wal"))
                    .map(|m| m.len())
                    .unwrap_or(0);
            let record = json!({
                "day": day,
                "dbBytes": file_bytes,
                "liveBytes": q("SELECT (page_count - freelist_count) * page_size FROM pragma_page_count, pragma_freelist_count, pragma_page_size"),
                "opTableBytes": q("SELECT SUM(pgsize) FROM dbstat WHERE name IN ('memory_operations','memory_operation_results')"),
                "events": q("SELECT COUNT(*) FROM memory_events"),
                "eventBytes": q("SELECT SUM(length(content_json)) FROM memory_events"),
                "opRows": q("SELECT COUNT(*) FROM memory_operations"),
                "opResultBytes": q("SELECT SUM(length(result_json)) FROM memory_operation_results"),
                "opRefs": q("SELECT COUNT(*) FROM memory_operation_refs"),
                "compactedRows": q("SELECT COUNT(*) FROM memory_operations o WHERE request_hash IS NOT NULL AND NOT EXISTS (SELECT 1 FROM memory_operation_results r WHERE r.idempotency_key=o.idempotency_key)"),
                "expiredRetained": q("SELECT COUNT(*) FROM memory_operation_results WHERE 1
                    AND created_at < strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-30 days')"),
                "activeUser": q("SELECT COUNT(*) FROM claims WHERE status='active' AND scope_id='user:me'"),
                "activeProjectA": q("SELECT COUNT(*) FROM claims WHERE status='active' AND scope_id='project:a'"),
                "pendingAll": q("SELECT COUNT(*) FROM claims WHERE status='pending'"),
                "byOp": by_op,
                "planMs": plan_ms / samples,
                "ctxMs": ctx_ms / samples,
                "purgeMs": purge_ms,
                "conflicts": sim.conflicts,
            });
            writeln!(month, "{record}").unwrap();
            month.flush().unwrap();
            eprintln!("{record}");
            plan_ms = 0.0;
            ctx_ms = 0.0;
            samples = 0.0;
        }
    }
    // Replay the early append: identical input must not run again, and
    // changed input must still be rejected.
    if let Some(probe) = sim.probe.take() {
        let kind = |result: anyhow::Result<MutationResult<MemoryEvent>>| match result {
            Ok(_) => "replayed-or-written".to_owned(),
            Err(error) => error
                .downcast_ref::<KernelError>()
                .map_or_else(|| error.to_string(), |e| format!("{:?}", e.kind())),
        };
        let mut changed = probe.clone();
        changed.content = json!("changed");
        let identical = kind(sim.s.append_event(probe));
        let changed = kind(sim.s.append_event(changed));
        let record = json!({"identicalReplay": identical, "changedReplay": changed});
        std::fs::write(out.join("final.json"), record.to_string()).unwrap();
        eprintln!("{record}");
    }
    let conn = rusqlite::Connection::open(&db).unwrap();
    let mut ops = BufWriter::new(File::create(out.join("ops.jsonl")).unwrap());
    let mut statement = conn
        .prepare("SELECT o.idempotency_key,o.operation,length(r.result_json),length(o.request_hash),length(o.idempotency_key) FROM memory_operations o LEFT JOIN memory_operation_results r ON r.idempotency_key=o.idempotency_key")
        .unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok(json!({"key": row.get::<_, String>(0)?, "op": row.get::<_, String>(1)?,
                      "result": row.get::<_, Option<i64>>(2)?, "hash": row.get::<_, Option<i64>>(3)?,
                      "keyLen": row.get::<_, i64>(4)?}))
        })
        .unwrap();
    for row in rows {
        writeln!(ops, "{}", row.unwrap()).unwrap();
    }
}
