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
//! log rows).
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
}

impl Sim {
    fn key(&mut self, what: &str) -> String {
        self.seq += 1;
        format!("d{}:{}:{}", self.day, what, self.seq)
    }

    fn event(&mut self, scope: &str, stream: &str, secret: bool) -> MemoryEvent {
        let key = self.key("event");
        let content = sentence(&mut self.rng, 40, 120);
        self.s
            .append_event(NewEvent {
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
            })
            .unwrap()
            .data
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
        let events: Vec<MemoryEvent> = (0..30)
            .map(|i| self.event(&thread, &stream, i == 7 && self.day % 10 == 0))
            .collect();
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
    };
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
                writeln!(snap, "{}", json!({"day": day, "claims": claims})).unwrap();
            }
        }
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
                    .prepare("SELECT operation,COUNT(*),SUM(length(result_json)) FROM memory_operations GROUP BY operation")
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
                "events": q("SELECT COUNT(*) FROM memory_events"),
                "eventBytes": q("SELECT SUM(length(content_json)) FROM memory_events"),
                "opRows": q("SELECT COUNT(*) FROM memory_operations"),
                "opResultBytes": q("SELECT SUM(length(result_json)) FROM memory_operations"),
                "opRefs": q("SELECT COUNT(*) FROM memory_operation_refs"),
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
    let conn = rusqlite::Connection::open(&db).unwrap();
    let mut ops = BufWriter::new(File::create(out.join("ops.jsonl")).unwrap());
    let mut statement = conn
        .prepare("SELECT idempotency_key,operation,length(result_json),length(request_hash),length(idempotency_key) FROM memory_operations")
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
