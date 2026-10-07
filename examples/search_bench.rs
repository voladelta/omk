//! Agent-eye benchmark for OMK search and name resolution.
//!
//! Drives one or more `omk` binaries through the CLI only, so the same run can
//! compare schema versions. Every binary gets an identical synthetic world:
//! people with canonical names, recorded aliases, facts, corrections, and
//! chat events that mention them in passing, skewed so a few people dominate.
//!
//! Usage: cargo run --release --example search_bench -- OUT_DIR BIN [BIN...] [--latency]
//!
//! Tasks:
//! - resolve: map a mention to an existing subject, flag ambiguity, or call it
//!   new. Schema 7 binaries follow the omk-memory skill procedure (search, then
//!   read each claim hit); schema 8 binaries also run `recall resolve`.
//! - lookup: find the current claim for "<name> <predicate>".
//! - latency (optional): search wall time with 211 scopes and 20,000 events.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use serde_json::Value;

const FIRST: &[(&str, &str)] = &[
    ("Alice", "Ali"),
    ("Robert", "Bob"),
    ("Katherine", "Kate"),
    ("William", "Will"),
    ("Margaret", "Maggie"),
    ("Thomas", "Tom"),
    ("Elizabeth", "Liz"),
    ("Daniel", "Dan"),
    ("Jennifer", "Jen"),
    ("Michael", "Mike"),
    ("Patricia", "Pat"),
    ("Christopher", "Chris"),
    ("Samantha", "Sam"),
    ("Jonathan", "Jon"),
    ("Victoria", "Vicky"),
    ("Nicholas", "Nick"),
    ("Rebecca", "Becky"),
    ("Anthony", "Tony"),
    ("Alexandra", "Alex"),
    ("Benjamin", "Ben"),
    ("Theodora", "Teddy"),
    ("Frederick", "Fred"),
    ("Gabriella", "Gabi"),
    ("Leonardo", "Leo"),
];

const LAST: &[&str] = &[
    "Moreau",
    "Chen",
    "Okafor",
    "Lindqvist",
    "Haddad",
    "Novak",
    "Tanaka",
    "Barros",
    "Kowalski",
    "Fitzgerald",
    "Achebe",
    "Rasmussen",
    "Delacroix",
    "Ivanova",
    "Mbeki",
    "Castellano",
    "Nakamura",
    "Oyelaran",
    "Petrov",
    "Quintero",
    "Sorensen",
    "Takahashi",
    "Underwood",
    "Valdivia",
    "Whitfield",
    "Yamamoto",
    "Zielinski",
    "Abernathy",
    "Bergstrom",
    "Cardenas",
    "Dubois",
    "Eriksen",
    "Ferreira",
    "Gallagher",
    "Hoffmann",
    "Iglesias",
    "Jaramillo",
    "Kaplan",
    "Laurent",
    "Mancini",
    "Nilsson",
    "Oconnell",
    "Pereira",
    "Quigley",
    "Rossi",
    "Schneider",
    "Thornton",
    "Ulrich",
    "Vasquez",
    "Wagner",
    "Xavier",
    "Yilmaz",
    "Zamora",
    "Albrecht",
    "Bianchi",
    "Costa",
    "Draper",
    "Esposito",
    "Fontaine",
    "Gutierrez",
    "Holloway",
    "Ibarra",
    "Jensen",
    "Keller",
    "Lombardi",
    "Marchetti",
    "Navarro",
    "Olsen",
    "Pellegrini",
    "Ramirez",
    "Santoro",
    "Tremblay",
    "Ueda",
    "Vidal",
    "Watanabe",
    "Yoshida",
    "Ziegler",
    "Andersen",
    "Bauer",
    "Carvalho",
    "Dimitrov",
    "Engel",
    "Falk",
    "Gomes",
    "Hartmann",
    "Ishikawa",
    "Janssen",
    "Krause",
    "Lehmann",
    "Moretti",
    "Nowak",
    "Ortega",
    "Popescu",
    "Reyes",
    "Silva",
    "Torres",
    "Urbano",
    "Vogel",
    "Weiss",
    "Young",
    "Zimmermann",
    "Aguilar",
    "Brandt",
    "Cruz",
    "Diaz",
    "Evans",
    "Fischer",
    "Garcia",
    "Hansen",
    "Ito",
    "Jovanovic",
];

const ORGS: &[&str] = &[
    "Northwind",
    "Contoso",
    "Globex",
    "Initech",
    "Umbrella",
    "Hooli",
];
const ROLES: &[&str] = &[
    "engineer",
    "designer",
    "manager",
    "analyst",
    "director",
    "recruiter",
    "counsel",
    "founder",
];
const CITIES: &[&str] = &[
    "Lisbon", "Osaka", "Denver", "Lagos", "Tallinn", "Montreal", "Hobart",
];
const TOPICS: &[&str] = &[
    "roadmap",
    "budget",
    "hiring",
    "launch",
    "migration",
    "pricing",
    "offsite",
    "contract",
    "security review",
    "onboarding",
];
const PREDICATES: &[&str] = &["email", "role", "city", "employer"];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

struct Entity {
    first: &'static str,
    nick: &'static str,
    last: &'static str,
    aliases: Vec<String>,
    /// predicate -> current claim id
    current: HashMap<&'static str, String>,
    superseded: Vec<String>,
}

impl Entity {
    fn subject(&self) -> String {
        format!("{} {}", self.first, self.last)
    }
}

struct Omk {
    bin: PathBuf,
    db: PathBuf,
    keys: u64,
    calls: u64,
    /// Characters of JSON the agent had to read, for a token estimate.
    read_chars: u64,
}

impl Omk {
    fn run(&mut self, args: &[&str]) -> Value {
        self.calls += 1;
        let output = Command::new(&self.bin)
            .arg("--db")
            .arg(&self.db)
            .args(args)
            .output()
            .expect("run omk");
        assert!(
            output.status.success(),
            "omk {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        self.read_chars += String::from_utf8_lossy(&output.stdout).chars().count() as u64;
        serde_json::from_slice(&output.stdout).expect("omk JSON output")
    }

    fn write(&mut self, args: &[&str]) -> Value {
        self.keys += 1;
        let key = format!("bench-{}", self.keys);
        let mut args = args.to_vec();
        args.extend(["--idempotency-key", &key]);
        self.run(&args)["data"].clone()
    }

    /// Search hits from either output shape: a bare array (schema 7) or a page.
    fn search(&mut self, args: &[&str]) -> Vec<Value> {
        let mut full = vec!["recall", "search"];
        full.extend(args);
        match self.run(&full) {
            Value::Array(hits) => hits,
            page => page["hits"].as_array().cloned().unwrap_or_default(),
        }
    }
}

fn open(bin: &Path, dir: &Path, name: &str) -> (Omk, i64) {
    let db = dir.join(name);
    let _ = std::fs::remove_file(&db);
    let mut omk = Omk {
        bin: bin.to_owned(),
        db,
        keys: 0,
        calls: 0,
        read_chars: 0,
    };
    let schema = omk.run(&["init"])["data"]["schemaVersion"]
        .as_i64()
        .unwrap_or(0);
    (omk, schema)
}

fn normalize(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn build_world(omk: &mut Omk) -> Vec<Entity> {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    omk.write(&["scope", "add", "--id", "user:me", "--kind", "user"]);
    let mut threads = Vec::new();
    for p in 0..4 {
        let project = format!("project:p{p}");
        omk.write(&[
            "scope", "add", "--id", &project, "--kind", "project", "--parent", "user:me",
        ]);
        for t in 0..5 {
            let thread = format!("thread:p{p}t{t}");
            omk.write(&[
                "scope", "add", "--id", &thread, "--kind", "thread", "--parent", &project,
            ]);
            threads.push(thread);
        }
    }
    let mut entities: Vec<Entity> = Vec::new();
    let mut used = BTreeSet::new();
    while entities.len() < 120 {
        let (first, nick) = *rng.pick(FIRST);
        let last = *rng.pick(&LAST[..90]);
        if !used.insert((first, last)) {
            continue;
        }
        entities.push(Entity {
            first,
            nick,
            last,
            aliases: Vec::new(),
            current: HashMap::new(),
            superseded: Vec::new(),
        });
    }
    for entity in &mut entities {
        let subject = entity.subject();
        let org = *rng.pick(ORGS);
        let values = [
            format!(
                "{}.{}@{}.com",
                entity.first.to_lowercase(),
                entity.last.to_lowercase(),
                org.to_lowercase()
            ),
            rng.pick(ROLES).to_string(),
            rng.pick(CITIES).to_string(),
            org.to_string(),
        ];
        for (predicate, value) in PREDICATES.iter().zip(values) {
            let value = Value::String(value).to_string();
            let claim = omk.write(&[
                "claim",
                "remember",
                "--scope",
                "user:me",
                "--kind",
                "fact",
                "--subject",
                &subject,
                "--predicate",
                predicate,
                "--value",
                &value,
            ]);
            entity
                .current
                .insert(predicate, claim["id"].as_str().unwrap().to_owned());
        }
        for (predicate, percent, pool) in [("role", 35, ROLES), ("city", 25, CITIES)] {
            if rng.chance(percent) {
                let old = entity.current[predicate].clone();
                let value = Value::String(rng.pick(pool).to_string()).to_string();
                let claim = omk.write(&["claim", "correct", "--id", &old, "--value", &value]);
                entity.superseded.push(old);
                entity
                    .current
                    .insert(predicate, claim["id"].as_str().unwrap().to_owned());
            }
        }
        let mut aliases = Vec::new();
        if rng.chance(50) {
            aliases.push(format!("{} {}", entity.nick, entity.last));
        }
        if rng.chance(40) {
            aliases.push(format!(
                "{}{}",
                entity.first[..1].to_lowercase(),
                entity.last.to_lowercase()
            ));
        }
        for alias in aliases {
            let value = Value::String(alias.clone()).to_string();
            omk.write(&[
                "claim",
                "remember",
                "--scope",
                "user:me",
                "--kind",
                "entity-alias",
                "--subject",
                &subject,
                "--predicate",
                "alias",
                "--cardinality",
                "set",
                "--value",
                &value,
            ]);
            entity.aliases.push(alias);
        }
    }
    // Chatter follows a Zipf curve, so a few people dominate the history the
    // way a manager or a close collaborator would.
    let weights: Vec<f64> = (1..=entities.len()).map(|rank| 1.0 / rank as f64).collect();
    let total: f64 = weights.iter().sum();
    for i in 0..4000 {
        let mut target = (rng.next() % 1_000_000) as f64 / 1_000_000.0 * total;
        let mut index = 0;
        while target > weights[index] && index + 1 < weights.len() {
            target -= weights[index];
            index += 1;
        }
        let e = &entities[index];
        let topic = *rng.pick(TOPICS);
        let content = match rng.below(7) {
            0 => format!("Met {} about the {topic}", e.first),
            1 => format!("{} {} says the {topic} slips a week", e.first, e.last),
            2 => format!("ping {} re {topic}", e.nick),
            3 => format!("Got an email from {} {} about the {topic}", e.first, e.last),
            4 => format!("{} {} wants a bigger role in the {topic}", e.first, e.last),
            5 => format!("{} {} asked which city hosts the {topic}", e.first, e.last),
            _ => format!(
                "Call with {} {}'s team on {topic}; {} will follow up",
                e.first, e.last, e.first
            ),
        };
        let thread = threads[i % threads.len()].clone();
        omk.write(&[
            "event",
            "append",
            "--scope",
            &thread,
            "--stream",
            &format!("s-{thread}"),
            "--kind",
            "user-message",
            "--content",
            &content,
        ]);
    }
    entities
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum Outcome {
    One(String),
    Ambiguous,
    New,
}

struct Mention {
    category: &'static str,
    text: String,
    expected: Outcome,
}

fn mentions(entities: &[Entity]) -> Vec<Mention> {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let subjects_matching = |words: &str| -> Vec<String> {
        let words = normalize(words);
        entities
            .iter()
            .filter(|e| {
                std::iter::once(e.subject())
                    .chain(e.aliases.iter().cloned())
                    .any(|name| format!(" {} ", normalize(&name)).contains(&format!(" {words} ")))
            })
            .map(Entity::subject)
            .collect()
    };
    let mut result = Vec::new();
    for e in entities.iter().step_by(2) {
        let subject = e.subject();
        let one = Outcome::One(subject.clone());
        result.push(Mention {
            category: "exact name",
            text: subject.clone(),
            expected: one.clone(),
        });
        result.push(Mention {
            category: "case variant",
            text: subject.to_uppercase(),
            expected: one.clone(),
        });
        result.push(Mention {
            category: "title + name",
            text: format!("Dr. {subject}"),
            expected: one.clone(),
        });
        if let Some(alias) = e.aliases.first() {
            result.push(Mention {
                category: "recorded alias",
                text: alias.clone(),
                expected: one.clone(),
            });
        }
        let mut last: Vec<char> = e.last.chars().collect();
        let at = 1 + rng.below(last.len() - 2);
        last.swap(at, at + 1);
        let typo: String = last.into_iter().collect();
        if typo != e.last {
            result.push(Mention {
                category: "typo",
                text: format!("{} {typo}", e.first),
                expected: one.clone(),
            });
        }
        let sharing = subjects_matching(e.last);
        result.push(Mention {
            category: "last name only",
            text: e.last.to_owned(),
            expected: if sharing.len() > 1 {
                Outcome::Ambiguous
            } else {
                one.clone()
            },
        });
    }
    for e in entities.iter().step_by(4) {
        let sharing = subjects_matching(e.first);
        result.push(Mention {
            category: "first name only",
            text: e.first.to_owned(),
            expected: if sharing.len() > 1 {
                Outcome::Ambiguous
            } else {
                Outcome::One(e.subject())
            },
        });
        // A nickname nobody recorded: a person would guess, then confirm.
        let alias = format!("{} {}", e.nick, e.last);
        if !e.aliases.contains(&alias) && subjects_matching(e.last).len() == 1 {
            result.push(Mention {
                category: "unrecorded nickname",
                text: alias,
                expected: Outcome::One(e.subject()),
            });
        }
        // A different, new person who shares a recorded last name.
        let (first, _) =
            FIRST[(FIRST.iter().position(|(f, _)| *f == e.first).unwrap() + 7) % FIRST.len()];
        if subjects_matching(&format!("{first} {}", e.last)).is_empty() {
            result.push(Mention {
                category: "new, shared last name",
                text: format!("{first} {}", e.last),
                expected: Outcome::New,
            });
        }
    }
    for i in 0..30 {
        let (first, _) = FIRST[i % FIRST.len()];
        result.push(Mention {
            category: "new person",
            text: format!("{first} {}", LAST[90 + i % (LAST.len() - 90)]),
            expected: Outcome::New,
        });
    }
    result
}

/// The omk-memory skill procedure: search the name, keep claim hits, read each
/// claim, and keep subjects whose own name or alias value matches.
fn resolve_by_search(omk: &mut Omk, name: &str, limit: &str, schema: i64) -> Outcome {
    let mut args = vec![
        "--scope",
        "user:me",
        "--query",
        name,
        "--terms",
        "--current-only",
        "--limit",
        limit,
    ];
    if schema >= 8 {
        args.extend(["--type", "claim"]);
    }
    let hits = omk.search(&args);
    let wanted = normalize(name);
    let matches = |candidate: &str| {
        let candidate = normalize(candidate);
        !candidate.is_empty()
            && (format!(" {candidate} ").contains(&format!(" {wanted} "))
                || format!(" {wanted} ").contains(&format!(" {candidate} ")))
    };
    let mut subjects = BTreeSet::new();
    let mut read = HashMap::new();
    for hit in hits.iter().filter(|hit| hit.get("claimStatus").is_some()) {
        let id = hit["id"].as_str().unwrap().to_owned();
        let claim = if schema >= 8 {
            // Schema 8 hits carry subject and predicate; the value is the preview tail.
            let subject = hit["subject"].as_str().unwrap_or_default().to_owned();
            let predicate = hit["predicate"].as_str().unwrap_or_default();
            let text = hit["text"].as_str().unwrap_or_default();
            let value = text
                .strip_prefix(&format!("{subject} {predicate} "))
                .unwrap_or_default()
                .to_owned();
            (subject, predicate == "alias", value)
        } else {
            read.entry(id.clone())
                .or_insert_with(|| {
                    let claim = omk.run(&[
                        "recall",
                        "explain-claim",
                        "--scope",
                        "user:me",
                        "--id",
                        &id,
                    ])["claim"]
                        .clone();
                    (
                        claim["subject"].as_str().unwrap_or_default().to_owned(),
                        claim["kind"] == "entity-alias",
                        claim["value"].as_str().unwrap_or_default().to_owned(),
                    )
                })
                .clone()
        };
        let (subject, is_alias, value) = claim;
        if matches(&subject) || (is_alias && matches(&value)) {
            subjects.insert(subject);
        }
    }
    match subjects.len() {
        0 => Outcome::New,
        1 => Outcome::One(subjects.into_iter().next().unwrap()),
        _ => Outcome::Ambiguous,
    }
}

fn resolve_by_command(omk: &mut Omk, name: &str) -> (Outcome, bool) {
    let result = omk.run(&["recall", "resolve", "--scope", "user:me", "--name", name]);
    let outcome = match result["status"].as_str().unwrap() {
        "resolved" | "probable" => Outcome::One(
            result["candidates"][0]["subject"]
                .as_str()
                .unwrap()
                .to_owned(),
        ),
        "ambiguous" => Outcome::Ambiguous,
        _ => Outcome::New,
    };
    (outcome, result["status"] == "probable")
}

#[derive(Default)]
struct ResolveScore {
    total: usize,
    correct: usize,
    duplicate: usize,
    wrong_merge: usize,
    needless_question: usize,
    probable: usize,
    calls: u64,
    read_chars: u64,
    by_category: HashMap<&'static str, (usize, usize)>,
}

impl ResolveScore {
    fn add(&mut self, mention: &Mention, got: &Outcome) {
        self.total += 1;
        let entry = self.by_category.entry(mention.category).or_default();
        entry.1 += 1;
        if *got == mention.expected {
            self.correct += 1;
            entry.0 += 1;
            return;
        }
        match (got, &mention.expected) {
            (Outcome::New, _) => self.duplicate += 1,
            (Outcome::One(_), _) => self.wrong_merge += 1,
            (Outcome::Ambiguous, _) => self.needless_question += 1,
        }
    }
}

/// OMK's own estimate: one token per four characters.
fn tokens_per(chars: u64, total: usize) -> String {
    format!("{:.0}", chars as f64 / 4.0 / total as f64)
}

fn pct(n: usize, d: usize) -> String {
    if d == 0 {
        return "-".to_owned();
    }
    format!("{:.1}%", 100.0 * n as f64 / d as f64)
}

fn run_resolve(
    label: &str,
    omk: &mut Omk,
    mentions: &[Mention],
    mut strategy: impl FnMut(&mut Omk, &str) -> (Outcome, bool),
) -> ResolveScore {
    let mut score = ResolveScore::default();
    let before = omk.calls;
    let chars_before = omk.read_chars;
    for mention in mentions {
        let (got, probable) = strategy(omk, &mention.text);
        if probable && got == mention.expected {
            score.probable += 1;
        }
        score.add(mention, &got);
    }
    score.calls = omk.calls - before;
    score.read_chars = omk.read_chars - chars_before;
    eprintln!("  resolve {label}: {}/{}", score.correct, score.total);
    score
}

struct LookupScore {
    hit1: usize,
    recall: usize,
    mrr: f64,
    stale_above: usize,
    total: usize,
    read_chars: u64,
}

fn run_lookup(omk: &mut Omk, entities: &[Entity], extra: &[&str]) -> LookupScore {
    let mut score = LookupScore {
        hit1: 0,
        recall: 0,
        mrr: 0.0,
        stale_above: 0,
        total: 0,
        read_chars: 0,
    };
    let chars_before = omk.read_chars;
    for e in entities {
        let stale: BTreeSet<&str> = e.superseded.iter().map(String::as_str).collect();
        for predicate in PREDICATES {
            let target = &e.current[predicate];
            let query = format!("{} {predicate}", e.subject());
            let mut args = vec![
                "--scope", "user:me", "--query", &query, "--terms", "--limit", "20",
            ];
            args.extend(extra);
            let hits = omk.search(&args);
            score.total += 1;
            if let Some(position) = hits.iter().position(|hit| hit["id"] == target.as_str()) {
                score.recall += 1;
                score.mrr += 1.0 / (position + 1) as f64;
                if position == 0 {
                    score.hit1 += 1;
                }
                if hits[..position]
                    .iter()
                    .any(|hit| stale.contains(hit["id"].as_str().unwrap_or_default()))
                {
                    score.stale_above += 1;
                }
            }
        }
    }
    score.read_chars = omk.read_chars - chars_before;
    score
}

fn median_ms(omk: &mut Omk, args: &[&str], runs: usize) -> f64 {
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        omk.search(args);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(f64::total_cmp);
    times[runs / 2]
}

fn latency(bin: &Path, dir: &Path, label: &str) -> Vec<String> {
    let (mut omk, _) = open(bin, dir, &format!("latency-{label}.db"));
    omk.write(&["scope", "add", "--id", "user:me", "--kind", "user"]);
    let mut threads = Vec::new();
    for p in 0..10 {
        let project = format!("project:p{p}");
        omk.write(&[
            "scope", "add", "--id", &project, "--kind", "project", "--parent", "user:me",
        ]);
        for t in 0..20 {
            let thread = format!("thread:p{p}t{t}");
            omk.write(&[
                "scope", "add", "--id", &thread, "--kind", "thread", "--parent", &project,
            ]);
            threads.push(thread);
        }
    }
    let mut rng = Rng(7);
    for i in 0..20_000 {
        let thread = &threads[i % threads.len()];
        let content = format!("deploy note {i}: {} {}", rng.pick(TOPICS), rng.pick(CITIES));
        omk.write(&[
            "event",
            "append",
            "--scope",
            thread,
            "--stream",
            &format!("s-{thread}"),
            "--kind",
            "user-message",
            "--content",
            &content,
        ]);
    }
    let base = ["--query", "deploy", "--limit", "10"];
    let mut rows = Vec::new();
    for (name, scope) in [
        ("one thread (3 visible scopes)", "thread:p3t7"),
        ("project (21 scopes)", "project:p3"),
        ("user (211 scopes)", "user:me"),
    ] {
        let mut args = vec!["--scope", scope];
        args.extend(base);
        let ms = median_ms(&mut omk, &args, 41);
        rows.push(format!("| {label} | {name} | {ms:.1} |"));
    }
    rows
}

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let with_latency = args.iter().any(|a| a == "--latency");
    args.retain(|a| a != "--latency");
    assert!(
        args.len() >= 2,
        "usage: search_bench OUT_DIR BIN [BIN...] [--latency]"
    );
    let dir = PathBuf::from(&args[0]);
    std::fs::create_dir_all(&dir).unwrap();
    let mut resolve_rows = Vec::new();
    let mut category_rows = Vec::new();
    let mut lookup_rows = Vec::new();
    let mut latency_rows = Vec::new();
    let mut mention_total = 0;
    for bin in &args[1..] {
        let bin = std::fs::canonicalize(bin).expect("binary path");
        let name = bin.file_name().unwrap().to_string_lossy().into_owned();
        let (mut omk, schema) = open(&bin, &dir, &format!("{name}.db"));
        let label = format!("{name} (schema {schema})");
        eprintln!("{label}: building world");
        let entities = build_world(&mut omk);
        let mentions = mentions(&entities);
        mention_total = mentions.len();
        let mut strategies: Vec<(String, ResolveScore)> = Vec::new();
        for limit in ["20", "100"] {
            let strategy = format!("skill search procedure, --limit {limit}");
            let score = run_resolve(&strategy, &mut omk, &mentions, |omk, name| {
                (resolve_by_search(omk, name, limit, schema), false)
            });
            strategies.push((strategy, score));
        }
        if schema >= 8 {
            let score = run_resolve("recall resolve", &mut omk, &mentions, resolve_by_command);
            strategies.push(("recall resolve".to_owned(), score));
        }
        for (strategy, s) in &strategies {
            resolve_rows.push(format!(
                "| {label} | {strategy} | {} | {} | {} | {} | {} | {:.1} | {} |",
                pct(s.correct, s.total),
                pct(s.duplicate, s.total),
                pct(s.wrong_merge, s.total),
                pct(s.needless_question, s.total),
                s.probable,
                s.calls as f64 / s.total as f64,
                tokens_per(s.read_chars, s.total)
            ));
            let mut categories: Vec<_> = s.by_category.iter().collect();
            categories.sort();
            let cells: Vec<String> = categories
                .iter()
                .map(|(category, (ok, n))| format!("{category} {}", pct(*ok, *n)))
                .collect();
            category_rows.push(format!("| {label} | {strategy} | {} |", cells.join(" · ")));
        }
        let mut lookups: Vec<(&str, Vec<&str>)> = vec![
            ("default", vec![]),
            ("--current-only", vec!["--current-only"]),
        ];
        if schema >= 8 {
            lookups.push((
                "--current-only --type claim",
                vec!["--current-only", "--type", "claim"],
            ));
        }
        for (variant, extra) in lookups {
            let s = run_lookup(&mut omk, &entities, &extra);
            lookup_rows.push(format!(
                "| {label} | {variant} | {} | {:.3} | {} | {} | {} |",
                pct(s.hit1, s.total),
                s.mrr / s.total as f64,
                pct(s.recall, s.total),
                s.stale_above,
                tokens_per(s.read_chars, s.total)
            ));
        }
        if with_latency {
            eprintln!("{label}: latency world");
            latency_rows.extend(latency(&bin, &dir, &name));
        }
    }
    let mut report = String::new();
    report.push_str("## Name resolution\n\n");
    report.push_str(&format!("{mention_total} mentions per binary.\n\n"));
    report.push_str("| binary | strategy | correct | duplicate entity | wrong merge | needless question | correct but probable | CLI calls per mention | tokens read per mention |\n|---|---|---|---|---|---|---|---|---|\n");
    report.push_str(&resolve_rows.join("\n"));
    report.push_str("\n\n### Correct by mention category\n\n| binary | strategy | categories |\n|---|---|---|\n");
    report.push_str(&category_rows.join("\n"));
    report.push_str("\n\n## Fact lookup\n\n480 questions: `<name> <predicate>` with `--terms --limit 20`; target is the current claim.\n\n| binary | flags | hit@1 | MRR@20 | recall@20 | stale claim ranked above target | tokens read per question |\n|---|---|---|---|---|---|---|\n");
    report.push_str(&lookup_rows.join("\n"));
    if with_latency {
        report.push_str("\n\n## Search latency\n\n20,000 events over 211 scopes; query `deploy` matches every event. Median CLI wall time of 41 runs, process start included.\n\n| binary | anchor scope | median ms |\n|---|---|---|\n");
        report.push_str(&latency_rows.join("\n"));
    }
    report.push('\n');
    std::fs::write(dir.join("report.md"), &report).unwrap();
    println!("{report}");
}
