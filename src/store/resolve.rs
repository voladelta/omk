use super::*;

/// Values that stand for a missing name. They never match and cannot be resolved.
const PLACEHOLDERS: &[&str] = &[
    "",
    "unknown",
    "n a",
    "na",
    "none",
    "null",
    "nil",
    "tbd",
    "todo",
    "unspecified",
    "not specified",
];
const MAX_NAME_CHARS: usize = 512;
const MAX_CANDIDATES: usize = 20;
/// Containment and spelling distance ignore names shorter than this.
const MIN_LOOSE_CHARS: usize = 3;
const MIN_FUZZY_CHARS: usize = 4;

/// Case-folded alphanumeric words joined by single spaces.
pub(super) fn normalize_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_placeholder(normalized: &str) -> bool {
    PLACEHOLDERS.contains(&normalized)
}

/// Optimal string alignment distance: edits plus adjacent transpositions.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut previous2 = vec![0; b.len() + 1];
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0; b.len() + 1];
    for i in 1..=a.len() {
        current[0] = i;
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            current[j] = (previous[j] + 1)
                .min(current[j - 1] + 1)
                .min(previous[j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                current[j] = current[j].min(previous2[j - 2] + 1);
            }
        }
        std::mem::swap(&mut previous2, &mut previous);
        std::mem::swap(&mut previous, &mut current);
    }
    previous[b.len()]
}

fn contains_words(haystack: &str, needle: &str) -> bool {
    needle.chars().count() >= MIN_LOOSE_CHARS
        && format!(" {haystack} ").contains(&format!(" {needle} "))
}

/// Distance of a known name from the query, or None when the tier misses it.
type TierTest<'a> = Box<dyn Fn(&KnownName) -> Option<usize> + 'a>;

struct KnownName {
    name: String,
    normalized: String,
    subject: String,
    via: &'static str,
    claim_id: Option<String>,
    scope_id: String,
}

fn known_names(conn: &Connection, scope_ids: &[String]) -> Result<(Vec<KnownName>, usize, usize)> {
    let placeholders = std::iter::repeat_n("?", scope_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let mut names = Vec::new();
    let mut statement = conn.prepare(&format!(
        "SELECT DISTINCT subject,scope_id FROM claims
         WHERE status='active' AND scope_id IN ({placeholders}) ORDER BY subject,scope_id"
    ))?;
    let subjects = collect_rows(
        statement.query_map(rusqlite::params_from_iter(scope_ids), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?,
    )?;
    for (subject, scope_id) in subjects {
        names.push(KnownName {
            normalized: normalize_name(&subject),
            name: subject.clone(),
            subject,
            via: "subject",
            claim_id: None,
            scope_id,
        });
    }
    let mut statement = conn.prepare(&format!(
        "SELECT id,subject,scope_id,value_json FROM claims
         WHERE status='active' AND kind='entity-alias' AND scope_id IN ({placeholders})
         ORDER BY subject,id"
    ))?;
    let aliases = collect_rows(statement.query_map(
        rusqlite::params_from_iter(scope_ids),
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        },
    )?)?;
    let mut alias_count = 0;
    for (id, subject, scope_id, value_json) in aliases {
        let Ok(Value::String(alias)) = serde_json::from_str::<Value>(&value_json) else {
            continue;
        };
        alias_count += 1;
        names.push(KnownName {
            normalized: normalize_name(&alias),
            name: alias,
            subject,
            via: "alias",
            claim_id: Some(id),
            scope_id,
        });
    }
    let subject_count = names
        .iter()
        .filter(|known| known.via == "subject")
        .map(|known| known.subject.as_str())
        .collect::<HashSet<_>>()
        .len();
    names.retain(|known| !is_placeholder(&known.normalized));
    Ok((names, subject_count, alias_count))
}

fn fuzzy_distance(query: &str, known: &str) -> usize {
    let mut best = edit_distance(query, known);
    if !query.contains(' ') {
        for word in known.split(' ') {
            if word.chars().count() >= MIN_FUZZY_CHARS {
                best = best.min(edit_distance(query, word));
            }
        }
    }
    best
}

pub(super) fn resolve_name(
    conn: &Connection,
    scope_ids: &[String],
    name: &str,
) -> Result<Resolution> {
    validate_nonempty("name", name)?;
    ensure!(
        name.chars().count() <= MAX_NAME_CHARS,
        KernelError::invalid_input(format!("name must be at most {MAX_NAME_CHARS} characters"))
    );
    let query = normalize_name(name);
    ensure!(
        !is_placeholder(&query),
        KernelError::invalid_input(format!(
            "name {name:?} is a placeholder for a missing name; resolve the real name first"
        ))
    );
    let (names, considered_subjects, considered_aliases) = known_names(conn, scope_ids)?;
    let query_chars = query.chars().count();
    let threshold = query_chars.div_ceil(5).clamp(1, 3);
    let tiers: [(ResolveTier, TierTest<'_>); 4] = [
        (
            ResolveTier::Exact,
            Box::new(|known| (known.name == name).then_some(0)),
        ),
        (
            ResolveTier::Name,
            Box::new(|known| (known.normalized == query).then_some(0)),
        ),
        (
            ResolveTier::Contains,
            Box::new(|known| {
                (contains_words(&known.normalized, &query)
                    || contains_words(&query, &known.normalized))
                .then_some(0)
            }),
        ),
        (
            ResolveTier::Fuzzy,
            Box::new(|known| {
                if query_chars < MIN_FUZZY_CHARS
                    || known.normalized.chars().count() < MIN_FUZZY_CHARS
                {
                    return None;
                }
                let distance = fuzzy_distance(&query, &known.normalized);
                (distance <= threshold).then_some(distance)
            }),
        ),
    ];
    let mut tier = None;
    let mut matched: Vec<(&KnownName, usize)> = Vec::new();
    for (candidate_tier, test) in &tiers {
        matched = names
            .iter()
            .filter_map(|known| test(known).map(|distance| (known, distance)))
            .collect();
        if let Some(best) = matched.iter().map(|(_, distance)| *distance).min() {
            matched.retain(|(_, distance)| *distance == best);
            tier = Some(*candidate_tier);
            break;
        }
    }
    let mut candidates: Vec<ResolveCandidate> = Vec::new();
    for (known, distance) in matched {
        let entry = ResolvedName {
            name: known.name.clone(),
            via: known.via.to_owned(),
            claim_id: known.claim_id.clone(),
            scope_id: known.scope_id.clone(),
        };
        match candidates.iter_mut().find(|c| c.subject == known.subject) {
            Some(candidate) => candidate.matched.push(entry),
            None => candidates.push(ResolveCandidate {
                subject: known.subject.clone(),
                matched: vec![entry],
                distance: (tier == Some(ResolveTier::Fuzzy)).then_some(distance),
            }),
        }
    }
    let status = match (candidates.len(), tier) {
        (0, _) => ResolveStatus::None,
        (1, Some(ResolveTier::Exact | ResolveTier::Name)) => ResolveStatus::Resolved,
        (1, _) => ResolveStatus::Probable,
        _ => ResolveStatus::Ambiguous,
    };
    let next_action = match status {
        ResolveStatus::Resolved => format!("reuse subject {:?} verbatim", candidates[0].subject),
        ResolveStatus::Probable => format!(
            "{name:?} only resembles subject {:?}; use it only if the source or user confirms they are the same, then record {name:?} as an entity-alias",
            candidates[0].subject
        ),
        ResolveStatus::Ambiguous => format!(
            "{name:?} matches {} subjects; ask which one is meant and do not write under the raw name",
            candidates.len()
        ),
        ResolveStatus::None if considered_subjects == 0 => {
            "no active claims in the visible scopes; check --scope, or treat the name as new"
                .to_owned()
        }
        ResolveStatus::None => format!(
            "no match among {considered_subjects} subjects and {considered_aliases} aliases; treat {name:?} as new only if the source makes that clear, otherwise ask"
        ),
    };
    candidates.truncate(MAX_CANDIDATES);
    Ok(Resolution {
        query: name.to_owned(),
        status,
        tier,
        candidates,
        considered_subjects,
        considered_aliases,
        next_action,
    })
}
