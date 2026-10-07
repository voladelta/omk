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

/// Common English diminutives and the formal names they stand for. A
/// nickname that is a prefix of its formal name (Kate, Will) needs no entry.
const NICKNAMES: &[(&str, &[&str])] = &[
    ("abigail", &["abby", "gail"]),
    ("albert", &["bert", "al"]),
    ("alexander", &["sasha", "xander", "sandy"]),
    ("alexandra", &["sasha", "sandra", "lexi"]),
    ("alfred", &["fred", "alf"]),
    ("andrew", &["drew", "andy"]),
    ("anthony", &["tony"]),
    ("barbara", &["babs", "barb"]),
    ("catherine", &["cathy", "kate", "katie", "kat"]),
    ("charles", &["chuck", "charlie", "chas"]),
    ("christina", &["tina", "chrissy"]),
    ("christine", &["tina", "chrissy"]),
    ("christopher", &["kit", "topher"]),
    ("deborah", &["debbie", "deb"]),
    ("dorothy", &["dot", "dotty", "dolly"]),
    ("edward", &["ted", "teddy", "ned", "eddie"]),
    (
        "elizabeth",
        &["liz", "lizzie", "beth", "betty", "betsy", "libby", "eliza"],
    ),
    ("eleanor", &["nell", "nora", "ellie"]),
    ("frederick", &["freddie"]),
    ("gabriella", &["gabby"]),
    ("harold", &["harry", "hal"]),
    ("henry", &["harry", "hank", "hal"]),
    ("james", &["jim", "jimmy", "jamie"]),
    ("jennifer", &["jenny"]),
    ("john", &["jack", "johnny"]),
    ("jonathan", &["jonny"]),
    ("joseph", &["joe", "joey"]),
    ("katherine", &["kate", "katie", "kathy", "kat", "kay"]),
    ("kathryn", &["kate", "katie", "kathy"]),
    ("lawrence", &["larry"]),
    ("leonardo", &["leo"]),
    (
        "margaret",
        &["maggie", "meg", "peggy", "marge", "greta", "daisy"],
    ),
    ("matthew", &["matt"]),
    ("michael", &["mike", "mikey", "mick"]),
    ("nicholas", &["nick", "nicky"]),
    ("patricia", &["patty", "trish", "tricia"]),
    ("patrick", &["paddy", "rick"]),
    ("peter", &["pete"]),
    ("rebecca", &["becky", "becca"]),
    ("richard", &["rick", "dick", "rich", "ricky"]),
    ("robert", &["bob", "bobby", "rob", "robbie", "bert"]),
    ("samantha", &["sammy"]),
    ("samuel", &["sammy"]),
    ("stephen", &["steve"]),
    ("steven", &["steve"]),
    ("susan", &["sue", "susie"]),
    ("theodora", &["teddy", "dora", "thea"]),
    ("theodore", &["ted", "teddy", "theo"]),
    ("thomas", &["tommy"]),
    ("victoria", &["vicky", "tori", "vic"]),
    ("william", &["bill", "billy", "will", "willy", "liam"]),
];

/// Fold the Latin accents people often drop when typing a name.
fn fold_accent(c: char) -> &'static str {
    match c {
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => "a",
        'ç' | 'ć' | 'č' => "c",
        'ď' | 'đ' => "d",
        'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ė' | 'ę' | 'ě' => "e",
        'ğ' => "g",
        'ì' | 'í' | 'î' | 'ï' | 'ī' | 'ı' => "i",
        'ł' | 'ľ' => "l",
        'ñ' | 'ń' | 'ň' => "n",
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' | 'ő' => "o",
        'ř' => "r",
        'ś' | 'š' | 'ş' => "s",
        'ť' | 'ţ' => "t",
        'ù' | 'ú' | 'û' | 'ü' | 'ū' | 'ů' | 'ű' => "u",
        'ý' | 'ÿ' => "y",
        'ź' | 'ż' | 'ž' => "z",
        'ß' => "ss",
        'æ' => "ae",
        'œ' => "oe",
        'þ' => "th",
        _ => "",
    }
}

/// Case-folded, accent-folded alphanumeric words joined by single spaces.
/// Apostrophes join their neighbours, so O'Connell and OConnell agree.
pub(super) fn normalize_name(name: &str) -> String {
    let mut folded = String::with_capacity(name.len());
    for c in name.to_lowercase().chars() {
        match fold_accent(c) {
            "" if matches!(c, '\'' | '\u{2019}' | '`') => {}
            "" if c.is_alphanumeric() => folded.push(c),
            "" => folded.push(' '),
            ascii => folded.push_str(ascii),
        }
    }
    folded.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Normalized words in sorted order, so word order does not matter.
fn word_key(normalized: &str) -> String {
    let mut words: Vec<&str> = normalized.split(' ').collect();
    words.sort_unstable();
    words.join(" ")
}

fn is_nickname(short: &str, formal: &str) -> bool {
    NICKNAMES
        .iter()
        .any(|(name, nicknames)| *name == formal && nicknames.contains(&short))
}

/// How a query word can stand for a known word in the tokens tier.
fn word_matches(query: &str, known: &str) -> bool {
    query == known
        || known.starts_with(query)
        || (known.chars().count() >= MIN_LOOSE_CHARS && query.starts_with(known))
        || is_nickname(query, known)
        || is_nickname(known, query)
}

/// Every query word stands for a different known word, in any order, and at
/// least one pair matches whole: "A. Moreau" or "Bob Novak", not "A. B.".
///
/// Each whole pair is tried as the anchor, and the other query words are
/// paired by augmenting paths, so the cost stays polynomial in the word
/// counts instead of trying every assignment.
fn words_match(query: &[&str], known: &[&str]) -> bool {
    if query.len() < 2 || query.len() > known.len() {
        return false;
    }
    let anchors: Vec<(usize, usize)> = query
        .iter()
        .enumerate()
        .filter(|(_, word)| word.chars().count() >= MIN_LOOSE_CHARS)
        .flat_map(|(q, word)| {
            known
                .iter()
                .enumerate()
                .filter(move |(_, other)| *other == word)
                .map(move |(k, _)| (q, k))
        })
        .collect();
    if anchors.is_empty() {
        return false;
    }
    let fits: Vec<Vec<bool>> = query
        .iter()
        .map(|q| known.iter().map(|k| word_matches(q, k)).collect())
        .collect();
    anchors
        .into_iter()
        .any(|(anchor_q, anchor_k)| pairs_rest(&fits, anchor_q, anchor_k))
}

/// Whether every query word except `anchor_q` fits its own known word other
/// than `anchor_k`, by Kuhn's augmenting-path matching.
fn pairs_rest(fits: &[Vec<bool>], anchor_q: usize, anchor_k: usize) -> bool {
    fn augment(
        q: usize,
        fits: &[Vec<bool>],
        owner: &mut [Option<usize>],
        seen: &mut [bool],
    ) -> bool {
        for k in 0..owner.len() {
            if fits[q][k] && !seen[k] {
                seen[k] = true;
                if owner[k].is_none_or(|other| augment(other, fits, owner, seen)) {
                    owner[k] = Some(q);
                    return true;
                }
            }
        }
        false
    }
    let mut owner = vec![None; fits[anchor_q].len()];
    (0..fits.len()).filter(|q| *q != anchor_q).all(|q| {
        let mut seen = vec![false; owner.len()];
        seen[anchor_k] = true;
        augment(q, fits, &mut owner, &mut seen)
    })
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

/// Whether `needle` occurs in `haystack` as whole words. Both are normalized,
/// so words are separated by single spaces.
fn contains_words(haystack: &str, needle: &str) -> bool {
    if needle.chars().count() < MIN_LOOSE_CHARS {
        return false;
    }
    let bytes = haystack.as_bytes();
    let mut from = 0;
    while let Some(offset) = haystack[from..].find(needle) {
        let at = from + offset;
        let end = at + needle.len();
        if (at == 0 || bytes[at - 1] == b' ') && (end == bytes.len() || bytes[end] == b' ') {
            return true;
        }
        // Occurrences may overlap, so resume one character later.
        from = at + haystack[at..].chars().next().map_or(1, char::len_utf8);
    }
    false
}

/// Distance of a known name from the query, or None when the tier misses it.
type TierTest<'a> = Box<dyn Fn(&KnownName) -> Option<usize> + 'a>;

struct KnownName {
    name: String,
    normalized: String,
    key: String,
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
        let normalized = normalize_name(&subject);
        names.push(KnownName {
            key: word_key(&normalized),
            normalized,
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
    for (id, subject, scope_id, value_json) in aliases {
        let Ok(Value::String(alias)) = serde_json::from_str::<Value>(&value_json) else {
            continue;
        };
        let normalized = normalize_name(&alias);
        names.push(KnownName {
            key: word_key(&normalized),
            normalized,
            name: alias,
            subject,
            via: "alias",
            claim_id: Some(id),
            scope_id,
        });
    }
    // Placeholders never match, so they do not count as considered either.
    names.retain(|known| !is_placeholder(&known.normalized));
    let subject_count = names
        .iter()
        .filter(|known| known.via == "subject")
        .map(|known| known.subject.as_str())
        .collect::<HashSet<_>>()
        .len();
    let alias_count = names.iter().filter(|known| known.via == "alias").count();
    Ok((names, subject_count, alias_count))
}

/// Edits one word may absorb: none up to 3 characters, then one, then two
/// from 8 characters. A per-word budget keeps a long shared surname from
/// paying for a different first name.
fn word_budget(a: &str, b: &str) -> usize {
    match a.chars().count().min(b.chars().count()) {
        0..=3 => 0,
        4..=7 => 1,
        _ => 2,
    }
}

/// Total edits when each word stays within its budget and, for names of
/// several words, at least one word matches exactly.
fn aligned_distance(query: &[&str], known: &[&str]) -> Option<usize> {
    let mut total = 0;
    let mut exact = false;
    for (q, k) in query.iter().zip(known) {
        let distance = within_budget(q, k)?;
        exact |= distance == 0;
        total += distance;
    }
    exact.then_some(total)
}

/// Edit distance of two words when it fits their budget. The length gap is a
/// lower bound on the distance, so a larger gap skips the full computation.
fn within_budget(a: &str, b: &str) -> Option<usize> {
    let budget = word_budget(a, b);
    if a.chars().count().abs_diff(b.chars().count()) > budget {
        return None;
    }
    let distance = edit_distance(a, b);
    (distance <= budget).then_some(distance)
}

fn fuzzy_distance(query_words: &[&str], key_words: &[&str], known: &KnownName) -> Option<usize> {
    let known_words: Vec<&str> = known.normalized.split(' ').collect();
    if let [word] = query_words {
        return known_words
            .iter()
            .filter(|known| known.chars().count() >= MIN_FUZZY_CHARS)
            .filter_map(|known| within_budget(word, known))
            .min();
    }
    if query_words.len() != known_words.len() {
        return None;
    }
    let known_key: Vec<&str> = known.key.split(' ').collect();
    [
        aligned_distance(query_words, &known_words),
        aligned_distance(key_words, &known_key),
    ]
    .into_iter()
    .flatten()
    .min()
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
    let query_key = word_key(&query);
    let query_words: Vec<&str> = query.split(' ').collect();
    let key_words: Vec<&str> = query_key.split(' ').collect();
    let tiers: [(ResolveTier, TierTest<'_>); 5] = [
        (
            ResolveTier::Exact,
            Box::new(|known| (known.name == name).then_some(0)),
        ),
        (
            ResolveTier::Name,
            Box::new(|known| (known.key == query_key).then_some(0)),
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
            ResolveTier::Tokens,
            Box::new(|known| {
                let known_words: Vec<&str> = known.normalized.split(' ').collect();
                words_match(&query_words, &known_words).then_some(0)
            }),
        ),
        (
            ResolveTier::Fuzzy,
            Box::new(|known| {
                if query_chars < MIN_FUZZY_CHARS {
                    return None;
                }
                fuzzy_distance(&query_words, &key_words, known)
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
            "no active claims with a usable name in the visible scopes; check --scope, or treat the name as new"
                .to_owned()
        }
        ResolveStatus::None => format!(
            "no match among {considered_subjects} subjects and {considered_aliases} aliases; treat {name:?} as new only if the source makes that clear, otherwise ask"
        ),
    };
    let matched = candidates.len();
    candidates.truncate(MAX_CANDIDATES);
    Ok(Resolution {
        query: name.to_owned(),
        status,
        tier,
        shown: candidates.len(),
        matched,
        candidates,
        considered_subjects,
        considered_aliases,
        next_action,
    })
}
