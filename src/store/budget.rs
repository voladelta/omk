use super::*;

// One extra character covers the array separator; rounding per item is conservative.
pub(super) fn serialized_item_tokens(value: &impl Serialize) -> i64 {
    estimate_tokens(&(serde_json::to_string(value).expect("model value serializes") + ","))
}

pub(super) fn event_hint_extra(event: &MemoryEvent) -> i64 {
    (event.token_count - estimate_event_tokens(&event.content, &event.metadata)).max(0)
}

pub(super) fn view_hint_extra(view: &MemoryView) -> i64 {
    (view.token_count - estimate_tokens(&view.content)).max(0)
}

pub(super) fn sort_claims_by_scope(claims: &mut [Claim], scope_order: &[String]) {
    claims.sort_by_key(|claim| {
        scope_order
            .iter()
            .position(|scope_id| scope_id == &claim.scope_id)
            .unwrap_or(usize::MAX)
    });
}

/// Split active claims into those in force and those shadowed by a deeper
/// scope. A `single` claim loses to any claim for the same logical key in a
/// descendant scope; `set` claims are unioned and never shadowed.
pub(super) fn split_shadowed_claims(
    claims: Vec<Claim>,
    scope_order: &[String],
) -> (Vec<Claim>, Vec<Claim>) {
    let depth = |claim: &Claim| {
        scope_order
            .iter()
            .position(|scope_id| scope_id == &claim.scope_id)
            .unwrap_or(0)
    };
    let key = |claim: &Claim| (claim.subject.clone(), claim.predicate.clone());
    let mut deepest: HashMap<(String, String), usize> = HashMap::new();
    for claim in &claims {
        let entry = deepest.entry(key(claim)).or_insert(0);
        *entry = (*entry).max(depth(claim));
    }
    claims.into_iter().partition(|claim| {
        claim.cardinality != ClaimCardinality::Single || depth(claim) >= deepest[&key(claim)]
    })
}

/// Active claims may use at most this share of a context or plan budget.
pub(super) const CLAIM_BUDGET_PERCENT: i64 = 50;
/// User-scope claims are pinned first, up to this share of the claim budget.
pub(super) const USER_CLAIM_PIN_PERCENT: i64 = 50;

pub(super) fn percent_of(total: i64, percent: i64) -> i64 {
    (i128::from(total) * i128::from(percent) / 100) as i64
}

/// Choose the active claims that fit `budget` tokens. User-scope claims go
/// first, newest update first, up to the pinned share; every other claim and
/// any user claim left over then fill the rest, newest update first. Each
/// pass ends at a landmark when it overflows (see `CLAIM_LANDMARK_SPACING`).
/// Both returned lists keep the input order.
pub(super) fn budget_claims(
    conn: &Connection,
    claims: Vec<Claim>,
    budget: i64,
    cost: impl Fn(&Claim) -> i64,
) -> Result<(Vec<Claim>, Vec<Claim>)> {
    let scope_ids: Vec<&str> = claims.iter().map(|claim| claim.scope_id.as_str()).collect();
    let user_scopes: HashSet<String> = {
        let mut statement = conn.prepare(
            "SELECT id FROM memory_scopes WHERE kind='user' AND id IN (SELECT value FROM json_each(?1))",
        )?;
        collect_rows(statement.query_map([serde_json::to_string(&scope_ids)?], |row| row.get(0))?)?
            .into_iter()
            .collect()
    };
    let costs: Vec<i64> = claims.iter().map(&cost).collect();
    let mut newest_first: Vec<usize> = (0..claims.len()).collect();
    newest_first.sort_by(|&left, &right| {
        claims[right]
            .updated_at
            .cmp(&claims[left].updated_at)
            .then_with(|| claims[left].id.cmp(&claims[right].id))
    });
    let mut kept = vec![false; claims.len()];
    let pin_budget = percent_of(budget, USER_CLAIM_PIN_PERCENT);
    let pinned = take_newest(
        &claims,
        &newest_first,
        &costs,
        &mut kept,
        pin_budget,
        |claim| user_scopes.contains(&claim.scope_id),
    );
    take_newest(
        &claims,
        &newest_first,
        &costs,
        &mut kept,
        budget - pinned,
        |_| true,
    );
    let (kept_claims, omitted): (Vec<_>, Vec<_>) =
        claims.into_iter().zip(kept).partition(|(_, kept)| *kept);
    Ok((
        kept_claims.into_iter().map(|(claim, _)| claim).collect(),
        omitted.into_iter().map(|(claim, _)| claim).collect(),
    ))
}

/// About one claim in this many is a landmark (by a hash of its id), and an
/// over-budget claim selection ends only at one. The cutoff then holds for
/// several additions instead of moving with each, so consecutive contexts keep
/// a longer identical prefix for prompt caches. Simulated use measured 30%
/// fewer changed prefix characters per call for 3% fewer claims at 16,000
/// tokens; larger spacings save more and keep fewer.
const CLAIM_LANDMARK_SPACING: u64 = 4;

fn is_landmark(id: &str) -> bool {
    // FNV-1a: stable across builds and platforms, unlike the std hasher.
    let hash = id.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    hash % CLAIM_LANDMARK_SPACING == 0
}

/// How many of the newest events, given newest first, an overflowing raw tail
/// keeps. The tail starts at the event in the older half of the run whose
/// sequence number has the most trailing zero bits (the oldest on a tie), as
/// in a binary counter. That start holds while new events arrive, then jumps
/// forward by about half the run, so the tail grows from about half the budget
/// to all of it and drops back at once. Between jumps consecutive contexts
/// share the whole tail but the newest event, which prompt caches can reuse.
pub(super) fn aligned_tail_len(newest_first: &[i64]) -> usize {
    let half = newest_first.len() / 2;
    newest_first
        .iter()
        .enumerate()
        .skip(half)
        .max_by_key(|&(index, sequence)| (sequence.trailing_zeros(), index))
        .map_or(0, |(index, _)| index + 1)
}

/// Keep the newest eligible claims that fit `budget`, as one run in
/// `newest_first` order. When some do not fit, the run ends at its oldest
/// landmark, or where it stopped fitting if it holds none. Returns the
/// tokens used.
fn take_newest(
    claims: &[Claim],
    newest_first: &[usize],
    costs: &[i64],
    kept: &mut [bool],
    budget: i64,
    eligible: impl Fn(&Claim) -> bool,
) -> i64 {
    let candidates: Vec<usize> = newest_first
        .iter()
        .copied()
        .filter(|&index| !kept[index] && eligible(&claims[index]))
        .collect();
    let mut used = 0;
    let mut fit = 0;
    for &index in &candidates {
        if costs[index] > budget - used {
            break;
        }
        used += costs[index];
        fit += 1;
    }
    if fit < candidates.len()
        && let Some(last) = (0..fit)
            .rev()
            .find(|&n| is_landmark(&claims[candidates[n]].id))
    {
        fit = last + 1;
    }
    candidates[..fit]
        .iter()
        .map(|&index| {
            kept[index] = true;
            costs[index]
        })
        .sum()
}
