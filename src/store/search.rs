use super::*;

/// One search row. `text` holds the whole record; claims also fill
/// `subject`, `predicate` and `value` so a search can skip a record's own
/// subject when it looks for a name in values.
pub(super) struct FtsRow<'a> {
    pub record_type: RecordType,
    pub record_id: &'a str,
    pub scope_id: &'a str,
    pub kind: &'a str,
    pub text: &'a str,
    pub subject: &'a str,
    pub predicate: &'a str,
    pub value: &'a str,
}

/// Filter tokens live in the `facet` column, so scope, record type and kind
/// narrow the FTS match itself instead of filtering its rows afterwards.
/// unicode61 splits on punctuation, so each token is a prefix plus
/// alphanumerics only.
pub(super) fn facet_type_token(record_type: RecordType) -> String {
    format!("rt{}", record_type.as_str())
}

pub(super) fn facet_kind_token(kind: &str) -> String {
    let kind: String = kind.chars().filter(char::is_ascii_alphanumeric).collect();
    format!("rk{kind}")
}

/// Scope IDs are free text, so the token is a hash prefix. Search still
/// checks the exact scope ID, so a collision can only widen the FTS match.
pub(super) fn facet_scope_token(scope_id: &str) -> String {
    let digest = Sha256::digest(scope_id.as_bytes());
    let hex: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("rs{hex}")
}

pub(super) fn insert_fts(conn: &Connection, row: &FtsRow<'_>) -> Result<()> {
    let facet = format!(
        "{} {} {}",
        facet_type_token(row.record_type),
        facet_kind_token(row.kind),
        facet_scope_token(row.scope_id)
    );
    conn.execute(
        "INSERT INTO memory_fts(record_type,record_id,scope_id,text,subject,predicate,value,facet)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            row.record_type.as_str(),
            row.record_id,
            row.scope_id,
            row.text,
            row.subject,
            row.predicate,
            row.value,
            facet
        ],
    )?;
    let (record_type, record_id) = (row.record_type, row.record_id);
    // record_id is UNINDEXED, so remember the rowid for deletes by key.
    conn.execute(
        "INSERT INTO memory_fts_refs(record_type,record_id,fts_rowid) VALUES (?1,?2,?3)",
        params![record_type.as_str(), record_id, conn.last_insert_rowid()],
    )?;
    Ok(())
}

pub(super) fn delete_fts(
    conn: &Connection,
    record_type: RecordType,
    record_id: &str,
) -> Result<()> {
    let rowid: Option<i64> = conn
        .query_row(
            "SELECT fts_rowid FROM memory_fts_refs WHERE record_type=?1 AND record_id=?2",
            params![record_type.as_str(), record_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(rowid) = rowid {
        conn.execute("DELETE FROM memory_fts WHERE rowid=?1", [rowid])?;
        conn.execute(
            "DELETE FROM memory_fts_refs WHERE record_type=?1 AND record_id=?2",
            params![record_type.as_str(), record_id],
        )?;
    }
    Ok(())
}

/// Most scope tokens one search MATCH may carry. Each token adds an OR branch
/// that FTS5 must merge, so large lists cost more than they save.
const MAX_SCOPE_FACETS: usize = 64;
/// Highest record boost; bounds how far a later rank row can climb.
const MAX_BOOST: f64 = 2.0;
/// Queries matching at most this many rows before scope filtering check each
/// row's scope exactly instead of merging scope tokens.
const SELECTIVE_ROWS: usize = 1000;

/// How a search limits scope. Tokens keep the match inside the index;
/// `Exact` reads each matching row's scope, which costs a content lookup.
enum ScopeFilter {
    Everything,
    Include(Vec<String>),
    Exclude(Vec<String>),
    Exact,
}

fn scope_filter(conn: &Connection, scope_ids: &[String]) -> Result<ScopeFilter> {
    if scope_ids.len() <= MAX_SCOPE_FACETS {
        return Ok(ScopeFilter::Include(scope_ids.to_vec()));
    }
    let visible: HashSet<&str> = scope_ids.iter().map(String::as_str).collect();
    let mut statement = conn.prepare("SELECT id FROM memory_scopes")?;
    let hidden: Vec<String> = collect_rows(statement.query_map([], |row| row.get(0))?)?
        .into_iter()
        .filter(|id: &String| !visible.contains(id.as_str()))
        .collect();
    Ok(if hidden.is_empty() {
        ScopeFilter::Everything
    } else if hidden.len() <= MAX_SCOPE_FACETS {
        ScopeFilter::Exclude(hidden)
    } else {
        ScopeFilter::Exact
    })
}

fn facet_tokens(ids: &[String]) -> String {
    ids.iter()
        .map(|id| facet_scope_token(id))
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Wrap a query part with the record type, scope and command-echo filters,
/// all as facet tokens.
fn facet_match(query: &str, scope: &ScopeFilter, options: &SearchOptions) -> String {
    let SearchTypes {
        claims,
        observations,
        events,
    } = options.types;
    let all = !(claims || observations || events);
    let mut expression = format!("({query})");
    if !all {
        let types: Vec<String> = [
            (RecordType::Claim, claims),
            (RecordType::Observation, observations),
            (RecordType::Event, events),
        ]
        .into_iter()
        .filter(|(_, wanted)| *wanted)
        .map(|(record_type, _)| facet_type_token(record_type))
        .collect();
        expression = format!("{expression} AND (facet : ({}))", types.join(" OR "));
    }
    if let ScopeFilter::Include(ids) = scope {
        expression = format!("{expression} AND (facet : ({}))", facet_tokens(ids));
    }
    let mut excluded = Vec::new();
    if let ScopeFilter::Exclude(ids) = scope {
        excluded.push(facet_tokens(ids));
    }
    if !options.include_commands && (all || events) {
        excluded.push(facet_kind_token("memory-command"));
    }
    if !excluded.is_empty() {
        expression = format!("({expression}) NOT (facet : ({}))", excluded.join(" OR "));
    }
    expression
}

/// Score multiplier per record: current claims first, then pending state and
/// observations, then raw events, with replaced claims last.
fn search_boost(record_type: RecordType, status: Option<&ClaimStatus>) -> f64 {
    match (record_type, status) {
        (RecordType::Claim, Some(ClaimStatus::Active)) => MAX_BOOST,
        (RecordType::Claim, Some(ClaimStatus::Pending | ClaimStatus::Disputed)) => 1.25,
        (RecordType::Claim, _) => 0.5,
        (RecordType::Observation, _) => 1.25,
        (RecordType::Event, _) => 1.0,
    }
}

pub(super) fn search_fts(
    conn: &Connection,
    scope_ids: &[String],
    query: &str,
    limit: usize,
    options: SearchOptions,
    count: bool,
) -> Result<SearchPage> {
    if scope_ids.is_empty() {
        return Ok(SearchPage::new(Vec::new(), 0, Some(0), limit));
    }
    let column = match options.field {
        SearchField::Text => "text",
        SearchField::Subject => "subject",
        SearchField::Predicate => "predicate",
        SearchField::Value => "value",
    };
    let scope = scope_filter(conn, scope_ids)?;
    let visible: HashSet<&str> = scope_ids.iter().map(String::as_str).collect();
    // The query sits inside a column filter; bounded_fts_query keeps its
    // parentheses balanced so it cannot escape into the facet filters.
    let column_query = format!("{{{column}}} : ({query})");
    // FTS5 reports bad query syntax as a plain SQLITE_ERROR while stepping;
    // busy and other failures keep their own classification.
    let invalid = |error: rusqlite::Error| -> anyhow::Error {
        match &error {
            rusqlite::Error::SqliteFailure(failure, _) if failure.extended_code == 1 => {
                KernelError::invalid_search_query(format!(
                    "running SQLite FTS query {query:?}: {error}"
                ))
                .into()
            }
            _ => error.into(),
        }
    };
    let exact_sql = format!(
        " AND memory_fts.scope_id IN ({})",
        placeholders(scope_ids.len())
    );
    let scope_sql = |filter: &ScopeFilter| {
        if matches!(filter, ScopeFilter::Exact) {
            exact_sql.as_str()
        } else {
            ""
        }
    };
    let values = |filter: &ScopeFilter, expression: &str| {
        let mut values = vec![rusqlite::types::Value::Text(expression.to_owned())];
        if matches!(filter, ScopeFilter::Exact) {
            values.extend(scope_ids.iter().cloned().map(rusqlite::types::Value::Text));
        }
        values
    };
    let raw_count = |filter: &ScopeFilter, expression: &str| -> Result<i64> {
        conn.query_row(
            &format!(
                "SELECT count(*) FROM memory_fts WHERE memory_fts MATCH ?{}",
                scope_sql(filter)
            ),
            rusqlite::params_from_iter(values(filter, expression)),
            |row| row.get(0),
        )
        .map_err(invalid)
    };
    let count_of = |filter: &ScopeFilter, expression: &str| -> Result<usize> {
        let exact_scope = scope_sql(filter);
        let mut total = raw_count(filter, expression)?;
        // Under --current-only, claims that are not active are counted
        // separately and subtracted.
        if options.current_only {
            let claims = format!(
                "({expression}) AND (facet : {})",
                facet_type_token(RecordType::Claim)
            );
            let inactive: i64 = conn
                .query_row(
                    &format!(
                        "SELECT count(*) FROM memory_fts JOIN claims ON claims.id=memory_fts.record_id
                         WHERE memory_fts MATCH ?{exact_scope} AND claims.status!='active'"
                    ),
                    rusqlite::params_from_iter(values(filter, &claims)),
                    |row| row.get(0),
                )
                .map_err(invalid)?;
            total -= inactive;
        }
        Ok(total as usize)
    };
    // A selective query, counted from the index without scope, reads all
    // of its rows once: the exact scope check costs one content read per
    // row, and the read rows give `matched` without another count. A broad
    // query filters scope with tokens, whose cost follows the visible scopes'
    // postings instead of the matches. The count stops one row past the
    // threshold, so a broad query does not walk all of its matches here.
    let unscoped = facet_match(&column_query, &ScopeFilter::Everything, &options);
    let probe: i64 = conn
        .query_row(
            "SELECT count(*) FROM (SELECT 1 FROM memory_fts WHERE memory_fts MATCH ?1 LIMIT ?2)",
            params![unscoped, SELECTIVE_ROWS as i64 + 1],
            |row| row.get(0),
        )
        .map_err(invalid)?;
    let selective = probe <= SELECTIVE_ROWS as i64;
    let match_scope = if selective && !matches!(scope, ScopeFilter::Everything) {
        &ScopeFilter::Exact
    } else {
        &scope
    };
    let full = facet_match(&column_query, match_scope, &options);
    let exact_scope = scope_sql(match_scope);

    // A broad query reads each boost class separately through FTS5's ORDER BY
    // rank fast path. Events and observations share one boost per class, so
    // their top `limit` rows by rank are their top rows after boosting too.
    // Claim boosts depend on status, so claims stream last and stop once a
    // row's best possible boosted score cannot reach the page.
    let SearchTypes {
        claims,
        observations,
        events,
    } = options.types;
    let all = !(claims || observations || events);
    let plans: Vec<Option<RecordType>> = if selective {
        vec![None]
    } else {
        [
            (RecordType::Event, events),
            (RecordType::Observation, observations),
            (RecordType::Claim, claims),
        ]
        .into_iter()
        .filter(|(_, wanted)| all || *wanted)
        .map(|(class, _)| Some(class))
        .collect()
    };
    let mut hits: Vec<SearchHit> = Vec::new();
    let mut qualified = 0;
    for class in plans {
        let limited = matches!(class, Some(RecordType::Event | RecordType::Observation));
        let bounded = class == Some(RecordType::Claim);
        let expression = match class {
            Some(class) => format!("({full}) AND (facet : {})", facet_type_token(class)),
            None => full.clone(),
        };
        let mut statement = conn.prepare(&format!(
            "SELECT record_type,record_id,memory_fts.scope_id,substr(text,1,512),memory_fts.rank,
                    claims.status,claims.subject,claims.predicate
             FROM memory_fts LEFT JOIN claims ON record_type='claim' AND claims.id=record_id
             WHERE memory_fts MATCH ?{exact_scope}
               AND memory_fts.rank MATCH 'bm25(0,0,0,1,1,1,1,0)'
             ORDER BY memory_fts.rank{}",
            if limited { " LIMIT ?" } else { "" }
        ))?;
        let mut class_values = values(match_scope, &expression);
        if limited {
            class_values.push(rusqlite::types::Value::Integer(limit as i64));
        }
        let mut rows = statement
            .query(rusqlite::params_from_iter(class_values))
            .map_err(invalid)?;
        while let Some(row) = rows.next().map_err(invalid)? {
            let rank: f64 = row.get(4)?;
            // hits stays sorted, so its last entry is the page's worst.
            if bounded
                && hits.len() >= limit
                && hits
                    .last()
                    .is_some_and(|worst| rank * MAX_BOOST > worst.rank)
            {
                break;
            }
            let record_type: RecordType = parse_enum(&row.get::<_, String>(0)?)?;
            let scope_id: String = row.get(2)?;
            if !visible.contains(scope_id.as_str()) {
                continue;
            }
            let id: String = row.get(1)?;
            // The join fills these for claims only.
            let status = row
                .get::<_, Option<String>>(5)?
                .map(|status| parse_enum::<ClaimStatus>(&status))
                .transpose()?;
            let subject: Option<String> = row.get(6)?;
            let predicate: Option<String> = row.get(7)?;
            if options.current_only && status.as_ref().is_some_and(|s| *s != ClaimStatus::Active) {
                continue;
            }
            qualified += 1;
            let hit = SearchHit {
                rank: rank * search_boost(record_type, status.as_ref()),
                record_type,
                id,
                scope_id,
                text: row.get(3)?,
                claim_status: status,
                subject,
                predicate,
            };
            let position = hits.partition_point(|other| {
                other
                    .rank
                    .total_cmp(&hit.rank)
                    .then_with(|| other.record_type.cmp(&hit.record_type))
                    .then_with(|| other.id.cmp(&hit.id))
                    .is_lt()
            });
            if position < limit {
                hits.insert(position, hit);
                hits.truncate(limit);
            }
        }
    }
    if !count {
        let shown = hits.len();
        return Ok(SearchPage::new(hits, shown, None, limit));
    }
    let matched = if selective {
        qualified
    } else {
        count_of(match_scope, &full)?.max(hits.len())
    };
    // Searchable only matters for an empty page, so it is counted only then.
    let searchable = if matched == 0 {
        // Only claims fill the subject, predicate and value fields, so a
        // field search has only claims to search.
        let record_types: &[RecordType] = match options.field {
            SearchField::Text => &[
                RecordType::Claim,
                RecordType::Observation,
                RecordType::Event,
            ],
            _ => &[RecordType::Claim],
        };
        let any_record = format!(
            "facet : ({})",
            record_types
                .iter()
                .map(|record_type| facet_type_token(*record_type))
                .collect::<Vec<_>>()
                .join(" OR ")
        );
        let mut total = raw_count(&scope, &facet_match(&any_record, &scope, &options))?;
        // Every claim has one index row in its own scope, so the claims
        // table counts the inactive ones without reading index rows.
        if options.current_only && (all || claims) {
            let visible: &[String] = match scope {
                ScopeFilter::Everything => &[],
                _ => scope_ids,
            };
            let mut sql = "SELECT count(*) FROM claims WHERE status!='active'".to_owned();
            if !visible.is_empty() {
                sql.push_str(&format!(
                    " AND scope_id IN ({})",
                    placeholders(visible.len())
                ));
            }
            let inactive: i64 =
                conn.query_row(&sql, rusqlite::params_from_iter(visible), |row| row.get(0))?;
            total -= inactive;
        }
        Some(total as usize)
    } else {
        None
    };
    Ok(SearchPage::new(hits, matched, searchable, limit))
}

pub(super) fn bounded_fts_query(query: &str, mode: SearchMode) -> Result<String> {
    ensure!(
        !query.trim().is_empty() && query.len() <= 4096 && query.split_whitespace().count() <= 64,
        KernelError::invalid_search_query(
            "search query must contain 1 to 4096 bytes and at most 64 whitespace terms"
        )
    );
    Ok(match mode {
        SearchMode::Phrase => literal_fts_query(query),
        SearchMode::Terms => query
            .split_whitespace()
            .map(literal_fts_query)
            .collect::<Vec<_>>()
            .join(" AND "),
        SearchMode::Advanced => {
            ensure!(
                balanced_outside_strings(query),
                KernelError::invalid_search_query(
                    "FTS5 query must have balanced parentheses and closed strings"
                )
            );
            query.to_owned()
        }
    })
}

/// Search wraps a raw query in parentheses inside a column filter, so a raw
/// query must not close that group early.
fn balanced_outside_strings(query: &str) -> bool {
    let mut depth = 0usize;
    let mut in_string = false;
    for c in query.chars() {
        match c {
            '"' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => match depth.checked_sub(1) {
                Some(next) => depth = next,
                None => return false,
            },
            _ => {}
        }
    }
    depth == 0 && !in_string
}
