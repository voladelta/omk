# Search

## Search across scopes

Search treats your input as a literal phrase. Punctuation and hyphens are safe:

```sh
omk recall search --scope project:omk --query 'settlement ETH-only'
```

Search returns a page:

```json
{"hits": [...], "shown": 20, "matched": 57, "nextAction": "37 more matches not shown; raise --limit (now 20) or narrow the query"}
```

`matched` counts every hit the filters allow, not just the page. When `matched` is 0, the page also carries `searchable`: the records the scope and filters allow before the query runs. Together they separate three cases an empty or full page used to hide: more hits past `--limit` (`matched > shown`), a real miss (`matched: 0`, `searchable > 0`), and a scope or filter with nothing in it (`searchable: 0`). `nextAction` names the case.

Each hit has a preview of at most 512 Unicode characters in `text`, with `claimStatus`, `subject` and `predicate` for claim hits. Use exact recall to read full evidence. The default includes historical claims and matches a literal phrase. `--terms` matches all whitespace-separated literal terms; `--fts-query` enables raw FTS5 syntax and conflicts with `--terms`. `--current-only` filters claims to active status while retaining matching events and observations. Queries allow at most 4,096 UTF-8 bytes and 64 whitespace terms.

Use `--fts-query` only when you need SQLite FTS5 syntax.

Search includes the target scope, its ancestors and its descendants. For how [context](context.md#scope-inheritance) differs, see scope inheritance.

## Narrow a search

- `--type claim|observation|event` returns only those record types; repeat it for several
- `--field subject|predicate|value` matches only that part of a claim, so a search for a name in alias values does not match every claim whose subject holds the name; only claims have these fields, so `searchable` then counts claims alone; the default `text` field matches the whole record
- `--include-commands` adds the `memory-command` events that every direct claim write records; search leaves them out by default because each repeats its claim

## How filtering works

Record type and command filters are tokens in the full-text index, so they narrow the match itself rather than filtering its rows afterwards. Scope depends on how many rows the query matches, which search counts from the index first, stopping one row past 1,000. A query matching at most 1,000 rows reads them all once, checks each row's scope ID and ranks them, and the rows read give `matched`. A broader query filters scope with index tokens, using the smaller of two lists: the visible scopes, or the scopes they leave out. A root scope that sees everything needs no scope filter. When both lists exceed 64 scopes, a broad query checks each matching row's scope ID instead, which is slower. Every returned hit is also checked against the exact visible scope IDs. Raw `--fts-query` input must have balanced parentheses and closed strings, so it cannot step outside the filters.

## Ranking

Results sort by BM25 multiplied by a record boost, then record type and record ID. Active claims count double, pending and disputed claims and observations count 1.25, events count 1, and superseded, rejected and expired claims count 0.5. `rank` reports that product; lower sorts first. A broad query reads events and observations as separate top-`--limit` lists, since one boost applies to each, and streams claims in BM25 order until no later claim could reach the page even at the highest boost, so only those rows are decoded. When several events or observations tie on BM25 at the edge of the page, the index order picks which ones appear. A broad query counts `matched` from the index, and `searchable` is counted from the index; under `--current-only`, the inactive claims it leaves out are counted from the claims table, since each claim has one index row in its own scope.

## Measure search

`examples/search_bench.rs` drives `omk` binaries only through the CLI, so one run compares versions on an identical synthetic world: 120 people with recorded aliases and corrected facts, some with accented surnames, plus 4,000 chat events that mention them on a Zipf curve. Each world yields about 600 name mentions in 16 categories, including reordered names, initials, dropped accents, typos, titles, nicknames, and new people who share a surname or initial with someone recorded. Run `cargo run --release --example search_bench -- OUT_DIR BIN [BIN...] [--latency] [--seed N]`; each run writes every miss to `OUT_DIR`.

These results compare OMK 0.7 with OMK 0.8 over six seeds. The resolver was tuned while reading misses from seeds 0–2; seeds 3–5 were run once after the code was frozen.

| name resolution | OMK 0.7, skill search procedure | OMK 0.7, plus word-by-word retries | OMK 0.8, `recall resolve` |
|---|---|---|---|
| correct, seeds 0–2 | 57.2–57.7% | 44.9–45.7% | 99.3–99.5% |
| correct, seeds 3–5 | 57.1–58.1% | 45.2–46.5% | 99.2–99.8% |
| new duplicate entity | 41.9–42.9% | 0.0–0.3% | 0.2–0.7% |
| silent wrong merge | 0% | 0.0–0.3% | 0% |
| needless question | 0% | 53.5–54.9% | 0.0–0.2% |
| correct, but `probable`, so the agent confirms first | 0% | 0% | 32.7–34.2% |
| CLI calls and tokens read per mention | 4.5–4.7 calls, 1,565–1,654 tokens | 16.5–17.8 calls, 5,872–6,288 tokens | 1 call, 97–101 tokens |

The skill procedure misses every typo, title, initial, reordered name, dropped accent and nickname, and each miss would create a duplicate entity. Retrying word by word removes the duplicates but turns new people into false ambiguities. `recall resolve` merged no new person into an existing subject in any seed, but a third of its correct answers are `probable` and need a confirmation. Its nickname matching is only as good as its table: prefix nicknames such as `Kate` matched 35 of 35, nicknames in the table matched 26 of 33 (missing `Gabi` and `Tom`), and the five irregular nicknames deliberately left out of the table matched 0 of 12.

| other measures | OMK 0.7 | OMK 0.8 |
|---|---|---|
| current claim first for `<name> <predicate>`, default flags | 91.5–93.5%; a replaced claim ranked above it 31–41 times | 100% |
| the same with `--current-only` | 100% | 100% |
| tokens read per fact question | 268–277 | 186–197; 70 with `--current-only --type claim` |
| median search over 20,000 events, query matching every event: thread, project, root scope | 16.9, 17.6, 37.1 ms | 8.9, 14.2, 24.5 ms |
| the same, query matching one event | 4.2, 4.3, 4.6 ms | 4.3, 4.2, 4.6 ms |

Tokens use OMK's estimate of four characters each, and wall times include process start. The agents are scripted procedures, not models, and the world is generated, so the rates depend on this workload.
