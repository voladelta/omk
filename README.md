# OMK

OMK gives local agents durable, source-backed memory through a JSON command-line interface (CLI).

It stores 4 types of record:

- events record what happened
- observations record source-backed interpretations
- claims record proposed or accepted state
- views provide replaceable context

SQLite is the only runtime dependency. OMK does not run or schedule models. Your agent requests a redacted observation plan, produces strict JSON and commits the result atomically.

## Build and start OMK

Build the release binary:

```sh
cargo build --release
./target/release/omk --help
```

OMK stores data in `.omk/memory.db` by default. Use `OMK_DB` or `--db` to choose another path.

Run `omk` without a command to show help. Use `omk help <command>` or `<command> --help` for command help.

## Read JSON output

Every successful data command writes one compact JSON value to standard output. Help and version commands write plain text.

Read commands return data directly. `init` and each idempotent write return an operation envelope:

```json
{
  "data": { "id": "..." },
  "operation": { "replayed": false }
}
```

The operation fields tell you how to recover:

- `retryable` means you can retry the command without changing it
- `sameKeyReusable` means validation failed before OMK recorded the operation
- `nextAction` tells you what to do before you retry

An identical retry returns the original data with `replayed: true`. If another process holds the database past the five second busy timeout, OMK returns code `busy` with `retryable` and `sameKeyReusable` both true. Retry the identical request with the same key. OMK rejects a reused key if any input changes.

`do-not-store` is the exception. It replays requests when only the payload, metadata or token hint changes. OMK keeps no fingerprint derived from that data.

Saved results are kept for 30 days. After that, each write compacts a small batch of expired operations down to their key, operation name and request hash. A compacted key still rejects changed input with `idempotency_conflict`. An identical retry returns `operation_expired` instead of running the operation again: it already committed, so inspect its records instead of retrying. A purge still tombstones compacted operations that mention a purged record.

`observe plan` saves only its run ID. A replay rebuilds the plan: it keeps the same run and exact event range, but active claims and the previous continuation reflect the store at replay time. Storing whole plans made them most of the operation log, because each plan copied every active claim.

Failures are JSON on standard error:

```json
{
  "error": {
    "code": "budget_exceeded",
    "message": "context budget too small: minimumRequiredTokens=12 for the context structure",
    "retryable": false,
    "sameKeyReusable": true,
    "nextAction": "increase the token budget and retry with the same key"
  }
}
```

## Set up agent memory

Create a scope tree before you add evidence:

```sh
omk init
omk scope add --id user:me --kind user --idempotency-key scope-user-me
omk scope add --id project:omk --kind project --parent user:me --idempotency-key scope-project-omk
omk scope add --id thread:build --kind thread --parent project:omk --idempotency-key scope-thread-build
```

Add an event:

```sh
omk event append \
  --scope thread:build \
  --stream codex-thread-1 \
  --kind user-message \
  --content 'Implement the memory kernel in Rust' \
  --idempotency-key codex-thread-1-message-1
```

Every write needs a stable, globally unique idempotency key. Reuse a key only when you retry an identical command.

## Add secret evidence safely

Pass secret content through standard input or `--content-file`. Do not use `--content`, because the value could enter shell history or process listings.

```sh
printf '%s' 'credential material' | omk event append \
  --scope thread:build \
  --stream codex-thread-1 \
  --kind tool-result \
  --sensitivity secret \
  --idempotency-key codex-thread-1-secret-1
```

Use `--metadata-file` for secret metadata. One command cannot read content and metadata from standard input. Put one value in a file.

Secret append and replay results contain redacted content and empty metadata. Read commands also redact secrets by default.

Pass the intended `--scope` and `--reveal-secret` only when the agent needs exact local evidence. Use `do-not-store` when OMK must not save the content or metadata.

## Observe new events

Plan a batch of events for an external observer:

```sh
omk observe plan \
  --scope thread:build \
  --stream codex-thread-1 \
  --model codex \
  --idempotency-key codex-thread-1-observe-plan-1 \
  > observation-plan.json

jq -r '.data.runId' observation-plan.json
jq -r '.data.events[].id' observation-plan.json
jq '.data' observation-plan.json > observer-input.json
```

The `jq` commands are optional examples. Agents can parse the JSON directly.

A ready plan provides stable paths for the run and source event IDs:

```json
{
  "data": {
    "status": "ready",
    "runId": "...",
    "events": [{"id": "..."}],
    "nextAction": "produce a strict ObserverResult for run ... and commit it"
  },
  "operation": {"replayed": false}
}
```

When there are no new events, OMK returns a caught-up result without a run:

```json
{
  "data": {
    "status": "caught-up",
    "scopeId": "thread:build",
    "streamId": "codex-thread-1",
    "observedThroughSequence": 42,
    "nextAction": "append new evidence or wait for new events; use a new idempotency key for the next plan"
  },
  "operation": {"replayed": false}
}
```

Apply the [observer prompt and output contract](prompts/observer.v1.md) to the returned `scope`, `events`, `activeClaims`, and `previousContinuation` fields. Keep `runId` for commit routing. You can also get the full output shape from `omk observe commit --help`.

The result must include `observations`, `claims`, `continuation` and `ambiguities`. Set `emptyReason` when all sections are empty. OMK then keeps the existing continuation view.

For a non-empty result, `continuation` replaces the previous snapshot. Include all state that still applies from `previousContinuation`.

Commit the result, then review every pending observer claim. The response lists claim IDs in `nextRequiredAction`. Confirm or reject each one:

```sh
omk observe commit \
  --run RUN_ID \
  --input observer-result.json \
  --idempotency-key codex-thread-1-observe-commit-1

omk claim confirm --id CLAIM_ID --idempotency-key codex-thread-1-confirm-1
# or
omk claim reject --id CLAIM_ID --idempotency-key codex-thread-1-reject-1
```

`claim reconcile` can classify pending state as duplicate or disputed, but it never activates a claim unless a trusted ingestion path has already given it `trusted-source` authority. OMK has no such path today, so every activation needs an explicit claim command. It never promotes observer-origin claims.

## Recover observation work

Inspect runs and stream progress:

```sh
omk observe get --scope thread:build --run RUN_ID
omk observe list --scope thread:build --stream codex-thread-1 --status pending
omk observe status --scope thread:build --stream codex-thread-1
omk observe fail --run RUN_ID --reason model-timeout --idempotency-key observe-failure-1
```

A failed run does not move the cursor. A new plan retries the same range.

`observe list` returns runs from the anchor scope, its ancestors and its descendants, subject to the stream and status filters. Unrelated runs are excluded before their stored fields are decoded.

OMK allows competing plans, but only one can commit. It returns a structured recovery error for stale runs.

Each run records `cursorAtPlan`. This lets OMK recover across privacy-purged sequence gaps without reusing sequence numbers.

Run inspection also returns `sourceIntegrity`. A committed run changes from `intact` to `privacy-purged` when a purge removes its sources. OMK removes dependent records and returns a recovery action. Affected pending runs become stale.

## Build bounded context

Set a token estimate budget when you build context:

```sh
omk context \
  --scope thread:build \
  --stream codex-thread-1 \
  --max-tokens 16000 \
  --recent-raw-tokens 6000
```

Add `--compact` to emit the direct model payload with the same top-level sections and a budget calculated from compact records. The default command still returns the complete context bundle with diagnostics. Compact records keep claim status, modality, authority, confidence, scope and value; events keep their kind, scope, time, content and sensitivity. Record UUIDs remain intact for `omk event get`, `omk recall explain-claim`, and `omk recall observation`. Routine storage fields such as hashes, token counts, observer model and creation timestamps are omitted. Compact output has no diagnostics field; use the default command when you need omitted-item details. The library equivalents are `compose_compact_context(...)` and `ContextBundle::compact_model_payload()`.

Active claims may use at most half of the budget. OMK fills that half in two passes. First it pins user-scope claims, newest update first, up to half of the claim share. Then it fills the rest with any remaining claims, newest update first. Each claim that does not fit appears in `diagnostics.omittedItems` with reason `active claim budget`. It stays active and is still returned by `recall search --current-only` and exact recall. Context fails with `budget_exceeded` and `minimumRequiredTokens` only when the budget cannot hold the empty context structure.

Simulated agent use showed why: active claims passed 16,000 tokens after about 100 days, so a context that required every claim would fail every day after that. With a 16,000-token budget, this ranking never failed over two simulated years and kept 73–83% of the claims an agent needed when needs favor recent claims and user preferences, and 57–67% when they favor durable facts. Ranking by recency alone scored 10–14 points lower. An unlimited user pin did no better than recency alone in the second year at this budget, and up to 22 points worse at 8,000 tokens, once user preferences filled the share. These rates depend on the simulated workload and need models. Run `cargo run --release --example sim -- OUT_DIR [DAYS] [HYGIENE]` to reproduce them.

Context reads all of its sections inside one SQLite read transaction, so a concurrent commit cannot split the bundle. The result separates pending and disputed claims from active claims. OMK treats `--token-count` as a conservative hint and never stores a value below its estimate. Visible redaction markers also use part of the budget.

The estimate covers JSON model input, including the record fields selected by the chosen context format. For plans, the fields are `scope`, `events`, `activeClaims`, and `previousContinuation`. For context, they are all bundle fields except `diagnostics`. The Rust `model_payload()` and `compact_model_payload()` methods return these objects. OMK uses one token per four Unicode characters, rounded up per selected item with array separators, plus any excess token hints. Commands, run routing fields, diagnostics, prompts, and renderer overhead are outside this estimate. The caller must check the final rendered input with the target model's tokenizer before sending it.

Observation planning reserves the budgeted active claims (the same half-share ranking as context, without diagnostics) and previous continuation before selecting the next events. If the first event alone cannot fit but required state can, the plan includes that event as a stub with content `{"truncated": true, "reason": "exceeds observation budget", "preview": ...}` and empty metadata, so the run covers it and the cursor can advance. The preview keeps as much serialized content as fits, possibly none. The run records stubbed event IDs, and commit rejects any observation, claim or ambiguity that cites one with `invalid_input`. If required state and an empty stub cannot fit, planning returns `budget_exceeded` without saving a run. Context places budgeted active claims first and assigns query evidence space before optional continuity views and observations.

Context omits an observation when any of its source events is already present in the selected raw tail or query evidence, or when a selected continuity view represents it. View coverage includes observations inherited through previous generations. OMK applies view coverage before it limits candidates, and it picks the newest remaining observations first, so a large reflected backlog cannot hide new ones. A continuity view that cannot fit the budget does not suppress observations. These rules apply to both full and compact context; exact recall still returns the stored evidence.

Plans and recent context read events in pages of 32. Context considers at most 256 general observations and 256 pending/disputed claims (reading at most 257 observations and 257 claims per status to detect truncation). When a backlog exceeds a limit, OMK keeps the newest records and presents them in a stable order. Each of at most 10 search hits expands to at most 256 source events. `diagnostics.truncated` marks candidate, source, or raw-tail truncation; `omittedItems` describes inspected items only. Exact recall remains complete. Every active claim is inspected before the claim budget is applied. These bounds limit decoded rows and source loading; SQLite sorting, scope traversal, and view-chain checks can still scan growing history, so they do not guarantee constant latency.

Observer input is limited to 1,048,576 bytes before CLI JSON parsing and after store serialization, 256 total observations/claims/ambiguities/continuation list items, and 256 source IDs per item. Oversized input fails with `invalid_input` before commit.

Within the byte limit, identical saved commits replay before new item and source admission checks. Older committed results remain replayable; changed requests and privacy tombstones retain their existing errors.

## Manage claims and evidence

Use claim commands for these actions:

```text
omk claim remember   Store explicit current state; conflicts become disputed.
omk claim propose    Store a proposal; it cannot replace active state.
omk claim confirm    Accept a claim and supersede state with the same logical key.
omk claim correct    Add a correction and keep the old claim.
omk claim rescope    Create a source-backed replacement in another scope.
omk claim reject     Reject a pending or disputed claim.
omk claim forget     Make a claim inactive but keep its history.
omk claim purge      Delete a claim and its provenance links.
omk event purge      Delete an event and dependent records.
```

Correct accepts only an active, pending or disputed claim; correcting a superseded, rejected or expired claim returns `invalid_input`. Rescope keeps a disputed claim disputed and a pending claim pending; only an active claim stays active. Rescope merges with an active destination only when the values are equal. A different active value returns `claim_conflict` and leaves claims, provenance, command events, and the operation key unchanged. Resolve that conflict with an explicitly authorized confirmation or correction before retrying.

Direct claim commands create a `memory-command` event. This keeps commands source-backed when you omit `--source-event`. The `--source-event` value must be an event UUID, not a stream sequence.

Claims default to `--cardinality single`. This allows one active value for each scope, kind, subject and predicate.

Use `--cardinality set` when distinct values can be active at the same time. A claim slot cannot switch cardinality by accident. Observation commit applies the same rule: it returns `invalid_input` before any write when an observer claim uses a different cardinality from its existing slot, so the same key stays reusable.

Observer-produced claims stay pending, even if the model labels one as an accepted decision. Use a claim command to confirm it. You can promote it only to an ancestor scope.

Recall interpretations and exact evidence:

```sh
omk recall explain-claim --scope thread:build --id CLAIM_ID
omk recall observation --scope thread:build --id OBSERVATION_ID
omk recall event-range --scope thread:build --stream codex-thread-1 --from 1 --to 20
```

`recall observation` returns the observation and its raw source events.

Exact reads allow the anchor scope, its ancestors and its descendants. Knowing a record ID does not bypass this check: claim and observation recall also check every source event. An out-of-scope source fails the request with `scope_violation`; secret sources remain redacted unless you pass `--reveal-secret`. Scope visibility is resolved afresh for each request.

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

Narrow a search with these flags:

- `--type claim|observation|event` returns only those record types; repeat it for several
- `--field subject|predicate|value` matches only that part of a claim, so a search for a name in alias values does not match every claim whose subject holds the name; the default `text` field matches the whole record
- `--include-commands` adds the `memory-command` events that every direct claim write records; search leaves them out by default because each repeats its claim

Record type and command filters are tokens in the full-text index, so they narrow the match itself rather than filtering its rows afterwards. Scope depends on how many rows the query matches, which search counts from the index first, stopping one row past 1,000. A query matching at most 1,000 rows reads them all once, checks each row's scope ID and ranks them, and the rows read give `matched`. A broader query filters scope with index tokens, using the smaller of two lists: the visible scopes, or the scopes they leave out. A root scope that sees everything needs no scope filter. When both lists exceed 64 scopes, a broad query checks each matching row's scope ID instead, which is slower. Every returned hit is also checked against the exact visible scope IDs. Raw `--fts-query` input must have balanced parentheses and closed strings, so it cannot step outside the filters.

Results sort by BM25 multiplied by a record boost, then record type and record ID. Active claims count double, pending and disputed claims and observations count 1.25, events count 1, and superseded, rejected and expired claims count 0.5. `rank` reports that product; lower sorts first. A broad query reads events and observations as separate top-`--limit` lists, since one boost applies to each, and streams claims in BM25 order until no later claim could reach the page even at the highest boost, so only those rows are decoded. When several events or observations tie on BM25 at the edge of the page, the index order picks which ones appear. A broad query counts `matched` from the index, and `searchable` is counted from the index; under `--current-only`, the inactive claims it leaves out are counted from the claims table, since each claim has one index row in its own scope.

Use `--fts-query` only when you need SQLite FTS5 syntax.

Search includes the target scope, its ancestors and its descendants. Context inherits state from ancestors only. When a `single` claim has the same kind, subject and predicate in several visible scopes, the deepest scope wins: context and observation plans drop the shadowed ancestor claim, and context lists it in `diagnostics.omittedItems` as `shadowed by descendant scope claim`. Shadowed claims do not count toward the required budget. `set` claims are combined across scopes without shadowing. `claim list` and exact recall still return every claim. A project context can also render one named descendant stream.

Context evidence queries use the same search modes: `omk context --scope SCOPE --stream STREAM --query 'rollback CLOCK_SKEW_17' --terms` matches separated literal terms. Use `--fts-query` for SQLite FTS5 syntax. Both flags require `--query`, conflict with each other, and work with `--compact`. Omitting them preserves literal phrase matching. Evidence queries use search's default filters, so `memory-command` events are not hits of their own; a matching claim still brings in its command event as a source. Library callers can pass a `ContextQuery` to `compose_context_with_query(...)` or `compose_compact_context_with_query(...)`; existing composition methods retain their defaults.

## Resolve entity names

`recall resolve` maps a name to the subject of existing active claims:

```sh
omk recall resolve --scope project:omk --name 'Dr. Alice Moreau'
```

It compares the name against every active subject and every string value of an active `entity-alias` claim in the visible scopes. Names are compared after case folding, accent folding (`José Núñez` equals `Jose Nunez`), joining apostrophes (`O'Connell` equals `OConnell`) and turning other punctuation into spaces. Five tiers run in order, and the first with a match decides:

1. exact: the name equals a subject or alias
2. name: the same words after folding, in any order (`Moreau, Alice`)
3. contains: one side contains the other as whole words, with at least 3 characters on the shorter side (`Dr. Alice Moreau`)
4. tokens: every word of a name of two or more words stands for a different known word, in any order, as the same word, an initial (`A. Moreau`), a prefix of at least two letters (`Kate` for `Katherine`), or a nickname from OMK's built-in table of common English diminutives (`Bob` for `Robert`); at least one word of 3 or more characters must match whole
5. fuzzy: word by word, in the given or sorted order, each word within an edit budget (with adjacent swaps) of none up to 3 characters, one up to 7 and two beyond, and at least one word exact; a one-word query is compared with each known word of at least 4 characters

`status` is `resolved` for one subject at the exact or name tier, `probable` for one subject at a later tier, `ambiguous` for several subjects, and `none` otherwise. Each candidate lists the names that matched, whether through the subject or an alias, and the alias claim ID. `consideredSubjects` and `consideredAliases` tell an empty store from a real miss. Placeholder names such as `unknown`, `n/a` and `tbd` never match, and resolving one returns `invalid_input`. An irregular nickname missing from the table, such as `Sally` for `Sarah`, does not match; record it as an alias once the user confirms it.

### Measure search

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

## Protect private data

OMK applies these privacy rules:

- `secret` content and metadata stay out of plans, context and full-text search
- read commands require an explicit anchor scope and redact stored secrets unless `--reveal-secret` is also present
- `do-not-store` creates a sequence marker and discards the content and metadata
- redacted events cannot support observations or claims
- event purge removes dependent records and keeps sequence allocation monotonic
- purged operation results become tombstones, so retries cannot restore deleted data
- event purge reports `dependentViews`, `dependentViewIds` and `affectedRunIds`
- event purge also reports affected observations and claims
- claim and event purge remove owned command events and records derived from them
- saved operation results hold the redacted event, never secret content or metadata
- OMK enables SQLite `secure_delete`, so deleted rows are overwritten with zeros

Each purge commits dependency deletion, search cleanup, run invalidation and operation tombstones in one transaction. It follows owned command evidence and removes dependent views along with their later generations. Each affected run is updated once: pending runs become stale, while committed and failed runs keep their status. All affected runs report `sourceIntegrity: "privacy-purged"` and have their ambiguities cleared.

OMK indexes the record IDs in each saved operation result and the search row of each record. A purge uses those indexes to find matching operations and search rows instead of scanning every operation or search entry. Matching operation tombstones discard both the saved result and request hash. Unrelated operations remain replayable, and an identical retry of the purge returns its saved result.

## Use the current schema

OMK 0.8 uses schema v8. Existing schema v8 databases reopen without changes. Schema v8 split the full-text index into text, subject, predicate, value and filter columns, so OMK 0.8 cannot open a schema v7 database; start a fresh one.

Opening a database compares its required table, column, constraint, index, and FTS definitions against the schema created by OMK. A missing or changed definition returns `schema_mismatch` before record writes. The comparison is deliberately exact for OMK-created databases; it does not repair altered schemas or replace a full integrity check.

OMK does not provide migrations before 1.0. It rejects any other nonzero schema version before writing changes. Use a fresh database path for an older schema.

## Create continuity views

Run an external reflector with the [reflector prompt](prompts/reflector.v1.md). Commit the result with `omk view create --kind continuity --stream STREAM --expected-previous-view VIEW_ID`.

Omit `--expected-previous-view` only for generation 1.

Each stream has its own view chain. Every view links to the exact previous view. A stale commit fails without writing. The previous view stays active after a failed reflection.

OMK 0.8 does not provide project-wide views, historical claim state queries or encryption at rest. It does not guarantee forensic erasure: `secure_delete` does not reach copies in the write-ahead log (WAL) before a checkpoint, backups or filesystem snapshots.

`--scope` states the agent's intent and prevents accidental scope leaks. It does not authenticate a process that can choose another scope or read the database.

## Check the implementation

Run these checks:

```sh
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

The integration tests cover:

- schema setup and incompatible versions
- request-bound retries and sequence order through purge
- privacy and strict observer validation
- concurrency and recovery
- command provenance and claim authority
- scope retrieval, per-source recall checks and full-text search modes
- search page counts, index-level filters, ranking boosts and name resolution tiers
- hard context budgets and deduplication through inherited continuity views
- overlapping purge dependencies and preservation of unrelated replays
- structured CLI errors and exact evidence recall
