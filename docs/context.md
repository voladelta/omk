# Bounded context

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

Context reads all of its sections inside one SQLite read transaction, so a concurrent commit cannot split the bundle. The result separates pending and disputed claims from active claims. OMK treats `--token-count` as a conservative hint and never stores a value below its estimate. Visible redaction markers also use part of the budget.

## Token estimate

The estimate covers JSON model input, including the record fields selected by the chosen context format. For plans, the fields are `scope`, `events`, `activeClaims`, and `previousContinuation`. For context, they are all bundle fields except `diagnostics`. The Rust `model_payload()` and `compact_model_payload()` methods return these objects. OMK uses one token per four Unicode characters, rounded up per selected item with array separators, plus any excess token hints. Commands, run routing fields, diagnostics, prompts, and renderer overhead are outside this estimate. The caller must check the final rendered input with the target model's tokenizer before sending it.

## Claim budget

Active claims may use at most half of the budget. OMK fills that half in two passes. First it pins user-scope claims, newest update first, up to half of the claim share. Then it fills the rest with any remaining claims, newest update first. Each pass keeps one unbroken run of the newest claims that fit. When some do not fit, the run ends at its oldest landmark: about one claim in four, chosen by a hash of its ID. The cutoff then stays put for several additions instead of moving with each one, so consecutive contexts share a longer identical prefix that prompt caches can reuse. A run with no landmark ends where it stopped fitting. Each claim that does not fit appears in `diagnostics.omittedItems` with reason `active claim budget`. It stays active and is still returned by `recall search --current-only` and exact recall. Context fails with `budget_exceeded` and `minimumRequiredTokens` only when the budget cannot hold the empty context structure.

Simulated agent use showed why: active claims passed 16,000 tokens after about 100 days, so a context that required every claim would fail every day after that. With a 16,000-token budget, this ranking never failed over two simulated years and kept 73–83% of the claims an agent needed when needs favor recent claims and user preferences, and 57–67% when they favor durable facts. Ranking by recency alone scored 10–14 points lower. An unlimited user pin did no better than recency alone in the second year at this budget, and up to 22 points worse at 8,000 tokens, once user preferences filled the share. These rates were measured before landmark cutoffs, which keep 3–4% fewer claims at 16,000 tokens, all from the oldest end of each pass. In the same simulation, consecutive contexts in one thread then differed in 30–33% fewer serialized characters after day 100, with 1.7% fewer claims and 43% fewer differing characters at 32,000 tokens, and 7% fewer claims and 21% fewer at 8,000. These rates depend on the simulated workload and need models. Run `cargo run --release --example sim -- OUT_DIR [DAYS] [HYGIENE]` to reproduce them; `cache.jsonl` holds the prefix measurements, and `CACHE_BUDGET` sets their budget.

Claims are listed by scope, oldest first, so a new claim lands near the end of the claim section; what breaks a cached prefix early is a budget eviction at the oldest end of a pass, about 30% into the section. Batching evictions harder was measured and not adopted. With a context after every appended event at 16,000 tokens, after day 100 of 200, cutting at the claim with the most trailing zero hash bits in the oldest quarter of the run saved 7% of uncached characters per call (19% on calls after a claim change) for 3% fewer claims; the oldest half, 11% for 12% fewer; one landmark in 16 instead of 4, 11% for 9% fewer. Most uncached characters come from calls after an appended event, which claims do not touch.

## Recent raw tail

`--recent-raw-tokens` caps the tail of the stream's newest events. When the whole stream fits, context includes all of it. When it does not, the tail starts at an aligned event instead of at the oldest event that fits: among the older half of the events that fit, the one whose sequence number has the most trailing zero bits, as in a binary counter. That start holds as new events arrive, then jumps forward by about half the tail, so the tail grows from about half its budget to all of it and drops back at once. Between jumps, consecutive contexts share the whole tail but the newest event, which a prompt cache can reuse; a tail that slid forward with each event changed its first event on every call. Every event left out appears in `diagnostics.omittedItems` with reason `outside recent raw token budget`, and its observations become eligible for context again.

In the simulation, with a context after every appended event at 16,000 tokens, after day 100: at 2,000 raw tokens the uncached characters per call fell 54% (67% on calls after an append) for 18% fewer tail events; at 1,000 raw tokens, 33% for 16% fewer; at 3,000, 59% for 15% fewer. Keeping at least two thirds of the tail instead of half saved 43% at 2,000 tokens for 11% fewer events. Set `CACHE_RAW` to reproduce them with `cargo run --release --example sim`.

## Observation plans

Observation planning reserves the budgeted active claims (the same half-share ranking as context, without diagnostics) and previous continuation before selecting the next events. If the first event alone cannot fit but required state can, the plan includes that event as a stub with content `{"truncated": true, "reason": "exceeds observation budget", "eventTokens": N}` and empty metadata, so the run covers it and the cursor can advance. The stub holds none of the event's content, because an observer shown part of an event acts on half of it. `eventTokens` is the whole event's estimated cost; raise the budget by at least that much to plan the event in full. The run records stubbed event IDs, and commit rejects any observation, claim or ambiguity that cites one with `invalid_input`. If required state and an empty stub cannot fit, planning returns `budget_exceeded` without saving a run. Context places budgeted active claims first and assigns query evidence space before optional continuity views and observations.

## Observation deduplication

Context omits an observation when any of its source events is already present in the selected raw tail or query evidence, or when a selected continuity view represents it. View coverage includes observations inherited through previous generations. OMK applies view coverage before it limits candidates, and it picks the newest remaining observations first, so a large reflected backlog cannot hide new ones. A continuity view that cannot fit the budget does not suppress observations. These rules apply to both full and compact context; exact recall still returns the stored evidence.

## Read bounds

Plans and recent context read events in pages of 32. Context considers at most 256 general observations and 256 pending/disputed claims (reading at most 257 observations and 257 claims per status to detect truncation). When a backlog exceeds a limit, OMK keeps the newest records and presents them in a stable order. Each of at most 10 search hits expands to at most 256 source events. `diagnostics.truncated` marks candidate, source, or raw-tail truncation; `omittedItems` describes inspected items only. Exact recall remains complete. Every active claim is inspected before the claim budget is applied. These bounds limit decoded rows and source loading; SQLite sorting, scope traversal, and view-chain checks can still scan growing history, so they do not guarantee constant latency.

## Scope inheritance

Context inherits state from ancestors only. When a `single` claim has the same kind, subject and predicate in several visible scopes, the deepest scope wins: context and observation plans drop the shadowed ancestor claim, and context lists it in `diagnostics.omittedItems` as `shadowed by descendant scope claim`. Shadowed claims do not count toward the required budget. `set` claims are combined across scopes without shadowing. `claim list` and exact recall still return every claim. A project context can also render one named descendant stream.

## Evidence queries

Context evidence queries use the same search modes as [search](search.md): `omk context --scope SCOPE --stream STREAM --query 'rollback CLOCK_SKEW_17' --terms` matches separated literal terms. Use `--fts-query` for SQLite FTS5 syntax. Both flags require `--query`, conflict with each other, and work with `--compact`. Omitting them preserves literal phrase matching. Evidence queries use search's default filters, so `memory-command` events are not hits of their own; a matching claim still brings in its command event as a source. Library callers can pass a `ContextQuery` to `compose_context_with_query(...)` or `compose_compact_context_with_query(...)`; existing composition methods retain their defaults.
