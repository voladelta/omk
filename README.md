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

An identical retry returns the original data with `replayed: true`. OMK rejects a reused key if any input changes.

`do-not-store` is the exception. It replays requests when only the payload, metadata or token hint changes. OMK keeps no fingerprint derived from that data.

Failures are JSON on standard error:

```json
{
  "error": {
    "code": "budget_exceeded",
    "message": "context budget too small: minimumRequiredTokens=12 for active claims",
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

Apply the [observer prompt and output contract](prompts/observer.v1.md) to the `.data` object. You can also get the full input shape from `omk observe commit --help`.

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

`claim reconcile` can classify trusted non-observer pending state, but it intentionally never promotes observer-origin claims.

## Recover observation work

Inspect runs and stream progress:

```sh
omk observe get --scope thread:build --run RUN_ID
omk observe list --scope thread:build --stream codex-thread-1 --status pending
omk observe status --scope thread:build --stream codex-thread-1
omk observe fail --run RUN_ID --reason model-timeout --idempotency-key observe-failure-1
```

A failed run does not move the cursor. A new plan retries the same range.

OMK allows competing plans, but only one can commit. It returns a structured recovery error for stale runs.

Each run records `cursorAtPlan`. This lets OMK recover across privacy-purged sequence gaps without reusing sequence numbers.

Run inspection also returns `sourceIntegrity`. A committed run changes from `intact` to `privacy-purged` when a purge removes its sources. OMK removes dependent records and returns a recovery action. Affected pending runs become stale.

## Build bounded context

Set a hard token budget when you build context:

```sh
omk context \
  --scope thread:build \
  --stream codex-thread-1 \
  --max-tokens 16000 \
  --recent-raw-tokens 6000
```

OMK never silently removes active state. If it cannot fit, the command returns `budget_exceeded` and `minimumRequiredTokens`.

The result separates pending and disputed claims from active claims. OMK treats `--token-count` as a conservative hint and never stores a value below its estimate. Visible redaction markers also use part of the budget.

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

Direct claim commands create a `memory-command` event. This keeps commands source-backed when you omit `--source-event`. The `--source-event` value must be an event UUID, not a stream sequence.

Claims default to `--cardinality single`. This allows one active value for each scope, kind, subject and predicate.

Use `--cardinality set` when distinct values can be active at the same time. A claim slot cannot switch cardinality by accident.

Observer-produced claims stay pending, even if the model labels one as an accepted decision. Use a claim command to confirm it. You can promote it only to an ancestor scope.

Recall interpretations and exact evidence:

```sh
omk recall explain-claim --scope thread:build --id CLAIM_ID
omk recall observation --scope thread:build --id OBSERVATION_ID
omk recall event-range --scope thread:build --stream codex-thread-1 --from 1 --to 20
```

`recall observation` returns the observation and its raw source events.

## Search across scopes

Search treats your input as a literal phrase. Punctuation and hyphens are safe:

```sh
omk recall search --scope project:omk --query 'settlement ETH-only'
```

Use `--fts-query` only when you need SQLite FTS5 syntax.

Search includes the target scope, its ancestors and its descendants. Context inherits state from ancestors only. A project context can also render one named descendant stream.

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

## Use the current schema

OMK 0.6 uses schema v6. Existing schema v6 databases reopen without changes.

OMK does not provide migrations before 1.0. It rejects any other nonzero schema version before writing changes. Use a fresh database path for an older schema.

## Create continuity views

Run an external reflector with the [reflector prompt](prompts/reflector.v1.md). Commit the result with `omk view create --kind continuity --stream STREAM --expected-previous-view VIEW_ID`.

Omit `--expected-previous-view` only for generation 1.

Each stream has its own view chain. Every view links to the exact previous view. A stale commit fails without writing. The previous view stays active after a failed reflection.

OMK 0.6 does not provide project-wide views, historical claim state queries or encryption at rest. It does not guarantee forensic erasure.

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
- scope retrieval and literal full-text search
- hard context budgets and context composition
- structured CLI errors and exact evidence recall
