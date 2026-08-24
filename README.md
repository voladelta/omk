# OMK

OMK is a local-first observational memory kernel and JSON CLI for agents:

- Events are immutable evidence.
- Observations are source-backed interpretations.
- Claims are proposed or canonical state.
- Views are disposable, append-only context renderings.

SQLite is the only runtime dependency. Model execution and scheduling stay outside the kernel: an agent requests a redacted observation plan, produces strict JSON, and commits it atomically.

## Build

```sh
cargo build --release
./target/release/omk --help
```

The default database is `.omk/memory.db`. Set `OMK_DB` or pass `--db` to use another path.

## JSON contract

Successful read commands return their data directly. `init` and every idempotent mutation return an operation envelope:

```json
{
  "data": { "id": "..." },
  "operation": { "replayed": false }
}
```

`retryable` means the unchanged command can be retried. `sameKeyReusable` means validation failed before an operation was recorded, so the corrected request may reuse its idempotency key. Follow `nextAction` before retrying.

An identical retry returns the original data with `replayed: true`. Reusing the key with changed scope, payload, model, metadata, or other input fails with `idempotency_conflict`.

Failures are JSON on stderr:

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

## Agent workflow

Initialize a scope tree:

```sh
omk init
omk scope add --id user:me --kind user --idempotency-key scope-user-me
omk scope add --id project:omk --kind project --parent user:me --idempotency-key scope-project-omk
omk scope add --id thread:build --kind thread --parent project:omk --idempotency-key scope-thread-build
```

Append evidence:

```sh
omk event append \
  --scope thread:build \
  --stream codex-thread-1 \
  --kind user-message \
  --content 'Implement the memory kernel in Rust' \
  --idempotency-key codex-thread-1-message-1
```

Every write requires a request-stable, globally unique idempotency key. Use the same key only for an identical retry.

Append privacy-sensitive evidence with `--sensitivity`:

```sh
omk event append \
  --scope thread:build \
  --stream codex-thread-1 \
  --kind tool-result \
  --content 'credential material' \
  --metadata '{"provider":"example"}' \
  --sensitivity secret \
  --idempotency-key codex-thread-1-secret-1
```

Secret append and replay envelopes return a redacted content tombstone and empty metadata. Exact content is available only through the explicit local evidence commands described under Privacy. Use `do-not-store` when neither content nor metadata may be persisted.

Plan an observation batch. A ready result keeps the stable `.data.runId` and `.data.events[].id` paths needed for strict provenance:

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

The `jq` commands are optional shell examples, not runtime dependencies; agents may parse the JSON directly.

Ready output is shaped as:

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

When the stream has no pending events, planning succeeds explicitly without a run:

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

Apply [prompts/observer.v1.md](prompts/observer.v1.md) to the `.data` object, then commit the strict `ObserverResult`. The four primary sections are required. A completely empty result also requires a non-empty `emptyReason` acknowledgement. Such an acknowledgement preserves the existing continuation view. For any non-empty result, `continuation` is a full replacement snapshot and must carry forward still-valid prior state from `previousContinuation`.

```sh
omk observe commit \
  --run RUN_ID \
  --input observer-result.json \
  --idempotency-key codex-thread-1-observe-commit-1

omk claim reconcile \
  --scope thread:build \
  --idempotency-key codex-thread-1-reconcile-1
```

Inspect lifecycle and recovery state:

```sh
omk observe get --run RUN_ID
omk observe list --stream codex-thread-1 --status pending
omk observe status --stream codex-thread-1
omk observe fail --run RUN_ID --reason model-timeout --idempotency-key observe-failure-1
```

Failure does not advance the cursor. A new plan retries the same range. Competing plans are allowed, but only one can commit; stale runs return a structured recovery error.
Runs record `cursorAtPlan`, so privacy-purged sequence gaps remain recoverable without reusing sequence numbers.
Run inspection also exposes `sourceIntegrity`. A historical committed run remains `committed` after privacy purge but changes from `intact` to `privacy-purged`; its dependent derived records are removed and `nextAction` explains recovery. Pending or running affected runs become stale.

Compose hard-bounded context:

```sh
omk context \
  --scope thread:build \
  --stream codex-thread-1 \
  --max-tokens 16000 \
  --recent-raw-tokens 6000 \
  --format markdown
```

Active state is never silently trimmed. If it cannot fit, the command returns `budget_exceeded` with `minimumRequiredTokens`. Pending and disputed claims appear separately from active claims.
Explicit `--token-count` values are treated as conservative hints: OMK never stores a value below its own estimate, and redaction tombstones still consume their visible estimated size.

## Claims and provenance

```text
omk claim remember   Store explicit current state; conflicts become disputed.
omk claim propose    Store a proposal; it cannot replace active state.
omk claim confirm    Explicitly accept a claim and supersede same-key state.
omk claim correct    Add a correction and preserve the old claim.
omk claim rescope    Create a source-preserving replacement in another scope.
omk claim reject     Reject a pending or disputed claim.
omk claim forget     Make a claim inactive while retaining history.
omk claim purge      Physically delete a claim and provenance links.
omk event purge      Delete an event and dependent derived records.
```

Direct claim commands automatically create a `memory-command` event, so even commands without `--source-event` remain source-backed. `--source-event` accepts an event UUID, not a stream sequence.

Recover interpretations and exact evidence:

```sh
omk recall explain-claim --id CLAIM_ID
omk recall observation --id OBSERVATION_ID
omk recall event-range --stream codex-thread-1 --from 1 --to 20
```

`recall observation` returns both the observation and its raw source events.

## Search and scopes

Search treats input as a literal phrase, so punctuation and hyphens are safe:

```sh
omk recall search --scope project:omk --query 'settlement ETH-only'
```

Add `--fts-query` only when intentionally supplying SQLite FTS5 syntax. Retrieval includes the target scope, its ancestors, and its descendants. Context state inheritance remains ancestor-based, while a project context may explicitly render one descendant stream.

## Privacy

- `secret` content and metadata are redacted from observation plans and context and excluded from FTS.
- Exact `event get` and event-range recall remain an explicit local evidence boundary and can return stored secret content.
- `do-not-store` content becomes a sequence-preserving tombstone; its metadata is discarded before persistence.
- Redacted events cannot source observations or claims.
- Privacy purge removes dependent derived records, preserves monotonic sequence allocation, and invalidates cached operation results without allowing purged data to reappear.
- Event purge output reports `dependentViews`, `dependentViewIds`, and `affectedRunIds` in addition to dependent observations and claims.
- Purging a direct claim also removes any now-orphaned auto-generated command evidence that contains that claim request.

## Schema compatibility

OMK is pre-1.0 and intentionally carries no database migrations. It initializes fresh databases at schema v3 and reopens v3 databases unchanged. Any other nonzero schema version is rejected before schema writes; start with a fresh database path when the development schema changes.

## Views and reflection

Use an external reflector with [prompts/reflector.v1.md](prompts/reflector.v1.md), then commit its text with `omk view create --kind continuity`. Each view is a new generation linked to its predecessor. Failed reflection performs no write, so the previous view remains active.

## Verification

```sh
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

The integration suite covers fresh schema initialization, incompatible-version rejection, request-bound retries, monotonic ordering through purge, envelope privacy, strict observer validation, concurrency and recovery, command provenance, claim authority, scope retrieval, literal FTS, hard budgets, context composition, structured CLI errors, and exact evidence recall.
