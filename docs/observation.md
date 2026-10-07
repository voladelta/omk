# Observation

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

Apply the [observer prompt and output contract](../prompts/observer.v1.md) to the returned `scope`, `events`, `activeClaims`, and `previousContinuation` fields. Keep `runId` for commit routing. You can also get the full output shape from `omk observe commit --help`.

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

## Observer input limits

Observer input is limited to 1,048,576 bytes before CLI JSON parsing and after store serialization, 256 total observations/claims/ambiguities/continuation list items, and 256 source IDs per item. Oversized input fails with `invalid_input` before commit.

Within the byte limit, identical saved commits replay before new item and source admission checks. Older committed results remain replayable; changed requests and privacy tombstones retain their existing errors.

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

## Create continuity views

Run an external reflector with the [reflector prompt](../prompts/reflector.v1.md). Commit the result with `omk view create --kind continuity --stream STREAM --expected-previous-view VIEW_ID`.

Omit `--expected-previous-view` only for generation 1.

Each stream has its own view chain. Every view links to the exact previous view. A stale commit fails without writing. The previous view stays active after a failed reflection.
