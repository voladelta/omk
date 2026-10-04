# Recover OMK operations

Read this branch after a failed, interrupted, or uncertain OMK command.

Parse the JSON error envelope before choosing a retry:

- `retryable: true`: satisfy `nextAction`, then retry the identical request with the same idempotency key.
- `busy` (retryable): another process held the database lock. Nothing was recorded; retry the identical request with the same key, with a short pause if it repeats.
- `sameKeyReusable: true`: correct the rejected pre-write input and retry with that same key. Completion requires the corrected operation to retain the original key.
- `sameKeyReusable: false`: preserve the failure. Replay only the identical request with that key. When the user intends a distinct changed operation, give that separate operation a new globally unique key.

A successful envelope is not a failed test. It proves the write committed. Accept that result as the one logical operation and stop retrying; if its immutable input was wrong, report the accepted record and obtain explicit correction intent rather than writing a duplicate.

Before retrying after interruption, inspect the relevant event, run, view, claim, or stream status. A successful write may already have committed even when its output was lost. Prefer replaying the identical request over issuing a changed duplicate.

For `claim_conflict` during rescope, inspect both values and obtain the user's intended resolution before an explicit confirmation or correction unless that intent is already supplied. The failed rescope changes no claims or sources and leaves its key reusable. For `schema_mismatch`, stop record writes and restore a valid v7 database from backup; do not delete data or reconstruct constraints automatically.

For `scope_violation`, verify the intended anchor scope and the cited source IDs. Claim and observation recall check each source, so a visible parent record can still fail when one source is outside the allowed scope. Keep the scope tied to the current task; do not switch to a broader scope just to bypass the error. Scope visibility is resolved again on the next request.

For stale observation or view work, refresh the cursor or latest view and regenerate the derived output from that accepted base.

For privacy purges, inspect the reported dependent records, `dependentViewIds`, `affectedRunIds`, and recovery action. Purge follows owned command evidence and later view generations. Deletion, search cleanup, run invalidation, and operation tombstones commit in one transaction. Each affected run is updated once: pending runs become stale; committed and failed runs retain their status. All affected runs report `sourceIntegrity: "privacy-purged"` and have their ambiguities cleared. Do not infer that a committed status means its source evidence is still intact.

Purged operation tombstones discard both the saved result and request hash. A `privacy_purged` replay error is not a reason to use a new key to restore deleted evidence. Unrelated operations remain replayable. If the purge response was lost, retry the identical purge with its original key to read its saved result.

For `operation_expired`, the operation committed more than 30 days ago and its saved result was compacted. Do not retry with a new key; read the target event, claim, run or view directly. Replays of `observe plan` rebuild the plan from its run, so its active claims and continuation reflect current state.

Recovery is complete when inspection proves either one accepted write, a safe corrected retry path, or an explicit unresolved blocker. Report the operation code, whether the key remains reusable, and the required next action.
