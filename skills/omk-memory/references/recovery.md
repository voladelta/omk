# Recover OMK operations

Read this branch after a failed, interrupted, or uncertain OMK command.

Parse the JSON error envelope before choosing a retry:

- `retryable: true`: satisfy `nextAction`, then retry the identical request with the same idempotency key.
- `sameKeyReusable: true`: correct the rejected pre-write input and retry with that same key. Completion requires the corrected operation to retain the original key.
- `sameKeyReusable: false`: preserve the failure. Replay only the identical request with that key. When the user intends a distinct changed operation, give that separate operation a new globally unique key.

A successful envelope is not a failed test. It proves the write committed. Accept that result as the one logical operation and stop retrying; if its immutable input was wrong, report the accepted record and obtain explicit correction intent rather than writing a duplicate.

Before retrying after interruption, inspect the relevant event, run, view, claim, or stream status. A successful write may already have committed even when its output was lost. Prefer replaying the identical request over issuing a changed duplicate.

For `claim_conflict` during rescope, inspect both values and obtain the user's intended resolution before an explicit confirmation or correction unless that intent is already supplied. The failed rescope changes no claims or sources and leaves its key reusable. For `schema_mismatch`, stop record writes and restore a valid v6 database from backup; do not delete data or reconstruct constraints automatically.

For stale observation or view work, refresh the cursor or latest view and regenerate the derived output from that accepted base. For privacy purges, follow the reported dependent records, affected runs, and recovery action; purged operation tombstones prevent deleted evidence from returning through replay.

Recovery is complete when inspection proves either one accepted write, a safe corrected retry path, or an explicit unresolved blocker. Report the operation code, whether the key remains reusable, and the required next action.
