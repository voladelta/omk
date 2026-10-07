# Claims and recall

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

## Recall interpretations and exact evidence

```sh
omk recall explain-claim --scope thread:build --id CLAIM_ID
omk recall observation --scope thread:build --id OBSERVATION_ID
omk recall event-range --scope thread:build --stream codex-thread-1 --from 1 --to 20
```

`recall observation` returns the observation and its raw source events.

Exact reads allow the anchor scope, its ancestors and its descendants. Knowing a record ID does not bypass this check: claim and observation recall also check every source event. An out-of-scope source fails the request with `scope_violation`; secret sources remain redacted unless you pass `--reveal-secret`. Scope visibility is resolved afresh for each request.

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

`status` is `resolved` for one subject at the exact or name tier, `probable` for one subject at a later tier, `ambiguous` for several subjects, and `none` otherwise. Each candidate lists the names that matched, whether through the subject or an alias, and the alias claim ID. At most 20 candidates are returned: `shown` counts them and `matched` counts every subject that matched at the deciding tier. `consideredSubjects` and `consideredAliases` count the names compared, placeholders excluded, and tell an empty store from a real miss. Placeholder names such as `unknown`, `n/a` and `tbd` never match, and resolving one returns `invalid_input`. An irregular nickname missing from the table, such as `Sally` for `Sarah`, does not match; record it as an alias once the user confirms it.

For benchmark results, see [Search and name resolution](search.md#measure-search).
