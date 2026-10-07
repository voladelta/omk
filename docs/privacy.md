# Privacy and limits

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

## Limitations

OMK 0.8 does not provide project-wide views, historical claim state queries or encryption at rest. It does not guarantee forensic erasure: `secure_delete` does not reach copies in the write-ahead log (WAL) before a checkpoint, backups or filesystem snapshots.

`--scope` states the agent's intent and prevents accidental scope leaks. It does not authenticate a process that can choose another scope or read the database.
