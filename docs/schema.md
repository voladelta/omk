# Schema compatibility

OMK uses schema v9. Existing schema v9 databases reopen without changes. Schema v9 removed the claim `authority` column and took `kind` out of the claim slot key, so OMK cannot open a schema v8 database; start a fresh one.

Opening a database compares its required table, column, constraint, index, and FTS definitions against the schema created by OMK. A missing or changed definition returns `schema_mismatch` before record writes. The comparison is deliberately exact for OMK-created databases; it does not repair altered schemas or replace a full integrity check.

OMK does not provide migrations before 1.0. It rejects any other nonzero schema version before writing changes. Use a fresh database path for an older schema.
