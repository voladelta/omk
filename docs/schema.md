# Schema compatibility

OMK 0.8 uses schema v8. Existing schema v8 databases reopen without changes. Schema v8 split the full-text index into text, subject, predicate, value and filter columns, so OMK 0.8 cannot open a schema v7 database; start a fresh one.

Opening a database compares its required table, column, constraint, index, and FTS definitions against the schema created by OMK. A missing or changed definition returns `schema_mismatch` before record writes. The comparison is deliberately exact for OMK-created databases; it does not repair altered schemas or replace a full integrity check.

OMK does not provide migrations before 1.0. It rejects any other nonzero schema version before writing changes. Use a fresh database path for an older schema.
