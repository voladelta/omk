# OMK

Durable, source-backed memory for local agents, behind a JSON command-line interface (CLI) with SQLite as the only runtime dependency.

```sh
cargo build --release
./target/release/omk --help
```

## TL;DR

**Problem:** agents forget between sessions, and ad-hoc notes lose track of where a fact came from, what replaced it and what is safe to keep.

**Solution:** OMK stores evidence and the state derived from it, and every derived record points back to its sources. OMK does not run or schedule models. Your agent requests a redacted observation plan, produces strict JSON and commits the result atomically.

| Record | Holds |
|---|---|
| event | what happened |
| observation | a source-backed interpretation of events |
| claim | proposed or accepted state |
| view | replaceable context |

| Feature | Where |
|---|---|
| Idempotent writes with safe retries and replay | [CLI output](docs/cli-output.md) |
| Observer plan and commit loop with recovery | [Observation](docs/observation.md) |
| Token-budgeted context | [Context](docs/context.md) |
| Claim lifecycle, exact recall and name resolution | [Claims and recall](docs/claims-and-recall.md) |
| Full-text search with ranking | [Search](docs/search.md) |
| Secret redaction and purge | [Privacy and limits](docs/privacy.md) |

## Quick example

```sh
omk init
omk scope add --id user:me --kind user --idempotency-key scope-user-me
omk scope add --id thread:build --kind thread --parent user:me --idempotency-key scope-thread-build

omk event append \
  --scope thread:build \
  --stream codex-thread-1 \
  --kind user-message \
  --content 'Implement the memory kernel in Rust' \
  --idempotency-key codex-thread-1-message-1

omk observe plan \
  --scope thread:build \
  --stream codex-thread-1 \
  --model codex \
  --idempotency-key codex-thread-1-observe-plan-1
```

The plan lists the new events for an external observer. The [observation guide](docs/observation.md) covers the rest of the loop: commit the observer result, then confirm or reject each pending claim.

## Install

OMK is built from source. It needs a Rust toolchain that supports the 2024 edition.

```sh
cargo build --release
```

The binary is `target/release/omk`. It stores data in `.omk/memory.db` by default. Use `OMK_DB` or `--db` to choose another path. Run `omk help <command>` or `<command> --help` for command help.

## Documentation

| Guide | Covers |
|---|---|
| [Getting started](docs/getting-started.md) | build, scope tree, first events, secret evidence |
| [CLI output and idempotency](docs/cli-output.md) | JSON envelopes, retries, replays, errors |
| [Observation](docs/observation.md) | plan, commit, recovery, continuity views |
| [Context](docs/context.md) | budgets, compact output, scope inheritance |
| [Claims and recall](docs/claims-and-recall.md) | claim commands, exact recall, entity name resolution |
| [Search](docs/search.md) | modes, filters, ranking, benchmarks |
| [Privacy and limits](docs/privacy.md) | redaction, purge, what OMK does not guarantee |
| [Schema compatibility](docs/schema.md) | schema v8, no migrations before 1.0 |
| [Development](docs/development.md) | checks and test coverage |

The observer and reflector prompts live in [`prompts/`](prompts/). An agent skill for using OMK lives in [`skills/omk-memory`](skills/omk-memory).

## Limitations

- No project-wide views, historical claim state queries or encryption at rest.
- `secure_delete` does not reach copies in the write-ahead log before a checkpoint, backups or filesystem snapshots.
- `--scope` states intent and prevents accidental leaks. It does not authenticate a process that can read the database.
- No migrations before 1.0: an incompatible schema needs a fresh database.

See [Privacy and limits](docs/privacy.md) and [Schema compatibility](docs/schema.md) for detail.

## License

MIT. See [LICENSE](LICENSE).
