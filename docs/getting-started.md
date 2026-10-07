# Getting started

## Build and start OMK

Build the release binary:

```sh
cargo build --release
./target/release/omk --help
```

OMK stores data in `.omk/memory.db` by default. Use `OMK_DB` or `--db` to choose another path.

Run `omk` without a command to show help. Use `omk help <command>` or `<command> --help` for command help.

For the shape of command output, retries and errors, see [CLI output and idempotency](cli-output.md).

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

Pass the intended `--scope` and `--reveal-secret` only when the agent needs exact local evidence. Use `do-not-store` when OMK must not save the content or metadata. See [Privacy and limits](privacy.md) for the full rules.

## Next steps

- [Observe events](observation.md) to turn events into observations and claims
- [Build context](context.md) for a bounded model payload
