# Development

Run these checks:

```sh
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

The integration tests cover:

- schema setup and incompatible versions
- request-bound retries and sequence order through purge
- privacy and strict observer validation
- concurrency and recovery
- command provenance and claim authority
- scope retrieval, per-source recall checks and full-text search modes
- search page counts, index-level filters, ranking boosts and name resolution tiers
- hard context budgets and deduplication through inherited continuity views
- overlapping purge dependencies and preservation of unrelated replays
- structured CLI errors and exact evidence recall

## Examples

- `examples/sim.rs` simulates agent use to evaluate the [claim budget](context.md#claim-budget), the [recent raw tail](context.md#recent-raw-tail) and how much of consecutive contexts a prompt cache could reuse
- `examples/search_bench.rs` benchmarks [name resolution and search](search.md#measure-search)
