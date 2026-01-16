# Benchmarks

This crate includes small microbenchmarks to keep performance work grounded in measurements.

## Running

From the repo root:

- Keyspace encoding/decoding:
  - `cargo bench --bench keyspace`

- Request plan (shard/dispatch/merge) overhead (mock-driven, no TiKV cluster required):
  - `cargo bench --features test-util --bench plan`

## Flamegraphs (optional)

Use `cargo-flamegraph` in your environment:

- `cargo install flamegraph --locked`
- `cargo flamegraph --bench plan --features test-util`
