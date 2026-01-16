# Development

This document describes the developer workflows for the Rust client crate at the repo root.

## Quick Start

From the repo root:

```bash
# Compile + rustfmt + clippy (warnings are denied).
make check

# Unit tests (uses `cargo nextest` if installed, otherwise `cargo test`).
make unit-test

# Build rustdoc (warnings are denied).
make doc
```

## Proto Generation

`tikv-client` vendors kvproto sources and generates Rust types via the `tikv-client-proto-build`
workspace crate.

```bash
make generate
```

Notes:
- `make check/unit-test/integration-test-*` already run `make generate` first.
- Generated code lives under `src/generated/`.

## Integration Tests (TiKV + PD Required)

Integration tests are behind the `integration-tests` feature and expect a running PD+TiKV cluster.

The easiest way to start a local playground is TiUp:

```bash
make tiup-up
```

Run a small smoke suite:

```bash
make integration-test-smoke
```

Run the full integration test sets:

```bash
make integration-test        # txn + raw
make integration-test-txn
make integration-test-raw
```

Key environment variables:
- `PD_ADDRS` (default `127.0.0.1:2379`): comma-separated PD endpoints
- `MULTI_REGION` (default `1`): pre-splits regions in `tests/common/init()` for multi-region tests
- `TIKV_VERSION` (default `v8.5.1`): TiUp playground TiKV/PD version used by `make tiup-up`

Stop and clean up:

```bash
make tiup-down
make tiup-clean
```

## Benchmarks

See `doc/bench.md` for microbenchmarks (`cargo bench`) and optional flamegraph workflows.

## CI-Like Local Runs

The CI workflow is roughly:

```bash
make all
```

This runs: proto generation, `cargo check`, `cargo fmt --check`, `cargo clippy`, rustdoc, unit
tests, and (optionally) integration tests when a cluster is available.
