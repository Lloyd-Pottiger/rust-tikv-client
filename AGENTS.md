# Repository Guidelines

## Project Structure & Module Organization

- `src/`: Rust library crate (`tikv_client`); protobuf bindings are generated into `src/generated/`.
- `proto/`, `proto-build/`: vendored kvproto sources + generator workspace crate (`make generate`).
- `tests/`: `integration_tests.rs`, `failpoint_tests.rs`, plus shared helpers in `tests/common/`.
- `examples/`: runnable client examples (e.g. `raw.rs`, `transaction.rs`).
- `benches/`: Criterion microbenchmarks.
- `doc/`: architecture, parity roadmap, development notes.
- `config/`: `nextest` and TiUp playground configs.
- `client-go/`, `client-rust/`: upstream snapshots for reference; not built by the root Cargo workspace.

## Build, Test, and Development Commands

```bash
make generate               # (re)generate protobuf bindings (requires protoc)
make check                  # generate + cargo check + fmt --check + clippy (warnings denied)
make unit-test              # unit tests (uses cargo-nextest if installed)
make doc                    # rustdoc (warnings denied)
make tiup-up                # start local PD+TiKV playground (logs: target/tiup-playground.log)
make integration-test-smoke # quick integration subset (cluster required)
make all                    # CI-like run

cargo run --example raw -- --pd 127.0.0.1:2379
```

- Toolchain: use the pinned toolchain in `rust-toolchain.toml` (MSRV is enforced in CI).
- Integration env: `PD_ADDRS` (comma-separated PD endpoints), `TIKV_VERSION`, `MULTI_REGION`.

## Coding Style & Naming Conventions

- Rust 2021; format with `cargo fmt` (see `rustfmt.toml`).
- Lint with `cargo clippy`; this repo treats warnings as errors (`RUSTFLAGS=-Dwarnings`).
- Follow PingCAP's Rust style guide (linked from `README.md`).
- Do not hand-edit `src/generated/`; update it via `make generate`.
- Integration tests are commonly grouped by name prefix (`txn_...` / `raw_...`) for Makefile/nextest filters.

## Testing Guidelines

- Prefer adding unit tests close to the code; run with `make unit-test`.
- Integration tests require a running TiKV+PD cluster and the `integration-tests` feature:
  `make tiup-up` then `make integration-test`, `make integration-test-txn`, or `make integration-test-raw`.
- Keep integration tests deterministic and avoid cross-test state leakage (tests run with `--test-threads 1`).

## Commit & Pull Request Guidelines

- Commit subjects typically use an `<area>: <summary>` prefix (e.g. `feat:`, `fix:`, `docs:`, `test:`, `infra:`).
- Sign commits for DCO: `git commit -s`.
- PRs should explain intent + testing performed; CI must be green (fmt/clippy/doc/tests), and at least one review is expected.
- Keep generated code committed (CI checks `git diff` after `make generate`).
