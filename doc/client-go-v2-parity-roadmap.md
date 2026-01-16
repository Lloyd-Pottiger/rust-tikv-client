# Roadmap: client-go(v2) parity

This repo's Rust TiKV client implementation (crate at the repo root) is developed here.
The long-term target is **feature + public-API parity** with `./client-go` (v2), while keeping the
Rust surface idiomatic and high-performance.

## What “parity” means here

- Match `client-go` v2 capabilities (including advanced features like async commit / 1PC / follower read).
- Public API is Rust-idiomatic; we avoid Go-style global mutable configuration.
- Deprecated/removed `client-go` APIs are out of scope.

## Current minimum public surface (stable-to-users)

The crate keeps a small “front door” and hides most internals:

- Raw: `RawClient`, `ColumnFamily`, `RawChecksum`
- Transactional: `TransactionClient`, `TransactionOptions`, `Transaction`, `Snapshot`, `Timestamp`
- Types: `Key`, `Value`, `KvPair`, `BoundRange`
- Config/Security: `Config`, `SecurityManager`

Everything else (PD/store/region cache/request planning) is implementation detail until we need to
expose it for parity.

## Milestones (incremental delivery)

- M0: Baseline bootstrap from existing Rust client (done)
- M1: Raw parity (done)
  - Raw get/put/batch/scan/delete_range/TTL/CAS/checksum/coprocessor covered with unit tests
- M2: Transaction parity (done)
  - 2PC/1PC/async-commit, pipelined DML, txn local latches, lock resolver/GC covered with unit tests
- M3: Read-path parity (done)
  - Replica read / stale read / follower read routing + correctness checks
- M4: Control plane parity (done)
  - `tikvrpc`-like request classification, interceptor chain, resource control (resource group tags)
- M5: Hardening (ongoing)
  - Integration tests against real TiKV/PD (feature-gated)
  - Docs + examples for all `pub` APIs
  - Benchmarking and profiling guidance

## Verification

- Unit tests: `cargo test`
- Lints: `cargo clippy --all-targets`
- Integration tests (needs a running PD/TiKV): `PD_ADDRS=127.0.0.1:2379 cargo test --features integration-tests`

## Repo-local artifacts used to drive the plan

- `.codex/progress/client-go-api-inventory.md`: exported symbol inventory (name-only)
- `.codex/progress/gap-analysis.md`: capability gaps vs `client-go` v2
- `.codex/progress/parity-map.md`: Go→Rust module mapping draft
