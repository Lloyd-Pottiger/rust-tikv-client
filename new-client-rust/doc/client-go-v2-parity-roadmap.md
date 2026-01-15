# new-client-rust roadmap: client-go(v2) parity

This directory (`./new-client-rust`) is the new Rust TiKV client implementation used in this repo.
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
- M1: Raw parity (ongoing)
  - Add missing raw requests from `client-go` (e.g. checksum) with unit tests
- M2: Transaction parity (ongoing)
  - Validate/complete async commit semantics (`max_commit_ts`/`min_commit_ts`)
  - Large txn, pipelined txn, latch-like concurrency control (as required by parity)
- M3: Read-path parity
  - Replica read / stale read / follower read routing + correctness checks
- M4: Control plane parity
  - `tikvrpc`-like request classification, interceptor chain, resource control (resource group tags)
- M5: Hardening
  - Integration tests against real TiKV/PD (feature-gated)
  - Docs + examples for all `pub` APIs
  - Benchmarking and profiling guidance

## Repo-local artifacts used to drive the plan

- `.codex/progress/client-go-api-inventory.md`: exported symbol inventory (name-only)
- `.codex/progress/gap-analysis.md`: capability gaps vs `client-go` v2
- `.codex/progress/parity-map.md`: Go→Rust module mapping draft


