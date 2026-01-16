# Architecture

This document explains the major modules and control-flow in this crate so future contributors can
change behavior with confidence (and without re-learning client-go internals).

Scope: this is an implementation/maintenance guide, not an API reference.

## Goals

- Feature + public-API parity with `client-go` v2, with a Rust-idiomatic surface (`Config`,
  `RawClient`, `TransactionClient`, `TransactionOptions`, ...).
- High performance (batching, sharding, minimal allocations) and predictable behavior under load.
- Memory safety and robustness: avoid panicking on malformed inputs / partial failures; return
  typed errors and make retries explicit.

Non-goals / explicit differences:

- No Go-style `tikvrpc.CmdType/Request/Response` "mega wrapper". Rust uses typed kvproto requests
  + a plan/execution framework (see `doc/tikvrpc-public-wrapper.md`).
- No legacy compatibility or deprecated APIs.

## Layers

The client is intentionally layered so that the high-level APIs are thin and most complexity lives
in reusable mid-layer components:

1. High level: `RawClient` (`src/raw/client.rs`) and `TransactionClient`/`Transaction`
   (`src/transaction/{client,transaction}.rs`).
2. Mid level: `request::PlanBuilder` and `request::Plan` execution (`src/request/*`).
3. Low level: typed kvproto requests + gRPC clients (`src/pd/*`, `src/region_cache.rs`,
   `src/store/*`, `src/kv/*`).

## Core Modules (What Lives Where)

- `src/pd/*`
  - PD connection / member validation / RPC retries (`pd::RetryClient`, `pd::Cluster`).
  - Timestamp oracle and keyspace bootstrap (`load_keyspace`).
- `src/region_cache.rs`
  - Region metadata cache, TTL/invalidations, and store cache.
  - "Resolve key/range -> region(s)" is the critical dependency for all sharded requests.
- `src/request/*`
  - `PlanBuilder`: attaches retry/shard/merge/lock-resolve behaviors to a typed kvproto request.
  - `Shardable`: how to split a request by region (key-based or range-based).
  - `Plan`: an executable object that handles concurrency limits, per-region dispatch, merging,
    retries, and error extraction.
- `src/store/*`
  - `RegionStore`: "a region + an addressable TiKV store + request context", and helpers to attach
    store/region metadata to outgoing kvproto requests.
- `src/raw/*`
  - Raw API lowering helpers (`raw_lowering`) and high level request methods.
- `src/transaction/*`
  - `Transaction`: buffering/mutation tracking + commit/rollback protocols.
  - `Buffer` (`src/transaction/buffer.rs`): caches reads and stages mutations before prewrite.
  - Local latches (`src/transaction/latch.rs`): reduces write conflict amplification under
    contention.
  - Lock resolver / cleanup locks (`src/transaction/lock.rs`).
- `src/interceptor.rs`, `src/request_context.rs`
  - Per-request context injection (request_source, resource control, priority, disk_full_opt, ...).
  - Interceptor chain to override/patch context fields at runtime.
- `src/trace.rs`, `src/metrics.rs`, `src/stats.rs`
  - Minimal trace hooks and optional Prometheus metrics (feature-gated).

## End-to-End Flows

### Raw Request (Example: `RawClient::get`)

1. `RawClient` builds a typed kvproto request (or uses `raw_lowering`).
2. A `PlanBuilder` is created with `(pd_client, keyspace, request)`.
3. The plan is decorated with behaviors:
   - sharding by key/range (`Shardable`)
   - retry policy (`Backoff`, `RetryOptions`)
   - optional lock resolving (raw requests can still hit locks)
   - merge/collect strategy (`Merge`, `Processor`)
4. `Plan::execute()`:
   - resolves key/range -> region(s) via `RegionCache`
   - maps region -> store address via `pd`/store cache
   - dispatches per-region RPCs (concurrency limited by semaphores)
   - extracts TiKV errors, resolves region errors, and retries as configured
   - merges region results and returns a Rust type (`Option<Value>`, `Vec<KvPair>`, ...)

### Transaction Commit (High-Level)

At a high level, transactional operations follow the usual TiKV MVCC protocols, but are expressed
as plans so sharding/retry/error handling stays consistent:

- Mutations are buffered in `Buffer` and converted to a set of per-region prewrite requests.
- Commit path chooses between:
  - 2PC
  - 1PC when allowed
  - async-commit when enabled (with safe-window handling)
- Lock resolution is integrated:
  - failed RPCs / key errors can produce lock info
  - lock resolver scans/checks txn status and resolves locks in batches
- Local latches may gate writes to reduce conflicts (especially for pipelined mode).

## Error Handling and Retries

- Never assume "unreachable" states based on remote inputs; validate PD/TiKV responses at the
  boundary and return `Error` instead of panicking.
- Plan execution distinguishes:
  - gRPC transport errors (invalidate store cache, retry)
  - region errors (invalidate region cache, re-resolve)
  - key errors (may be retryable, may require lock resolve, or may be surfaced)
- Backoff strategies are explicit (`Backoff`), and constructors clamp invalid inputs to avoid
  runtime panics (jitter uses `gen_range`, which must not be called with empty ranges).

## Concurrency Model

- The library is async-first and uses Tokio.
- Region/store caches are shared and guarded by async locks (`tokio::sync::{RwLock, Mutex}`).
- Request execution uses semaphores to bound concurrency; pipelined transactions add extra
  coordination to keep correctness and avoid unbounded outstanding requests.

## Keyspace Encoding

When keyspace is enabled, all user keys/ranges are encoded with a fixed prefix before being sent
to TiKV, and response keys are truncated back to the user view. See:

- `src/request/keyspace.rs`
- `Config::with_keyspace` + `pd::RetryClient::load_keyspace`

## Tests

- Unit tests live next to the implementation (`src/**`).
- Integration tests require a real PD/TiKV cluster:
  - `tests/integration_tests.rs` (feature `integration-tests`)
  - `Makefile` has a `tiup` helper and `integration-test-*` targets.

## Unsafe Code

Unsafe code is intentionally rare and should be locally justified:

- `src/kv/key.rs`: `Key` is `#[repr(transparent)]` over `Vec<u8>` for zero-cost conversions.
- `src/request/keyspace.rs`: prefix prepend/truncate uses memmove-style pointer copies.
- `src/kv/codec.rs`: in-place decode uses `ptr::copy` and shrinks `Vec` with `set_len`.
- `src/compat.rs`: manual pin-projection for a loop-based stream adapter.

Every unsafe block should have a `// SAFETY: ...` comment describing required invariants.
