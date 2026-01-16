# Migrating from `client-go` v2 to `new-client-rust`

This document is a user-facing guide for migrating common `client-go` v2 usage patterns to this
repo’s Rust client (`./new-client-rust`). The goal is **feature parity** with a **Rust-idiomatic**
public API, so the Rust API is *not* a 1:1 mapping of Go packages/types.

## High-level mapping

- Go `rawkv.Client` → Rust `tikv_client::RawClient`
- Go `txnkv.Client` → Rust `tikv_client::TransactionClient`
- Go txn handle (`KVTxn`) → Rust `tikv_client::Transaction`
- Go “snapshot reads” → Rust `tikv_client::Snapshot` (created from a transaction client/transaction)
- Go `Backoffer` / retry knobs → Rust `tikv_client::{RetryOptions, Backoff}` + per-client/per-txn
  options

## Connecting and configuration

In `new-client-rust`, configuration is **explicit** and passed at construction time (no global
mutable config singleton).

```rust,no_run
# use std::time::Duration;
# use tikv_client::{Config, RawClient, Result};
# async fn example() -> Result<()> {
let config = Config::default()
    .with_timeout(Duration::from_secs(5))
    .with_default_keyspace();

let client = RawClient::new_with_config(vec!["127.0.0.1:2379"], config).await?;
# Ok(())
# }
```

## Raw API

The raw API maps directly to `RawClient` methods (`get`/`put`/`batch_*`/`scan`/`delete_range`/TTL
and CAS variants).

```rust,no_run
# use tikv_client::{RawClient, Result};
# async fn example() -> Result<()> {
let client = RawClient::new(vec!["127.0.0.1:2379"]).await?;
client.put("k".to_owned(), "v".to_owned()).await?;
let v = client.get("k".to_owned()).await?;
assert_eq!(v, Some("v".as_bytes().to_vec()));
# Ok(())
# }
```

## Transaction API and options

Transactional mode starts from `TransactionClient` and produces `Transaction` handles.

Retry and routing are configured via `TransactionOptions` (builder-style). Examples:

- Disable region resolution retries for snapshot reads:
  `TransactionOptions::new_pessimistic().no_resolve_regions()`
- Configure replica/stale read routing for snapshot reads:
  `TransactionOptions::new_optimistic().replica_read(...).stale_read(true)`

```rust,no_run
# use tikv_client::{Result, TransactionClient, TransactionOptions};
# async fn example() -> Result<()> {
let txn_client = TransactionClient::new(vec!["127.0.0.1:2379"]).await?;

let mut txn = txn_client
    .begin_with_options(TransactionOptions::new_optimistic().use_async_commit())
    .await?;

txn.put("k".to_owned(), "v".to_owned()).await?;
txn.commit().await?;
# Ok(())
# }
```

## Low-level requests (`tikv_client::request`)

`client-go` exposes a public “request wrapper” layer (`tikvrpc`). `new-client-rust` intentionally
avoids an enum-based “mega wrapper” and instead exposes a typed request-plan abstraction:

- Build a `request::PlanBuilder` over a concrete kvproto request type
- Execute it to get a concrete kvproto response type

This keeps request/response types explicit while still reusing the client’s sharding/retry/merge
machinery.

## Observability (metrics + tracing)

- Metrics: enabled by default via the `prometheus` feature.
- Trace events: use `tikv_client::trace` hooks. If your application uses the `tracing` ecosystem,
  enable feature `tracing` and call `tikv_client::trace::enable_tracing_events()`.

## Capability-only vs out-of-scope (differences vs Go)

Some `client-go` exports exist only because Go lacks fine-grained visibility (or because they are
Go-ecosystem conveniences like global config mutation). In `new-client-rust`:

- “Implementation details” are kept crate-private but the underlying capability exists.
- Go-style global config and some internal tuning knobs are intentionally not exposed; prefer
  explicit `Config` / `TransactionOptions` / builder APIs.
