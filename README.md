# TiKV Client (Rust)

This is the new Rust TiKV client implementation in this repo.

**Target:** feature + public-API parity with `client-go` v2 (Rust-idiomatic surface).

**Roadmap:** `doc/client-go-v2-parity-roadmap.md`.

**Architecture:** `doc/architecture.md`.

**Migration guide:** `doc/client-go-v2-migration.md`.

**Development:** `doc/development.md`.

This crate provides an easy-to-use client for [TiKV](https://github.com/tikv/tikv), a distributed,
transactional key-value database written in Rust.

This crate lets you connect to a TiKV cluster and use either a transactional or raw (simple get/put
style without transactional consistency guarantees) API to access and update your data.

Status: client-go v2 parity achieved; hardening ongoing (APIs may still evolve).

## Getting started

The TiKV client is a Rust library (crate). To use this crate in your project, add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
tikv-client = { path = "/path/to/tikv-client" }
```

### Prerequisites

- [`rust`](https://www.rust-lang.org/): use the pinned toolchain in `rust-toolchain.toml` (currently `1.84.1`)

The general flow of using the client crate is to create either a raw or transaction client object (which can be configured) then send commands using the client object, or use it to create transactions objects. In the latter case, the transaction is built up using various commands and then committed (or rolled back).

### Examples

Raw mode:

```rust,no_run
# use tikv_client::{RawClient, Result};
# async fn example() -> Result<()> {
let client = RawClient::new(vec!["127.0.0.1:2379"]).await?;
client.put("key".to_owned(), "value".to_owned()).await?;
let _value = client.get("key".to_owned()).await?;
# Ok(())
# }
```

Transactional mode:

```rust,no_run
# use tikv_client::{Result, TransactionClient};
# async fn example() -> Result<()> {
let txn_client = TransactionClient::new(vec!["127.0.0.1:2379"]).await?;
let mut txn = txn_client.begin_optimistic().await?;
txn.put("key".to_owned(), "value".to_owned()).await?;
let _value = txn.get("key".to_owned()).await?;
txn.commit().await?;
# Ok(())
# }
```

Since the TiKV client provides an async API, you'll need to use an async runtime (we currently only support Tokio). See [getting-started.md](getting-started.md) for a complete example.

## API summary

The TiKV Rust client supports several levels of abstraction. The most convenient way to use the client is via `RawClient` and `TransactionClient`. This gives a very high-level API which mostly abstracts over the distributed nature of the store and has sensible defaults for all protocols. This interface can be configured, primarily when creating the client or transaction objects via the `Config` and `TransactionOptions` structs. Using some options, you can take over parts of the protocols (such as retrying failed messages) yourself.

The lowest public abstraction is to create and execute a `request::PlanBuilder` over typed kvproto
requests/responses. Plans reuse the same sharding/retry/lock-resolve/merge machinery used by the
high-level clients, but keep request/response types explicit.

See `tikv_client::raw_lowering` and `tikv_client::transaction_lowering` for convenience helpers to
construct kvproto request objects.

The rest of this document describes only the `RawClient`/`TransactionClient` APIs.

Important note: It is **not recommended or supported** to use both the raw and transactional APIs on the same database.

### Types

`Key`: a key in the store. `String` and `Vec<u8>` implement `Into<Key>`, so you can pass them directly into client functions.

`Value`: a value in the store; just an alias of `Vec<u8>`.

`KvPair`: a pair of a `Key` and a `Value`. It provides convenience methods for conversion to and from other types.

`BoundRange`: used for range related requests like `scan`. It implements `From` for Rust ranges so you can pass a Rust range of keys to the request, e.g., `client.delete_range(vec![]..)`.

### Raw requests

| Request            | Main parameter type | Result type             | Noteworthy Behavior                                                            |
|--------------------|---------------------|-------------------------|--------------------------------------------------------------------------------|
| `put`              | `KvPair`            |                         |                                                                                |
| `get`              | `Key`               | `Option<Value>`         |                                                                                |
| `delete`           | `Key`               |                         |                                                                                |
| `delete_range`     | `BoundRange`        |                         |                                                                                |
| `scan`             | `BoundRange`        | `Vec<KvPair>`           |                                                                                |
| `batch_put`        | `Iter<KvPair>`      |                         |                                                                                |
| `batch_get`        | `Iter<Key>`         | `Vec<KvPair>`           | Skips non-existent keys; does not retain order                                 |
| `batch_delete`     | `Iter<Key>`         |                         |                                                                                |
| `batch_scan`       | `Iter<BoundRange>`  | `Vec<KvPair>`           | See docs for `each_limit` parameter behavior. The order of ranges is retained. |
| `batch_scan_keys`  | `Iter<BoundRange>`  | `Vec<Key>`              | See docs for `each_limit` parameter behavior. The order of ranges is retained. |
| `compare_and_swap` | `Key` + 2x `Value`  | `(Option<Value>, bool)` |                                                                                |

### Transactional requests

| Request                | Main parameter type | Result type     | Noteworthy Behavior                                             |
|------------------------|---------------------|-----------------|-----------------------------------------------------------------|
| `put`                  | `KvPair`            |                 |                                                                 |
| `get`                  | `Key`               | `Option<value>` |                                                                 |
| `get_for_update`       | `Key`               | `Option<value>` |                                                                 |
| `key_exists`           | `Key`               | `bool`          |                                                                 |
| `delete`               | `Key`               |                 |                                                                 |
| `scan`                 | `BoundRange`        | `Iter<KvPair>`  |                                                                 |
| `scan_keys`            | `BoundRange`        | `Iter<Key>`     |                                                                 |
| `batch_get`            | `Iter<Key>`         | `Iter<KvPair>`  | Skips non-existent keys; does not retain order                  |
| `batch_get_for_update` | `Iter<Key>`         | `Iter<KvPair>`  | Skips non-existent keys; does not retain order                  |
| `lock_keys`            | `Iter<Key>`         |                 |                                                                 |
| `send_heart_beat`      |                     | `u64` (TTL)     |                                                                 |
| `gc`                   | `Timestamp`         | `bool`          | Returns true if the latest safepoint in PD equals the parameter |

# Development and contributing

We welcome your contributions! Contributing code is great, we also appreciate filing [issues](https://github.com/tikv/client-rust/issues/new) to identify bugs and provide feedback, adding tests or examples, and improvements to documentation.

## Building and testing

See `doc/development.md` for a consolidated, CI-aligned development workflow.

We use the standard Cargo workflows, e.g., `cargo build` and `cargo test`. For faster test runs we
recommend [nextest](https://nexte.st/index.html):

```
cargo install cargo-nextest --locked
```

Running integration tests or manually testing the client with a TiKV cluster is a little bit more involved. The easiest way is to use [TiUp](https://github.com/pingcap/tiup) (>= 1.5) to initialise a cluster on your local machine:

```
tiup playground nightly --mode tikv-slim
```

Or use this repo's Makefile (starts the playground in the background):

```
make tiup-up
```

Then if you want to run integration tests:

```
PD_ADDRS="127.0.0.1:2379" cargo test --package tikv-client --test integration_tests --features integration-tests
```

For a small smoke suite (one raw + one txn test):

```
make integration-test-smoke
```

## Benchmarks

See `doc/bench.md` for microbenchmarks (`cargo bench`) and optional flamegraph workflow.

Stop and clean up the playground:

```
make tiup-down
make tiup-clean  # also removes target/tiup-playground.log
```

## Client-side tracing

This crate provides a small, TiKV-aligned trace hook API under `tikv_client::trace`. By default,
client-side trace events are disabled.

If your application uses the `tracing` ecosystem, enable the `tracing` feature and install the
forwarder:

```toml
[dependencies]
tikv-client = { path = "/path/to/tikv-client", features = ["tracing"] }
```

```rust,ignore
// In your application:
tikv_client::trace::enable_tracing_events();
// Then configure a subscriber/exporter (e.g. tracing-subscriber, tracing-opentelemetry).
```

## Creating a PR

We use a standard GitHub PR workflow. We run CI on every PR and require all PRs to build without warnings (including clippy and Rustfmt warnings), pass tests, have a DCO sign-off (use `-s` when you commit, the DCO bot will guide you through completing the DCO agreement for your first PR), and have at least one review. If any of this is difficult for you, don't worry about it and ask on the PR.

To run CI-like tests locally, we recommend you run `cargo clippy`, `cargo test/nextest run`, and `cargo fmt` before submitting your PR. See above for running integration tests, but you probably won't need to worry about this for your first few PRs.

Please follow PingCAP's  [Rust style guide](https://pingcap.github.io/style-guide/rust/). All code PRs should include new tests or test cases.

## Getting help

If you need help, either to find something to work on, or with any technical problem, the easiest way to get it is via internals.tidb.io, the forum for TiDB developers.

You can also ask in Slack. We monitor the #client-rust channel on the [tikv-wg slack](https://tikv.org/chat).

You can just ask a question on GitHub issues or PRs directly; if you don't get a response, you should ping @ekexium or @andylokandy.
