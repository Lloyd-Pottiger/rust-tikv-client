// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "256"]

//! This crate provides an easy-to-use client for [TiKV](https://github.com/tikv/tikv), a
//! distributed, transactional key-value database written in Rust.
//!
//! This crate lets you connect to a TiKV cluster and use either a transactional or raw (simple
//! get/put style without transactional consistency guarantees) API to access and update your data.
//!
//! The TiKV Rust client supports several levels of abstraction. The most convenient way to use the
//! client is via [`RawClient`] and [`TransactionClient`]. This gives a very high-level API which
//! mostly abstracts over the distributed nature of the store and has sensible defaults for all
//! protocols. This interface can be configured, primarily when creating the client or transaction
//! objects via the [`Config`] and [`TransactionOptions`] structs. Using some options, you can take
//! over parts of the protocols (such as retrying failed messages) yourself.
//!
//! The lowest level of abstraction is to create and send gRPC messages directly to TiKV (and PD)
//! nodes. The `tikv-client-store` and `tikv-client-pd` crates make this easier than using the
//! protobuf definitions and a gRPC library directly, but give you the same level of control.
//!
//! In between these levels of abstraction, you can send and receive individual messages to the TiKV
//! cluster, but take advantage of library code for common operations such as resolving data to
//! regions and thus nodes in the cluster, or retrying failed messages. This can be useful for
//! testing a TiKV cluster or for some advanced use cases. See the [`request`] module for
//! this API, and [`raw::lowering`] and [`transaction::lowering`] for
//! convenience methods for creating request objects.
//!
//! ## Choosing an API
//!
//! This crate offers both [raw](RawClient) and
//! [transactional](Transaction) APIs. You should choose just one for your system.
//!
//! The consequence of supporting transactions is increased overhead of coordination with the
//! placement driver and TiKV, and additional code complexity.
//!
//! *While it is possible to use both APIs at the same time, doing so is unsafe and unsupported.*
//!
//! ### Transactional
//!
//! The [transactional](Transaction) API supports **transactions** via multi-version
//! concurrency control (MVCC).
//!
//! Best when you mostly do complex sets of actions, actions which may require a rollback,
//! operations affecting multiple keys or values, or operations that depend on strong consistency.
//!
//!
//! ### Raw
//!
//! The [raw](RawClient) API has reduced coordination overhead, but lacks any
//! transactional abilities.
//!
//! Best when you mostly do single value changes, and have very limited cross-value
//! requirements. You will not be able to use transactions with this API.
//!
//! ## Usage
//!
//! The general flow of using the client crate is to create either a raw or transaction client
//! object (which can be configured) then send commands using the client object, or use it to create
//! transactions objects. In the latter case, the transaction is built up using various commands and
//! then committed (or rolled back).
//!
//! ### Examples
//!
//! Raw mode:
//!
//! ```rust,no_run
//! # use tikv_client::{RawClient, Result};
//! # use futures::prelude::*;
//! # fn main() -> Result<()> {
//! # futures::executor::block_on(async {
//! let client = RawClient::new(vec!["127.0.0.1:2379"]).await?;
//! client.put("key".to_owned(), "value".to_owned()).await?;
//! let value = client.get("key".to_owned()).await?;
//! # Ok(())
//! # })}
//! ```
//!
//! Transactional mode:
//!
//! ```rust,no_run
//! # use tikv_client::{TransactionClient, Result};
//! # use futures::prelude::*;
//! # fn main() -> Result<()> {
//! # futures::executor::block_on(async {
//! let txn_client = TransactionClient::new(vec!["127.0.0.1:2379"]).await?;
//! let mut txn = txn_client.begin_optimistic().await?;
//! txn.put("key".to_owned(), "value".to_owned()).await?;
//! let value = txn.get("key".to_owned()).await?;
//! txn.commit().await?;
//! # Ok(())
//! # })}
//! ```
//!
//! To begin a transaction with an explicit start timestamp (without fetching a new PD TSO), use
//! [`TransactionClient::begin_with_start_timestamp`].
//!
//! To begin using a local TSO for a given transaction scope (`txn_scope`/PD `dc_location`), use
//! [`TransactionClient::begin_with_txn_scope`].

#![allow(clippy::field_reassign_with_default)]

pub mod async_util;
pub mod backoff;
#[doc(hidden)]
pub mod coprocessor;
pub mod oracle;
#[doc(hidden)]
pub mod raw;
pub mod redact;
pub mod request;
pub mod store_vars;
pub mod tikvrpc;
pub mod trace;
#[doc(hidden)]
pub mod transaction;

mod common;
mod compat;
pub mod config;
mod gc_safe_point;
mod kv;
mod pd;
mod proto;
mod region;
mod region_cache;
mod replica_read;
mod request_context;
mod rpc_interceptor;
mod safe_ts;
mod stats;
mod store;
mod timestamp;
pub mod util;

#[cfg(test)]
mod mock;
#[cfg(test)]
mod proptests;

#[doc(inline)]
pub use crate::pd::PdClient;
#[doc(inline)]
pub use crate::pd::PdRpcClient;
#[doc(inline)]
pub use common::extract_debug_info_str_from_key_error;
#[doc(inline)]
pub use common::security::SecurityManager;
#[doc(inline)]
pub use common::AssertionFailedError;
#[doc(inline)]
pub use common::DeadlockError;
#[doc(inline)]
pub use common::Error;
#[doc(inline)]
pub use common::ProtoAssertionFailed;
#[doc(inline)]
pub use common::ProtoDeadlock;
#[doc(inline)]
pub use common::ProtoKeyError;
#[doc(inline)]
pub use common::ProtoRegionError;
#[doc(inline)]
pub use common::ProtoWriteConflict;
#[doc(inline)]
pub use common::Result;
#[doc(inline)]
pub use common::WriteConflictError;
#[doc(inline)]
pub use config::Config;
#[doc(inline)]
pub use config::GrpcCompressionType;
#[doc(inline)]
pub use config::Security;

#[doc(inline)]
pub use crate::backoff::Backoff;
#[doc(hidden)]
pub use crate::coprocessor::lowering as coprocessor_lowering;
#[doc(inline)]
pub use crate::kv::codec::decode_comparable_uvarint;
#[doc(inline)]
pub use crate::kv::codec::decode_comparable_varint;
#[doc(inline)]
pub use crate::kv::codec::encode_comparable_uvarint;
#[doc(inline)]
pub use crate::kv::codec::encode_comparable_varint;
#[doc(inline)]
pub use crate::kv::BoundRange;
#[doc(inline)]
pub use crate::kv::IntoOwnedRange;
#[doc(inline)]
pub use crate::kv::Key;
#[doc(inline)]
pub use crate::kv::KvPair;
#[doc(inline)]
pub use crate::kv::Value;
#[doc(inline)]
pub use crate::raw::lowering as raw_lowering;
#[doc(inline)]
pub use crate::raw::Client as RawClient;
#[doc(inline)]
pub use crate::raw::ColumnFamily;
#[doc(inline)]
pub use crate::raw::RawChecksum;
#[doc(inline)]
pub use crate::region_cache::RegionCache;
#[doc(inline)]
pub use crate::replica_read::ReplicaReadAdjuster;
#[doc(inline)]
pub use crate::replica_read::ReplicaReadType;
#[doc(inline)]
pub use crate::request::RetryOptions;
#[doc(inline)]
pub use crate::request_context::CommandPriority;
#[doc(inline)]
pub use crate::request_context::DiskFullOpt;
#[doc(inline)]
pub use crate::request_context::IsolationLevel;
#[doc(inline)]
pub use crate::request_context::RequestSource;
#[doc(inline)]
pub use crate::request_context::TraceControlFlags;
#[doc(inline)]
pub use crate::rpc_interceptor::chain_rpc_interceptors;
#[doc(inline)]
pub use crate::rpc_interceptor::FnRpcInterceptor;
#[doc(inline)]
pub use crate::rpc_interceptor::RpcCallResult;
#[doc(inline)]
pub use crate::rpc_interceptor::RpcInterceptor;
#[doc(inline)]
pub use crate::rpc_interceptor::RpcInterceptorChain;
#[doc(inline)]
pub use crate::rpc_interceptor::RpcRequest;
#[doc(inline)]
pub use crate::store_vars::AccessLocationType;
#[doc(inline)]
pub use crate::tikvrpc::CmdType;
#[doc(inline)]
pub use crate::tikvrpc::EndpointType;
#[doc(inline)]
pub use crate::timestamp::Timestamp;
#[doc(inline)]
pub use crate::timestamp::TimestampExt;
#[doc(inline)]
pub use crate::transaction::lowering as transaction_lowering;
#[doc(inline)]
pub use crate::transaction::AssertionLevel;
#[doc(inline)]
pub use crate::transaction::BackoffRuntimeStats;
#[doc(inline)]
pub use crate::transaction::BinlogExecutor;
#[doc(inline)]
pub use crate::transaction::BinlogWriteResult;
#[doc(inline)]
pub use crate::transaction::BoundLockResolver;
#[doc(inline)]
pub use crate::transaction::CheckLevel;
#[doc(inline)]
pub use crate::transaction::Client as TransactionClient;
#[doc(inline)]
pub use crate::transaction::DeleteRangeTask;
#[doc(inline)]
pub use crate::transaction::KvFilter;
#[doc(inline)]
pub use crate::transaction::KvFilterOp;
#[doc(inline)]
pub use crate::transaction::LifecycleHooks;
#[doc(inline)]
pub use crate::transaction::Lock;
#[doc(inline)]
pub use crate::transaction::LockResolver;
#[doc(inline)]
pub use crate::transaction::LockWaitTimeout;
#[doc(inline)]
pub use crate::transaction::PipelinedTxnOptions;
#[doc(inline)]
pub use crate::transaction::PrewriteEncounterLockPolicy;
#[doc(inline)]
pub use crate::transaction::ProtoCompactResponse;
#[doc(inline)]
pub use crate::transaction::ProtoLockInfo;
#[doc(inline)]
pub use crate::transaction::ProtoTiFlashSystemTableResponse;
#[doc(inline)]
pub use crate::transaction::ProtoWaitForEntry;
#[doc(inline)]
pub use crate::transaction::RangeTaskHandler;
#[doc(inline)]
pub use crate::transaction::RangeTaskRunner;
#[doc(inline)]
pub use crate::transaction::RangeTaskStat;
#[doc(inline)]
pub use crate::transaction::ResolveLockDetail;
#[doc(inline)]
pub use crate::transaction::ResolveLocksContext;
#[doc(inline)]
pub use crate::transaction::ResolveLocksForReadResult;
#[doc(inline)]
pub use crate::transaction::ResolveLocksOptions;
#[doc(inline)]
pub use crate::transaction::ResolveLocksResult;
#[doc(inline)]
pub use crate::transaction::ResolvingLock;
#[doc(inline)]
pub use crate::transaction::RpcRuntimeStats;
#[doc(inline)]
pub use crate::transaction::SchemaLeaseChecker;
#[doc(inline)]
pub use crate::transaction::Snapshot;
#[doc(inline)]
pub use crate::transaction::SnapshotCacheEntry;
#[doc(inline)]
pub use crate::transaction::SnapshotRuntimeStats;
#[doc(inline)]
pub use crate::transaction::SnapshotScanDetail;
#[doc(inline)]
pub use crate::transaction::SnapshotTimeDetail;
#[doc(inline)]
pub use crate::transaction::SyncSnapshot;
#[doc(inline)]
pub use crate::transaction::SyncTransaction;
#[doc(inline)]
pub use crate::transaction::SyncTransactionClient;
#[doc(inline)]
pub use crate::transaction::Transaction;
#[doc(inline)]
pub use crate::transaction::TransactionOptions;
#[doc(inline)]
pub use crate::transaction::TxnStatus;
#[doc(inline)]
pub use crate::transaction::Variables;

/// Protobuf-generated store label metadata returned by PD.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::metapb::StoreLabel;
