// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Transactional related functionality.
//!
//! Using the [`TransactionClient`](client::Client) you can utilize TiKV's transactional interface.
//!
//! This interface offers SQL-like transactions on top of the raw interface.
//!
//! **Warning:** It is not advisable to use both raw and transactional functionality in the same keyspace.

pub use client::Client;
pub use client::ProtoCompactResponse;
pub use client::ProtoLockInfo;
pub use client::ProtoTiFlashSystemTableResponse;
pub use client::ProtoWaitForEntry;
pub(crate) use lock::resolve_locks_for_read;
pub(crate) use lock::resolve_locks_with_options;
pub(crate) use lock::HasLocks;
pub(crate) use lock::LockResolverRpcContext;
pub(crate) use lock::ReadLockTracker;
pub use snapshot::Snapshot;
pub use sync_client::SyncTransactionClient;
pub use sync_snapshot::SyncSnapshot;
pub use sync_transaction::SyncTransaction;
pub use transaction::AssertionLevel;
pub use transaction::CheckLevel;
#[doc(hidden)]
pub use transaction::HeartbeatOption;
pub use transaction::LockWaitTimeout;
pub use transaction::Mutation;
pub use transaction::PipelinedTxnOptions;
pub use transaction::PrewriteEncounterLockPolicy;
pub use transaction::SchemaLeaseChecker;
pub use transaction::Transaction;
pub use transaction::TransactionOptions;
pub use vars::Variables;

mod buffer;
mod client;
mod lock;
pub mod lowering;
mod requests;
pub use lock::new_lock;
pub use lock::BoundLockResolver;
pub use lock::Lock;
pub use lock::LockResolver;
pub use lock::ResolveLocksContext;
pub use lock::ResolveLocksForReadResult;
pub use lock::ResolveLocksOptions;
pub use lock::ResolveLocksResult;
pub use lock::ResolvingLock;
pub use requests::TransactionStatus;
pub use requests::TransactionStatusKind;
mod snapshot;
mod sync_client;
mod sync_snapshot;
mod sync_transaction;
#[allow(clippy::module_inception)]
mod transaction;
mod vars;
