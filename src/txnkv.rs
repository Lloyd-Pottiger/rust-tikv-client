//! Transactional KV client module.
//!
//! This module mirrors client-go's public `txnkv` package layout. It mainly provides a stable
//! namespace for the transactional client types.

pub use crate::transaction::new_lock;
pub use crate::transaction::Client;
pub use crate::transaction::Lock;
pub use crate::transaction::LockResolver;
pub use crate::transaction::Snapshot;
pub use crate::transaction::Transaction;
pub use crate::transaction::TransactionOptions;
pub use crate::transaction::TxnStatus;

/// Mirrors client-go `txnkv/transaction`.
pub mod transaction {
    pub use crate::transaction::AssertionLevel;
    pub use crate::transaction::LockWaitTimeout;
    pub use crate::transaction::PipelinedTxnOptions;
    pub use crate::transaction::PrewriteEncounterLockPolicy;
    pub use crate::transaction::Transaction;
    pub use crate::transaction::TransactionOptions;
}

/// Mirrors client-go `txnkv/txnsnapshot`.
pub mod txnsnapshot {
    pub use crate::transaction::Snapshot;
    pub use crate::transaction::SyncSnapshot;
}

/// Mirrors client-go `txnkv/txnlock`.
pub mod txnlock {
    pub use crate::transaction::Lock;
    pub use crate::transaction::LockProbe;
    pub use crate::transaction::LockResolver;
    pub use crate::transaction::LockResolverProbe;
    pub use crate::transaction::ResolveLockDetail;
    pub use crate::transaction::ResolveLocksContext;
    pub use crate::transaction::ResolveLocksForReadResult;
    pub use crate::transaction::ResolveLocksOptions;
    pub use crate::transaction::ResolveLocksResult;
    pub use crate::transaction::ResolvingLock;
    pub use crate::transaction::TxnStatus;
}

/// Mirrors client-go `txnkv/rangetask`.
pub mod rangetask {
    pub use crate::transaction::DeleteRangeTask;
    pub use crate::transaction::RangeTaskHandler;
    pub use crate::transaction::RangeTaskRunner;
    pub use crate::transaction::RangeTaskStat;
}

/// Mirrors client-go `txnkv/txnutil`.
pub mod txnutil {
    pub use crate::transaction::TxnStatus;
}
