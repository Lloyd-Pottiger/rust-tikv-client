//! Transactional KV client module.
//!
//! This module mirrors client-go's public `txnkv` package layout. It mainly provides a stable
//! namespace for the transactional client types.

pub use crate::transaction::Client;
pub use crate::transaction::Snapshot;
pub use crate::transaction::Transaction;
pub use crate::transaction::TransactionOptions;
pub use crate::transaction::TxnStatus;
