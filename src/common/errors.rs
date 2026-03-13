// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::result;

use thiserror::Error;

use crate::proto::kvrpcpb;
use crate::region::RegionVerId;
use crate::BoundRange;

/// Protobuf-generated region-level error returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::errorpb::Error as ProtoRegionError;

/// Protobuf-generated per-key error returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::KeyError as ProtoKeyError;

/// Protobuf-generated deadlock error returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::Deadlock as ProtoDeadlock;

/// Protobuf-generated write conflict error returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::WriteConflict as ProtoWriteConflict;

/// Protobuf-generated assertion failed error returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::AssertionFailed as ProtoAssertionFailed;

/// Deadlock detected when acquiring pessimistic locks.
#[derive(Clone, Debug)]
pub struct DeadlockError {
    deadlock: ProtoDeadlock,
}

impl DeadlockError {
    pub fn new(deadlock: ProtoDeadlock) -> Self {
        Self { deadlock }
    }

    pub fn lock_ts(&self) -> u64 {
        self.deadlock.lock_ts
    }

    pub fn deadlock_key_hash(&self) -> u64 {
        self.deadlock.deadlock_key_hash
    }

    pub fn wait_chain_len(&self) -> usize {
        self.deadlock.wait_chain.len()
    }

    pub fn deadlock(&self) -> &ProtoDeadlock {
        &self.deadlock
    }

    pub fn into_inner(self) -> ProtoDeadlock {
        self.deadlock
    }
}

impl fmt::Display for DeadlockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "deadlock detected (lock_ts={}, deadlock_key_hash={}, wait_chain_len={})",
            self.lock_ts(),
            self.deadlock_key_hash(),
            self.wait_chain_len()
        )
    }
}

impl std::error::Error for DeadlockError {}

impl From<ProtoDeadlock> for DeadlockError {
    fn from(deadlock: ProtoDeadlock) -> Self {
        Self::new(deadlock)
    }
}

/// Write conflict detected when committing or prewriting a transaction.
#[derive(Clone, Debug)]
pub struct WriteConflictError {
    conflict: ProtoWriteConflict,
}

impl WriteConflictError {
    pub fn new(conflict: ProtoWriteConflict) -> Self {
        Self { conflict }
    }

    pub fn start_ts(&self) -> u64 {
        self.conflict.start_ts
    }

    pub fn conflict_ts(&self) -> u64 {
        self.conflict.conflict_ts
    }

    pub fn conflict_commit_ts(&self) -> u64 {
        self.conflict.conflict_commit_ts
    }

    pub fn key(&self) -> &[u8] {
        &self.conflict.key
    }

    pub fn primary(&self) -> &[u8] {
        &self.conflict.primary
    }

    pub fn reason_i32(&self) -> i32 {
        self.conflict.reason
    }

    pub fn reason(&self) -> kvrpcpb::write_conflict::Reason {
        kvrpcpb::write_conflict::Reason::try_from(self.reason_i32())
            .unwrap_or(kvrpcpb::write_conflict::Reason::Unknown)
    }

    pub fn write_conflict(&self) -> &ProtoWriteConflict {
        &self.conflict
    }

    pub fn into_inner(self) -> ProtoWriteConflict {
        self.conflict
    }
}

impl fmt::Display for WriteConflictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "write conflict detected (start_ts={}, conflict_ts={}, conflict_commit_ts={}, reason={}, key_len={}, primary_len={})",
            self.start_ts(),
            self.conflict_ts(),
            self.conflict_commit_ts(),
            self.reason().as_str_name(),
            self.key().len(),
            self.primary().len()
        )
    }
}

impl std::error::Error for WriteConflictError {}

impl From<ProtoWriteConflict> for WriteConflictError {
    fn from(conflict: ProtoWriteConflict) -> Self {
        Self::new(conflict)
    }
}

/// Assertion failed when committing or prewriting a transaction.
#[derive(Clone, Debug)]
pub struct AssertionFailedError {
    assertion_failed: ProtoAssertionFailed,
}

impl AssertionFailedError {
    pub fn new(assertion_failed: ProtoAssertionFailed) -> Self {
        Self { assertion_failed }
    }

    pub fn start_ts(&self) -> u64 {
        self.assertion_failed.start_ts
    }

    pub fn key(&self) -> &[u8] {
        &self.assertion_failed.key
    }

    pub fn assertion_i32(&self) -> i32 {
        self.assertion_failed.assertion
    }

    pub fn assertion(&self) -> Option<kvrpcpb::Assertion> {
        kvrpcpb::Assertion::try_from(self.assertion_i32()).ok()
    }

    pub fn existing_start_ts(&self) -> u64 {
        self.assertion_failed.existing_start_ts
    }

    pub fn existing_commit_ts(&self) -> u64 {
        self.assertion_failed.existing_commit_ts
    }

    pub fn assertion_failed(&self) -> &ProtoAssertionFailed {
        &self.assertion_failed
    }

    pub fn into_inner(self) -> ProtoAssertionFailed {
        self.assertion_failed
    }
}

impl fmt::Display for AssertionFailedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let assertion = self
            .assertion()
            .map(|assertion| assertion.as_str_name())
            .unwrap_or("unknown");
        write!(
            f,
            "assertion failed (start_ts={}, assertion={}, existing_start_ts={}, existing_commit_ts={}, key_len={})",
            self.start_ts(),
            assertion,
            self.existing_start_ts(),
            self.existing_commit_ts(),
            self.key().len(),
        )
    }
}

impl std::error::Error for AssertionFailedError {}

impl From<ProtoAssertionFailed> for AssertionFailedError {
    fn from(assertion_failed: ProtoAssertionFailed) -> Self {
        Self::new(assertion_failed)
    }
}

/// An error originating from the TiKV client or dependencies.
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    /// Feature is not implemented.
    #[error("Unimplemented feature")]
    Unimplemented,
    /// Duplicate key insertion happens.
    #[error("Duplicate key insertion")]
    DuplicateKeyInsertion,
    /// Failed to resolve a lock
    #[error("Failed to resolve lock")]
    ResolveLockError(Vec<kvrpcpb::LockInfo>),
    /// Will raise this error when using a pessimistic txn only operation on an optimistic txn
    #[error("Invalid operation for this type of transaction")]
    InvalidTransactionType,
    /// It's not allowed to perform operations in a transaction after it has been committed or rolled back.
    #[error("Cannot read or write data after any attempt to commit or roll back the transaction")]
    OperationAfterCommitError,
    /// We tried to use 1pc for a transaction, but it didn't work. Probably should have used 2pc.
    #[error("1PC transaction could not be committed.")]
    OnePcFailure,
    /// An operation requires a primary key, but the transaction was empty.
    #[error("transaction has no primary key")]
    NoPrimaryKey,
    /// For raw client, operation is not supported in atomic/non-atomic mode.
    #[error(
        "The operation is not supported in current mode, please consider using RawClient with or without atomic mode"
    )]
    UnsupportedMode,
    #[error("There is no current_regions in the EpochNotMatch error")]
    NoCurrentRegions,
    #[error("The specified entry is not found in the region cache")]
    EntryNotFoundInRegionCache,
    /// Wraps a `std::io::Error`.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Wraps a `std::io::Error`.
    #[error("tokio channel error: {0}")]
    Channel(#[from] tokio::sync::oneshot::error::RecvError),
    /// Wraps a `grpcio::Error`.
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::transport::Error),
    /// Wraps a `reqwest::Error`.
    /// Wraps a `grpcio::Error`.
    #[error("gRPC api error: {0}")]
    GrpcAPI(#[from] tonic::Status),
    /// Wraps a `grpcio::Error`.
    #[error("url error: {0}")]
    Url(#[from] tonic::codegen::http::uri::InvalidUri),
    /// Represents that a futures oneshot channel was cancelled.
    #[error("A futures oneshot channel was canceled. {0}")]
    Canceled(#[from] futures::channel::oneshot::Canceled),
    /// Errors caused by changes of region information
    #[error("Region error: {0:?}")]
    RegionError(Box<ProtoRegionError>),
    /// Whether the transaction is committed or not is undetermined
    #[error("Whether the transaction is committed or not is undetermined")]
    UndeterminedError(Box<Error>),
    /// Wraps a per-key error returned by TiKV.
    #[error("{0:?}")]
    KeyError(Box<ProtoKeyError>),
    /// Write conflict detected when committing or prewriting a transaction.
    #[error("{0}")]
    WriteConflict(WriteConflictError),
    /// Assertion failed detected when committing or prewriting a transaction.
    #[error("{0}")]
    AssertionFailed(AssertionFailedError),
    /// Deadlock detected when acquiring pessimistic locks.
    #[error("{0}")]
    Deadlock(DeadlockError),
    /// Multiple errors generated from the ExtractError plan.
    #[error("Multiple errors: {0:?}")]
    ExtractedErrors(Vec<Error>),
    /// Multiple key errors
    #[error("Multiple key errors: {0:?}")]
    MultipleKeyErrors(Vec<Error>),
    /// Invalid ColumnFamily
    #[error("Unsupported column family {}", _0)]
    ColumnFamilyError(String),
    /// Can't join tokio tasks
    #[error("Failed to join tokio tasks")]
    JoinError(#[from] tokio::task::JoinError),
    /// No region is found for the given key.
    #[error("Region is not found for key: {:?}", key)]
    RegionForKeyNotFound { key: Vec<u8> },
    #[error("Region is not found for range: {:?}", range)]
    RegionForRangeNotFound { range: BoundRange },
    /// No region is found for the given id. note: distinguish it with the RegionNotFound error in errorpb.
    #[error("Region {} is not found in the response", region_id)]
    RegionNotFoundInResponse { region_id: u64 },
    /// No leader is found for the given id.
    #[error("Leader of region {} is not found", region.id)]
    LeaderNotFound { region: RegionVerId },
    /// Scan limit exceeds the maximum
    #[error("Limit {} exceeds max scan limit {}", limit, max_limit)]
    MaxScanLimitExceeded { limit: u32, max_limit: u32 },
    #[error("Invalid Semver string: {0:?}")]
    InvalidSemver(#[from] semver::Error),
    /// A string error returned by TiKV server
    #[error("Kv error. {}", message)]
    KvError { message: String },
    #[error("{}", message)]
    InternalError { message: String },
    #[error("{0}")]
    StringError(String),
    /// Failed to acquire a pessimistic lock when no-wait is configured.
    #[error("lock acquire failed and no wait is set")]
    LockAcquireFailAndNoWaitSet,
    /// Failed to acquire a pessimistic lock within the configured wait timeout.
    #[error("lock wait timeout")]
    LockWaitTimeout,
    /// Commit timestamp is required but not returned.
    #[error("commit timestamp is required but not returned")]
    CommitTsRequiredButNotReturned,
    #[error("PessimisticLock error: {:?}", inner)]
    PessimisticLockError {
        inner: Box<Error>,
        success_keys: Vec<Vec<u8>>,
    },
    #[error("Keyspace not found: {0}")]
    KeyspaceNotFound(String),
    #[error("Transaction not found error: {:?}", _0)]
    TxnNotFound(kvrpcpb::TxnNotFound),
    /// Attempted to create or use the sync client (including calling its methods) from within a Tokio async runtime context
    #[error(
        "Nested Tokio runtime detected: cannot use SyncTransactionClient from within an async context. \
Use the async TransactionClient instead, or create and use SyncTransactionClient outside of any Tokio runtime.{0}"
    )]
    NestedRuntimeError(String),
}

impl From<ProtoRegionError> for Error {
    fn from(e: ProtoRegionError) -> Error {
        Error::RegionError(Box::new(e))
    }
}

impl From<ProtoKeyError> for Error {
    fn from(mut e: ProtoKeyError) -> Error {
        if let Some(conflict) = e.conflict.take() {
            return Error::WriteConflict(WriteConflictError::new(conflict));
        }

        if !e.retryable.is_empty() {
            return Error::KvError {
                message: std::mem::take(&mut e.retryable),
            };
        }

        if let Some(assertion_failed) = e.assertion_failed.take() {
            return Error::AssertionFailed(AssertionFailedError::new(assertion_failed));
        }

        if e.already_exist.take().is_some() {
            return Error::DuplicateKeyInsertion;
        }

        if let Some(deadlock) = e.deadlock.take() {
            return Error::Deadlock(DeadlockError::new(deadlock));
        }

        if !e.abort.is_empty() {
            return Error::KvError {
                message: format!("tikv aborts txn: {}", std::mem::take(&mut e.abort)),
            };
        }

        if let Some(commit_ts_too_large) = e.commit_ts_too_large.take() {
            return Error::KvError {
                message: format!("commit TS {} is too large", commit_ts_too_large.commit_ts),
            };
        }

        if let Some(txn_not_found) = e.txn_not_found.take() {
            return Error::TxnNotFound(txn_not_found);
        }

        Error::KeyError(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::proto::kvrpcpb;

    #[test]
    fn test_key_error_already_exist_maps_to_duplicate_key_insertion() {
        let mut key_err = kvrpcpb::KeyError::default();
        key_err.already_exist = Some(kvrpcpb::AlreadyExist { key: vec![1, 2, 3] });
        let err: Error = key_err.into();
        assert!(matches!(err, Error::DuplicateKeyInsertion));
    }

    #[test]
    fn test_key_error_deadlock_maps_to_deadlock() {
        let mut deadlock = kvrpcpb::Deadlock::default();
        deadlock.lock_ts = 42;
        deadlock.deadlock_key_hash = 7;

        let mut key_err = kvrpcpb::KeyError::default();
        key_err.deadlock = Some(deadlock);

        let err: Error = key_err.into();
        match err {
            Error::Deadlock(deadlock) => {
                assert_eq!(deadlock.lock_ts(), 42);
                assert_eq!(deadlock.deadlock_key_hash(), 7);
            }
            other => panic!("expected deadlock, got {other:?}"),
        }
    }
}

/// A result holding an [`Error`](enum@Error).
pub type Result<T> = result::Result<T, Error>;

#[doc(hidden)]
#[macro_export]
macro_rules! internal_err {
    ($e:expr) => ({
        $crate::Error::InternalError {
            message: format!("[{}:{}]: {}", file!(), line!(),  $e)
        }
    });
    ($f:tt, $($arg:expr),+) => ({
        internal_err!(format!($f, $($arg),+))
    });
}
