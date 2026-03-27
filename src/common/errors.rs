// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::result;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
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

/// Extract debug info JSON string from a TiKV `KeyError`.
///
/// Returns an empty string when `debug_info` is missing.
///
/// This mirrors client-go `error.ExtractDebugInfoStrFromKeyErr`. When redaction is enabled via
/// [`crate::redact::set_redact_mode`], key material inside `debug_info` will be redacted.
pub fn extract_debug_info_str_from_key_error(key_err: &ProtoKeyError) -> String {
    extract_debug_info_str_from_key_error_with_redaction(key_err, crate::redact::need_redact())
}

fn extract_debug_info_str_from_key_error_with_redaction(
    key_err: &ProtoKeyError,
    need_redact: bool,
) -> String {
    let Some(debug_info) = key_err.debug_info.as_ref() else {
        return String::new();
    };

    let mut debug_info = debug_info.clone();
    if need_redact {
        redact_debug_info(&mut debug_info);
    }
    debug_info_to_json_string(&debug_info)
}

fn redact_debug_info(debug_info: &mut kvrpcpb::DebugInfo) {
    let redact_marker = vec![b'?'];
    for mvcc_info in &mut debug_info.mvcc_info {
        mvcc_info.key = redact_marker.clone();
        if let Some(mvcc) = mvcc_info.mvcc.as_mut() {
            if let Some(lock) = mvcc.lock.as_mut() {
                lock.primary = redact_marker.clone();
                lock.short_value = redact_marker.clone();
                for secondary in &mut lock.secondaries {
                    *secondary = redact_marker.clone();
                }
            }

            for write in &mut mvcc.writes {
                write.short_value = redact_marker.clone();
            }

            for value in &mut mvcc.values {
                value.value = redact_marker.clone();
            }
        }
    }
}

fn debug_info_to_json_string(debug_info: &kvrpcpb::DebugInfo) -> String {
    fn write_field_name(out: &mut String, first: &mut bool, name: &str) {
        if *first {
            *first = false;
        } else {
            out.push(',');
        }
        out.push('"');
        out.push_str(name);
        out.push_str("\":");
    }

    fn write_str(out: &mut String, value: &str) {
        out.push('"');
        out.push_str(value);
        out.push('"');
    }

    fn write_bytes(out: &mut String, value: &[u8]) {
        write_str(out, &BASE64_STANDARD.encode(value));
    }

    fn write_u64(out: &mut String, value: u64) {
        use std::fmt::Write as _;
        write!(out, "{value}").expect("writing to string should not fail");
    }

    fn write_i32(out: &mut String, value: i32) {
        use std::fmt::Write as _;
        write!(out, "{value}").expect("writing to string should not fail");
    }

    fn write_bool(out: &mut String, value: bool) {
        out.push_str(if value { "true" } else { "false" });
    }

    fn write_u64_array(out: &mut String, values: &[u64]) {
        out.push('[');
        for (idx, value) in values.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            write_u64(out, *value);
        }
        out.push(']');
    }

    fn write_bytes_array(out: &mut String, values: &[Vec<u8>]) {
        out.push('[');
        for (idx, value) in values.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            write_bytes(out, value);
        }
        out.push(']');
    }

    fn write_mvcc_lock(out: &mut String, lock: &kvrpcpb::MvccLock) {
        out.push('{');
        let mut first = true;
        if lock.r#type != 0 {
            write_field_name(out, &mut first, "type");
            write_i32(out, lock.r#type);
        }
        if lock.start_ts != 0 {
            write_field_name(out, &mut first, "start_ts");
            write_u64(out, lock.start_ts);
        }
        if !lock.primary.is_empty() {
            write_field_name(out, &mut first, "primary");
            write_bytes(out, &lock.primary);
        }
        if !lock.short_value.is_empty() {
            write_field_name(out, &mut first, "short_value");
            write_bytes(out, &lock.short_value);
        }
        if lock.ttl != 0 {
            write_field_name(out, &mut first, "ttl");
            write_u64(out, lock.ttl);
        }
        if lock.for_update_ts != 0 {
            write_field_name(out, &mut first, "for_update_ts");
            write_u64(out, lock.for_update_ts);
        }
        if lock.txn_size != 0 {
            write_field_name(out, &mut first, "txn_size");
            write_u64(out, lock.txn_size);
        }
        if lock.use_async_commit {
            write_field_name(out, &mut first, "use_async_commit");
            write_bool(out, lock.use_async_commit);
        }
        if !lock.secondaries.is_empty() {
            write_field_name(out, &mut first, "secondaries");
            write_bytes_array(out, &lock.secondaries);
        }
        if !lock.rollback_ts.is_empty() {
            write_field_name(out, &mut first, "rollback_ts");
            write_u64_array(out, &lock.rollback_ts);
        }
        if lock.last_change_ts != 0 {
            write_field_name(out, &mut first, "last_change_ts");
            write_u64(out, lock.last_change_ts);
        }
        if lock.versions_to_last_change != 0 {
            write_field_name(out, &mut first, "versions_to_last_change");
            write_u64(out, lock.versions_to_last_change);
        }
        out.push('}');
    }

    fn write_mvcc_write(out: &mut String, write: &kvrpcpb::MvccWrite) {
        out.push('{');
        let mut first = true;
        if write.r#type != 0 {
            write_field_name(out, &mut first, "type");
            write_i32(out, write.r#type);
        }
        if write.start_ts != 0 {
            write_field_name(out, &mut first, "start_ts");
            write_u64(out, write.start_ts);
        }
        if write.commit_ts != 0 {
            write_field_name(out, &mut first, "commit_ts");
            write_u64(out, write.commit_ts);
        }
        if !write.short_value.is_empty() {
            write_field_name(out, &mut first, "short_value");
            write_bytes(out, &write.short_value);
        }
        if write.has_overlapped_rollback {
            write_field_name(out, &mut first, "has_overlapped_rollback");
            write_bool(out, write.has_overlapped_rollback);
        }
        if write.has_gc_fence {
            write_field_name(out, &mut first, "has_gc_fence");
            write_bool(out, write.has_gc_fence);
        }
        if write.gc_fence != 0 {
            write_field_name(out, &mut first, "gc_fence");
            write_u64(out, write.gc_fence);
        }
        if write.last_change_ts != 0 {
            write_field_name(out, &mut first, "last_change_ts");
            write_u64(out, write.last_change_ts);
        }
        if write.versions_to_last_change != 0 {
            write_field_name(out, &mut first, "versions_to_last_change");
            write_u64(out, write.versions_to_last_change);
        }
        out.push('}');
    }

    fn write_mvcc_value(out: &mut String, value: &kvrpcpb::MvccValue) {
        out.push('{');
        let mut first = true;
        if value.start_ts != 0 {
            write_field_name(out, &mut first, "start_ts");
            write_u64(out, value.start_ts);
        }
        if !value.value.is_empty() {
            write_field_name(out, &mut first, "value");
            write_bytes(out, &value.value);
        }
        out.push('}');
    }

    fn write_mvcc_info(out: &mut String, info: &kvrpcpb::MvccInfo) {
        out.push('{');
        let mut first = true;
        if let Some(lock) = info.lock.as_ref() {
            write_field_name(out, &mut first, "lock");
            write_mvcc_lock(out, lock);
        }
        if !info.writes.is_empty() {
            write_field_name(out, &mut first, "writes");
            out.push('[');
            for (idx, write) in info.writes.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_mvcc_write(out, write);
            }
            out.push(']');
        }
        if !info.values.is_empty() {
            write_field_name(out, &mut first, "values");
            out.push('[');
            for (idx, value) in info.values.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_mvcc_value(out, value);
            }
            out.push(']');
        }
        out.push('}');
    }

    fn write_mvcc_debug_info(out: &mut String, info: &kvrpcpb::MvccDebugInfo) {
        out.push('{');
        let mut first = true;
        if !info.key.is_empty() {
            write_field_name(out, &mut first, "key");
            write_bytes(out, &info.key);
        }
        if let Some(mvcc) = info.mvcc.as_ref() {
            write_field_name(out, &mut first, "mvcc");
            write_mvcc_info(out, mvcc);
        }
        out.push('}');
    }

    let mut out = String::new();
    out.push('{');
    let mut first = true;
    if !debug_info.mvcc_info.is_empty() {
        write_field_name(&mut out, &mut first, "mvcc_info");
        out.push('[');
        for (idx, info) in debug_info.mvcc_info.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            write_mvcc_debug_info(&mut out, info);
        }
        out.push(']');
    }
    out.push('}');
    out
}

/// Deadlock detected when acquiring pessimistic locks.
#[derive(Clone, Debug)]
pub struct DeadlockError {
    deadlock: ProtoDeadlock,
}

impl DeadlockError {
    /// Wraps the PD/TiKV deadlock payload returned by the server.
    pub fn new(deadlock: ProtoDeadlock) -> Self {
        Self { deadlock }
    }

    /// The transaction start timestamp that owns the lock involved in the deadlock.
    pub fn lock_ts(&self) -> u64 {
        self.deadlock.lock_ts
    }

    /// The key that TiKV reports as participating in the deadlock cycle.
    pub fn deadlock_key(&self) -> &[u8] {
        &self.deadlock.deadlock_key
    }

    /// The hash of [`Self::deadlock_key`] reported by TiKV.
    pub fn deadlock_key_hash(&self) -> u64 {
        self.deadlock.deadlock_key_hash
    }

    /// The number of wait-chain entries carried by the underlying deadlock payload.
    pub fn wait_chain_len(&self) -> usize {
        self.deadlock.wait_chain.len()
    }

    /// Borrows the original protobuf payload for callers that need full field access.
    pub fn deadlock(&self) -> &ProtoDeadlock {
        &self.deadlock
    }

    /// Returns the original protobuf payload by value.
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
    /// Wraps the TiKV write-conflict payload and records the conflict metric.
    pub fn new(conflict: ProtoWriteConflict) -> Self {
        crate::stats::inc_txn_write_conflict_counter();
        Self { conflict }
    }

    /// The start timestamp of the transaction that encountered the conflict.
    pub fn start_ts(&self) -> u64 {
        self.conflict.start_ts
    }

    /// The start timestamp of the conflicting transaction/version.
    pub fn conflict_ts(&self) -> u64 {
        self.conflict.conflict_ts
    }

    /// The commit timestamp of the conflicting write when TiKV reports one.
    pub fn conflict_commit_ts(&self) -> u64 {
        self.conflict.conflict_commit_ts
    }

    /// The user key that triggered the write conflict.
    pub fn key(&self) -> &[u8] {
        &self.conflict.key
    }

    /// The primary key of the conflicting transaction.
    pub fn primary(&self) -> &[u8] {
        &self.conflict.primary
    }

    /// The raw protobuf enum value for the conflict reason.
    pub fn reason_i32(&self) -> i32 {
        self.conflict.reason
    }

    /// The decoded conflict reason, or `Unknown` for unrecognized enum values.
    pub fn reason(&self) -> kvrpcpb::write_conflict::Reason {
        kvrpcpb::write_conflict::Reason::try_from(self.reason_i32())
            .unwrap_or(kvrpcpb::write_conflict::Reason::Unknown)
    }

    /// Borrows the original protobuf payload for callers that need full field access.
    pub fn write_conflict(&self) -> &ProtoWriteConflict {
        &self.conflict
    }

    /// Returns the original protobuf payload by value.
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
    /// Wraps the TiKV assertion-failure payload returned by the server.
    pub fn new(assertion_failed: ProtoAssertionFailed) -> Self {
        Self { assertion_failed }
    }

    /// The start timestamp of the transaction whose assertion failed.
    pub fn start_ts(&self) -> u64 {
        self.assertion_failed.start_ts
    }

    /// The key whose assertion check failed.
    pub fn key(&self) -> &[u8] {
        &self.assertion_failed.key
    }

    /// The raw protobuf enum value for the asserted condition.
    pub fn assertion_i32(&self) -> i32 {
        self.assertion_failed.assertion
    }

    /// The decoded asserted condition, or `None` for unrecognized enum values.
    pub fn assertion(&self) -> Option<kvrpcpb::Assertion> {
        kvrpcpb::Assertion::try_from(self.assertion_i32()).ok()
    }

    /// The start timestamp of the existing value that violated the assertion.
    pub fn existing_start_ts(&self) -> u64 {
        self.assertion_failed.existing_start_ts
    }

    /// The commit timestamp of the existing value that violated the assertion.
    pub fn existing_commit_ts(&self) -> u64 {
        self.assertion_failed.existing_commit_ts
    }

    /// Borrows the original protobuf payload for callers that need full field access.
    pub fn assertion_failed(&self) -> &ProtoAssertionFailed {
        &self.assertion_failed
    }

    /// Returns the original protobuf payload by value.
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

/// Store token is up to the limit.
///
/// This mirrors client-go `tikverr.ErrTokenLimit`.
#[derive(Clone, Debug, Error)]
#[error("Store token is up to the limit, store id = {store_id}.")]
pub struct TokenLimitError {
    store_id: u64,
}

impl TokenLimitError {
    /// Creates a store token-limit error for the given TiKV store ID.
    #[must_use]
    pub fn new(store_id: u64) -> Self {
        Self { store_id }
    }

    /// The TiKV store ID whose token budget was exhausted.
    #[must_use]
    pub fn store_id(&self) -> u64 {
        self.store_id
    }
}

/// PD server is timeout/unavailable.
///
/// This mirrors client-go `tikverr.ErrPDServerTimeout`.
#[derive(Clone, Debug, Error)]
#[error("{message}")]
pub struct PdServerTimeoutError {
    message: String,
}

impl PdServerTimeoutError {
    /// Creates a PD timeout/unavailable error with a caller-provided message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// The human-readable timeout/unavailable message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
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
    /// Transaction is aborted by GC because its start timestamp falls behind the GC safe point.
    ///
    /// This mirrors client-go `tikverr.ErrTxnAbortedByGC`.
    #[error("transaction aborted by gc (start_ts={start_ts}, safe_point={safe_point})")]
    TxnAbortedByGc {
        /// Transaction start timestamp that fell behind GC.
        start_ts: u64,
        /// GC safe point that made the transaction invalid.
        safe_point: u64,
    },
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
    /// `EpochNotMatch` did not include any replacement regions.
    #[error("There is no current_regions in the EpochNotMatch error")]
    NoCurrentRegions,
    /// The requested entry was missing from the local region cache.
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
    /// Write conflict detected by transaction local latches.
    ///
    /// This mirrors client-go `ErrWriteConflictInLatch` and indicates that the transaction should
    /// be retried with a new start timestamp.
    #[error("write conflict in latch (start_ts={start_ts})")]
    WriteConflictInLatch {
        /// Transaction start timestamp that lost the latch race.
        start_ts: u64,
    },
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
    RegionForKeyNotFound {
        /// Encoded key that could not be mapped to a region.
        key: Vec<u8>,
    },
    /// No region is found for the given range.
    #[error("Region is not found for range: {:?}", range)]
    RegionForRangeNotFound {
        /// Range that could not be mapped to any region.
        range: BoundRange,
    },
    /// No region is found for the given id. note: distinguish it with the RegionNotFound error in errorpb.
    #[error("Region {} is not found in the response", region_id)]
    RegionNotFoundInResponse {
        /// Region id that was expected in the response.
        region_id: u64,
    },
    /// No leader is found for the given id.
    #[error("Leader of region {} is not found", region.id)]
    LeaderNotFound {
        /// Region whose leader metadata was missing.
        region: RegionVerId,
    },
    /// Scan limit exceeds the maximum
    #[error("Limit {} exceeds max scan limit {}", limit, max_limit)]
    MaxScanLimitExceeded {
        /// Scan limit requested by the caller.
        limit: u32,
        /// Maximum limit accepted by the client/server contract.
        max_limit: u32,
    },
    /// Wraps an invalid semantic-version string.
    #[error("Invalid Semver string: {0:?}")]
    InvalidSemver(#[from] semver::Error),
    /// A string error returned by TiKV server
    #[error("Kv error. {}", message)]
    KvError {
        /// Error message returned by TiKV.
        message: String,
    },
    /// Internal client error used for invariant violations and unexpected states.
    #[error("{}", message)]
    InternalError {
        /// Human-readable description of the internal failure.
        message: String,
    },
    /// Opaque string-based error preserved for compatibility with existing call sites.
    #[error("{0}")]
    StringError(String),
    /// A read timestamp is in the future.
    ///
    /// This mirrors client-go `oracle.ErrFutureTSRead`.
    #[error("{0}")]
    FutureTsRead(#[from] crate::oracle::ErrFutureTsRead),
    /// A stale read cannot use `u64::MAX` as a timestamp.
    ///
    /// This mirrors client-go `oracle.ErrLatestStaleRead`.
    #[error("{0}")]
    LatestStaleRead(#[from] crate::oracle::ErrLatestStaleRead),
    /// Commit timestamp lags behind expected.
    ///
    /// This mirrors client-go `tikverr.ErrCommitTSLag`.
    #[error("{message}")]
    CommitTsLag {
        /// Server-provided description of the commit-ts lag condition.
        message: String,
    },
    /// Store token is up to the limit.
    ///
    /// This mirrors client-go `tikverr.ErrTokenLimit`.
    #[error("{0}")]
    TokenLimit(#[from] TokenLimitError),
    /// PD server is timeout/unavailable.
    ///
    /// This mirrors client-go `tikverr.ErrPDServerTimeout`.
    #[error("{0}")]
    PdServerTimeout(#[from] PdServerTimeoutError),
    /// Failed to acquire a pessimistic lock when no-wait is configured.
    #[error("lock acquire failed and no wait is set")]
    LockAcquireFailAndNoWaitSet,
    /// Failed to acquire a pessimistic lock within the configured wait timeout.
    #[error("lock wait timeout")]
    LockWaitTimeout,
    /// Commit timestamp is required but not returned.
    #[error("commit timestamp is required but not returned")]
    CommitTsRequiredButNotReturned,
    /// Pessimistic-lock RPC returned partial success together with an error.
    #[error("PessimisticLock error: {:?}", inner)]
    PessimisticLockError {
        /// Underlying pessimistic-lock error.
        inner: Box<Error>,
        /// Keys that were locked successfully before the error occurred.
        success_keys: Vec<Vec<u8>>,
    },
    /// Requested keyspace does not exist in PD metadata.
    #[error("Keyspace not found: {0}")]
    KeyspaceNotFound(String),
    /// Transaction metadata lookup reported a missing transaction.
    #[error("Transaction not found error: {:?}", _0)]
    TxnNotFound(kvrpcpb::TxnNotFound),
    /// Attempted to create or use the sync client (including calling its methods) from within a Tokio async runtime context
    #[error(
        "Nested Tokio runtime detected: cannot use SyncTransactionClient from within an async context. \
Use the async TransactionClient instead, or create and use SyncTransactionClient outside of any Tokio runtime.{0}"
    )]
    NestedRuntimeError(String),
}

/// Returns `true` when `err` indicates the result is undetermined.
///
/// This mirrors client-go `tikverr.IsErrorUndetermined`.
pub fn is_error_undetermined(err: &Error) -> bool {
    matches!(err, Error::UndeterminedError(_))
}

/// Returns `true` when `err` indicates the PD TSO lags behind an expected value.
///
/// This mirrors client-go `tikverr.IsErrorCommitTSLag`.
#[doc(alias = "IsErrorCommitTSLag")]
pub fn is_error_commit_ts_lag(err: &Error) -> bool {
    matches!(err, Error::CommitTsLag { .. })
}

/// Returns `true` when `err` indicates a key already exists.
///
/// This mirrors client-go `tikverr.IsErrKeyExist`.
#[doc(alias = "IsErrKeyExist")]
pub fn is_err_key_exist(err: &Error) -> bool {
    matches!(err, Error::DuplicateKeyInsertion)
}

/// Returns `true` when `err` indicates a write conflict.
///
/// This mirrors client-go `tikverr.IsErrWriteConflict`.
#[doc(alias = "IsErrWriteConflict")]
pub fn is_err_write_conflict(err: &Error) -> bool {
    matches!(
        err,
        Error::WriteConflict(_) | Error::WriteConflictInLatch { .. }
    )
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

#[cfg(test)]
mod tests {
    use super::{
        is_err_key_exist, is_err_write_conflict, is_error_commit_ts_lag, is_error_undetermined,
        DeadlockError, Error,
    };
    use crate::proto::kvrpcpb;

    #[test]
    fn test_is_error_undetermined_matches_variant() {
        let inner = Error::StringError("inner".to_owned());
        let err = Error::UndeterminedError(Box::new(inner));
        assert!(is_error_undetermined(&err));
        assert!(!is_error_undetermined(&Error::Unimplemented));
    }

    #[test]
    fn test_is_error_commit_ts_lag_matches_variant() {
        let err = Error::CommitTsLag {
            message: "boom".to_owned(),
        };
        assert!(is_error_commit_ts_lag(&err));
        assert!(!is_error_commit_ts_lag(&Error::Unimplemented));
    }

    #[test]
    fn test_is_err_key_exist_matches_duplicate_key_insertion() {
        assert!(is_err_key_exist(&Error::DuplicateKeyInsertion));
        assert!(!is_err_key_exist(&Error::Unimplemented));
    }

    #[test]
    fn test_is_err_write_conflict_matches_write_conflict_variants() {
        let err = Error::WriteConflict(super::WriteConflictError::new(kvrpcpb::WriteConflict {
            start_ts: 1,
            conflict_ts: 2,
            key: b"k".to_vec(),
            ..Default::default()
        }));
        assert!(is_err_write_conflict(&err));
        assert!(is_err_write_conflict(&Error::WriteConflictInLatch {
            start_ts: 7
        }));
        assert!(!is_err_write_conflict(&Error::Unimplemented));
    }

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

    #[test]
    fn test_key_error_commit_ts_expired_remains_key_error() {
        let mut key_err = kvrpcpb::KeyError::default();
        key_err.commit_ts_expired = Some(kvrpcpb::CommitTsExpired {
            start_ts: 7,
            attempted_commit_ts: 8,
            key: vec![1, 2, 3],
            min_commit_ts: 9,
        });

        let err: Error = key_err.into();
        match err {
            Error::KeyError(key_err) => assert!(key_err.commit_ts_expired.is_some()),
            other => panic!("expected KeyError(commit_ts_expired), got {other:?}"),
        }
    }

    #[test]
    fn test_key_error_primary_mismatch_remains_key_error() {
        let mut key_err = kvrpcpb::KeyError::default();
        key_err.primary_mismatch = Some(kvrpcpb::PrimaryMismatch::default());

        let err: Error = key_err.into();
        match err {
            Error::KeyError(key_err) => assert!(key_err.primary_mismatch.is_some()),
            other => panic!("expected KeyError(primary_mismatch), got {other:?}"),
        }
    }

    #[test]
    fn test_deadlock_error_exposes_deadlock_key() {
        let deadlock = kvrpcpb::Deadlock {
            deadlock_key: b"held-key".to_vec(),
            ..Default::default()
        };
        let err = DeadlockError::new(deadlock);
        assert_eq!(err.deadlock_key(), b"held-key");
    }

    #[test]
    fn test_extract_debug_info_str_from_key_error() {
        let empty = kvrpcpb::KeyError {
            txn_lock_not_found: Some(kvrpcpb::TxnLockNotFound {
                key: b"byte".to_vec(),
            }),
            ..Default::default()
        };
        assert_eq!(
            "",
            super::extract_debug_info_str_from_key_error_with_redaction(&empty, false)
        );

        let debug_info = kvrpcpb::DebugInfo {
            mvcc_info: vec![kvrpcpb::MvccDebugInfo {
                key: b"byte".to_vec(),
                mvcc: Some(kvrpcpb::MvccInfo {
                    lock: Some(kvrpcpb::MvccLock {
                        r#type: kvrpcpb::Op::Del as i32,
                        start_ts: 128,
                        primary: b"k1".to_vec(),
                        secondaries: vec![b"k1".to_vec(), b"k2".to_vec()],
                        short_value: b"v1".to_vec(),
                        ..Default::default()
                    }),
                    writes: vec![kvrpcpb::MvccWrite {
                        r#type: kvrpcpb::Op::Insert as i32,
                        start_ts: 64,
                        commit_ts: 86,
                        short_value: vec![0x1, 0x2, 0x3, 0x4, 0x5, 0x6],
                        ..Default::default()
                    }],
                    values: vec![kvrpcpb::MvccValue {
                        start_ts: 64,
                        value: vec![0x11, 0x12],
                    }],
                }),
            }],
        };

        let key_err = kvrpcpb::KeyError {
            txn_lock_not_found: Some(kvrpcpb::TxnLockNotFound {
                key: b"byte".to_vec(),
            }),
            debug_info: Some(debug_info),
            ..Default::default()
        };

        let expected = r#"{"mvcc_info":[{"key":"Ynl0ZQ==","mvcc":{"lock":{"type":1,"start_ts":128,"primary":"azE=","short_value":"djE=","secondaries":["azE=","azI="]},"writes":[{"type":4,"start_ts":64,"commit_ts":86,"short_value":"AQIDBAUG"}],"values":[{"start_ts":64,"value":"ERI="}]}}]}"#;
        assert_eq!(
            expected,
            super::extract_debug_info_str_from_key_error_with_redaction(&key_err, false)
        );

        let expected_redacted = r#"{"mvcc_info":[{"key":"Pw==","mvcc":{"lock":{"type":1,"start_ts":128,"primary":"Pw==","short_value":"Pw==","secondaries":["Pw==","Pw=="]},"writes":[{"type":4,"start_ts":64,"commit_ts":86,"short_value":"Pw=="}],"values":[{"start_ts":64,"value":"Pw=="}]}}]}"#;
        assert_eq!(
            expected_redacted,
            super::extract_debug_info_str_from_key_error_with_redaction(&key_err, true)
        );
    }
}
