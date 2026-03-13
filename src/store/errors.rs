// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::proto::kvrpcpb;
use crate::Error;

// Those that can have a single region error
pub trait HasRegionError {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error>;
}

// Those that can have multiple region errors
pub trait HasRegionErrors {
    fn region_errors(&mut self) -> Option<Vec<crate::proto::errorpb::Error>>;
}

pub trait HasKeyErrors {
    fn key_errors(&mut self) -> Option<Vec<Error>>;
}

impl<T: HasRegionError> HasRegionErrors for T {
    fn region_errors(&mut self) -> Option<Vec<crate::proto::errorpb::Error>> {
        self.region_error().map(|e| vec![e])
    }
}

macro_rules! has_region_error {
    ($type:ty) => {
        impl HasRegionError for $type {
            fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
                self.region_error.take().map(|e| e.into())
            }
        }
    };
}

has_region_error!(kvrpcpb::GetResponse);
has_region_error!(kvrpcpb::ScanResponse);
has_region_error!(kvrpcpb::PrewriteResponse);
has_region_error!(kvrpcpb::FlushResponse);
has_region_error!(kvrpcpb::CommitResponse);
has_region_error!(kvrpcpb::PessimisticLockResponse);
has_region_error!(kvrpcpb::ImportResponse);
has_region_error!(kvrpcpb::BatchRollbackResponse);
has_region_error!(kvrpcpb::PessimisticRollbackResponse);
has_region_error!(kvrpcpb::BatchGetResponse);
has_region_error!(kvrpcpb::BufferBatchGetResponse);
has_region_error!(kvrpcpb::ScanLockResponse);
has_region_error!(kvrpcpb::ResolveLockResponse);
has_region_error!(kvrpcpb::TxnHeartBeatResponse);
has_region_error!(kvrpcpb::CheckTxnStatusResponse);
has_region_error!(kvrpcpb::CheckSecondaryLocksResponse);
has_region_error!(kvrpcpb::DeleteRangeResponse);
has_region_error!(kvrpcpb::GcResponse);
has_region_error!(kvrpcpb::UnsafeDestroyRangeResponse);
has_region_error!(kvrpcpb::RawGetResponse);
has_region_error!(kvrpcpb::RawBatchGetResponse);
has_region_error!(kvrpcpb::RawGetKeyTtlResponse);
has_region_error!(kvrpcpb::RawPutResponse);
has_region_error!(kvrpcpb::RawBatchPutResponse);
has_region_error!(kvrpcpb::RawDeleteResponse);
has_region_error!(kvrpcpb::RawBatchDeleteResponse);
has_region_error!(kvrpcpb::RawDeleteRangeResponse);
has_region_error!(kvrpcpb::RawScanResponse);
has_region_error!(kvrpcpb::RawBatchScanResponse);
has_region_error!(kvrpcpb::RawCasResponse);
has_region_error!(kvrpcpb::RawCoprocessorResponse);
has_region_error!(kvrpcpb::RawChecksumResponse);
has_region_error!(kvrpcpb::GetLockWaitInfoResponse);
has_region_error!(kvrpcpb::GetLockWaitHistoryResponse);

macro_rules! has_key_error {
    ($type:ty) => {
        impl HasKeyErrors for $type {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                self.error.take().map(|e| vec![e.into()])
            }
        }
    };
}

has_key_error!(kvrpcpb::GetResponse);
has_key_error!(kvrpcpb::CommitResponse);
has_key_error!(kvrpcpb::BatchRollbackResponse);
has_key_error!(kvrpcpb::ScanLockResponse);
has_key_error!(kvrpcpb::ResolveLockResponse);
has_key_error!(kvrpcpb::GcResponse);
has_key_error!(kvrpcpb::TxnHeartBeatResponse);
has_key_error!(kvrpcpb::CheckTxnStatusResponse);
has_key_error!(kvrpcpb::CheckSecondaryLocksResponse);

macro_rules! has_str_error {
    ($type:ty) => {
        impl HasKeyErrors for $type {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                if self.error.is_empty() {
                    None
                } else {
                    Some(vec![Error::KvError {
                        message: std::mem::take(&mut self.error),
                    }])
                }
            }
        }
    };
}

has_str_error!(kvrpcpb::RawGetResponse);
has_str_error!(kvrpcpb::RawGetKeyTtlResponse);
has_str_error!(kvrpcpb::RawPutResponse);
has_str_error!(kvrpcpb::RawBatchPutResponse);
has_str_error!(kvrpcpb::RawDeleteResponse);
has_str_error!(kvrpcpb::RawBatchDeleteResponse);
has_str_error!(kvrpcpb::RawDeleteRangeResponse);
has_str_error!(kvrpcpb::RawCasResponse);
has_str_error!(kvrpcpb::RawCoprocessorResponse);
has_str_error!(kvrpcpb::RawChecksumResponse);
has_str_error!(kvrpcpb::ImportResponse);
has_str_error!(kvrpcpb::DeleteRangeResponse);
has_str_error!(kvrpcpb::UnsafeDestroyRangeResponse);
has_str_error!(kvrpcpb::RegisterLockObserverResponse);
has_str_error!(kvrpcpb::CheckLockObserverResponse);
has_str_error!(kvrpcpb::RemoveLockObserverResponse);
has_str_error!(kvrpcpb::PhysicalScanLockResponse);
has_str_error!(kvrpcpb::GetLockWaitInfoResponse);
has_str_error!(kvrpcpb::GetLockWaitHistoryResponse);

impl HasRegionError for kvrpcpb::RegisterLockObserverResponse {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        None
    }
}

impl HasRegionError for kvrpcpb::CheckLockObserverResponse {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        None
    }
}

impl HasRegionError for kvrpcpb::RemoveLockObserverResponse {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        None
    }
}

impl HasRegionError for kvrpcpb::PhysicalScanLockResponse {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        None
    }
}

impl HasRegionError for kvrpcpb::StoreSafeTsResponse {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        None
    }
}

impl HasRegionError for kvrpcpb::CompactResponse {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        None
    }
}

impl HasRegionError for kvrpcpb::TiFlashSystemTableResponse {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        None
    }
}

impl HasKeyErrors for kvrpcpb::StoreSafeTsResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        None
    }
}

impl HasKeyErrors for kvrpcpb::CompactResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        let error = self.error.take()?;
        let message = match error.error {
            Some(kvrpcpb::compact_error::Error::ErrInvalidStartKey(_)) => {
                "compact invalid start key".to_owned()
            }
            Some(kvrpcpb::compact_error::Error::ErrPhysicalTableNotExist(_)) => {
                "compact physical table not exist".to_owned()
            }
            Some(kvrpcpb::compact_error::Error::ErrCompactInProgress(_)) => {
                "compact in progress".to_owned()
            }
            Some(kvrpcpb::compact_error::Error::ErrTooManyPendingTasks(_)) => {
                "compact too many pending tasks".to_owned()
            }
            None => "compact error".to_owned(),
        };

        Some(vec![Error::KvError { message }])
    }
}

impl HasKeyErrors for kvrpcpb::TiFlashSystemTableResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        None
    }
}

impl HasKeyErrors for kvrpcpb::ScanResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.pairs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::BatchGetResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        let error = self.error.take();
        extract_errors(
            std::iter::once(error).chain(self.pairs.iter_mut().map(|pair| pair.error.take())),
        )
    }
}

impl HasKeyErrors for kvrpcpb::BufferBatchGetResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        let error = self.error.take();
        extract_errors(
            std::iter::once(error).chain(self.pairs.iter_mut().map(|pair| pair.error.take())),
        )
    }
}

impl HasKeyErrors for kvrpcpb::RawBatchGetResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.pairs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::RawScanResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.kvs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::RawBatchScanResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.kvs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::PrewriteResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::FlushResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::PessimisticLockResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::PessimisticRollbackResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl<T: HasKeyErrors> HasKeyErrors for Result<T, Error> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        match self {
            Ok(x) => x.key_errors(),
            Err(Error::MultipleKeyErrors(errs)) => Some(std::mem::take(errs)),
            Err(e) => Some(vec![std::mem::replace(
                e,
                Error::StringError("".to_string()), // placeholder, no use.
            )]),
        }
    }
}

impl<T: HasKeyErrors> HasKeyErrors for Vec<T> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        for t in self {
            if let Some(e) = t.key_errors() {
                return Some(e);
            }
        }

        None
    }
}

impl<T: HasRegionError, E> HasRegionError for Result<T, E> {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        self.as_mut().ok().and_then(|t| t.region_error())
    }
}

impl<T: HasRegionError> HasRegionErrors for Vec<T> {
    fn region_errors(&mut self) -> Option<Vec<crate::proto::errorpb::Error>> {
        let errors: Vec<_> = self.iter_mut().filter_map(|x| x.region_error()).collect();
        if errors.is_empty() {
            None
        } else {
            Some(errors)
        }
    }
}

fn extract_errors(
    error_iter: impl Iterator<Item = Option<kvrpcpb::KeyError>>,
) -> Option<Vec<Error>> {
    let errors: Vec<Error> = error_iter.flatten().map(Into::into).collect();
    if errors.is_empty() {
        None
    } else {
        Some(errors)
    }
}

#[cfg(test)]
mod test {
    use super::HasKeyErrors;
    use crate::common::Error;
    use crate::internal_err;
    use crate::proto::kvrpcpb;
    #[test]
    fn result_haslocks() {
        let mut resp: Result<_, Error> = Ok(kvrpcpb::CommitResponse::default());
        assert!(resp.key_errors().is_none());

        let mut resp: Result<_, Error> = Ok(kvrpcpb::CommitResponse {
            error: Some(kvrpcpb::KeyError::default()),
            ..Default::default()
        });
        assert!(resp.key_errors().is_some());

        let mut resp: Result<kvrpcpb::CommitResponse, _> = Err(internal_err!("some error"));
        assert!(resp.key_errors().is_some());
    }

    #[test]
    fn prewrite_key_errors_maps_write_conflict() {
        let mut resp = kvrpcpb::PrewriteResponse {
            errors: vec![kvrpcpb::KeyError {
                conflict: Some(kvrpcpb::WriteConflict {
                    start_ts: 42,
                    conflict_ts: 43,
                    key: vec![1, 2, 3],
                    primary: vec![9],
                    conflict_commit_ts: 44,
                    reason: kvrpcpb::write_conflict::Reason::Optimistic as i32,
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        let errors = resp.key_errors().expect("expected key errors");
        assert_eq!(errors.len(), 1);

        match &errors[0] {
            Error::WriteConflict(conflict) => {
                assert_eq!(conflict.start_ts(), 42);
                assert_eq!(conflict.conflict_ts(), 43);
                assert_eq!(conflict.conflict_commit_ts(), 44);
                assert_eq!(conflict.key(), &[1, 2, 3]);
                assert_eq!(conflict.primary(), &[9]);
                assert_eq!(conflict.reason_i32(), conflict.write_conflict().reason);
                assert_eq!(
                    conflict.reason(),
                    kvrpcpb::write_conflict::Reason::Optimistic
                );
            }
            other => panic!("expected write conflict, got {other:?}"),
        }
    }

    #[test]
    fn prewrite_key_errors_maps_assertion_failed() {
        let mut resp = kvrpcpb::PrewriteResponse {
            errors: vec![kvrpcpb::KeyError {
                assertion_failed: Some(kvrpcpb::AssertionFailed {
                    start_ts: 7,
                    key: vec![1, 2, 3],
                    assertion: kvrpcpb::Assertion::NotExist as i32,
                    existing_start_ts: 8,
                    existing_commit_ts: 9,
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        let errors = resp.key_errors().expect("expected key errors");
        assert_eq!(errors.len(), 1);

        match &errors[0] {
            Error::AssertionFailed(assertion_failed) => {
                assert_eq!(assertion_failed.start_ts(), 7);
                assert_eq!(assertion_failed.key(), &[1, 2, 3]);
                assert_eq!(assertion_failed.existing_start_ts(), 8);
                assert_eq!(assertion_failed.existing_commit_ts(), 9);
                assert_eq!(
                    assertion_failed.assertion(),
                    Some(kvrpcpb::Assertion::NotExist)
                );
                assert_eq!(
                    assertion_failed.assertion_i32(),
                    assertion_failed.assertion_failed().assertion
                );
            }
            other => panic!("expected assertion failed error, got {other:?}"),
        }
    }

    #[test]
    fn prewrite_key_errors_maps_retryable_to_kv_error() {
        let mut resp = kvrpcpb::PrewriteResponse {
            errors: vec![kvrpcpb::KeyError {
                retryable: "mock retryable error".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let errors = resp.key_errors().expect("expected key errors");
        assert_eq!(errors.len(), 1);

        match &errors[0] {
            Error::KvError { message } => assert_eq!(message, "mock retryable error"),
            other => panic!("expected kv error, got {other:?}"),
        }
    }

    #[test]
    fn prewrite_key_errors_maps_abort_to_kv_error() {
        let mut resp = kvrpcpb::PrewriteResponse {
            errors: vec![kvrpcpb::KeyError {
                abort: "mock abort".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let errors = resp.key_errors().expect("expected key errors");
        assert_eq!(errors.len(), 1);

        match &errors[0] {
            Error::KvError { message } => assert_eq!(message, "tikv aborts txn: mock abort"),
            other => panic!("expected kv error, got {other:?}"),
        }
    }

    #[test]
    fn commit_key_errors_maps_commit_ts_too_large_to_kv_error() {
        let mut resp = kvrpcpb::CommitResponse {
            error: Some(kvrpcpb::KeyError {
                commit_ts_too_large: Some(kvrpcpb::CommitTsTooLarge { commit_ts: 233 }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let errors = resp.key_errors().expect("expected key errors");
        assert_eq!(errors.len(), 1);

        match &errors[0] {
            Error::KvError { message } => assert_eq!(message, "commit TS 233 is too large"),
            other => panic!("expected kv error, got {other:?}"),
        }
    }

    #[test]
    fn check_txn_status_key_errors_maps_txn_not_found() {
        let mut resp = kvrpcpb::CheckTxnStatusResponse {
            error: Some(kvrpcpb::KeyError {
                txn_not_found: Some(kvrpcpb::TxnNotFound {
                    start_ts: 42,
                    primary_key: vec![1, 2],
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let errors = resp.key_errors().expect("expected key errors");
        assert_eq!(errors.len(), 1);

        match &errors[0] {
            Error::TxnNotFound(txn_not_found) => {
                assert_eq!(txn_not_found.start_ts, 42);
                assert_eq!(txn_not_found.primary_key.as_slice(), &[1, 2]);
            }
            other => panic!("expected txn not found error, got {other:?}"),
        }
    }

    #[test]
    fn compact_key_errors_maps_compact_in_progress_to_kv_error() {
        let mut resp = kvrpcpb::CompactResponse {
            error: Some(kvrpcpb::CompactError {
                error: Some(kvrpcpb::compact_error::Error::ErrCompactInProgress(
                    kvrpcpb::CompactErrorCompactInProgress {},
                )),
            }),
            ..Default::default()
        };

        let errors = resp.key_errors().expect("expected key errors");
        assert_eq!(errors.len(), 1);

        match &errors[0] {
            Error::KvError { message } => assert_eq!(message, "compact in progress"),
            other => panic!("expected kv error, got {other:?}"),
        }
    }
}
