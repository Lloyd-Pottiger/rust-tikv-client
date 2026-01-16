// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::iter;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::stream::{self};
use futures::StreamExt;

use super::transaction::TXN_COMMIT_BATCH_SIZE;
use crate::collect_single;
use crate::common::Error::PessimisticLockError;
use crate::pd::PdClient;
use crate::proto::kvrpcpb::Action;
use crate::proto::kvrpcpb::LockInfo;
use crate::proto::kvrpcpb::TxnHeartBeatResponse;
use crate::proto::kvrpcpb::TxnInfo;
use crate::proto::kvrpcpb::{self};
use crate::proto::pdpb::Timestamp;
use crate::region::RegionWithLeader;
use crate::request::Collect;
use crate::request::CollectSingle;
use crate::request::CollectWithShard;
use crate::request::DefaultProcessor;
use crate::request::HasNextBatch;
use crate::request::KvRequest;
use crate::request::Merge;
use crate::request::NextBatch;
use crate::request::Process;
use crate::request::RangeRequest;
use crate::request::ResponseWithShard;
use crate::request::Shardable;
use crate::request::SingleKey;
use crate::request::{Batchable, StoreRequest};
use crate::reversible_range_request;
use crate::shardable_key;
use crate::shardable_keys;
use crate::shardable_range;
use crate::store::RegionStore;
use crate::store::Store;
use crate::store::{region_stream_for_keys, region_stream_for_range};
use crate::timestamp::TimestampExt;
use crate::transaction::requests::kvrpcpb::prewrite_request::PessimisticAction;
use crate::transaction::HasLocks;
use crate::transaction::LockOptions;
use crate::util::iter::FlatMapOkIterExt;
use crate::KvPair;
use crate::Result;
use crate::Value;

// implement HasLocks for a response type that has a `pairs` field,
// where locks can be extracted from both the `pairs` and `error` fields
macro_rules! pair_locks {
    ($response_type:ty) => {
        impl HasLocks for $response_type {
            fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
                if self.pairs.is_empty() {
                    self.error
                        .as_mut()
                        .and_then(|error| error.locked.take())
                        .into_iter()
                        .collect()
                } else {
                    self.pairs
                        .iter_mut()
                        .filter_map(|pair| {
                            pair.error.as_mut().and_then(|error| error.locked.take())
                        })
                        .collect()
                }
            }
        }
    };
}

// implement HasLocks for a response type that does not have a `pairs` field,
// where locks are only extracted from the `error` field
macro_rules! error_locks {
    ($response_type:ty) => {
        impl HasLocks for $response_type {
            fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
                self.error
                    .as_mut()
                    .and_then(|error| error.locked.take())
                    .into_iter()
                    .collect()
            }
        }
    };
}

pub fn new_get_request(key: Vec<u8>, timestamp: u64) -> kvrpcpb::GetRequest {
    let mut req = kvrpcpb::GetRequest::default();
    req.key = key;
    req.version = timestamp;
    req
}

impl KvRequest for kvrpcpb::GetRequest {
    type Response = kvrpcpb::GetResponse;
}

shardable_key!(kvrpcpb::GetRequest);
collect_single!(kvrpcpb::GetResponse);
impl SingleKey for kvrpcpb::GetRequest {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

impl Process<kvrpcpb::GetResponse> for DefaultProcessor {
    type Out = Option<Value>;

    fn process(&self, input: Result<kvrpcpb::GetResponse>) -> Result<Self::Out> {
        let input = input?;
        Ok(if input.not_found {
            None
        } else {
            Some(input.value)
        })
    }
}

pub fn new_batch_get_request(keys: Vec<Vec<u8>>, timestamp: u64) -> kvrpcpb::BatchGetRequest {
    let mut req = kvrpcpb::BatchGetRequest::default();
    req.keys = keys;
    req.version = timestamp;
    req
}

impl KvRequest for kvrpcpb::BatchGetRequest {
    type Response = kvrpcpb::BatchGetResponse;
}

shardable_keys!(kvrpcpb::BatchGetRequest);

impl Merge<kvrpcpb::BatchGetResponse> for Collect {
    type Out = Vec<KvPair>;

    fn merge(&self, input: Vec<Result<kvrpcpb::BatchGetResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|resp| resp.pairs.into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_scan_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    timestamp: u64,
    limit: u32,
    key_only: bool,
    reverse: bool,
) -> kvrpcpb::ScanRequest {
    let mut req = kvrpcpb::ScanRequest::default();
    if !reverse {
        req.start_key = start_key;
        req.end_key = end_key;
    } else {
        req.start_key = end_key;
        req.end_key = start_key;
    }
    req.limit = limit;
    req.key_only = key_only;
    req.version = timestamp;
    req.reverse = reverse;
    req
}

impl KvRequest for kvrpcpb::ScanRequest {
    type Response = kvrpcpb::ScanResponse;
}

reversible_range_request!(kvrpcpb::ScanRequest);
shardable_range!(kvrpcpb::ScanRequest);

impl Merge<kvrpcpb::ScanResponse> for Collect {
    type Out = Vec<KvPair>;

    fn merge(&self, input: Vec<Result<kvrpcpb::ScanResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|resp| resp.pairs.into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_resolve_lock_request(
    start_version: u64,
    commit_version: u64,
) -> kvrpcpb::ResolveLockRequest {
    let mut req = kvrpcpb::ResolveLockRequest::default();
    req.start_version = start_version;
    req.commit_version = commit_version;

    req
}

pub fn new_batch_resolve_lock_request(txn_infos: Vec<TxnInfo>) -> kvrpcpb::ResolveLockRequest {
    let mut req = kvrpcpb::ResolveLockRequest::default();
    req.txn_infos = txn_infos;
    req
}

// Note: ResolveLockRequest is a special one: it can be sent to a specified
// region without keys. So it's not Shardable. And we don't automatically retry
// on its region errors (in the Plan level). The region error must be manually
// handled (in the upper level).
impl KvRequest for kvrpcpb::ResolveLockRequest {
    type Response = kvrpcpb::ResolveLockResponse;
}

pub fn new_cleanup_request(key: Vec<u8>, start_version: u64) -> kvrpcpb::CleanupRequest {
    let mut req = kvrpcpb::CleanupRequest::default();
    req.key = key;
    req.start_version = start_version;

    req
}

impl KvRequest for kvrpcpb::CleanupRequest {
    type Response = kvrpcpb::CleanupResponse;
}

shardable_key!(kvrpcpb::CleanupRequest);
collect_single!(kvrpcpb::CleanupResponse);
impl SingleKey for kvrpcpb::CleanupRequest {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

impl Process<kvrpcpb::CleanupResponse> for DefaultProcessor {
    type Out = u64;

    fn process(&self, input: Result<kvrpcpb::CleanupResponse>) -> Result<Self::Out> {
        Ok(input?.commit_version)
    }
}

pub fn new_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Vec<u8>,
    start_version: u64,
    lock_ttl: u64,
) -> kvrpcpb::PrewriteRequest {
    let mut req = kvrpcpb::PrewriteRequest::default();
    req.mutations = mutations;
    req.primary_lock = primary_lock;
    req.start_version = start_version;
    req.lock_ttl = lock_ttl;

    req
}

pub fn new_pessimistic_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Vec<u8>,
    start_version: u64,
    lock_ttl: u64,
    for_update_ts: u64,
) -> kvrpcpb::PrewriteRequest {
    let len = mutations.len();
    let mut req = new_prewrite_request(mutations, primary_lock, start_version, lock_ttl);
    req.for_update_ts = for_update_ts;
    req.pessimistic_actions = iter::repeat(PessimisticAction::DoPessimisticCheck.into())
        .take(len)
        .collect();
    req
}

impl KvRequest for kvrpcpb::PrewriteRequest {
    type Response = kvrpcpb::PrewriteResponse;
}

#[derive(Clone, Debug)]
pub struct PrewriteShard {
    pub mutations: Vec<kvrpcpb::Mutation>,
    /// The number of keys involved in this transaction *within the target region*.
    ///
    /// This mirrors client-go's semantics: all locks written in the same region should carry the
    /// same `txn_size` (regardless of batching).
    pub txn_size: u64,
}

impl Shardable for kvrpcpb::PrewriteRequest {
    type Shard = PrewriteShard;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut mutations = self.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));

        region_stream_for_keys(mutations.into_iter(), pd_client.clone())
            .flat_map(|result| match result {
                Ok((mutations, region)) => {
                    let txn_size = mutations.len() as u64;
                    stream::iter(kvrpcpb::PrewriteRequest::batches(
                        mutations,
                        TXN_COMMIT_BATCH_SIZE,
                    ))
                    .map(move |batch| {
                        Ok((
                            PrewriteShard {
                                mutations: batch,
                                txn_size,
                            },
                            region.clone(),
                        ))
                    })
                    .boxed()
                }
                Err(e) => stream::iter(Err(e)).boxed(),
            })
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        let is_primary_shard = shard.mutations.iter().any(|m| m.key == self.primary_lock);

        // Only need to set secondary keys if we're sending the primary key.
        if self.use_async_commit && !is_primary_shard {
            self.secondaries = vec![];
        }

        // Only if there is only one request to send
        if self.try_one_pc
            && (!is_primary_shard || shard.mutations.len() != self.secondaries.len() + 1)
        {
            self.try_one_pc = false;
        }

        self.txn_size = shard.txn_size;
        self.mutations = shard.mutations;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        store.apply_to_request(self)?;
        // When we retry due to region errors we may lose the original region-level txn size
        // information. Disable resolve-lock-lite for these requests to match client-go.
        if store.attempt > 0 {
            self.txn_size = u64::MAX;
        }
        Ok(())
    }
}

impl Batchable for kvrpcpb::PrewriteRequest {
    type Item = kvrpcpb::Mutation;

    fn item_size(item: &Self::Item) -> u64 {
        let mut size = item.key.len() as u64;
        size += item.value.len() as u64;
        size
    }
}

pub fn new_flush_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_key: Vec<u8>,
    start_ts: u64,
    generation: u64,
    lock_ttl: u64,
) -> kvrpcpb::FlushRequest {
    let mut req = kvrpcpb::FlushRequest::default();
    req.mutations = mutations;
    req.primary_key = primary_key;
    req.start_ts = start_ts;
    req.min_commit_ts = start_ts.saturating_add(1);
    req.generation = generation;
    req.lock_ttl = lock_ttl;
    req
}

impl KvRequest for kvrpcpb::FlushRequest {
    type Response = kvrpcpb::FlushResponse;
}

impl Shardable for kvrpcpb::FlushRequest {
    type Shard = Vec<kvrpcpb::Mutation>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut mutations = self.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));

        region_stream_for_keys(mutations.into_iter(), pd_client.clone())
            .flat_map(|result| match result {
                Ok((mutations, region)) => stream::iter(kvrpcpb::FlushRequest::batches(
                    mutations,
                    TXN_COMMIT_BATCH_SIZE,
                ))
                .map(move |batch| Ok((batch, region.clone())))
                .boxed(),
                Err(e) => stream::iter(Err(e)).boxed(),
            })
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.mutations = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        store.apply_to_request(self)
    }
}

impl Batchable for kvrpcpb::FlushRequest {
    type Item = kvrpcpb::Mutation;

    fn item_size(item: &Self::Item) -> u64 {
        let mut size = item.key.len() as u64;
        size += item.value.len() as u64;
        size
    }
}

pub fn new_commit_request(
    keys: Vec<Vec<u8>>,
    start_version: u64,
    commit_version: u64,
) -> kvrpcpb::CommitRequest {
    let mut req = kvrpcpb::CommitRequest::default();
    req.keys = keys;
    req.start_version = start_version;
    req.commit_version = commit_version;

    req
}

impl KvRequest for kvrpcpb::CommitRequest {
    type Response = kvrpcpb::CommitResponse;
}

impl Shardable for kvrpcpb::CommitRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut keys = self.keys.clone();
        keys.sort();

        region_stream_for_keys(keys.into_iter(), pd_client.clone())
            .flat_map(|result| match result {
                Ok((keys, region)) => {
                    stream::iter(kvrpcpb::CommitRequest::batches(keys, TXN_COMMIT_BATCH_SIZE))
                        .map(move |batch| Ok((batch, region.clone())))
                        .boxed()
                }
                Err(e) => stream::iter(Err(e)).boxed(),
            })
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.keys = shard.into_iter().map(Into::into).collect();
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        store.apply_to_request(self)
    }
}

impl Batchable for kvrpcpb::CommitRequest {
    type Item = Vec<u8>;

    fn item_size(item: &Self::Item) -> u64 {
        item.len() as u64
    }
}

pub fn new_batch_rollback_request(
    keys: Vec<Vec<u8>>,
    start_version: u64,
) -> kvrpcpb::BatchRollbackRequest {
    let mut req = kvrpcpb::BatchRollbackRequest::default();
    req.keys = keys;
    req.start_version = start_version;

    req
}

impl KvRequest for kvrpcpb::BatchRollbackRequest {
    type Response = kvrpcpb::BatchRollbackResponse;
}

shardable_keys!(kvrpcpb::BatchRollbackRequest);

pub fn new_pessimistic_rollback_request(
    keys: Vec<Vec<u8>>,
    start_version: u64,
    for_update_ts: u64,
) -> kvrpcpb::PessimisticRollbackRequest {
    let mut req = kvrpcpb::PessimisticRollbackRequest::default();
    req.keys = keys;
    req.start_version = start_version;
    req.for_update_ts = for_update_ts;

    req
}

impl KvRequest for kvrpcpb::PessimisticRollbackRequest {
    type Response = kvrpcpb::PessimisticRollbackResponse;
}

shardable_keys!(kvrpcpb::PessimisticRollbackRequest);

pub fn new_pessimistic_lock_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Vec<u8>,
    start_version: u64,
    lock_ttl: u64,
    for_update_ts: u64,
    is_first_lock: bool,
    options: LockOptions,
) -> kvrpcpb::PessimisticLockRequest {
    let mut req = kvrpcpb::PessimisticLockRequest::default();
    req.mutations = mutations;
    req.primary_lock = primary_lock;
    req.start_version = start_version;
    req.lock_ttl = lock_ttl;
    req.for_update_ts = for_update_ts;
    req.is_first_lock = is_first_lock;
    req.wait_timeout = options.wait_timeout_ms_value();
    req.return_values = options.return_values_value();
    req.check_existence = options.check_existence_value();
    req.lock_only_if_exists = options.lock_only_if_exists_value();
    req.wake_up_mode = options.wake_up_mode_value().into();
    // Match client-go: ensure commit_ts > for_update_ts for pessimistic lock requests.
    req.min_commit_ts = for_update_ts.saturating_add(1);

    req
}

impl KvRequest for kvrpcpb::PessimisticLockRequest {
    type Response = kvrpcpb::PessimisticLockResponse;
}

impl Shardable for kvrpcpb::PessimisticLockRequest {
    type Shard = Vec<kvrpcpb::Mutation>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut mutations = self.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));
        region_stream_for_keys(mutations.into_iter(), pd_client.clone())
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.mutations = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        store.apply_to_request(self)
    }
}

// PessimisticLockResponse returns values that preserves the order with keys in request, thus the
// kvpair result should be produced by zipping the keys in request and the values in respponse.
impl Merge<ResponseWithShard<kvrpcpb::PessimisticLockResponse, Vec<kvrpcpb::Mutation>>>
    for CollectWithShard
{
    type Out = Vec<KvPair>;

    fn merge(
        &self,
        input: Vec<
            Result<ResponseWithShard<kvrpcpb::PessimisticLockResponse, Vec<kvrpcpb::Mutation>>>,
        >,
    ) -> Result<Self::Out> {
        if input.iter().any(Result::is_err) {
            let (success, mut errors): (Vec<_>, Vec<_>) =
                input.into_iter().partition(Result::is_ok);
            let first_err = errors.pop().unwrap();
            let success_keys = success
                .into_iter()
                .map(Result::unwrap)
                .flat_map(|ResponseWithShard(_resp, mutations)| {
                    mutations.into_iter().map(|m| m.key)
                })
                .collect();
            Err(PessimisticLockError {
                inner: Box::new(first_err.unwrap_err()),
                success_keys,
            })
        } else {
            let mut out = Vec::new();
            for ResponseWithShard(resp, mutations) in input.into_iter().map(Result::unwrap) {
                if !resp.results.is_empty() {
                    if resp.results.len() != mutations.len() {
                        return Err(crate::Error::InternalError {
                            message: format!(
                                "PessimisticLockResponse.results length mismatch: mutations={}, results={}",
                                mutations.len(),
                                resp.results.len()
                            ),
                        });
                    }
                    for (mutation, result) in mutations.into_iter().zip(resp.results.into_iter()) {
                        let result_type =
                            kvrpcpb::PessimisticLockKeyResultType::try_from(result.r#type)
                                .unwrap_or(kvrpcpb::PessimisticLockKeyResultType::LockResultFailed);
                        match result_type {
                            kvrpcpb::PessimisticLockKeyResultType::LockResultNormal
                            | kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict => {
                                if result.existence {
                                    out.push(KvPair::new(mutation.key, result.value));
                                }
                            }
                            kvrpcpb::PessimisticLockKeyResultType::LockResultFailed => {}
                        }
                    }
                    continue;
                }

                let mut values = resp.values;
                let not_founds = resp.not_founds;

                // When `return_values` is false, TiKV may return an empty `values` list even if
                // `check_existence` is true (in which case `not_founds` is populated). To keep
                // merging logic uniform and avoid panics, treat missing values as empty bytes.
                if values.is_empty() && !mutations.is_empty() {
                    values = iter::repeat_with(Vec::new).take(mutations.len()).collect();
                }
                if values.len() != mutations.len() {
                    return Err(crate::Error::InternalError {
                        message: format!(
                            "PessimisticLockResponse.values length mismatch: mutations={}, values={}",
                            mutations.len(),
                            values.len()
                        ),
                    });
                }
                if !not_founds.is_empty() && not_founds.len() != mutations.len() {
                    return Err(crate::Error::InternalError {
                        message: format!(
                            "PessimisticLockResponse.not_founds length mismatch: mutations={}, not_founds={}",
                            mutations.len(),
                            not_founds.len()
                        ),
                    });
                }

                if not_founds.is_empty() {
                    // Legacy TiKV does not distinguish not existing key and existing key with an
                    // empty value. We assume that key does not exist if the returned value is
                    // empty.
                    for (mutation, value) in mutations.into_iter().zip(values) {
                        if !value.is_empty() {
                            out.push(KvPair::new(mutation.key, value));
                        }
                    }
                } else {
                    for ((mutation, value), not_found) in
                        mutations.into_iter().zip(values).zip(not_founds)
                    {
                        if !not_found {
                            out.push(KvPair::new(mutation.key, value));
                        }
                    }
                }
            }

            Ok(out)
        }
    }
}

pub fn new_scan_lock_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    safepoint: u64,
    limit: u32,
) -> kvrpcpb::ScanLockRequest {
    let mut req = kvrpcpb::ScanLockRequest::default();
    req.start_key = start_key;
    req.end_key = end_key;
    req.max_version = safepoint;
    req.limit = limit;
    req
}

impl KvRequest for kvrpcpb::ScanLockRequest {
    type Response = kvrpcpb::ScanLockResponse;
}

impl Shardable for kvrpcpb::ScanLockRequest {
    type Shard = (Vec<u8>, Vec<u8>);

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        region_stream_for_range(
            (self.start_key.clone(), self.end_key.clone()),
            pd_client.clone(),
        )
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.start_key = shard.0;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        store.apply_to_request(self)
    }
}

impl HasNextBatch for kvrpcpb::ScanLockResponse {
    fn has_next_batch(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.locks.last().map(|lock| {
            // TODO: if last key is larger or equal than ScanLockRequest.end_key, return None.
            let mut start_key: Vec<u8> = lock.key.clone();
            start_key.push(0);
            (start_key, vec![])
        })
    }
}

impl NextBatch for kvrpcpb::ScanLockRequest {
    fn next_batch(&mut self, range: (Vec<u8>, Vec<u8>)) {
        self.start_key = range.0;
    }
}

impl Merge<kvrpcpb::ScanLockResponse> for Collect {
    type Out = Vec<kvrpcpb::LockInfo>;

    fn merge(&self, input: Vec<Result<kvrpcpb::ScanLockResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|mut resp| resp.take_locks().into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_heart_beat_request(
    start_ts: u64,
    primary_lock: Vec<u8>,
    ttl: u64,
) -> kvrpcpb::TxnHeartBeatRequest {
    let mut req = kvrpcpb::TxnHeartBeatRequest::default();
    req.start_version = start_ts;
    req.primary_lock = primary_lock;
    req.advise_lock_ttl = ttl;
    req
}

impl KvRequest for kvrpcpb::TxnHeartBeatRequest {
    type Response = kvrpcpb::TxnHeartBeatResponse;
}

impl Shardable for kvrpcpb::TxnHeartBeatRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        region_stream_for_keys(std::iter::once(self.key().clone()), pd_client.clone())
    }

    fn apply_shard(&mut self, mut shard: Self::Shard) {
        assert!(shard.len() == 1);
        self.primary_lock = shard.pop().unwrap();
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        store.apply_to_request(self)
    }
}

collect_single!(TxnHeartBeatResponse);

impl SingleKey for kvrpcpb::TxnHeartBeatRequest {
    fn key(&self) -> &Vec<u8> {
        &self.primary_lock
    }
}

impl Process<kvrpcpb::TxnHeartBeatResponse> for DefaultProcessor {
    type Out = u64;

    fn process(&self, input: Result<kvrpcpb::TxnHeartBeatResponse>) -> Result<Self::Out> {
        Ok(input?.lock_ttl)
    }
}

pub fn new_check_txn_status_request(
    primary_key: Vec<u8>,
    lock_ts: u64,
    caller_start_ts: u64,
    current_ts: u64,
    rollback_if_not_exist: bool,
    force_sync_commit: bool,
    resolving_pessimistic_lock: bool,
) -> kvrpcpb::CheckTxnStatusRequest {
    let mut req = kvrpcpb::CheckTxnStatusRequest::default();
    req.primary_key = primary_key;
    req.lock_ts = lock_ts;
    req.caller_start_ts = caller_start_ts;
    req.current_ts = current_ts;
    req.rollback_if_not_exist = rollback_if_not_exist;
    req.force_sync_commit = force_sync_commit;
    req.resolving_pessimistic_lock = resolving_pessimistic_lock;
    req
}

impl KvRequest for kvrpcpb::CheckTxnStatusRequest {
    type Response = kvrpcpb::CheckTxnStatusResponse;
}

impl Shardable for kvrpcpb::CheckTxnStatusRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        region_stream_for_keys(std::iter::once(self.key().clone()), pd_client.clone())
    }

    fn apply_shard(&mut self, mut shard: Self::Shard) {
        assert!(shard.len() == 1);
        self.primary_key = shard.pop().unwrap();
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        store.apply_to_request(self)
    }
}

impl SingleKey for kvrpcpb::CheckTxnStatusRequest {
    fn key(&self) -> &Vec<u8> {
        &self.primary_key
    }
}

collect_single!(kvrpcpb::CheckTxnStatusResponse);

impl Process<kvrpcpb::CheckTxnStatusResponse> for DefaultProcessor {
    type Out = TransactionStatus;

    fn process(&self, input: Result<kvrpcpb::CheckTxnStatusResponse>) -> Result<Self::Out> {
        Ok(input?.into())
    }
}

#[derive(Debug, Clone)]
pub struct TransactionStatus {
    pub kind: TransactionStatusKind,
    pub action: kvrpcpb::Action,
    pub is_expired: bool, // Available only when kind is Locked.
}

impl From<kvrpcpb::CheckTxnStatusResponse> for TransactionStatus {
    fn from(mut resp: kvrpcpb::CheckTxnStatusResponse) -> TransactionStatus {
        TransactionStatus {
            action: Action::try_from(resp.action).unwrap(),
            kind: (resp.commit_version, resp.lock_ttl, resp.lock_info.take()).into(),
            is_expired: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransactionStatusKind {
    Committed(Timestamp),
    RolledBack,
    Locked(u64, kvrpcpb::LockInfo), // None of ttl means expired.
}

impl TransactionStatus {
    pub fn check_ttl(&mut self, current: Timestamp) {
        if let TransactionStatusKind::Locked(ref ttl, ref lock_info) = self.kind {
            if current.physical - Timestamp::from_version(lock_info.lock_version).physical
                >= *ttl as i64
            {
                self.is_expired = true
            }
        }
    }

    // is_cacheable checks whether the transaction status is certain.
    // If transaction is already committed, the result could be cached.
    // Otherwise:
    //   If l.LockType is pessimistic lock type:
    //       - if its primary lock is pessimistic too, the check txn status result should not be cached.
    //       - if its primary lock is prewrite lock type, the check txn status could be cached.
    //   If l.lockType is prewrite lock type:
    //       - always cache the check txn status result.
    // For prewrite locks, their primary keys should ALWAYS be the correct one and will NOT change.
    pub fn is_cacheable(&self) -> bool {
        match &self.kind {
            TransactionStatusKind::RolledBack | TransactionStatusKind::Committed(..) => true,
            TransactionStatusKind::Locked(..) if self.is_expired => matches!(
                self.action,
                kvrpcpb::Action::NoAction
                    | kvrpcpb::Action::LockNotExistRollback
                    | kvrpcpb::Action::TtlExpireRollback
            ),
            _ => false,
        }
    }
}

impl From<(u64, u64, Option<kvrpcpb::LockInfo>)> for TransactionStatusKind {
    fn from((ts, ttl, info): (u64, u64, Option<kvrpcpb::LockInfo>)) -> TransactionStatusKind {
        match (ts, ttl, info) {
            (0, 0, None) => TransactionStatusKind::RolledBack,
            (ts, 0, None) => TransactionStatusKind::Committed(Timestamp::from_version(ts)),
            (0, ttl, Some(info)) => TransactionStatusKind::Locked(ttl, info),
            _ => unreachable!(),
        }
    }
}

pub fn new_check_secondary_locks_request(
    keys: Vec<Vec<u8>>,
    start_version: u64,
) -> kvrpcpb::CheckSecondaryLocksRequest {
    let mut req = kvrpcpb::CheckSecondaryLocksRequest::default();
    req.keys = keys;
    req.start_version = start_version;
    req
}

impl KvRequest for kvrpcpb::CheckSecondaryLocksRequest {
    type Response = kvrpcpb::CheckSecondaryLocksResponse;
}

shardable_keys!(kvrpcpb::CheckSecondaryLocksRequest);

impl Merge<kvrpcpb::CheckSecondaryLocksResponse> for Collect {
    type Out = SecondaryLocksStatus;

    fn merge(&self, input: Vec<Result<kvrpcpb::CheckSecondaryLocksResponse>>) -> Result<Self::Out> {
        let mut out = SecondaryLocksStatus {
            commit_ts: None,
            min_commit_ts: 0,
            fallback_2pc: false,
        };
        for resp in input {
            let resp = resp?;
            for lock in resp.locks.into_iter() {
                if !lock.use_async_commit {
                    out.fallback_2pc = true;
                    return Ok(out);
                }
                out.min_commit_ts = cmp::max(out.min_commit_ts, lock.min_commit_ts);
            }
            out.commit_ts = match (
                out.commit_ts.take(),
                Timestamp::try_from_version(resp.commit_ts),
            ) {
                (Some(a), Some(b)) => {
                    assert_eq!(a, b);
                    Some(a)
                }
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
        }
        Ok(out)
    }
}

pub struct SecondaryLocksStatus {
    pub commit_ts: Option<Timestamp>,
    pub min_commit_ts: u64,
    pub fallback_2pc: bool,
}

pair_locks!(kvrpcpb::BatchGetResponse);
pair_locks!(kvrpcpb::ScanResponse);
error_locks!(kvrpcpb::GetResponse);
error_locks!(kvrpcpb::ResolveLockResponse);
error_locks!(kvrpcpb::CommitResponse);
error_locks!(kvrpcpb::BatchRollbackResponse);
error_locks!(kvrpcpb::TxnHeartBeatResponse);
error_locks!(kvrpcpb::CheckTxnStatusResponse);
error_locks!(kvrpcpb::CheckSecondaryLocksResponse);

impl HasLocks for kvrpcpb::CleanupResponse {}

impl HasLocks for kvrpcpb::ScanLockResponse {
    fn take_locks(&mut self) -> Vec<LockInfo> {
        std::mem::take(&mut self.locks)
    }
}

impl HasLocks for kvrpcpb::PessimisticRollbackResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

impl HasLocks for kvrpcpb::PessimisticLockResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

impl HasLocks for kvrpcpb::PrewriteResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

impl HasLocks for kvrpcpb::FlushResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

pub fn new_unsafe_destroy_range_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
) -> kvrpcpb::UnsafeDestroyRangeRequest {
    let mut req = kvrpcpb::UnsafeDestroyRangeRequest::default();
    req.start_key = start_key;
    req.end_key = end_key;
    req
}

impl KvRequest for kvrpcpb::UnsafeDestroyRangeRequest {
    type Response = kvrpcpb::UnsafeDestroyRangeResponse;
}

impl StoreRequest for kvrpcpb::UnsafeDestroyRangeRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::UnsafeDestroyRangeResponse {}

impl Merge<kvrpcpb::UnsafeDestroyRangeResponse> for Collect {
    type Out = ();

    fn merge(&self, input: Vec<Result<kvrpcpb::UnsafeDestroyRangeResponse>>) -> Result<Self::Out> {
        let _: Vec<kvrpcpb::UnsafeDestroyRangeResponse> =
            input.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::common::Error::PessimisticLockError;
    use crate::common::Error::ResolveLockError;
    use crate::mock::MockPdClient;
    use crate::pd::PdClient;
    use crate::proto::kvrpcpb;
    use crate::request::plan::Merge;
    use crate::request::CollectWithShard;
    use crate::request::ResponseWithShard;
    use crate::request::Shardable;
    use crate::KvPair;

    #[tokio::test]
    async fn test_prewrite_txn_size_is_region_key_count_and_retry_disables_lite() {
        let pd = Arc::new(MockPdClient::default());

        let keys: Vec<Vec<u8>> = vec![
            vec![10],
            vec![2],
            vec![1],
            vec![11],
            vec![5],
            vec![12],
            vec![4],
            vec![3],
        ];
        let primary_lock = vec![1];
        let secondaries = keys
            .iter()
            .filter(|k| k.as_slice() != primary_lock.as_slice())
            .cloned()
            .collect::<Vec<_>>();
        let mutations = keys
            .iter()
            .map(|k| kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: k.clone(),
                value: vec![0],
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let mut req = super::new_prewrite_request(mutations, primary_lock.clone(), 42, 100);
        // Cover async-commit + 1PC flag handling in sharding.
        req.use_async_commit = true;
        req.try_one_pc = true;
        req.secondaries = secondaries;

        let shards = req.shards(&pd).collect::<Vec<_>>().await;
        assert_eq!(
            shards.len(),
            2,
            "expected 2 regions (mock region1 + region2)"
        );

        let mut saw_primary = false;
        for shard in shards {
            let (shard, region) = shard.unwrap();
            let expected_txn_size = match region.region.id {
                1 => 5, // keys < 10
                2 => 3, // keys in [10, 250..)
                other => panic!("unexpected region id {other}"),
            };
            assert_eq!(
                shard.txn_size, expected_txn_size,
                "unexpected txn_size for region {}",
                region.region.id
            );

            let mut sharded_req = req.clone_then_apply_shard(shard.clone());
            assert_eq!(sharded_req.txn_size, shard.txn_size);

            let is_primary_shard = shard
                .mutations
                .iter()
                .any(|m| m.key.as_slice() == primary_lock.as_slice());
            if is_primary_shard {
                saw_primary = true;
                assert!(
                    !sharded_req.secondaries.is_empty(),
                    "primary prewrite should include secondaries"
                );
            } else {
                assert!(
                    sharded_req.secondaries.is_empty(),
                    "secondary prewrite should not include secondaries"
                );
            }
            assert!(
                !sharded_req.try_one_pc,
                "1PC must be disabled when prewrite is split into multiple requests"
            );

            // attempt=0 keeps region-level txn_size
            let mut store = pd
                .clone()
                .map_region_to_store(region.clone())
                .await
                .unwrap();
            store.attempt = 0;
            sharded_req.apply_store(&store).unwrap();
            assert_eq!(
                sharded_req.txn_size, expected_txn_size,
                "attempt=0 must preserve region txn_size"
            );

            // attempt>0 disables resolve-lock-lite by forcing txn_size to MAX
            store.attempt = 1;
            let mut retry_req = sharded_req.clone();
            retry_req.apply_store(&store).unwrap();
            assert_eq!(
                retry_req.txn_size,
                u64::MAX,
                "attempt>0 must disable resolve-lock-lite by forcing txn_size to MAX"
            );
        }
        assert!(saw_primary, "must include a primary prewrite shard");
    }

    #[tokio::test]
    async fn test_pessimistic_prewrite_txn_size_is_region_key_count() {
        let pd = Arc::new(MockPdClient::default());

        let keys: Vec<Vec<u8>> = vec![vec![1], vec![2], vec![3], vec![10], vec![11]];
        let mutations = keys
            .iter()
            .map(|k| kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: k.clone(),
                value: vec![0],
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let req = super::new_pessimistic_prewrite_request(mutations, vec![1], 42, 100, 44);
        let shards = req.shards(&pd).collect::<Vec<_>>().await;
        assert_eq!(shards.len(), 2, "expected 2 regions");

        for shard in shards {
            let (shard, region) = shard.unwrap();
            let expected_txn_size = match region.region.id {
                1 => 3, // keys < 10
                2 => 2, // keys in [10, 250..)
                other => panic!("unexpected region id {other}"),
            };
            assert_eq!(shard.txn_size, expected_txn_size);
        }
    }

    #[tokio::test]
    async fn test_merge_pessimistic_lock_response() {
        let (key1, key2, key3, key4) = (b"key1", b"key2", b"key3", b"key4");
        let (value1, value4) = (b"value1", b"value4");
        let value_empty = b"";

        let resp1 = ResponseWithShard(
            kvrpcpb::PessimisticLockResponse {
                values: vec![value1.to_vec()],
                ..Default::default()
            },
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::PessimisticLock.into(),
                key: key1.to_vec(),
                ..Default::default()
            }],
        );

        let resp_empty_value = ResponseWithShard(
            kvrpcpb::PessimisticLockResponse {
                values: vec![value_empty.to_vec()],
                ..Default::default()
            },
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::PessimisticLock.into(),
                key: key2.to_vec(),
                ..Default::default()
            }],
        );

        let resp_not_found = ResponseWithShard(
            kvrpcpb::PessimisticLockResponse {
                values: vec![value_empty.to_vec(), value4.to_vec()],
                not_founds: vec![true, false],
                ..Default::default()
            },
            vec![
                kvrpcpb::Mutation {
                    op: kvrpcpb::Op::PessimisticLock.into(),
                    key: key3.to_vec(),
                    ..Default::default()
                },
                kvrpcpb::Mutation {
                    op: kvrpcpb::Op::PessimisticLock.into(),
                    key: key4.to_vec(),
                    ..Default::default()
                },
            ],
        );

        let merger = CollectWithShard {};
        {
            // empty values & not founds are filtered.
            let input = vec![
                Ok(resp1.clone()),
                Ok(resp_empty_value.clone()),
                Ok(resp_not_found.clone()),
            ];
            let result = merger.merge(input);

            assert_eq!(
                result.unwrap(),
                vec![
                    KvPair::new(key1.to_vec(), value1.to_vec()),
                    KvPair::new(key4.to_vec(), value4.to_vec()),
                ]
            );
        }
        {
            let input = vec![
                Ok(resp1),
                Ok(resp_empty_value),
                Err(ResolveLockError(vec![])),
                Ok(resp_not_found),
            ];
            let result = merger.merge(input);

            if let PessimisticLockError {
                inner,
                success_keys,
            } = result.unwrap_err()
            {
                assert!(matches!(*inner, ResolveLockError(_)));
                assert_eq!(
                    success_keys,
                    vec![key1.to_vec(), key2.to_vec(), key3.to_vec(), key4.to_vec()]
                );
            } else {
                panic!();
            }
        }
    }
}
