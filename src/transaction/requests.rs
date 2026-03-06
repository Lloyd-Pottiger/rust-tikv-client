// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::iter;
use std::sync::Arc;

use either::Either;
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
use crate::store::Request;
use crate::store::Store;
use crate::store::{region_stream_for_keys, region_stream_for_range};
use crate::timestamp::TimestampExt;
use crate::transaction::requests::kvrpcpb::prewrite_request::PessimisticAction;
use crate::transaction::HasLocks;
use crate::util::iter::FlatMapOkIterExt;
use crate::Key;
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
    is_txn_file: bool,
) -> kvrpcpb::ResolveLockRequest {
    let mut req = kvrpcpb::ResolveLockRequest::default();
    req.start_version = start_version;
    req.commit_version = commit_version;
    req.is_txn_file = is_txn_file;
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

pub fn new_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Vec<u8>,
    start_version: u64,
    lock_ttl: u64,
) -> kvrpcpb::PrewriteRequest {
    let mut req = kvrpcpb::PrewriteRequest::default();
    req.txn_size = mutations.len() as u64;
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

#[derive(Debug, Clone)]
pub struct PrewriteRequestShard {
    mutations: Vec<kvrpcpb::Mutation>,
    pessimistic_actions: Vec<i32>,
    for_update_ts_constraints: Vec<kvrpcpb::prewrite_request::ForUpdateTsConstraint>,
    txn_size: u64,
}

#[derive(Debug, Clone)]
struct PrewriteMutation {
    key: Key,
    mutation: kvrpcpb::Mutation,
    pessimistic_action: Option<i32>,
    expected_for_update_ts: Option<u64>,
}

impl AsRef<Key> for PrewriteMutation {
    fn as_ref(&self) -> &Key {
        &self.key
    }
}

impl Shardable for kvrpcpb::PrewriteRequest {
    type Shard = PrewriteRequestShard;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let pessimistic_action_enabled = !self.pessimistic_actions.is_empty();
        debug_assert!(
            !pessimistic_action_enabled || self.pessimistic_actions.len() == self.mutations.len()
        );

        let mut expected_for_update_ts_by_index = vec![None; self.mutations.len()];
        for constraint in &self.for_update_ts_constraints {
            let index = constraint.index as usize;
            if index < expected_for_update_ts_by_index.len() {
                expected_for_update_ts_by_index[index] = Some(constraint.expected_for_update_ts);
            }
        }

        let mut mutations = self
            .mutations
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, mutation)| PrewriteMutation {
                key: Key::from(mutation.key.clone()),
                pessimistic_action: pessimistic_action_enabled
                    .then(|| self.pessimistic_actions[index]),
                expected_for_update_ts: expected_for_update_ts_by_index[index],
                mutation,
            })
            .collect::<Vec<_>>();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));

        region_stream_for_keys(mutations.into_iter(), pd_client.clone())
            .flat_map(
                move |result: Result<(Vec<PrewriteMutation>, RegionWithLeader)>| match result {
                    Ok((mutations, region)) => {
                        let region_txn_size = mutations.len() as u64;
                        let batches = {
                            let mut batches: Vec<Vec<PrewriteMutation>> = Vec::new();
                            let mut batch: Vec<PrewriteMutation> = Vec::new();
                            let mut size = 0;

                            for item in mutations {
                                let item_size = item.mutation.key.len() as u64
                                    + item.mutation.value.len() as u64;
                                if size + item_size >= TXN_COMMIT_BATCH_SIZE && !batch.is_empty() {
                                    batches.push(batch);
                                    batch = Vec::new();
                                    size = 0;
                                }
                                size += item_size;
                                batch.push(item);
                            }
                            if !batch.is_empty() {
                                batches.push(batch)
                            }
                            batches
                        };

                        stream::iter(batches)
                            .map(move |batch| {
                                let mut mutations = Vec::with_capacity(batch.len());
                                let mut pessimistic_actions = pessimistic_action_enabled
                                    .then(|| Vec::with_capacity(batch.len()))
                                    .unwrap_or_default();
                                let mut for_update_ts_constraints = Vec::new();

                                for (index, item) in batch.into_iter().enumerate() {
                                    mutations.push(item.mutation);
                                    if pessimistic_action_enabled {
                                        pessimistic_actions.push(item.pessimistic_action.unwrap());
                                    }
                                    if let Some(expected_for_update_ts) =
                                        item.expected_for_update_ts
                                    {
                                        for_update_ts_constraints.push(
                                            kvrpcpb::prewrite_request::ForUpdateTsConstraint {
                                                index: index as u32,
                                                expected_for_update_ts,
                                            },
                                        );
                                    }
                                }

                                Ok((
                                    PrewriteRequestShard {
                                        mutations,
                                        pessimistic_actions,
                                        for_update_ts_constraints,
                                        txn_size: region_txn_size,
                                    },
                                    region.clone(),
                                ))
                            })
                            .boxed()
                    }
                    Err(e) => stream::iter(iter::once(Err(e))).boxed(),
                },
            )
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        let shard_contains_primary = shard.mutations.iter().any(|m| m.key == self.primary_lock);

        // Only need to set secondary keys if we're sending the primary key.
        if self.use_async_commit && !shard_contains_primary {
            self.secondaries = vec![];
        }

        // Only if there is only one request to send.
        if self.try_one_pc && shard.mutations.len() != self.mutations.len() {
            self.try_one_pc = false;
        }

        self.txn_size = shard.txn_size;
        self.mutations = shard.mutations;
        self.pessimistic_actions = shard.pessimistic_actions;
        self.for_update_ts_constraints = shard.for_update_ts_constraints;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.region_with_leader)
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
                Err(e) => stream::iter(iter::once(Err(e))).boxed(),
            })
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.keys = shard.into_iter().map(Into::into).collect();
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.region_with_leader)
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
    need_value: bool,
) -> kvrpcpb::PessimisticLockRequest {
    let mut req = kvrpcpb::PessimisticLockRequest::default();
    req.mutations = mutations;
    req.primary_lock = primary_lock;
    req.start_version = start_version;
    req.lock_ttl = lock_ttl;
    req.for_update_ts = for_update_ts;
    // FIXME: make them configurable
    req.is_first_lock = false;
    req.wait_timeout = 0;
    req.return_values = need_value;
    // FIXME: support large transaction
    req.min_commit_ts = 0;

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
        self.set_leader(&store.region_with_leader)
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
            Ok(input
                .into_iter()
                .map(Result::unwrap)
                .flat_map(|ResponseWithShard(resp, mutations)| {
                    let values: Vec<Vec<u8>> = resp.values;
                    let values_len = values.len();
                    let not_founds = resp.not_founds;
                    let kvpairs = mutations
                        .into_iter()
                        .map(|m| m.key)
                        .zip(values)
                        .map(KvPair::from);
                    assert_eq!(kvpairs.len(), values_len);
                    if not_founds.is_empty() {
                        // Legacy TiKV does not distinguish not existing key and existing key
                        // that with empty value. We assume that key does not exist if value
                        // is empty.
                        Either::Left(kvpairs.filter(|kvpair| !kvpair.value().is_empty()))
                    } else {
                        assert_eq!(kvpairs.len(), not_founds.len());
                        Either::Right(kvpairs.zip(not_founds).filter_map(|(kvpair, not_found)| {
                            if not_found {
                                None
                            } else {
                                Some(kvpair)
                            }
                        }))
                    }
                })
                .collect())
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
        self.set_leader(&store.region_with_leader)
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
        self.set_leader(&store.region_with_leader)
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

#[allow(clippy::too_many_arguments)]
pub fn new_check_txn_status_request(
    primary_key: Vec<u8>,
    lock_ts: u64,
    caller_start_ts: u64,
    current_ts: u64,
    rollback_if_not_exist: bool,
    force_sync_commit: bool,
    resolving_pessimistic_lock: bool,
    is_txn_file: bool,
) -> kvrpcpb::CheckTxnStatusRequest {
    let mut req = kvrpcpb::CheckTxnStatusRequest::default();
    req.primary_key = primary_key;
    req.lock_ts = lock_ts;
    req.caller_start_ts = caller_start_ts;
    req.current_ts = current_ts;
    req.rollback_if_not_exist = rollback_if_not_exist;
    req.force_sync_commit = force_sync_commit;
    req.resolving_pessimistic_lock = resolving_pessimistic_lock;
    req.verify_is_primary = true;
    req.is_txn_file = is_txn_file;
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
        self.set_leader(&store.region_with_leader)
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
        input?.try_into()
    }
}

#[derive(Debug, Clone)]
pub struct TransactionStatus {
    pub kind: TransactionStatusKind,
    pub action: kvrpcpb::Action,
    pub is_expired: bool, // Available only when kind is Locked.
}

impl TryFrom<kvrpcpb::CheckTxnStatusResponse> for TransactionStatus {
    type Error = crate::common::Error;

    fn try_from(mut resp: kvrpcpb::CheckTxnStatusResponse) -> Result<Self> {
        let action = Action::try_from(resp.action).map_err(|value| {
            crate::common::Error::StringError(format!(
                "check_txn_status returned unknown action value {value}"
            ))
        })?;
        let kind = transaction_status_kind_from_parts(
            resp.commit_version,
            resp.lock_ttl,
            resp.lock_info.take(),
        )?;
        Ok(TransactionStatus {
            action,
            kind,
            is_expired: false,
        })
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

fn transaction_status_kind_from_parts(
    commit_version: u64,
    lock_ttl: u64,
    lock_info: Option<kvrpcpb::LockInfo>,
) -> Result<TransactionStatusKind> {
    match (commit_version, lock_ttl, lock_info) {
        (0, 0, None) => Ok(TransactionStatusKind::RolledBack),
        (ts, 0, None) if ts > 0 => Ok(TransactionStatusKind::Committed(Timestamp::from_version(ts))),
        (0, ttl, Some(info)) if ttl > 0 => Ok(TransactionStatusKind::Locked(ttl, info)),
        (ts, ttl, info) => Err(crate::common::Error::StringError(format!(
            "invalid check_txn_status response shape: commit_version={ts}, lock_ttl={ttl}, has_lock_info={}",
            info.is_some()
        ))),
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
            if let Some(commit_ts) = Timestamp::try_from_version(resp.commit_ts) {
                if let Some(existing) = out.commit_ts.as_ref() {
                    if existing != &commit_ts {
                        return Err(crate::Error::StringError(format!(
                            "check_secondary_locks commit_ts mismatch: {} vs {}",
                            existing.version(),
                            commit_ts.version(),
                        )));
                    }
                } else {
                    out.commit_ts = Some(commit_ts);
                }
            }

            if let Some(commit_ts) = out.commit_ts.as_ref() {
                if commit_ts.version() < out.min_commit_ts {
                    return Err(crate::Error::StringError(format!(
                        "check_secondary_locks commit_ts {} is less than min_commit_ts {}",
                        commit_ts.version(),
                        out.min_commit_ts,
                    )));
                }
            }
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

    use crate::common::Error;
    use crate::common::Error::PessimisticLockError;
    use crate::common::Error::ResolveLockError;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb;
    use crate::request::plan::Merge;
    use crate::request::CollectWithShard;
    use crate::request::Process;
    use crate::request::ResponseWithShard;
    use crate::request::Shardable;
    use crate::KvPair;

    #[test]
    fn test_process_check_txn_status_rejects_unknown_action_value() {
        let processor = super::DefaultProcessor;
        let err = processor
            .process(Ok(kvrpcpb::CheckTxnStatusResponse {
                action: i32::MAX,
                ..Default::default()
            }))
            .expect_err("unexpected action value should not panic");
        match err {
            Error::StringError(message) => {
                assert!(message.contains("unknown action value"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_process_check_txn_status_rejects_invalid_status_shape() {
        let processor = super::DefaultProcessor;
        let err = processor
            .process(Ok(kvrpcpb::CheckTxnStatusResponse {
                action: kvrpcpb::Action::NoAction.into(),
                commit_version: 100,
                lock_ttl: 1,
                ..Default::default()
            }))
            .expect_err("invalid status shape should not panic");
        match err {
            Error::StringError(message) => {
                assert!(message.contains("invalid check_txn_status response shape"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_merge_check_secondary_locks_rejects_commit_ts_mismatch() {
        let merger = super::Collect;
        let input = vec![
            Ok(kvrpcpb::CheckSecondaryLocksResponse {
                commit_ts: 100,
                ..Default::default()
            }),
            Ok(kvrpcpb::CheckSecondaryLocksResponse {
                commit_ts: 101,
                ..Default::default()
            }),
        ];

        let err = match merger.merge(input) {
            Ok(_) => panic!("expected commit_ts mismatch error"),
            Err(err) => err,
        };
        match err {
            Error::StringError(message) => {
                assert!(message.contains("commit_ts mismatch"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_merge_check_secondary_locks_rejects_commit_ts_below_min_commit_ts() {
        let merger = super::Collect;
        let input = vec![
            Ok(kvrpcpb::CheckSecondaryLocksResponse {
                locks: vec![kvrpcpb::LockInfo {
                    use_async_commit: true,
                    min_commit_ts: 120,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Ok(kvrpcpb::CheckSecondaryLocksResponse {
                commit_ts: 119,
                ..Default::default()
            }),
        ];

        let err = match merger.merge(input) {
            Ok(_) => panic!("expected commit_ts/min_commit_ts validation error"),
            Err(err) => err,
        };
        match err {
            Error::StringError(message) => {
                assert!(message.contains("less than min_commit_ts"));
            }
            other => panic!("unexpected error: {other:?}"),
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

    #[tokio::test]
    async fn test_prewrite_request_shards_set_txn_size_and_async_commit_secondaries() {
        let large_value = vec![0_u8; 8 * 1024];

        let primary = vec![1_u8];
        let key2 = vec![2_u8];
        let key3 = vec![3_u8];
        let key10 = vec![10_u8];

        // Intentionally unsorted to test mapping logic.
        let mutations = vec![
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: key2.clone(),
                value: large_value.clone(),
                ..Default::default()
            },
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: primary.clone(),
                value: large_value.clone(),
                ..Default::default()
            },
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: key10.clone(),
                value: vec![0],
                ..Default::default()
            },
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: key3.clone(),
                value: large_value,
                ..Default::default()
            },
        ];

        let mut req =
            super::new_pessimistic_prewrite_request(mutations, primary.clone(), 1, 100, 5);
        req.use_async_commit = true;
        req.try_one_pc = true;
        req.secondaries = vec![key2.clone(), key10.clone(), key3.clone()];
        req.for_update_ts_constraints = vec![kvrpcpb::prewrite_request::ForUpdateTsConstraint {
            index: 3,
            expected_for_update_ts: 123,
        }];

        let pd_client = Arc::new(MockPdClient::default());
        let shards = req
            .shards(&pd_client)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(shards.len(), 4);

        let do_pessimistic_check: i32 =
            kvrpcpb::prewrite_request::PessimisticAction::DoPessimisticCheck.into();

        let mut primary_shard_count = 0;
        for (shard, _) in shards {
            let mut shard_req = req.clone();
            shard_req.apply_shard(shard);

            assert!(!shard_req.mutations.is_empty());
            assert!(!shard_req.try_one_pc);
            assert_eq!(shard_req.pessimistic_actions, vec![do_pessimistic_check]);

            let shard_key = shard_req.mutations[0].key.as_slice();
            if shard_key == primary.as_slice() {
                primary_shard_count += 1;
                assert_eq!(shard_req.txn_size, 3);
                assert_eq!(
                    shard_req.secondaries,
                    vec![key2.clone(), key10.clone(), key3.clone()]
                );
                assert!(shard_req.for_update_ts_constraints.is_empty());
            } else if shard_key == key10.as_slice() {
                assert_eq!(shard_req.txn_size, 1);
                assert!(shard_req.secondaries.is_empty());
                assert!(shard_req.for_update_ts_constraints.is_empty());
            } else if shard_key == key2.as_slice() {
                assert_eq!(shard_req.txn_size, 3);
                assert!(shard_req.secondaries.is_empty());
                assert!(shard_req.for_update_ts_constraints.is_empty());
            } else if shard_key == key3.as_slice() {
                assert_eq!(shard_req.txn_size, 3);
                assert!(shard_req.secondaries.is_empty());
                assert_eq!(
                    shard_req.for_update_ts_constraints,
                    vec![kvrpcpb::prewrite_request::ForUpdateTsConstraint {
                        index: 0,
                        expected_for_update_ts: 123,
                    }]
                );
            } else {
                panic!("unexpected shard key: {:?}", shard_key);
            }
        }
        assert_eq!(primary_shard_count, 1);
    }
}
