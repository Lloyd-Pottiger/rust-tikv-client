// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::iter;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::stream::{self};
use futures::StreamExt;
use futures::TryStreamExt;

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
use crate::range_request;
use crate::region::RegionWithLeader;
use crate::request::Collect;
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
fn flatten_lock_info(lock: LockInfo) -> Vec<LockInfo> {
    if lock.shared_lock_infos.is_empty() {
        vec![lock]
    } else {
        lock.shared_lock_infos
    }
}

macro_rules! pair_locks {
    ($response_type:ty) => {
        impl HasLocks for $response_type {
            fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
                if self.pairs.is_empty() {
                    self.error
                        .as_mut()
                        .and_then(|error| error.locked.take())
                        .into_iter()
                        .flat_map(flatten_lock_info)
                        .collect()
                } else {
                    self.pairs
                        .iter_mut()
                        .filter_map(|pair| {
                            pair.error.as_mut().and_then(|error| error.locked.take())
                        })
                        .flat_map(flatten_lock_info)
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
                    .flat_map(flatten_lock_info)
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

pub fn new_buffer_batch_get_request(
    keys: Vec<Vec<u8>>,
    timestamp: u64,
) -> kvrpcpb::BufferBatchGetRequest {
    let mut req = kvrpcpb::BufferBatchGetRequest::default();
    req.keys = keys;
    req.version = timestamp;
    req
}

impl KvRequest for kvrpcpb::BufferBatchGetRequest {
    type Response = kvrpcpb::BufferBatchGetResponse;
}

shardable_keys!(kvrpcpb::BufferBatchGetRequest);

impl Merge<kvrpcpb::BufferBatchGetResponse> for Collect {
    type Out = Vec<KvPair>;

    fn merge(&self, input: Vec<Result<kvrpcpb::BufferBatchGetResponse>>) -> Result<Self::Out> {
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

/// A pipelined resolve-lock request that resolves locks for the given range.
///
/// `kvrpcpb::ResolveLockRequest` itself doesn't carry keys, so it cannot be sharded by region.
/// This wrapper provides the key range used for sharding while delegating the RPC request body.
#[derive(Clone, Debug)]
pub(crate) struct ResolveLockRangeRequest {
    inner: kvrpcpb::ResolveLockRequest,
    range_start: Key,
    range_end: Key,
}

impl ResolveLockRangeRequest {
    pub(crate) fn new(
        inner: kvrpcpb::ResolveLockRequest,
        range_start: Key,
        range_end: Key,
    ) -> ResolveLockRangeRequest {
        ResolveLockRangeRequest {
            inner,
            range_start,
            range_end,
        }
    }

    pub(crate) fn inner_mut(&mut self) -> &mut kvrpcpb::ResolveLockRequest {
        &mut self.inner
    }
}

#[async_trait]
impl Request for ResolveLockRangeRequest {
    async fn dispatch(
        &self,
        client: &crate::proto::tikvpb::tikv_client::TikvClient<tonic::transport::Channel>,
        timeout: std::time::Duration,
    ) -> Result<Box<dyn std::any::Any>> {
        self.inner.dispatch(client, timeout).await
    }

    fn label(&self) -> &'static str {
        self.inner.label()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        // Expose the inner request for dispatch hooks and tests.
        self.inner.as_any()
    }

    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
        self.inner.set_leader(leader)
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.inner.set_api_version(api_version)
    }

    fn set_is_retry_request(&mut self, is_retry_request: bool) {
        self.inner.set_is_retry_request(is_retry_request)
    }

    fn context(&self) -> Option<&kvrpcpb::Context> {
        self.inner.context()
    }

    fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.context_mut()
    }
}

impl KvRequest for ResolveLockRangeRequest {
    type Response = kvrpcpb::ResolveLockResponse;
}

impl Shardable for ResolveLockRangeRequest {
    type Shard = ();

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let range = (
            Vec::<u8>::from(self.range_start.clone()),
            Vec::<u8>::from(self.range_end.clone()),
        );

        region_stream_for_range(range, pd_client.clone())
            .map(|res| res.map(|(_range, region)| ((), region)))
            .boxed()
    }

    fn apply_shard(&mut self, _shard: Self::Shard) {}

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.inner.set_leader(&store.region_with_leader)
    }
}

/// A resolve-lock request that resolves locks for an explicit set of keys.
///
/// `kvrpcpb::ResolveLockRequest` itself doesn't have to carry keys (non-lite mode), so it cannot
/// always be sharded by region. This wrapper is intended for resolve-lock lite mode where keys are
/// present and can be grouped and sharded by region.
#[derive(Clone, Debug)]
pub(crate) struct ResolveLockKeysRequest {
    inner: kvrpcpb::ResolveLockRequest,
}

impl ResolveLockKeysRequest {
    pub(crate) fn new(
        inner: kvrpcpb::ResolveLockRequest,
        keys: Vec<Vec<u8>>,
    ) -> ResolveLockKeysRequest {
        let mut inner = inner;
        inner.keys = keys;
        ResolveLockKeysRequest { inner }
    }
}

#[async_trait]
impl Request for ResolveLockKeysRequest {
    async fn dispatch(
        &self,
        client: &crate::proto::tikvpb::tikv_client::TikvClient<tonic::transport::Channel>,
        timeout: std::time::Duration,
    ) -> Result<Box<dyn std::any::Any>> {
        self.inner.dispatch(client, timeout).await
    }

    fn label(&self) -> &'static str {
        self.inner.label()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        // Expose the inner request for dispatch hooks and tests.
        self.inner.as_any()
    }

    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
        self.inner.set_leader(leader)
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.inner.set_api_version(api_version)
    }

    fn set_is_retry_request(&mut self, is_retry_request: bool) {
        self.inner.set_is_retry_request(is_retry_request)
    }

    fn context(&self) -> Option<&kvrpcpb::Context> {
        self.inner.context()
    }

    fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.context_mut()
    }
}

impl KvRequest for ResolveLockKeysRequest {
    type Response = kvrpcpb::ResolveLockResponse;
}

impl Shardable for ResolveLockKeysRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut keys = self.inner.keys.clone();
        keys.sort();
        region_stream_for_keys(keys.into_iter(), pd_client.clone())
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.inner.keys = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.inner.set_leader(&store.region_with_leader)
    }
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

pub fn new_flush_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_key: Vec<u8>,
    start_ts: u64,
    min_commit_ts: u64,
    generation: u64,
    lock_ttl: u64,
    assertion_level: kvrpcpb::AssertionLevel,
) -> kvrpcpb::FlushRequest {
    let mut req = kvrpcpb::FlushRequest::default();
    req.mutations = mutations;
    req.primary_key = primary_key;
    req.start_ts = start_ts;
    req.min_commit_ts = min_commit_ts;
    req.generation = generation;
    req.lock_ttl = lock_ttl;
    req.assertion_level = assertion_level.into();
    req
}

impl KvRequest for kvrpcpb::FlushRequest {
    type Response = kvrpcpb::FlushResponse;
}

#[derive(Debug, Clone)]
pub struct FlushRequestShard {
    mutations: Vec<kvrpcpb::Mutation>,
}

#[derive(Debug, Clone)]
struct FlushMutation {
    key: Key,
    mutation: kvrpcpb::Mutation,
}

impl AsRef<Key> for FlushMutation {
    fn as_ref(&self) -> &Key {
        &self.key
    }
}

impl Shardable for kvrpcpb::FlushRequest {
    type Shard = FlushRequestShard;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut mutations = self
            .mutations
            .iter()
            .cloned()
            .map(|mutation| FlushMutation {
                key: Key::from(mutation.key.clone()),
                mutation,
            })
            .collect::<Vec<_>>();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));

        region_stream_for_keys(mutations.into_iter(), pd_client.clone())
            .flat_map(
                move |result: Result<(Vec<FlushMutation>, RegionWithLeader)>| match result {
                    Ok((mutations, region)) => {
                        let batches = {
                            let mut batches: Vec<Vec<FlushMutation>> = Vec::new();
                            let mut batch: Vec<FlushMutation> = Vec::new();
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
                                let mutations =
                                    batch.into_iter().map(|item| item.mutation).collect();
                                Ok((FlushRequestShard { mutations }, region.clone()))
                            })
                            .boxed()
                    }
                    Err(e) => stream::iter(iter::once(Err(e))).boxed(),
                },
            )
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.mutations = shard.mutations;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.region_with_leader)
    }
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
        if pessimistic_action_enabled && self.pessimistic_actions.len() != self.mutations.len() {
            return stream::iter(iter::once(Err(crate::Error::StringError(format!(
                "prewrite request pessimistic_actions length {} does not match mutations length {}",
                self.pessimistic_actions.len(),
                self.mutations.len(),
            )))))
            .boxed();
        }

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
                pessimistic_action: if pessimistic_action_enabled {
                    self.pessimistic_actions.get(index).copied()
                } else {
                    None
                },
                expected_for_update_ts: expected_for_update_ts_by_index[index],
                mutation,
            })
            .collect::<Vec<_>>();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));

        // Match client-go behavior: once a prewrite request is retried (e.g. region-miss retry),
        // set txn_size to a large value to avoid unexpected resolve-lock-lite.
        let is_retry_request = self
            .context
            .as_ref()
            .map(|ctx| ctx.is_retry_request)
            .unwrap_or(false);

        region_stream_for_keys(mutations.into_iter(), pd_client.clone())
            .flat_map(
                move |result: Result<(Vec<PrewriteMutation>, RegionWithLeader)>| match result {
                    Ok((mutations, region)) => {
                        let region_txn_size = if is_retry_request {
                            u64::MAX
                        } else {
                            mutations.len() as u64
                        };
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
                                        let action = item.pessimistic_action.ok_or_else(|| {
                                            crate::Error::StringError(
                                                "prewrite request missing pessimistic action for mutation"
                                                    .to_owned(),
                                            )
                                        })?;
                                        pessimistic_actions.push(action);
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
    is_first_lock: bool,
) -> kvrpcpb::PessimisticLockRequest {
    let mut req = kvrpcpb::PessimisticLockRequest::default();
    req.mutations = mutations;
    req.primary_lock = primary_lock;
    req.start_version = start_version;
    req.lock_ttl = lock_ttl;
    req.for_update_ts = for_update_ts;
    req.is_first_lock = is_first_lock;
    req.wait_timeout = 0;
    req.return_values = need_value;
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
        self.set_leader(&store.region_with_leader)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CollectPessimisticLock {
    need_value: bool,
}

impl CollectPessimisticLock {
    pub(super) fn new(need_value: bool) -> Self {
        Self { need_value }
    }
}

// PessimisticLockResponse returns values that preserves the order with keys in request, thus the
// kvpair result should be produced by zipping the keys in request and the values in response.
impl Merge<ResponseWithShard<kvrpcpb::PessimisticLockResponse, Vec<kvrpcpb::Mutation>>>
    for CollectPessimisticLock
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
            let last_err = match errors.pop() {
                Some(err) => match err {
                    Ok(_) => {
                        return Err(crate::Error::StringError(
                            "pessimistic lock merge error partition contained Ok".to_owned(),
                        ));
                    }
                    Err(err) => err,
                },
                None => {
                    return Err(crate::Error::StringError(
                        "pessimistic lock merge encountered an error but no errors were collected"
                            .to_owned(),
                    ));
                }
            };
            let success_keys = success
                .into_iter()
                .filter_map(Result::ok)
                .flat_map(|ResponseWithShard(_resp, mutations)| {
                    mutations.into_iter().map(|m| m.key)
                })
                .collect();
            Err(PessimisticLockError {
                inner: Box::new(last_err),
                success_keys,
            })
        } else {
            let mut out = Vec::new();
            for ResponseWithShard(resp, mutations) in input.into_iter().filter_map(Result::ok) {
                let values = resp.values;
                let not_founds = resp.not_founds;

                if !not_founds.is_empty() && not_founds.len() != mutations.len() {
                    return Err(crate::Error::StringError(format!(
                        "pessimistic lock response not_founds length {} does not match mutations length {}",
                        not_founds.len(),
                        mutations.len(),
                    )));
                }

                if values.is_empty() {
                    if self.need_value {
                        return Err(crate::Error::StringError(format!(
                            "pessimistic lock response missing values for {} mutations",
                            mutations.len()
                        )));
                    }
                    continue;
                }

                if values.len() != mutations.len() {
                    return Err(crate::Error::StringError(format!(
                        "pessimistic lock response values length {} does not match mutations length {}",
                        values.len(),
                        mutations.len(),
                    )));
                }

                if !self.need_value {
                    continue;
                }

                if not_founds.is_empty() {
                    // Legacy TiKV does not distinguish not existing key and existing key
                    // that with empty value. We assume that key does not exist if value
                    // is empty.
                    out.extend(
                        mutations
                            .into_iter()
                            .map(|m| m.key)
                            .zip(values)
                            .filter(|(_key, value)| !value.is_empty())
                            .map(|(key, value)| KvPair::new(key, value)),
                    );
                } else {
                    out.extend(
                        mutations
                            .into_iter()
                            .map(|m| m.key)
                            .zip(values)
                            .zip(not_founds.into_iter())
                            .filter_map(|((key, value), not_found)| {
                                (!not_found).then(|| KvPair::new(key, value))
                            }),
                    );
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
        self.end_key = shard.1;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.region_with_leader)
    }
}

impl HasNextBatch for kvrpcpb::ScanLockResponse {
    fn has_next_batch(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.locks.last().map(|lock| {
            // `ScanLockResponse` does not carry the request end key; `CleanupLocks` uses
            // `PreserveShard` + `ResponseWithShard` to stop batching once the shard end key
            // is reached.
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
        self.primary_lock = shard.pop().unwrap_or_default();
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
        self.primary_key = shard.pop().unwrap_or_default();
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

    // Match client-go `TxnStatus.StatusCacheable`: cache only when final status is determined.
    // Committed is always determined. RolledBack is determined only for rollback actions
    // (`NoAction`, `LockNotExistRollback`, `TtlExpireRollback`); other actions like
    // `LockNotExistDoNothing` are not cacheable.
    pub fn is_cacheable(&self) -> bool {
        match &self.kind {
            TransactionStatusKind::Committed(..) => true,
            TransactionStatusKind::RolledBack => matches!(
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
            locked_keys: 0,
            missing_lock: false,
            resolve_keys: Vec::new(),
        };
        for resp in input {
            let resp = resp?;
            out.locked_keys = out.locked_keys.saturating_add(resp.locks.len());
            for lock in resp.locks.into_iter() {
                if !lock.use_async_commit {
                    out.fallback_2pc = true;
                    return Ok(out);
                }
                out.min_commit_ts = cmp::max(out.min_commit_ts, lock.min_commit_ts);
                out.resolve_keys.push(lock.key);
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
    pub locked_keys: usize,
    pub missing_lock: bool,
    pub resolve_keys: Vec<Vec<u8>>,
}

pair_locks!(kvrpcpb::BatchGetResponse);
pair_locks!(kvrpcpb::BufferBatchGetResponse);
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
            .into_iter()
            .flat_map(flatten_lock_info)
            .collect()
    }
}

impl HasLocks for kvrpcpb::PessimisticRollbackResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .flat_map(flatten_lock_info)
            .collect()
    }
}

impl HasLocks for kvrpcpb::PessimisticLockResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .flat_map(flatten_lock_info)
            .collect()
    }
}

impl HasLocks for kvrpcpb::PrewriteResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .flat_map(flatten_lock_info)
            .collect()
    }
}

impl HasLocks for kvrpcpb::FlushResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .flat_map(flatten_lock_info)
            .collect()
    }
}

pub fn new_delete_range_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    notify_only: bool,
) -> kvrpcpb::DeleteRangeRequest {
    let mut req = kvrpcpb::DeleteRangeRequest::default();
    req.start_key = start_key;
    req.end_key = end_key;
    req.notify_only = notify_only;
    req
}

impl KvRequest for kvrpcpb::DeleteRangeRequest {
    type Response = kvrpcpb::DeleteRangeResponse;
}

range_request!(kvrpcpb::DeleteRangeRequest);
shardable_range!(kvrpcpb::DeleteRangeRequest);
impl HasLocks for kvrpcpb::DeleteRangeResponse {}

impl Merge<kvrpcpb::DeleteRangeResponse> for Collect {
    type Out = usize;

    fn merge(&self, input: Vec<Result<kvrpcpb::DeleteRangeResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .try_fold(0usize, |acc, res| res.map(|_| acc + 1))
    }
}

pub fn new_prepare_flashback_to_version_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    start_ts: u64,
    version: u64,
) -> kvrpcpb::PrepareFlashbackToVersionRequest {
    let mut req = kvrpcpb::PrepareFlashbackToVersionRequest::default();
    req.start_key = start_key;
    req.end_key = end_key;
    req.start_ts = start_ts;
    req.version = version;
    req
}

impl KvRequest for kvrpcpb::PrepareFlashbackToVersionRequest {
    type Response = kvrpcpb::PrepareFlashbackToVersionResponse;
}

range_request!(kvrpcpb::PrepareFlashbackToVersionRequest);
shardable_range!(kvrpcpb::PrepareFlashbackToVersionRequest);
impl HasLocks for kvrpcpb::PrepareFlashbackToVersionResponse {}

impl Merge<kvrpcpb::PrepareFlashbackToVersionResponse> for Collect {
    type Out = usize;

    fn merge(
        &self,
        input: Vec<Result<kvrpcpb::PrepareFlashbackToVersionResponse>>,
    ) -> Result<Self::Out> {
        input
            .into_iter()
            .try_fold(0usize, |acc, res| res.map(|_| acc + 1))
    }
}

pub fn new_flashback_to_version_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    version: u64,
    start_ts: u64,
    commit_ts: u64,
) -> kvrpcpb::FlashbackToVersionRequest {
    let mut req = kvrpcpb::FlashbackToVersionRequest::default();
    req.version = version;
    req.start_key = start_key;
    req.end_key = end_key;
    req.start_ts = start_ts;
    req.commit_ts = commit_ts;
    req
}

impl KvRequest for kvrpcpb::FlashbackToVersionRequest {
    type Response = kvrpcpb::FlashbackToVersionResponse;
}

range_request!(kvrpcpb::FlashbackToVersionRequest);
shardable_range!(kvrpcpb::FlashbackToVersionRequest);
impl HasLocks for kvrpcpb::FlashbackToVersionResponse {}

impl Merge<kvrpcpb::FlashbackToVersionResponse> for Collect {
    type Out = usize;

    fn merge(&self, input: Vec<Result<kvrpcpb::FlashbackToVersionResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .try_fold(0usize, |acc, res| res.map(|_| acc + 1))
    }
}

const SPLIT_REGION_BATCH_LIMIT: usize = 2048;

pub fn new_split_region_request(
    split_keys: Vec<Vec<u8>>,
    is_raw_kv: bool,
) -> kvrpcpb::SplitRegionRequest {
    let mut req = kvrpcpb::SplitRegionRequest::default();
    req.split_keys = split_keys;
    req.is_raw_kv = is_raw_kv;
    req
}

impl KvRequest for kvrpcpb::SplitRegionRequest {
    type Response = kvrpcpb::SplitRegionResponse;
}

impl Shardable for kvrpcpb::SplitRegionRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut keys = self.split_keys.clone();
        keys.sort();

        region_stream_for_keys(keys.into_iter(), pd_client.clone())
            .map_ok(|(keys, region): (Vec<Vec<u8>>, RegionWithLeader)| {
                let region_start = region.region.start_key.clone();
                let keys = keys
                    .into_iter()
                    .filter(|key| key.as_slice() != region_start.as_slice())
                    .collect::<Vec<_>>();
                (keys, region)
            })
            .try_filter_map(|(keys, region)| async move {
                if keys.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some((keys, region)))
                }
            })
            .map_ok(|(keys, region)| {
                let batches = keys
                    .chunks(SPLIT_REGION_BATCH_LIMIT)
                    .map(|chunk| chunk.to_vec())
                    .collect::<Vec<_>>();
                stream::iter(
                    batches
                        .into_iter()
                        .map(move |batch| Ok((batch, region.clone()))),
                )
            })
            .try_flatten()
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.split_keys = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.region_with_leader)
    }
}

impl HasLocks for kvrpcpb::SplitRegionResponse {}

impl Merge<kvrpcpb::SplitRegionResponse> for Collect {
    type Out = Vec<u64>;

    fn merge(&self, input: Vec<Result<kvrpcpb::SplitRegionResponse>>) -> Result<Self::Out> {
        let mut region_ids = Vec::new();
        for resp in input {
            let mut resp = resp?;
            if resp.regions.is_empty() {
                continue;
            }

            let mut regions = std::mem::take(&mut resp.regions);
            regions.pop();
            region_ids.extend(regions.into_iter().map(|region| region.id));
        }
        Ok(region_ids)
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
        let mut errors = Vec::new();
        for resp in input {
            match resp {
                Ok(resp) => {
                    if !resp.error.is_empty() {
                        crate::stats::inc_gc_unsafe_destroy_range_failures("send");
                        errors.push(resp.error);
                    }
                }
                Err(err) => {
                    crate::stats::inc_gc_unsafe_destroy_range_failures("send");
                    errors.push(err.to_string());
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(crate::Error::StringError(format!(
                "[unsafe destroy range] destroy range finished with errors: {errors:?}"
            )))
        }
    }
}

pub fn new_compact_request(
    start_key: Vec<u8>,
    physical_table_id: i64,
    logical_table_id: i64,
    keyspace_id: u32,
) -> kvrpcpb::CompactRequest {
    let mut req = kvrpcpb::CompactRequest::default();
    req.start_key = start_key;
    req.physical_table_id = physical_table_id;
    req.logical_table_id = logical_table_id;
    req.keyspace_id = keyspace_id;
    req
}

impl KvRequest for kvrpcpb::CompactRequest {
    type Response = kvrpcpb::CompactResponse;
}

impl StoreRequest for kvrpcpb::CompactRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::CompactResponse {}

impl Merge<kvrpcpb::CompactResponse> for Collect {
    type Out = Vec<kvrpcpb::CompactResponse>;

    fn merge(&self, input: Vec<Result<kvrpcpb::CompactResponse>>) -> Result<Self::Out> {
        input.into_iter().collect()
    }
}

pub fn new_get_ti_flash_system_table_request(sql: String) -> kvrpcpb::TiFlashSystemTableRequest {
    let mut req = kvrpcpb::TiFlashSystemTableRequest::default();
    req.sql = sql;
    req
}

impl KvRequest for kvrpcpb::TiFlashSystemTableRequest {
    type Response = kvrpcpb::TiFlashSystemTableResponse;
}

impl StoreRequest for kvrpcpb::TiFlashSystemTableRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::TiFlashSystemTableResponse {}

impl Merge<kvrpcpb::TiFlashSystemTableResponse> for Collect {
    type Out = Vec<kvrpcpb::TiFlashSystemTableResponse>;

    fn merge(&self, input: Vec<Result<kvrpcpb::TiFlashSystemTableResponse>>) -> Result<Self::Out> {
        input.into_iter().collect()
    }
}

pub fn new_register_lock_observer_request(max_ts: u64) -> kvrpcpb::RegisterLockObserverRequest {
    let mut req = kvrpcpb::RegisterLockObserverRequest::default();
    req.max_ts = max_ts;
    req
}

impl KvRequest for kvrpcpb::RegisterLockObserverRequest {
    type Response = kvrpcpb::RegisterLockObserverResponse;
}

impl StoreRequest for kvrpcpb::RegisterLockObserverRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::RegisterLockObserverResponse {}

impl Merge<kvrpcpb::RegisterLockObserverResponse> for Collect {
    type Out = ();

    fn merge(
        &self,
        input: Vec<Result<kvrpcpb::RegisterLockObserverResponse>>,
    ) -> Result<Self::Out> {
        let _: Vec<kvrpcpb::RegisterLockObserverResponse> =
            input.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

pub fn new_check_lock_observer_request(max_ts: u64) -> kvrpcpb::CheckLockObserverRequest {
    let mut req = kvrpcpb::CheckLockObserverRequest::default();
    req.max_ts = max_ts;
    req
}

impl KvRequest for kvrpcpb::CheckLockObserverRequest {
    type Response = kvrpcpb::CheckLockObserverResponse;
}

impl StoreRequest for kvrpcpb::CheckLockObserverRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::CheckLockObserverResponse {
    fn take_locks(&mut self) -> Vec<LockInfo> {
        std::mem::take(&mut self.locks)
            .into_iter()
            .flat_map(flatten_lock_info)
            .collect()
    }
}

impl Merge<kvrpcpb::CheckLockObserverResponse> for Collect {
    type Out = (bool, Vec<kvrpcpb::LockInfo>);

    fn merge(&self, input: Vec<Result<kvrpcpb::CheckLockObserverResponse>>) -> Result<Self::Out> {
        let mut is_clean = true;
        let mut locks = Vec::new();
        for resp in input {
            let mut resp = resp?;
            is_clean &= resp.is_clean;
            locks.extend(resp.take_locks().into_iter().map(Into::into));
        }
        Ok((is_clean, locks))
    }
}

pub fn new_remove_lock_observer_request(max_ts: u64) -> kvrpcpb::RemoveLockObserverRequest {
    let mut req = kvrpcpb::RemoveLockObserverRequest::default();
    req.max_ts = max_ts;
    req
}

impl KvRequest for kvrpcpb::RemoveLockObserverRequest {
    type Response = kvrpcpb::RemoveLockObserverResponse;
}

impl StoreRequest for kvrpcpb::RemoveLockObserverRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::RemoveLockObserverResponse {}

impl Merge<kvrpcpb::RemoveLockObserverResponse> for Collect {
    type Out = ();

    fn merge(&self, input: Vec<Result<kvrpcpb::RemoveLockObserverResponse>>) -> Result<Self::Out> {
        let _: Vec<kvrpcpb::RemoveLockObserverResponse> =
            input.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

pub fn new_physical_scan_lock_request(
    max_ts: u64,
    start_key: Vec<u8>,
    limit: u32,
) -> kvrpcpb::PhysicalScanLockRequest {
    let mut req = kvrpcpb::PhysicalScanLockRequest::default();
    req.max_ts = max_ts;
    req.start_key = start_key;
    req.limit = limit;
    req
}

impl KvRequest for kvrpcpb::PhysicalScanLockRequest {
    type Response = kvrpcpb::PhysicalScanLockResponse;
}

impl StoreRequest for kvrpcpb::PhysicalScanLockRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::PhysicalScanLockResponse {
    fn take_locks(&mut self) -> Vec<LockInfo> {
        std::mem::take(&mut self.locks)
            .into_iter()
            .flat_map(flatten_lock_info)
            .collect()
    }
}

impl Merge<kvrpcpb::PhysicalScanLockResponse> for Collect {
    type Out = Vec<kvrpcpb::LockInfo>;

    fn merge(&self, input: Vec<Result<kvrpcpb::PhysicalScanLockResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|mut resp| resp.take_locks().into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_get_lock_wait_info_request() -> kvrpcpb::GetLockWaitInfoRequest {
    kvrpcpb::GetLockWaitInfoRequest::default()
}

impl KvRequest for kvrpcpb::GetLockWaitInfoRequest {
    type Response = kvrpcpb::GetLockWaitInfoResponse;
}

impl StoreRequest for kvrpcpb::GetLockWaitInfoRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::GetLockWaitInfoResponse {}

impl Merge<kvrpcpb::GetLockWaitInfoResponse> for Collect {
    type Out = Vec<crate::proto::deadlock::WaitForEntry>;

    fn merge(&self, input: Vec<Result<kvrpcpb::GetLockWaitInfoResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|resp| resp.entries.into_iter())
            .collect()
    }
}

pub fn new_get_lock_wait_history_request() -> kvrpcpb::GetLockWaitHistoryRequest {
    kvrpcpb::GetLockWaitHistoryRequest::default()
}

impl KvRequest for kvrpcpb::GetLockWaitHistoryRequest {
    type Response = kvrpcpb::GetLockWaitHistoryResponse;
}

impl StoreRequest for kvrpcpb::GetLockWaitHistoryRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::GetLockWaitHistoryResponse {}

impl Merge<kvrpcpb::GetLockWaitHistoryResponse> for Collect {
    type Out = Vec<crate::proto::deadlock::WaitForEntry>;

    fn merge(&self, input: Vec<Result<kvrpcpb::GetLockWaitHistoryResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|resp| resp.entries.into_iter())
            .collect()
    }
}

impl KvRequest for kvrpcpb::StoreSafeTsRequest {
    type Response = kvrpcpb::StoreSafeTsResponse;
}

impl StoreRequest for kvrpcpb::StoreSafeTsRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::StoreSafeTsResponse {}

impl Merge<kvrpcpb::StoreSafeTsResponse> for Collect {
    type Out = u64;

    fn merge(&self, input: Vec<Result<kvrpcpb::StoreSafeTsResponse>>) -> Result<Self::Out> {
        let mut min_safe_ts = u64::MAX;
        for resp in input {
            let resp = match resp {
                Ok(resp) => resp,
                Err(_) => return Ok(0),
            };
            if resp.safe_ts != 0 && resp.safe_ts < min_safe_ts {
                min_safe_ts = resp.safe_ts;
            }
        }
        Ok(if min_safe_ts == u64::MAX {
            0
        } else {
            min_safe_ts
        })
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
    use crate::request::Process;
    use crate::request::ResponseWithShard;
    use crate::request::Shardable;
    use crate::KvPair;

    #[test]
    fn test_merge_store_safe_ts_returns_min_non_zero_safe_ts() {
        let merged = crate::request::Collect
            .merge(vec![
                Ok(kvrpcpb::StoreSafeTsResponse { safe_ts: 100 }),
                Ok(kvrpcpb::StoreSafeTsResponse { safe_ts: 80 }),
            ])
            .unwrap();
        assert_eq!(merged, 80);
    }

    #[test]
    fn test_merge_store_safe_ts_ignores_zero_safe_ts() {
        let merged = crate::request::Collect
            .merge(vec![
                Ok(kvrpcpb::StoreSafeTsResponse { safe_ts: 0 }),
                Ok(kvrpcpb::StoreSafeTsResponse { safe_ts: 12 }),
            ])
            .unwrap();
        assert_eq!(merged, 12);
    }

    #[test]
    fn test_merge_store_safe_ts_returns_zero_on_store_error() {
        let merged = crate::request::Collect
            .merge(vec![
                Ok(kvrpcpb::StoreSafeTsResponse { safe_ts: 12 }),
                Err(Error::Unimplemented),
            ])
            .unwrap();
        assert_eq!(merged, 0);
    }

    #[test]
    fn test_merge_store_safe_ts_returns_zero_when_no_store_results() {
        let merged = crate::request::Collect
            .merge(Vec::<crate::Result<kvrpcpb::StoreSafeTsResponse>>::new())
            .unwrap();
        assert_eq!(merged, 0);
    }

    #[test]
    fn test_merge_lock_wait_info_collects_entries_across_stores() {
        let merged = crate::request::Collect
            .merge(vec![
                Ok(kvrpcpb::GetLockWaitInfoResponse {
                    region_error: None,
                    error: "".to_owned(),
                    entries: vec![crate::proto::deadlock::WaitForEntry {
                        txn: 1,
                        wait_for_txn: 2,
                        key_hash: 3,
                        key: b"k1".to_vec(),
                        resource_group_tag: b"tag1".to_vec(),
                        wait_time: 4,
                    }],
                }),
                Ok(kvrpcpb::GetLockWaitInfoResponse {
                    region_error: None,
                    error: "".to_owned(),
                    entries: vec![crate::proto::deadlock::WaitForEntry {
                        txn: 10,
                        wait_for_txn: 20,
                        key_hash: 30,
                        key: b"k2".to_vec(),
                        resource_group_tag: b"tag2".to_vec(),
                        wait_time: 40,
                    }],
                }),
            ])
            .unwrap();

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].txn, 1);
        assert_eq!(merged[1].txn, 10);
    }

    #[test]
    fn test_merge_lock_wait_history_collects_entries_across_stores() {
        let merged = crate::request::Collect
            .merge(vec![
                Ok(kvrpcpb::GetLockWaitHistoryResponse {
                    region_error: None,
                    error: "".to_owned(),
                    entries: vec![crate::proto::deadlock::WaitForEntry {
                        txn: 100,
                        wait_for_txn: 200,
                        key_hash: 300,
                        key: b"k1".to_vec(),
                        resource_group_tag: b"tag1".to_vec(),
                        wait_time: 400,
                    }],
                }),
                Ok(kvrpcpb::GetLockWaitHistoryResponse {
                    region_error: None,
                    error: "".to_owned(),
                    entries: vec![crate::proto::deadlock::WaitForEntry {
                        txn: 1000,
                        wait_for_txn: 2000,
                        key_hash: 3000,
                        key: b"k2".to_vec(),
                        resource_group_tag: b"tag2".to_vec(),
                        wait_time: 4000,
                    }],
                }),
            ])
            .unwrap();

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].txn, 100);
        assert_eq!(merged[1].txn, 1000);
    }

    #[test]
    fn test_merge_check_lock_observer_collects_locks_and_ands_is_clean() {
        let (is_clean, locks) = crate::request::Collect
            .merge(vec![
                Ok(kvrpcpb::CheckLockObserverResponse {
                    error: "".to_owned(),
                    is_clean: true,
                    locks: vec![],
                }),
                Ok(kvrpcpb::CheckLockObserverResponse {
                    error: "".to_owned(),
                    is_clean: false,
                    locks: vec![kvrpcpb::LockInfo {
                        key: b"k1".to_vec(),
                        primary_lock: b"p".to_vec(),
                        ..Default::default()
                    }],
                }),
            ])
            .unwrap();

        assert!(!is_clean);
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].key, b"k1".to_vec());
    }

    #[test]
    fn test_merge_physical_scan_lock_collects_locks_across_stores() {
        let locks = crate::request::Collect
            .merge(vec![
                Ok(kvrpcpb::PhysicalScanLockResponse {
                    error: "".to_owned(),
                    locks: vec![kvrpcpb::LockInfo {
                        key: b"k1".to_vec(),
                        primary_lock: b"p1".to_vec(),
                        ..Default::default()
                    }],
                }),
                Ok(kvrpcpb::PhysicalScanLockResponse {
                    error: "".to_owned(),
                    locks: vec![kvrpcpb::LockInfo {
                        key: b"k2".to_vec(),
                        primary_lock: b"p2".to_vec(),
                        ..Default::default()
                    }],
                }),
            ])
            .unwrap();

        assert_eq!(locks.len(), 2);
        assert_eq!(locks[0].key, b"k1".to_vec());
        assert_eq!(locks[1].key, b"k2".to_vec());
    }

    #[test]
    fn test_scan_lock_request_apply_shard_sets_end_key() {
        let mut req = kvrpcpb::ScanLockRequest {
            start_key: vec![0],
            end_key: vec![100],
            max_version: 1,
            limit: 10,
            ..Default::default()
        };
        req.apply_shard((vec![10], vec![20]));
        assert_eq!(req.start_key, vec![10]);
        assert_eq!(req.end_key, vec![20]);
    }

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
    fn test_process_check_txn_status_lock_not_exist_do_nothing_not_cacheable() {
        let processor = super::DefaultProcessor;
        let status = processor
            .process(Ok(kvrpcpb::CheckTxnStatusResponse {
                action: kvrpcpb::Action::LockNotExistDoNothing.into(),
                ..Default::default()
            }))
            .expect("lock-not-exist-do-nothing should decode successfully");

        assert!(matches!(
            status.kind,
            super::TransactionStatusKind::RolledBack
        ));
        assert_eq!(status.action, kvrpcpb::Action::LockNotExistDoNothing);
        assert!(
            !status.is_cacheable(),
            "lock-not-exist-do-nothing status must not be cached"
        );
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

        let merger = super::CollectPessimisticLock::new(true);
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
            let resp_no_values = ResponseWithShard(
                kvrpcpb::PessimisticLockResponse::default(),
                vec![kvrpcpb::Mutation {
                    op: kvrpcpb::Op::PessimisticLock.into(),
                    key: key1.to_vec(),
                    ..Default::default()
                }],
            );
            let merger = super::CollectPessimisticLock::new(false);
            let result = merger.merge(vec![Ok(resp_no_values)]);
            assert!(result.unwrap().is_empty());
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
        {
            let resp_values_len_mismatch = ResponseWithShard(
                kvrpcpb::PessimisticLockResponse {
                    values: vec![value1.to_vec(), value4.to_vec()],
                    ..Default::default()
                },
                vec![kvrpcpb::Mutation {
                    op: kvrpcpb::Op::PessimisticLock.into(),
                    key: key1.to_vec(),
                    ..Default::default()
                }],
            );

            let result = merger.merge(vec![Ok(resp_values_len_mismatch)]);
            match result.unwrap_err() {
                Error::StringError(message) => {
                    assert!(message.contains("values length"));
                }
                other => panic!("unexpected error: {other:?}"),
            }
        }
        {
            let resp_not_founds_len_mismatch = ResponseWithShard(
                kvrpcpb::PessimisticLockResponse {
                    values: vec![value1.to_vec()],
                    not_founds: vec![true, false],
                    ..Default::default()
                },
                vec![kvrpcpb::Mutation {
                    op: kvrpcpb::Op::PessimisticLock.into(),
                    key: key1.to_vec(),
                    ..Default::default()
                }],
            );

            let result = merger.merge(vec![Ok(resp_not_founds_len_mismatch)]);
            match result.unwrap_err() {
                Error::StringError(message) => {
                    assert!(message.contains("not_founds length"));
                }
                other => panic!("unexpected error: {other:?}"),
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

    #[tokio::test]
    async fn test_prewrite_request_shards_retry_uses_u64_max_txn_size() {
        let large_value = vec![0_u8; 8 * 1024];

        let primary = vec![1_u8];
        let key2 = vec![2_u8];
        let key3 = vec![3_u8];

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
                key: key3.clone(),
                value: large_value,
                ..Default::default()
            },
        ];

        let req = super::new_pessimistic_prewrite_request(mutations, primary.clone(), 1, 100, 5);
        let pd_client = Arc::new(MockPdClient::default());

        let shards = req
            .shards(&pd_client)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();

        let (shard, _) = shards
            .into_iter()
            .find(|(shard, _)| {
                shard
                    .mutations
                    .first()
                    .map(|m| m.key == key2)
                    .unwrap_or(false)
            })
            .expect("missing expected shard for key2");

        let mut shard_req = req.clone();
        shard_req.apply_shard(shard);
        assert_eq!(shard_req.mutations.len(), 1);
        assert_eq!(shard_req.txn_size, 3);

        shard_req
            .context
            .get_or_insert_with(kvrpcpb::Context::default)
            .is_retry_request = true;

        let retry_shards = shard_req
            .shards(&pd_client)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
        assert!(!retry_shards.is_empty());

        for (retry_shard, _) in retry_shards {
            assert_eq!(retry_shard.txn_size, u64::MAX);
            let mut retry_req = shard_req.clone();
            retry_req.apply_shard(retry_shard);
            assert_eq!(retry_req.txn_size, u64::MAX);
        }
    }

    #[tokio::test]
    async fn test_prewrite_request_shards_rejects_pessimistic_actions_len_mismatch() {
        let primary = vec![1_u8];
        let key2 = vec![2_u8];

        let mutations = vec![
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: primary.clone(),
                value: vec![0],
                ..Default::default()
            },
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: key2,
                value: vec![0],
                ..Default::default()
            },
        ];

        let mut req = super::new_prewrite_request(mutations, primary, 1, 100);
        req.pessimistic_actions =
            vec![kvrpcpb::prewrite_request::PessimisticAction::DoPessimisticCheck.into()];

        let pd_client = Arc::new(MockPdClient::default());
        let err = req
            .shards(&pd_client)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<crate::Result<Vec<_>>>()
            .expect_err("invalid pessimistic_actions length should return an error");

        match err {
            Error::StringError(message) => {
                assert!(message.contains("pessimistic_actions length"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_take_locks_flattens_shared_lock_infos_from_error() {
        let embedded_1 = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![9],
            lock_version: 10,
            lock_ttl: 50,
            lock_type: kvrpcpb::Op::Put as i32,
            ..Default::default()
        };
        let embedded_2 = kvrpcpb::LockInfo {
            key: vec![2],
            primary_lock: vec![9],
            lock_version: 11,
            lock_ttl: 60,
            lock_type: kvrpcpb::Op::PessimisticLock as i32,
            ..Default::default()
        };

        let wrapper = kvrpcpb::LockInfo {
            lock_type: kvrpcpb::Op::SharedLock as i32,
            shared_lock_infos: vec![embedded_1.clone(), embedded_2.clone()],
            ..Default::default()
        };

        let mut resp = kvrpcpb::GetResponse {
            error: Some(kvrpcpb::KeyError {
                locked: Some(wrapper),
                ..Default::default()
            }),
            ..Default::default()
        };

        let locks = crate::transaction::HasLocks::take_locks(&mut resp);
        assert_eq!(locks, vec![embedded_1, embedded_2]);
    }

    #[test]
    fn test_take_locks_flattens_shared_lock_infos_from_scan_lock() {
        let embedded = kvrpcpb::LockInfo {
            key: vec![42],
            primary_lock: vec![7],
            lock_version: 99,
            lock_ttl: 123,
            lock_type: kvrpcpb::Op::Put as i32,
            ..Default::default()
        };

        let wrapper = kvrpcpb::LockInfo {
            lock_type: kvrpcpb::Op::SharedLock as i32,
            shared_lock_infos: vec![embedded.clone()],
            ..Default::default()
        };

        let mut resp = kvrpcpb::ScanLockResponse {
            locks: vec![wrapper],
            ..Default::default()
        };

        let locks = crate::transaction::HasLocks::take_locks(&mut resp);
        assert_eq!(locks, vec![embedded]);
    }

    #[tokio::test]
    async fn test_split_region_request_shards_by_region_filters_region_start_keys() {
        let req = super::new_split_region_request(
            vec![
                vec![10],
                vec![1],
                vec![9],
                Vec::new(),
                vec![11],
                vec![250, 250],
                vec![250, 250, 1],
            ],
            false,
        );

        let pd_client = Arc::new(MockPdClient::default());
        let shards = req
            .shards(&pd_client)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(shards.len(), 3);

        assert_eq!(shards[0].0, vec![vec![1], vec![9]]);
        assert_eq!(shards[0].1.id(), 1);

        assert_eq!(shards[1].0, vec![vec![11]]);
        assert_eq!(shards[1].1.id(), 2);

        assert_eq!(shards[2].0, vec![vec![250, 250, 1]]);
        assert_eq!(shards[2].1.id(), 3);
    }

    #[tokio::test]
    async fn test_split_region_request_shards_batches_by_limit() {
        let mut keys = (0..=(super::SPLIT_REGION_BATCH_LIMIT as u16))
            .map(|i| vec![250, 250, (i >> 8) as u8, (i & 0xff) as u8])
            .collect::<Vec<_>>();
        keys.reverse();

        let req = super::new_split_region_request(keys, false);
        let pd_client = Arc::new(MockPdClient::default());
        let shards = req
            .shards(&pd_client)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].0.len(), super::SPLIT_REGION_BATCH_LIMIT);
        assert_eq!(shards[1].0.len(), 1);

        assert_eq!(shards[0].1.id(), 3);
        assert_eq!(shards[1].1.id(), 3);

        assert_eq!(shards[0].0[0], vec![250, 250, 0, 0]);
    }

    #[test]
    fn test_merge_split_region_returns_all_but_last_region_id() {
        let merged = crate::request::Collect
            .merge(vec![Ok(kvrpcpb::SplitRegionResponse {
                regions: vec![
                    crate::proto::metapb::Region {
                        id: 1,
                        ..Default::default()
                    },
                    crate::proto::metapb::Region {
                        id: 2,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            })])
            .unwrap();

        assert_eq!(merged, vec![1]);
    }
}
