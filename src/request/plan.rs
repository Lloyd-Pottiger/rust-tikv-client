// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::prelude::*;
use log::debug;
use log::info;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::backoff::Backoff;
use crate::pd::PdClient;
use crate::proto::errorpb;
use crate::proto::errorpb::EpochNotMatch;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::proto::pdpb::Timestamp;
use crate::region::StoreId;
use crate::region::{RegionVerId, RegionWithLeader};
use crate::request::shard::HasNextBatch;
use crate::request::NextBatch;
use crate::request::Shardable;
use crate::request::{KvRequest, StoreRequest};
use crate::stats::tikv_stats;
use crate::store::HasRegionError;
use crate::store::HasRegionErrors;
use crate::store::KvClient;
use crate::store::RegionStore;
use crate::store::{HasKeyErrors, Store};
use crate::transaction::resolve_locks_for_read;
use crate::transaction::resolve_locks_with_options;
use crate::transaction::HasLocks;
use crate::transaction::ReadLockTracker;
use crate::transaction::ResolveLocksContext;
use crate::transaction::ResolveLocksOptions;
use crate::util::iter::FlatMapOkIterExt;
use crate::Error;
use crate::Key;
use crate::ReplicaReadType;
use crate::Result;

use super::keyspace::Keyspace;

/// A plan for how to execute a request. A user builds up a plan with various
/// options, then exectutes it.
#[async_trait]
pub trait Plan: Sized + Clone + Sync + Send + 'static {
    /// The ultimate result of executing the plan (should be a high-level type, not a GRPC response).
    type Result: Send;

    /// Execute the plan.
    async fn execute(&self) -> Result<Self::Result>;
}

/// The simplest plan which just dispatches a request to a specific kv server.
#[derive(Clone)]
pub struct Dispatch<Req: KvRequest> {
    pub request: Req,
    pub kv_client: Option<Arc<dyn KvClient + Send + Sync>>,
}

#[async_trait]
impl<Req: KvRequest> Plan for Dispatch<Req> {
    type Result = Req::Response;

    async fn execute(&self) -> Result<Self::Result> {
        let stats = tikv_stats(self.request.label());
        let result = match self.kv_client.as_ref() {
            Some(kv_client) => kv_client.dispatch(&self.request).await.and_then(|r| {
                r.downcast::<Req::Response>()
                    .map(|r| *r)
                    .map_err(|_| Error::InternalError {
                        message: format!(
                            "downcast failed: request and response type mismatch, expected {}",
                            std::any::type_name::<Req::Response>()
                        ),
                    })
            }),
            None => Err(Error::InternalError {
                message: "kv_client has not been initialised in Dispatch".to_owned(),
            }),
        };
        stats.done(result)
    }
}

impl<Req: KvRequest + StoreRequest> StoreRequest for Dispatch<Req> {
    fn apply_store(&mut self, store: &Store) {
        self.kv_client = Some(store.client.clone());
        self.request.apply_store(store);
    }
}

const MULTI_REGION_CONCURRENCY: usize = 16;
const MULTI_STORES_CONCURRENCY: usize = 16;

pub(crate) fn is_grpc_error(e: &Error) -> bool {
    matches!(e, Error::GrpcAPI(_) | Error::Grpc(_))
}

#[doc(hidden)]
pub trait HasKvContext {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context>;
}

impl<Req: KvRequest> HasKvContext for Dispatch<Req> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.request.context_mut()
    }
}

impl<P: Plan + Shardable + HasKvContext> HasKvContext for PreserveShard<P> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.kv_context_mut()
    }
}

impl<P: Plan + HasKvContext, PdC: PdClient> HasKvContext for ResolveLock<P, PdC> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.kv_context_mut()
    }
}

impl<P: Plan + HasKvContext, PdC: PdClient> HasKvContext for ResolveLockForRead<P, PdC> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.kv_context_mut()
    }
}

impl<P: Plan + HasKvContext, PdC: PdClient> HasKvContext for CleanupLocks<P, PdC> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.kv_context_mut()
    }
}

fn select_replica_read_peer(
    region: &RegionWithLeader,
    replica_read: ReplicaReadType,
    attempt: u32,
) -> Option<metapb::Peer> {
    let leader_store_id = region.leader.as_ref().map(|p| p.store_id);
    let peers = &region.region.peers;

    fn select_peer_with_attempt<F>(
        peers: &[metapb::Peer],
        attempt: u32,
        predicate: F,
    ) -> Option<metapb::Peer>
    where
        F: Copy + Fn(&metapb::Peer) -> bool,
    {
        let candidates = peers.iter().filter(|peer| predicate(peer)).count();
        if candidates == 0 {
            return None;
        }
        let target = (attempt as usize) % candidates;
        peers
            .iter()
            .filter(|peer| predicate(peer))
            .nth(target)
            .cloned()
    }

    let peer = match replica_read {
        ReplicaReadType::Follower => select_peer_with_attempt(peers, attempt, |peer| {
            Some(peer.store_id) != leader_store_id && peer.role != metapb::PeerRole::Learner as i32
        }),
        ReplicaReadType::Learner => select_peer_with_attempt(peers, attempt, |peer| {
            peer.role == metapb::PeerRole::Learner as i32
        }),
        ReplicaReadType::Mixed => select_peer_with_attempt(peers, attempt, |peer| {
            Some(peer.store_id) != leader_store_id
        }),
        ReplicaReadType::Leader | ReplicaReadType::PreferLeader => None,
    };

    peer.or_else(|| region.leader.clone())
        .or_else(|| region.region.peers.first().cloned())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReplicaReadState {
    read_type: ReplicaReadType,
    attempt_base: u32,
}

impl ReplicaReadState {
    fn new(read_type: ReplicaReadType) -> Self {
        Self {
            read_type,
            attempt_base: 0,
        }
    }

    fn attempt(self, current_attempts: u32) -> u32 {
        current_attempts.saturating_sub(self.attempt_base)
    }

    fn switch_to(self, read_type: ReplicaReadType, current_attempts: u32) -> Self {
        Self {
            read_type,
            attempt_base: current_attempts,
        }
    }

    fn keep_current_attempt_on_retry(self) -> Self {
        Self {
            read_type: self.read_type,
            attempt_base: self.attempt_base.saturating_add(1),
        }
    }
}

pub struct RetryableMultiRegion<P: Plan, PdC: PdClient> {
    pub(super) inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,

    /// Preserve all regions' results for other downstream plans to handle.
    /// If true, return Ok and preserve all regions' results, even if some of them are Err.
    /// Otherwise, return the first Err if there is any.
    pub preserve_region_results: bool,

    pub(super) replica_read: Option<ReplicaReadType>,
}

impl<P: Plan + Shardable + HasKvContext, PdC: PdClient> RetryableMultiRegion<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    // A plan may involve multiple shards
    #[async_recursion]
    async fn single_plan_handler(
        pd_client: Arc<PdC>,
        mut current_plan: P,
        backoff: Backoff,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        replica_read: Option<ReplicaReadState>,
    ) -> Result<<Self as Plan>::Result> {
        if backoff.current_attempts() > 0 {
            if let Some(ctx) = current_plan.kv_context_mut() {
                ctx.is_retry_request = true;
            }
        }
        let shards = current_plan.shards(&pd_client).collect::<Vec<_>>().await;
        debug!("single_plan_handler, shards: {}", shards.len());
        let mut handles = Vec::with_capacity(shards.len());
        for shard in shards {
            let (shard, region) = shard?;
            let clone = current_plan.clone_then_apply_shard(shard);
            let handle = tokio::spawn(Self::single_shard_handler(
                pd_client.clone(),
                clone,
                region,
                backoff.clone(),
                permits.clone(),
                preserve_region_results,
                replica_read,
            ));
            handles.push(handle);
        }

        let results = try_join_all(handles).await?;
        if preserve_region_results {
            Ok(results
                .into_iter()
                .flat_map_ok(|x| x)
                .map(|x| match x {
                    Ok(r) => r,
                    Err(e) => Err(e),
                })
                .collect())
        } else {
            Ok(results
                .into_iter()
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect())
        }
    }

    #[async_recursion]
    async fn single_shard_handler(
        pd_client: Arc<PdC>,
        mut plan: P,
        mut region: RegionWithLeader,
        mut backoff: Backoff,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        replica_read: Option<ReplicaReadState>,
    ) -> Result<<Self as Plan>::Result> {
        debug!("single_shard_handler");
        let region_leader = region.leader.clone();
        let leader_store_id = region_leader.as_ref().map(|peer| peer.store_id);
        let is_stale_read = plan
            .kv_context_mut()
            .map(|ctx| ctx.stale_read)
            .unwrap_or(false);
        let current_attempts = backoff.current_attempts();
        if let Some(replica_read) = replica_read {
            let attempt = replica_read.attempt(current_attempts);
            let read_type = replica_read.read_type;
            if is_stale_read && attempt == 1 {
                // Align with client-go replica selector behavior:
                // stale-read retries should fall back to normal snapshot reads on the leader.
                if let Some(ctx) = plan.kv_context_mut() {
                    ctx.stale_read = false;
                }
                if let Some(region_leader) = region_leader {
                    region.leader = Some(region_leader);
                }
            } else if read_type.is_follower_read() {
                if let Some(peer) = select_replica_read_peer(&region, read_type, attempt) {
                    region.leader = Some(peer);
                }
            }
        }
        let region_store = match pd_client
            .clone()
            .map_region_to_store(region)
            .await
            .and_then(|region_store| {
                plan.apply_store(&region_store)?;
                if let Some(replica_read) = replica_read {
                    if let Some(ctx) = plan.kv_context_mut() {
                        adjust_replica_read_flag(ctx, leader_store_id, replica_read.read_type);
                    }
                }
                Ok(region_store)
            }) {
            Ok(region_store) => region_store,
            Err(Error::LeaderNotFound { region }) => {
                debug!(
                    "single_shard_handler::sharding: leader not found: {:?}",
                    region
                );
                return Self::handle_other_error(
                    pd_client,
                    plan,
                    region.clone(),
                    None,
                    backoff,
                    permits,
                    preserve_region_results,
                    replica_read,
                    Error::LeaderNotFound { region },
                )
                .await;
            }
            Err(err) => {
                debug!("single_shard_handler::sharding, error: {:?}", err);
                return Err(err);
            }
        };

        // limit concurrent requests
        let permit = permits.acquire().await.map_err(|_| Error::InternalError {
            message: "request concurrency semaphore closed".to_owned(),
        })?;
        let res = plan.execute().await;
        drop(permit);

        let mut resp = match res {
            Ok(resp) => resp,
            Err(e) if is_grpc_error(&e) => {
                debug!("single_shard_handler:execute: grpc error: {:?}", e);
                return Self::handle_other_error(
                    pd_client,
                    plan,
                    region_store.region_with_leader.ver_id(),
                    region_store.region_with_leader.get_store_id().ok(),
                    backoff,
                    permits,
                    preserve_region_results,
                    replica_read,
                    e,
                )
                .await;
            }
            Err(e) => {
                debug!("single_shard_handler:execute: error: {:?}", e);
                return Err(e);
            }
        };

        if let Some(e) = resp.key_errors() {
            debug!("single_shard_handler:execute: key errors: {:?}", e);
            Ok(vec![Err(Error::MultipleKeyErrors(e))])
        } else if let Some(e) = resp.region_error() {
            debug!("single_shard_handler:execute: region error: {:?}", e);
            let is_server_busy = e.server_is_busy.is_some();
            let retry_same_replica = !plan
                .kv_context_mut()
                .map(|ctx| ctx.stale_read)
                .unwrap_or(false)
                && (is_server_busy
                    || e.max_timestamp_not_synced.is_some()
                    || e.read_index_not_ready.is_some()
                    || e.proposal_in_merging_mode.is_some());
            match backoff.next_delay_duration() {
                Some(duration) => {
                    let region_error_resolved =
                        handle_region_error(pd_client.clone(), e, region_store).await?;
                    // don't sleep if we have resolved the region error
                    if !region_error_resolved {
                        sleep(duration).await;
                    }
                    let replica_read = match (is_server_busy, replica_read) {
                        (true, Some(state)) if state.read_type == ReplicaReadType::PreferLeader => {
                            Some(
                                state.switch_to(ReplicaReadType::Mixed, backoff.current_attempts()),
                            )
                        }
                        _ => replica_read,
                    };
                    let replica_read = if retry_same_replica {
                        replica_read.map(ReplicaReadState::keep_current_attempt_on_retry)
                    } else {
                        replica_read
                    };
                    Self::single_plan_handler(
                        pd_client,
                        plan,
                        backoff,
                        permits,
                        preserve_region_results,
                        replica_read,
                    )
                    .await
                }
                None => Err(Error::RegionError(Box::new(e))),
            }
        } else {
            Ok(vec![Ok(resp)])
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_other_error(
        pd_client: Arc<PdC>,
        plan: P,
        region: RegionVerId,
        store: Option<StoreId>,
        mut backoff: Backoff,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        replica_read: Option<ReplicaReadState>,
        e: Error,
    ) -> Result<<Self as Plan>::Result> {
        debug!("handle_other_error: {:?}", e);
        let is_grpc_error = is_grpc_error(&e);
        let mut replica_read = replica_read;
        if is_grpc_error {
            if let Some(store_id) = store {
                pd_client.invalidate_store_cache(store_id).await;
            }
        } else {
            pd_client.invalidate_region_cache(region).await;
        }
        match backoff.next_delay_duration() {
            Some(duration) => {
                if is_grpc_error {
                    replica_read = match replica_read {
                        Some(state) if state.read_type == ReplicaReadType::PreferLeader => Some(
                            state.switch_to(ReplicaReadType::Mixed, backoff.current_attempts()),
                        ),
                        _ => replica_read,
                    };
                }
                sleep(duration).await;
                Self::single_plan_handler(
                    pd_client,
                    plan,
                    backoff,
                    permits,
                    preserve_region_results,
                    replica_read,
                )
                .await
            }
            None => Err(e),
        }
    }
}

fn adjust_replica_read_flag(
    ctx: &mut kvrpcpb::Context,
    leader_store_id: Option<StoreId>,
    replica_read: ReplicaReadType,
) {
    if ctx.stale_read || !replica_read.is_follower_read() {
        ctx.replica_read = false;
        return;
    }

    let (Some(leader_store_id), Some(peer_store_id)) =
        (leader_store_id, ctx.peer.as_ref().map(|peer| peer.store_id))
    else {
        return;
    };

    ctx.replica_read = peer_store_id != leader_store_id;
}

// Returns
// 1. Ok(true): error has been resolved, retry immediately
// 2. Ok(false): backoff, and then retry
// 3. Err(Error): can't be resolved, return the error to upper level
pub(crate) async fn handle_region_error<PdC: PdClient>(
    pd_client: Arc<PdC>,
    mut e: errorpb::Error,
    region_store: RegionStore,
) -> Result<bool> {
    debug!("handle_region_error: {:?}", e);
    let ver_id = region_store.region_with_leader.ver_id();
    let store_id = region_store.region_with_leader.get_store_id();
    if let Some(not_leader) = e.not_leader {
        if let Some(leader) = not_leader.leader {
            match pd_client
                .update_leader(region_store.region_with_leader.ver_id(), leader)
                .await
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    pd_client.invalidate_region_cache(ver_id).await;
                    Err(e)
                }
            }
        } else {
            // The peer doesn't know who is the current leader. Generally it's because
            // the Raft group is in an election, but it's possible that the peer is
            // isolated and removed from the Raft group. So it's necessary to reload
            // the region from PD.
            pd_client.invalidate_region_cache(ver_id).await;
            Ok(false)
        }
    } else if e.store_not_match.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        if let Ok(store_id) = store_id {
            pd_client.invalidate_store_cache(store_id).await;
        }
        Ok(false)
    } else if e.disk_full.is_some() {
        // Match client-go `RegionRequestSender.onRegionError`: disk-full is treated as retryable
        // with backoff.
        Ok(false)
    } else if let Some(epoch_not_match) = e.epoch_not_match.take() {
        on_region_epoch_not_match(pd_client.clone(), region_store, epoch_not_match).await
    } else if e.stale_command.is_some() || e.region_not_found.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        Ok(false)
    } else if e.data_is_not_ready.is_some() {
        // Specific to stale read. The target replica is randomly selected and may not have caught up
        // yet. Retry immediately.
        Ok(true)
    } else if e.server_is_busy.is_some() || e.max_timestamp_not_synced.is_some() {
        Ok(false)
    } else if e.raft_entry_too_large.is_some() {
        Err(Error::RegionError(Box::new(e)))
    } else {
        // TODO: pass the logger around
        // info!("unknwon region error: {:?}", e);
        pd_client.invalidate_region_cache(ver_id).await;
        if let Ok(store_id) = store_id {
            pd_client.invalidate_store_cache(store_id).await;
        }
        Ok(false)
    }
}

// Returns
// 1. Ok(true): error has been resolved, retry immediately
// 2. Ok(false): backoff, and then retry
// 3. Err(Error): can't be resolved, return the error to upper level
pub(crate) async fn on_region_epoch_not_match<PdC: PdClient>(
    pd_client: Arc<PdC>,
    region_store: RegionStore,
    error: EpochNotMatch,
) -> Result<bool> {
    let ver_id = region_store.region_with_leader.ver_id();
    if error.current_regions.is_empty() {
        pd_client.invalidate_region_cache(ver_id).await;
        return Ok(true);
    }

    let current_conf_ver = ver_id.conf_ver;
    let current_version = ver_id.ver;
    for r in error.current_regions {
        if r.id == region_store.region_with_leader.id() {
            let Some(region_epoch) = r.region_epoch else {
                break;
            };
            let returned_conf_ver = region_epoch.conf_ver;
            let returned_version = region_epoch.version;

            // Find whether the current region is ahead of TiKV's. If so, backoff.
            if returned_conf_ver < current_conf_ver || returned_version < current_version {
                return Ok(false);
            }
            break;
        }
    }
    // TODO: finer grained processing
    pd_client.invalidate_region_cache(ver_id).await;
    Ok(false)
}

impl<P: Plan, PdC: PdClient> Clone for RetryableMultiRegion<P, PdC> {
    fn clone(&self) -> Self {
        RetryableMultiRegion {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            preserve_region_results: self.preserve_region_results,
            replica_read: self.replica_read,
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable + HasKvContext, PdC: PdClient> Plan for RetryableMultiRegion<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    type Result = Vec<Result<P::Result>>;

    async fn execute(&self) -> Result<Self::Result> {
        // Limit the maximum concurrency of multi-region request. If there are
        // too many concurrent requests, TiKV is more likely to return a "TiKV
        // is busy" error
        let concurrency_permits = Arc::new(Semaphore::new(MULTI_REGION_CONCURRENCY));
        Self::single_plan_handler(
            self.pd_client.clone(),
            self.inner.clone(),
            self.backoff.clone(),
            concurrency_permits.clone(),
            self.preserve_region_results,
            self.replica_read.map(ReplicaReadState::new),
        )
        .await
    }
}

pub struct RetryableAllStores<P: Plan, PdC: PdClient> {
    pub(super) inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
}

impl<P: Plan, PdC: PdClient> Clone for RetryableAllStores<P, PdC> {
    fn clone(&self) -> Self {
        RetryableAllStores {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
        }
    }
}

// About `HasRegionError`:
// Store requests should be return region errors.
// But as the response of only store request by now (UnsafeDestroyRangeResponse) has the `region_error` field,
// we require `HasRegionError` to check whether there is region error returned from TiKV.
#[async_trait]
impl<P: Plan + StoreRequest, PdC: PdClient> Plan for RetryableAllStores<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    type Result = Vec<Result<P::Result>>;

    async fn execute(&self) -> Result<Self::Result> {
        let concurrency_permits = Arc::new(Semaphore::new(MULTI_STORES_CONCURRENCY));
        let stores = self.pd_client.clone().all_stores().await?;
        let mut handles = Vec::with_capacity(stores.len());
        for store in stores {
            let mut clone = self.inner.clone();
            clone.apply_store(&store);
            let handle = tokio::spawn(Self::single_store_handler(
                clone,
                self.backoff.clone(),
                concurrency_permits.clone(),
            ));
            handles.push(handle);
        }
        let results = try_join_all(handles).await?;
        Ok(results.into_iter().collect::<Vec<_>>())
    }
}

impl<P: Plan, PdC: PdClient> RetryableAllStores<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    async fn single_store_handler(
        plan: P,
        mut backoff: Backoff,
        permits: Arc<Semaphore>,
    ) -> Result<P::Result> {
        loop {
            let permit = permits.acquire().await.map_err(|_| Error::InternalError {
                message: "store request concurrency semaphore closed".to_owned(),
            })?;
            let res = plan.execute().await;
            drop(permit);

            match res {
                Ok(mut resp) => {
                    if let Some(e) = resp.key_errors() {
                        return Err(Error::MultipleKeyErrors(e));
                    } else if let Some(e) = resp.region_error() {
                        // Store request should not return region error.
                        return Err(Error::RegionError(Box::new(e)));
                    } else {
                        return Ok(resp);
                    }
                }
                Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                    Some(duration) => {
                        sleep(duration).await;
                        continue;
                    }
                    None => return Err(e),
                },
                Err(e) => return Err(e),
            }
        }
    }
}

/// A technique for merging responses into a single result (with type `Out`).
pub trait Merge<In>: Sized + Clone + Send + Sync + 'static {
    type Out: Send;

    fn merge(&self, input: Vec<Result<In>>) -> Result<Self::Out>;
}

#[derive(Clone)]
pub struct MergeResponse<P: Plan, In, M: Merge<In>> {
    pub inner: P,
    pub merge: M,
    pub phantom: PhantomData<In>,
}

#[async_trait]
impl<In: Clone + Send + Sync + 'static, P: Plan<Result = Vec<Result<In>>>, M: Merge<In>> Plan
    for MergeResponse<P, In, M>
{
    type Result = M::Out;

    async fn execute(&self) -> Result<Self::Result> {
        self.merge.merge(self.inner.execute().await?)
    }
}

/// A merge strategy which collects data from a response into a single type.
#[derive(Clone, Copy)]
pub struct Collect;

/// A merge strategy that only takes the first element. It's used for requests
/// that should have exactly one response, e.g. a get request.
#[derive(Clone, Copy)]
pub struct CollectSingle;

#[doc(hidden)]
#[macro_export]
macro_rules! collect_single {
    ($type_: ty) => {
        impl $crate::request::Merge<$type_> for $crate::request::CollectSingle {
            type Out = $type_;

            fn merge(&self, mut input: Vec<$crate::Result<$type_>>) -> $crate::Result<Self::Out> {
                if input.len() != 1 {
                    return Err($crate::Error::InternalError {
                        message: format!("expected a single response, got {}", input.len()),
                    });
                }

                input.pop().ok_or_else(|| $crate::Error::InternalError {
                    message: "expected a single response".to_owned(),
                })?
            }
        }
    };
}

/// A merge strategy to be used with
/// [`preserve_shard`](super::plan_builder::PlanBuilder::preserve_shard).
/// It matches the shards preserved before and the values returned in the response.
#[derive(Clone, Debug)]
pub struct CollectWithShard;

/// A merge strategy which returns an error if any response is an error and
/// otherwise returns a Vec of the results.
#[derive(Clone, Copy)]
pub struct CollectError;

impl<T: Send> Merge<T> for CollectError {
    type Out = Vec<T>;

    fn merge(&self, input: Vec<Result<T>>) -> Result<Self::Out> {
        input.into_iter().collect()
    }
}

/// Process data into another kind of data.
pub trait Process<In>: Sized + Clone + Send + Sync + 'static {
    type Out: Send;

    fn process(&self, input: Result<In>) -> Result<Self::Out>;
}

#[derive(Clone)]
pub struct ProcessResponse<P: Plan, Pr: Process<P::Result>> {
    pub inner: P,
    pub processor: Pr,
}

#[async_trait]
impl<P: Plan, Pr: Process<P::Result>> Plan for ProcessResponse<P, Pr> {
    type Result = Pr::Out;

    async fn execute(&self) -> Result<Self::Result> {
        self.processor.process(self.inner.execute().await)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DefaultProcessor;

pub struct ResolveLock<P: Plan, PdC: PdClient> {
    pub inner: P,
    pub timestamp: Timestamp,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
    pub keyspace: Keyspace,
    pub pessimistic_region_resolve: bool,
}

impl<P: Plan, PdC: PdClient> Clone for ResolveLock<P, PdC> {
    fn clone(&self) -> Self {
        ResolveLock {
            inner: self.inner.clone(),
            timestamp: self.timestamp.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            keyspace: self.keyspace,
            pessimistic_region_resolve: self.pessimistic_region_resolve,
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable, PdC: PdClient> Plan for ResolveLock<P, PdC>
where
    P::Result: HasLocks,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut plan = self.inner.clone();
        let mut result = plan.execute().await?;
        let mut backoff = self.backoff.clone();
        let mut forced_leader = false;
        loop {
            let locks = result.take_locks();
            if locks.is_empty() {
                return Ok(result);
            }

            if backoff.is_none() {
                return Err(Error::ResolveLockError(locks));
            }

            if !forced_leader {
                if let Some(lock_key) = locks.first().map(|lock| lock.key.clone()) {
                    // Once we meet a lock, retrying against a follower/learner can keep seeing stale
                    // state. Align with client-go behavior by retrying on the region leader.
                    let region = self.pd_client.region_for_key(&Key::from(lock_key)).await?;
                    let region_store = self.pd_client.clone().map_region_to_store(region).await?;
                    plan.apply_store(&region_store)?;
                    forced_leader = true;
                }
            }

            let resolve_result: crate::transaction::ResolveLocksResult =
                resolve_locks_with_options(
                    locks,
                    self.timestamp.clone(),
                    self.pd_client.clone(),
                    self.keyspace,
                    self.pessimistic_region_resolve,
                )
                .await?;
            let ms_before_txn_expired = resolve_result.ms_before_txn_expired;
            let live_locks = resolve_result.live_locks;
            if live_locks.is_empty() {
                result = plan.execute().await?;
            } else {
                match backoff.next_delay_duration() {
                    None => return Err(Error::ResolveLockError(live_locks)),
                    Some(delay_duration) => {
                        let delay_duration = if ms_before_txn_expired > 0 {
                            delay_duration.min(Duration::from_millis(ms_before_txn_expired as u64))
                        } else {
                            delay_duration
                        };
                        sleep(delay_duration).await;
                        result = plan.execute().await?;
                    }
                }
            }
        }
    }
}

pub(crate) struct ResolveLockForRead<P: Plan, PdC: PdClient> {
    pub(crate) inner: P,
    pub(crate) timestamp: Timestamp,
    pub(crate) pd_client: Arc<PdC>,
    pub(crate) backoff: Backoff,
    pub(crate) keyspace: Keyspace,
    pub(crate) lock_tracker: ReadLockTracker,
}

impl<P: Plan, PdC: PdClient> Clone for ResolveLockForRead<P, PdC> {
    fn clone(&self) -> Self {
        ResolveLockForRead {
            inner: self.inner.clone(),
            timestamp: self.timestamp.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            keyspace: self.keyspace,
            lock_tracker: self.lock_tracker.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable + HasKvContext, PdC: PdClient> Plan for ResolveLockForRead<P, PdC>
where
    P::Result: HasLocks,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut plan = self.inner.clone();
        let mut backoff = self.backoff.clone();
        let mut forced_leader = false;

        let (resolved_locks, committed_locks) = self.lock_tracker.snapshot().await;
        if let Some(ctx) = plan.kv_context_mut() {
            ctx.resolved_locks = resolved_locks;
            ctx.committed_locks = committed_locks;
        }

        let mut result = plan.execute().await?;
        loop {
            let locks = result.take_locks();
            if locks.is_empty() {
                return Ok(result);
            }

            if backoff.is_none() {
                return Err(Error::ResolveLockError(locks));
            }

            if !forced_leader {
                if let Some(lock_key) = locks.first().map(|lock| lock.key.clone()) {
                    // Once we meet a lock, retrying against a follower/learner can keep seeing stale
                    // state. Align with client-go behavior by retrying on the region leader.
                    let region = self.pd_client.region_for_key(&Key::from(lock_key)).await?;
                    let region_store = self.pd_client.clone().map_region_to_store(region).await?;
                    plan.apply_store(&region_store)?;
                    if let Some(ctx) = plan.kv_context_mut() {
                        if ctx.stale_read {
                            // Align with client-go `DisableStaleReadMeetLock`: once a stale read
                            // meets a lock, fall back to normal reads on the leader.
                            ctx.stale_read = false;
                            ctx.replica_read = false;
                        }
                    }
                    forced_leader = true;
                }
            }

            let resolve_result: crate::transaction::ResolveLocksForReadResult =
                resolve_locks_for_read(
                    locks,
                    self.timestamp.clone(),
                    self.pd_client.clone(),
                    self.keyspace,
                    self.lock_tracker.clone(),
                )
                .await?;

            let ms_before_txn_expired = resolve_result.ms_before_txn_expired;
            self.lock_tracker
                .extend(
                    resolve_result.resolved_locks,
                    resolve_result.committed_locks,
                )
                .await;

            let (resolved_locks, committed_locks) = self.lock_tracker.snapshot().await;
            if let Some(ctx) = plan.kv_context_mut() {
                ctx.resolved_locks = resolved_locks;
                ctx.committed_locks = committed_locks;
            }

            if ms_before_txn_expired <= 0 {
                result = plan.execute().await?;
                continue;
            }

            match backoff.next_delay_duration() {
                None => return Err(Error::ResolveLockError(resolve_result.live_locks)),
                Some(delay_duration) => {
                    let delay_duration =
                        delay_duration.min(Duration::from_millis(ms_before_txn_expired as u64));
                    sleep(delay_duration).await;
                    result = plan.execute().await?;
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct CleanupLocksResult {
    pub region_error: Option<errorpb::Error>,
    pub key_error: Option<Vec<Error>>,
    pub resolved_locks: usize,
}

impl Clone for CleanupLocksResult {
    fn clone(&self) -> Self {
        Self {
            resolved_locks: self.resolved_locks,
            ..Default::default() // Ignore errors, which should be extracted by `extract_error()`.
        }
    }
}

impl HasRegionError for CleanupLocksResult {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.region_error.take()
    }
}

impl HasKeyErrors for CleanupLocksResult {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.key_error.take()
    }
}

impl Merge<CleanupLocksResult> for Collect {
    type Out = CleanupLocksResult;

    fn merge(&self, input: Vec<Result<CleanupLocksResult>>) -> Result<Self::Out> {
        input
            .into_iter()
            .try_fold(CleanupLocksResult::default(), |acc, x| {
                Ok(CleanupLocksResult {
                    resolved_locks: acc.resolved_locks + x?.resolved_locks,
                    ..Default::default()
                })
            })
    }
}

pub struct CleanupLocks<P: Plan, PdC: PdClient> {
    pub inner: P,
    pub ctx: ResolveLocksContext,
    pub options: ResolveLocksOptions,
    pub store: Option<RegionStore>,
    pub pd_client: Arc<PdC>,
    pub keyspace: Keyspace,
    pub backoff: Backoff,
}

impl<P: Plan, PdC: PdClient> Clone for CleanupLocks<P, PdC> {
    fn clone(&self) -> Self {
        CleanupLocks {
            inner: self.inner.clone(),
            ctx: self.ctx.clone(),
            options: self.options,
            store: None,
            pd_client: self.pd_client.clone(),
            keyspace: self.keyspace,
            backoff: self.backoff.clone(),
        }
    }
}

fn classify_cleanup_extracted_errors(result: &mut CleanupLocksResult, mut errors: Vec<Error>) {
    if errors.is_empty() {
        // Preserve existing behavior for malformed empty error lists.
        result.key_error = Some(Vec::new());
        return;
    }

    // Keep key errors regardless of ordering in extracted-error vectors.
    if errors.iter().any(|err| matches!(err, Error::KeyError(_))) {
        result.key_error = Some(errors);
        return;
    }

    if let Some(index) = errors
        .iter()
        .rposition(|err| matches!(err, Error::RegionError(_)))
    {
        if let Error::RegionError(e) = errors.swap_remove(index) {
            result.region_error = Some(*e);
            return;
        }
    }

    result.key_error = Some(errors);
}

#[async_trait]
impl<P: Plan + Shardable + NextBatch, PdC: PdClient> Plan for CleanupLocks<P, PdC>
where
    P::Result: HasLocks + HasNextBatch + HasKeyErrors + HasRegionError,
{
    type Result = CleanupLocksResult;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = CleanupLocksResult::default();
        let mut inner = self.inner.clone();
        let mut lock_resolver = crate::transaction::LockResolver::new(self.ctx.clone());
        let store = self.store.as_ref().ok_or_else(|| Error::InternalError {
            message: "cleanup locks executed without store".to_owned(),
        })?;
        let region = &store.region_with_leader;
        let mut has_more_batch = true;

        while has_more_batch {
            let mut scan_lock_resp = inner.execute().await?;

            // Propagate errors to `retry_multi_region` for retry.
            if let Some(e) = scan_lock_resp.key_errors() {
                info!("CleanupLocks::execute, inner key errors:{:?}", e);
                result.key_error = Some(e);
                return Ok(result);
            } else if let Some(e) = scan_lock_resp.region_error() {
                info!("CleanupLocks::execute, inner region error:{}", e.message);
                result.region_error = Some(e);
                return Ok(result);
            }

            // Iterate to next batch of inner.
            match scan_lock_resp.has_next_batch() {
                Some(range) if region.contains(range.0.as_ref()) => {
                    debug!("CleanupLocks::execute, next range:{:?}", range);
                    inner.next_batch(range);
                }
                _ => has_more_batch = false,
            }

            let mut locks = scan_lock_resp.take_locks();
            if locks.is_empty() {
                break;
            }
            if locks.len() < self.options.batch_size as usize {
                has_more_batch = false;
            }

            if self.options.async_commit_only {
                locks = locks
                    .into_iter()
                    .filter(|l| l.use_async_commit)
                    .collect::<Vec<_>>();
            }
            debug!("CleanupLocks::execute, meet locks:{}", locks.len());

            let lock_size = locks.len();
            match lock_resolver
                .cleanup_locks(store.clone(), locks, self.pd_client.clone(), self.keyspace)
                .await
            {
                Ok(()) => {
                    result.resolved_locks += lock_size;
                }
                Err(Error::ExtractedErrors(errors)) => {
                    // Propagate errors to `retry_multi_region` for retry.
                    classify_cleanup_extracted_errors(&mut result, errors);
                    return Ok(result);
                }
                Err(e) => {
                    return Err(e);
                }
            }

            // TODO: improve backoff
            // if self.backoff.is_none() {
            //     return Err(Error::ResolveLockError);
            // }
        }

        Ok(result)
    }
}

/// When executed, the plan extracts errors from its inner plan, and returns an
/// `Err` wrapping the error.
///
/// We usually need to apply this plan if (and only if) the output of the inner
/// plan is of a response type.
///
/// The errors come from two places: `Err` from inner plans, and `Ok(response)`
/// where `response` contains unresolved errors (`error` and `region_error`).
pub struct ExtractError<P: Plan> {
    pub inner: P,
}

impl<P: Plan> Clone for ExtractError<P> {
    fn clone(&self) -> Self {
        ExtractError {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan> Plan for ExtractError<P>
where
    P::Result: HasKeyErrors + HasRegionErrors,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = self.inner.execute().await?;
        if let Some(errors) = result.key_errors() {
            Err(Error::ExtractedErrors(errors))
        } else if let Some(errors) = result.region_errors() {
            Err(Error::ExtractedErrors(
                errors
                    .into_iter()
                    .map(|e| Error::RegionError(Box::new(e)))
                    .collect(),
            ))
        } else {
            Ok(result)
        }
    }
}

/// When executed, the plan clones the shard and execute its inner plan, then
/// returns `(shard, response)`.
///
/// It's useful when the information of shard are lost in the response but needed
/// for processing.
pub struct PreserveShard<P: Plan + Shardable> {
    pub inner: P,
    pub shard: Option<P::Shard>,
}

impl<P: Plan + Shardable> Clone for PreserveShard<P> {
    fn clone(&self) -> Self {
        PreserveShard {
            inner: self.inner.clone(),
            shard: None,
        }
    }
}

#[async_trait]
impl<P> Plan for PreserveShard<P>
where
    P: Plan + Shardable,
{
    type Result = ResponseWithShard<P::Result, P::Shard>;

    async fn execute(&self) -> Result<Self::Result> {
        let shard = self
            .shard
            .as_ref()
            .ok_or_else(|| Error::InternalError {
                message: "preserve shard executed without shard".to_owned(),
            })?
            .clone();
        let res = self.inner.execute().await?;
        Ok(ResponseWithShard(res, shard))
    }
}

// contains a response and the corresponding shards
#[derive(Debug, Clone)]
pub struct ResponseWithShard<Resp, Shard>(pub Resp, pub Shard);

impl<Resp: HasKeyErrors, Shard> HasKeyErrors for ResponseWithShard<Resp, Shard> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.0.key_errors()
    }
}

impl<Resp: HasLocks, Shard> HasLocks for ResponseWithShard<Resp, Shard> {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.0.take_locks()
    }
}

impl<Resp: HasRegionError, Shard> HasRegionError for ResponseWithShard<Resp, Shard> {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.0.region_error()
    }
}

#[cfg(test)]
mod test {
    use futures::stream::BoxStream;
    use futures::stream::{self};

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb::BatchGetResponse;

    #[derive(Clone)]
    struct ErrPlan;

    #[async_trait]
    impl Plan for ErrPlan {
        type Result = BatchGetResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Err(Error::Unimplemented)
        }
    }

    impl HasKvContext for ErrPlan {
        fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
            None
        }
    }

    impl Shardable for ErrPlan {
        type Shard = ();

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            Box::pin(stream::iter(1..=3).map(|_| Err(Error::Unimplemented))).boxed()
        }

        fn apply_shard(&mut self, _: Self::Shard) {}

        fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_err() {
        let plan = RetryableMultiRegion {
            inner: ResolveLock {
                inner: ErrPlan,
                timestamp: Timestamp::default(),
                backoff: Backoff::no_backoff(),
                pd_client: Arc::new(MockPdClient::default()),
                keyspace: Keyspace::Disable,
                pessimistic_region_resolve: false,
            },
            pd_client: Arc::new(MockPdClient::default()),
            backoff: Backoff::no_backoff(),
            preserve_region_results: false,
            replica_read: None,
        };
        assert!(plan.execute().await.is_err())
    }

    #[tokio::test]
    async fn test_preserve_shard_execute_missing_shard_returns_error_without_executing_inner() {
        #[derive(Clone)]
        struct PanicPlan;

        #[async_trait]
        impl Plan for PanicPlan {
            type Result = BatchGetResponse;

            async fn execute(&self) -> Result<Self::Result> {
                panic!("inner plan executed unexpectedly");
            }
        }

        impl Shardable for PanicPlan {
            type Shard = ();

            fn shards(
                &self,
                _: &Arc<impl crate::pd::PdClient>,
            ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
                Box::pin(stream::empty()).boxed()
            }

            fn apply_shard(&mut self, _: Self::Shard) {}

            fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
                Ok(())
            }
        }

        let plan = PreserveShard {
            inner: PanicPlan,
            shard: None,
        };

        let err = plan.execute().await.unwrap_err();
        match err {
            Error::InternalError { message } => {
                assert!(message.contains("preserve shard executed without shard"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_cleanup_locks_execute_missing_store_returns_error_without_executing_inner() {
        #[derive(Clone)]
        struct DummyCleanupResp;

        impl HasLocks for DummyCleanupResp {}

        impl HasNextBatch for DummyCleanupResp {
            fn has_next_batch(&self) -> Option<(Vec<u8>, Vec<u8>)> {
                None
            }
        }

        impl HasKeyErrors for DummyCleanupResp {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                None
            }
        }

        impl HasRegionError for DummyCleanupResp {
            fn region_error(&mut self) -> Option<errorpb::Error> {
                None
            }
        }

        #[derive(Clone)]
        struct PanicPlan;

        #[async_trait]
        impl Plan for PanicPlan {
            type Result = DummyCleanupResp;

            async fn execute(&self) -> Result<Self::Result> {
                panic!("inner plan executed unexpectedly");
            }
        }

        impl Shardable for PanicPlan {
            type Shard = ();

            fn shards(
                &self,
                _: &Arc<impl crate::pd::PdClient>,
            ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
                Box::pin(stream::empty()).boxed()
            }

            fn apply_shard(&mut self, _: Self::Shard) {}

            fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
                Ok(())
            }
        }

        impl NextBatch for PanicPlan {
            fn next_batch(&mut self, _: (Vec<u8>, Vec<u8>)) {}
        }

        let plan = CleanupLocks {
            inner: PanicPlan,
            ctx: ResolveLocksContext::default(),
            options: ResolveLocksOptions::default(),
            store: None,
            pd_client: Arc::new(MockPdClient::default()),
            keyspace: Keyspace::Disable,
            backoff: Backoff::no_backoff(),
        };

        let err = plan.execute().await.unwrap_err();
        match err {
            Error::InternalError { message } => {
                assert!(message.contains("cleanup locks executed without store"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_collect_single_merge_requires_exactly_one_response() {
        let merge = CollectSingle;
        let err =
            <CollectSingle as Merge<kvrpcpb::RawGetResponse>>::merge(&merge, vec![]).unwrap_err();
        assert!(matches!(err, Error::InternalError { .. }));

        let err = <CollectSingle as Merge<kvrpcpb::RawGetResponse>>::merge(
            &merge,
            vec![
                Ok(kvrpcpb::RawGetResponse::default()),
                Ok(kvrpcpb::RawGetResponse::default()),
            ],
        )
        .unwrap_err();
        assert!(matches!(err, Error::InternalError { .. }));

        let out = <CollectSingle as Merge<kvrpcpb::RawGetResponse>>::merge(
            &merge,
            vec![Ok(kvrpcpb::RawGetResponse::default())],
        )
        .unwrap();
        let mut out = out;
        assert!(out.key_errors().is_none());
    }

    #[test]
    fn test_classify_cleanup_extracted_errors_keeps_key_error() {
        let mut result = CleanupLocksResult::default();
        classify_cleanup_extracted_errors(&mut result, vec![Error::KeyError(Box::default())]);

        assert!(result.region_error.is_none());
        let errors = result
            .key_error
            .expect("cleanup result should keep key errors");
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], Error::KeyError(_)));
    }

    #[test]
    fn test_classify_cleanup_extracted_errors_sets_region_error() {
        let mut result = CleanupLocksResult::default();
        let mut region_error = errorpb::Error::default();
        region_error.message = "region error".to_string();
        classify_cleanup_extracted_errors(
            &mut result,
            vec![Error::RegionError(Box::new(region_error))],
        );

        assert!(result.key_error.is_none());
        assert_eq!(
            result.region_error.expect("expected region error").message,
            "region error"
        );
    }

    #[test]
    fn test_classify_cleanup_extracted_errors_prefers_key_error_when_region_is_last() {
        let mut result = CleanupLocksResult::default();
        let mut region_error = errorpb::Error::default();
        region_error.message = "region error".to_string();
        classify_cleanup_extracted_errors(
            &mut result,
            vec![
                Error::KeyError(Box::default()),
                Error::RegionError(Box::new(region_error)),
            ],
        );

        assert!(result.region_error.is_none());
        let errors = result
            .key_error
            .expect("cleanup result should preserve key error vectors");
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().any(|err| matches!(err, Error::KeyError(_))));
    }

    #[tokio::test]
    async fn test_on_region_epoch_not_match_missing_region_epoch_does_not_panic() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
        );

        let mut current_region = metapb::Region::default();
        current_region.id = store.region_with_leader.id();
        current_region.region_epoch = None;

        let mut err = EpochNotMatch::default();
        err.current_regions = vec![current_region];

        let resolved = on_region_epoch_not_match(pd_client, store, err)
            .await
            .unwrap();
        assert!(!resolved);
    }

    #[tokio::test]
    async fn test_handle_region_error_not_leader_with_leader_retries_immediately() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
        );

        let mut err = errorpb::Error::default();
        let mut not_leader = errorpb::NotLeader::default();
        not_leader.leader = Some(metapb::Peer {
            store_id: 123,
            ..Default::default()
        });
        err.not_leader = Some(not_leader);

        let resolved = handle_region_error(pd_client, err, store).await.unwrap();
        assert!(resolved);
    }

    #[tokio::test]
    async fn test_handle_region_error_disk_full_retries_with_backoff() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
        );

        let mut err = errorpb::Error::default();
        err.disk_full = Some(errorpb::DiskFull {
            store_id: vec![123],
            reason: "disk full".to_owned(),
        });

        let resolved = handle_region_error(pd_client, err, store).await.unwrap();
        assert!(!resolved);
    }

    #[tokio::test]
    async fn test_dispatch_execute_missing_kv_client_returns_error() {
        let plan = Dispatch {
            request: kvrpcpb::GetRequest::default(),
            kv_client: None,
        };

        let err = plan.execute().await.unwrap_err();
        match err {
            Error::InternalError { message } => {
                assert!(message.contains("kv_client has not been initialised in Dispatch"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_dispatch_execute_downcast_failure_returns_error() {
        let kv_client: Arc<dyn KvClient + Send + Sync> =
            Arc::new(MockKvClient::with_dispatch_hook(|_| Ok(Box::new(()))));
        let plan = Dispatch {
            request: kvrpcpb::GetRequest::default(),
            kv_client: Some(kv_client),
        };

        let err = plan.execute().await.unwrap_err();
        match err {
            Error::InternalError { message } => {
                assert!(message.contains("downcast failed"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
