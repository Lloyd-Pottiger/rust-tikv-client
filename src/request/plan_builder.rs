// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use super::plan::PreserveShard;
use super::Keyspace;
use crate::backoff::Backoff;
use crate::pd::PdClient;
use crate::request::plan::DispatchWithInterceptor;
use crate::request::plan::ResolveLockInContext;
use crate::request::plan::{
    CleanupLocks, HasKvContext, RetryableAllStores, RetryableStores,
    DEFAULT_MULTI_REGION_CONCURRENCY,
};
use crate::request::shard::HasNextBatch;
use crate::request::Dispatch;
use crate::request::ExtractError;
use crate::request::KvRequest;
use crate::request::Merge;
use crate::request::MergeResponse;
use crate::request::NextBatch;
use crate::request::Plan;
use crate::request::Process;
use crate::request::ProcessResponse;
use crate::request::ResolveLock;
use crate::request::ResolveLockForRead;
use crate::request::RetryableMultiRegion;
use crate::request::Shardable;
use crate::request::{DefaultProcessor, StoreRequest};
use crate::rpc_interceptor::RpcInterceptors;
use crate::store::HasKeyErrors;
use crate::store::HasRegionError;
use crate::store::HasRegionErrors;
use crate::store::RegionStore;
use crate::store::Store;
use crate::transaction::HasLocks;
use crate::transaction::LockResolverRpcContext;
use crate::transaction::ReadLockTracker;
use crate::transaction::ResolveLocksContext;
use crate::transaction::ResolveLocksOptions;
use crate::ReplicaReadType;
use crate::Result;
use crate::StoreLabel;
use crate::Timestamp;

/// Builder type for plans (see that module for more).
pub struct PlanBuilder<PdC: PdClient, P: Plan, Ph: PlanBuilderPhase> {
    pd_client: Arc<PdC>,
    plan: P,
    phantom: PhantomData<Ph>,
}

/// Used to ensure that a plan has a designated target or targets, a target is
/// a particular TiKV server.
pub trait PlanBuilderPhase {}
pub struct NoTarget;
impl PlanBuilderPhase for NoTarget {}
pub struct Targetted;
impl PlanBuilderPhase for Targetted {}

impl<PdC: PdClient, Req: KvRequest> PlanBuilder<PdC, Dispatch<Req>, NoTarget> {
    pub fn new(pd_client: Arc<PdC>, keyspace: Keyspace, mut request: Req) -> Self {
        request.set_api_version(keyspace.api_version());
        if let Keyspace::Enable { keyspace_id } = keyspace {
            if let Some(ctx) = request.context_mut() {
                ctx.keyspace_id = keyspace_id;
            }
        }
        PlanBuilder {
            pd_client,
            plan: Dispatch {
                request,
                kv_client: None,
            },
            phantom: PhantomData,
        }
    }

    pub(crate) fn new_with_rpc_interceptors(
        pd_client: Arc<PdC>,
        keyspace: Keyspace,
        mut request: Req,
        rpc_interceptors: RpcInterceptors,
    ) -> PlanBuilder<PdC, DispatchWithInterceptor<Req>, NoTarget> {
        request.set_api_version(keyspace.api_version());
        if let Keyspace::Enable { keyspace_id } = keyspace {
            if let Some(ctx) = request.context_mut() {
                ctx.keyspace_id = keyspace_id;
            }
        }
        PlanBuilder {
            pd_client,
            plan: DispatchWithInterceptor {
                request,
                kv_client: None,
                store_address: None,
                rpc_interceptors,
            },
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan> PlanBuilder<PdC, P, Targetted> {
    /// Return the built plan, note that this can only be called once the plan
    /// has a target.
    pub fn plan(self) -> P {
        self.plan
    }
}

impl<PdC: PdClient, P: Plan, Ph: PlanBuilderPhase> PlanBuilder<PdC, P, Ph> {
    /// If there is a lock error, then resolve the lock and retry the request.
    pub fn resolve_lock(
        self,
        timestamp: Timestamp,
        backoff: Backoff,
        keyspace: Keyspace,
    ) -> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
    where
        P: Shardable,
        P::Result: HasLocks,
    {
        self.resolve_lock_with_pessimistic_region(timestamp, backoff, keyspace, false)
    }

    /// If there is a lock error, then resolve the lock for read requests by updating
    /// `kvrpcpb::Context.{resolved_locks, committed_locks}` and retrying the request.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve_lock_for_read(
        self,
        ctx: ResolveLocksContext,
        timestamp: Timestamp,
        backoff: Backoff,
        killed: Option<Arc<AtomicU32>>,
        keyspace: Keyspace,
        force_resolve_lock_lite: bool,
        lock_tracker: ReadLockTracker,
        rpc_context: LockResolverRpcContext,
    ) -> PlanBuilder<PdC, ResolveLockForRead<P, PdC>, Ph>
    where
        P: Shardable + HasKvContext,
        P::Result: HasLocks,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ResolveLockForRead {
                inner: self.plan,
                ctx,
                timestamp,
                backoff,
                killed,
                pd_client: self.pd_client,
                keyspace,
                force_resolve_lock_lite,
                lock_tracker,
                rpc_context,
            },
            phantom: PhantomData,
        }
    }

    pub(crate) fn resolve_lock_in_context(
        self,
        ctx: ResolveLocksContext,
        timestamp: Timestamp,
        backoff: Backoff,
        killed: Option<Arc<AtomicU32>>,
        keyspace: Keyspace,
        rpc_context: LockResolverRpcContext,
    ) -> PlanBuilder<PdC, ResolveLockInContext<P, PdC>, Ph>
    where
        P: Shardable,
        P::Result: HasLocks,
    {
        self.resolve_lock_with_pessimistic_region_in_context(
            ctx,
            timestamp,
            backoff,
            killed,
            keyspace,
            false,
            rpc_context,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve_lock_with_pessimistic_region_in_context(
        self,
        ctx: ResolveLocksContext,
        timestamp: Timestamp,
        backoff: Backoff,
        killed: Option<Arc<AtomicU32>>,
        keyspace: Keyspace,
        pessimistic_region_resolve: bool,
        rpc_context: LockResolverRpcContext,
    ) -> PlanBuilder<PdC, ResolveLockInContext<P, PdC>, Ph>
    where
        P: Shardable,
        P::Result: HasLocks,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ResolveLockInContext {
                inner: self.plan,
                ctx,
                timestamp,
                backoff,
                killed,
                pd_client: self.pd_client,
                keyspace,
                pessimistic_region_resolve,
                rpc_context,
            },
            phantom: PhantomData,
        }
    }

    pub fn resolve_lock_with_pessimistic_region(
        self,
        timestamp: Timestamp,
        backoff: Backoff,
        keyspace: Keyspace,
        pessimistic_region_resolve: bool,
    ) -> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
    where
        P: Shardable,
        P::Result: HasLocks,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ResolveLock {
                inner: self.plan,
                timestamp,
                backoff,
                pd_client: self.pd_client,
                keyspace,
                pessimistic_region_resolve,
            },
            phantom: PhantomData,
        }
    }

    pub fn cleanup_locks(
        self,
        ctx: ResolveLocksContext,
        options: ResolveLocksOptions,
        keyspace: Keyspace,
    ) -> PlanBuilder<PdC, CleanupLocks<P, PdC>, Ph>
    where
        P: Shardable + NextBatch + HasKvContext,
        P::Result: HasLocks + HasNextBatch + HasRegionError + HasKeyErrors,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: CleanupLocks {
                inner: self.plan,
                ctx,
                options,
                store: None,
                pd_client: self.pd_client,
                keyspace,
            },
            phantom: PhantomData,
        }
    }

    /// Merge the results of a request. Usually used where a request is sent to multiple regions
    /// to combine the responses from each region.
    pub fn merge<In, M: Merge<In>>(self, merge: M) -> PlanBuilder<PdC, MergeResponse<P, In, M>, Ph>
    where
        In: Clone + Send + Sync + 'static,
        P: Plan<Result = Vec<Result<In>>>,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: MergeResponse {
                inner: self.plan,
                merge,
                phantom: PhantomData,
            },
            phantom: PhantomData,
        }
    }

    /// Apply the default processing step to a response (usually only needed if the request is sent
    /// to a single region because post-porcessing can be incorporated in the merge step for
    /// multi-region requests).
    pub fn post_process_default(self) -> PlanBuilder<PdC, ProcessResponse<P, DefaultProcessor>, Ph>
    where
        P: Plan,
        DefaultProcessor: Process<P::Result>,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ProcessResponse {
                inner: self.plan,
                processor: DefaultProcessor,
            },
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan + Shardable + HasKvContext> PlanBuilder<PdC, P, NoTarget>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    /// Split the request into shards sending a request to the region of each shard.
    pub fn retry_multi_region(
        self,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(
            backoff,
            false,
            None,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            DEFAULT_MULTI_REGION_CONCURRENCY,
        )
    }

    /// Split the request into shards sending a request to the region of each shard, with a custom maximum concurrency.
    pub fn retry_multi_region_with_concurrency(
        self,
        backoff: Backoff,
        concurrency: usize,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(
            backoff,
            false,
            None,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            concurrency,
        )
    }

    /// Preserve all results, even some of them are Err.
    /// To pass all responses to merge, and handle partial successful results correctly.
    pub fn retry_multi_region_preserve_results(
        self,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(
            backoff,
            true,
            None,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            DEFAULT_MULTI_REGION_CONCURRENCY,
        )
    }

    /// Split the request into shards, routing to non-leader replicas when requested.
    ///
    /// This is primarily useful for read-only snapshots.
    pub fn retry_multi_region_with_replica_read(
        self,
        backoff: Backoff,
        replica_read: ReplicaReadType,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(
            backoff,
            false,
            Some(replica_read),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            DEFAULT_MULTI_REGION_CONCURRENCY,
        )
    }

    /// Split the request into shards, routing to non-leader replicas when requested and filtering
    /// target stores by matching labels.
    ///
    /// This is primarily useful for read-only snapshots.
    pub fn retry_multi_region_with_replica_read_and_match_store_labels(
        self,
        backoff: Backoff,
        replica_read: ReplicaReadType,
        match_store_labels: Arc<Vec<StoreLabel>>,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.retry_multi_region_with_replica_read_and_match_stores(
            backoff,
            replica_read,
            Arc::new(Vec::new()),
            match_store_labels,
        )
    }

    /// Split the request into shards, routing to non-leader replicas when requested and filtering
    /// target stores by matching store ids.
    ///
    /// This is primarily useful for read-only snapshots.
    pub fn retry_multi_region_with_replica_read_and_match_store_ids(
        self,
        backoff: Backoff,
        replica_read: ReplicaReadType,
        match_store_ids: Arc<Vec<u64>>,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.retry_multi_region_with_replica_read_and_match_stores(
            backoff,
            replica_read,
            match_store_ids,
            Arc::new(Vec::new()),
        )
    }

    /// Split the request into shards, routing to non-leader replicas when requested and filtering
    /// target stores by matching store ids and labels.
    ///
    /// This is primarily useful for read-only snapshots.
    pub fn retry_multi_region_with_replica_read_and_match_stores(
        self,
        backoff: Backoff,
        replica_read: ReplicaReadType,
        match_store_ids: Arc<Vec<u64>>,
        match_store_labels: Arc<Vec<StoreLabel>>,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(
            backoff,
            false,
            Some(replica_read),
            match_store_ids,
            match_store_labels,
            DEFAULT_MULTI_REGION_CONCURRENCY,
        )
    }

    fn make_retry_multi_region(
        self,
        backoff: Backoff,
        preserve_region_results: bool,
        replica_read: Option<ReplicaReadType>,
        match_store_ids: Arc<Vec<u64>>,
        match_store_labels: Arc<Vec<StoreLabel>>,
        concurrency: usize,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: RetryableMultiRegion {
                inner: self.plan,
                pd_client: self.pd_client,
                backoff,
                killed: None,
                concurrency,
                preserve_region_results,
                replica_read,
                match_store_ids,
                match_store_labels,
            },
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan + Shardable + HasKvContext>
    PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    pub(crate) fn with_killed(mut self, killed: Option<Arc<AtomicU32>>) -> Self {
        self.plan.killed = killed;
        self
    }
}

impl<PdC: PdClient, R: KvRequest> PlanBuilder<PdC, Dispatch<R>, NoTarget> {
    /// Target the request at a single region; caller supplies the store to target.
    pub async fn single_region_with_store(
        self,
        store: RegionStore,
    ) -> Result<PlanBuilder<PdC, Dispatch<R>, Targetted>> {
        set_single_region_store(self.plan, store, self.pd_client)
    }
}

impl<PdC: PdClient, R: KvRequest> PlanBuilder<PdC, DispatchWithInterceptor<R>, NoTarget> {
    /// Target the request at a single region; caller supplies the store to target.
    pub async fn single_region_with_store(
        mut self,
        store: RegionStore,
    ) -> Result<PlanBuilder<PdC, DispatchWithInterceptor<R>, Targetted>> {
        self.plan.request.set_leader(&store.region_with_leader)?;
        self.plan.kv_client = Some(store.client);
        self.plan.store_address = Some(store.store_address);
        Ok(PlanBuilder {
            plan: self.plan,
            pd_client: self.pd_client,
            phantom: PhantomData,
        })
    }
}

impl<PdC: PdClient, P: Plan + StoreRequest> PlanBuilder<PdC, P, NoTarget>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    pub fn all_stores(
        self,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableAllStores<P, PdC>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: RetryableAllStores {
                inner: self.plan,
                pd_client: self.pd_client,
                backoff,
            },
            phantom: PhantomData,
        }
    }

    pub fn stores(
        self,
        stores: Vec<Store>,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableStores<P>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: RetryableStores {
                inner: self.plan,
                stores: Arc::new(stores),
                backoff,
            },
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan + Shardable> PlanBuilder<PdC, P, NoTarget>
where
    P::Result: HasKeyErrors,
{
    pub fn preserve_shard(self) -> PlanBuilder<PdC, PreserveShard<P>, NoTarget> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: PreserveShard {
                inner: self.plan,
                shard: None,
            },
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan> PlanBuilder<PdC, P, Targetted>
where
    P::Result: HasKeyErrors + HasRegionErrors,
{
    pub fn extract_error(self) -> PlanBuilder<PdC, ExtractError<P>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client,
            plan: ExtractError { inner: self.plan },
            phantom: self.phantom,
        }
    }
}

fn set_single_region_store<PdC: PdClient, R: KvRequest>(
    mut plan: Dispatch<R>,
    store: RegionStore,
    pd_client: Arc<PdC>,
) -> Result<PlanBuilder<PdC, Dispatch<R>, Targetted>> {
    plan.request.set_leader(&store.region_with_leader)?;
    plan.kv_client = Some(store.client);
    Ok(PlanBuilder {
        plan,
        pd_client,
        phantom: PhantomData,
    })
}

/// Indicates that a request operates on a single key.
pub trait SingleKey {
    #[allow(clippy::ptr_arg)]
    fn key(&self) -> &Vec<u8>;
}
