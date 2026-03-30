// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! A utility module for managing and retrying PD requests.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use futures::future::BoxFuture;
use tokio::sync::RwLock;

use crate::internal_err;
use crate::pd::Cluster;
use crate::pd::Connection;
use crate::proto::keyspacepb;
use crate::proto::metapb;
use crate::proto::pdpb::Timestamp;
use crate::proto::pdpb::{self};
use crate::region::RegionId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::stats::pd_stats;
use crate::Error;
use crate::Result;
use crate::SecurityManager;

// Retry bounds for PD leader changes / reconnect churn.
//
// These are intentionally conservative defaults to keep caller latency predictable. If needed,
// we can expose them via `Config`.
const RECONNECT_INTERVAL_SEC: u64 = 1;
const MAX_REQUEST_COUNT: usize = 5;
const LEADER_CHANGE_RETRY: usize = 10;

#[async_trait]
/// Low-level PD RPC surface that adds reconnect-and-retry semantics around raw PD requests.
///
/// Unlike [`crate::PdClient`], this trait works with PD-native protobuf types and leaves any key
/// encoding/decoding to the caller.
pub trait RetryClientTrait {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    /// Loads the region containing `key` from PD.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader>;

    /// Retrieve region metadata from PD along with its buckets, if available.
    ///
    /// This mirrors client-go `opt.WithBuckets()` when loading regions from PD.
    ///
    /// The default implementation falls back to [`Self::get_region`] and returns no buckets.
    async fn get_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        let region = self.clone().get_region(key).await?;
        Ok((region, None))
    }

    #[doc(hidden)]
    async fn get_region_with_buckets_allow_follower_handle(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        self.get_region_with_buckets(key).await
    }

    /// Return the region whose end boundary is `key` (i.e. the region immediately before `key`).
    ///
    /// This maps to PD `GetPrevRegion` and is used by reverse raw scans (`LocateEndKey` behavior).
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        let _ = key;
        Err(Error::Unimplemented)
    }

    /// Retrieve "previous region" metadata from PD along with its buckets, if available.
    ///
    /// The default implementation falls back to [`Self::get_prev_region`] and returns no buckets.
    async fn get_prev_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        let region = self.clone().get_prev_region(key).await?;
        Ok((region, None))
    }

    /// Loads region metadata by region id from PD.
    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader>;

    /// Retrieve region metadata by id from PD along with its buckets, if available.
    ///
    /// The default implementation falls back to [`Self::get_region_by_id`] and returns no buckets.
    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        let region = self.clone().get_region_by_id(region_id).await?;
        Ok((region, None))
    }

    #[doc(hidden)]
    async fn get_region_by_id_with_buckets_allow_follower_handle(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        self.get_region_by_id_with_buckets(region_id).await
    }

    /// Scans region metadata over the half-open range `[start_key, end_key)`.
    async fn scan_regions(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: i32,
    ) -> Result<Vec<RegionWithLeader>> {
        let _ = (start_key, end_key, limit);
        Err(Error::Unimplemented)
    }

    #[doc(hidden)]
    async fn scan_regions_allow_follower_handle(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: i32,
    ) -> Result<Vec<RegionWithLeader>> {
        self.scan_regions(start_key, end_key, limit).await
    }

    /// Batch-scans region metadata for multiple ranges in one PD request.
    async fn batch_scan_regions(
        self: Arc<Self>,
        ranges: Vec<pdpb::KeyRange>,
        limit: i32,
        need_buckets: bool,
    ) -> Result<Vec<(RegionWithLeader, Option<metapb::Buckets>)>> {
        let _ = (ranges, limit, need_buckets);
        Err(Error::Unimplemented)
    }

    #[doc(hidden)]
    async fn batch_scan_regions_allow_follower_handle(
        self: Arc<Self>,
        ranges: Vec<pdpb::KeyRange>,
        limit: i32,
        need_buckets: bool,
    ) -> Result<Vec<(RegionWithLeader, Option<metapb::Buckets>)>> {
        self.batch_scan_regions(ranges, limit, need_buckets).await
    }

    /// Loads raw store metadata for a single store id from PD.
    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store>;

    /// Loads raw store metadata for all stores visible to PD.
    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>>;

    /// Allocates a fresh TSO timestamp from PD.
    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    /// Allocates a fresh TSO timestamp from the specified PD local TSO/DC location.
    ///
    /// Implementations that do not support local TSO may fall back to [`Self::get_timestamp`].
    async fn get_timestamp_with_dc_location(
        self: Arc<Self>,
        dc_location: String,
    ) -> Result<Timestamp> {
        let _ = dc_location;
        self.get_timestamp().await
    }

    /// Retrieves the minimum TSO across all PD keyspace groups, when supported.
    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        Err(Error::Unimplemented)
    }

    /// Reads the external consistency timestamp maintained by PD, when supported.
    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    /// Updates PD's external consistency timestamp, when supported.
    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        let _ = timestamp;
        Err(Error::Unimplemented)
    }

    /// Requests PD to scatter the specified regions, optionally within a scheduling group.
    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<u64>,
        group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        let _ = (region_ids, group);
        Err(Error::Unimplemented)
    }

    /// Reads the current PD operator status for `region_id`.
    async fn get_operator(self: Arc<Self>, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let _ = region_id;
        Err(Error::Unimplemented)
    }

    /// Returns the cluster-wide GC safe point from PD.
    async fn get_gc_safe_point(self: Arc<Self>) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    /// Returns the GC safe point for a specific keyspace from PD.
    async fn get_gc_safe_point_v2(self: Arc<Self>, keyspace_id: u32) -> Result<u64> {
        let _ = keyspace_id;
        Err(Error::Unimplemented)
    }

    /// Updates PD's service GC safe point for `service_id`.
    async fn update_service_gc_safe_point(
        self: Arc<Self>,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        let _ = (service_id, ttl, safe_point);
        Err(Error::Unimplemented)
    }

    /// Updates PD's keyspace-scoped service GC safe point for `service_id`.
    async fn update_service_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        let _ = (keyspace_id, service_id, ttl, safe_point);
        Err(Error::Unimplemented)
    }

    /// Updates the PD GC safe point for a specific keyspace.
    async fn update_gc_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        safe_point: u64,
    ) -> Result<u64> {
        let _ = (keyspace_id, safe_point);
        Err(Error::Unimplemented)
    }

    /// Updates the cluster-wide GC safe point in PD.
    ///
    /// On success, returns the effective safe point that PD accepted after monotonicity checks.
    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64>;

    /// Loads keyspace metadata for `keyspace` from PD.
    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta>;
}
/// Client for communication with a PD cluster. Has the facility to reconnect to the cluster.
pub struct RetryClient<Cl = Cluster> {
    cluster_id: u64,
    // Tuple is the cluster and the time of the cluster's last reconnect.
    cluster: RwLock<(Cl, Instant)>,
    connection: Connection,
    timeout: Duration,
    tso_max_pending_count: usize,
}

impl<Cl> RetryClient<Cl> {
    pub(crate) fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    async fn retry_mut_first_then_retry<T, First, Retry>(
        &self,
        tag: &'static str,
        mut first_call: First,
        mut retry_call: Retry,
    ) -> Result<T>
    where
        T: std::any::Any,
        RetryClient<Cl>: Reconnect<Cl = Cl>,
        First: for<'a> FnMut(&'a mut Cl) -> BoxFuture<'a, Result<T>>,
        Retry: for<'a> FnMut(&'a mut Cl) -> BoxFuture<'a, Result<T>>,
    {
        let stats = pd_stats(tag);
        let mut last_err = Ok(());

        'retry: {
            for attempt in 0..LEADER_CHANGE_RETRY {
                let res = {
                    let cluster = &mut self.cluster.write().await.0;
                    if attempt == 0 {
                        first_call(cluster).await
                    } else {
                        retry_call(cluster).await
                    }
                };

                match stats.done(res) {
                    Ok(r) => break 'retry Ok(r),
                    Err(Error::Unimplemented) => break 'retry Err(Error::Unimplemented),
                    Err(Error::GrpcAPI(status)) if status.code() == tonic::Code::Unimplemented => {
                        break 'retry Err(Error::Unimplemented);
                    }
                    Err(e) => last_err = Err(e),
                }

                let mut reconnect_count = MAX_REQUEST_COUNT;
                while let Err(e) = self.reconnect(RECONNECT_INTERVAL_SEC).await {
                    reconnect_count -= 1;
                    if reconnect_count == 0 {
                        break 'retry Err(e);
                    }
                    crate::util::sleep_backoff(Duration::from_secs(RECONNECT_INTERVAL_SEC)).await;
                }
            }

            match last_err {
                Ok(()) => Err(internal_err!("pd retry exhausted without error: {}", tag)),
                Err(e) => Err(e),
            }
        }
    }
}

#[cfg(test)]
impl<Cl> RetryClient<Cl> {
    /// Build a retrying PD client around a preconstructed test cluster implementation.
    ///
    /// This keeps the production reconnect/backoff wrapper intact while letting unit tests inject
    /// a mocked `Cluster` transport and deterministic timeout/cluster-id values.
    pub fn new_with_cluster(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        cluster_id: u64,
        cluster: Cl,
    ) -> RetryClient<Cl> {
        let connection = Connection::new(security_mgr);
        RetryClient {
            cluster_id,
            cluster: RwLock::new((cluster, Instant::now())),
            connection,
            timeout,
            tso_max_pending_count: crate::config::DEFAULT_TSO_MAX_PENDING_COUNT,
        }
    }
}

macro_rules! retry_core {
    ($self: ident, $tag: literal, $call: expr) => {{
        let stats = pd_stats($tag);
        let mut last_err = Ok(());
        'retry: {
            for _ in 0..LEADER_CHANGE_RETRY {
                let res = $call;

                match stats.done(res) {
                    Ok(r) => break 'retry Ok(r),
                    Err(Error::Unimplemented) => break 'retry Err(Error::Unimplemented),
                    Err(Error::GrpcAPI(status)) if status.code() == tonic::Code::Unimplemented => {
                        break 'retry Err(Error::Unimplemented);
                    }
                    Err(e) => last_err = Err(e),
                }

                let mut reconnect_count = MAX_REQUEST_COUNT;
                while let Err(e) = $self.reconnect(RECONNECT_INTERVAL_SEC).await {
                    reconnect_count -= 1;
                    if reconnect_count == 0 {
                        break 'retry Err(e);
                    }
                    crate::util::sleep_backoff(Duration::from_secs(RECONNECT_INTERVAL_SEC)).await;
                }
            }

            match last_err {
                Ok(()) => Err(internal_err!(concat!(
                    "pd retry exhausted without error: ",
                    $tag
                ))),
                Err(e) => Err(e),
            }
        }
    }};
}

macro_rules! retry_mut {
    ($self: ident, $tag: literal, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $tag, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let $cluster = &mut $self.cluster.write().await.0;
            $call.await
        })
    }};
}

macro_rules! retry {
    ($self: ident, $tag: literal, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $tag, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let $cluster = &$self.cluster.read().await.0;
            $call.await
        })
    }};
}

impl RetryClient<Cluster> {
    /// Connects to a PD cluster and prepares the retrying/reconnecting wrapper.
    ///
    /// `endpoints` seeds initial PD discovery, `security_mgr` configures TLS, `timeout` is used
    /// for PD RPCs, and `tso_max_pending_count` tunes TSO request batching/backpressure.
    pub async fn connect(
        endpoints: &[String],
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        tso_max_pending_count: usize,
    ) -> Result<RetryClient> {
        let connection = Connection::new(security_mgr);
        let cluster = connection
            .connect_cluster(endpoints, timeout, tso_max_pending_count)
            .await?;
        let cluster_id = cluster.cluster_id();
        let cluster = RwLock::new((cluster, Instant::now()));
        Ok(RetryClient {
            cluster_id,
            cluster,
            connection,
            timeout,
            tso_max_pending_count,
        })
    }

    pub(crate) async fn refresh_router_service_membership_once(&self) -> Result<()> {
        let (members, cluster_id) = {
            let guard = self.cluster.read().await;
            let cluster = &guard.0;
            (cluster.clone_members(), cluster.cluster_id())
        };

        let channels = self
            .connection
            .try_connect_router_channels(&members, cluster_id, self.timeout)
            .await?;

        let mut guard = self.cluster.write().await;
        guard.0.set_router_channels(channels);
        Ok(())
    }
}

#[async_trait]
impl RetryClientTrait for RetryClient<Cluster> {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_region", |cluster| {
                let key = key.clone();
                async {
                    cluster
                        .get_region(key.clone(), self.timeout)
                        .await
                        .and_then(|resp| {
                            region_from_response(resp, || Error::RegionForKeyNotFound { key })
                        })
                }
            }),
        )
    }

    async fn get_region_with_buckets_allow_follower_handle(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        let started_at = Instant::now();
        let timeout = self.timeout;
        finish_pd_wait(
            started_at,
            self.retry_mut_first_then_retry(
                "get_region",
                |cluster| {
                    let key = key.clone();
                    Box::pin(async move {
                        cluster
                            .get_region_with_buckets_allow_follower_handle(key.clone(), timeout)
                            .await
                            .and_then(|resp| {
                                region_and_buckets_from_response(resp, || {
                                    Error::RegionForKeyNotFound { key }
                                })
                            })
                    })
                },
                |cluster| {
                    let key = key.clone();
                    Box::pin(async move {
                        cluster
                            .get_region_with_buckets(key.clone(), timeout)
                            .await
                            .and_then(|resp| {
                                region_and_buckets_from_response(resp, || {
                                    Error::RegionForKeyNotFound { key }
                                })
                            })
                    })
                },
            )
            .await,
        )
    }

    async fn get_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_region", |cluster| {
                let key = key.clone();
                async {
                    cluster
                        .get_region_with_buckets(key.clone(), self.timeout)
                        .await
                        .and_then(|resp| {
                            region_and_buckets_from_response(resp, || Error::RegionForKeyNotFound {
                                key,
                            })
                        })
                }
            }),
        )
    }

    async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_prev_region", |cluster| {
                let key = key.clone();
                async {
                    cluster
                        .get_prev_region(key.clone(), self.timeout)
                        .await
                        .and_then(|resp| {
                            region_from_response(resp, || Error::RegionForKeyNotFound { key })
                        })
                }
            }),
        )
    }

    async fn get_prev_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_prev_region", |cluster| {
                let key = key.clone();
                async {
                    cluster
                        .get_prev_region_with_buckets(key.clone(), self.timeout)
                        .await
                        .and_then(|resp| {
                            region_and_buckets_from_response(resp, || Error::RegionForKeyNotFound {
                                key,
                            })
                        })
                }
            }),
        )
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_region_by_id", |cluster| async {
                cluster
                    .get_region_by_id(region_id, self.timeout)
                    .await
                    .and_then(|resp| {
                        region_from_response(resp, || Error::RegionNotFoundInResponse { region_id })
                    })
            }),
        )
    }

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_region_by_id", |cluster| async {
                cluster
                    .get_region_by_id_with_buckets(region_id, self.timeout)
                    .await
                    .and_then(|resp| {
                        region_and_buckets_from_response(resp, || Error::RegionNotFoundInResponse {
                            region_id,
                        })
                    })
            }),
        )
    }

    async fn get_region_by_id_with_buckets_allow_follower_handle(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
        let started_at = Instant::now();
        let timeout = self.timeout;
        finish_pd_wait(
            started_at,
            self.retry_mut_first_then_retry(
                "get_region_by_id",
                |cluster| {
                    Box::pin(async move {
                        cluster
                            .get_region_by_id_with_buckets_allow_follower_handle(region_id, timeout)
                            .await
                            .and_then(|resp| {
                                region_and_buckets_from_response(resp, || {
                                    Error::RegionNotFoundInResponse { region_id }
                                })
                            })
                    })
                },
                |cluster| {
                    Box::pin(async move {
                        cluster
                            .get_region_by_id_with_buckets(region_id, timeout)
                            .await
                            .and_then(|resp| {
                                region_and_buckets_from_response(resp, || {
                                    Error::RegionNotFoundInResponse { region_id }
                                })
                            })
                    })
                },
            )
            .await,
        )
    }

    async fn scan_regions(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: i32,
    ) -> Result<Vec<RegionWithLeader>> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "scan_regions", |cluster| {
                let start_key = start_key.clone();
                let end_key = end_key.clone();
                async {
                    cluster
                        .scan_regions(start_key, end_key, limit, self.timeout)
                        .await
                        .and_then(scan_regions_from_response)
                }
            }),
        )
    }

    async fn scan_regions_allow_follower_handle(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: i32,
    ) -> Result<Vec<RegionWithLeader>> {
        let started_at = Instant::now();
        let timeout = self.timeout;
        finish_pd_wait(
            started_at,
            self.retry_mut_first_then_retry(
                "scan_regions",
                |cluster| {
                    let start_key = start_key.clone();
                    let end_key = end_key.clone();
                    Box::pin(async move {
                        cluster
                            .scan_regions_allow_follower_handle(start_key, end_key, limit, timeout)
                            .await
                            .and_then(scan_regions_from_response)
                    })
                },
                |cluster| {
                    let start_key = start_key.clone();
                    let end_key = end_key.clone();
                    Box::pin(async move {
                        cluster
                            .scan_regions(start_key, end_key, limit, timeout)
                            .await
                            .and_then(scan_regions_from_response)
                    })
                },
            )
            .await,
        )
    }

    async fn batch_scan_regions(
        self: Arc<Self>,
        ranges: Vec<pdpb::KeyRange>,
        limit: i32,
        need_buckets: bool,
    ) -> Result<Vec<(RegionWithLeader, Option<metapb::Buckets>)>> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "batch_scan_regions", |cluster| {
                let ranges = ranges.clone();
                async {
                    cluster
                        .batch_scan_regions(ranges, limit, need_buckets, self.timeout)
                        .await
                        .and_then(batch_scan_regions_from_response)
                }
            }),
        )
    }

    async fn batch_scan_regions_allow_follower_handle(
        self: Arc<Self>,
        ranges: Vec<pdpb::KeyRange>,
        limit: i32,
        need_buckets: bool,
    ) -> Result<Vec<(RegionWithLeader, Option<metapb::Buckets>)>> {
        let started_at = Instant::now();
        let timeout = self.timeout;
        finish_pd_wait(
            started_at,
            self.retry_mut_first_then_retry(
                "batch_scan_regions",
                |cluster| {
                    let ranges = ranges.clone();
                    Box::pin(async move {
                        cluster
                            .batch_scan_regions_allow_follower_handle(
                                ranges,
                                limit,
                                need_buckets,
                                timeout,
                            )
                            .await
                            .and_then(batch_scan_regions_from_response)
                    })
                },
                |cluster| {
                    let ranges = ranges.clone();
                    Box::pin(async move {
                        cluster
                            .batch_scan_regions(ranges, limit, need_buckets, timeout)
                            .await
                            .and_then(batch_scan_regions_from_response)
                    })
                },
            )
            .await,
        )
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store> {
        let started_at = Instant::now();
        let timeout = self.timeout;
        finish_pd_wait(
            started_at,
            self.retry_mut_first_then_retry(
                "get_store",
                |cluster| {
                    Box::pin(async move {
                        cluster
                            .get_store(id, timeout)
                            .await
                            .and_then(|resp| store_from_response(resp, id))
                    })
                },
                |cluster| {
                    Box::pin(async move {
                        cluster
                            .get_store_from_leader(id, timeout)
                            .await
                            .and_then(|resp| store_from_response(resp, id))
                    })
                },
            )
            .await,
        )
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        let started_at = Instant::now();
        let timeout = self.timeout;
        finish_pd_wait(
            started_at,
            self.retry_mut_first_then_retry(
                "get_all_stores",
                |cluster| {
                    Box::pin(async move {
                        cluster
                            .get_all_stores(timeout)
                            .await
                            .map(|resp| resp.stores.into_iter().map(Into::into).collect())
                    })
                },
                |cluster| {
                    Box::pin(async move {
                        cluster
                            .get_all_stores_from_leader(timeout)
                            .await
                            .map(|resp| resp.stores.into_iter().map(Into::into).collect())
                    })
                },
            )
            .await,
        )
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry!(self, "get_timestamp", |cluster| cluster.get_timestamp()),
        )
    }

    async fn get_timestamp_with_dc_location(
        self: Arc<Self>,
        dc_location: String,
    ) -> Result<Timestamp> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry!(self, "get_timestamp_with_dc_location", |cluster| {
                cluster.get_timestamp_with_dc_location(dc_location.clone())
            }),
        )
    }

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_min_ts", |cluster| async {
                cluster.get_min_ts(self.timeout).await
            }),
        )
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_external_timestamp", |cluster| async {
                cluster.get_external_timestamp(self.timeout).await
            }),
        )
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "set_external_timestamp", |cluster| async {
                cluster
                    .set_external_timestamp(timestamp, self.timeout)
                    .await
            }),
        )
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<u64>,
        group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "scatter_regions", |cluster| {
                let region_ids = region_ids.clone();
                let group = group.clone();
                async {
                    cluster
                        .scatter_regions(region_ids, group, self.timeout)
                        .await
                }
            }),
        )
    }

    async fn get_operator(self: Arc<Self>, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_operator", |cluster| async {
                cluster.get_operator(region_id, self.timeout).await
            }),
        )
    }

    async fn get_gc_safe_point(self: Arc<Self>) -> Result<u64> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_gc_safe_point", |cluster| async {
                cluster
                    .get_gc_safe_point(self.timeout)
                    .await
                    .map(|resp| resp.safe_point)
            }),
        )
    }

    async fn get_gc_safe_point_v2(self: Arc<Self>, keyspace_id: u32) -> Result<u64> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "get_gc_safe_point_v2", |cluster| async {
                cluster
                    .get_gc_safe_point_v2(keyspace_id, self.timeout)
                    .await
                    .map(|resp| resp.safe_point)
            }),
        )
    }

    async fn update_service_gc_safe_point(
        self: Arc<Self>,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "update_service_gc_safe_point", |cluster| {
                let service_id = service_id.clone();
                async {
                    cluster
                        .update_service_gc_safe_point(service_id, ttl, safe_point, self.timeout)
                        .await
                        .map(|resp| resp.min_safe_point)
                }
            }),
        )
    }

    async fn update_service_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "update_service_safe_point_v2", |cluster| {
                let service_id = service_id.clone();
                async {
                    cluster
                        .update_service_safe_point_v2(
                            keyspace_id,
                            service_id,
                            ttl,
                            safe_point,
                            self.timeout,
                        )
                        .await
                        .map(|resp| resp.min_safe_point)
                }
            }),
        )
    }

    async fn update_gc_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        safe_point: u64,
    ) -> Result<u64> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "update_gc_safe_point_v2", |cluster| async {
                cluster
                    .update_gc_safe_point_v2(keyspace_id, safe_point, self.timeout)
                    .await
                    .map(|resp| resp.new_safe_point)
            }),
        )
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "update_gc_safepoint", |cluster| async {
                cluster
                    .update_safepoint(safepoint, self.timeout)
                    .await
                    .map(|resp| resp.new_safe_point)
            }),
        )
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        let started_at = Instant::now();
        finish_pd_wait(
            started_at,
            retry_mut!(self, "load_keyspace", |cluster| async {
                cluster.load_keyspace(keyspace, self.timeout).await
            }),
        )
    }
}

impl fmt::Debug for RetryClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("pd::RetryClient")
            .field("timeout", &self.timeout)
            .finish()
    }
}

fn finish_pd_wait<T>(started_at: Instant, result: Result<T>) -> Result<T> {
    crate::util::record_task_local_wait_pd_response(started_at.elapsed());
    result
}

fn region_from_response(
    mut resp: pdpb::GetRegionResponse,
    err: impl FnOnce() -> Error,
) -> Result<RegionWithLeader> {
    let region = resp.region.take().ok_or_else(err)?;
    if region.region_epoch.is_none() {
        return Err(internal_err!(
            "missing region_epoch in PD GetRegionResponse for region_id {}",
            region.id
        ));
    }
    Ok(RegionWithLeader::new(region, resp.leader.take()))
}

fn region_and_buckets_from_response(
    mut resp: pdpb::GetRegionResponse,
    err: impl FnOnce() -> Error,
) -> Result<(RegionWithLeader, Option<metapb::Buckets>)> {
    let buckets = resp.buckets.take();
    let region = resp.region.take().ok_or_else(err)?;
    if region.region_epoch.is_none() {
        return Err(internal_err!(
            "missing region_epoch in PD GetRegionResponse for region_id {}",
            region.id
        ));
    }
    Ok((RegionWithLeader::new(region, resp.leader.take()), buckets))
}

fn scan_regions_from_response(resp: pdpb::ScanRegionsResponse) -> Result<Vec<RegionWithLeader>> {
    if !resp.regions.is_empty() {
        resp.regions
            .into_iter()
            .map(|region_info| {
                let region = region_info.region.ok_or_else(|| {
                    internal_err!("missing region in PD ScanRegionsResponse.regions entry")
                })?;
                if region.region_epoch.is_none() {
                    return Err(internal_err!(
                        "missing region_epoch in PD ScanRegionsResponse for region_id {}",
                        region.id
                    ));
                }
                Ok(RegionWithLeader::new(region, region_info.leader))
            })
            .collect()
    } else {
        if !resp.leaders.is_empty() && resp.region_metas.len() != resp.leaders.len() {
            return Err(internal_err!(
                "PD ScanRegionsResponse region_metas/leaders length mismatch: {} vs {}",
                resp.region_metas.len(),
                resp.leaders.len()
            ));
        }
        resp.region_metas
            .into_iter()
            .enumerate()
            .map(|(idx, region)| {
                if region.region_epoch.is_none() {
                    return Err(internal_err!(
                        "missing region_epoch in PD ScanRegionsResponse for region_id {}",
                        region.id
                    ));
                }
                let leader = resp.leaders.get(idx).cloned();
                Ok(RegionWithLeader::new(region, leader))
            })
            .collect()
    }
}

fn batch_scan_regions_from_response(
    resp: pdpb::BatchScanRegionsResponse,
) -> Result<Vec<(RegionWithLeader, Option<metapb::Buckets>)>> {
    resp.regions
        .into_iter()
        .map(|region_info| {
            let region = region_info.region.ok_or_else(|| {
                internal_err!("missing region in PD BatchScanRegionsResponse.regions entry")
            })?;
            if region.region_epoch.is_none() {
                return Err(internal_err!(
                    "missing region_epoch in PD BatchScanRegionsResponse for region_id {}",
                    region.id
                ));
            }
            Ok((
                RegionWithLeader::new(region, region_info.leader),
                region_info.buckets,
            ))
        })
        .collect()
}

fn store_from_response(resp: pdpb::GetStoreResponse, store_id: StoreId) -> Result<metapb::Store> {
    resp.store.ok_or_else(|| {
        internal_err!(
            "missing store in PD GetStoreResponse for store_id {}",
            store_id
        )
    })
}

// A node-like thing that can be connected to.
#[async_trait]
trait Reconnect {
    type Cl;
    async fn reconnect(&self, interval_sec: u64) -> Result<()>;
}

#[async_trait]
impl Reconnect for RetryClient<Cluster> {
    type Cl = Cluster;

    async fn reconnect(&self, interval_sec: u64) -> Result<()> {
        let reconnect_begin = Instant::now();
        let mut lock = self.cluster.write().await;
        let (cluster, last_connected) = &mut *lock;
        // If `last_connected + interval_sec` is larger or equal than reconnect_begin,
        // a concurrent reconnect is just succeed when this thread trying to get write lock
        let should_connect = reconnect_begin > *last_connected + Duration::from_secs(interval_sec);
        if should_connect {
            self.connection
                .reconnect(cluster, self.timeout, self.tso_max_pending_count)
                .await?;
            *last_connected = Instant::now();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;

    use futures::executor;
    use futures::future::ready;

    use super::*;
    use crate::internal_err;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_reconnect() {
        struct MockClient {
            reconnect_count: AtomicUsize,
            cluster: RwLock<((), Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = ();

            async fn reconnect(&self, _: u64) -> Result<()> {
                self.reconnect_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Not actually unimplemented, we just don't care about the error.
                Err(Error::Unimplemented)
            }
        }

        async fn retry_err(client: Arc<MockClient>) -> Result<()> {
            retry_mut!(client, "test", |_c| ready(Err(internal_err!("whoops"))))
        }

        async fn retry_ok(client: Arc<MockClient>) -> Result<()> {
            retry!(client, "test", |_c| ready(Ok::<_, Error>(())))
        }

        executor::block_on(async {
            let client = Arc::new(MockClient {
                reconnect_count: AtomicUsize::new(0),
                cluster: RwLock::new(((), Instant::now())),
            });

            assert!(retry_err(client.clone()).await.is_err());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                MAX_REQUEST_COUNT
            );

            client
                .reconnect_count
                .store(0, std::sync::atomic::Ordering::SeqCst);
            assert!(retry_ok(client.clone()).await.is_ok());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            );
        })
    }

    #[test]
    fn test_retry_client_cluster_id_returns_configured_value() {
        let security_mgr = Arc::new(SecurityManager::default());
        let client = RetryClient::new_with_cluster(security_mgr, Duration::from_secs(1), 42, ());
        assert_eq!(client.cluster_id(), 42);
    }

    #[test]
    fn test_store_from_response_missing_store_returns_error() {
        let resp = pdpb::GetStoreResponse::default();
        let err = store_from_response(resp, 7).unwrap_err().to_string();
        assert!(err.contains("missing store"));
    }

    #[test]
    fn test_store_from_response_returns_store_when_present() {
        let mut resp = pdpb::GetStoreResponse::default();
        resp.store = Some(metapb::Store::default());

        store_from_response(resp, 7).unwrap();
    }

    #[test]
    fn test_region_from_response_missing_epoch_returns_error() {
        let mut resp = pdpb::GetRegionResponse::default();
        let mut region = metapb::Region::default();
        region.id = 42;
        resp.region = Some(region);

        let err = region_from_response(resp, || Error::Unimplemented)
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing region_epoch"));
    }

    #[test]
    fn test_region_from_response_returns_region_when_epoch_present() {
        let mut resp = pdpb::GetRegionResponse::default();
        let mut region = metapb::Region::default();
        region.id = 42;
        region.region_epoch = Some(metapb::RegionEpoch::default());
        resp.region = Some(region);

        region_from_response(resp, || Error::Unimplemented).unwrap();
    }

    #[test]
    fn test_scan_regions_from_response_uses_regions_field_when_present() {
        let mut region = metapb::Region::default();
        region.id = 1;
        region.start_key = vec![1];
        region.end_key = vec![2];
        region.region_epoch = Some(metapb::RegionEpoch::default());

        let leader = metapb::Peer {
            store_id: 9,
            ..Default::default()
        };

        let resp = pdpb::ScanRegionsResponse {
            regions: vec![pdpb::Region {
                region: Some(region.clone()),
                leader: Some(leader.clone()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let regions = scan_regions_from_response(resp).unwrap();
        assert_eq!(regions, vec![RegionWithLeader::new(region, Some(leader))]);
    }

    #[test]
    fn test_scan_regions_from_response_uses_region_metas_when_regions_empty() {
        let mut region = metapb::Region::default();
        region.id = 1;
        region.start_key = vec![1];
        region.end_key = vec![2];
        region.region_epoch = Some(metapb::RegionEpoch::default());

        let leader = metapb::Peer {
            store_id: 9,
            ..Default::default()
        };

        let resp = pdpb::ScanRegionsResponse {
            region_metas: vec![region.clone()],
            leaders: vec![leader.clone()],
            ..Default::default()
        };

        let regions = scan_regions_from_response(resp).unwrap();
        assert_eq!(regions, vec![RegionWithLeader::new(region, Some(leader))]);
    }

    #[test]
    fn test_scan_regions_from_response_missing_epoch_returns_error() {
        let mut region = metapb::Region::default();
        region.id = 1;
        region.start_key = vec![1];
        region.end_key = vec![2];

        let resp = pdpb::ScanRegionsResponse {
            region_metas: vec![region],
            ..Default::default()
        };

        let err = scan_regions_from_response(resp).unwrap_err().to_string();
        assert!(err.contains("missing region_epoch"));
    }

    #[test]
    fn test_scan_regions_from_response_length_mismatch_returns_error() {
        let mut region = metapb::Region::default();
        region.id = 1;
        region.start_key = vec![1];
        region.end_key = vec![2];
        region.region_epoch = Some(metapb::RegionEpoch::default());

        let leader = metapb::Peer {
            store_id: 9,
            ..Default::default()
        };

        let resp = pdpb::ScanRegionsResponse {
            region_metas: vec![region],
            leaders: vec![leader.clone(), leader],
            ..Default::default()
        };

        let err = scan_regions_from_response(resp).unwrap_err().to_string();
        assert!(err.contains("length mismatch"));
    }

    #[test]
    fn test_batch_scan_regions_from_response_returns_regions_and_buckets() {
        let mut region = metapb::Region::default();
        region.id = 7;
        region.start_key = vec![1];
        region.end_key = vec![2];
        region.region_epoch = Some(metapb::RegionEpoch::default());

        let leader = metapb::Peer {
            store_id: 11,
            ..Default::default()
        };

        let buckets = metapb::Buckets {
            region_id: 7,
            version: 9,
            keys: vec![vec![1], vec![2]],
            ..Default::default()
        };

        let resp = pdpb::BatchScanRegionsResponse {
            regions: vec![pdpb::Region {
                region: Some(region.clone()),
                leader: Some(leader.clone()),
                buckets: Some(buckets.clone()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let regions = batch_scan_regions_from_response(resp).unwrap();
        assert_eq!(
            regions,
            vec![(RegionWithLeader::new(region, Some(leader)), Some(buckets))]
        );
    }

    #[test]
    fn test_batch_scan_regions_from_response_missing_epoch_returns_error() {
        let mut region = metapb::Region::default();
        region.id = 7;
        region.start_key = vec![1];
        region.end_key = vec![2];

        let resp = pdpb::BatchScanRegionsResponse {
            regions: vec![pdpb::Region {
                region: Some(region),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = batch_scan_regions_from_response(resp)
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing region_epoch"));
    }

    #[test]
    fn test_batch_scan_regions_from_response_missing_region_returns_error() {
        let resp = pdpb::BatchScanRegionsResponse {
            regions: vec![pdpb::Region::default()],
            ..Default::default()
        };

        let err = batch_scan_regions_from_response(resp)
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing region"));
    }

    #[test]
    fn test_retry() {
        struct MockClient {
            cluster: RwLock<(AtomicUsize, Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = Mutex<usize>;

            async fn reconnect(&self, _: u64) -> Result<()> {
                Ok(())
            }
        }

        async fn retry_max_err(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry_mut!(client, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        async fn retry_max_ok(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry!(client, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        executor::block_on(async {
            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(1000));

            assert!(retry_max_err(client.clone(), max_retries).await.is_err());
            assert_eq!(
                client.cluster.read().await.0.load(Ordering::SeqCst),
                LEADER_CHANGE_RETRY
            );

            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(2));

            assert!(retry_max_ok(client.clone(), max_retries).await.is_ok());
            assert_eq!(client.cluster.read().await.0.load(Ordering::SeqCst), 2);
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_retry_mut_first_then_retry_switches_to_retry_call_after_initial_failure() {
        struct MockCluster {
            follower_calls: AtomicUsize,
            leader_calls: AtomicUsize,
        }

        #[async_trait]
        impl Reconnect for RetryClient<MockCluster> {
            type Cl = MockCluster;

            async fn reconnect(&self, _: u64) -> Result<()> {
                Ok(())
            }
        }

        let security_mgr = Arc::new(SecurityManager::default());
        let client = RetryClient::new_with_cluster(
            security_mgr,
            Duration::from_secs(1),
            42,
            MockCluster {
                follower_calls: AtomicUsize::new(0),
                leader_calls: AtomicUsize::new(0),
            },
        );

        let result = client
            .retry_mut_first_then_retry(
                "test_retry_policy",
                |cluster| {
                    Box::pin(async move {
                        cluster.follower_calls.fetch_add(1, Ordering::SeqCst);
                        Err(internal_err!("stale follower response"))
                    })
                },
                |cluster| {
                    Box::pin(async move {
                        cluster.leader_calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Error>(())
                    })
                },
            )
            .await;

        assert!(result.is_ok());
        let cluster = &client.cluster.read().await.0;
        assert_eq!(cluster.follower_calls.load(Ordering::SeqCst), 1);
        assert_eq!(cluster.leader_calls.load(Ordering::SeqCst), 1);
    }
}
