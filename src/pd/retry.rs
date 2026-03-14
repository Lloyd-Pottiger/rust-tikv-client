// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! A utility module for managing and retrying PD requests.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio::time::sleep;

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
pub trait RetryClientTrait {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader>;

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader>;

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store>;

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>>;

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    async fn get_timestamp_with_dc_location(
        self: Arc<Self>,
        dc_location: String,
    ) -> Result<Timestamp> {
        let _ = dc_location;
        self.get_timestamp().await
    }

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        Err(Error::Unimplemented)
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        let _ = timestamp;
        Err(Error::Unimplemented)
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<u64>,
        group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        let _ = (region_ids, group);
        Err(Error::Unimplemented)
    }

    async fn get_operator(self: Arc<Self>, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let _ = region_id;
        Err(Error::Unimplemented)
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64>;

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
}

#[cfg(test)]
impl<Cl> RetryClient<Cl> {
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
        for _ in 0..LEADER_CHANGE_RETRY {
            let res = $call;

            match stats.done(res) {
                Ok(r) => return Ok(r),
                Err(Error::Unimplemented) => return Err(Error::Unimplemented),
                Err(Error::GrpcAPI(status)) if status.code() == tonic::Code::Unimplemented => {
                    return Err(Error::Unimplemented);
                }
                Err(e) => last_err = Err(e),
            }

            let mut reconnect_count = MAX_REQUEST_COUNT;
            while let Err(e) = $self.reconnect(RECONNECT_INTERVAL_SEC).await {
                reconnect_count -= 1;
                if reconnect_count == 0 {
                    return Err(e);
                }
                sleep(Duration::from_secs(RECONNECT_INTERVAL_SEC)).await;
            }
        }

        last_err?;
        return Err(internal_err!(concat!(
            "pd retry exhausted without error: ",
            $tag
        )));
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
}

#[async_trait]
impl RetryClientTrait for RetryClient<Cluster> {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
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
        })
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_region_by_id", |cluster| async {
            cluster
                .get_region_by_id(region_id, self.timeout)
                .await
                .and_then(|resp| {
                    region_from_response(resp, || Error::RegionNotFoundInResponse { region_id })
                })
        })
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store> {
        retry_mut!(self, "get_store", |cluster| async {
            cluster
                .get_store(id, self.timeout)
                .await
                .and_then(|resp| store_from_response(resp, id))
        })
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        retry_mut!(self, "get_all_stores", |cluster| async {
            cluster
                .get_all_stores(self.timeout)
                .await
                .map(|resp| resp.stores.into_iter().map(Into::into).collect())
        })
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        retry!(self, "get_timestamp", |cluster| cluster.get_timestamp())
    }

    async fn get_timestamp_with_dc_location(
        self: Arc<Self>,
        dc_location: String,
    ) -> Result<Timestamp> {
        retry!(self, "get_timestamp_with_dc_location", |cluster| {
            cluster.get_timestamp_with_dc_location(dc_location.clone())
        })
    }

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        retry_mut!(self, "get_min_ts", |cluster| async {
            cluster.get_min_ts(self.timeout).await
        })
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        retry_mut!(self, "get_external_timestamp", |cluster| async {
            cluster.get_external_timestamp(self.timeout).await
        })
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        retry_mut!(self, "set_external_timestamp", |cluster| async {
            cluster
                .set_external_timestamp(timestamp, self.timeout)
                .await
        })
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<u64>,
        group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        retry_mut!(self, "scatter_regions", |cluster| {
            let region_ids = region_ids.clone();
            let group = group.clone();
            async {
                cluster
                    .scatter_regions(region_ids, group, self.timeout)
                    .await
            }
        })
    }

    async fn get_operator(self: Arc<Self>, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        retry_mut!(self, "get_operator", |cluster| async {
            cluster.get_operator(region_id, self.timeout).await
        })
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        retry_mut!(self, "update_gc_safepoint", |cluster| async {
            cluster
                .update_safepoint(safepoint, self.timeout)
                .await
                .map(|resp| resp.new_safe_point)
        })
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        retry_mut!(self, "load_keyspace", |cluster| async {
            cluster.load_keyspace(keyspace, self.timeout).await
        })
    }
}

impl fmt::Debug for RetryClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("pd::RetryClient")
            .field("timeout", &self.timeout)
            .finish()
    }
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
}
