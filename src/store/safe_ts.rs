// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Store-level safe timestamp helpers (ported from client-go `tikv/kv.go`).

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::IntoRequest;

use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::store::Request;
use crate::CommandPriority;
use crate::DiskFullOpt;
use crate::Error;
use crate::Result;

const DC_LABEL_KEY: &str = "zone";
const ENGINE_LABEL_KEY: &str = "engine";
const ENGINE_LABEL_TIFLASH: &str = "tiflash";
const ENGINE_LABEL_TIFLASH_COMPUTE: &str = "tiflash_compute";

fn is_valid_safe_ts(ts: u64) -> bool {
    ts != 0 && ts != u64::MAX
}

fn is_tiflash_store(store: &metapb::Store) -> bool {
    store.labels.iter().any(|label| {
        label.key == ENGINE_LABEL_KEY
            && (label.value == ENGINE_LABEL_TIFLASH || label.value == ENGINE_LABEL_TIFLASH_COMPUTE)
    })
}

fn store_label_value<'a>(store: &'a metapb::Store, key: &str) -> Option<&'a str> {
    store
        .labels
        .iter()
        .find(|label| label.key == key)
        .map(|label| label.value.as_str())
}

/// Client-go passes `kvrpcpb::Context` along with `StoreSafeTSRequest` even though the protobuf
/// message itself does not contain a `context` field.
///
/// We keep a local `Context` to allow request builders/interceptors to reuse the same APIs, but it
/// is not currently transmitted to the server.
#[derive(Clone, Debug)]
pub(crate) struct StoreSafeTsRequest {
    inner: kvrpcpb::StoreSafeTsRequest,
    context: kvrpcpb::Context,
}

impl StoreSafeTsRequest {
    pub(crate) fn new_all_ranges() -> StoreSafeTsRequest {
        let mut inner = kvrpcpb::StoreSafeTsRequest::default();
        inner.key_range = Some(kvrpcpb::KeyRange::default());
        StoreSafeTsRequest {
            inner,
            context: kvrpcpb::Context::default(),
        }
    }
}

#[async_trait]
impl Request for StoreSafeTsRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any + Send>> {
        let mut req = self.inner.clone().into_request();
        req.set_timeout(timeout);
        let resp = client
            .clone()
            .get_store_safe_ts(req)
            .await
            .map_err(Error::GrpcAPI)?;
        Ok(Box::new(resp.into_inner()))
    }

    fn label(&self) -> &'static str {
        "store_safe_ts"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn context_mut(&mut self) -> &mut kvrpcpb::Context {
        &mut self.context
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.context_mut().api_version = api_version.into();
    }

    fn set_request_source(&mut self, source: &str) {
        self.context_mut().request_source = source.to_owned();
    }

    fn set_resource_group_tag(&mut self, tag: &[u8]) {
        self.context_mut().resource_group_tag = tag.to_vec();
    }

    fn set_resource_group_name(&mut self, name: &str) {
        self.context_mut()
            .resource_control_context
            .get_or_insert_with(Default::default)
            .resource_group_name = name.to_owned();
    }

    fn set_priority(&mut self, priority: CommandPriority) {
        self.context_mut().priority = priority as i32;
    }

    fn set_disk_full_opt(&mut self, disk_full_opt: DiskFullOpt) {
        self.context_mut().disk_full_opt = i32::from(disk_full_opt);
    }

    fn set_txn_source(&mut self, txn_source: u64) {
        self.context_mut().txn_source = txn_source;
    }
}

#[async_trait]
pub(crate) trait StoreSafeTsProvider: Send + Sync {
    async fn get_store_safe_ts(&self, store: &metapb::Store) -> Result<u64>;
}

#[async_trait]
pub(crate) trait MinResolvedTsProvider: Send + Sync {
    async fn get_min_resolved_ts_by_store_ids(
        &self,
        store_ids: Option<&[StoreId]>,
    ) -> Result<(u64, Option<HashMap<StoreId, u64>>)>;
}

/// Best-effort cache of store safe ts and per-txn-scope min safe ts.
#[derive(Clone, Default, Debug)]
pub(crate) struct SafeTsManager {
    safe_ts_by_store: Arc<RwLock<HashMap<StoreId, u64>>>,
    min_safe_ts_by_scope: Arc<RwLock<HashMap<String, u64>>>,
}

impl SafeTsManager {
    pub(crate) fn get_min_safe_ts(&self, txn_scope: &str) -> u64 {
        let guard = match self.min_safe_ts_by_scope.read() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.get(txn_scope).copied().unwrap_or(0)
    }

    pub(crate) fn get_store_safe_ts(&self, store_id: StoreId) -> Option<u64> {
        let guard = match self.safe_ts_by_store.read() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.get(&store_id).copied()
    }

    fn set_store_safe_ts(&self, store_id: StoreId, safe_ts: u64) {
        if safe_ts == u64::MAX {
            return;
        }
        let mut guard = match self.safe_ts_by_store.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.insert(store_id, safe_ts);
    }

    fn set_min_safe_ts(&self, txn_scope: &str, safe_ts: u64) {
        if safe_ts == u64::MAX {
            return;
        }
        let mut guard = match self.min_safe_ts_by_scope.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.insert(txn_scope.to_owned(), safe_ts);
    }

    fn update_min_safe_ts(&self, txn_scope: &str, store_ids: &[StoreId]) {
        let mut min_safe_ts = u64::MAX;
        if store_ids.is_empty() {
            self.set_min_safe_ts(txn_scope, 0);
            return;
        }

        let guard = match self.safe_ts_by_store.read() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };

        for store_id in store_ids {
            let Some(&safe_ts) = guard.get(store_id) else {
                min_safe_ts = 0;
                continue;
            };
            if safe_ts != 0 && safe_ts < min_safe_ts {
                min_safe_ts = safe_ts;
            }
        }

        if min_safe_ts == u64::MAX {
            min_safe_ts = 0;
        }
        self.set_min_safe_ts(txn_scope, min_safe_ts);
    }

    async fn try_update_global_from_pd(&self, pd: Option<&dyn MinResolvedTsProvider>) -> bool {
        let Some(pd) = pd else {
            return false;
        };

        if crate::txn_scope_from_config() != crate::GLOBAL_TXN_SCOPE {
            return false;
        }

        let Ok((cluster_min_safe_ts, _)) = pd.get_min_resolved_ts_by_store_ids(None).await else {
            return false;
        };
        if !is_valid_safe_ts(cluster_min_safe_ts) {
            return false;
        }

        let prev = self.get_min_safe_ts(crate::GLOBAL_TXN_SCOPE);
        if prev > cluster_min_safe_ts {
            // Keep monotonicity.
            return true;
        }
        self.set_min_safe_ts(crate::GLOBAL_TXN_SCOPE, cluster_min_safe_ts);
        true
    }

    /// Refresh safe ts cache once (ported from client-go `KVStore.updateSafeTS`).
    pub(crate) async fn update_safe_ts_once(
        &self,
        stores: &[metapb::Store],
        pd: Option<&dyn MinResolvedTsProvider>,
        store_provider: &dyn StoreSafeTsProvider,
    ) -> Result<()> {
        if self.try_update_global_from_pd(pd).await {
            return Ok(());
        }

        let store_ids: Vec<StoreId> = stores.iter().map(|s| s.id).collect();
        let mut pd_err = false;
        let mut store_min_resolved_ts: Option<HashMap<StoreId, u64>> = None;
        if let Some(pd) = pd {
            match pd.get_min_resolved_ts_by_store_ids(Some(&store_ids)).await {
                Ok((_min, map)) => store_min_resolved_ts = map,
                Err(_) => pd_err = true,
            }
        }

        for store in stores {
            let from_pd = !pd_err
                && store_min_resolved_ts
                    .as_ref()
                    .and_then(|m| m.get(&store.id).copied())
                    .is_some_and(is_valid_safe_ts);

            let safe_ts = if from_pd {
                store_min_resolved_ts
                    .as_ref()
                    .and_then(|m| m.get(&store.id).copied())
                    .unwrap_or(0)
            } else {
                match store_provider.get_store_safe_ts(store).await {
                    Ok(ts) => ts,
                    Err(_) => continue,
                }
            };

            let prev = self.get_store_safe_ts(store.id).unwrap_or(0);
            if prev > safe_ts {
                continue;
            }
            self.set_store_safe_ts(store.id, safe_ts);
        }

        let mut scope_map: HashMap<String, Vec<StoreId>> = HashMap::new();
        for store in stores {
            scope_map
                .entry(crate::GLOBAL_TXN_SCOPE.to_owned())
                .or_default()
                .push(store.id);
            if let Some(zone) = store_label_value(store, DC_LABEL_KEY) {
                if !zone.is_empty() {
                    scope_map.entry(zone.to_owned()).or_default().push(store.id);
                }
            }
        }
        for (scope, store_ids) in scope_map {
            self.update_min_safe_ts(&scope, &store_ids);
        }

        Ok(())
    }

    /// Choose the address used to issue `StoreSafeTS` for a store.
    pub(crate) fn store_safe_ts_address(store: &metapb::Store) -> &str {
        if is_tiflash_store(store) {
            if store.peer_address.is_empty() {
                return store.address.as_str();
            }
            return store.peer_address.as_str();
        }
        store.address.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn make_store(id: StoreId, zone: &str, tiflash: bool) -> metapb::Store {
        let mut store = metapb::Store::default();
        store.id = id;
        store.address = format!("store{id}");
        store.peer_address = store.address.clone();
        store.state = metapb::StoreState::Up as i32;
        store.labels.push(metapb::StoreLabel {
            key: DC_LABEL_KEY.to_owned(),
            value: zone.to_owned(),
        });
        if tiflash {
            store.labels.push(metapb::StoreLabel {
                key: ENGINE_LABEL_KEY.to_owned(),
                value: ENGINE_LABEL_TIFLASH.to_owned(),
            });
        }
        store
    }

    #[derive(Clone)]
    struct MockStoreSafeTs {
        values: Arc<HashMap<StoreId, u64>>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StoreSafeTsProvider for MockStoreSafeTs {
        async fn get_store_safe_ts(&self, store: &metapb::Store) -> Result<u64> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(*self.values.get(&store.id).unwrap_or(&0))
        }
    }

    #[derive(Clone)]
    struct MockMinResolvedTs {
        cluster: (u64, Option<HashMap<StoreId, u64>>, bool),
        stores: (u64, Option<HashMap<StoreId, u64>>, bool),
    }

    #[async_trait]
    impl MinResolvedTsProvider for MockMinResolvedTs {
        async fn get_min_resolved_ts_by_store_ids(
            &self,
            store_ids: Option<&[StoreId]>,
        ) -> Result<(u64, Option<HashMap<StoreId, u64>>)> {
            let (min, map, fail) = if store_ids.is_some() {
                &self.stores
            } else {
                &self.cluster
            };
            if *fail {
                return Err(Error::StringError("mock pd error".to_owned()));
            }
            Ok((*min, map.clone()))
        }
    }

    #[tokio::test]
    async fn min_safe_ts_from_stores_matches_client_go_test() {
        let stores = vec![make_store(1, "z1", false), make_store(2, "z2", true)];
        let safe = MockStoreSafeTs {
            values: Arc::new(HashMap::from([(1, 100), (2, 80)])),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let pd = MockMinResolvedTs {
            cluster: (0, None, false),
            stores: (0, None, false),
        };

        let mgr = SafeTsManager::default();
        mgr.update_safe_ts_once(&stores, Some(&pd), &safe)
            .await
            .unwrap();

        assert_eq!(safe.calls.load(Ordering::SeqCst), 2);
        assert_eq!(mgr.get_min_safe_ts(crate::GLOBAL_TXN_SCOPE), 80);
        assert_eq!(mgr.get_min_safe_ts("z1"), 100);
        assert_eq!(mgr.get_min_safe_ts("z2"), 80);
    }

    #[tokio::test]
    async fn min_safe_ts_all_zeros_matches_client_go_test() {
        let stores = vec![make_store(1, "z1", false), make_store(2, "z2", true)];
        let safe = MockStoreSafeTs {
            values: Arc::new(HashMap::from([(1, 0), (2, 0)])),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let pd = MockMinResolvedTs {
            cluster: (0, None, false),
            stores: (0, None, false),
        };

        let mgr = SafeTsManager::default();
        mgr.update_safe_ts_once(&stores, Some(&pd), &safe)
            .await
            .unwrap();

        assert_eq!(mgr.get_min_safe_ts(crate::GLOBAL_TXN_SCOPE), 0);
    }

    #[tokio::test]
    async fn min_safe_ts_some_zeros_ignores_zero_store_matches_client_go_test() {
        let stores = vec![make_store(1, "z1", false), make_store(2, "z2", true)];
        let safe = MockStoreSafeTs {
            values: Arc::new(HashMap::from([(1, 100), (2, 0)])),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let pd = MockMinResolvedTs {
            cluster: (0, None, false),
            stores: (0, None, false),
        };

        let mgr = SafeTsManager::default();
        mgr.update_safe_ts_once(&stores, Some(&pd), &safe)
            .await
            .unwrap();

        assert_eq!(mgr.get_min_safe_ts(crate::GLOBAL_TXN_SCOPE), 100);
    }

    #[tokio::test]
    async fn min_safe_ts_prefers_pd_cluster_value_for_global() {
        let stores = vec![make_store(1, "z1", false), make_store(2, "z2", true)];
        let safe = MockStoreSafeTs {
            values: Arc::new(HashMap::from([(1, 100), (2, 80)])),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let pd = MockMinResolvedTs {
            cluster: (90, None, false),
            stores: (0, None, false),
        };

        let mgr = SafeTsManager::default();
        mgr.update_safe_ts_once(&stores, Some(&pd), &safe)
            .await
            .unwrap();

        assert_eq!(safe.calls.load(Ordering::SeqCst), 0);
        assert_eq!(mgr.get_min_safe_ts(crate::GLOBAL_TXN_SCOPE), 90);
    }

    #[tokio::test]
    async fn min_safe_ts_prefers_pd_per_store_values_when_valid() {
        let stores = vec![make_store(1, "z1", false), make_store(2, "z2", true)];
        let safe = MockStoreSafeTs {
            values: Arc::new(HashMap::from([(1, 100), (2, 80)])),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let pd = MockMinResolvedTs {
            cluster: (u64::MAX, None, false),
            stores: (u64::MAX, Some(HashMap::from([(1, 101), (2, 102)])), false),
        };

        let mgr = SafeTsManager::default();
        mgr.update_safe_ts_once(&stores, Some(&pd), &safe)
            .await
            .unwrap();

        assert_eq!(safe.calls.load(Ordering::SeqCst), 0);
        assert_eq!(mgr.get_min_safe_ts(crate::GLOBAL_TXN_SCOPE), 101);
    }

    #[tokio::test]
    async fn min_safe_ts_mixed_pd_and_store_fallbacks_match_client_go_tests() {
        let stores = vec![make_store(1, "z1", false), make_store(2, "z2", true)];

        // Mixed1: store2 is 0 in PD -> fall back to store safe ts for z2; global/z1 use PD 10.
        let safe = MockStoreSafeTs {
            values: Arc::new(HashMap::from([(1, 100), (2, 80)])),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let pd = MockMinResolvedTs {
            cluster: (u64::MAX, None, false),
            stores: (u64::MAX, Some(HashMap::from([(1, 10), (2, 0)])), false),
        };
        let mgr = SafeTsManager::default();
        mgr.update_safe_ts_once(&stores, Some(&pd), &safe)
            .await
            .unwrap();
        assert_eq!(mgr.get_min_safe_ts(crate::GLOBAL_TXN_SCOPE), 10);
        assert_eq!(mgr.get_min_safe_ts("z1"), 10);
        assert_eq!(mgr.get_min_safe_ts("z2"), 80);
        assert_eq!(safe.calls.load(Ordering::SeqCst), 1);

        // Mixed2: store1 is max in PD -> fall back to store safe ts for z1; global/z2 use PD 10.
        let safe = MockStoreSafeTs {
            values: Arc::new(HashMap::from([(1, 100), (2, 80)])),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let pd = MockMinResolvedTs {
            cluster: (u64::MAX, None, false),
            stores: (
                u64::MAX,
                Some(HashMap::from([(1, u64::MAX), (2, 10)])),
                false,
            ),
        };
        let mgr = SafeTsManager::default();
        mgr.update_safe_ts_once(&stores, Some(&pd), &safe)
            .await
            .unwrap();
        assert_eq!(mgr.get_min_safe_ts(crate::GLOBAL_TXN_SCOPE), 10);
        assert_eq!(mgr.get_min_safe_ts("z2"), 10);
        assert_eq!(mgr.get_min_safe_ts("z1"), 100);
        assert_eq!(safe.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn store_safe_ts_address_uses_peer_addr_for_tiflash() {
        let mut store = make_store(1, "z1", true);
        store.peer_address = "peer".to_owned();
        assert_eq!(SafeTsManager::store_safe_ts_address(&store), "peer");

        store.peer_address.clear();
        assert_eq!(SafeTsManager::store_safe_ts_address(&store), "store1");
    }
}
