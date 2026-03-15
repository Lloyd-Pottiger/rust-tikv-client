use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{watch, Mutex, RwLock};

use crate::pd::PdClient;
use crate::request::Keyspace;
use crate::Error;
use crate::Result;

const GC_SAFE_POINT_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Default)]
struct GcSafePointState {
    safe_point: u64,
    last_updated: Option<Instant>,
}

#[derive(Debug, Default)]
struct GcSafePointRefreshState {
    in_flight: bool,
    generation: u64,
    last_error: Option<String>,
}

struct GcSafePointCacheInner<PdC: PdClient> {
    pd: Arc<PdC>,
    keyspace: Keyspace,
    state: RwLock<GcSafePointState>,
    refresh: Mutex<GcSafePointRefreshState>,
    refresh_generation: watch::Sender<u64>,
    gc_safe_point_v2_unimplemented: AtomicBool,
}

pub(crate) struct GcSafePointCache<PdC: PdClient> {
    inner: Arc<GcSafePointCacheInner<PdC>>,
}

impl<PdC: PdClient> Clone for GcSafePointCache<PdC> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<PdC: PdClient> GcSafePointCache<PdC> {
    pub(crate) fn new(pd: Arc<PdC>, keyspace: Keyspace) -> Self {
        let (refresh_generation, _rx) = watch::channel(0);
        Self {
            inner: Arc::new(GcSafePointCacheInner {
                pd,
                keyspace,
                state: RwLock::new(GcSafePointState::default()),
                refresh: Mutex::new(GcSafePointRefreshState::default()),
                refresh_generation,
                gc_safe_point_v2_unimplemented: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) async fn check_visibility(&self, start_ts: u64) -> Result<()> {
        let safe_point = self.safe_point().await?;
        if start_ts < safe_point {
            return Err(Error::TxnAbortedByGc {
                start_ts,
                safe_point,
            });
        }
        Ok(())
    }

    pub(crate) async fn observe_safe_point(&self, safe_point: u64) {
        let mut state = self.inner.state.write().await;
        if safe_point > state.safe_point {
            state.safe_point = safe_point;
        }
        state.last_updated = Some(Instant::now());
    }

    pub(crate) async fn safe_point(&self) -> Result<u64> {
        loop {
            if let Some(cached) = self.cached_safe_point().await {
                return Ok(cached);
            }

            let wait_for_generation = {
                let mut refresh = self.inner.refresh.lock().await;
                if refresh.in_flight {
                    Some(refresh.generation)
                } else {
                    refresh.in_flight = true;
                    None
                }
            };

            if let Some(generation) = wait_for_generation {
                let mut refreshed = self.inner.refresh_generation.subscribe();
                while *refreshed.borrow() == generation {
                    // Sender is owned by the cache; treat closure as a best-effort wake-up.
                    if refreshed.changed().await.is_err() {
                        break;
                    }
                }

                let refresh = self.inner.refresh.lock().await;
                if let Some(err) = refresh.last_error.as_ref() {
                    return Err(Error::StringError(err.clone()));
                }
                continue;
            }

            let fetched = self.fetch_safe_point().await;
            let last_error = fetched.as_ref().err().map(|err| err.to_string());
            let result = match fetched {
                Ok(safe_point) => {
                    let mut state = self.inner.state.write().await;
                    state.safe_point = safe_point;
                    state.last_updated = Some(Instant::now());
                    Ok(safe_point)
                }
                Err(err) => Err(err),
            };

            let generation = {
                let mut refresh = self.inner.refresh.lock().await;
                refresh.in_flight = false;
                refresh.generation = refresh.generation.wrapping_add(1);
                refresh.last_error = last_error;
                refresh.generation
            };
            let _ = self.inner.refresh_generation.send(generation);

            return result;
        }
    }

    async fn cached_safe_point(&self) -> Option<u64> {
        let state = self.inner.state.read().await;
        let last_updated = state.last_updated?;
        if last_updated.elapsed() < GC_SAFE_POINT_REFRESH_INTERVAL {
            Some(state.safe_point)
        } else {
            None
        }
    }

    async fn fetch_safe_point(&self) -> Result<u64> {
        let pd = self.inner.pd.clone();
        match self.inner.keyspace {
            Keyspace::Enable { keyspace_id } => {
                if self
                    .inner
                    .gc_safe_point_v2_unimplemented
                    .load(Ordering::Acquire)
                {
                    return pd.clone().get_gc_safe_point().await;
                }

                match pd.clone().get_gc_safe_point_v2(keyspace_id).await {
                    Ok(safe_point) => Ok(safe_point),
                    Err(Error::Unimplemented) => {
                        self.inner
                            .gc_safe_point_v2_unimplemented
                            .store(true, Ordering::Release);
                        pd.get_gc_safe_point().await
                    }
                    Err(err) => Err(err),
                }
            }
            _ => pd.get_gc_safe_point().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::GcSafePointCache;
    use crate::mock::MockKvClient;
    use crate::pd::PdClient;
    use crate::proto::keyspacepb;
    use crate::proto::metapb;
    use crate::region::{RegionId, RegionVerId, RegionWithLeader, StoreId};
    use crate::request::Keyspace;
    use crate::store::{RegionStore, Store};
    use crate::{Error, Key, Result, Timestamp};

    #[derive(Clone)]
    struct BlockingGcSafePointPdClient {
        called: Arc<Notify>,
        ready: Arc<Notify>,
        safe_point: u64,
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait]
    impl PdClient for BlockingGcSafePointPdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            _region: RegionWithLeader,
        ) -> Result<RegionStore> {
            Err(Error::Unimplemented)
        }

        async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn get_gc_safe_point(self: Arc<Self>) -> Result<u64> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.called.notify_one();
            self.ready.notified().await;
            if self.fail {
                Err(Error::StringError(
                    "injected gc safe point error".to_owned(),
                ))
            } else {
                Ok(self.safe_point)
            }
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }

        async fn all_stores(&self) -> Result<Vec<Store>> {
            Ok(Vec::new())
        }

        async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
            Ok(())
        }

        async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

        async fn invalidate_store_cache(&self, _store_id: StoreId) {}
    }

    #[tokio::test]
    async fn test_gc_safe_point_refresh_gate_is_not_held_across_await() {
        let called = Arc::new(Notify::new());
        let ready = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let pd_client = Arc::new(BlockingGcSafePointPdClient {
            called: called.clone(),
            ready: ready.clone(),
            safe_point: 42,
            calls,
            fail: false,
        });
        let cache = GcSafePointCache::new(pd_client, Keyspace::Disable);

        let cache_clone = cache.clone();
        let handle = tokio::spawn(async move { cache_clone.safe_point().await });

        tokio::time::timeout(Duration::from_secs(1), called.notified())
            .await
            .expect("pd get_gc_safe_point called");

        assert!(
            cache.inner.refresh.try_lock().is_ok(),
            "refresh gate mutex must not be held across await"
        );

        ready.notify_one();
        assert_eq!(handle.await.unwrap().unwrap(), 42);
    }

    #[tokio::test]
    async fn test_gc_safe_point_refresh_dedupes_concurrent_calls() {
        let called = Arc::new(Notify::new());
        let ready = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let pd_client = Arc::new(BlockingGcSafePointPdClient {
            called: called.clone(),
            ready: ready.clone(),
            safe_point: 42,
            calls: calls.clone(),
            fail: false,
        });
        let cache = GcSafePointCache::new(pd_client, Keyspace::Disable);

        let barrier = Arc::new(tokio::sync::Barrier::new(11));
        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                cache.safe_point().await
            }));
        }
        barrier.wait().await;

        tokio::time::timeout(Duration::from_secs(1), called.notified())
            .await
            .expect("pd get_gc_safe_point called");
        ready.notify_waiters();

        for handle in handles {
            let value = tokio::time::timeout(Duration::from_secs(1), handle)
                .await
                .expect("safe_point task should finish")
                .unwrap()
                .unwrap();
            assert_eq!(value, 42);
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concurrent safe_point calls should be singleflighted"
        );
        assert_eq!(cache.safe_point().await.unwrap(), 42);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "cached safe_point should not refresh"
        );
    }

    #[tokio::test]
    async fn test_gc_safe_point_refresh_propagates_error_to_waiters() {
        let called = Arc::new(Notify::new());
        let ready = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let pd_client = Arc::new(BlockingGcSafePointPdClient {
            called: called.clone(),
            ready: ready.clone(),
            safe_point: 42,
            calls: calls.clone(),
            fail: true,
        });
        let cache = GcSafePointCache::new(pd_client, Keyspace::Disable);

        let barrier = Arc::new(tokio::sync::Barrier::new(11));
        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                cache.safe_point().await
            }));
        }
        barrier.wait().await;

        tokio::time::timeout(Duration::from_secs(1), called.notified())
            .await
            .expect("pd get_gc_safe_point called");
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        ready.notify_waiters();

        for handle in handles {
            let err = tokio::time::timeout(Duration::from_secs(1), handle)
                .await
                .expect("safe_point task should finish")
                .unwrap()
                .unwrap_err();
            match err {
                Error::StringError(message) => {
                    assert_eq!(message, "injected gc safe point error");
                }
                other => panic!("expected StringError, got {other:?}"),
            }
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "refresh errors should be returned to all waiters"
        );
    }
}
