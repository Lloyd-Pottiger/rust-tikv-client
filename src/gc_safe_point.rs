use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio::sync::RwLock;

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

struct GcSafePointCacheInner<PdC: PdClient> {
    pd: Arc<PdC>,
    keyspace: Keyspace,
    state: RwLock<GcSafePointState>,
    refresh_lock: Mutex<()>,
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
        Self {
            inner: Arc::new(GcSafePointCacheInner {
                pd,
                keyspace,
                state: RwLock::new(GcSafePointState::default()),
                refresh_lock: Mutex::new(()),
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
        if let Some(cached) = self.cached_safe_point().await {
            return Ok(cached);
        }

        let _guard = self.inner.refresh_lock.lock().await;
        if let Some(cached) = self.cached_safe_point().await {
            return Ok(cached);
        }

        let safe_point = self.fetch_safe_point().await?;
        let mut state = self.inner.state.write().await;
        state.safe_point = safe_point;
        state.last_updated = Some(Instant::now());
        Ok(safe_point)
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
