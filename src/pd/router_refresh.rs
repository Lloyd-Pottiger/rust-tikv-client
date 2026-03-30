use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::debug;

use crate::pd::Cluster;
use crate::pd::PdRpcClient;
use crate::store::KvConnect;
use crate::PdClient;

/// A small internal hook for refreshing router-service discovery state.
///
/// This exists to keep the background loop testable without requiring a real PD cluster.
#[async_trait]
pub(crate) trait RouterServiceMembershipRefresher: Send + Sync {
    fn is_closed(&self) -> bool;
    async fn refresh_router_service_membership(&self) -> crate::Result<()>;
}

/// client-go uses `MemberUpdateInterval = time.Minute` for router-service membership refresh.
pub(crate) const ROUTER_SERVICE_MEMBER_UPDATE_INTERVAL: Duration = Duration::from_secs(60);

#[async_trait]
impl<KvC> RouterServiceMembershipRefresher for PdRpcClient<KvC, Cluster>
where
    KvC: KvConnect + Send + Sync + 'static,
{
    fn is_closed(&self) -> bool {
        PdClient::is_closed(self)
    }

    async fn refresh_router_service_membership(&self) -> crate::Result<()> {
        self.refresh_router_service_membership_once().await
    }
}

pub(crate) fn spawn_router_service_membership_refresher<R>(
    refresher: Arc<R>,
    update_interval: Duration,
) where
    R: RouterServiceMembershipRefresher + 'static,
{
    if update_interval.is_zero() {
        return;
    }

    let weak = Arc::downgrade(&refresher);
    tokio::spawn(async move {
        // Align with other background updaters in this crate: start ticking after the initial
        // interval so callers aren't surprised by extra work during `new_with_config`.
        let start = tokio::time::Instant::now() + update_interval;
        let mut ticker = tokio::time::interval_at(start, update_interval);
        loop {
            ticker.tick().await;

            let Some(refresher) = weak.upgrade() else {
                return;
            };
            if refresher.is_closed() {
                return;
            }

            if let Err(err) = refresher.refresh_router_service_membership().await {
                debug!("router service membership refresh failed: {:?}", err);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    struct TestRefresher {
        closed: AtomicBool,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl RouterServiceMembershipRefresher for TestRefresher {
        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }

        async fn refresh_router_service_membership(&self) -> crate::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn router_refresh_invokes_refresh_callback() {
        let _ = crate::pd::ROUTER_SERVICE_MEMBER_UPDATE_INTERVAL;

        let calls = Arc::new(AtomicUsize::new(0));
        let refresher = Arc::new(TestRefresher {
            closed: AtomicBool::new(false),
            calls: calls.clone(),
        });

        crate::pd::spawn_router_service_membership_refresher(
            refresher.clone(),
            Duration::from_millis(5),
        );

        tokio::time::timeout(Duration::from_millis(100), async move {
            let _keep_alive = refresher;
            loop {
                if calls.load(Ordering::SeqCst) > 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("expected refresher to be invoked");
    }

    #[tokio::test]
    async fn router_refresh_stops_immediately_when_closed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let refresher = Arc::new(TestRefresher {
            closed: AtomicBool::new(true),
            calls: calls.clone(),
        });

        crate::pd::spawn_router_service_membership_refresher(
            refresher.clone(),
            Duration::from_millis(5),
        );

        // Keep the refresher alive for at least a few ticks.
        let _keep_alive = refresher;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn router_refresh_stops_after_drop() {
        let calls = Arc::new(AtomicUsize::new(0));
        let refresher = Arc::new(TestRefresher {
            closed: AtomicBool::new(false),
            calls: calls.clone(),
        });

        crate::pd::spawn_router_service_membership_refresher(
            refresher.clone(),
            Duration::from_millis(5),
        );

        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if calls.load(Ordering::SeqCst) > 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("expected initial refresh to run");

        // Give the in-flight tick a chance to finish before dropping the last strong ref.
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(refresher);

        let after_drop = calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(calls.load(Ordering::SeqCst), after_drop);
    }
}
