use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use futures::StreamExt;
use log::debug;

use crate::pd::PdClient;
use crate::region::StoreId;
use crate::Error;
use crate::Result;

const HEALTH_FEEDBACK_REFRESH_MAX_CONCURRENCY: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HealthFeedbackRefreshOutcome {
    Completed,
    Unsupported,
}

pub(crate) fn spawn_health_feedback_updater<PdC: PdClient>(
    pd: Arc<PdC>,
    update_interval: Duration,
) {
    if update_interval.is_zero() {
        return;
    }

    let weak = Arc::downgrade(&pd);
    tokio::spawn(async move {
        let start = tokio::time::Instant::now() + update_interval;
        let mut ticker = tokio::time::interval_at(start, update_interval);
        loop {
            ticker.tick().await;
            let Some(pd) = weak.upgrade() else {
                return;
            };

            match refresh_health_feedback_once(pd.clone()).await {
                Ok(HealthFeedbackRefreshOutcome::Completed) => {}
                Ok(HealthFeedbackRefreshOutcome::Unsupported) => return,
                Err(err) => debug!("health feedback refresh failed: {:?}", err),
            }
        }
    });
}

async fn refresh_health_feedback_once<PdC: PdClient>(
    pd: Arc<PdC>,
) -> Result<HealthFeedbackRefreshOutcome> {
    let store_ids: Vec<StoreId> = pd
        .all_stores()
        .await?
        .into_iter()
        .map(|store| store.meta.id)
        .collect();

    if store_ids.is_empty() {
        return Ok(HealthFeedbackRefreshOutcome::Completed);
    }

    let saw_supported = Arc::new(AtomicBool::new(false));
    let saw_unimplemented = Arc::new(AtomicBool::new(false));
    let saw_supported_for_tasks = saw_supported.clone();
    let saw_unimplemented_for_tasks = saw_unimplemented.clone();

    stream::iter(store_ids)
        .for_each_concurrent(
            Some(HEALTH_FEEDBACK_REFRESH_MAX_CONCURRENCY),
            move |store_id| {
                let pd = pd.clone();
                let saw_supported = saw_supported_for_tasks.clone();
                let saw_unimplemented = saw_unimplemented_for_tasks.clone();
                async move {
                    match pd.get_health_feedback(store_id).await {
                        Ok(_feedback) => {
                            saw_supported.store(true, Ordering::Relaxed);
                        }
                        Err(err) if is_unimplemented_error(&err) => {
                            saw_unimplemented.store(true, Ordering::Relaxed);
                        }
                        Err(err) => {
                            saw_supported.store(true, Ordering::Relaxed);
                            debug!(
                                "health feedback request to store {} failed: {:?}",
                                store_id, err
                            );
                        }
                    }
                }
            },
        )
        .await;

    if !saw_supported.load(Ordering::Relaxed) && saw_unimplemented.load(Ordering::Relaxed) {
        Ok(HealthFeedbackRefreshOutcome::Unsupported)
    } else {
        Ok(HealthFeedbackRefreshOutcome::Completed)
    }
}

fn is_unimplemented_error(err: &Error) -> bool {
    matches!(err, Error::Unimplemented)
        || matches!(err, Error::GrpcAPI(status) if status.code() == tonic::Code::Unimplemented)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
    use tonic::Status;

    #[test]
    fn test_is_unimplemented_error_matches_unimplemented_variants() {
        assert!(is_unimplemented_error(&Error::Unimplemented));
        assert!(is_unimplemented_error(&Error::GrpcAPI(
            Status::unimplemented("unimplemented",)
        )));
        assert!(!is_unimplemented_error(&Error::GrpcAPI(
            Status::unavailable("unavailable",)
        )));
    }

    #[tokio::test]
    async fn test_refresh_health_feedback_once_queries_all_stores() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                req.downcast_ref::<kvrpcpb::GetHealthFeedbackRequest>()
                    .expect("GetHealthFeedbackRequest");
                calls_captured.fetch_add(1, Ordering::SeqCst);

                let mut resp = kvrpcpb::GetHealthFeedbackResponse::default();
                resp.health_feedback = Some(kvrpcpb::HealthFeedback {
                    store_id: 1,
                    slow_score: 1,
                    ..Default::default()
                });
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        client
            .insert_store_meta(metapb::Store {
                id: 1,
                ..Default::default()
            })
            .await;
        client
            .insert_store_meta(metapb::Store {
                id: 2,
                ..Default::default()
            })
            .await;

        let outcome = refresh_health_feedback_once(client.clone()).await.unwrap();
        assert_eq!(outcome, HealthFeedbackRefreshOutcome::Completed);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_refresh_health_feedback_once_stops_when_unimplemented_everywhere() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                req.downcast_ref::<kvrpcpb::GetHealthFeedbackRequest>()
                    .expect("GetHealthFeedbackRequest");
                Err(crate::Error::Unimplemented)
            },
        )));

        client
            .insert_store_meta(metapb::Store {
                id: 1,
                ..Default::default()
            })
            .await;
        client
            .insert_store_meta(metapb::Store {
                id: 2,
                ..Default::default()
            })
            .await;

        let outcome = refresh_health_feedback_once(client.clone()).await.unwrap();
        assert_eq!(outcome, HealthFeedbackRefreshOutcome::Unsupported);
    }
}
