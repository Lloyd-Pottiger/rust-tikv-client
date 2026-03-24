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
            if pd.is_closed() {
                return;
            }

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
                    crate::stats::inc_health_feedback_ops_counter(store_id, "tick");
                    crate::stats::inc_health_feedback_ops_counter(store_id, "active_update");
                    match pd.get_health_feedback(store_id).await {
                        Ok(feedback) => {
                            saw_supported.store(true, Ordering::Relaxed);
                            crate::stats::set_feedback_slow_score(store_id, feedback.slow_score);
                        }
                        Err(err) if is_unimplemented_error(&err) => {
                            saw_unimplemented.store(true, Ordering::Relaxed);
                            crate::stats::inc_health_feedback_ops_counter(
                                store_id,
                                "active_update_err",
                            );
                        }
                        Err(err) => {
                            saw_supported.store(true, Ordering::Relaxed);
                            crate::stats::inc_health_feedback_ops_counter(
                                store_id,
                                "active_update_err",
                            );
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

    use serial_test::serial;

    use super::*;
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
    use tonic::Status;

    fn label_value<'a>(metric: &'a prometheus::proto::Metric, name: &str) -> Option<&'a str> {
        metric
            .get_label()
            .iter()
            .find(|pair| pair.get_name() == name)
            .map(|pair| pair.get_value())
    }

    fn feedback_slow_score(store: &str) -> Option<f64> {
        prometheus::gather()
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_feedback_slow_score")
            .and_then(|family| {
                family
                    .get_metric()
                    .iter()
                    .find(|metric| label_value(metric, "store") == Some(store))
            })
            .map(|metric| metric.get_gauge().get_value())
    }

    fn health_feedback_counter(scope: &str, ty: &str) -> f64 {
        prometheus::gather()
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_health_feedback_ops_counter")
            .and_then(|family| {
                family.get_metric().iter().find(|metric| {
                    label_value(metric, "scope") == Some(scope)
                        && label_value(metric, "type") == Some(ty)
                })
            })
            .map(|metric| metric.get_counter().get_value())
            .unwrap_or(0.0)
    }

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
    #[serial(metrics)]
    async fn test_refresh_health_feedback_once_queries_all_stores() {
        crate::stats::set_feedback_slow_score(1, 999);
        crate::stats::set_feedback_slow_score(2, 999);

        let tick_1_before = health_feedback_counter("1", "tick");
        let tick_2_before = health_feedback_counter("2", "tick");
        let active_update_1_before = health_feedback_counter("1", "active_update");
        let active_update_2_before = health_feedback_counter("2", "active_update");
        let active_update_err_1_before = health_feedback_counter("1", "active_update_err");
        let active_update_err_2_before = health_feedback_counter("2", "active_update_err");

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

        assert!(
            health_feedback_counter("1", "tick") >= tick_1_before + 1.0,
            "expected health_feedback_ops_counter(1, tick) to increase"
        );
        assert!(
            health_feedback_counter("2", "tick") >= tick_2_before + 1.0,
            "expected health_feedback_ops_counter(2, tick) to increase"
        );
        assert!(
            health_feedback_counter("1", "active_update") >= active_update_1_before + 1.0,
            "expected health_feedback_ops_counter(1, active_update) to increase"
        );
        assert!(
            health_feedback_counter("2", "active_update") >= active_update_2_before + 1.0,
            "expected health_feedback_ops_counter(2, active_update) to increase"
        );
        assert_eq!(
            health_feedback_counter("1", "active_update_err"),
            active_update_err_1_before,
            "expected health_feedback_ops_counter(1, active_update_err) not to change"
        );
        assert_eq!(
            health_feedback_counter("2", "active_update_err"),
            active_update_err_2_before,
            "expected health_feedback_ops_counter(2, active_update_err) not to change"
        );
        assert_eq!(
            feedback_slow_score("1"),
            Some(1.0),
            "expected feedback_slow_score gauge for store 1"
        );
        assert_eq!(
            feedback_slow_score("2"),
            Some(1.0),
            "expected feedback_slow_score gauge for store 2"
        );
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_refresh_health_feedback_once_stops_when_unimplemented_everywhere() {
        crate::stats::set_feedback_slow_score(1, 999);
        crate::stats::set_feedback_slow_score(2, 999);

        let tick_1_before = health_feedback_counter("1", "tick");
        let tick_2_before = health_feedback_counter("2", "tick");
        let active_update_1_before = health_feedback_counter("1", "active_update");
        let active_update_2_before = health_feedback_counter("2", "active_update");
        let active_update_err_1_before = health_feedback_counter("1", "active_update_err");
        let active_update_err_2_before = health_feedback_counter("2", "active_update_err");

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

        assert!(
            health_feedback_counter("1", "tick") >= tick_1_before + 1.0,
            "expected health_feedback_ops_counter(1, tick) to increase"
        );
        assert!(
            health_feedback_counter("2", "tick") >= tick_2_before + 1.0,
            "expected health_feedback_ops_counter(2, tick) to increase"
        );
        assert!(
            health_feedback_counter("1", "active_update") >= active_update_1_before + 1.0,
            "expected health_feedback_ops_counter(1, active_update) to increase"
        );
        assert!(
            health_feedback_counter("2", "active_update") >= active_update_2_before + 1.0,
            "expected health_feedback_ops_counter(2, active_update) to increase"
        );
        assert!(
            health_feedback_counter("1", "active_update_err") >= active_update_err_1_before + 1.0,
            "expected health_feedback_ops_counter(1, active_update_err) to increase"
        );
        assert!(
            health_feedback_counter("2", "active_update_err") >= active_update_err_2_before + 1.0,
            "expected health_feedback_ops_counter(2, active_update_err) to increase"
        );
        assert_eq!(
            feedback_slow_score("1"),
            Some(999.0),
            "expected feedback_slow_score gauge for store 1 not to change"
        );
        assert_eq!(
            feedback_slow_score("2"),
            Some(999.0),
            "expected feedback_slow_score gauge for store 2 not to change"
        );
    }
}
