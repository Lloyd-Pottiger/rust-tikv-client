use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use futures::StreamExt;
use log::debug;
use tonic::codegen::http;

use crate::pd::Cluster;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::metapb;
use crate::region_cache::is_tiflash_related_store;
use crate::store::KvConnect;
use crate::Result;
use crate::SecurityManager;

const STORE_LIVENESS_REFRESH_MAX_CONCURRENCY: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LivenessState {
    Reachable,
    Unreachable,
    Unknown,
}

impl LivenessState {
    fn as_u32(self) -> u32 {
        match self {
            LivenessState::Reachable => 0,
            LivenessState::Unreachable => 1,
            LivenessState::Unknown => 2,
        }
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct HealthCheckRequest {
    #[prost(string, tag = "1")]
    service: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct HealthCheckResponse {
    #[prost(enumeration = "ServingStatus", tag = "1")]
    status: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, ::prost::Enumeration)]
#[repr(i32)]
enum ServingStatus {
    Unknown = 0,
    Serving = 1,
    NotServing = 2,
    ServiceUnknown = 3,
}

pub(crate) fn spawn_store_liveness_updater<KvC>(
    pd: Arc<PdRpcClient<KvC, Cluster>>,
    update_interval: Duration,
    timeout: Duration,
) where
    KvC: KvConnect + Send + Sync + 'static,
{
    if update_interval.is_zero() || timeout.is_zero() {
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
            if let Err(err) = refresh_store_liveness_once(pd.clone(), timeout).await {
                debug!("store liveness refresh failed: {:?}", err);
            }
        }
    });
}

async fn refresh_store_liveness_once<KvC>(
    pd: Arc<PdRpcClient<KvC, Cluster>>,
    timeout: Duration,
) -> Result<()>
where
    KvC: KvConnect + Send + Sync + 'static,
{
    let security_mgr = pd.security_manager();
    let stores = pd
        .all_stores()
        .await?
        .into_iter()
        .map(|store| store.meta)
        .collect();
    refresh_store_liveness_for_stores(security_mgr, stores, timeout).await;
    Ok(())
}

async fn refresh_store_liveness_for_stores_with_resolver<R, Fut>(
    stores: Vec<metapb::Store>,
    timeout: Duration,
    resolver: &R,
) where
    R: Fn(String, Duration) -> Fut + Send + Sync,
    Fut: Future<Output = LivenessState> + Send,
{
    stream::iter(stores)
        .for_each_concurrent(
            Some(STORE_LIVENESS_REFRESH_MAX_CONCURRENCY),
            |store| async move {
                if is_tiflash_related_store(&store) {
                    return;
                }
                let store_id = store.id;
                let address = store.address;
                let start = std::time::Instant::now();
                let state = resolver(address.clone(), timeout).await;
                let elapsed = start.elapsed();

                crate::stats::observe_kv_status_api_duration(&address, elapsed);
                crate::stats::inc_kv_status_api_count(if state == LivenessState::Reachable {
                    "ok"
                } else {
                    "err"
                });
                crate::stats::set_store_liveness_state(store_id, state.as_u32());
            },
        )
        .await;
}

async fn refresh_store_liveness_for_stores(
    security_mgr: Arc<SecurityManager>,
    stores: Vec<metapb::Store>,
    timeout: Duration,
) {
    let resolver = |address: String, timeout: Duration| {
        let security_mgr = security_mgr.clone();
        async move { resolve_liveness_via_grpc(security_mgr, address, timeout).await }
    };
    refresh_store_liveness_for_stores_with_resolver(stores, timeout, &resolver).await;
}

async fn resolve_liveness_via_grpc(
    security_mgr: Arc<SecurityManager>,
    address: String,
    timeout: Duration,
) -> LivenessState {
    let fut = async {
        let channel = security_mgr.connect(&address, |ch| ch).await?;
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.map_err(|err| {
            tonic::Status::new(
                tonic::Code::Unknown,
                format!("service was not ready: {err}"),
            )
        })?;

        let mut request = tonic::Request::new(HealthCheckRequest {
            service: String::new(),
        });
        request
            .extensions_mut()
            .insert(tonic::codegen::GrpcMethod::new(
                "grpc.health.v1.Health",
                "Check",
            ));
        let codec = tonic::codec::ProstCodec::default();
        let path = http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Check");
        let response: tonic::Response<HealthCheckResponse> =
            client.unary(request, path, codec).await?;
        Ok::<_, crate::Error>(response.into_inner())
    };

    let response = match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            debug!("kv status api check to {} failed: {:?}", address, err);
            return LivenessState::Unreachable;
        }
        Err(_) => {
            debug!("kv status api check to {} timed out", address);
            return LivenessState::Unreachable;
        }
    };

    match ServingStatus::try_from(response.status) {
        Ok(ServingStatus::Serving) => LivenessState::Reachable,
        Ok(ServingStatus::Unknown) | Ok(ServingStatus::ServiceUnknown) => LivenessState::Unknown,
        Ok(ServingStatus::NotServing) => LivenessState::Unreachable,
        Err(_) => LivenessState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    fn label_value<'a>(metric: &'a prometheus::proto::Metric, name: &str) -> Option<&'a str> {
        metric
            .get_label()
            .iter()
            .find(|pair| pair.get_name() == name)
            .map(|pair| pair.get_value())
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_refresh_store_liveness_for_stores_sets_gauge_and_metrics() {
        fn counter_value(families: &[prometheus::proto::MetricFamily], result: &str) -> f64 {
            families
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_kv_status_api_count")
                .and_then(|family| {
                    family.get_metric().iter().find_map(|metric| {
                        if label_value(metric, "result") == Some(result) {
                            Some(metric.get_counter().get_value())
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or(0.0)
        }

        let stores = vec![
            metapb::Store {
                id: 5001,
                address: "unit_test_kv_status_s1".to_owned(),
                ..Default::default()
            },
            metapb::Store {
                id: 5002,
                address: "unit_test_kv_status_s2".to_owned(),
                ..Default::default()
            },
            metapb::Store {
                id: 5003,
                address: "unit_test_kv_status_tiflash".to_owned(),
                labels: vec![metapb::StoreLabel {
                    key: "engine".to_owned(),
                    value: "tiflash".to_owned(),
                }],
                ..Default::default()
            },
        ];

        let resolver = |address: String, _timeout: Duration| async move {
            match address.as_str() {
                "unit_test_kv_status_s1" => LivenessState::Reachable,
                "unit_test_kv_status_s2" => LivenessState::Unreachable,
                _ => LivenessState::Unknown,
            }
        };

        let before = prometheus::gather();
        let before_ok = counter_value(&before, "ok");
        let before_err = counter_value(&before, "err");

        refresh_store_liveness_for_stores_with_resolver(stores, Duration::from_secs(1), &resolver)
            .await;

        let families = prometheus::gather();

        let gauge_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_store_liveness_state")
            .expect("store_liveness_state gauge not registered");

        let reachable_found = gauge_family.get_metric().iter().any(|metric| {
            label_value(metric, "store") == Some("5001")
                && (metric.get_gauge().get_value() - 0.0).abs() < 1e-6
        });
        let unreachable_found = gauge_family.get_metric().iter().any(|metric| {
            label_value(metric, "store") == Some("5002")
                && (metric.get_gauge().get_value() - 1.0).abs() < 1e-6
        });
        let tiflash_found = gauge_family
            .get_metric()
            .iter()
            .any(|metric| label_value(metric, "store") == Some("5003"));

        assert!(
            reachable_found,
            "expected reachable liveness gauge for store 5001"
        );
        assert!(
            unreachable_found,
            "expected unreachable liveness gauge for store 5002"
        );
        assert!(
            !tiflash_found,
            "expected store liveness gauge to skip tiflash store 5003"
        );

        let after_ok = counter_value(&families, "ok");
        let after_err = counter_value(&families, "err");
        assert!(
            after_ok >= before_ok + 1.0,
            "expected kv_status_api_count(ok) to increase"
        );
        assert!(
            after_err >= before_err + 1.0,
            "expected kv_status_api_count(err) to increase"
        );

        let duration_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_kv_status_api_duration")
            .expect("kv_status_api_duration histogram not registered");

        let s1_found = duration_family.get_metric().iter().any(|metric| {
            label_value(metric, "store") == Some("unit_test_kv_status_s1")
                && metric.get_histogram().get_sample_count() >= 1
        });
        let s2_found = duration_family.get_metric().iter().any(|metric| {
            label_value(metric, "store") == Some("unit_test_kv_status_s2")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            s1_found,
            "expected kv_status_api_duration histogram for store s1"
        );
        assert!(
            s2_found,
            "expected kv_status_api_duration histogram for store s2"
        );
    }
}
