use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use std::any::Any;
use std::sync::Arc;
use tikv_client::backoff::DEFAULT_REGION_BACKOFF;
use tikv_client::kvrpcpb;
use tikv_client::request::Keyspace;
use tikv_client::request::Plan;
use tikv_client::request::PlanBuilder;
use tikv_client::test_util::MockKvClient;
use tikv_client::test_util::MockPdClient;

fn bench_plan_raw_batch_get_3regions(c: &mut Criterion) {
    let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
        move |req: &dyn Any| {
            let Some(req) = req.downcast_ref::<kvrpcpb::RawBatchGetRequest>() else {
                unreachable!("unexpected request type")
            };

            let pairs = req
                .keys
                .iter()
                .map(|key| kvrpcpb::KvPair {
                    key: key.clone(),
                    value: vec![1_u8],
                    ..Default::default()
                })
                .collect();

            Ok(Box::new(kvrpcpb::RawBatchGetResponse {
                pairs,
                ..Default::default()
            }) as Box<dyn Any + Send>)
        },
    )));

    let request = kvrpcpb::RawBatchGetRequest {
        // Ensure keys are sorted, as required by the sharding algorithm.
        keys: vec![
            vec![1_u8],
            vec![2_u8],
            vec![3_u8],
            vec![10_u8],
            vec![11_u8],
            vec![12_u8],
            vec![250_u8, 250_u8],
            vec![250_u8, 251_u8],
            vec![251_u8, 0_u8],
        ],
        ..Default::default()
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    c.bench_function("plan/raw_batch_get_3regions", |b| {
        b.iter(|| {
            let pd_client = pd_client.clone();
            let request = request.clone();
            rt.block_on(async move {
                let plan = PlanBuilder::new(pd_client, Keyspace::Disable, request)
                    .retry_multi_region(DEFAULT_REGION_BACKOFF)
                    .merge(tikv_client::request::Collect)
                    .plan();
                let out = plan.execute().await.expect("plan execute");
                black_box(out);
            })
        })
    });
}

criterion_group!(benches, bench_plan_raw_batch_get_3regions);
criterion_main!(benches);
