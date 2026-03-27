use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use tikv_client::proto::{errorpb, kvrpcpb};
use tikv_client::request::{self, plan, Merge, NextBatch, Plan, Shardable, StoreRequest};
use tikv_client::store::{HasKeyErrors, HasRegionError, RegionStore, Store};
use tikv_client::{PdClient, PdRpcClient, RegionWithLeader, Result};

fn assert_plan<T: request::Plan>() {}
fn assert_shardable<T: request::Shardable>() {}
fn assert_store_request<T: request::StoreRequest>() {}
fn assert_has_request_label<T: plan::HasRequestLabel>() {}
fn assert_has_kv_context<T: plan::HasKvContext>() {}

#[derive(Default)]
struct StorePlanResult {
    region_error: Option<errorpb::Error>,
    key_errors: Option<Vec<tikv_client::Error>>,
}

impl HasRegionError for StorePlanResult {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.region_error.take()
    }
}

impl HasKeyErrors for StorePlanResult {
    fn key_errors(&mut self) -> Option<Vec<tikv_client::Error>> {
        self.key_errors.take()
    }
}

#[derive(Clone, Default)]
struct StorePlan;

#[async_trait]
impl request::Plan for StorePlan {
    type Result = StorePlanResult;

    async fn execute(&self) -> Result<Self::Result> {
        Ok(StorePlanResult::default())
    }
}

impl StoreRequest for StorePlan {
    fn apply_store(&mut self, _store: &Store) {}
}

#[derive(Clone)]
struct LabelledVecPlan {
    values: Vec<u32>,
    label: &'static str,
}

#[async_trait]
impl request::Plan for LabelledVecPlan {
    type Result = Vec<Result<u32>>;

    async fn execute(&self) -> Result<Self::Result> {
        Ok(self.values.iter().copied().map(Ok).collect())
    }
}

impl plan::HasRequestLabel for LabelledVecPlan {
    fn request_label(&self) -> &'static str {
        self.label
    }
}

#[derive(Clone, Copy)]
struct SumMerge;

impl Merge<u32> for SumMerge {
    type Out = u32;

    fn merge(&self, input: Vec<Result<u32>>) -> Result<Self::Out> {
        input
            .into_iter()
            .try_fold(0, |acc, item| item.map(|value| acc + value))
    }
}

#[derive(Clone)]
struct ShardPlan {
    response: u32,
    label: &'static str,
    context: kvrpcpb::Context,
}

#[async_trait]
impl request::Plan for ShardPlan {
    type Result = u32;

    async fn execute(&self) -> Result<Self::Result> {
        Ok(self.response)
    }
}

impl Shardable for ShardPlan {
    type Shard = Vec<u8>;

    fn shards(
        &self,
        _pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        stream::empty().boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.context.resource_group_tag = shard;
    }

    fn apply_store(&mut self, _store: &RegionStore) -> Result<()> {
        Ok(())
    }
}

impl NextBatch for ShardPlan {
    fn next_batch(&mut self, range: (Vec<u8>, Vec<u8>)) {
        self.context.resolved_locks = range.0.into_iter().map(u64::from).collect();
        self.context.resource_group_tag = range.1;
    }
}

impl plan::HasRequestLabel for ShardPlan {
    fn request_label(&self) -> &'static str {
        self.label
    }
}

impl plan::HasKvContext for ShardPlan {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        Some(&mut self.context)
    }
}

#[test]
fn request_plan_public_api_exposes_wrapper_types_and_trait_impls() {
    assert_plan::<plan::DispatchWithInterceptor<kvrpcpb::GetRequest>>();
    assert_store_request::<plan::DispatchWithInterceptor<kvrpcpb::UnsafeDestroyRangeRequest>>();
    assert_has_request_label::<plan::DispatchWithInterceptor<kvrpcpb::GetRequest>>();
    assert_has_kv_context::<plan::DispatchWithInterceptor<kvrpcpb::GetRequest>>();

    assert_plan::<plan::RetryableAllStores<StorePlan, PdRpcClient>>();
    assert_plan::<plan::RetryableStores<StorePlan>>();

    let _: plan::CollectWithShard = request::CollectWithShard;
    let _: plan::CollectError = request::CollectError;
    let _: Option<plan::RetryableMultiRegion<ShardPlan, PdRpcClient>> = None;
    let _: Option<plan::RetryableAllStores<StorePlan, PdRpcClient>> = None;
    let _: Option<plan::RetryableStores<StorePlan>> = None;
    let _: Option<plan::CleanupLocks<ShardPlan, PdRpcClient>> = None;
    let _: Option<plan::ResolveLock<ShardPlan, PdRpcClient>> = None;

    assert_has_request_label::<plan::CleanupLocks<ShardPlan, PdRpcClient>>();
    assert_has_request_label::<plan::ResolveLock<ShardPlan, PdRpcClient>>();
    assert_has_kv_context::<plan::CleanupLocks<ShardPlan, PdRpcClient>>();
    assert_has_kv_context::<plan::ResolveLock<ShardPlan, PdRpcClient>>();
}

#[derive(Clone)]
struct LabelledValuePlan {
    value: u32,
    label: &'static str,
}

#[async_trait]
impl request::Plan for LabelledValuePlan {
    type Result = u32;

    async fn execute(&self) -> Result<Self::Result> {
        Ok(self.value)
    }
}

impl plan::HasRequestLabel for LabelledValuePlan {
    fn request_label(&self) -> &'static str {
        self.label
    }
}

#[derive(Clone, Copy)]
struct StringifyProcessor;

impl plan::Process<u32> for StringifyProcessor {
    type Out = String;

    fn process(&self, input: Result<u32>) -> Result<Self::Out> {
        input.map(|value| format!("value={value}"))
    }
}

#[derive(Default)]
struct ExtractableResponse {
    key_errors: Option<Vec<tikv_client::Error>>,
    region_error: Option<errorpb::Error>,
}

impl Clone for ExtractableResponse {
    fn clone(&self) -> Self {
        Self {
            key_errors: self.key_errors.as_ref().map(|errors| {
                errors
                    .iter()
                    .map(|error| match error {
                        tikv_client::Error::Unimplemented => tikv_client::Error::Unimplemented,
                        other => panic!("unsupported cloned test error variant: {other:?}"),
                    })
                    .collect()
            }),
            region_error: self.region_error.clone(),
        }
    }
}

impl HasKeyErrors for ExtractableResponse {
    fn key_errors(&mut self) -> Option<Vec<tikv_client::Error>> {
        self.key_errors.take()
    }
}

impl HasRegionError for ExtractableResponse {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.region_error.take()
    }
}

#[derive(Clone)]
struct ExtractPlan {
    response: ExtractableResponse,
    label: &'static str,
}

#[async_trait]
impl request::Plan for ExtractPlan {
    type Result = ExtractableResponse;

    async fn execute(&self) -> Result<Self::Result> {
        Ok(self.response.clone())
    }
}

impl plan::HasRequestLabel for ExtractPlan {
    fn request_label(&self) -> &'static str {
        self.label
    }
}

#[tokio::test]
async fn request_plan_public_api_executes_merge_response() {
    let merged = plan::MergeResponse {
        inner: LabelledVecPlan {
            values: vec![2, 5, 7],
            label: "kv_scan",
        },
        merge: SumMerge,
        phantom: PhantomData::<u32>,
    };

    assert_plan::<plan::MergeResponse<LabelledVecPlan, u32, SumMerge>>();
    assert_has_request_label::<plan::MergeResponse<LabelledVecPlan, u32, SumMerge>>();
    assert_eq!(plan::HasRequestLabel::request_label(&merged), "kv_scan");
    assert_eq!(
        merged
            .execute()
            .await
            .expect("merge response should execute"),
        14
    );
}

#[tokio::test]
async fn request_plan_public_api_preserve_shard_forwards_context_and_response() {
    let mut preserved = plan::PreserveShard {
        inner: ShardPlan {
            response: 9,
            label: "kv_get",
            context: kvrpcpb::Context::default(),
        },
        shard: Some(vec![1, 2, 3]),
    };

    assert_plan::<plan::PreserveShard<ShardPlan>>();
    assert_shardable::<plan::PreserveShard<ShardPlan>>();
    assert_has_request_label::<plan::PreserveShard<ShardPlan>>();
    assert_has_kv_context::<plan::PreserveShard<ShardPlan>>();

    let context = plan::HasKvContext::kv_context_mut(&mut preserved)
        .expect("preserve shard should expose inner context");
    context.region_id = 19;

    assert_eq!(plan::HasRequestLabel::request_label(&preserved), "kv_get");
    assert_eq!(preserved.inner.context.region_id, 19);

    let response = preserved
        .execute()
        .await
        .expect("preserve shard should execute the inner plan");
    assert_eq!(response.0, 9);
    assert_eq!(response.1, vec![1, 2, 3]);
}

#[test]
fn request_plan_public_api_exposes_cleanup_locks_result_behaviour() {
    let mut result = plan::CleanupLocksResult {
        region_error: Some(errorpb::Error {
            message: "region retry".to_owned(),
            ..Default::default()
        }),
        key_error: Some(vec![tikv_client::Error::Unimplemented]),
        resolved_locks: 3,
    };

    let cloned = result.clone();
    assert_eq!(cloned.resolved_locks, 3);
    assert!(cloned.region_error.is_none());
    assert!(cloned.key_error.is_none());

    let region_error = HasRegionError::region_error(&mut result)
        .expect("cleanup lock result should yield the stored region error once");
    assert_eq!(region_error.message, "region retry");
    assert!(HasRegionError::region_error(&mut result).is_none());

    let key_errors = HasKeyErrors::key_errors(&mut result)
        .expect("cleanup lock result should yield the stored key errors once");
    assert_eq!(key_errors.len(), 1);
    assert!(matches!(key_errors[0], tikv_client::Error::Unimplemented));
    assert!(HasKeyErrors::key_errors(&mut result).is_none());

    let merged = request::Collect
        .merge(vec![
            Ok(plan::CleanupLocksResult {
                resolved_locks: 2,
                ..Default::default()
            }),
            Ok(plan::CleanupLocksResult {
                resolved_locks: 5,
                ..Default::default()
            }),
        ])
        .expect("collect should sum resolved lock counts");
    assert_eq!(merged.resolved_locks, 7);
    assert!(merged.region_error.is_none());
    assert!(merged.key_error.is_none());
}

#[tokio::test]
async fn request_plan_public_api_exposes_process_extract_and_response_helpers() {
    let process_response = plan::ProcessResponse {
        inner: LabelledValuePlan {
            value: 11,
            label: "kv_batch_get",
        },
        processor: StringifyProcessor,
    };

    assert_plan::<plan::ProcessResponse<LabelledValuePlan, StringifyProcessor>>();
    assert_has_request_label::<plan::ProcessResponse<LabelledValuePlan, StringifyProcessor>>();
    assert_eq!(
        plan::HasRequestLabel::request_label(&process_response),
        "kv_batch_get"
    );
    assert_eq!(
        process_response
            .execute()
            .await
            .expect("process response should execute"),
        "value=11"
    );

    let default_processor = request::DefaultProcessor;
    let raw_value = <request::DefaultProcessor as plan::Process<kvrpcpb::RawGetResponse>>::process(
        &default_processor,
        Ok(kvrpcpb::RawGetResponse {
            value: b"raw".to_vec(),
            not_found: false,
            ..Default::default()
        }),
    )
    .expect("default raw-get processor should return the value");
    assert_eq!(raw_value, Some(b"raw".to_vec()));
    let missing_txn_value =
        <request::DefaultProcessor as plan::Process<kvrpcpb::GetResponse>>::process(
            &default_processor,
            Ok(kvrpcpb::GetResponse {
                not_found: true,
                ..Default::default()
            }),
        )
        .expect("default get processor should map not_found to None");
    assert_eq!(missing_txn_value, None);

    let collected = request::CollectError
        .merge(vec![Ok(1_u8), Ok(2_u8), Ok(3_u8)])
        .expect("collect error should preserve all successful values");
    assert_eq!(collected, vec![1, 2, 3]);
    assert!(matches!(
        request::CollectError.merge(vec![Ok(1_u8), Err(tikv_client::Error::Unimplemented)]),
        Err(tikv_client::Error::Unimplemented)
    ));

    let extracted_key_error = plan::ExtractError {
        inner: ExtractPlan {
            response: ExtractableResponse {
                key_errors: Some(vec![tikv_client::Error::Unimplemented]),
                ..Default::default()
            },
            label: "kv_scan",
        },
    };
    assert_plan::<plan::ExtractError<ExtractPlan>>();
    assert!(matches!(
        extracted_key_error.execute().await,
        Err(tikv_client::Error::ExtractedErrors(errors))
            if errors.len() == 1 && matches!(errors.first(), Some(tikv_client::Error::Unimplemented))
    ));

    let extracted_region_error = plan::ExtractError {
        inner: ExtractPlan {
            response: ExtractableResponse {
                region_error: Some(errorpb::Error {
                    message: "region fallback".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            label: "kv_scan",
        },
    };
    assert!(matches!(
        extracted_region_error.execute().await,
        Err(tikv_client::Error::ExtractedErrors(errors))
            if errors.len() == 1
                && matches!(
                    errors.first(),
                    Some(tikv_client::Error::RegionError(region_error))
                        if region_error.message == "region fallback"
                )
    ));

    let mut response = plan::ResponseWithShard(
        ExtractableResponse {
            key_errors: Some(vec![tikv_client::Error::Unimplemented]),
            region_error: Some(errorpb::Error {
                message: "region retry".to_owned(),
                ..Default::default()
            }),
        },
        vec![9_u8],
    );
    let key_errors = HasKeyErrors::key_errors(&mut response)
        .expect("response-with-shard should forward key errors");
    assert_eq!(key_errors.len(), 1);
    assert!(matches!(key_errors[0], tikv_client::Error::Unimplemented));
    let region_error = HasRegionError::region_error(&mut response)
        .expect("response-with-shard should forward region errors");
    assert_eq!(region_error.message, "region retry");
    assert_eq!(response.1, vec![9_u8]);
}
