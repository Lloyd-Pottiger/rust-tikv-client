// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::time::Duration;

use async_trait::async_trait;
use fail::fail_point;
use futures::stream;
use tonic::transport::Channel;
use tonic::IntoRequest;
use tonic::IntoStreamingRequest;

use crate::proto::kvrpcpb;
use crate::proto::tikvpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::store::RegionWithLeader;
use crate::Error;
use crate::Result;

#[async_trait]
pub trait Request: Any + Sync + Send + 'static {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>>;
    fn label(&self) -> &'static str;
    fn as_any(&self) -> &dyn Any;
    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()>;
    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion);
    fn set_is_retry_request(&mut self, is_retry_request: bool);

    fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        None
    }
}

macro_rules! impl_request {
    ($name: ident, $fun: ident, $label: literal) => {
        #[async_trait]
        impl Request for kvrpcpb::$name {
            async fn dispatch(
                &self,
                client: &TikvClient<Channel>,
                timeout: Duration,
            ) -> Result<Box<dyn Any>> {
                let mut req = self.clone().into_request();
                req.set_timeout(timeout);
                client
                    .clone()
                    .$fun(req)
                    .await
                    .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
                    .map_err(Error::GrpcAPI)
            }

            fn label(&self) -> &'static str {
                $label
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                let leader_peer = leader.leader.as_ref().ok_or(Error::LeaderNotFound {
                    region: leader.ver_id(),
                })?;
                ctx.region_id = leader.region.id;
                ctx.region_epoch = leader.region.region_epoch.clone();
                ctx.peer = Some(leader_peer.clone());
                Ok(())
            }

            fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.api_version = api_version.into();
            }

            fn set_is_retry_request(&mut self, is_retry_request: bool) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.is_retry_request = is_retry_request;
            }

            fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
                Some(self.context.get_or_insert(kvrpcpb::Context::default()))
            }
        }
    };
}

impl_request!(RawGetRequest, raw_get, "raw_get");
impl_request!(RawBatchGetRequest, raw_batch_get, "raw_batch_get");
impl_request!(RawGetKeyTtlRequest, raw_get_key_ttl, "raw_get_key_ttl");
impl_request!(RawPutRequest, raw_put, "raw_put");
impl_request!(RawBatchPutRequest, raw_batch_put, "raw_batch_put");
impl_request!(RawDeleteRequest, raw_delete, "raw_delete");
impl_request!(RawBatchDeleteRequest, raw_batch_delete, "raw_batch_delete");
impl_request!(RawScanRequest, raw_scan, "raw_scan");
impl_request!(RawBatchScanRequest, raw_batch_scan, "raw_batch_scan");
impl_request!(RawDeleteRangeRequest, raw_delete_range, "raw_delete_range");
impl_request!(RawCasRequest, raw_compare_and_swap, "raw_compare_and_swap");
impl_request!(RawCoprocessorRequest, raw_coprocessor, "raw_coprocessor");
impl_request!(RawChecksumRequest, raw_checksum, "raw_checksum");

impl_request!(GetRequest, kv_get, "kv_get");
impl_request!(ScanRequest, kv_scan, "kv_scan");
impl_request!(PrewriteRequest, kv_prewrite, "kv_prewrite");
impl_request!(CommitRequest, kv_commit, "kv_commit");
impl_request!(BatchGetRequest, kv_batch_get, "kv_batch_get");
impl_request!(BatchRollbackRequest, kv_batch_rollback, "kv_batch_rollback");
impl_request!(FlushRequest, kv_flush, "kv_flush");
impl_request!(
    PessimisticRollbackRequest,
    kv_pessimistic_rollback,
    "kv_pessimistic_rollback"
);
#[async_trait]
impl Request for kvrpcpb::ResolveLockRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        fail_point!("kv_resolve_lock_region_error", |_| {
            let resp = kvrpcpb::ResolveLockResponse {
                region_error: Some(crate::proto::errorpb::Error::default()),
                ..Default::default()
            };
            Ok(Box::new(resp) as Box<dyn Any>)
        });

        let mut req = self.clone().into_request();
        req.set_timeout(timeout);
        client
            .clone()
            .kv_resolve_lock(req)
            .await
            .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
            .map_err(Error::GrpcAPI)
    }

    fn label(&self) -> &'static str {
        "kv_resolve_lock"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
        let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
        let leader_peer = leader.leader.as_ref().ok_or(Error::LeaderNotFound {
            region: leader.ver_id(),
        })?;
        ctx.region_id = leader.region.id;
        ctx.region_epoch = leader.region.region_epoch.clone();
        ctx.peer = Some(leader_peer.clone());
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
        ctx.api_version = api_version.into();
    }

    fn set_is_retry_request(&mut self, is_retry_request: bool) {
        let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
        ctx.is_retry_request = is_retry_request;
    }

    fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        Some(self.context.get_or_insert(kvrpcpb::Context::default()))
    }
}
impl_request!(ScanLockRequest, kv_scan_lock, "kv_scan_lock");
impl_request!(
    PessimisticLockRequest,
    kv_pessimistic_lock,
    "kv_pessimistic_lock"
);
impl_request!(TxnHeartBeatRequest, kv_txn_heart_beat, "kv_txn_heart_beat");
impl_request!(
    CheckTxnStatusRequest,
    kv_check_txn_status,
    "kv_check_txn_status"
);
impl_request!(
    CheckSecondaryLocksRequest,
    kv_check_secondary_locks,
    "kv_check_secondary_locks_request"
);
impl_request!(
    BufferBatchGetRequest,
    kv_buffer_batch_get,
    "kv_buffer_batch_get"
);
impl_request!(GcRequest, kv_gc, "kv_gc");
impl_request!(DeleteRangeRequest, kv_delete_range, "kv_delete_range");
impl_request!(SplitRegionRequest, split_region, "split_region");
impl_request!(
    UnsafeDestroyRangeRequest,
    unsafe_destroy_range,
    "unsafe_destroy_range"
);
impl_request!(
    RegisterLockObserverRequest,
    register_lock_observer,
    "register_lock_observer"
);
impl_request!(
    CheckLockObserverRequest,
    check_lock_observer,
    "check_lock_observer"
);
impl_request!(
    RemoveLockObserverRequest,
    remove_lock_observer,
    "remove_lock_observer"
);
impl_request!(
    PhysicalScanLockRequest,
    physical_scan_lock,
    "physical_scan_lock"
);
impl_request!(
    GetLockWaitInfoRequest,
    get_lock_wait_info,
    "get_lock_wait_info"
);
impl_request!(
    GetLockWaitHistoryRequest,
    get_lock_wait_history,
    "get_lock_wait_history"
);

#[async_trait]
impl Request for kvrpcpb::CompactRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut req = self.clone().into_request();
        req.set_timeout(timeout);
        client
            .clone()
            .compact(req)
            .await
            .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
            .map_err(Error::GrpcAPI)
    }

    fn label(&self) -> &'static str {
        "compact"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.api_version = api_version.into();
    }

    fn set_is_retry_request(&mut self, _is_retry_request: bool) {}
}

#[async_trait]
impl Request for kvrpcpb::TiFlashSystemTableRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut req = self.clone().into_request();
        req.set_timeout(timeout);
        client
            .clone()
            .get_ti_flash_system_table(req)
            .await
            .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
            .map_err(Error::GrpcAPI)
    }

    fn label(&self) -> &'static str {
        "get_ti_flash_system_table"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}

    fn set_is_retry_request(&mut self, _is_retry_request: bool) {}
}

#[async_trait]
impl Request for kvrpcpb::StoreSafeTsRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut req = self.clone().into_request();
        req.set_timeout(timeout);
        client
            .clone()
            .get_store_safe_ts(req)
            .await
            .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
            .map_err(Error::GrpcAPI)
    }

    fn label(&self) -> &'static str {
        "get_store_safe_ts"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}

    fn set_is_retry_request(&mut self, _is_retry_request: bool) {}
}

#[async_trait]
impl Request for kvrpcpb::GetHealthFeedbackRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let dispatch_with_batch = async {
            let batch_request = tikvpb::BatchCommandsRequest {
                requests: vec![tikvpb::batch_commands_request::Request {
                    cmd: Some(
                        tikvpb::batch_commands_request::request::Cmd::GetHealthFeedback(
                            self.clone(),
                        ),
                    ),
                }],
                request_ids: vec![0],
            };
            let request_stream = stream::iter(vec![batch_request]);
            let mut req = request_stream.into_streaming_request();
            req.set_timeout(timeout);
            let mut resp_stream = client
                .clone()
                .batch_commands(req)
                .await
                .map_err(Error::GrpcAPI)?
                .into_inner();
            let resp = resp_stream
                .message()
                .await
                .map_err(Error::GrpcAPI)?
                .ok_or_else(|| {
                    Error::StringError("batch_commands finished without response".to_owned())
                })?;

            let mut health_feedback = resp.health_feedback;
            if health_feedback.is_none() {
                for response in resp.responses {
                    let Some(cmd) = response.cmd else {
                        continue;
                    };
                    if let tikvpb::batch_commands_response::response::Cmd::GetHealthFeedback(
                        inner,
                    ) = cmd
                    {
                        health_feedback = inner.health_feedback;
                        break;
                    }
                }
            }
            Ok::<_, Error>(health_feedback)
        };

        let health_feedback = match dispatch_with_batch.await {
            Ok(feedback) => feedback,
            Err(Error::GrpcAPI(status)) if status.code() == tonic::Code::Unimplemented => {
                let mut req = self.clone().into_request();
                req.set_timeout(timeout);
                return client
                    .clone()
                    .get_health_feedback(req)
                    .await
                    .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
                    .map_err(Error::GrpcAPI);
            }
            Err(err) => return Err(err),
        };

        let resp = kvrpcpb::GetHealthFeedbackResponse {
            region_error: None,
            health_feedback,
        };
        Ok(Box::new(resp) as Box<dyn Any>)
    }

    fn label(&self) -> &'static str {
        "get_health_feedback"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
        ctx.api_version = api_version.into();
    }

    fn set_is_retry_request(&mut self, is_retry_request: bool) {
        let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
        ctx.is_retry_request = is_retry_request;
    }

    fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        Some(self.context.get_or_insert(kvrpcpb::Context::default()))
    }
}

#[async_trait]
impl Request for kvrpcpb::BroadcastTxnStatusRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut req = self.clone().into_request();
        req.set_timeout(timeout);
        client
            .clone()
            .broadcast_txn_status(req)
            .await
            .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
            .map_err(Error::GrpcAPI)
    }

    fn label(&self) -> &'static str {
        "broadcast_txn_status"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
        ctx.api_version = api_version.into();
    }

    fn set_is_retry_request(&mut self, is_retry_request: bool) {
        let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
        ctx.is_retry_request = is_retry_request;
    }

    fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        Some(self.context.get_or_insert(kvrpcpb::Context::default()))
    }
}
