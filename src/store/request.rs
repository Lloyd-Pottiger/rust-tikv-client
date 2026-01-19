// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use tonic::transport::Channel;
use tonic::IntoRequest;

use crate::proto::kvrpcpb;
use crate::proto::resource_manager;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::store::RegionWithLeader;
use crate::CommandPriority;
use crate::DiskFullOpt;
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
    fn context_mut(&mut self) -> &mut kvrpcpb::Context;
    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()>;
    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion);

    fn set_request_source(&mut self, _source: &str) {}

    fn set_resource_group_tag(&mut self, _tag: &[u8]) {}

    fn set_resource_group_name(&mut self, _name: &str) {}

    fn set_priority(&mut self, _priority: CommandPriority) {}

    fn set_disk_full_opt(&mut self, _disk_full_opt: DiskFullOpt) {}

    fn set_txn_source(&mut self, _txn_source: u64) {}

    fn set_resource_control_override_priority(&mut self, _override_priority: u64) {}

    fn set_resource_control_penalty(&mut self, _penalty: &resource_manager::Consumption) {}

    fn set_replica_read(&mut self, _replica_read: bool) {}

    fn set_stale_read(&mut self, _stale_read: bool) {}
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
                let stale_read = self
                    .context
                    .as_ref()
                    .map(|ctx| ctx.stale_read)
                    .unwrap_or(false);
                if stale_read {
                    // Access locality is not tracked in the Rust client yet; treat it as local-zone.
                    crate::stats::observe_stale_read_request(false, self.encoded_len());
                }

                let mut req = self.clone().into_request();
                req.set_timeout(timeout);
                let resp = client.clone().$fun(req).await.map_err(Error::GrpcAPI)?;
                let inner = resp.into_inner();
                if stale_read {
                    // Access locality is not tracked in the Rust client yet; treat it as local-zone.
                    crate::stats::observe_stale_read_response(false, inner.encoded_len());
                }
                Ok(Box::new(inner) as Box<dyn Any>)
            }

            fn label(&self) -> &'static str {
                $label
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn context_mut(&mut self) -> &mut kvrpcpb::Context {
                self.context.get_or_insert(kvrpcpb::Context::default())
            }

            fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
                let ctx = self.context_mut();
                let leader_peer = leader.leader.as_ref().ok_or(Error::LeaderNotFound {
                    region: leader.ver_id(),
                })?;
                ctx.region_id = leader.region.id;
                ctx.region_epoch = leader.region.region_epoch.clone();
                ctx.peer = Some(leader_peer.clone());
                Ok(())
            }

            fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
                let ctx = self.context_mut();
                ctx.api_version = api_version.into();
            }

            fn set_request_source(&mut self, source: &str) {
                let ctx = self.context_mut();
                ctx.request_source = source.to_owned();
            }

            fn set_resource_group_tag(&mut self, tag: &[u8]) {
                let ctx = self.context_mut();
                ctx.resource_group_tag = tag.to_vec();
            }

            fn set_resource_group_name(&mut self, name: &str) {
                let ctx = self.context_mut();
                let resource_ctl_ctx = ctx
                    .resource_control_context
                    .get_or_insert(kvrpcpb::ResourceControlContext::default());
                resource_ctl_ctx.resource_group_name = name.to_owned();
            }

            fn set_priority(&mut self, priority: CommandPriority) {
                let ctx = self.context_mut();
                ctx.priority = priority.into();
            }

            fn set_disk_full_opt(&mut self, disk_full_opt: DiskFullOpt) {
                let ctx = self.context_mut();
                ctx.disk_full_opt = disk_full_opt.into();
            }

            fn set_txn_source(&mut self, txn_source: u64) {
                let ctx = self.context_mut();
                ctx.txn_source = txn_source;
            }

            fn set_resource_control_override_priority(&mut self, override_priority: u64) {
                let ctx = self.context_mut();
                let resource_ctl_ctx = ctx
                    .resource_control_context
                    .get_or_insert(kvrpcpb::ResourceControlContext::default());
                resource_ctl_ctx.override_priority = override_priority;
            }

            fn set_resource_control_penalty(&mut self, penalty: &resource_manager::Consumption) {
                let ctx = self.context_mut();
                let resource_ctl_ctx = ctx
                    .resource_control_context
                    .get_or_insert(kvrpcpb::ResourceControlContext::default());
                resource_ctl_ctx.penalty = Some(penalty.clone());
            }

            fn set_replica_read(&mut self, replica_read: bool) {
                let ctx = self.context_mut();
                ctx.replica_read = replica_read;
            }

            fn set_stale_read(&mut self, stale_read: bool) {
                let ctx = self.context_mut();
                ctx.stale_read = stale_read;
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
impl_request!(CleanupRequest, kv_cleanup, "kv_cleanup");
impl_request!(BatchGetRequest, kv_batch_get, "kv_batch_get");
impl_request!(BatchRollbackRequest, kv_batch_rollback, "kv_batch_rollback");
impl_request!(
    PessimisticRollbackRequest,
    kv_pessimistic_rollback,
    "kv_pessimistic_rollback"
);
impl_request!(ResolveLockRequest, kv_resolve_lock, "kv_resolve_lock");
impl_request!(ScanLockRequest, kv_scan_lock, "kv_scan_lock");
impl_request!(FlushRequest, kv_flush, "kv_flush");
impl_request!(
    BufferBatchGetRequest,
    kv_buffer_batch_get,
    "kv_buffer_batch_get"
);
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
impl_request!(GcRequest, kv_gc, "kv_gc");
impl_request!(DeleteRangeRequest, kv_delete_range, "kv_delete_range");
impl_request!(
    UnsafeDestroyRangeRequest,
    unsafe_destroy_range,
    "unsafe_destroy_range"
);
impl_request!(
    BroadcastTxnStatusRequest,
    broadcast_txn_status,
    "broadcast_txn_status"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::metapb;
    use crate::region::RegionWithLeader;

    #[test]
    fn request_label_and_context_fields() {
        let mut req = kvrpcpb::GetRequest::default();
        assert_eq!(req.label(), "kv_get");

        req.set_request_source("src");
        req.set_resource_group_tag(b"tag");
        req.set_resource_group_name("rg");
        req.set_priority(CommandPriority::High);
        req.set_disk_full_opt(DiskFullOpt::AllowedOnAlmostFull);
        req.set_txn_source(42);
        req.set_resource_control_override_priority(7);
        req.set_resource_control_penalty(&resource_manager::Consumption::default());
        req.set_replica_read(true);
        req.set_stale_read(true);

        let ctx = req.context_mut();
        assert_eq!(ctx.request_source, "src");
        assert_eq!(ctx.resource_group_tag, b"tag".to_vec());
        assert!(ctx.resource_control_context.is_some());
        assert_eq!(
            ctx.resource_control_context
                .as_ref()
                .unwrap()
                .resource_group_name,
            "rg"
        );
        assert_eq!(
            ctx.resource_control_context
                .as_ref()
                .unwrap()
                .override_priority,
            7
        );
        assert!(ctx
            .resource_control_context
            .as_ref()
            .unwrap()
            .penalty
            .is_some());
        assert_eq!(ctx.priority, i32::from(CommandPriority::High));
        assert_eq!(
            ctx.disk_full_opt,
            i32::from(DiskFullOpt::AllowedOnAlmostFull)
        );
        assert_eq!(ctx.txn_source, 42);
        assert!(ctx.replica_read);
        assert!(ctx.stale_read);
    }

    #[test]
    fn set_leader_and_api_version() {
        let mut req = kvrpcpb::GetRequest::default();
        let region = RegionWithLeader {
            region: metapb::Region {
                id: 10,
                region_epoch: Some(metapb::RegionEpoch {
                    conf_ver: 1,
                    version: 2,
                }),
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 42,
                ..Default::default()
            }),
        };
        req.set_leader(&region).unwrap();
        req.set_api_version(kvrpcpb::ApiVersion::V2);

        let ctx = req.context_mut();
        assert_eq!(ctx.region_id, 10);
        assert!(ctx.region_epoch.is_some());
        assert!(ctx.peer.is_some());
        assert_eq!(ctx.peer.as_ref().unwrap().store_id, 42);
        assert_eq!(ctx.api_version, kvrpcpb::ApiVersion::V2 as i32);
    }

    #[test]
    fn set_leader_errors_when_missing() {
        let mut req = kvrpcpb::GetRequest::default();
        let region = RegionWithLeader::default();
        let err = req.set_leader(&region).unwrap_err();
        match err {
            Error::LeaderNotFound { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
