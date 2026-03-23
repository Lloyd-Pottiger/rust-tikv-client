// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::prelude::*;
use log::debug;
use log::info;
use prost::Message;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::sleep;

use crate::backoff::Backoff;
use crate::pd::PdClient;
use crate::proto::errorpb;
use crate::proto::errorpb::EpochNotMatch;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::proto::pdpb::Timestamp;
use crate::region::StoreId;
use crate::region::{RegionVerId, RegionWithLeader};
use crate::request::shard::HasNextBatch;
use crate::request::NextBatch;
use crate::request::Shardable;
use crate::request::{KvRequest, StoreRequest};
use crate::rpc_interceptor::RpcCallResult;
use crate::rpc_interceptor::RpcInterceptors;
use crate::rpc_interceptor::RpcRequest;
use crate::stats::inc_async_batch_get_total;
use crate::stats::inc_async_send_req_total;
use crate::stats::inc_connection_transient_failure_count;
use crate::stats::inc_replica_selector_failure_counter;
use crate::stats::observe_backoff_seconds;
use crate::stats::observe_kv_request_traffic_metrics;
use crate::stats::observe_request_retry_times;
use crate::stats::observe_stale_read_hit_miss;
use crate::stats::tikv_stats_with_context;
use crate::store::HasRegionError;
use crate::store::HasRegionErrors;
use crate::store::KvClient;
use crate::store::RegionStore;
use crate::store::{HasKeyErrors, Store};
use crate::timestamp::TimestampExt;
use crate::trace::{self, Category, TraceField};
use crate::transaction::resolve_locks_for_read;
use crate::transaction::resolve_locks_with_options;
use crate::transaction::HasLocks;
use crate::transaction::LockResolverRpcContext;
use crate::transaction::ReadLockTracker;
use crate::transaction::ResolveLocksContext;
use crate::transaction::ResolveLocksOptions;
use crate::transaction::SnapshotRuntimeStats;
use crate::util::iter::FlatMapOkIterExt;
use crate::Error;
use crate::Key;
use crate::ReplicaReadType;
use crate::Result;
use crate::StoreLabel;

use super::keyspace::Keyspace;

fn check_killed(killed: &Option<Arc<AtomicU32>>) -> Result<()> {
    let Some(killed) = killed.as_ref() else {
        return Ok(());
    };
    let killed_signal = killed.load(Ordering::SeqCst);
    if killed_signal == 0 {
        Ok(())
    } else {
        Err(Error::StringError(format!(
            "query interrupted by signal {killed_signal}"
        )))
    }
}

fn collapse_key_errors(mut errors: Vec<Error>) -> Error {
    match errors.len() {
        0 => Error::InternalError {
            message: "response returned empty key errors".to_owned(),
        },
        1 => errors.pop().unwrap_or_else(|| Error::InternalError {
            message: "response returned empty key errors".to_owned(),
        }),
        _ => Error::MultipleKeyErrors(errors),
    }
}

fn duration_to_ms_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn kv_cmd_from_label(label: &'static str) -> String {
    let label =
        if (label.starts_with("kv_") || label.starts_with("raw_") || label == "batch_coprocessor")
            && label.ends_with("_request")
        {
            label.strip_suffix("_request").unwrap_or(label)
        } else {
            label
        };

    match label {
        "coprocessor" => return "Cop".to_owned(),
        "coprocessor_stream" => return "CopStream".to_owned(),
        "batch_coprocessor" => return "BatchCop".to_owned(),
        _ => {}
    }

    let (prefix, rest) = if let Some(rest) = label.strip_prefix("kv_") {
        ("", rest)
    } else if let Some(rest) = label.strip_prefix("raw_") {
        ("Raw", rest)
    } else {
        ("", label)
    };

    let mut out = String::new();
    out.push_str(prefix);
    for part in rest.split('_').filter(|part| !part.is_empty()) {
        match part {
            "gc" => out.push_str("GC"),
            "id" => out.push_str("ID"),
            "mpp" => out.push_str("MPP"),
            "rpc" => out.push_str("RPC"),
            "ts" => out.push_str("TS"),
            "tso" => out.push_str("TSO"),
            "ttl" => out.push_str("TTL"),
            _ => {
                let mut chars = part.chars();
                if let Some(first) = chars.next() {
                    out.extend(first.to_uppercase());
                    out.push_str(chars.as_str());
                }
            }
        }
    }

    if out.is_empty() {
        label.to_owned()
    } else {
        out
    }
}

fn kv_request_trace_fields(
    label: &'static str,
    cmd: &str,
    ctx: Option<&kvrpcpb::Context>,
    store_addr: &str,
    timeout: Duration,
) -> Vec<TraceField> {
    let (region_id, region_ver, region_conf_ver, store_id, ver_id) = match ctx {
        Some(ctx) => {
            let epoch = ctx.region_epoch.as_ref();
            let conf_ver = epoch.map(|epoch| epoch.conf_ver).unwrap_or(0);
            let ver = epoch.map(|epoch| epoch.version).unwrap_or(0);
            let store_id = ctx.peer.as_ref().map(|peer| peer.store_id).unwrap_or(0);
            let ver_id = RegionVerId {
                id: ctx.region_id,
                conf_ver,
                ver,
            };
            (ctx.region_id, ver, conf_ver, store_id, Some(ver_id))
        }
        None => (0, 0, 0, 0, None),
    };

    let mut fields = vec![
        TraceField::str("label", label),
        TraceField::str("cmd", cmd.to_owned()),
        TraceField::u64("region_id", region_id),
        TraceField::u64("region_ver", region_ver),
        TraceField::u64("region_confVer", region_conf_ver),
        TraceField::u64("store_id", store_id),
        TraceField::str("store_addr", store_addr.to_owned()),
        TraceField::u64("timeout_ms", duration_to_ms_saturating(timeout)),
    ];

    if let Some(ver_id) = ver_id {
        if let Some((start_key, end_key)) = trace::kv_request_region_range(&ver_id) {
            fields.push(TraceField::str(
                "region_start_key",
                crate::redact::key(&start_key),
            ));
            fields.push(TraceField::str(
                "region_end_key",
                crate::redact::key(&end_key),
            ));
        }
    }

    fields
}

fn trace_cop_other_error_if_present(
    ctx: Option<&kvrpcpb::Context>,
    store_addr: &str,
    resp: &dyn Any,
) {
    let Some(resp) = resp.downcast_ref::<crate::proto::coprocessor::Response>() else {
        return;
    };
    if resp.other_error.is_empty() {
        return;
    }

    let (region_id, region_ver, region_conf_ver, store_id) = match ctx {
        Some(ctx) => {
            let epoch = ctx.region_epoch.as_ref();
            let conf_ver = epoch.map(|epoch| epoch.conf_ver).unwrap_or(0);
            let ver = epoch.map(|epoch| epoch.version).unwrap_or(0);
            let store_id = ctx.peer.as_ref().map(|peer| peer.store_id).unwrap_or(0);
            (ctx.region_id, ver, conf_ver, store_id)
        }
        None => (0, 0, 0, 0),
    };

    let fields = vec![
        TraceField::str("other_error", resp.other_error.clone()),
        TraceField::u64("region_id", region_id),
        TraceField::u64("region_ver", region_ver),
        TraceField::u64("region_confVer", region_conf_ver),
        TraceField::u64("store_id", store_id),
        TraceField::str("store_addr", store_addr.to_owned()),
    ];
    trace::trace(Category::KvRequest, "cop.other_error", &fields);
}

/// A plan for how to execute a request. A user builds up a plan with various
/// options, then exectutes it.
#[async_trait]
pub trait Plan: Sized + Clone + Sync + Send + 'static {
    /// The ultimate result of executing the plan (should be a high-level type, not a GRPC response).
    type Result: Send;

    /// Execute the plan.
    async fn execute(&self) -> Result<Self::Result>;

    /// Snapshot runtime stats collected while executing this plan.
    ///
    /// This is internal plumbing used to propagate snapshot runtime stats through plan wrappers.
    #[doc(hidden)]
    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        None
    }
}

/// The simplest plan which just dispatches a request to a specific kv server.
#[derive(Clone)]
pub struct Dispatch<Req: KvRequest> {
    pub request: Req,
    pub kv_client: Option<Arc<dyn KvClient + Send + Sync>>,
}

#[async_trait]
impl<Req: KvRequest> Plan for Dispatch<Req> {
    type Result = Req::Response;

    async fn execute(&self) -> Result<Self::Result> {
        let label = self.request.label();
        let stats = tikv_stats_with_context(label, self.request.context());

        let Some(kv_client) = self.kv_client.as_ref() else {
            return stats.done(Err(Error::InternalError {
                message: "kv_client has not been initialised in Dispatch".to_owned(),
            }));
        };

        let prewrite_req = (&self.request as &dyn Any).downcast_ref::<kvrpcpb::PrewriteRequest>();
        let commit_req = (&self.request as &dyn Any).downcast_ref::<kvrpcpb::CommitRequest>();

        let kv_trace_enabled = trace::is_category_enabled(Category::KvRequest);
        let txn_2pc_trace_enabled = (prewrite_req.is_some() || commit_req.is_some())
            && trace::is_category_enabled(Category::Txn2Pc);

        let kv_trace_cmd = kv_trace_enabled.then(|| kv_cmd_from_label(label));
        let kv_trace_timeout = kv_trace_enabled.then(|| {
            self.request
                .timeout_override()
                .unwrap_or_else(|| kv_client.timeout())
        });
        let kv_trace_store_addr = kv_trace_enabled.then(|| kv_client.store_address().unwrap_or(""));

        if kv_trace_enabled {
            let fields = kv_request_trace_fields(
                label,
                kv_trace_cmd.as_deref().unwrap_or(label),
                self.request.context(),
                kv_trace_store_addr.unwrap_or(""),
                kv_trace_timeout.unwrap_or_default(),
            );
            trace::trace(Category::KvRequest, "kv.request.send", &fields);
        }

        if txn_2pc_trace_enabled {
            if let Some(req) = prewrite_req {
                let region_id = req.context.as_ref().map(|ctx| ctx.region_id).unwrap_or(0);
                let is_primary = req.mutations.iter().any(|m| m.key == req.primary_lock);
                let key_count = u64::try_from(req.mutations.len()).unwrap_or(u64::MAX);

                let fields = vec![
                    TraceField::u64("startTS", req.start_version),
                    TraceField::u64("regionID", region_id),
                    TraceField::bool("isPrimary", is_primary),
                    TraceField::u64("keyCount", key_count),
                ];
                trace::trace(Category::Txn2Pc, "prewrite.batch.start", &fields);
            }

            if let Some(req) = commit_req {
                let region_id = req.context.as_ref().map(|ctx| ctx.region_id).unwrap_or(0);
                let key_count = u64::try_from(req.keys.len()).unwrap_or(u64::MAX);

                let fields = vec![
                    TraceField::u64("startTS", req.start_version),
                    TraceField::u64("commitTS", req.commit_version),
                    TraceField::u64("regionID", region_id),
                    TraceField::u64("keyCount", key_count),
                ];
                trace::trace(Category::Txn2Pc, "commit.batch.start", &fields);
            }
        }

        let track_exec_details = crate::util::exec_details().is_some();
        let track_traffic_metrics = self
            .request
            .context()
            .is_some_and(|context| context.stale_read);
        let request_bytes = (track_exec_details || track_traffic_metrics)
            .then(|| request_encoded_len(&self.request));
        let started_at = if kv_trace_enabled || txn_2pc_trace_enabled || track_exec_details {
            Some(Instant::now())
        } else {
            None
        };

        let result = kv_client.dispatch(&self.request).await.and_then(|r| {
            r.downcast::<Req::Response>()
                .map(|r| *r)
                .map_err(|_| Error::InternalError {
                    message: format!(
                        "downcast failed: request and response type mismatch, expected {}",
                        std::any::type_name::<Req::Response>()
                    ),
                })
        });

        if let Some(started_at) = started_at.as_ref() {
            crate::util::record_task_local_wait_kv_response(started_at.elapsed());
        }

        if let Some(sent_bytes) = request_bytes {
            let received_bytes = result
                .as_ref()
                .ok()
                .map(|resp| response_encoded_len(resp as &dyn Any))
                .unwrap_or(0);
            if track_exec_details {
                crate::util::record_task_local_kv_traffic(sent_bytes, received_bytes);
            }
            let (_, is_cross_zone) = crate::util::task_traffic_kind();
            observe_kv_request_traffic_metrics(
                label,
                self.request.context(),
                is_cross_zone,
                sent_bytes,
                received_bytes,
                result.is_ok(),
            );
        }

        if kv_trace_enabled {
            let elapsed_ms = started_at
                .as_ref()
                .map(|at| u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0);
            let mut fields = kv_request_trace_fields(
                label,
                kv_trace_cmd.as_deref().unwrap_or(label),
                self.request.context(),
                kv_trace_store_addr.unwrap_or(""),
                kv_trace_timeout.unwrap_or_default(),
            );
            fields.push(TraceField::bool("success", result.is_ok()));
            fields.push(TraceField::u64("latency_ms", elapsed_ms));
            if crate::util::trace_exec_details_enabled() {
                if let Ok(resp) = result.as_ref() {
                    if let Some(details) = exec_details_v2_from_response(resp as &dyn Any) {
                        append_trace_exec_details_fields(&mut fields, details);
                    }
                }
            }
            trace::trace(Category::KvRequest, "kv.request.result", &fields);
            if let Ok(resp) = result.as_ref() {
                trace_cop_other_error_if_present(
                    self.request.context(),
                    kv_trace_store_addr.unwrap_or(""),
                    resp as &dyn Any,
                );
            }
        }

        if txn_2pc_trace_enabled {
            let success = result.is_ok();

            if prewrite_req.is_some() {
                let region_id = prewrite_req
                    .and_then(|req| req.context.as_ref().map(|ctx| ctx.region_id))
                    .unwrap_or(0);
                let fields = vec![
                    TraceField::u64("regionID", region_id),
                    TraceField::bool("success", success),
                ];
                trace::trace(Category::Txn2Pc, "prewrite.batch.result", &fields);
            }

            if commit_req.is_some() {
                let region_id = commit_req
                    .and_then(|req| req.context.as_ref().map(|ctx| ctx.region_id))
                    .unwrap_or(0);
                let fields = vec![
                    TraceField::u64("regionID", region_id),
                    TraceField::bool("success", success),
                ];
                trace::trace(Category::Txn2Pc, "commit.batch.result", &fields);
            }
        }

        stats.done(result)
    }
}

/// Dispatch with RPC interceptor support.
///
/// This is used internally by transactional requests when the caller has configured RPC
/// interceptors (client-go `KVTxn.SetRPCInterceptor` / `KVSnapshot.SetRPCInterceptor` parity).
#[derive(Clone)]
pub struct DispatchWithInterceptor<Req: KvRequest> {
    pub request: Req,
    pub kv_client: Option<Arc<dyn KvClient + Send + Sync>>,
    pub store_address: Option<String>,
    pub rpc_interceptors: RpcInterceptors,
}

#[async_trait]
impl<Req: KvRequest> Plan for DispatchWithInterceptor<Req> {
    type Result = Req::Response;

    async fn execute(&self) -> Result<Self::Result> {
        let label = self.request.label();
        let kv_client = self.kv_client.as_ref().ok_or(Error::InternalError {
            message: "kv_client has not been initialised in DispatchWithInterceptor".to_owned(),
        })?;
        let target = self.store_address.as_deref().unwrap_or("<unknown>");

        let prewrite_trace = (&self.request as &dyn Any)
            .downcast_ref::<kvrpcpb::PrewriteRequest>()
            .map(|req| {
                let region_id = req.context.as_ref().map(|ctx| ctx.region_id).unwrap_or(0);
                let is_primary = req.mutations.iter().any(|m| m.key == req.primary_lock);
                let key_count = u64::try_from(req.mutations.len()).unwrap_or(u64::MAX);
                (req.start_version, region_id, is_primary, key_count)
            });
        let commit_trace = (&self.request as &dyn Any)
            .downcast_ref::<kvrpcpb::CommitRequest>()
            .map(|req| {
                let region_id = req.context.as_ref().map(|ctx| ctx.region_id).unwrap_or(0);
                let key_count = u64::try_from(req.keys.len()).unwrap_or(u64::MAX);
                (req.start_version, req.commit_version, region_id, key_count)
            });

        let kv_trace_enabled = trace::is_category_enabled(Category::KvRequest);
        let txn_2pc_trace_enabled = (prewrite_trace.is_some() || commit_trace.is_some())
            && trace::is_category_enabled(Category::Txn2Pc);

        let kv_trace_cmd = kv_trace_enabled.then(|| kv_cmd_from_label(label));
        let kv_trace_timeout = kv_trace_enabled.then(|| {
            self.request
                .timeout_override()
                .unwrap_or_else(|| kv_client.timeout())
        });

        if self.rpc_interceptors.is_empty() {
            let stats = tikv_stats_with_context(label, self.request.context());
            if kv_trace_enabled {
                let mut fields = kv_request_trace_fields(
                    label,
                    kv_trace_cmd.as_deref().unwrap_or(label),
                    self.request.context(),
                    target,
                    kv_trace_timeout.unwrap_or_default(),
                );
                fields.push(TraceField::str("target", target.to_owned()));
                trace::trace(Category::KvRequest, "kv.request.send", &fields);
            }

            if txn_2pc_trace_enabled {
                if let Some((start_ts, region_id, is_primary, key_count)) = prewrite_trace {
                    let fields = vec![
                        TraceField::u64("startTS", start_ts),
                        TraceField::u64("regionID", region_id),
                        TraceField::bool("isPrimary", is_primary),
                        TraceField::u64("keyCount", key_count),
                    ];
                    trace::trace(Category::Txn2Pc, "prewrite.batch.start", &fields);
                }

                if let Some((start_ts, commit_ts, region_id, key_count)) = commit_trace {
                    let fields = vec![
                        TraceField::u64("startTS", start_ts),
                        TraceField::u64("commitTS", commit_ts),
                        TraceField::u64("regionID", region_id),
                        TraceField::u64("keyCount", key_count),
                    ];
                    trace::trace(Category::Txn2Pc, "commit.batch.start", &fields);
                }
            }

            let track_exec_details = crate::util::exec_details().is_some();
            let track_traffic_metrics = self
                .request
                .context()
                .is_some_and(|context| context.stale_read);
            let request_bytes = (track_exec_details || track_traffic_metrics)
                .then(|| request_encoded_len(&self.request));
            let started_at = if kv_trace_enabled || txn_2pc_trace_enabled || track_exec_details {
                Some(Instant::now())
            } else {
                None
            };

            let result = kv_client.dispatch(&self.request).await.and_then(|r| {
                r.downcast::<Req::Response>()
                    .map(|r| *r)
                    .map_err(|_| Error::InternalError {
                        message: format!(
                            "downcast failed: request and response type mismatch, expected {}",
                            std::any::type_name::<Req::Response>()
                        ),
                    })
            });

            if let Some(started_at) = started_at.as_ref() {
                crate::util::record_task_local_wait_kv_response(started_at.elapsed());
            }

            if let Some(sent_bytes) = request_bytes {
                let received_bytes = result
                    .as_ref()
                    .ok()
                    .map(|resp| response_encoded_len(resp as &dyn Any))
                    .unwrap_or(0);
                if track_exec_details {
                    crate::util::record_task_local_kv_traffic(sent_bytes, received_bytes);
                }
                let (_, is_cross_zone) = crate::util::task_traffic_kind();
                observe_kv_request_traffic_metrics(
                    label,
                    self.request.context(),
                    is_cross_zone,
                    sent_bytes,
                    received_bytes,
                    result.is_ok(),
                );
            }

            if kv_trace_enabled {
                let elapsed_ms = started_at
                    .as_ref()
                    .map(|at| u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX))
                    .unwrap_or(0);
                let mut fields = kv_request_trace_fields(
                    label,
                    kv_trace_cmd.as_deref().unwrap_or(label),
                    self.request.context(),
                    target,
                    kv_trace_timeout.unwrap_or_default(),
                );
                fields.push(TraceField::str("target", target.to_owned()));
                fields.push(TraceField::bool("success", result.is_ok()));
                fields.push(TraceField::u64("latency_ms", elapsed_ms));
                if crate::util::trace_exec_details_enabled() {
                    if let Ok(resp) = result.as_ref() {
                        if let Some(details) = exec_details_v2_from_response(resp as &dyn Any) {
                            append_trace_exec_details_fields(&mut fields, details);
                        }
                    }
                }
                trace::trace(Category::KvRequest, "kv.request.result", &fields);
                if let Ok(resp) = result.as_ref() {
                    trace_cop_other_error_if_present(
                        self.request.context(),
                        target,
                        resp as &dyn Any,
                    );
                }
            }

            if txn_2pc_trace_enabled {
                let success = result.is_ok();

                if let Some((_, region_id, _, _)) = prewrite_trace {
                    let fields = vec![
                        TraceField::u64("regionID", region_id),
                        TraceField::bool("success", success),
                    ];
                    trace::trace(Category::Txn2Pc, "prewrite.batch.result", &fields);
                }

                if let Some((_, _, region_id, _)) = commit_trace {
                    let fields = vec![
                        TraceField::u64("regionID", region_id),
                        TraceField::bool("success", success),
                    ];
                    trace::trace(Category::Txn2Pc, "commit.batch.result", &fields);
                }
            }

            return stats.done(result);
        }

        let mut request = self.request.clone();
        let label = request.label();
        let mut rpc_request = RpcRequest::new(target, label, request.context_mut());
        for interceptor in self.rpc_interceptors.iter() {
            interceptor.before(&mut rpc_request);
        }
        let stats = tikv_stats_with_context(label, request.context());

        let prewrite_trace = (&request as &dyn Any)
            .downcast_ref::<kvrpcpb::PrewriteRequest>()
            .map(|req| {
                let region_id = req.context.as_ref().map(|ctx| ctx.region_id).unwrap_or(0);
                let is_primary = req.mutations.iter().any(|m| m.key == req.primary_lock);
                let key_count = u64::try_from(req.mutations.len()).unwrap_or(u64::MAX);
                (req.start_version, region_id, is_primary, key_count)
            });
        let commit_trace = (&request as &dyn Any)
            .downcast_ref::<kvrpcpb::CommitRequest>()
            .map(|req| {
                let region_id = req.context.as_ref().map(|ctx| ctx.region_id).unwrap_or(0);
                let key_count = u64::try_from(req.keys.len()).unwrap_or(u64::MAX);
                (req.start_version, req.commit_version, region_id, key_count)
            });

        let kv_trace_cmd = kv_trace_enabled.then(|| kv_cmd_from_label(label));
        let kv_trace_timeout = kv_trace_enabled.then(|| {
            request
                .timeout_override()
                .unwrap_or_else(|| kv_client.timeout())
        });

        if kv_trace_enabled {
            let mut fields = kv_request_trace_fields(
                label,
                kv_trace_cmd.as_deref().unwrap_or(label),
                request.context(),
                target,
                kv_trace_timeout.unwrap_or_default(),
            );
            fields.push(TraceField::str("target", target.to_owned()));
            trace::trace(Category::KvRequest, "kv.request.send", &fields);
        }

        if txn_2pc_trace_enabled {
            if let Some((start_ts, region_id, is_primary, key_count)) = prewrite_trace {
                let fields = vec![
                    TraceField::u64("startTS", start_ts),
                    TraceField::u64("regionID", region_id),
                    TraceField::bool("isPrimary", is_primary),
                    TraceField::u64("keyCount", key_count),
                ];
                trace::trace(Category::Txn2Pc, "prewrite.batch.start", &fields);
            }

            if let Some((start_ts, commit_ts, region_id, key_count)) = commit_trace {
                let fields = vec![
                    TraceField::u64("startTS", start_ts),
                    TraceField::u64("commitTS", commit_ts),
                    TraceField::u64("regionID", region_id),
                    TraceField::u64("keyCount", key_count),
                ];
                trace::trace(Category::Txn2Pc, "commit.batch.start", &fields);
            }
        }

        let track_exec_details = crate::util::exec_details().is_some();
        let track_traffic_metrics = request.context().is_some_and(|context| context.stale_read);
        let request_bytes =
            (track_exec_details || track_traffic_metrics).then(|| request_encoded_len(&request));
        let started_at = if kv_trace_enabled || txn_2pc_trace_enabled || track_exec_details {
            Some(Instant::now())
        } else {
            None
        };

        let result = kv_client.dispatch(&request).await.and_then(|r| {
            r.downcast::<Req::Response>()
                .map(|r| *r)
                .map_err(|_| Error::InternalError {
                    message: format!(
                        "downcast failed: request and response type mismatch, expected {}",
                        std::any::type_name::<Req::Response>()
                    ),
                })
        });

        if let Some(started_at) = started_at.as_ref() {
            crate::util::record_task_local_wait_kv_response(started_at.elapsed());
        }

        if let Some(sent_bytes) = request_bytes {
            let received_bytes = result
                .as_ref()
                .ok()
                .map(|resp| response_encoded_len(resp as &dyn Any))
                .unwrap_or(0);
            if track_exec_details {
                crate::util::record_task_local_kv_traffic(sent_bytes, received_bytes);
            }
            let (_, is_cross_zone) = crate::util::task_traffic_kind();
            observe_kv_request_traffic_metrics(
                label,
                request.context(),
                is_cross_zone,
                sent_bytes,
                received_bytes,
                result.is_ok(),
            );
        }

        let label = request.label();
        let rpc_request = RpcRequest::new(target, label, request.context_mut());
        let call_result = match &result {
            Ok(_) => RpcCallResult::Ok,
            Err(err) => RpcCallResult::Err(err),
        };
        for interceptor in self.rpc_interceptors.iter().rev() {
            interceptor.after(&rpc_request, call_result);
        }

        if kv_trace_enabled {
            let elapsed_ms = started_at
                .as_ref()
                .map(|at| u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0);
            let mut fields = kv_request_trace_fields(
                label,
                kv_trace_cmd.as_deref().unwrap_or(label),
                request.context(),
                target,
                kv_trace_timeout.unwrap_or_default(),
            );
            fields.push(TraceField::str("target", target.to_owned()));
            fields.push(TraceField::bool("success", result.is_ok()));
            fields.push(TraceField::u64("latency_ms", elapsed_ms));
            if crate::util::trace_exec_details_enabled() {
                if let Ok(resp) = result.as_ref() {
                    if let Some(details) = exec_details_v2_from_response(resp as &dyn Any) {
                        append_trace_exec_details_fields(&mut fields, details);
                    }
                }
            }
            trace::trace(Category::KvRequest, "kv.request.result", &fields);
            if let Ok(resp) = result.as_ref() {
                trace_cop_other_error_if_present(request.context(), target, resp as &dyn Any);
            }
        }

        if txn_2pc_trace_enabled {
            let success = result.is_ok();

            if let Some((_, region_id, _, _)) = prewrite_trace {
                let fields = vec![
                    TraceField::u64("regionID", region_id),
                    TraceField::bool("success", success),
                ];
                trace::trace(Category::Txn2Pc, "prewrite.batch.result", &fields);
            }

            if let Some((_, _, region_id, _)) = commit_trace {
                let fields = vec![
                    TraceField::u64("regionID", region_id),
                    TraceField::bool("success", success),
                ];
                trace::trace(Category::Txn2Pc, "commit.batch.result", &fields);
            }
        }

        stats.done(result)
    }
}

fn append_trace_exec_details_fields(
    fields: &mut Vec<TraceField>,
    details: &kvrpcpb::ExecDetailsV2,
) {
    fn push_field(fields: &mut Vec<TraceField>, key: &'static str, value: u64) {
        if value > 0 {
            fields.push(TraceField::u64(key, value));
        }
    }

    let (total_rpc_ns, wait_ns, process_ns, suspend_ns, kv_read_ns) =
        if let Some(detail) = details.time_detail_v2.as_ref() {
            (
                detail.total_rpc_wall_time_ns,
                detail.wait_wall_time_ns,
                detail.process_wall_time_ns,
                detail.process_suspend_wall_time_ns,
                detail.kv_read_wall_time_ns,
            )
        } else if let Some(detail) = details.time_detail.as_ref() {
            (
                detail.total_rpc_wall_time_ns,
                detail.wait_wall_time_ms.saturating_mul(1_000_000),
                detail.process_wall_time_ms.saturating_mul(1_000_000),
                0,
                detail.kv_read_wall_time_ms.saturating_mul(1_000_000),
            )
        } else {
            return;
        };

    push_field(fields, "tikv_exec_total_rpc_ns", total_rpc_ns);
    push_field(fields, "tikv_exec_wait_ns", wait_ns);
    push_field(fields, "tikv_exec_process_ns", process_ns);
    push_field(fields, "tikv_exec_suspend_ns", suspend_ns);
    push_field(fields, "tikv_exec_kv_read_ns", kv_read_ns);

    if let Some(detail) = details.scan_detail_v2.as_ref() {
        push_field(
            fields,
            "tikv_exec_get_snapshot_ns",
            detail.get_snapshot_nanos,
        );
        push_field(
            fields,
            "tikv_exec_rocksdb_block_read_ns",
            detail.rocksdb_block_read_nanos,
        );
        push_field(
            fields,
            "tikv_exec_rocksdb_block_read_bytes",
            detail.rocksdb_block_read_byte,
        );
        push_field(
            fields,
            "tikv_exec_read_index_propose_wait_ns",
            detail.read_index_propose_wait_nanos,
        );
        push_field(
            fields,
            "tikv_exec_read_index_confirm_wait_ns",
            detail.read_index_confirm_wait_nanos,
        );
        push_field(
            fields,
            "tikv_exec_read_pool_schedule_wait_ns",
            detail.read_pool_schedule_wait_nanos,
        );
    }

    if let Some(detail) = details.write_detail.as_ref() {
        push_field(
            fields,
            "tikv_exec_store_batch_wait_ns",
            detail.store_batch_wait_nanos,
        );
        push_field(
            fields,
            "tikv_exec_propose_send_wait_ns",
            detail.propose_send_wait_nanos,
        );
        push_field(fields, "tikv_exec_persist_log_ns", detail.persist_log_nanos);
        push_field(
            fields,
            "tikv_exec_raft_db_write_leader_wait_ns",
            detail.raft_db_write_leader_wait_nanos,
        );
        push_field(
            fields,
            "tikv_exec_raft_db_sync_log_ns",
            detail.raft_db_sync_log_nanos,
        );
        push_field(
            fields,
            "tikv_exec_raft_db_write_memtable_ns",
            detail.raft_db_write_memtable_nanos,
        );
        push_field(fields, "tikv_exec_commit_log_ns", detail.commit_log_nanos);
        push_field(
            fields,
            "tikv_exec_apply_batch_wait_ns",
            detail.apply_batch_wait_nanos,
        );
        push_field(fields, "tikv_exec_apply_log_ns", detail.apply_log_nanos);
        push_field(
            fields,
            "tikv_exec_apply_mutex_lock_ns",
            detail.apply_mutex_lock_nanos,
        );
        push_field(
            fields,
            "tikv_exec_apply_write_leader_wait_ns",
            detail.apply_write_leader_wait_nanos,
        );
        push_field(
            fields,
            "tikv_exec_apply_write_wal_ns",
            detail.apply_write_wal_nanos,
        );
        push_field(
            fields,
            "tikv_exec_apply_write_memtable_ns",
            detail.apply_write_memtable_nanos,
        );
        push_field(fields, "tikv_exec_latch_wait_ns", detail.latch_wait_nanos);
        push_field(fields, "tikv_exec_write_process_ns", detail.process_nanos);
        push_field(fields, "tikv_exec_throttle_ns", detail.throttle_nanos);
        push_field(
            fields,
            "tikv_exec_pessimistic_lock_wait_ns",
            detail.pessimistic_lock_wait_nanos,
        );
    }
}

fn exec_details_v2_from_response(resp: &dyn Any) -> Option<&kvrpcpb::ExecDetailsV2> {
    if let Some(resp) = resp.downcast_ref::<kvrpcpb::GetResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = resp.downcast_ref::<kvrpcpb::BatchGetResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = resp.downcast_ref::<kvrpcpb::BatchRollbackResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = resp.downcast_ref::<kvrpcpb::CheckSecondaryLocksResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = resp.downcast_ref::<kvrpcpb::CheckTxnStatusResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = resp.downcast_ref::<kvrpcpb::ResolveLockResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = resp.downcast_ref::<kvrpcpb::ScanLockResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    None
}

fn response_encoded_len(resp: &dyn Any) -> i64 {
    macro_rules! encoded_len_from_response {
        ($resp:expr, $( $ty:path ),+ $(,)?) => {{
            $(
                if let Some(resp) = $resp.downcast_ref::<$ty>() {
                    return i64::try_from(resp.encoded_len()).unwrap_or(i64::MAX);
                }
            )+
            0
        }};
    }

    encoded_len_from_response!(
        resp,
        kvrpcpb::RawGetResponse,
        kvrpcpb::RawBatchGetResponse,
        kvrpcpb::RawGetKeyTtlResponse,
        kvrpcpb::RawPutResponse,
        kvrpcpb::RawBatchPutResponse,
        kvrpcpb::RawDeleteResponse,
        kvrpcpb::RawBatchDeleteResponse,
        kvrpcpb::RawDeleteRangeResponse,
        kvrpcpb::RawScanResponse,
        kvrpcpb::RawBatchScanResponse,
        kvrpcpb::RawChecksumResponse,
        kvrpcpb::RawCasResponse,
        kvrpcpb::RawCoprocessorResponse,
        kvrpcpb::GetResponse,
        kvrpcpb::BatchGetResponse,
        kvrpcpb::BufferBatchGetResponse,
        kvrpcpb::ScanResponse,
        kvrpcpb::ResolveLockResponse,
        kvrpcpb::PrewriteResponse,
        kvrpcpb::FlushResponse,
        kvrpcpb::CommitResponse,
        kvrpcpb::BatchRollbackResponse,
        kvrpcpb::PessimisticRollbackResponse,
        kvrpcpb::PessimisticLockResponse,
        kvrpcpb::ScanLockResponse,
        kvrpcpb::TxnHeartBeatResponse,
        kvrpcpb::CheckTxnStatusResponse,
        kvrpcpb::CheckSecondaryLocksResponse,
        kvrpcpb::DeleteRangeResponse,
        kvrpcpb::PrepareFlashbackToVersionResponse,
        kvrpcpb::FlashbackToVersionResponse,
        kvrpcpb::SplitRegionResponse,
        kvrpcpb::UnsafeDestroyRangeResponse,
        kvrpcpb::CompactResponse,
        kvrpcpb::TiFlashSystemTableResponse,
        kvrpcpb::RegisterLockObserverResponse,
        kvrpcpb::CheckLockObserverResponse,
        kvrpcpb::RemoveLockObserverResponse,
        kvrpcpb::PhysicalScanLockResponse,
        kvrpcpb::GetLockWaitInfoResponse,
        kvrpcpb::GetLockWaitHistoryResponse,
        kvrpcpb::StoreSafeTsResponse,
    )
}

fn request_encoded_len(req: &dyn crate::store::Request) -> i64 {
    i64::try_from(req.encoded_len()).unwrap_or(i64::MAX)
}

/// A dispatch plan that records snapshot runtime stats.
///
/// This is internal plumbing used by [`PlanBuilder`](crate::request::PlanBuilder) when snapshot
/// runtime stats are enabled via `Snapshot::set_runtime_stats`.
#[derive(Clone)]
pub(crate) struct DispatchWithRuntimeStats<Req: KvRequest> {
    pub(crate) inner: Dispatch<Req>,
    pub(crate) runtime_stats: Option<Arc<SnapshotRuntimeStats>>,
}

#[async_trait]
impl<Req: KvRequest> Plan for DispatchWithRuntimeStats<Req> {
    type Result = Req::Response;

    async fn execute(&self) -> Result<Self::Result> {
        let label = self.inner.request.label();
        let should_record = self.runtime_stats.is_some() && self.inner.kv_client.is_some();
        if !should_record {
            return self.inner.execute().await;
        }
        let started_at = Instant::now();
        let result = self.inner.execute().await;
        let elapsed = started_at.elapsed();

        if let Some(stats) = self.runtime_stats.as_ref() {
            stats.record_rpc(label, elapsed);
            if let Ok(resp) = result.as_ref() {
                if let Some(details) = exec_details_v2_from_response(resp as &dyn Any) {
                    stats.merge_exec_details_v2(details);
                }
            }
        }

        result
    }

    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.runtime_stats.clone()
    }
}

/// A dispatch-with-interceptors plan that records snapshot runtime stats.
#[derive(Clone)]
pub(crate) struct DispatchWithInterceptorRuntimeStats<Req: KvRequest> {
    pub(crate) inner: DispatchWithInterceptor<Req>,
    pub(crate) runtime_stats: Option<Arc<SnapshotRuntimeStats>>,
}

#[async_trait]
impl<Req: KvRequest> Plan for DispatchWithInterceptorRuntimeStats<Req> {
    type Result = Req::Response;

    async fn execute(&self) -> Result<Self::Result> {
        let label = self.inner.request.label();
        let should_record = self.runtime_stats.is_some() && self.inner.kv_client.is_some();
        if !should_record {
            return self.inner.execute().await;
        }
        let started_at = Instant::now();
        let result = self.inner.execute().await;
        let elapsed = started_at.elapsed();

        if let Some(stats) = self.runtime_stats.as_ref() {
            stats.record_rpc(label, elapsed);
            if let Ok(resp) = result.as_ref() {
                if let Some(details) = exec_details_v2_from_response(resp as &dyn Any) {
                    stats.merge_exec_details_v2(details);
                }
            }
        }

        result
    }

    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.runtime_stats.clone()
    }
}

impl<Req: KvRequest + StoreRequest> StoreRequest for Dispatch<Req> {
    fn apply_store(&mut self, store: &Store) {
        self.kv_client = Some(store.client.clone());
        self.request.apply_store(store);
    }
}

impl<Req: KvRequest + StoreRequest> StoreRequest for DispatchWithInterceptor<Req> {
    fn apply_store(&mut self, store: &Store) {
        self.kv_client = Some(store.client.clone());
        self.store_address = Some(store.meta.address.clone());
        self.request.apply_store(store);
    }
}

impl<Req: KvRequest + StoreRequest> StoreRequest for DispatchWithRuntimeStats<Req> {
    fn apply_store(&mut self, store: &Store) {
        self.inner.apply_store(store);
    }
}

impl<Req: KvRequest + StoreRequest> StoreRequest for DispatchWithInterceptorRuntimeStats<Req> {
    fn apply_store(&mut self, store: &Store) {
        self.inner.apply_store(store);
    }
}

pub(crate) const DEFAULT_MULTI_REGION_CONCURRENCY: usize = 16;
const MULTI_STORES_CONCURRENCY: usize = 16;

pub(crate) fn is_grpc_error(e: &Error) -> bool {
    matches!(e, Error::GrpcAPI(_) | Error::Grpc(_))
}

const SLOW_STORE_TTL_ON_SERVER_IS_BUSY: Duration = Duration::from_secs(10);
const SLOW_STORE_TTL_ON_GRPC_DEADLINE_EXCEEDED: Duration = Duration::from_secs(10);
const ZONE_LABEL_KEY: &str = "zone";

fn is_grpc_deadline_exceeded(e: &Error) -> bool {
    matches!(
        e,
        Error::GrpcAPI(status) if status.code() == tonic::Code::DeadlineExceeded
    )
}

fn is_deadline_exceeded_region_error(e: &errorpb::Error) -> bool {
    // Match client-go's `isDeadlineExceeded` check for configurable timeout errors.
    e.message.contains("Deadline is exceeded")
}

fn is_invalid_max_ts_update_region_error(e: &errorpb::Error) -> bool {
    // Match client-go's `isInvalidMaxTsUpdate` check.
    e.message.contains("invalid max_ts update")
}

#[doc(hidden)]
pub trait HasKvContext {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context>;
}

#[doc(hidden)]
pub trait HasRequestLabel {
    fn request_label(&self) -> &'static str;
}

impl<Req: KvRequest> HasRequestLabel for Dispatch<Req> {
    fn request_label(&self) -> &'static str {
        self.request.label()
    }
}

impl<Req: KvRequest> HasRequestLabel for DispatchWithInterceptor<Req> {
    fn request_label(&self) -> &'static str {
        self.request.label()
    }
}

impl<Req: KvRequest> HasRequestLabel for DispatchWithRuntimeStats<Req> {
    fn request_label(&self) -> &'static str {
        self.inner.request.label()
    }
}

impl<Req: KvRequest> HasRequestLabel for DispatchWithInterceptorRuntimeStats<Req> {
    fn request_label(&self) -> &'static str {
        self.inner.request.label()
    }
}

impl<P: Plan + HasRequestLabel, In, M: Merge<In>> HasRequestLabel for MergeResponse<P, In, M> {
    fn request_label(&self) -> &'static str {
        self.inner.request_label()
    }
}

impl<P: Plan + HasRequestLabel, Pr: Process<P::Result>> HasRequestLabel for ProcessResponse<P, Pr> {
    fn request_label(&self) -> &'static str {
        self.inner.request_label()
    }
}

impl<P: Plan + HasRequestLabel, PdC: PdClient> HasRequestLabel for ResolveLock<P, PdC> {
    fn request_label(&self) -> &'static str {
        self.inner.request_label()
    }
}

impl<P: Plan + HasRequestLabel, PdC: PdClient> HasRequestLabel for ResolveLockInContext<P, PdC> {
    fn request_label(&self) -> &'static str {
        self.inner.request_label()
    }
}

impl<P: Plan + HasRequestLabel, PdC: PdClient> HasRequestLabel for ResolveLockForRead<P, PdC> {
    fn request_label(&self) -> &'static str {
        self.inner.request_label()
    }
}

impl<P: Plan + HasRequestLabel, PdC: PdClient> HasRequestLabel for CleanupLocks<P, PdC> {
    fn request_label(&self) -> &'static str {
        self.inner.request_label()
    }
}

impl<P: Plan + Shardable + HasRequestLabel> HasRequestLabel for PreserveShard<P> {
    fn request_label(&self) -> &'static str {
        self.inner.request_label()
    }
}

impl<P: Plan + HasRequestLabel> HasRequestLabel for ExtractError<P> {
    fn request_label(&self) -> &'static str {
        self.inner.request_label()
    }
}

impl<Req: KvRequest> HasKvContext for Dispatch<Req> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.request.context_mut()
    }
}

impl<Req: KvRequest> HasKvContext for DispatchWithInterceptor<Req> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.request.context_mut()
    }
}

impl<Req: KvRequest> HasKvContext for DispatchWithRuntimeStats<Req> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.request.context_mut()
    }
}

impl<Req: KvRequest> HasKvContext for DispatchWithInterceptorRuntimeStats<Req> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.request.context_mut()
    }
}

impl<P: Plan + Shardable + HasKvContext> HasKvContext for PreserveShard<P> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.kv_context_mut()
    }
}

impl<P: Plan + HasKvContext, PdC: PdClient> HasKvContext for ResolveLock<P, PdC> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.kv_context_mut()
    }
}

impl<P: Plan + HasKvContext, PdC: PdClient> HasKvContext for ResolveLockInContext<P, PdC> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.kv_context_mut()
    }
}

impl<P: Plan + HasKvContext, PdC: PdClient> HasKvContext for ResolveLockForRead<P, PdC> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.kv_context_mut()
    }
}

impl<P: Plan + HasKvContext, PdC: PdClient> HasKvContext for CleanupLocks<P, PdC> {
    fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.kv_context_mut()
    }
}

fn store_labels_match(store: &metapb::Store, labels: &[StoreLabel]) -> bool {
    if labels.is_empty() {
        return true;
    }

    labels.iter().all(|target| {
        store
            .labels
            .iter()
            .any(|label| label.key == target.key && label.value == target.value)
    })
}

fn store_zone_label(store: &metapb::Store) -> Option<&str> {
    store
        .labels
        .iter()
        .find(|label| label.key == ZONE_LABEL_KEY)
        .map(|label| label.value.as_str())
}

fn is_cross_zone(self_zone_label: Option<&str>, store: &metapb::Store) -> bool {
    let Some(self_zone_label) = self_zone_label else {
        return false;
    };
    if self_zone_label.is_empty() {
        return false;
    }
    let Some(target_zone_label) = store_zone_label(store) else {
        return false;
    };
    if target_zone_label.is_empty() {
        return false;
    }
    self_zone_label != target_zone_label
}

#[allow(clippy::too_many_arguments)]
fn select_replica_read_peer(
    region: &RegionWithLeader,
    replica_read: ReplicaReadType,
    attempt: u32,
    unavailable_store_ids: &[StoreId],
    busy_store_ids: &[StoreId],
    attempted_store_ids: &[StoreId],
    data_is_not_ready_store_ids: &[StoreId],
    match_configured: bool,
    labels_configured: bool,
    label_matched_store_ids: &[StoreId],
) -> Option<metapb::Peer> {
    let leader_store_id = region.leader.as_ref().map(|p| p.store_id);
    let peers = &region.region.peers;
    if peers.is_empty() {
        inc_replica_selector_failure_counter("invalid");
        return None;
    }

    fn is_store_available(unavailable_store_ids: &[StoreId], store_id: StoreId) -> bool {
        !unavailable_store_ids.contains(&store_id)
    }

    fn select_replica_read_peer_by_score(
        region: &RegionWithLeader,
        peers: &[metapb::Peer],
        replica_read: ReplicaReadType,
        _attempt: u32,
        unavailable_store_ids: &[StoreId],
        busy_store_ids: &[StoreId],
        attempted_store_ids: &[StoreId],
        data_is_not_ready_store_ids: &[StoreId],
        leader_store_id: Option<StoreId>,
        labels_configured: bool,
        label_matched_store_ids: &[StoreId],
        non_leader_first: bool,
    ) -> Option<metapb::Peer> {
        const FLAG_NOT_ATTEMPTED: i64 = 1 << 0;
        const FLAG_NORMAL_PEER: i64 = 1 << 1;
        const FLAG_PREFER_LEADER: i64 = 1 << 2;
        const FLAG_LABEL_MATCHES: i64 = 1 << 3;
        const FLAG_NOT_SLOW: i64 = 1 << 4;

        let is_candidate = |peer: &metapb::Peer| -> bool {
            if !is_store_available(unavailable_store_ids, peer.store_id) {
                return false;
            }
            if replica_read == ReplicaReadType::PreferLeader
                && Some(peer.store_id) != leader_store_id
                && busy_store_ids.contains(&peer.store_id)
            {
                // Align with client-go PreferLeader behavior: skip slow non-leader replicas.
                return false;
            }
            if matches!(
                replica_read,
                ReplicaReadType::Mixed | ReplicaReadType::PreferLeader
            ) {
                let allow_data_is_not_ready_retry = data_is_not_ready_store_ids
                    .contains(&peer.store_id)
                    && Some(peer.store_id) != leader_store_id;
                if attempted_store_ids.contains(&peer.store_id) && !allow_data_is_not_ready_retry {
                    return false;
                }
            }
            match replica_read {
                ReplicaReadType::Follower => {
                    Some(peer.store_id) != leader_store_id
                        && peer.role != metapb::PeerRole::Learner as i32
                }
                ReplicaReadType::Learner => peer.role == metapb::PeerRole::Learner as i32,
                ReplicaReadType::Mixed | ReplicaReadType::PreferLeader => true,
                ReplicaReadType::Leader => false,
            }
        };

        let candidates = if non_leader_first && replica_read == ReplicaReadType::Mixed {
            let mut rotation = peers
                .iter()
                .filter(|peer| Some(peer.store_id) != leader_store_id)
                .filter(|peer| is_candidate(peer))
                .cloned()
                .collect::<Vec<_>>();
            if let Some(leader) = region.leader.clone() {
                if is_store_available(unavailable_store_ids, leader.store_id)
                    && !rotation.iter().any(|peer| peer.store_id == leader.store_id)
                {
                    rotation.push(leader);
                }
            }
            rotation
        } else {
            let mut candidates = peers
                .iter()
                .filter(|peer| is_candidate(peer))
                .cloned()
                .collect::<Vec<_>>();
            if matches!(
                replica_read,
                ReplicaReadType::Mixed | ReplicaReadType::PreferLeader
            ) {
                if let Some(leader) = region.leader.clone() {
                    if is_store_available(unavailable_store_ids, leader.store_id)
                        && !candidates
                            .iter()
                            .any(|peer| peer.store_id == leader.store_id)
                    {
                        candidates.push(leader);
                    }
                }
            }
            candidates
        };
        if candidates.is_empty() {
            return None;
        }

        let mut scores = Vec::with_capacity(candidates.len());
        let mut max_score = i64::MIN;
        for peer in candidates.iter() {
            let is_leader = Some(peer.store_id) == leader_store_id;
            let not_slow = !busy_store_ids.contains(&peer.store_id);
            let label_matches = label_matched_store_ids.contains(&peer.store_id);
            let not_attempted = !attempted_store_ids.contains(&peer.store_id);

            let mut score = 0;
            if not_slow {
                score |= FLAG_NOT_SLOW;
            }
            if label_matches {
                score |= FLAG_LABEL_MATCHES;
            }
            if is_leader {
                match replica_read {
                    ReplicaReadType::PreferLeader => {
                        if not_slow {
                            score |= FLAG_PREFER_LEADER;
                        } else {
                            score |= FLAG_NORMAL_PEER;
                        }
                    }
                    ReplicaReadType::Mixed => {
                        if labels_configured {
                            score |= FLAG_PREFER_LEADER;
                        } else {
                            score |= FLAG_NORMAL_PEER;
                        }
                    }
                    _ => {}
                }
            } else {
                score |= FLAG_NORMAL_PEER;
            }
            if not_attempted {
                score |= FLAG_NOT_ATTEMPTED;
            }

            if score > max_score {
                max_score = score;
            }
            scores.push(score);
        }

        let best_indices = scores
            .iter()
            .enumerate()
            .filter(|(_, score)| **score == max_score)
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        if best_indices.is_empty() {
            return None;
        }
        let mut best_indices = best_indices;
        if non_leader_first
            && replica_read == ReplicaReadType::Mixed
            && !unavailable_store_ids.is_empty()
        {
            if let Some(leader_store_id) = leader_store_id {
                if best_indices
                    .iter()
                    .any(|&idx| candidates[idx].store_id != leader_store_id)
                {
                    best_indices.retain(|&idx| candidates[idx].store_id != leader_store_id);
                }
            }
        }
        let selected = {
            #[cfg(test)]
            {
                let start = (_attempt as usize) % best_indices.len();
                best_indices[start]
            }
            #[cfg(not(test))]
            {
                use rand::seq::SliceRandom;

                let mut rng = rand::thread_rng();
                match best_indices.choose(&mut rng) {
                    Some(selected) => *selected,
                    None => 0,
                }
            }
        };
        candidates.get(selected).cloned()
    }

    if match_configured {
        let peer = select_replica_read_peer_by_score(
            region,
            peers,
            replica_read,
            attempt,
            unavailable_store_ids,
            busy_store_ids,
            attempted_store_ids,
            data_is_not_ready_store_ids,
            leader_store_id,
            labels_configured,
            label_matched_store_ids,
            false,
        );
        if peer.is_none() {
            inc_replica_selector_failure_counter("exhausted");
        }
        return peer;
    }

    fn select_peer_with_attempt<F>(
        peers: &[metapb::Peer],
        attempt: u32,
        busy_store_ids: &[StoreId],
        attempted_store_ids: &[StoreId],
        predicate: F,
    ) -> Option<metapb::Peer>
    where
        F: Copy + Fn(&metapb::Peer) -> bool,
    {
        let mut candidates = peers
            .iter()
            .filter(|peer| predicate(peer))
            .cloned()
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return None;
        }
        let preferred = candidates
            .iter()
            .filter(|peer| !busy_store_ids.contains(&peer.store_id))
            .cloned()
            .collect::<Vec<_>>();
        if !preferred.is_empty() {
            candidates = preferred;
        }
        let unattempted = candidates
            .iter()
            .filter(|peer| !attempted_store_ids.contains(&peer.store_id))
            .cloned()
            .collect::<Vec<_>>();
        if !unattempted.is_empty() {
            candidates = unattempted;
        }
        let target = (attempt as usize) % candidates.len();
        candidates.get(target).cloned()
    }

    let peer = match replica_read {
        ReplicaReadType::Follower => select_peer_with_attempt(
            peers,
            attempt,
            busy_store_ids,
            attempted_store_ids,
            |peer| {
                Some(peer.store_id) != leader_store_id
                    && peer.role != metapb::PeerRole::Learner as i32
                    && is_store_available(unavailable_store_ids, peer.store_id)
            },
        ),
        ReplicaReadType::Learner => select_peer_with_attempt(
            peers,
            attempt,
            busy_store_ids,
            attempted_store_ids,
            |peer| {
                peer.role == metapb::PeerRole::Learner as i32
                    && is_store_available(unavailable_store_ids, peer.store_id)
            },
        ),
        ReplicaReadType::Mixed => select_replica_read_peer_by_score(
            region,
            peers,
            replica_read,
            attempt,
            unavailable_store_ids,
            busy_store_ids,
            attempted_store_ids,
            data_is_not_ready_store_ids,
            leader_store_id,
            labels_configured,
            label_matched_store_ids,
            true,
        ),
        ReplicaReadType::Leader => None,
        ReplicaReadType::PreferLeader => match leader_store_id {
            Some(store_id) if is_store_available(unavailable_store_ids, store_id) => {
                if !busy_store_ids.contains(&store_id) {
                    None
                } else {
                    let has_idle_non_leader = peers.iter().any(|peer| {
                        Some(peer.store_id) != leader_store_id
                            && is_store_available(unavailable_store_ids, peer.store_id)
                            && !busy_store_ids.contains(&peer.store_id)
                    });
                    if !has_idle_non_leader {
                        None
                    } else {
                        select_peer_with_attempt(
                            peers,
                            attempt,
                            busy_store_ids,
                            attempted_store_ids,
                            |peer| {
                                Some(peer.store_id) != leader_store_id
                                    && is_store_available(unavailable_store_ids, peer.store_id)
                                    && !busy_store_ids.contains(&peer.store_id)
                            },
                        )
                    }
                }
            }
            _ => select_peer_with_attempt(
                peers,
                attempt,
                busy_store_ids,
                attempted_store_ids,
                |peer| {
                    Some(peer.store_id) != leader_store_id
                        && is_store_available(unavailable_store_ids, peer.store_id)
                        && !busy_store_ids.contains(&peer.store_id)
                },
            ),
        },
    };

    if peer.is_none() {
        match replica_read {
            ReplicaReadType::Follower | ReplicaReadType::Learner => {
                inc_replica_selector_failure_counter("exhausted");
            }
            ReplicaReadType::Mixed | ReplicaReadType::PreferLeader => match leader_store_id {
                Some(leader_store_id) => {
                    if unavailable_store_ids.contains(&leader_store_id) {
                        inc_replica_selector_failure_counter("exhausted");
                    }
                }
                None => inc_replica_selector_failure_counter("exhausted"),
            },
            ReplicaReadType::Leader => {}
        }
    }

    peer.or_else(|| region.leader.clone())
        .or_else(|| region.region.peers.first().cloned())
}

#[derive(Debug, Default)]
struct StoreLiveness {
    unreachable_store_ids: RwLock<Vec<StoreId>>,
    server_is_busy_store_ids: RwLock<Vec<StoreId>>,
    attempted_store_ids: RwLock<Vec<StoreId>>,
    data_is_not_ready_store_ids: RwLock<Vec<StoreId>>,
}

impl StoreLiveness {
    async fn mark_unreachable(&self, store_id: StoreId) {
        let mut unreachable_store_ids = self.unreachable_store_ids.write().await;
        if !unreachable_store_ids.contains(&store_id) {
            unreachable_store_ids.push(store_id);
        }
    }

    async fn mark_server_is_busy(&self, store_id: StoreId) {
        let mut server_is_busy_store_ids = self.server_is_busy_store_ids.write().await;
        if !server_is_busy_store_ids.contains(&store_id) {
            server_is_busy_store_ids.push(store_id);
        }
    }

    async fn mark_attempted(&self, store_id: StoreId) {
        let already_attempted = {
            let mut attempted_store_ids = self.attempted_store_ids.write().await;
            if attempted_store_ids.contains(&store_id) {
                true
            } else {
                attempted_store_ids.push(store_id);
                false
            }
        };
        if already_attempted {
            self.clear_data_is_not_ready(store_id).await;
        }
    }

    async fn mark_data_is_not_ready(&self, store_id: StoreId) {
        let mut data_is_not_ready_store_ids = self.data_is_not_ready_store_ids.write().await;
        if !data_is_not_ready_store_ids.contains(&store_id) {
            data_is_not_ready_store_ids.push(store_id);
        }
    }

    async fn clear_data_is_not_ready(&self, store_id: StoreId) {
        let mut data_is_not_ready_store_ids = self.data_is_not_ready_store_ids.write().await;
        data_is_not_ready_store_ids.retain(|id| *id != store_id);
    }

    async fn unreachable_store_ids(&self) -> Vec<StoreId> {
        self.unreachable_store_ids.read().await.clone()
    }

    async fn server_is_busy_store_ids(&self) -> Vec<StoreId> {
        self.server_is_busy_store_ids.read().await.clone()
    }

    async fn attempted_store_ids(&self) -> Vec<StoreId> {
        self.attempted_store_ids.read().await.clone()
    }

    async fn data_is_not_ready_store_ids(&self) -> Vec<StoreId> {
        self.data_is_not_ready_store_ids.read().await.clone()
    }

    async fn is_server_is_busy(&self, store_id: StoreId) -> bool {
        self.server_is_busy_store_ids
            .read()
            .await
            .contains(&store_id)
    }

    async fn has_attempted(&self, store_id: StoreId) -> bool {
        self.attempted_store_ids.read().await.contains(&store_id)
    }
}

#[derive(Clone, Debug)]
struct ReplicaReadState {
    read_type: ReplicaReadType,
    attempt_base: u32,
    retry_same_replica: bool,
    stale_read: bool,
    stale_read_disabled: Arc<AtomicBool>,
    store_liveness: Arc<StoreLiveness>,
}

impl ReplicaReadState {
    fn new(read_type: ReplicaReadType, stale_read: bool) -> Self {
        Self {
            read_type,
            attempt_base: 0,
            retry_same_replica: false,
            stale_read,
            stale_read_disabled: Arc::new(AtomicBool::new(false)),
            store_liveness: Arc::new(StoreLiveness::default()),
        }
    }

    fn stale_read_enabled(&self) -> bool {
        self.stale_read && !self.stale_read_disabled.load(Ordering::Relaxed)
    }

    fn disable_stale_read(&self) {
        self.stale_read_disabled.store(true, Ordering::Relaxed);
    }

    async fn mark_store_unreachable(&self, store_id: StoreId) {
        self.store_liveness.mark_unreachable(store_id).await;
    }

    async fn mark_store_server_is_busy(&self, store_id: StoreId) {
        self.store_liveness.mark_server_is_busy(store_id).await;
    }

    async fn mark_store_attempted(&self, store_id: StoreId) {
        self.store_liveness.mark_attempted(store_id).await;
    }

    async fn mark_store_data_is_not_ready(&self, store_id: StoreId) {
        self.store_liveness.mark_data_is_not_ready(store_id).await;
    }

    async fn unreachable_store_ids(&self) -> Vec<StoreId> {
        self.store_liveness.unreachable_store_ids().await
    }

    async fn is_store_server_is_busy(&self, store_id: StoreId) -> bool {
        self.store_liveness.is_server_is_busy(store_id).await
    }

    async fn server_is_busy_store_ids(&self) -> Vec<StoreId> {
        self.store_liveness.server_is_busy_store_ids().await
    }

    async fn attempted_store_ids(&self) -> Vec<StoreId> {
        self.store_liveness.attempted_store_ids().await
    }

    async fn data_is_not_ready_store_ids(&self) -> Vec<StoreId> {
        self.store_liveness.data_is_not_ready_store_ids().await
    }

    async fn has_attempted_store(&self, store_id: StoreId) -> bool {
        self.store_liveness.has_attempted(store_id).await
    }

    fn attempt(&self, current_attempts: u32) -> u32 {
        current_attempts.saturating_sub(self.attempt_base)
    }

    fn switch_to(self, read_type: ReplicaReadType, current_attempts: u32) -> Self {
        Self {
            read_type,
            attempt_base: current_attempts,
            retry_same_replica: false,
            stale_read: self.stale_read,
            stale_read_disabled: self.stale_read_disabled,
            store_liveness: self.store_liveness,
        }
    }

    fn keep_current_attempt_on_retry(self) -> Self {
        Self {
            read_type: self.read_type,
            attempt_base: self.attempt_base.saturating_add(1),
            retry_same_replica: true,
            stale_read: self.stale_read,
            stale_read_disabled: self.stale_read_disabled,
            store_liveness: self.store_liveness,
        }
    }

    fn clear_retry_same_replica(self) -> Self {
        Self {
            retry_same_replica: false,
            ..self
        }
    }
}

pub struct RetryableMultiRegion<P: Plan, PdC: PdClient> {
    pub(super) inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
    pub(super) killed: Option<Arc<AtomicU32>>,

    pub(super) concurrency: usize,
    pub(super) txn_regions_num_observer: Option<(&'static str, bool)>,
    pub(super) batch_executor_token_wait_observer: bool,

    /// Preserve all regions' results for other downstream plans to handle.
    /// If true, return Ok and preserve all regions' results, even if some of them are Err.
    /// Otherwise, return the first Err if there is any.
    pub preserve_region_results: bool,

    pub(super) replica_read: Option<ReplicaReadType>,
    pub(super) match_store_ids: Arc<Vec<u64>>,
    pub(super) match_store_labels: Arc<Vec<StoreLabel>>,
}

impl<P: Plan + Shardable + HasKvContext + HasRequestLabel, PdC: PdClient>
    RetryableMultiRegion<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    // A plan may involve multiple shards
    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn single_plan_handler(
        pd_client: Arc<PdC>,
        mut current_plan: P,
        backoff: Backoff,
        killed: Option<Arc<AtomicU32>>,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        stale_read: bool,
        replica_read: Option<ReplicaReadState>,
        match_store_ids: Arc<Vec<u64>>,
        match_store_labels: Arc<Vec<StoreLabel>>,
        txn_regions_num_observer: Option<(&'static str, bool)>,
        batch_executor_token_wait_observer: bool,
    ) -> Result<<Self as Plan>::Result> {
        if backoff.current_attempts() > 0 {
            if let Some(ctx) = current_plan.kv_context_mut() {
                ctx.is_retry_request = true;
            }
        }
        let shards = current_plan.shards(&pd_client).collect::<Vec<_>>().await;
        if let Some((label, internal)) = txn_regions_num_observer {
            if shards.iter().all(|shard| shard.is_ok()) {
                let mut unique_region_ids = std::collections::HashSet::new();
                for shard in &shards {
                    let (_shard, region) = shard.as_ref().expect("checked shard ok");
                    unique_region_ids.insert(region.id());
                }
                crate::stats::observe_txn_regions_num(label, internal, unique_region_ids.len());
            }
        }
        let batch_executor_token_wait_nanos = if batch_executor_token_wait_observer {
            Some(Arc::new(AtomicU64::new(0)))
        } else {
            None
        };
        debug!("single_plan_handler, shards: {}", shards.len());
        let mut handles = Vec::with_capacity(shards.len());
        for shard in shards {
            let (shard, region) = shard?;
            let clone = current_plan.clone_then_apply_shard(shard);
            let replica_read = replica_read.clone();
            let match_store_ids = match_store_ids.clone();
            let match_store_labels = match_store_labels.clone();
            let batch_executor_token_wait_nanos = batch_executor_token_wait_nanos.clone();
            let handle = crate::util::spawn_with_inherited_task_locals(Self::single_shard_handler(
                pd_client.clone(),
                clone,
                region,
                backoff.clone(),
                killed.clone(),
                permits.clone(),
                preserve_region_results,
                stale_read,
                replica_read,
                match_store_ids,
                match_store_labels,
                txn_regions_num_observer,
                batch_executor_token_wait_nanos,
            ));
            handles.push(handle);
        }

        let results = try_join_all(handles).await;
        if let Some(batch_executor_token_wait_nanos) = batch_executor_token_wait_nanos.as_ref() {
            crate::stats::observe_batch_executor_token_wait_duration(
                batch_executor_token_wait_nanos.load(Ordering::Relaxed),
            );
        }
        let results = results?;
        if preserve_region_results {
            Ok(results
                .into_iter()
                .flat_map_ok(|x| x)
                .map(|x| match x {
                    Ok(r) => r,
                    Err(e) => Err(e),
                })
                .collect())
        } else {
            Ok(results
                .into_iter()
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect())
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn single_shard_handler(
        pd_client: Arc<PdC>,
        mut plan: P,
        mut region: RegionWithLeader,
        mut backoff: Backoff,
        killed: Option<Arc<AtomicU32>>,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        stale_read: bool,
        replica_read: Option<ReplicaReadState>,
        match_store_ids: Arc<Vec<u64>>,
        match_store_labels: Arc<Vec<StoreLabel>>,
        txn_regions_num_observer: Option<(&'static str, bool)>,
        batch_executor_token_wait_nanos: Option<Arc<AtomicU64>>,
    ) -> Result<<Self as Plan>::Result> {
        debug!("single_shard_handler");
        let self_zone_label = crate::config::get_global_config().zone_label;
        let mut replica_read = replica_read;
        let region_leader = region.leader.clone();
        let leader_store_id = region_leader.as_ref().map(|peer| peer.store_id);
        let busy_threshold_ms = plan
            .kv_context_mut()
            .map(|ctx| ctx.busy_threshold_ms)
            .unwrap_or(0);
        let current_attempts = backoff.current_attempts();
        let unreachable_store_ids = match replica_read.as_ref() {
            Some(replica_read) => replica_read.unreachable_store_ids().await,
            None => Vec::new(),
        };
        let data_is_not_ready_store_ids = match replica_read.as_ref() {
            Some(replica_read) => replica_read.data_is_not_ready_store_ids().await,
            None => Vec::new(),
        };
        let mut patched_stale_read = false;
        let mut fallback_to_leader_under_busy_threshold = false;
        let mut force_replica_read = false;
        let mut stale_read_leader_fallback = false;
        if let Some(replica_read) = replica_read.as_ref() {
            let attempt = replica_read.attempt(current_attempts);
            let read_type = replica_read.read_type;
            let store_ids_configured = !match_store_ids.is_empty();
            let labels_configured = !match_store_labels.is_empty();
            let match_configured = store_ids_configured || labels_configured;
            let (labels_matched_store_ids, label_matched_store_ids) =
                if match_configured && read_type.is_follower_read() {
                    let mut labels_matched_store_ids = Vec::new();
                    let mut label_matched_store_ids = Vec::new();
                    for peer in region.region.peers.iter() {
                        let store_id = peer.store_id;
                        let store_id_matches =
                            !store_ids_configured || match_store_ids.contains(&store_id);
                        if store_ids_configured && !store_id_matches {
                            continue;
                        }
                        if !labels_configured {
                            if store_id_matches {
                                label_matched_store_ids.push(store_id);
                            }
                            continue;
                        }

                        match pd_client.store_meta_by_id(store_id).await {
                            Ok(store) => {
                                if store_labels_match(&store, match_store_labels.as_slice()) {
                                    labels_matched_store_ids.push(store_id);
                                    if store_id_matches {
                                        label_matched_store_ids.push(store_id);
                                    }
                                }
                            }
                            Err(err) => {
                                debug!("store_meta_by_id failed for store {}: {:?}", store_id, err);
                            }
                        }
                    }
                    (labels_matched_store_ids, label_matched_store_ids)
                } else {
                    (Vec::new(), Vec::new())
                };

            let mut can_send_replica_read = false;
            let stale_read_enabled = replica_read.stale_read_enabled();
            if stale_read_enabled {
                let leader_attempted = match leader_store_id {
                    Some(leader_store_id) => {
                        replica_read.has_attempted_store(leader_store_id).await
                    }
                    None => false,
                };
                can_send_replica_read = if let Some(leader_store_id) = leader_store_id {
                    leader_attempted
                        && !unreachable_store_ids.contains(&leader_store_id)
                        && !replica_read.is_store_server_is_busy(leader_store_id).await
                } else {
                    false
                };
                let leader_can_send_replica_read = attempt >= 1 && can_send_replica_read;

                if attempt == 1 && !leader_attempted {
                    // Align with client-go replica selector behavior:
                    // stale-read retries should fall back to normal snapshot reads on the leader.
                    if let Some(region_leader) = region_leader {
                        if let Some(ctx) = plan.kv_context_mut() {
                            ctx.stale_read = false;
                            ctx.replica_read = false;
                        }
                        region.leader = Some(region_leader);
                        stale_read_leader_fallback = true;
                    } else if let Some(ctx) = plan.kv_context_mut() {
                        ctx.stale_read = true;
                        ctx.replica_read = false;
                        patched_stale_read = true;
                    }
                } else if leader_can_send_replica_read {
                    if let Some(ctx) = plan.kv_context_mut() {
                        ctx.stale_read = false;
                        ctx.replica_read = false;
                    }
                    force_replica_read = true;
                } else if let Some(ctx) = plan.kv_context_mut() {
                    ctx.stale_read = true;
                    ctx.replica_read = false;
                    patched_stale_read = true;
                }
            }

            let leader_is_busy_under_threshold = if read_type == ReplicaReadType::Leader
                && busy_threshold_ms > 0
                && !stale_read_enabled
                && !replica_read.retry_same_replica
            {
                if let Some(leader_store_id) = leader_store_id {
                    let busy_threshold = Duration::from_millis(u64::from(busy_threshold_ms));
                    pd_client.store_estimated_wait_time(leader_store_id) > busy_threshold
                        || replica_read.is_store_server_is_busy(leader_store_id).await
                } else {
                    false
                }
            } else {
                false
            };

            if !stale_read_leader_fallback && leader_is_busy_under_threshold {
                // Align with client-go load-based replica read: when the leader is busy, try an
                // idle replica first.
                let server_is_busy_store_ids = replica_read.server_is_busy_store_ids().await;
                let busy_threshold = Duration::from_millis(u64::from(busy_threshold_ms));
                let mut unavailable_store_ids = unreachable_store_ids.clone();
                unavailable_store_ids.extend(server_is_busy_store_ids.iter().copied());
                for peer in region.region.peers.iter() {
                    if pd_client.store_estimated_wait_time(peer.store_id) > busy_threshold
                        && !unavailable_store_ids.contains(&peer.store_id)
                    {
                        unavailable_store_ids.push(peer.store_id);
                    }
                }
                if let Some(leader_store_id) = leader_store_id {
                    if !unavailable_store_ids.contains(&leader_store_id) {
                        unavailable_store_ids.push(leader_store_id);
                    }
                }

                let attempted_store_ids = replica_read.attempted_store_ids().await;
                if let Some(peer) = select_replica_read_peer(
                    &region,
                    ReplicaReadType::Mixed,
                    attempt,
                    &unavailable_store_ids,
                    &server_is_busy_store_ids,
                    &attempted_store_ids,
                    &data_is_not_ready_store_ids,
                    false,
                    false,
                    &[],
                ) {
                    region.leader = Some(peer);
                }

                if region.leader.as_ref().map(|peer| peer.store_id) == leader_store_id {
                    fallback_to_leader_under_busy_threshold = true;
                }
            } else if !stale_read_leader_fallback && read_type.is_follower_read() {
                let server_is_busy_store_ids = if replica_read.retry_same_replica {
                    Vec::new()
                } else {
                    replica_read.server_is_busy_store_ids().await
                };
                let mut busy_store_ids = server_is_busy_store_ids.clone();
                if !replica_read.retry_same_replica {
                    for peer in region.region.peers.iter() {
                        if pd_client.is_store_slow(peer.store_id)
                            && !busy_store_ids.contains(&peer.store_id)
                        {
                            busy_store_ids.push(peer.store_id);
                        }
                    }
                }
                // When busy-threshold is enabled, we exclude stores that are explicitly busy
                // (`ServerIsBusy` or high estimated wait) from selection. Slow-store TTL is only a
                // best-effort signal and should not hard-exclude replicas under busy-threshold.
                let mut unavailable_busy_store_ids = server_is_busy_store_ids;
                if busy_threshold_ms > 0
                    && read_type == ReplicaReadType::Mixed
                    && !stale_read_enabled
                    && !replica_read.retry_same_replica
                {
                    let busy_threshold = Duration::from_millis(u64::from(busy_threshold_ms));
                    for peer in region.region.peers.iter() {
                        if pd_client.store_estimated_wait_time(peer.store_id) > busy_threshold {
                            if !unavailable_busy_store_ids.contains(&peer.store_id) {
                                unavailable_busy_store_ids.push(peer.store_id);
                            }
                            if !busy_store_ids.contains(&peer.store_id) {
                                busy_store_ids.push(peer.store_id);
                            }
                        }
                    }
                }
                let attempted_store_ids = if replica_read.retry_same_replica {
                    Vec::new()
                } else {
                    replica_read.attempted_store_ids().await
                };
                let mut unavailable_store_ids = unreachable_store_ids.clone();
                if busy_threshold_ms > 0
                    && read_type == ReplicaReadType::Mixed
                    && !stale_read_enabled
                {
                    unavailable_store_ids.extend(unavailable_busy_store_ids.iter().copied());
                    if let Some(leader_store_id) = leader_store_id {
                        unavailable_store_ids.push(leader_store_id);
                    }
                }
                if force_replica_read {
                    if let Some(leader_store_id) = leader_store_id {
                        if !unavailable_store_ids.contains(&leader_store_id) {
                            unavailable_store_ids.push(leader_store_id);
                        }
                    }
                }

                if let Some(peer) = select_replica_read_peer(
                    &region,
                    read_type,
                    attempt,
                    &unavailable_store_ids,
                    &busy_store_ids,
                    &attempted_store_ids,
                    &data_is_not_ready_store_ids,
                    match_configured,
                    labels_configured,
                    &label_matched_store_ids,
                ) {
                    region.leader = Some(peer);
                }

                if patched_stale_read && attempt == 0 && labels_configured && can_send_replica_read
                {
                    let target_store_id = region.leader.as_ref().map(|peer| peer.store_id);
                    let is_leader_target = target_store_id == leader_store_id;
                    let target_labels_match = target_store_id
                        .map(|store_id| labels_matched_store_ids.contains(&store_id))
                        .unwrap_or(true);
                    if !is_leader_target && !target_labels_match {
                        // Match client-go stale read behavior when label matching is enabled: if
                        // the chosen target does not match labels (and is not the leader), prefer
                        // replica-read when the leader is healthy enough to flip.
                        if let Some(ctx) = plan.kv_context_mut() {
                            ctx.stale_read = false;
                            ctx.replica_read = false;
                        }
                        patched_stale_read = false;
                    }
                }
            }

            if busy_threshold_ms > 0
                && read_type == ReplicaReadType::Mixed
                && !stale_read_enabled
                && region.leader.as_ref().map(|peer| peer.store_id) == leader_store_id
            {
                fallback_to_leader_under_busy_threshold = true;
            }
        }
        if fallback_to_leader_under_busy_threshold {
            // Match client-go behavior for load-based replica read: when all replicas are too busy,
            // remove `busy_threshold_ms` and fall back to leader reads.
            if let Some(ctx) = plan.kv_context_mut() {
                ctx.busy_threshold_ms = 0;
            }
            replica_read = replica_read
                .map(|state| state.switch_to(ReplicaReadType::Leader, backoff.current_attempts()));
        }
        let replica_read_type = replica_read.as_ref().map(|state| state.read_type);
        let region_store = match pd_client
            .clone()
            .map_region_to_store(region)
            .await
            .and_then(|region_store| {
                plan.apply_store(&region_store)?;
                if let Some(replica_read_type) = replica_read_type {
                    if let Some(ctx) = plan.kv_context_mut() {
                        adjust_replica_read_flag(ctx, leader_store_id, replica_read_type);
                    }
                }
                Ok(region_store)
            }) {
            Ok(region_store) => region_store,
            Err(Error::LeaderNotFound { region }) => {
                debug!(
                    "single_shard_handler::sharding: leader not found: {:?}",
                    region
                );
                return Self::handle_other_error(
                    pd_client,
                    plan,
                    region.clone(),
                    None,
                    backoff,
                    killed.clone(),
                    permits,
                    preserve_region_results,
                    replica_read,
                    match_store_ids,
                    match_store_labels,
                    txn_regions_num_observer,
                    batch_executor_token_wait_nanos.is_some(),
                    stale_read,
                    false,
                    Error::LeaderNotFound { region },
                )
                .await;
            }
            Err(err) => {
                debug!("single_shard_handler::sharding, error: {:?}", err);
                return Err(err);
            }
        };

        if let Some(replica_read) = replica_read.as_ref() {
            if let Ok(store_id) = region_store.region_with_leader.get_store_id() {
                replica_read.mark_store_attempted(store_id).await;
            }
        }

        // limit concurrent requests
        let (is_mpp, is_cross_zone) = match region_store.region_with_leader.get_store_id() {
            Ok(store_id) => match pd_client.store_meta_by_id(store_id).await {
                Ok(store) => (
                    crate::region_cache::is_tiflash_related_store(&store),
                    is_cross_zone(self_zone_label.as_deref(), &store),
                ),
                Err(Error::Unimplemented) => (false, false),
                Err(err) => {
                    debug!(
                        "single_shard_handler: traffic classification skipped: {:?}",
                        err
                    );
                    (false, false)
                }
            },
            Err(_) => (false, false),
        };
        let permit_wait_started_at = batch_executor_token_wait_nanos.is_some().then(Instant::now);
        let permit = permits.acquire().await.map_err(|_| Error::InternalError {
            message: "request concurrency semaphore closed".to_owned(),
        })?;
        if let (Some(permit_wait_started_at), Some(batch_executor_token_wait_nanos)) = (
            permit_wait_started_at,
            batch_executor_token_wait_nanos.as_ref(),
        ) {
            let waited = permit_wait_started_at.elapsed();
            let waited_nanos = u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX);
            batch_executor_token_wait_nanos.fetch_add(waited_nanos, Ordering::Relaxed);
        }
        let res = crate::util::scope_task_traffic_kind(is_mpp, is_cross_zone, plan.execute()).await;
        drop(permit);

        if patched_stale_read {
            if let (Some(replica_read), Some(ctx)) = (replica_read.as_ref(), plan.kv_context_mut())
            {
                if !ctx.stale_read {
                    replica_read.disable_stale_read();
                }
            }
        }

        let mut resp = match res {
            Ok(resp) => resp,
            Err(e) if is_grpc_error(&e) => {
                inc_async_send_req_total("rpc_error");
                let is_transient_failure = match &e {
                    Error::Grpc(_) => true,
                    Error::GrpcAPI(status) if status.code() == tonic::Code::Unavailable => true,
                    _ => false,
                };
                if is_transient_failure {
                    if let Ok(store_id) = region_store.region_with_leader.get_store_id() {
                        inc_connection_transient_failure_count(
                            &region_store.store_address,
                            store_id,
                        );
                    }
                }
                debug!("single_shard_handler:execute: grpc error: {:?}", e);
                return Self::handle_other_error(
                    pd_client,
                    plan,
                    region_store.region_with_leader.ver_id(),
                    region_store.region_with_leader.get_store_id().ok(),
                    backoff,
                    killed.clone(),
                    permits,
                    preserve_region_results,
                    replica_read,
                    match_store_ids,
                    match_store_labels,
                    txn_regions_num_observer,
                    batch_executor_token_wait_nanos.is_some(),
                    stale_read,
                    true,
                    e,
                )
                .await;
            }
            Err(e) => {
                let result_label = match &e {
                    Error::InternalError { .. } => "other_error",
                    _ => "send_error",
                };
                inc_async_send_req_total(result_label);
                debug!("single_shard_handler:execute: error: {:?}", e);
                let retry_times = backoff.current_attempts();
                observe_request_retry_times(retry_times);
                observe_stale_read_hit_miss(stale_read, retry_times);
                return Err(e);
            }
        };

        if let Some(e) = resp.key_errors() {
            inc_async_send_req_total("ok");
            debug!("single_shard_handler:execute: key errors: {:?}", e);
            let retry_times = backoff.current_attempts();
            observe_request_retry_times(retry_times);
            observe_stale_read_hit_miss(stale_read, retry_times);
            Ok(vec![Err(collapse_key_errors(e))])
        } else if let Some(e) = resp.region_error() {
            inc_async_send_req_total("region_error");
            debug!("single_shard_handler:execute: region error: {:?}", e);
            let is_server_busy = e.server_is_busy.is_some();
            let is_data_is_not_ready = e.data_is_not_ready.is_some();
            let server_is_busy_estimated_wait_ms = e
                .server_is_busy
                .as_ref()
                .map(|busy| busy.estimated_wait_ms)
                .unwrap_or(0);
            let is_stale_read_leader_fallback_attempt = stale_read_leader_fallback;
            let is_stale_read_request = plan
                .kv_context_mut()
                .map(|ctx| ctx.stale_read)
                .unwrap_or(false);
            let busy_threshold_ms = plan
                .kv_context_mut()
                .map(|ctx| ctx.busy_threshold_ms)
                .unwrap_or(0);
            let max_execution_duration_ms = plan
                .kv_context_mut()
                .map(|ctx| ctx.max_execution_duration_ms)
                .unwrap_or(0);
            let is_deadline_exceeded =
                max_execution_duration_ms > 0 && is_deadline_exceeded_region_error(&e);
            let retry_same_replica = !is_stale_read_request
                && !is_stale_read_leader_fallback_attempt
                && ((is_server_busy && busy_threshold_ms == 0)
                    || e.max_timestamp_not_synced.is_some()
                    || e.read_index_not_ready.is_some()
                    || e.proposal_in_merging_mode.is_some());
            match backoff.next_delay_duration() {
                Some(duration) => {
                    let store_id = region_store.region_with_leader.get_store_id().ok();
                    if is_deadline_exceeded {
                        // Match client-go `deadlineErrUsingConfTimeoutFlag` behavior by treating
                        // deadline-exceeded stores as temporarily unavailable for replica-read
                        // selection and stale-read replica-read flips.
                        if let (Some(store_id), Some(replica_read)) =
                            (store_id, replica_read.as_ref())
                        {
                            replica_read.mark_store_unreachable(store_id).await;
                        }
                    }
                    if is_server_busy {
                        if let Some(store_id) = store_id {
                            pd_client.update_store_load_stats(
                                store_id,
                                server_is_busy_estimated_wait_ms,
                            );
                            pd_client.mark_store_slow(store_id, SLOW_STORE_TTL_ON_SERVER_IS_BUSY);
                        }
                        if let (Some(store_id), Some(replica_read)) =
                            (store_id, replica_read.as_ref())
                        {
                            replica_read.mark_store_server_is_busy(store_id).await;
                        }
                    }
                    if is_data_is_not_ready {
                        if let (Some(store_id), Some(replica_read)) =
                            (store_id, replica_read.as_ref())
                        {
                            replica_read.mark_store_data_is_not_ready(store_id).await;
                        }
                    }
                    let region_error_resolved =
                        handle_region_error(pd_client.clone(), e, region_store).await?;
                    // don't sleep if we have resolved the region error
                    if !region_error_resolved {
                        check_killed(&killed)?;
                        observe_backoff_seconds("region", duration);
                        crate::util::record_task_local_backoff(duration);
                        if let Some(stats) = plan.runtime_stats() {
                            stats.record_backoff("region", duration);
                        }
                        sleep(duration).await;
                    }
                    let replica_read = match (is_server_busy, replica_read) {
                        (true, Some(state)) if state.read_type == ReplicaReadType::PreferLeader => {
                            Some(
                                state.switch_to(ReplicaReadType::Mixed, backoff.current_attempts()),
                            )
                        }
                        (true, Some(state))
                            if state.read_type == ReplicaReadType::Leader
                                && busy_threshold_ms > 0 =>
                        {
                            Some(
                                state.switch_to(ReplicaReadType::Mixed, backoff.current_attempts()),
                            )
                        }
                        (_, replica_read) => replica_read,
                    };
                    let replica_read = match (is_deadline_exceeded, replica_read) {
                        (true, Some(state)) if state.read_type == ReplicaReadType::Leader => Some(
                            state.switch_to(ReplicaReadType::Mixed, backoff.current_attempts()),
                        ),
                        (_, replica_read) => replica_read,
                    };
                    let replica_read = if retry_same_replica {
                        replica_read.map(ReplicaReadState::keep_current_attempt_on_retry)
                    } else {
                        replica_read.map(ReplicaReadState::clear_retry_same_replica)
                    };
                    Self::single_plan_handler(
                        pd_client,
                        plan,
                        backoff,
                        killed,
                        permits,
                        preserve_region_results,
                        stale_read,
                        replica_read,
                        match_store_ids,
                        match_store_labels,
                        txn_regions_num_observer,
                        batch_executor_token_wait_nanos.is_some(),
                    )
                    .await
                }
                None => {
                    let retry_times = backoff.current_attempts();
                    observe_request_retry_times(retry_times);
                    observe_stale_read_hit_miss(stale_read, retry_times);
                    Err(Error::RegionError(Box::new(e)))
                }
            }
        } else {
            inc_async_send_req_total("ok");
            let retry_times = backoff.current_attempts();
            observe_request_retry_times(retry_times);
            observe_stale_read_hit_miss(stale_read, retry_times);
            Ok(vec![Ok(resp)])
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_other_error(
        pd_client: Arc<PdC>,
        plan: P,
        region: RegionVerId,
        store: Option<StoreId>,
        mut backoff: Backoff,
        killed: Option<Arc<AtomicU32>>,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        replica_read: Option<ReplicaReadState>,
        match_store_ids: Arc<Vec<u64>>,
        match_store_labels: Arc<Vec<StoreLabel>>,
        txn_regions_num_observer: Option<(&'static str, bool)>,
        batch_executor_token_wait_observer: bool,
        stale_read: bool,
        executed: bool,
        e: Error,
    ) -> Result<<Self as Plan>::Result> {
        debug!("handle_other_error: {:?}", e);
        let is_grpc_error = is_grpc_error(&e);
        let mut replica_read = replica_read;
        replica_read = replica_read.map(ReplicaReadState::clear_retry_same_replica);
        if is_grpc_error {
            if let (Some(store_id), true) = (store, is_grpc_deadline_exceeded(&e)) {
                pd_client.mark_store_slow(store_id, SLOW_STORE_TTL_ON_GRPC_DEADLINE_EXCEEDED);
            }
            if let (Some(store_id), Some(replica_read)) = (store, replica_read.as_ref()) {
                replica_read.mark_store_unreachable(store_id).await;
            }
            if let Some(store_id) = store {
                pd_client.invalidate_store_cache(store_id).await;
            }
        } else {
            pd_client.invalidate_region_cache(region).await;
        }
        match backoff.next_delay_duration() {
            Some(duration) => {
                if is_grpc_error {
                    replica_read = match replica_read {
                        Some(state) if state.read_type == ReplicaReadType::PreferLeader => Some(
                            state.switch_to(ReplicaReadType::Mixed, backoff.current_attempts()),
                        ),
                        _ => replica_read,
                    };
                }
                check_killed(&killed)?;
                let label = if is_grpc_error { "grpc" } else { "region" };
                observe_backoff_seconds(label, duration);
                crate::util::record_task_local_backoff(duration);
                if let Some(stats) = plan.runtime_stats() {
                    stats.record_backoff(label, duration);
                }
                sleep(duration).await;
                Self::single_plan_handler(
                    pd_client,
                    plan,
                    backoff,
                    killed,
                    permits,
                    preserve_region_results,
                    stale_read,
                    replica_read,
                    match_store_ids,
                    match_store_labels,
                    txn_regions_num_observer,
                    batch_executor_token_wait_observer,
                )
                .await
            }
            None => {
                if executed {
                    let retry_times = backoff.current_attempts();
                    observe_request_retry_times(retry_times);
                    observe_stale_read_hit_miss(stale_read, retry_times);
                }
                Err(e)
            }
        }
    }
}

fn adjust_replica_read_flag(
    ctx: &mut kvrpcpb::Context,
    leader_store_id: Option<StoreId>,
    replica_read: ReplicaReadType,
) {
    if ctx.stale_read {
        ctx.replica_read = false;
        return;
    }

    let (Some(leader_store_id), Some(peer_store_id)) =
        (leader_store_id, ctx.peer.as_ref().map(|peer| peer.store_id))
    else {
        return;
    };

    ctx.replica_read = match replica_read {
        ReplicaReadType::Leader => ctx.busy_threshold_ms > 0 && peer_store_id != leader_store_id,
        _ if replica_read.is_follower_read() => peer_store_id != leader_store_id,
        _ => false,
    };
}

// Returns
// 1. Ok(true): error has been resolved, retry immediately
// 2. Ok(false): backoff, and then retry
// 3. Err(Error): can't be resolved, return the error to upper level
pub(crate) async fn handle_region_error<PdC: PdClient>(
    pd_client: Arc<PdC>,
    mut e: errorpb::Error,
    region_store: RegionStore,
) -> Result<bool> {
    debug!("handle_region_error: {:?}", e);
    let ver_id = region_store.region_with_leader.ver_id();
    let store_id = region_store.region_with_leader.get_store_id();
    if let Some(not_leader) = e.not_leader {
        if let Some(leader) = not_leader.leader {
            match pd_client
                .update_leader(region_store.region_with_leader.ver_id(), leader)
                .await
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    pd_client.invalidate_region_cache(ver_id).await;
                    Err(e)
                }
            }
        } else {
            // The peer doesn't know who is the current leader. Generally it's because
            // the Raft group is in an election, but it's possible that the peer is
            // isolated and removed from the Raft group. So it's necessary to reload
            // the region from PD.
            pd_client.invalidate_region_cache(ver_id).await;
            Ok(false)
        }
    } else if e.store_not_match.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        if let Ok(store_id) = store_id {
            pd_client.invalidate_store_cache(store_id).await;
        }
        Ok(false)
    } else if e.disk_full.is_some() {
        // Match client-go `RegionRequestSender.onRegionError`: disk-full is treated as retryable
        // with backoff.
        Ok(false)
    } else if let Some(epoch_not_match) = e.epoch_not_match.take() {
        on_region_epoch_not_match(pd_client.clone(), region_store, epoch_not_match).await
    } else if e.stale_command.is_some()
        || e.region_not_found.is_some()
        || e.key_not_in_region.is_some()
    {
        pd_client.invalidate_region_cache(ver_id).await;
        Ok(false)
    } else if e.data_is_not_ready.is_some() {
        // Specific to stale read. The target replica is randomly selected and may not have caught up
        // yet. Retry immediately.
        Ok(true)
    } else if is_deadline_exceeded_region_error(&e) {
        // Match client-go `RegionRequestSender.onRegionError` configurable-timeout fast retry
        // behavior: do not invalidate caches or backoff for deadline-exceeded region errors.
        Ok(true)
    } else if e.server_is_busy.is_some()
        || e.max_timestamp_not_synced.is_some()
        || e.read_index_not_ready.is_some()
        || e.proposal_in_merging_mode.is_some()
        || e.region_not_initialized.is_some()
    {
        Ok(false)
    } else if e.raft_entry_too_large.is_some() {
        Err(Error::RegionError(Box::new(e)))
    } else if e.recovery_in_progress.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        Ok(false)
    } else if e.flashback_in_progress.is_some() || e.flashback_not_prepared.is_some() {
        Err(Error::RegionError(Box::new(e)))
    } else if e.is_witness.is_some()
        || e.mismatch_peer_id.is_some()
        || e.bucket_version_not_match.is_some()
    {
        pd_client.invalidate_region_cache(ver_id).await;
        Ok(false)
    } else if is_invalid_max_ts_update_region_error(&e) {
        // Match client-go `isInvalidMaxTsUpdate`: fail fast without invalidating caches.
        Err(Error::RegionError(Box::new(e)))
    } else {
        info!("unknown region error: {:?}", e);
        pd_client.invalidate_region_cache(ver_id).await;
        if let Ok(store_id) = store_id {
            pd_client.invalidate_store_cache(store_id).await;
        }
        Ok(false)
    }
}

// Returns
// 1. Ok(true): error has been resolved, retry immediately
// 2. Ok(false): backoff, and then retry
// 3. Err(Error): can't be resolved, return the error to upper level
pub(crate) async fn on_region_epoch_not_match<PdC: PdClient>(
    pd_client: Arc<PdC>,
    region_store: RegionStore,
    error: EpochNotMatch,
) -> Result<bool> {
    let ver_id = region_store.region_with_leader.ver_id();
    if error.current_regions.is_empty() {
        pd_client.invalidate_region_cache(ver_id).await;
        return Ok(false);
    }

    let current_conf_ver = ver_id.conf_ver;
    let current_version = ver_id.ver;
    for r in &error.current_regions {
        if r.id == region_store.region_with_leader.id() {
            let Some(region_epoch) = r.region_epoch.as_ref() else {
                pd_client.invalidate_region_cache(ver_id).await;
                return Ok(false);
            };
            let returned_conf_ver = region_epoch.conf_ver;
            let returned_version = region_epoch.version;

            // Find whether the current region is ahead of TiKV's. If so, backoff.
            if returned_conf_ver < current_conf_ver || returned_version < current_version {
                return Ok(false);
            }
            break;
        }
    }

    let init_leader_store_id = region_store.region_with_leader.get_store_id().ok();
    let mut need_invalidate_old = true;
    for meta in error.current_regions {
        let leader = init_leader_store_id
            .and_then(|store_id| meta.peers.iter().find(|peer| peer.store_id == store_id))
            .or_else(|| meta.peers.first())
            .cloned();
        let region = RegionWithLeader {
            region: meta,
            leader,
        };
        if region.ver_id() == ver_id {
            need_invalidate_old = false;
        }
        pd_client.add_region_to_cache(region).await;
    }
    if need_invalidate_old {
        pd_client.invalidate_region_cache(ver_id).await;
    }

    Ok(true)
}

impl<P: Plan, PdC: PdClient> Clone for RetryableMultiRegion<P, PdC> {
    fn clone(&self) -> Self {
        RetryableMultiRegion {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            killed: self.killed.clone(),
            concurrency: self.concurrency,
            txn_regions_num_observer: self.txn_regions_num_observer,
            batch_executor_token_wait_observer: self.batch_executor_token_wait_observer,
            preserve_region_results: self.preserve_region_results,
            replica_read: self.replica_read,
            match_store_ids: self.match_store_ids.clone(),
            match_store_labels: self.match_store_labels.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable + HasKvContext + HasRequestLabel, PdC: PdClient> Plan
    for RetryableMultiRegion<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    type Result = Vec<Result<P::Result>>;

    async fn execute(&self) -> Result<Self::Result> {
        // Limit the maximum concurrency of multi-region request. If there are
        // too many concurrent requests, TiKV is more likely to return a "TiKV
        // is busy" error
        if self.concurrency == 0 {
            return Err(Error::InternalError {
                message: "multi-region request concurrency must be greater than 0".to_owned(),
            });
        }
        let concurrency_permits = Arc::new(Semaphore::new(self.concurrency));
        let mut inner = self.inner.clone();
        let stale_read = inner
            .kv_context_mut()
            .map(|ctx| ctx.stale_read)
            .unwrap_or(false);
        Self::single_plan_handler(
            self.pd_client.clone(),
            inner,
            self.backoff.clone(),
            self.killed.clone(),
            concurrency_permits.clone(),
            self.preserve_region_results,
            stale_read,
            self.replica_read
                .map(|read_type| ReplicaReadState::new(read_type, stale_read)),
            self.match_store_ids.clone(),
            self.match_store_labels.clone(),
            self.txn_regions_num_observer,
            self.batch_executor_token_wait_observer,
        )
        .await
    }

    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.inner.runtime_stats()
    }
}

pub struct RetryableAllStores<P: Plan, PdC: PdClient> {
    pub(super) inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
}

impl<P: Plan, PdC: PdClient> Clone for RetryableAllStores<P, PdC> {
    fn clone(&self) -> Self {
        RetryableAllStores {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
        }
    }
}

// About `HasRegionError`:
// Store requests should be return region errors.
// But as the response of only store request by now (UnsafeDestroyRangeResponse) has the `region_error` field,
// we require `HasRegionError` to check whether there is region error returned from TiKV.
#[async_trait]
impl<P: Plan + StoreRequest, PdC: PdClient> Plan for RetryableAllStores<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    type Result = Vec<Result<P::Result>>;

    async fn execute(&self) -> Result<Self::Result> {
        let self_zone_label = crate::config::get_global_config().zone_label;
        let concurrency_permits = Arc::new(Semaphore::new(MULTI_STORES_CONCURRENCY));
        let stores = self.pd_client.clone().all_stores().await?;
        let mut handles = Vec::with_capacity(stores.len());
        for store in stores {
            let mut clone = self.inner.clone();
            clone.apply_store(&store);
            let is_mpp = crate::region_cache::is_tiflash_related_store(&store.meta);
            let cross_zone = is_cross_zone(self_zone_label.as_deref(), &store.meta);
            let handle = crate::util::spawn_with_inherited_task_locals(Self::single_store_handler(
                clone,
                self.backoff.clone(),
                concurrency_permits.clone(),
                is_mpp,
                cross_zone,
            ));
            handles.push(handle);
        }
        let results = try_join_all(handles).await?;
        Ok(results.into_iter().collect::<Vec<_>>())
    }

    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.inner.runtime_stats()
    }
}

impl<P: Plan, PdC: PdClient> RetryableAllStores<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    async fn single_store_handler(
        plan: P,
        mut backoff: Backoff,
        permits: Arc<Semaphore>,
        is_mpp: bool,
        is_cross_zone: bool,
    ) -> Result<P::Result> {
        loop {
            let permit = permits.acquire().await.map_err(|_| Error::InternalError {
                message: "store request concurrency semaphore closed".to_owned(),
            })?;
            let res =
                crate::util::scope_task_traffic_kind(is_mpp, is_cross_zone, plan.execute()).await;
            drop(permit);

            match res {
                Ok(mut resp) => {
                    if let Some(e) = resp.key_errors() {
                        return Err(collapse_key_errors(e));
                    } else if let Some(e) = resp.region_error() {
                        // Store request should not return region error.
                        return Err(Error::RegionError(Box::new(e)));
                    } else {
                        return Ok(resp);
                    }
                }
                Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                    Some(duration) => {
                        observe_backoff_seconds("grpc", duration);
                        crate::util::record_task_local_backoff(duration);
                        if let Some(stats) = plan.runtime_stats() {
                            stats.record_backoff("grpc", duration);
                        }
                        sleep(duration).await;
                        continue;
                    }
                    None => return Err(e),
                },
                Err(e) => return Err(e),
            }
        }
    }
}

pub struct RetryableStores<P: Plan> {
    pub(super) inner: P,
    pub stores: Arc<Vec<Store>>,
    pub backoff: Backoff,
}

impl<P: Plan> Clone for RetryableStores<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            stores: self.stores.clone(),
            backoff: self.backoff.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan + StoreRequest> Plan for RetryableStores<P>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    type Result = Vec<Result<P::Result>>;

    async fn execute(&self) -> Result<Self::Result> {
        let self_zone_label = crate::config::get_global_config().zone_label;
        let concurrency_permits = Arc::new(Semaphore::new(MULTI_STORES_CONCURRENCY));
        let mut handles = Vec::with_capacity(self.stores.len());
        for store in self.stores.iter() {
            let mut clone = self.inner.clone();
            clone.apply_store(store);
            let is_mpp = crate::region_cache::is_tiflash_related_store(&store.meta);
            let cross_zone = is_cross_zone(self_zone_label.as_deref(), &store.meta);
            let handle = crate::util::spawn_with_inherited_task_locals(Self::single_store_handler(
                clone,
                self.backoff.clone(),
                concurrency_permits.clone(),
                is_mpp,
                cross_zone,
            ));
            handles.push(handle);
        }
        let results = try_join_all(handles).await?;
        Ok(results.into_iter().collect::<Vec<_>>())
    }

    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.inner.runtime_stats()
    }
}

impl<P: Plan> RetryableStores<P>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    async fn single_store_handler(
        plan: P,
        mut backoff: Backoff,
        permits: Arc<Semaphore>,
        is_mpp: bool,
        is_cross_zone: bool,
    ) -> Result<P::Result> {
        loop {
            let permit = permits.acquire().await.map_err(|_| Error::InternalError {
                message: "store request concurrency semaphore closed".to_owned(),
            })?;
            let res =
                crate::util::scope_task_traffic_kind(is_mpp, is_cross_zone, plan.execute()).await;
            drop(permit);

            match res {
                Ok(mut resp) => {
                    if let Some(e) = resp.key_errors() {
                        return Err(collapse_key_errors(e));
                    } else if let Some(e) = resp.region_error() {
                        // Store request should not return region error.
                        return Err(Error::RegionError(Box::new(e)));
                    } else {
                        return Ok(resp);
                    }
                }
                Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                    Some(duration) => {
                        observe_backoff_seconds("grpc", duration);
                        crate::util::record_task_local_backoff(duration);
                        if let Some(stats) = plan.runtime_stats() {
                            stats.record_backoff("grpc", duration);
                        }
                        sleep(duration).await;
                        continue;
                    }
                    None => return Err(e),
                },
                Err(e) => return Err(e),
            }
        }
    }
}

/// A technique for merging responses into a single result (with type `Out`).
pub trait Merge<In>: Sized + Clone + Send + Sync + 'static {
    type Out: Send;

    fn merge(&self, input: Vec<Result<In>>) -> Result<Self::Out>;
}

#[derive(Clone)]
pub struct MergeResponse<P: Plan, In, M: Merge<In>> {
    pub inner: P,
    pub merge: M,
    pub phantom: PhantomData<In>,
}

#[async_trait]
impl<In: Clone + Send + Sync + 'static, P: Plan<Result = Vec<Result<In>>>, M: Merge<In>> Plan
    for MergeResponse<P, In, M>
{
    type Result = M::Out;

    async fn execute(&self) -> Result<Self::Result> {
        self.merge.merge(self.inner.execute().await?)
    }
}

/// A merge strategy which collects data from a response into a single type.
#[derive(Clone, Copy)]
pub struct Collect;

/// A merge strategy that only takes the first element. It's used for requests
/// that should have exactly one response, e.g. a get request.
#[derive(Clone, Copy)]
pub struct CollectSingle;

#[doc(hidden)]
#[macro_export]
macro_rules! collect_single {
    ($type_: ty) => {
        impl $crate::request::Merge<$type_> for $crate::request::CollectSingle {
            type Out = $type_;

            fn merge(&self, mut input: Vec<$crate::Result<$type_>>) -> $crate::Result<Self::Out> {
                if input.len() != 1 {
                    return Err($crate::Error::InternalError {
                        message: format!("expected a single response, got {}", input.len()),
                    });
                }

                input.pop().ok_or_else(|| $crate::Error::InternalError {
                    message: "expected a single response".to_owned(),
                })?
            }
        }
    };
}

/// A merge strategy to be used with
/// [`preserve_shard`](super::plan_builder::PlanBuilder::preserve_shard).
/// It matches the shards preserved before and the values returned in the response.
#[derive(Clone, Debug)]
pub struct CollectWithShard;

/// A merge strategy which returns an error if any response is an error and
/// otherwise returns a Vec of the results.
#[derive(Clone, Copy)]
pub struct CollectError;

impl<T: Send> Merge<T> for CollectError {
    type Out = Vec<T>;

    fn merge(&self, input: Vec<Result<T>>) -> Result<Self::Out> {
        input.into_iter().collect()
    }
}

/// Process data into another kind of data.
pub trait Process<In>: Sized + Clone + Send + Sync + 'static {
    type Out: Send;

    fn process(&self, input: Result<In>) -> Result<Self::Out>;
}

#[derive(Clone)]
pub struct ProcessResponse<P: Plan, Pr: Process<P::Result>> {
    pub inner: P,
    pub processor: Pr,
}

#[async_trait]
impl<P: Plan, Pr: Process<P::Result>> Plan for ProcessResponse<P, Pr> {
    type Result = Pr::Out;

    async fn execute(&self) -> Result<Self::Result> {
        self.processor.process(self.inner.execute().await)
    }

    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.inner.runtime_stats()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DefaultProcessor;

pub struct ResolveLock<P: Plan, PdC: PdClient> {
    pub inner: P,
    pub timestamp: Timestamp,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
    pub keyspace: Keyspace,
    pub pessimistic_region_resolve: bool,
}

impl<P: Plan, PdC: PdClient> Clone for ResolveLock<P, PdC> {
    fn clone(&self) -> Self {
        ResolveLock {
            inner: self.inner.clone(),
            timestamp: self.timestamp.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            keyspace: self.keyspace,
            pessimistic_region_resolve: self.pessimistic_region_resolve,
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable, PdC: PdClient> Plan for ResolveLock<P, PdC>
where
    P::Result: HasLocks,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut plan = self.inner.clone();
        let mut result = plan.execute().await?;
        let mut backoff = self.backoff.clone();
        let ctx = ResolveLocksContext::default();
        let mut forced_leader = false;
        loop {
            let locks = result.take_locks();
            if locks.is_empty() {
                return Ok(result);
            }

            if backoff.is_none() {
                return Err(Error::ResolveLockError(locks));
            }

            if !forced_leader {
                if let Some(lock_key) = locks.first().map(|lock| lock.key.clone()) {
                    // Once we meet a lock, retrying against a follower/learner can keep seeing stale
                    // state. Align with client-go behavior by retrying on the region leader.
                    let region = self.pd_client.region_for_key(&Key::from(lock_key)).await?;
                    let region_store = self.pd_client.clone().map_region_to_store(region).await?;
                    plan.apply_store(&region_store)?;
                    forced_leader = true;
                }
            }

            let resolve_result: crate::transaction::ResolveLocksResult =
                resolve_locks_with_options(
                    ctx.clone(),
                    locks,
                    self.timestamp.clone(),
                    self.pd_client.clone(),
                    self.keyspace,
                    self.pessimistic_region_resolve,
                    backoff.clone(),
                    None,
                    LockResolverRpcContext::default(),
                )
                .await?;
            let ms_before_txn_expired = resolve_result.ms_before_txn_expired;
            let live_locks = resolve_result.live_locks;
            if live_locks.is_empty() {
                result = plan.execute().await?;
            } else {
                match backoff.next_delay_duration() {
                    None => return Err(Error::ResolveLockError(live_locks)),
                    Some(delay_duration) => {
                        let delay_duration = if ms_before_txn_expired > 0 {
                            delay_duration.min(Duration::from_millis(ms_before_txn_expired as u64))
                        } else {
                            delay_duration
                        };
                        observe_backoff_seconds("txnLockFast", delay_duration);
                        crate::util::record_task_local_backoff(delay_duration);
                        if let Some(stats) = plan.runtime_stats() {
                            stats.record_backoff("txnLockFast", delay_duration);
                        }
                        sleep(delay_duration).await;
                        result = plan.execute().await?;
                    }
                }
            }
        }
    }
}

pub(crate) struct ResolveLockInContext<P: Plan, PdC: PdClient> {
    pub(crate) inner: P,
    pub(crate) ctx: ResolveLocksContext,
    pub(crate) timestamp: Timestamp,
    pub(crate) pd_client: Arc<PdC>,
    pub(crate) backoff: Backoff,
    pub(crate) killed: Option<Arc<AtomicU32>>,
    pub(crate) keyspace: Keyspace,
    pub(crate) pessimistic_region_resolve: bool,
    pub(crate) rpc_context: LockResolverRpcContext,
}

impl<P: Plan, PdC: PdClient> Clone for ResolveLockInContext<P, PdC> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            ctx: self.ctx.clone(),
            timestamp: self.timestamp.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            killed: self.killed.clone(),
            keyspace: self.keyspace,
            pessimistic_region_resolve: self.pessimistic_region_resolve,
            rpc_context: self.rpc_context.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable, PdC: PdClient> Plan for ResolveLockInContext<P, PdC>
where
    P::Result: HasLocks,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut plan = self.inner.clone();
        let mut result = plan.execute().await?;
        let mut backoff = self.backoff.clone();
        let mut forced_leader = false;
        loop {
            let locks = result.take_locks();
            if locks.is_empty() {
                return Ok(result);
            }

            if backoff.is_none() {
                return Err(Error::ResolveLockError(locks));
            }

            if !forced_leader {
                if let Some(lock_key) = locks.first().map(|lock| lock.key.clone()) {
                    // Once we meet a lock, retrying against a follower/learner can keep seeing stale
                    // state. Align with client-go behavior by retrying on the region leader.
                    let region = self.pd_client.region_for_key(&Key::from(lock_key)).await?;
                    let region_store = self.pd_client.clone().map_region_to_store(region).await?;
                    plan.apply_store(&region_store)?;
                    forced_leader = true;
                }
            }

            let resolve_result: crate::transaction::ResolveLocksResult =
                resolve_locks_with_options(
                    self.ctx.clone(),
                    locks,
                    self.timestamp.clone(),
                    self.pd_client.clone(),
                    self.keyspace,
                    self.pessimistic_region_resolve,
                    backoff.clone(),
                    self.killed.clone(),
                    self.rpc_context.clone(),
                )
                .await?;
            let ms_before_txn_expired = resolve_result.ms_before_txn_expired;
            let live_locks = resolve_result.live_locks;
            if live_locks.is_empty() {
                result = plan.execute().await?;
            } else {
                match backoff.next_delay_duration() {
                    None => return Err(Error::ResolveLockError(live_locks)),
                    Some(delay_duration) => {
                        let delay_duration = if ms_before_txn_expired > 0 {
                            delay_duration.min(Duration::from_millis(ms_before_txn_expired as u64))
                        } else {
                            delay_duration
                        };
                        check_killed(&self.killed)?;
                        observe_backoff_seconds("txnLockFast", delay_duration);
                        crate::util::record_task_local_backoff(delay_duration);
                        if let Some(stats) = plan.runtime_stats() {
                            stats.record_backoff("txnLockFast", delay_duration);
                        }
                        sleep(delay_duration).await;
                        result = plan.execute().await?;
                    }
                }
            }
        }
    }

    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.inner.runtime_stats()
    }
}

pub(crate) struct ResolveLockForRead<P: Plan, PdC: PdClient> {
    pub(crate) inner: P,
    pub(crate) ctx: ResolveLocksContext,
    pub(crate) timestamp: Timestamp,
    pub(crate) pd_client: Arc<PdC>,
    pub(crate) backoff: Backoff,
    pub(crate) killed: Option<Arc<AtomicU32>>,
    pub(crate) keyspace: Keyspace,
    pub(crate) force_resolve_lock_lite: bool,
    pub(crate) lock_tracker: ReadLockTracker,
    pub(crate) rpc_context: LockResolverRpcContext,
}

impl<P: Plan, PdC: PdClient> Clone for ResolveLockForRead<P, PdC> {
    fn clone(&self) -> Self {
        ResolveLockForRead {
            inner: self.inner.clone(),
            ctx: self.ctx.clone(),
            timestamp: self.timestamp.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            killed: self.killed.clone(),
            keyspace: self.keyspace,
            force_resolve_lock_lite: self.force_resolve_lock_lite,
            lock_tracker: self.lock_tracker.clone(),
            rpc_context: self.rpc_context.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable + HasKvContext + HasRequestLabel, PdC: PdClient> Plan
    for ResolveLockForRead<P, PdC>
where
    P::Result: HasLocks + HasKeyErrors + HasRegionError + Clone,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut plan = self.inner.clone();
        let is_async_batch_get = plan.request_label() == "kv_batch_get";
        let is_retry_request = plan
            .kv_context_mut()
            .map(|ctx| ctx.is_retry_request)
            .unwrap_or(false);
        let mut backoff = self.backoff.clone();
        let mut forced_leader = false;
        let caller_start_ts = self.timestamp.version();
        let lock_resolver = crate::transaction::LockResolver::new(self.ctx.clone());
        let mut resolving_record_token: Option<usize> = None;

        let (resolved_locks, committed_locks) = self.lock_tracker.snapshot().await;
        if let Some(ctx) = plan.kv_context_mut() {
            ctx.resolved_locks = resolved_locks;
            ctx.committed_locks = committed_locks;
        }

        let execute_res = plan.execute().await;
        if is_async_batch_get && !is_retry_request {
            let label = match &execute_res {
                Err(_) => "other_error",
                Ok(result) => {
                    let mut cloned = result.clone();
                    if cloned.region_error().is_some() {
                        "region_error"
                    } else if !cloned.take_locks().is_empty() {
                        "lock_error"
                    } else if cloned.key_errors().is_some() {
                        "other_error"
                    } else {
                        "ok"
                    }
                }
            };
            inc_async_batch_get_total(label);
        }

        let mut result = execute_res?;
        loop {
            let locks = result.take_locks();
            if locks.is_empty() {
                if let Some(token) = resolving_record_token.take() {
                    lock_resolver
                        .resolve_locks_done(caller_start_ts, token)
                        .await;
                }
                return Ok(result);
            }

            if backoff.is_none() {
                if let Some(token) = resolving_record_token.take() {
                    lock_resolver
                        .resolve_locks_done(caller_start_ts, token)
                        .await;
                }
                return Err(Error::ResolveLockError(locks));
            }

            if !forced_leader {
                if let Some(lock_key) = locks.first().map(|lock| lock.key.clone()) {
                    // Once we meet a lock, retrying against a follower/learner can keep seeing stale
                    // state. Align with client-go behavior by retrying on the region leader.
                    let region = self.pd_client.region_for_key(&Key::from(lock_key)).await?;
                    let region_store = self.pd_client.clone().map_region_to_store(region).await?;
                    plan.apply_store(&region_store)?;
                    if let Some(ctx) = plan.kv_context_mut() {
                        if ctx.stale_read {
                            // Align with client-go `DisableStaleReadMeetLock`: once a stale read
                            // meets a lock, fall back to normal reads on the leader.
                            ctx.stale_read = false;
                            ctx.replica_read = false;
                            // client-go also clears busy-threshold (load-based replica read) once
                            // stale read meets a lock.
                            ctx.busy_threshold_ms = 0;
                        }
                    }
                    forced_leader = true;
                }
            }

            let token = match resolving_record_token {
                Some(token) => {
                    lock_resolver
                        .update_resolving_locks(&locks, caller_start_ts, token)
                        .await;
                    token
                }
                None => {
                    let token = lock_resolver
                        .record_resolving_locks(&locks, caller_start_ts)
                        .await;
                    resolving_record_token = Some(token);
                    token
                }
            };

            let resolve_result: crate::transaction::ResolveLocksForReadResult =
                match resolve_locks_for_read(
                    self.ctx.clone(),
                    locks,
                    self.timestamp.clone(),
                    self.pd_client.clone(),
                    self.keyspace,
                    backoff.clone(),
                    self.killed.clone(),
                    self.force_resolve_lock_lite,
                    Some(self.lock_tracker.clone()),
                    self.rpc_context.clone(),
                )
                .await
                {
                    Ok(resolve_result) => resolve_result,
                    Err(err) => {
                        lock_resolver
                            .resolve_locks_done(caller_start_ts, token)
                            .await;
                        return Err(err);
                    }
                };

            let ms_before_txn_expired = resolve_result.ms_before_txn_expired;
            self.lock_tracker
                .extend(
                    resolve_result.resolved_locks,
                    resolve_result.committed_locks,
                )
                .await;

            let (resolved_locks, committed_locks) = self.lock_tracker.snapshot().await;
            if let Some(ctx) = plan.kv_context_mut() {
                ctx.resolved_locks = resolved_locks;
                ctx.committed_locks = committed_locks;
            }

            if ms_before_txn_expired <= 0 {
                result = match plan.execute().await {
                    Ok(result) => result,
                    Err(err) => {
                        lock_resolver
                            .resolve_locks_done(caller_start_ts, token)
                            .await;
                        return Err(err);
                    }
                };
                continue;
            }

            match backoff.next_delay_duration() {
                None => {
                    lock_resolver
                        .resolve_locks_done(caller_start_ts, token)
                        .await;
                    return Err(Error::ResolveLockError(resolve_result.live_locks));
                }
                Some(delay_duration) => {
                    let delay_duration =
                        delay_duration.min(Duration::from_millis(ms_before_txn_expired as u64));
                    check_killed(&self.killed)?;
                    observe_backoff_seconds("txnLockFast", delay_duration);
                    crate::util::record_task_local_backoff(delay_duration);
                    if let Some(stats) = plan.runtime_stats() {
                        stats.record_backoff("txnLockFast", delay_duration);
                    }
                    sleep(delay_duration).await;
                    result = match plan.execute().await {
                        Ok(result) => result,
                        Err(err) => {
                            lock_resolver
                                .resolve_locks_done(caller_start_ts, token)
                                .await;
                            return Err(err);
                        }
                    };
                }
            }
        }
    }

    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.inner.runtime_stats()
    }
}

#[derive(Debug, Default)]
pub struct CleanupLocksResult {
    pub region_error: Option<errorpb::Error>,
    pub key_error: Option<Vec<Error>>,
    pub resolved_locks: usize,
}

impl Clone for CleanupLocksResult {
    fn clone(&self) -> Self {
        Self {
            resolved_locks: self.resolved_locks,
            ..Default::default() // Ignore errors, which should be extracted by `extract_error()`.
        }
    }
}

impl HasRegionError for CleanupLocksResult {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.region_error.take()
    }
}

impl HasKeyErrors for CleanupLocksResult {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.key_error.take()
    }
}

impl Merge<CleanupLocksResult> for Collect {
    type Out = CleanupLocksResult;

    fn merge(&self, input: Vec<Result<CleanupLocksResult>>) -> Result<Self::Out> {
        input
            .into_iter()
            .try_fold(CleanupLocksResult::default(), |acc, x| {
                Ok(CleanupLocksResult {
                    resolved_locks: acc.resolved_locks + x?.resolved_locks,
                    ..Default::default()
                })
            })
    }
}

pub struct CleanupLocks<P: Plan, PdC: PdClient> {
    pub inner: P,
    pub ctx: ResolveLocksContext,
    pub options: ResolveLocksOptions,
    pub store: Option<RegionStore>,
    pub pd_client: Arc<PdC>,
    pub keyspace: Keyspace,
}

impl<P: Plan, PdC: PdClient> Clone for CleanupLocks<P, PdC> {
    fn clone(&self) -> Self {
        CleanupLocks {
            inner: self.inner.clone(),
            ctx: self.ctx.clone(),
            options: self.options,
            store: None,
            pd_client: self.pd_client.clone(),
            keyspace: self.keyspace,
        }
    }
}

fn classify_cleanup_extracted_errors(result: &mut CleanupLocksResult, mut errors: Vec<Error>) {
    if errors.is_empty() {
        // Preserve existing behavior for malformed empty error lists.
        result.key_error = Some(Vec::new());
        return;
    }

    // Keep key errors regardless of ordering in extracted-error vectors.
    if errors.iter().any(|err| matches!(err, Error::KeyError(_))) {
        result.key_error = Some(errors);
        return;
    }

    if let Some(index) = errors
        .iter()
        .rposition(|err| matches!(err, Error::RegionError(_)))
    {
        if let Error::RegionError(e) = errors.swap_remove(index) {
            result.region_error = Some(*e);
            return;
        }
    }

    result.key_error = Some(errors);
}

#[async_trait]
impl<P: Plan + Shardable + NextBatch + HasKvContext, PdC: PdClient> Plan for CleanupLocks<P, PdC>
where
    P::Result: HasLocks + HasNextBatch + HasKeyErrors + HasRegionError,
{
    type Result = CleanupLocksResult;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = CleanupLocksResult::default();
        let mut inner = self.inner.clone();

        let mut lock_resolver_rpc_context = LockResolverRpcContext::default();
        if let Some(ctx) = inner.kv_context_mut() {
            lock_resolver_rpc_context.context = Some(ctx.clone());
            lock_resolver_rpc_context.resource_group_tag_set = !ctx.resource_group_tag.is_empty();
        }

        let mut lock_resolver = crate::transaction::LockResolver::new(self.ctx.clone());
        lock_resolver.set_rpc_context(lock_resolver_rpc_context);
        let store = self.store.as_ref().ok_or_else(|| Error::InternalError {
            message: "cleanup locks executed without store".to_owned(),
        })?;
        let region = &store.region_with_leader;
        let mut has_more_batch = true;

        while has_more_batch {
            let mut scan_lock_resp = inner.execute().await?;

            // Propagate errors to `retry_multi_region` for retry.
            if let Some(e) = scan_lock_resp.key_errors() {
                info!("CleanupLocks::execute, inner key errors:{:?}", e);
                result.key_error = Some(e);
                return Ok(result);
            } else if let Some(e) = scan_lock_resp.region_error() {
                info!("CleanupLocks::execute, inner region error:{}", e.message);
                result.region_error = Some(e);
                return Ok(result);
            }

            // Iterate to next batch of inner.
            match scan_lock_resp.has_next_batch() {
                Some(range) if region.contains(range.0.as_ref()) => {
                    debug!("CleanupLocks::execute, next range:{:?}", range);
                    inner.next_batch(range);
                }
                _ => has_more_batch = false,
            }

            let mut locks = scan_lock_resp.take_locks();
            if locks.is_empty() {
                break;
            }
            if locks.len() < self.options.batch_size as usize {
                has_more_batch = false;
            }

            if self.options.async_commit_only {
                locks = locks
                    .into_iter()
                    .filter(|l| l.use_async_commit)
                    .collect::<Vec<_>>();
            }
            debug!("CleanupLocks::execute, meet locks:{}", locks.len());

            let lock_size = locks.len();
            match lock_resolver
                .cleanup_locks(store.clone(), locks, self.pd_client.clone(), self.keyspace)
                .await
            {
                Ok(()) => {
                    result.resolved_locks += lock_size;
                }
                Err(Error::ExtractedErrors(errors)) => {
                    // Propagate errors to `retry_multi_region` for retry.
                    classify_cleanup_extracted_errors(&mut result, errors);
                    return Ok(result);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(result)
    }
}

/// When executed, the plan extracts errors from its inner plan, and returns an
/// `Err` wrapping the error.
///
/// We usually need to apply this plan if (and only if) the output of the inner
/// plan is of a response type.
///
/// The errors come from two places: `Err` from inner plans, and `Ok(response)`
/// where `response` contains unresolved errors (`error` and `region_error`).
pub struct ExtractError<P: Plan> {
    pub inner: P,
}

impl<P: Plan> Clone for ExtractError<P> {
    fn clone(&self) -> Self {
        ExtractError {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan> Plan for ExtractError<P>
where
    P::Result: HasKeyErrors + HasRegionErrors,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = self.inner.execute().await?;
        if let Some(errors) = result.key_errors() {
            Err(Error::ExtractedErrors(errors))
        } else if let Some(errors) = result.region_errors() {
            Err(Error::ExtractedErrors(
                errors
                    .into_iter()
                    .map(|e| Error::RegionError(Box::new(e)))
                    .collect(),
            ))
        } else {
            Ok(result)
        }
    }
}

/// When executed, the plan clones the shard and execute its inner plan, then
/// returns `(shard, response)`.
///
/// It's useful when the information of shard are lost in the response but needed
/// for processing.
pub struct PreserveShard<P: Plan + Shardable> {
    pub inner: P,
    pub shard: Option<P::Shard>,
}

impl<P: Plan + Shardable> Clone for PreserveShard<P> {
    fn clone(&self) -> Self {
        PreserveShard {
            inner: self.inner.clone(),
            shard: self.shard.clone(),
        }
    }
}

#[async_trait]
impl<P> Plan for PreserveShard<P>
where
    P: Plan + Shardable,
{
    type Result = ResponseWithShard<P::Result, P::Shard>;

    async fn execute(&self) -> Result<Self::Result> {
        let shard = self
            .shard
            .as_ref()
            .ok_or_else(|| Error::InternalError {
                message: "preserve shard executed without shard".to_owned(),
            })?
            .clone();
        let res = self.inner.execute().await?;
        Ok(ResponseWithShard(res, shard))
    }

    fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.inner.runtime_stats()
    }
}

// contains a response and the corresponding shards
#[derive(Debug, Clone)]
pub struct ResponseWithShard<Resp, Shard>(pub Resp, pub Shard);

impl<Resp: HasKeyErrors, Shard> HasKeyErrors for ResponseWithShard<Resp, Shard> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.0.key_errors()
    }
}

impl<Resp: HasLocks, Shard> HasLocks for ResponseWithShard<Resp, Shard> {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.0.take_locks()
    }
}

impl<Resp: HasRegionError, Shard> HasRegionError for ResponseWithShard<Resp, Shard> {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.0.region_error()
    }
}

impl HasNextBatch for ResponseWithShard<kvrpcpb::ScanLockResponse, (Vec<u8>, Vec<u8>)> {
    fn has_next_batch(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let end_key = &self.1 .1;
        let last_lock = self.0.locks.last()?;
        let mut start_key: Vec<u8> = last_lock.key.clone();
        start_key.push(0);
        if !end_key.is_empty() && start_key.as_slice() >= end_key.as_slice() {
            return None;
        }
        Some((start_key, end_key.clone()))
    }
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::sync::Mutex;

    use futures::stream::BoxStream;
    use futures::stream::{self};
    use serial_test::serial;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb::BatchGetResponse;

    struct GlobalConfigGuard {
        prev: crate::Config,
    }

    impl Drop for GlobalConfigGuard {
        fn drop(&mut self) {
            crate::config::set_global_config(self.prev.clone());
        }
    }

    fn set_global_config_scoped(config: crate::Config) -> GlobalConfigGuard {
        let prev = crate::config::get_global_config();
        crate::config::set_global_config(config);
        GlobalConfigGuard { prev }
    }

    struct TraceHookReset;

    impl Drop for TraceHookReset {
        fn drop(&mut self) {
            crate::trace::set_trace_event_func(None);
            crate::trace::set_is_category_enabled_func(None);
        }
    }

    fn trace_field_str<'a>(fields: &'a [crate::trace::TraceField], key: &str) -> Option<&'a str> {
        fields
            .iter()
            .find(|field| field.key == key)
            .and_then(|field| match &field.value {
                crate::trace::TraceValue::Str(value) => Some(value.as_ref()),
                _ => None,
            })
    }

    fn trace_field_u64(fields: &[crate::trace::TraceField], key: &str) -> Option<u64> {
        fields
            .iter()
            .find(|field| field.key == key)
            .and_then(|field| match &field.value {
                crate::trace::TraceValue::U64(value) => Some(*value),
                _ => None,
            })
    }

    fn trace_field_bool(fields: &[crate::trace::TraceField], key: &str) -> Option<bool> {
        fields
            .iter()
            .find(|field| field.key == key)
            .and_then(|field| match &field.value {
                crate::trace::TraceValue::Bool(value) => Some(*value),
                _ => None,
            })
    }

    #[derive(Clone, Default)]
    struct TraceTestRequest;

    #[async_trait::async_trait]
    impl crate::store::Request for TraceTestRequest {
        async fn dispatch(
            &self,
            _client: &crate::proto::tikvpb::tikv_client::TikvClient<tonic::transport::Channel>,
            _timeout: std::time::Duration,
        ) -> crate::Result<Box<dyn Any>> {
            unreachable!("TraceTestRequest::dispatch should not be called")
        }

        fn label(&self) -> &'static str {
            "trace_test_request"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn set_leader(&mut self, _leader: &crate::region::RegionWithLeader) -> crate::Result<()> {
            Ok(())
        }

        fn set_api_version(&mut self, _api_version: crate::proto::kvrpcpb::ApiVersion) {}

        fn set_is_retry_request(&mut self, _is_retry_request: bool) {}

        fn encoded_len(&self) -> usize {
            11
        }
    }

    impl crate::request::KvRequest for TraceTestRequest {
        type Response = crate::proto::kvrpcpb::GetResponse;
    }

    impl Shardable for TraceTestRequest {
        type Shard = ();

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            Box::pin(stream::empty()).boxed()
        }

        fn apply_shard(&mut self, _: Self::Shard) {}

        fn apply_store(&mut self, store: &crate::store::RegionStore) -> Result<()> {
            crate::store::Request::set_leader(self, &store.region_with_leader)
        }
    }

    #[tokio::test]
    async fn test_dispatch_records_task_local_exec_details_wait_kv_duration() {
        let details = std::sync::Arc::new(crate::util::ExecDetails::default());
        let response = crate::proto::kvrpcpb::GetResponse {
            value: b"resp".to_vec(),
            ..Default::default()
        };
        let expected_received =
            i64::try_from(prost::Message::encoded_len(&response)).unwrap_or(i64::MAX);
        let kv = std::sync::Arc::new(MockKvClient::with_dispatch_hook(move |_| {
            std::thread::sleep(Duration::from_millis(2));
            Ok(Box::new(response.clone()))
        }));

        let plan = Dispatch {
            request: TraceTestRequest,
            kv_client: Some(kv),
        };

        crate::util::with_exec_details(details.clone(), async move {
            let _ = plan.execute().await.expect("dispatch should succeed");
        })
        .await;

        assert!(details.wait_kv_resp_duration() >= Duration::from_millis(1));
        assert_eq!(details.traffic_details().unpacked_bytes_sent_kv_total(), 11);
        assert_eq!(
            details.traffic_details().unpacked_bytes_received_kv_total(),
            expected_received
        );
    }

    #[tokio::test]
    async fn test_dispatch_with_interceptor_fast_path_records_task_local_exec_details_wait_kv_duration(
    ) {
        let details = std::sync::Arc::new(crate::util::ExecDetails::default());
        let response = crate::proto::kvrpcpb::GetResponse {
            value: b"resp".to_vec(),
            ..Default::default()
        };
        let expected_received =
            i64::try_from(prost::Message::encoded_len(&response)).unwrap_or(i64::MAX);
        let kv = MockKvClient::with_dispatch_hook(move |_| {
            std::thread::sleep(Duration::from_millis(2));
            Ok(Box::new(response.clone()))
        });

        let rpc_interceptors: crate::rpc_interceptor::RpcInterceptors =
            std::sync::Arc::new(Vec::new());
        let plan = DispatchWithInterceptor {
            request: TraceTestRequest,
            kv_client: Some(std::sync::Arc::new(kv)),
            store_address: Some("test-store".to_owned()),
            rpc_interceptors,
        };

        crate::util::with_exec_details(details.clone(), async {
            let _ = plan.execute().await.unwrap();
        })
        .await;

        assert!(details.wait_kv_resp_duration() >= Duration::from_millis(1));
        assert_eq!(details.traffic_details().unpacked_bytes_sent_kv_total(), 11);
        assert_eq!(
            details.traffic_details().unpacked_bytes_received_kv_total(),
            expected_received
        );
    }

    #[tokio::test]
    async fn test_single_shard_handler_records_mpp_and_cross_zone_traffic() {
        let _lock = crate::config::GLOBAL_CONFIG_TEST_LOCK.lock().await;
        let _guard = set_global_config_scoped(crate::Config::default().with_zone_label("zone1"));

        let details = std::sync::Arc::new(crate::util::ExecDetails::default());
        let response = crate::proto::kvrpcpb::GetResponse {
            value: b"resp".to_vec(),
            ..Default::default()
        };
        let expected_received =
            i64::try_from(prost::Message::encoded_len(&response)).unwrap_or(i64::MAX);
        let kv = MockKvClient::with_dispatch_hook(move |_| Ok(Box::new(response.clone())));

        let pd_client = std::sync::Arc::new(MockPdClient::new(kv));
        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                labels: vec![
                    StoreLabel {
                        key: "zone".to_owned(),
                        value: "zone2".to_owned(),
                    },
                    StoreLabel {
                        key: "engine".to_owned(),
                        value: "tiflash".to_owned(),
                    },
                ],
                ..Default::default()
            })
            .await;

        let region = RegionWithLeader {
            region: metapb::Region {
                id: 42,
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 1,
                ..Default::default()
            }),
        };

        let plan = Dispatch {
            request: TraceTestRequest,
            kv_client: None,
        };
        let permits = std::sync::Arc::new(Semaphore::new(1));
        let match_store_ids = std::sync::Arc::new(Vec::new());
        let match_store_labels = std::sync::Arc::new(Vec::new());

        crate::util::with_exec_details(details.clone(), async {
            let results = RetryableMultiRegion::<Dispatch<TraceTestRequest>, MockPdClient>::single_shard_handler(
                pd_client,
                plan,
                region,
                Backoff::no_backoff(),
                None,
                permits,
                false,
                false,
                None,
                match_store_ids,
                match_store_labels,
                None,
                None,
            )
            .await
            .unwrap();
            assert_eq!(results.len(), 1);
            assert!(results[0].is_ok());
        })
        .await;

        assert_eq!(details.traffic_details().unpacked_bytes_sent_kv_total(), 0);
        assert_eq!(
            details.traffic_details().unpacked_bytes_received_kv_total(),
            0
        );
        assert_eq!(
            details.traffic_details().unpacked_bytes_sent_mpp_total(),
            11
        );
        assert_eq!(
            details
                .traffic_details()
                .unpacked_bytes_received_mpp_total(),
            expected_received
        );
        assert_eq!(
            details
                .traffic_details()
                .unpacked_bytes_sent_mpp_cross_zone(),
            11
        );
        assert_eq!(
            details
                .traffic_details()
                .unpacked_bytes_received_mpp_cross_zone(),
            expected_received
        );
    }

    #[derive(Clone)]
    struct MultiShardTrafficRequest;

    #[async_trait]
    impl crate::store::Request for MultiShardTrafficRequest {
        async fn dispatch(
            &self,
            _client: &crate::proto::tikvpb::tikv_client::TikvClient<tonic::transport::Channel>,
            _timeout: std::time::Duration,
        ) -> crate::Result<Box<dyn Any>> {
            unreachable!("MultiShardTrafficRequest::dispatch should not be called")
        }

        fn label(&self) -> &'static str {
            "multi_shard_traffic_request"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn set_leader(&mut self, _leader: &crate::region::RegionWithLeader) -> crate::Result<()> {
            Ok(())
        }

        fn set_api_version(&mut self, _api_version: crate::proto::kvrpcpb::ApiVersion) {}

        fn set_is_retry_request(&mut self, _is_retry_request: bool) {}

        fn encoded_len(&self) -> usize {
            11
        }
    }

    impl crate::request::KvRequest for MultiShardTrafficRequest {
        type Response = crate::proto::kvrpcpb::GetResponse;
    }

    impl Shardable for MultiShardTrafficRequest {
        type Shard = ();

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            Box::pin(stream::iter([store_region(1), store_region(2)]).map(Ok)).boxed()
        }

        fn apply_shard(&mut self, _: Self::Shard) {}

        fn apply_store(&mut self, store: &crate::store::RegionStore) -> Result<()> {
            crate::store::Request::set_leader(self, &store.region_with_leader)
        }
    }

    fn store_region(store_id: u64) -> ((), RegionWithLeader) {
        let region = RegionWithLeader {
            region: metapb::Region {
                id: store_id,
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id,
                ..Default::default()
            }),
        };
        ((), region)
    }

    #[tokio::test]
    async fn test_retryable_multi_region_spawns_inherit_exec_details_task_local() {
        let details = std::sync::Arc::new(crate::util::ExecDetails::default());
        let response = crate::proto::kvrpcpb::GetResponse {
            value: b"resp".to_vec(),
            ..Default::default()
        };
        let expected_received =
            i64::try_from(prost::Message::encoded_len(&response)).unwrap_or(i64::MAX);
        let kv = MockKvClient::with_dispatch_hook(move |_| Ok(Box::new(response.clone())));
        let pd_client = Arc::new(MockPdClient::new(kv));

        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request: MultiShardTrafficRequest,
                kv_client: None,
            },
            pd_client,
            backoff: Backoff::no_backoff(),
            killed: None,
            concurrency: 2,
            txn_regions_num_observer: None,
            batch_executor_token_wait_observer: false,
            preserve_region_results: false,
            replica_read: None,
            match_store_ids: Arc::new(Vec::new()),
            match_store_labels: Arc::new(Vec::new()),
        };

        crate::util::with_exec_details(details.clone(), async {
            let results = plan.execute().await.unwrap();
            assert_eq!(results.len(), 2);
            assert!(results.iter().all(|r| r.is_ok()));
        })
        .await;

        assert_eq!(details.traffic_details().unpacked_bytes_sent_kv_total(), 22);
        assert_eq!(
            details.traffic_details().unpacked_bytes_received_kv_total(),
            expected_received * 2
        );
    }

    #[tokio::test]
    async fn test_trace_kv_request_send_and_result_emitted() {
        let _lock = crate::trace::TRACE_HOOK_TEST_LOCK.lock().await;
        let _reset = TraceHookReset;

        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(
            crate::trace::Category,
            String,
            Vec<crate::trace::TraceField>,
        )>::new()));

        let seen_event = seen.clone();
        let event: crate::trace::TraceEventFunc =
            std::sync::Arc::new(move |category, name, fields| {
                if category != crate::trace::Category::KvRequest {
                    return;
                }
                if name != "kv.request.send" && name != "kv.request.result" {
                    return;
                }
                if trace_field_str(fields, "label") != Some("trace_test_request") {
                    return;
                }
                seen_event
                    .lock()
                    .unwrap()
                    .push((category, name.to_owned(), fields.to_vec()));
            });
        crate::trace::set_trace_event_func(Some(event));

        let enabled: crate::trace::IsCategoryEnabledFunc =
            std::sync::Arc::new(|category| category == crate::trace::Category::KvRequest);
        crate::trace::set_is_category_enabled_func(Some(enabled));

        let kv = MockKvClient::with_dispatch_hook(|req| {
            req.downcast_ref::<TraceTestRequest>()
                .expect("expected trace test request");
            Ok(Box::new(crate::proto::kvrpcpb::GetResponse::default()) as Box<dyn Any>)
        });

        let plan = Dispatch {
            request: TraceTestRequest,
            kv_client: Some(std::sync::Arc::new(kv)),
        };

        let _ = plan.execute().await.unwrap();

        let events = seen.lock().unwrap().clone();
        assert_eq!(events.len(), 2);

        assert_eq!(events[0].0, crate::trace::Category::KvRequest);
        assert_eq!(events[0].1, "kv.request.send");
        assert_eq!(
            trace_field_str(&events[0].2, "label"),
            Some("trace_test_request")
        );
        assert_eq!(
            trace_field_str(&events[0].2, "cmd"),
            Some("TraceTestRequest")
        );
        assert_eq!(trace_field_u64(&events[0].2, "region_id"), Some(0));
        assert_eq!(trace_field_u64(&events[0].2, "region_ver"), Some(0));
        assert_eq!(trace_field_u64(&events[0].2, "region_confVer"), Some(0));
        assert_eq!(trace_field_u64(&events[0].2, "store_id"), Some(0));
        assert_eq!(trace_field_str(&events[0].2, "store_addr"), Some(""));
        assert_eq!(trace_field_u64(&events[0].2, "timeout_ms"), Some(0));
        assert_eq!(trace_field_str(&events[0].2, "region_start_key"), None);
        assert_eq!(trace_field_str(&events[0].2, "region_end_key"), None);

        assert_eq!(events[1].0, crate::trace::Category::KvRequest);
        assert_eq!(events[1].1, "kv.request.result");
        assert_eq!(
            trace_field_str(&events[1].2, "label"),
            Some("trace_test_request")
        );
        assert_eq!(
            trace_field_str(&events[1].2, "cmd"),
            Some("TraceTestRequest")
        );
        assert_eq!(trace_field_u64(&events[1].2, "region_id"), Some(0));
        assert_eq!(trace_field_u64(&events[1].2, "region_ver"), Some(0));
        assert_eq!(trace_field_u64(&events[1].2, "region_confVer"), Some(0));
        assert_eq!(trace_field_u64(&events[1].2, "store_id"), Some(0));
        assert_eq!(trace_field_str(&events[1].2, "store_addr"), Some(""));
        assert_eq!(trace_field_u64(&events[1].2, "timeout_ms"), Some(0));
        assert_eq!(trace_field_str(&events[1].2, "region_start_key"), None);
        assert_eq!(trace_field_str(&events[1].2, "region_end_key"), None);
        assert_eq!(trace_field_bool(&events[1].2, "success"), Some(true));
        assert!(trace_field_u64(&events[1].2, "latency_ms").is_some());
    }

    #[tokio::test]
    async fn test_trace_kv_request_includes_region_store_timeout_and_range() {
        let _lock = crate::trace::TRACE_HOOK_TEST_LOCK.lock().await;
        let _reset = TraceHookReset;

        crate::trace::clear_kv_request_region_ranges_for_test();

        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(
            crate::trace::Category,
            String,
            Vec<crate::trace::TraceField>,
        )>::new()));

        let seen_event = seen.clone();
        let event: crate::trace::TraceEventFunc =
            std::sync::Arc::new(move |category, name, fields| {
                if category != crate::trace::Category::KvRequest {
                    return;
                }
                if name != "kv.request.send" && name != "kv.request.result" {
                    return;
                }
                if trace_field_str(fields, "label") != Some("kv_get") {
                    return;
                }
                seen_event
                    .lock()
                    .unwrap()
                    .push((category, name.to_owned(), fields.to_vec()));
            });
        crate::trace::set_trace_event_func(Some(event));

        let enabled: crate::trace::IsCategoryEnabledFunc =
            std::sync::Arc::new(|category| category == crate::trace::Category::KvRequest);
        crate::trace::set_is_category_enabled_func(Some(enabled));

        let mut region = crate::proto::metapb::Region::default();
        region.id = 42;
        region.start_key = vec![0x01, 0x02];
        region.end_key = vec![0x02, 0x03];
        region.region_epoch = Some(crate::proto::metapb::RegionEpoch {
            conf_ver: 7,
            version: 9,
        });
        let mut peer = crate::proto::metapb::Peer::default();
        peer.store_id = 99;
        let region = crate::region::RegionWithLeader {
            region,
            leader: Some(peer),
        };
        let expected_start = crate::redact::key(&region.region.start_key);
        let expected_end = crate::redact::key(&region.region.end_key);

        let mut get = crate::proto::kvrpcpb::GetRequest::default();
        get.key = vec![1];
        let mut request =
            crate::request::RequestWithTimeout::new(get, std::time::Duration::from_secs(5));
        crate::store::Request::set_leader(&mut request, &region)
            .expect("set_leader should succeed");

        let mut kv = MockKvClient::with_dispatch_hook(|req| {
            req.downcast_ref::<crate::proto::kvrpcpb::GetRequest>()
                .expect("expected get request");
            Ok(Box::new(crate::proto::kvrpcpb::GetResponse::default()) as Box<dyn Any>)
        });
        kv.addr = "test-store".to_owned();

        let plan = Dispatch {
            request,
            kv_client: Some(std::sync::Arc::new(kv)),
        };

        let _ = plan.execute().await.unwrap();

        let events = seen.lock().unwrap().clone();
        assert_eq!(events.len(), 2);

        for (_, name, fields) in events {
            assert!(name == "kv.request.send" || name == "kv.request.result");
            assert_eq!(trace_field_str(&fields, "label"), Some("kv_get"));
            assert_eq!(trace_field_str(&fields, "cmd"), Some("Get"));
            assert_eq!(trace_field_u64(&fields, "region_id"), Some(42));
            assert_eq!(trace_field_u64(&fields, "region_ver"), Some(9));
            assert_eq!(trace_field_u64(&fields, "region_confVer"), Some(7));
            assert_eq!(trace_field_u64(&fields, "store_id"), Some(99));
            assert_eq!(trace_field_str(&fields, "store_addr"), Some("test-store"));
            assert_eq!(trace_field_u64(&fields, "timeout_ms"), Some(5000));
            assert_eq!(
                trace_field_str(&fields, "region_start_key"),
                Some(expected_start.as_str())
            );
            assert_eq!(
                trace_field_str(&fields, "region_end_key"),
                Some(expected_end.as_str())
            );
        }
    }

    #[tokio::test]
    async fn test_trace_cop_other_error_emitted() {
        let _lock = crate::trace::TRACE_HOOK_TEST_LOCK.lock().await;
        let _reset = TraceHookReset;

        let seen = std::sync::Arc::new(std::sync::Mutex::new(
            Vec::<Vec<crate::trace::TraceField>>::new(),
        ));

        let seen_event = seen.clone();
        let event: crate::trace::TraceEventFunc =
            std::sync::Arc::new(move |category, name, fields| {
                if category != crate::trace::Category::KvRequest {
                    return;
                }
                if name != "cop.other_error" {
                    return;
                }
                seen_event.lock().unwrap().push(fields.to_vec());
            });
        crate::trace::set_trace_event_func(Some(event));

        let enabled: crate::trace::IsCategoryEnabledFunc =
            std::sync::Arc::new(|category| category == crate::trace::Category::KvRequest);
        crate::trace::set_is_category_enabled_func(Some(enabled));

        let mut region = crate::proto::metapb::Region::default();
        region.id = 7;
        region.region_epoch = Some(crate::proto::metapb::RegionEpoch {
            conf_ver: 8,
            version: 9,
        });
        let mut peer = crate::proto::metapb::Peer::default();
        peer.store_id = 41;
        let region = crate::region::RegionWithLeader {
            region,
            leader: Some(peer),
        };

        let mut request = crate::proto::coprocessor::Request::default();
        crate::store::Request::set_leader(&mut request, &region)
            .expect("set_leader should succeed");

        let mut kv = MockKvClient::with_dispatch_hook(|req| {
            req.downcast_ref::<crate::proto::coprocessor::Request>()
                .expect("expected coprocessor request");
            let mut resp = crate::proto::coprocessor::Response::default();
            resp.other_error = "boom".to_owned();
            Ok(Box::new(resp) as Box<dyn Any>)
        });
        kv.addr = "test-store".to_owned();

        let plan = Dispatch {
            request,
            kv_client: Some(std::sync::Arc::new(kv)),
        };

        let _ = plan.execute().await.unwrap();

        let events = seen.lock().unwrap().clone();
        assert_eq!(events.len(), 1);
        let fields = &events[0];
        assert_eq!(trace_field_str(fields, "other_error"), Some("boom"));
        assert_eq!(trace_field_u64(fields, "region_id"), Some(7));
        assert_eq!(trace_field_u64(fields, "region_ver"), Some(9));
        assert_eq!(trace_field_u64(fields, "region_confVer"), Some(8));
        assert_eq!(trace_field_u64(fields, "store_id"), Some(41));
        assert_eq!(trace_field_str(fields, "store_addr"), Some("test-store"));
    }

    #[tokio::test]
    async fn test_trace_kv_request_result_includes_exec_details_when_enabled() {
        let _lock = crate::trace::TRACE_HOOK_TEST_LOCK.lock().await;
        let _reset = TraceHookReset;

        let seen = std::sync::Arc::new(std::sync::Mutex::new(
            Vec::<Vec<crate::trace::TraceField>>::new(),
        ));

        let seen_event = seen.clone();
        let event: crate::trace::TraceEventFunc =
            std::sync::Arc::new(move |category, name, fields| {
                if category != crate::trace::Category::KvRequest {
                    return;
                }
                if name != "kv.request.result" {
                    return;
                }
                seen_event.lock().unwrap().push(fields.to_vec());
            });
        crate::trace::set_trace_event_func(Some(event));

        let enabled: crate::trace::IsCategoryEnabledFunc =
            std::sync::Arc::new(|category| category == crate::trace::Category::KvRequest);
        crate::trace::set_is_category_enabled_func(Some(enabled));

        let exec_details = crate::proto::kvrpcpb::ExecDetailsV2 {
            time_detail: None,
            scan_detail_v2: Some(crate::proto::kvrpcpb::ScanDetailV2 {
                get_snapshot_nanos: 6,
                rocksdb_block_read_nanos: 7,
                rocksdb_block_read_byte: 8,
                read_index_propose_wait_nanos: 9,
                ..Default::default()
            }),
            write_detail: Some(crate::proto::kvrpcpb::WriteDetail {
                store_batch_wait_nanos: 10,
                ..Default::default()
            }),
            time_detail_v2: Some(crate::proto::kvrpcpb::TimeDetailV2 {
                wait_wall_time_ns: 1,
                process_wall_time_ns: 2,
                process_suspend_wall_time_ns: 3,
                kv_read_wall_time_ns: 4,
                total_rpc_wall_time_ns: 5,
            }),
        };

        let response = crate::proto::kvrpcpb::GetResponse {
            exec_details_v2: Some(exec_details),
            ..Default::default()
        };
        let kv = MockKvClient::with_dispatch_hook(move |_| Ok(Box::new(response.clone())));
        let plan = Dispatch {
            request: TraceTestRequest,
            kv_client: Some(std::sync::Arc::new(kv)),
        };

        crate::util::with_trace_exec_details(async {
            let _ = plan.execute().await.unwrap();
        })
        .await;

        let events = seen.lock().unwrap().clone();
        assert_eq!(events.len(), 1);
        let fields = &events[0];
        assert_eq!(trace_field_str(fields, "label"), Some("trace_test_request"));
        assert_eq!(trace_field_bool(fields, "success"), Some(true));
        assert!(trace_field_u64(fields, "latency_ms").is_some());

        assert_eq!(trace_field_u64(fields, "tikv_exec_total_rpc_ns"), Some(5));
        assert_eq!(trace_field_u64(fields, "tikv_exec_wait_ns"), Some(1));
        assert_eq!(trace_field_u64(fields, "tikv_exec_process_ns"), Some(2));
        assert_eq!(trace_field_u64(fields, "tikv_exec_suspend_ns"), Some(3));
        assert_eq!(trace_field_u64(fields, "tikv_exec_kv_read_ns"), Some(4));
        assert_eq!(
            trace_field_u64(fields, "tikv_exec_get_snapshot_ns"),
            Some(6)
        );
        assert_eq!(
            trace_field_u64(fields, "tikv_exec_rocksdb_block_read_ns"),
            Some(7)
        );
        assert_eq!(
            trace_field_u64(fields, "tikv_exec_rocksdb_block_read_bytes"),
            Some(8)
        );
        assert_eq!(
            trace_field_u64(fields, "tikv_exec_read_index_propose_wait_ns"),
            Some(9)
        );
        assert_eq!(
            trace_field_u64(fields, "tikv_exec_store_batch_wait_ns"),
            Some(10)
        );
    }

    #[tokio::test]
    async fn test_trace_kv_request_includes_target_for_dispatch_with_interceptor() {
        let _lock = crate::trace::TRACE_HOOK_TEST_LOCK.lock().await;
        let _reset = TraceHookReset;

        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(
            crate::trace::Category,
            String,
            Vec<crate::trace::TraceField>,
        )>::new()));

        let seen_event = seen.clone();
        let event: crate::trace::TraceEventFunc =
            std::sync::Arc::new(move |category, name, fields| {
                if category != crate::trace::Category::KvRequest {
                    return;
                }
                if name != "kv.request.send" && name != "kv.request.result" {
                    return;
                }
                if trace_field_str(fields, "target") != Some("test-store") {
                    return;
                }
                seen_event
                    .lock()
                    .unwrap()
                    .push((category, name.to_owned(), fields.to_vec()));
            });
        crate::trace::set_trace_event_func(Some(event));

        let enabled: crate::trace::IsCategoryEnabledFunc =
            std::sync::Arc::new(|category| category == crate::trace::Category::KvRequest);
        crate::trace::set_is_category_enabled_func(Some(enabled));

        let kv = MockKvClient::with_dispatch_hook(|req| {
            req.downcast_ref::<crate::proto::kvrpcpb::GetRequest>()
                .expect("expected get request");
            Ok(Box::new(crate::proto::kvrpcpb::GetResponse::default()) as Box<dyn Any>)
        });

        let mut req = crate::proto::kvrpcpb::GetRequest::default();
        req.key = vec![1];

        let rpc_interceptors: crate::rpc_interceptor::RpcInterceptors =
            std::sync::Arc::new(Vec::new());
        let plan = DispatchWithInterceptor {
            request: req,
            kv_client: Some(std::sync::Arc::new(kv)),
            store_address: Some("test-store".to_owned()),
            rpc_interceptors,
        };

        let _ = plan.execute().await.unwrap();

        let events = seen.lock().unwrap().clone();
        assert_eq!(events.len(), 2);

        assert_eq!(events[0].0, crate::trace::Category::KvRequest);
        assert_eq!(events[0].1, "kv.request.send");
        assert_eq!(trace_field_str(&events[0].2, "label"), Some("kv_get"));
        assert_eq!(trace_field_str(&events[0].2, "cmd"), Some("Get"));
        assert_eq!(trace_field_u64(&events[0].2, "region_id"), Some(0));
        assert_eq!(trace_field_u64(&events[0].2, "region_ver"), Some(0));
        assert_eq!(trace_field_u64(&events[0].2, "region_confVer"), Some(0));
        assert_eq!(trace_field_u64(&events[0].2, "store_id"), Some(0));
        assert_eq!(
            trace_field_str(&events[0].2, "store_addr"),
            Some("test-store")
        );
        assert_eq!(trace_field_u64(&events[0].2, "timeout_ms"), Some(0));
        assert_eq!(trace_field_str(&events[0].2, "region_start_key"), None);
        assert_eq!(trace_field_str(&events[0].2, "region_end_key"), None);
        assert_eq!(trace_field_str(&events[0].2, "target"), Some("test-store"));

        assert_eq!(events[1].0, crate::trace::Category::KvRequest);
        assert_eq!(events[1].1, "kv.request.result");
        assert_eq!(trace_field_str(&events[1].2, "label"), Some("kv_get"));
        assert_eq!(trace_field_str(&events[1].2, "cmd"), Some("Get"));
        assert_eq!(trace_field_u64(&events[1].2, "region_id"), Some(0));
        assert_eq!(trace_field_u64(&events[1].2, "region_ver"), Some(0));
        assert_eq!(trace_field_u64(&events[1].2, "region_confVer"), Some(0));
        assert_eq!(trace_field_u64(&events[1].2, "store_id"), Some(0));
        assert_eq!(
            trace_field_str(&events[1].2, "store_addr"),
            Some("test-store")
        );
        assert_eq!(trace_field_u64(&events[1].2, "timeout_ms"), Some(0));
        assert_eq!(trace_field_str(&events[1].2, "region_start_key"), None);
        assert_eq!(trace_field_str(&events[1].2, "region_end_key"), None);
        assert_eq!(trace_field_str(&events[1].2, "target"), Some("test-store"));
        assert_eq!(trace_field_bool(&events[1].2, "success"), Some(true));
    }

    #[tokio::test]
    async fn test_trace_2pc_prewrite_batch_events_emitted() {
        let _lock = crate::trace::TRACE_HOOK_TEST_LOCK.lock().await;
        let _reset = TraceHookReset;

        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(
            crate::trace::Category,
            String,
            Vec<crate::trace::TraceField>,
        )>::new()));

        let seen_event = seen.clone();
        let event: crate::trace::TraceEventFunc =
            std::sync::Arc::new(move |category, name, fields| {
                if category != crate::trace::Category::Txn2Pc {
                    return;
                }
                if name != "prewrite.batch.start" && name != "prewrite.batch.result" {
                    return;
                }
                if trace_field_u64(fields, "regionID") != Some(7_000_000_007) {
                    return;
                }
                seen_event
                    .lock()
                    .unwrap()
                    .push((category, name.to_owned(), fields.to_vec()));
            });
        crate::trace::set_trace_event_func(Some(event));

        let enabled: crate::trace::IsCategoryEnabledFunc =
            std::sync::Arc::new(|category| category == crate::trace::Category::Txn2Pc);
        crate::trace::set_is_category_enabled_func(Some(enabled));

        let kv = MockKvClient::with_dispatch_hook(|req| {
            req.downcast_ref::<crate::proto::kvrpcpb::PrewriteRequest>()
                .expect("expected prewrite request");
            Ok(Box::new(crate::proto::kvrpcpb::PrewriteResponse::default()) as Box<dyn Any>)
        });

        let primary_key = vec![1];
        let mut primary_mutation = crate::proto::kvrpcpb::Mutation::default();
        primary_mutation.op = crate::proto::kvrpcpb::Op::Put as i32;
        primary_mutation.key = primary_key.clone();
        primary_mutation.value = vec![10];

        let mut secondary_mutation = crate::proto::kvrpcpb::Mutation::default();
        secondary_mutation.op = crate::proto::kvrpcpb::Op::Put as i32;
        secondary_mutation.key = vec![2];
        secondary_mutation.value = vec![11];

        let mut req = crate::proto::kvrpcpb::PrewriteRequest::default();
        req.context = Some(crate::proto::kvrpcpb::Context {
            region_id: 7_000_000_007,
            ..Default::default()
        });
        req.start_version = 42;
        req.primary_lock = primary_key;
        req.mutations = vec![primary_mutation, secondary_mutation];

        let plan = Dispatch {
            request: req,
            kv_client: Some(std::sync::Arc::new(kv)),
        };
        let _ = plan.execute().await.unwrap();

        let events = seen.lock().unwrap().clone();
        assert_eq!(events.len(), 2);

        assert_eq!(events[0].0, crate::trace::Category::Txn2Pc);
        assert_eq!(events[0].1, "prewrite.batch.start");
        assert_eq!(trace_field_u64(&events[0].2, "startTS"), Some(42));
        assert_eq!(
            trace_field_u64(&events[0].2, "regionID"),
            Some(7_000_000_007)
        );
        assert_eq!(trace_field_bool(&events[0].2, "isPrimary"), Some(true));
        assert_eq!(trace_field_u64(&events[0].2, "keyCount"), Some(2));

        assert_eq!(events[1].0, crate::trace::Category::Txn2Pc);
        assert_eq!(events[1].1, "prewrite.batch.result");
        assert_eq!(
            trace_field_u64(&events[1].2, "regionID"),
            Some(7_000_000_007)
        );
        assert_eq!(trace_field_bool(&events[1].2, "success"), Some(true));
    }

    #[tokio::test]
    async fn test_trace_2pc_commit_batch_events_emitted() {
        let _lock = crate::trace::TRACE_HOOK_TEST_LOCK.lock().await;
        let _reset = TraceHookReset;

        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(
            crate::trace::Category,
            String,
            Vec<crate::trace::TraceField>,
        )>::new()));

        let seen_event = seen.clone();
        let event: crate::trace::TraceEventFunc =
            std::sync::Arc::new(move |category, name, fields| {
                if category != crate::trace::Category::Txn2Pc {
                    return;
                }
                if name != "commit.batch.start" && name != "commit.batch.result" {
                    return;
                }
                if trace_field_u64(fields, "regionID") != Some(7_000_000_009) {
                    return;
                }
                seen_event
                    .lock()
                    .unwrap()
                    .push((category, name.to_owned(), fields.to_vec()));
            });
        crate::trace::set_trace_event_func(Some(event));

        let enabled: crate::trace::IsCategoryEnabledFunc =
            std::sync::Arc::new(|category| category == crate::trace::Category::Txn2Pc);
        crate::trace::set_is_category_enabled_func(Some(enabled));

        let kv = MockKvClient::with_dispatch_hook(|req| {
            req.downcast_ref::<crate::proto::kvrpcpb::CommitRequest>()
                .expect("expected commit request");
            Ok(Box::new(crate::proto::kvrpcpb::CommitResponse::default()) as Box<dyn Any>)
        });

        let mut req = crate::proto::kvrpcpb::CommitRequest::default();
        req.context = Some(crate::proto::kvrpcpb::Context {
            region_id: 7_000_000_009,
            ..Default::default()
        });
        req.start_version = 42;
        req.commit_version = 43;
        req.keys = vec![vec![1], vec![2]];

        let plan = Dispatch {
            request: req,
            kv_client: Some(std::sync::Arc::new(kv)),
        };
        let _ = plan.execute().await.unwrap();

        let events = seen.lock().unwrap().clone();
        assert_eq!(events.len(), 2);

        assert_eq!(events[0].0, crate::trace::Category::Txn2Pc);
        assert_eq!(events[0].1, "commit.batch.start");
        assert_eq!(trace_field_u64(&events[0].2, "startTS"), Some(42));
        assert_eq!(trace_field_u64(&events[0].2, "commitTS"), Some(43));
        assert_eq!(
            trace_field_u64(&events[0].2, "regionID"),
            Some(7_000_000_009)
        );
        assert_eq!(trace_field_u64(&events[0].2, "keyCount"), Some(2));

        assert_eq!(events[1].0, crate::trace::Category::Txn2Pc);
        assert_eq!(events[1].1, "commit.batch.result");
        assert_eq!(
            trace_field_u64(&events[1].2, "regionID"),
            Some(7_000_000_009)
        );
        assert_eq!(trace_field_bool(&events[1].2, "success"), Some(true));
    }

    #[test]
    fn test_select_replica_read_peer_mixed_skips_unreachable_store() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::Mixed,
            0,
            &[51],
            &[],
            &[],
            &[],
            false,
            false,
            &[],
        )
        .expect("expected a reachable replica");
        assert_eq!(peer.store_id, 61);
    }

    #[test]
    #[serial(metrics)]
    fn test_select_replica_read_peer_records_replica_selector_failure_metrics() {
        fn counter_value(label: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_rust_replica_selector_failure_counter"
                })
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|pair| pair.get_name() == "type" && pair.get_value() == label)
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before_invalid = counter_value("invalid");
        let invalid_region = RegionWithLeader::default();
        assert!(select_replica_read_peer(
            &invalid_region,
            ReplicaReadType::Mixed,
            0,
            &[],
            &[],
            &[],
            &[],
            false,
            false,
            &[],
        )
        .is_none());
        let after_invalid = counter_value("invalid");
        assert!(
            after_invalid >= before_invalid + 1.0,
            "expected replica_selector_failure_counter(invalid) to increase"
        );

        let before_exhausted = counter_value("exhausted");
        let region = MockPdClient::region1();
        let unavailable_store_ids: Vec<StoreId> = region
            .region
            .peers
            .iter()
            .map(|peer| peer.store_id)
            .collect();
        let _ = select_replica_read_peer(
            &region,
            ReplicaReadType::Mixed,
            0,
            &unavailable_store_ids,
            &[],
            &[],
            &[],
            false,
            false,
            &[],
        );
        let after_exhausted = counter_value("exhausted");
        assert!(
            after_exhausted >= before_exhausted + 1.0,
            "expected replica_selector_failure_counter(exhausted) to increase"
        );
    }

    #[test]
    fn test_select_replica_read_peer_mixed_includes_leader() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::Mixed,
            0,
            &[],
            &[],
            &[],
            &[],
            false,
            false,
            &[],
        )
        .expect("expected a replica");
        assert_eq!(peer.store_id, 51);

        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::Mixed,
            1,
            &[],
            &[],
            &[],
            &[],
            false,
            false,
            &[],
        )
        .expect("expected a replica");
        assert_eq!(peer.store_id, 61);

        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::Mixed,
            2,
            &[],
            &[],
            &[],
            &[],
            false,
            false,
            &[],
        )
        .expect("expected a replica");
        assert_eq!(peer.store_id, 41);
    }

    #[test]
    fn test_select_replica_read_peer_mixed_match_store_ids_does_not_always_prefer_leader() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::Mixed,
            1,
            &[],
            &[],
            &[],
            &[],
            true,
            false,
            &[41, 51],
        )
        .expect("expected a replica");
        assert_eq!(peer.store_id, 51);
    }

    #[test]
    fn test_select_replica_read_peer_mixed_prefers_unattempted_replica() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::Mixed,
            1,
            &[51],
            &[],
            &[61],
            &[],
            false,
            false,
            &[],
        )
        .expect("expected a replica");
        assert_eq!(peer.store_id, 41);
    }

    #[test]
    fn test_select_replica_read_peer_mixed_allows_data_is_not_ready_retry() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::Mixed,
            0,
            &[],
            &[],
            &[51],
            &[51],
            true,
            true,
            &[51],
        )
        .expect("expected a replica");
        assert_eq!(peer.store_id, 51);
    }

    #[test]
    fn test_select_replica_read_peer_prefer_leader_skips_unreachable_leader() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::PreferLeader,
            0,
            &[41],
            &[],
            &[],
            &[],
            false,
            false,
            &[],
        )
        .expect("expected a reachable replica");
        assert_eq!(peer.store_id, 51);
    }

    #[test]
    fn test_select_replica_read_peer_mixed_prefers_non_busy_replica() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::Mixed,
            0,
            &[],
            &[51],
            &[],
            &[],
            false,
            false,
            &[],
        )
        .expect("expected a replica");
        assert_eq!(peer.store_id, 61);
    }

    #[test]
    fn test_select_replica_read_peer_prefer_leader_avoids_busy_leader() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::PreferLeader,
            0,
            &[],
            &[41],
            &[],
            &[],
            false,
            false,
            &[],
        )
        .expect("expected a reachable replica");
        assert_eq!(peer.store_id, 51);
    }

    #[test]
    fn test_select_replica_read_peer_prefer_leader_keeps_busy_leader_when_all_followers_busy() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::PreferLeader,
            0,
            &[],
            &[41, 51, 61],
            &[],
            &[],
            false,
            false,
            &[],
        )
        .expect("expected a replica");
        assert_eq!(peer.store_id, 41);
    }

    #[test]
    fn test_select_replica_read_peer_prefer_leader_match_configured_skips_slow_non_leader() {
        let region = MockPdClient::region1();
        let peer = select_replica_read_peer(
            &region,
            ReplicaReadType::PreferLeader,
            0,
            &[],
            &[41, 51, 61],
            &[],
            &[],
            true,
            true,
            &[51],
        )
        .expect("expected a replica");
        assert_eq!(peer.store_id, 41);
    }

    #[tokio::test]
    async fn test_replica_read_mixed_with_match_store_labels_prefers_matching_store() {
        let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
        let seen_captured = seen.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push(peer.store_id);
                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 41,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 51,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-west".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 61,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-west".to_owned(),
                }],
                ..Default::default()
            })
            .await;

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read_and_match_store_labels(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
                Arc::new(vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }]),
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![41],
            "match-store-labels should override mixed replica-read rotation"
        );
    }

    #[tokio::test]
    async fn test_replica_read_mixed_without_match_store_labels_does_not_query_store_meta() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
        })));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;

        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        assert_eq!(
            pd_client.store_meta_by_id_call_count(),
            0,
            "default replica-read path should not consult PD store metadata"
        );
    }

    #[tokio::test]
    async fn test_replica_read_mixed_with_match_store_ids_prefers_matching_store_without_querying_store_meta(
    ) {
        let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
        let seen_captured = seen.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push(peer.store_id);
                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;

        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, request)
            .retry_multi_region_with_replica_read_and_match_store_ids(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
                Arc::new(vec![41]),
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![41],
            "match-store-ids should override mixed replica-read rotation"
        );
        assert_eq!(
            pd_client.store_meta_by_id_call_count(),
            0,
            "match-store-ids should not consult PD store metadata"
        );
    }

    #[tokio::test]
    async fn test_replica_read_mixed_with_match_store_labels_avoids_slow_store() {
        let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
        let seen_captured = seen.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push(peer.store_id);
                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 41,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-west".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 51,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 61,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-west".to_owned(),
                }],
                ..Default::default()
            })
            .await;

        let labels = Arc::new(vec![StoreLabel {
            key: "zone".to_owned(),
            value: "us-east".to_owned(),
        }]);

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, request)
            .retry_multi_region_with_replica_read_and_match_store_labels(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
                labels.clone(),
            )
            .plan();
        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        pd_client.mark_store_slow(51, Duration::from_secs(60));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, request)
            .retry_multi_region_with_replica_read_and_match_store_labels(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
                labels,
            )
            .plan();
        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![51, 41],
            "slow store cache should override label-matching preference"
        );
    }

    #[tokio::test]
    async fn test_replica_read_mixed_without_match_store_labels_avoids_slow_store_cache() {
        let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
        let seen_captured = seen.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push(peer.store_id);
                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        pd_client.mark_store_slow(51, Duration::from_secs(60));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![61],
            "slow store cache should affect default replica-read selection"
        );
    }

    #[tokio::test]
    async fn test_replica_read_mixed_server_is_busy_retries_same_replica() {
        let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
        let seen_captured = seen.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push(peer.store_id);

                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                let mut resp = kvrpcpb::GetResponse::default();
                if call == 0 {
                    let mut region_error = errorpb::Error::default();
                    region_error.server_is_busy = Some(errorpb::ServerIsBusy::default());
                    resp.region_error = Some(region_error);
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![51, 51],
            "mixed replica-read should retry the same replica for ServerIsBusy when busy_threshold_ms is unset"
        );
    }

    #[tokio::test]
    async fn test_load_based_replica_read_server_busy_rotates_and_disables_threshold() {
        let seen = Arc::new(Mutex::new(Vec::<(u64, u32)>::new()));
        let seen_captured = seen.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured
                    .lock()
                    .unwrap()
                    .push((peer.store_id, ctx.busy_threshold_ms));

                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                let mut resp = kvrpcpb::GetResponse::default();
                if call < 3 {
                    let mut region_error = errorpb::Error::default();
                    region_error.server_is_busy = Some(errorpb::ServerIsBusy::default());
                    resp.region_error = Some(region_error);
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            busy_threshold_ms: 123,
            ..Default::default()
        });

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Leader,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![(41, 123), (51, 123), (61, 123), (41, 0)],
            "should rotate replicas for ServerIsBusy when busy_threshold_ms is set, then disable threshold when all replicas are busy"
        );
    }

    #[tokio::test]
    async fn test_load_based_replica_read_avoids_store_with_high_estimated_wait() {
        let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
        let seen_captured = seen.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push(peer.store_id);
                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        // Simulate prior ServerIsBusy feedback: store 51 has a large estimated wait.
        pd_client.update_store_load_stats(51, 10_000);

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            busy_threshold_ms: 100,
            ..Default::default()
        });

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![61],
            "load-based replica read should avoid stores whose estimated wait exceeds busy_threshold_ms"
        );
    }

    #[tokio::test]
    async fn test_load_based_replica_read_leader_avoids_busy_leader_with_high_estimated_wait() {
        let seen = Arc::new(Mutex::new(Vec::<(u64, bool, u32)>::new()));
        let seen_captured = seen.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push((
                    peer.store_id,
                    ctx.replica_read,
                    ctx.busy_threshold_ms,
                ));
                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        // Simulate leader being busy via store load stats.
        pd_client.update_store_load_stats(41, 10_000);

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            busy_threshold_ms: 100,
            ..Default::default()
        });

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Leader,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![(51, true, 100)],
            "load-based replica read should route leader-mode reads to an idle replica when the leader is busy"
        );
    }

    #[tokio::test]
    async fn test_load_based_replica_read_leader_disables_threshold_when_all_peers_are_busy() {
        let seen = Arc::new(Mutex::new(Vec::<(u64, bool, u32)>::new()));
        let seen_captured = seen.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push((
                    peer.store_id,
                    ctx.replica_read,
                    ctx.busy_threshold_ms,
                ));
                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        // All replicas exceed the busy threshold.
        pd_client.update_store_load_stats(41, 10_000);
        pd_client.update_store_load_stats(51, 10_000);
        pd_client.update_store_load_stats(61, 10_000);

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            busy_threshold_ms: 100,
            ..Default::default()
        });

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Leader,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![(41, false, 0)],
            "load-based replica read should disable busy_threshold_ms and fall back to leader when all replicas are too busy"
        );
    }

    #[tokio::test]
    async fn test_leader_read_timeout_falls_back_to_replica_read_replicas() {
        let seen = Arc::new(Mutex::new(Vec::<(u64, bool, bool, u64)>::new()));
        let seen_captured = seen.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push((
                    peer.store_id,
                    ctx.replica_read,
                    ctx.stale_read,
                    ctx.max_execution_duration_ms,
                ));

                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                let mut resp = kvrpcpb::GetResponse::default();
                if call < 2 {
                    resp.region_error = Some(errorpb::Error {
                        message: "Deadline is exceeded".to_owned(),
                        ..Default::default()
                    });
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            max_execution_duration_ms: 1,
            ..Default::default()
        });

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Leader,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0], (41, false, false, 1));
        assert_eq!(seen[1], (51, true, false, 1));
        assert_eq!(seen[2], (61, true, false, 1));
    }

    #[tokio::test]
    async fn test_stale_read_retry_switches_to_replica_read_when_leader_can_serve() {
        let seen = Arc::new(Mutex::new(Vec::<(u64, bool, bool)>::new()));
        let seen_captured = seen.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push((
                    peer.store_id,
                    ctx.stale_read,
                    ctx.replica_read,
                ));

                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                let mut resp = kvrpcpb::GetResponse::default();
                if call < 2 {
                    let mut region_error = errorpb::Error::default();
                    region_error.data_is_not_ready = Some(errorpb::DataIsNotReady {
                        region_id: 1,
                        peer_id: peer.id,
                        safe_ts: 0,
                    });
                    resp.region_error = Some(region_error);
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            stale_read: true,
            replica_read: false,
            ..Default::default()
        });

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![(51, true, false), (41, false, false), (61, false, true)],
            "stale read retries should fall back to leader read once, then switch to replica read when the leader is healthy"
        );
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_retry_times_and_stale_read_hit_miss_metrics_recorded() {
        fn metric_label_value<'a>(
            metric: &'a prometheus::proto::Metric,
            name: &str,
        ) -> Option<&'a str> {
            metric
                .get_label()
                .iter()
                .find(|pair| pair.get_name() == name)
                .map(|pair| pair.get_value())
        }

        fn histogram_sample_sum(name: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == name)
                .map(|family| {
                    family
                        .get_metric()
                        .iter()
                        .map(|metric| metric.get_histogram().get_sample_sum())
                        .sum::<f64>()
                })
                .unwrap_or(0.0)
        }

        fn stale_read_counter_value(result: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_stale_read_counter")
                .and_then(|family| {
                    family
                        .get_metric()
                        .iter()
                        .find(|metric| metric_label_value(metric, "result") == Some(result))
                        .map(|metric| metric.get_counter().get_value())
                })
                .unwrap_or(0.0)
        }

        let before_retry_sum = histogram_sample_sum("tikv_client_rust_request_retry_times");
        let before_miss = stale_read_counter_value("miss");

        // Execute a stale-read request that retries twice (2 region errors, then success) to get
        // a deterministic + noticeable delta even when other tests run.
        const RUNS: usize = 20;
        for _ in 0..RUNS {
            let call_count = Arc::new(AtomicUsize::new(0));
            let call_count_captured = call_count.clone();
            let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
                move |req: &dyn Any| {
                    let req = req
                        .downcast_ref::<kvrpcpb::GetRequest>()
                        .expect("expected get request");
                    let ctx = req.context.as_ref().expect("expected context");
                    let peer = ctx.peer.as_ref().expect("expected peer");
                    let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                    let mut resp = kvrpcpb::GetResponse::default();
                    if call < 2 {
                        let mut region_error = errorpb::Error::default();
                        region_error.data_is_not_ready = Some(errorpb::DataIsNotReady {
                            region_id: 1,
                            peer_id: peer.id,
                            safe_ts: 0,
                        });
                        resp.region_error = Some(region_error);
                    }
                    Ok(Box::new(resp) as Box<dyn Any>)
                },
            )));

            let mut request = kvrpcpb::GetRequest::default();
            request.key = vec![1];
            request.version = 10;
            request.context = Some(kvrpcpb::Context {
                stale_read: true,
                replica_read: false,
                ..Default::default()
            });

            let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
                .retry_multi_region_with_replica_read(
                    Backoff::no_jitter_backoff(0, 0, 10),
                    ReplicaReadType::Mixed,
                )
                .plan();

            let results = plan.execute().await.expect("plan should succeed");
            assert_eq!(results.len(), 1);
            assert!(results[0].is_ok());
        }

        let after_retry_sum = histogram_sample_sum("tikv_client_rust_request_retry_times");
        let after_miss = stale_read_counter_value("miss");

        assert!(
            after_retry_sum >= before_retry_sum + (RUNS as f64) * 2.0,
            "expected request_retry_times histogram to observe retry counts"
        );
        assert!(
            after_miss >= before_miss + RUNS as f64,
            "expected stale_read_counter miss to increase"
        );
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_async_send_req_total_counter_records_ok_and_region_error() {
        fn metric_label_value<'a>(
            metric: &'a prometheus::proto::Metric,
            name: &str,
        ) -> Option<&'a str> {
            metric
                .get_label()
                .iter()
                .find(|pair| pair.get_name() == name)
                .map(|pair| pair.get_value())
        }

        fn async_send_req_counter_value(result: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_async_send_req_total")
                .and_then(|family| {
                    family
                        .get_metric()
                        .iter()
                        .find(|metric| metric_label_value(metric, "result") == Some(result))
                        .map(|metric| metric.get_counter().get_value())
                })
                .unwrap_or(0.0)
        }

        let before_ok = async_send_req_counter_value("ok");
        let before_region_error = async_send_req_counter_value("region_error");

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");

                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                let mut resp = kvrpcpb::GetResponse::default();
                if call < 2 {
                    let mut region_error = errorpb::Error::default();
                    region_error.data_is_not_ready = Some(errorpb::DataIsNotReady {
                        region_id: 1,
                        peer_id: peer.id,
                        safe_ts: 0,
                    });
                    resp.region_error = Some(region_error);
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context::default());

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 10))
            .plan();

        let results = plan.execute().await.expect("plan should succeed");
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let after_ok = async_send_req_counter_value("ok");
        let after_region_error = async_send_req_counter_value("region_error");

        assert!(
            after_region_error >= before_region_error + 2.0,
            "expected async_send_req_total(region_error) to increase"
        );
        assert!(
            after_ok >= before_ok + 1.0,
            "expected async_send_req_total(ok) to increase"
        );
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_connection_transient_failure_count_counter_records_unavailable() {
        fn counter_sum(families: &[prometheus::proto::MetricFamily]) -> f64 {
            families
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_rust_connection_transient_failure_count"
                })
                .map(|family| {
                    family
                        .get_metric()
                        .iter()
                        .map(|metric| metric.get_counter().get_value())
                        .sum()
                })
                .unwrap_or(0.0)
        }

        let before = counter_sum(&prometheus::gather());

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_req: &dyn Any| {
                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    return Err(Error::GrpcAPI(tonic::Status::unavailable(
                        "unit_test_transient_failure",
                    )));
                }
                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context::default());

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 10))
            .plan();

        let results = plan.execute().await.expect("plan should succeed");
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let after = counter_sum(&prometheus::gather());
        assert!(
            after >= before + 1.0,
            "expected connection_transient_failure_count to increase"
        );
    }

    #[tokio::test]
    async fn test_stale_read_does_not_flip_to_replica_read_when_leader_deadline_exceeded() {
        let seen = Arc::new(Mutex::new(Vec::<(u64, bool, bool)>::new()));
        let seen_captured = seen.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push((
                    peer.store_id,
                    ctx.stale_read,
                    ctx.replica_read,
                ));

                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                let mut resp = kvrpcpb::GetResponse::default();
                if call == 0 {
                    resp.region_error = Some(errorpb::Error {
                        message: "Deadline is exceeded".to_owned(),
                        ..Default::default()
                    });
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            stale_read: true,
            replica_read: false,
            max_execution_duration_ms: 1,
            ..Default::default()
        });

        // Force the first stale-read attempt to hit the leader, so the deadline-exceeded feedback
        // is registered on the leader store.
        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read_and_match_store_ids(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
                Arc::new(vec![41]),
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (41, true, false));

        let (store_id, stale_read, replica_read) = seen[1];
        assert_ne!(store_id, 41);
        assert!(stale_read);
        assert!(!replica_read);
    }

    #[tokio::test]
    async fn test_stale_read_first_retry_switches_to_replica_read_when_leader_already_tried() {
        let seen = Arc::new(Mutex::new(Vec::<(u64, bool, bool)>::new()));
        let seen_captured = seen.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push((
                    peer.store_id,
                    ctx.stale_read,
                    ctx.replica_read,
                ));

                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                let mut resp = kvrpcpb::GetResponse::default();
                if call == 0 {
                    let mut region_error = errorpb::Error::default();
                    region_error.data_is_not_ready = Some(errorpb::DataIsNotReady {
                        region_id: 1,
                        peer_id: peer.id,
                        safe_ts: 0,
                    });
                    resp.region_error = Some(region_error);
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            stale_read: true,
            replica_read: false,
            ..Default::default()
        });

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read_and_match_store_ids(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::Mixed,
                Arc::new(vec![41]),
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (41, true, false));

        let (store_id, stale_read, replica_read) = seen[1];
        assert_ne!(store_id, 41);
        assert!(!stale_read);
        assert!(replica_read);
    }

    #[tokio::test]
    async fn test_stale_read_prefer_leader_first_retry_switches_to_replica_read() {
        let seen = Arc::new(Mutex::new(Vec::<(u64, bool, bool)>::new()));
        let seen_captured = seen.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push((
                    peer.store_id,
                    ctx.stale_read,
                    ctx.replica_read,
                ));

                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                let mut resp = kvrpcpb::GetResponse::default();
                if call == 0 {
                    let mut region_error = errorpb::Error::default();
                    region_error.data_is_not_ready = Some(errorpb::DataIsNotReady {
                        region_id: 1,
                        peer_id: peer.id,
                        safe_ts: 0,
                    });
                    resp.region_error = Some(region_error);
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            stale_read: true,
            replica_read: false,
            ..Default::default()
        });

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::PreferLeader,
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (41, true, false));

        let (store_id, stale_read, replica_read) = seen[1];
        assert_ne!(store_id, 41);
        assert!(!stale_read);
        assert!(replica_read);
    }

    #[tokio::test]
    async fn test_stale_read_label_mismatch_first_attempt_flips_to_replica_read_after_switching_to_mixed(
    ) {
        let seen = Arc::new(Mutex::new(Vec::<(u64, bool, bool)>::new()));
        let seen_captured = seen.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_captured = call_count.clone();

        let pd_client_holder: Arc<Mutex<Option<Arc<MockPdClient>>>> = Arc::new(Mutex::new(None));
        let pd_client_holder_captured = pd_client_holder.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("expected context");
                let peer = ctx.peer.as_ref().expect("expected peer");
                seen_captured.lock().unwrap().push((
                    peer.store_id,
                    ctx.stale_read,
                    ctx.replica_read,
                ));

                let call = call_count_captured.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    // Make the leader "slow" so when we switch PreferLeader -> Mixed after a gRPC
                    // error, replica selection picks a non-leader with mismatched labels.
                    if let Some(pd_client) = pd_client_holder_captured.lock().unwrap().clone() {
                        pd_client.mark_store_slow(41, Duration::from_secs(60));
                    }

                    let mut resp = kvrpcpb::GetResponse::default();
                    let mut region_error = errorpb::Error::default();
                    region_error.data_is_not_ready = Some(errorpb::DataIsNotReady {
                        region_id: 1,
                        peer_id: peer.id,
                        safe_ts: 0,
                    });
                    resp.region_error = Some(region_error);
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if call == 1 {
                    return Err(Error::GrpcAPI(tonic::Status::unavailable("boom")));
                }

                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));
        *pd_client_holder.lock().unwrap() = Some(pd_client.clone());

        pd_client
            .insert_store_meta(metapb::Store {
                id: 41,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 51,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-west".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 61,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-west".to_owned(),
                }],
                ..Default::default()
            })
            .await;

        let mut request = kvrpcpb::GetRequest::default();
        request.key = vec![1];
        request.version = 10;
        request.context = Some(kvrpcpb::Context {
            stale_read: true,
            replica_read: false,
            ..Default::default()
        });

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .retry_multi_region_with_replica_read_and_match_store_labels(
                Backoff::no_jitter_backoff(0, 0, 10),
                ReplicaReadType::PreferLeader,
                Arc::new(vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }]),
            )
            .plan();

        let results = plan.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![(41, true, false), (61, false, true), (51, false, true)],
            "label-mismatch should flip stale-read to replica-read even when the attempt counter resets after switching to mixed",
        );
    }

    #[derive(Clone)]
    struct ErrPlan;

    #[async_trait]
    impl Plan for ErrPlan {
        type Result = BatchGetResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Err(Error::Unimplemented)
        }
    }

    impl HasKvContext for ErrPlan {
        fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
            None
        }
    }

    impl Shardable for ErrPlan {
        type Shard = ();

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            Box::pin(stream::iter(1..=3).map(|_| Err(Error::Unimplemented))).boxed()
        }

        fn apply_shard(&mut self, _: Self::Shard) {}

        fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
            Ok(())
        }
    }

    impl HasRequestLabel for ErrPlan {
        fn request_label(&self) -> &'static str {
            "unit_test_err_plan"
        }
    }

    #[tokio::test]
    async fn test_err() {
        let plan = RetryableMultiRegion {
            inner: ResolveLock {
                inner: ErrPlan,
                timestamp: Timestamp::default(),
                backoff: Backoff::no_backoff(),
                pd_client: Arc::new(MockPdClient::default()),
                keyspace: Keyspace::Disable,
                pessimistic_region_resolve: false,
            },
            pd_client: Arc::new(MockPdClient::default()),
            backoff: Backoff::no_backoff(),
            killed: None,
            concurrency: DEFAULT_MULTI_REGION_CONCURRENCY,
            txn_regions_num_observer: None,
            batch_executor_token_wait_observer: false,
            preserve_region_results: false,
            replica_read: None,
            match_store_ids: Arc::new(Vec::new()),
            match_store_labels: Arc::new(Vec::new()),
        };
        assert!(plan.execute().await.is_err())
    }

    #[tokio::test]
    async fn test_preserve_shard_execute_missing_shard_returns_error_without_executing_inner() {
        #[derive(Clone)]
        struct PanicPlan;

        #[async_trait]
        impl Plan for PanicPlan {
            type Result = BatchGetResponse;

            async fn execute(&self) -> Result<Self::Result> {
                panic!("inner plan executed unexpectedly");
            }
        }

        impl Shardable for PanicPlan {
            type Shard = ();

            fn shards(
                &self,
                _: &Arc<impl crate::pd::PdClient>,
            ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
                Box::pin(stream::empty()).boxed()
            }

            fn apply_shard(&mut self, _: Self::Shard) {}

            fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
                Ok(())
            }
        }

        let plan = PreserveShard {
            inner: PanicPlan,
            shard: None,
        };

        let err = plan.execute().await.unwrap_err();
        match err {
            Error::InternalError { message } => {
                assert!(message.contains("preserve shard executed without shard"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_cleanup_locks_execute_missing_store_returns_error_without_executing_inner() {
        #[derive(Clone)]
        struct DummyCleanupResp;

        impl HasLocks for DummyCleanupResp {}

        impl HasNextBatch for DummyCleanupResp {
            fn has_next_batch(&self) -> Option<(Vec<u8>, Vec<u8>)> {
                None
            }
        }

        impl HasKeyErrors for DummyCleanupResp {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                None
            }
        }

        impl HasRegionError for DummyCleanupResp {
            fn region_error(&mut self) -> Option<errorpb::Error> {
                None
            }
        }

        #[derive(Clone)]
        struct PanicPlan;

        #[async_trait]
        impl Plan for PanicPlan {
            type Result = DummyCleanupResp;

            async fn execute(&self) -> Result<Self::Result> {
                panic!("inner plan executed unexpectedly");
            }
        }

        impl Shardable for PanicPlan {
            type Shard = ();

            fn shards(
                &self,
                _: &Arc<impl crate::pd::PdClient>,
            ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
                Box::pin(stream::empty()).boxed()
            }

            fn apply_shard(&mut self, _: Self::Shard) {}

            fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
                Ok(())
            }
        }

        impl NextBatch for PanicPlan {
            fn next_batch(&mut self, _: (Vec<u8>, Vec<u8>)) {}
        }

        impl HasKvContext for PanicPlan {
            fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
                None
            }
        }

        let plan = CleanupLocks {
            inner: PanicPlan,
            ctx: ResolveLocksContext::default(),
            options: ResolveLocksOptions::default(),
            store: None,
            pd_client: Arc::new(MockPdClient::default()),
            keyspace: Keyspace::Disable,
        };

        let err = plan.execute().await.unwrap_err();
        match err {
            Error::InternalError { message } => {
                assert!(message.contains("cleanup locks executed without store"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_cleanup_locks_clone_resets_store() {
        #[derive(Clone)]
        struct NoopPlan;

        #[async_trait]
        impl Plan for NoopPlan {
            type Result = ();

            async fn execute(&self) -> Result<Self::Result> {
                Ok(())
            }
        }

        let plan = CleanupLocks {
            inner: NoopPlan,
            ctx: ResolveLocksContext::default(),
            options: ResolveLocksOptions::default(),
            store: Some(RegionStore::new(
                MockPdClient::region1(),
                Arc::new(MockKvClient::default()),
                "mock://41".to_owned(),
            )),
            pd_client: Arc::new(MockPdClient::default()),
            keyspace: Keyspace::Disable,
        };
        let cloned = plan.clone();

        assert!(plan.store.is_some());
        assert!(cloned.store.is_none());
    }

    #[test]
    fn test_collect_single_merge_requires_exactly_one_response() {
        let merge = CollectSingle;
        let err =
            <CollectSingle as Merge<kvrpcpb::RawGetResponse>>::merge(&merge, vec![]).unwrap_err();
        assert!(matches!(err, Error::InternalError { .. }));

        let err = <CollectSingle as Merge<kvrpcpb::RawGetResponse>>::merge(
            &merge,
            vec![
                Ok(kvrpcpb::RawGetResponse::default()),
                Ok(kvrpcpb::RawGetResponse::default()),
            ],
        )
        .unwrap_err();
        assert!(matches!(err, Error::InternalError { .. }));

        let out = <CollectSingle as Merge<kvrpcpb::RawGetResponse>>::merge(
            &merge,
            vec![Ok(kvrpcpb::RawGetResponse::default())],
        )
        .unwrap();
        let mut out = out;
        assert!(out.key_errors().is_none());
    }

    #[test]
    fn test_classify_cleanup_extracted_errors_keeps_key_error() {
        let mut result = CleanupLocksResult::default();
        classify_cleanup_extracted_errors(&mut result, vec![Error::KeyError(Box::default())]);

        assert!(result.region_error.is_none());
        let errors = result
            .key_error
            .expect("cleanup result should keep key errors");
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], Error::KeyError(_)));
    }

    #[test]
    fn test_classify_cleanup_extracted_errors_sets_region_error() {
        let mut result = CleanupLocksResult::default();
        let mut region_error = errorpb::Error::default();
        region_error.message = "region error".to_string();
        classify_cleanup_extracted_errors(
            &mut result,
            vec![Error::RegionError(Box::new(region_error))],
        );

        assert!(result.key_error.is_none());
        assert_eq!(
            result.region_error.expect("expected region error").message,
            "region error"
        );
    }

    #[test]
    fn test_classify_cleanup_extracted_errors_prefers_key_error_when_region_is_last() {
        let mut result = CleanupLocksResult::default();
        let mut region_error = errorpb::Error::default();
        region_error.message = "region error".to_string();
        classify_cleanup_extracted_errors(
            &mut result,
            vec![
                Error::KeyError(Box::default()),
                Error::RegionError(Box::new(region_error)),
            ],
        );

        assert!(result.region_error.is_none());
        let errors = result
            .key_error
            .expect("cleanup result should preserve key error vectors");
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().any(|err| matches!(err, Error::KeyError(_))));
    }

    #[tokio::test]
    async fn test_on_region_epoch_not_match_missing_region_epoch_does_not_panic() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );

        let mut current_region = metapb::Region::default();
        current_region.id = store.region_with_leader.id();
        current_region.region_epoch = None;

        let mut err = EpochNotMatch::default();
        err.current_regions = vec![current_region];

        let resolved = on_region_epoch_not_match(pd_client, store, err)
            .await
            .unwrap();
        assert!(!resolved);
    }

    #[tokio::test]
    async fn test_on_region_epoch_not_match_empty_current_regions_invalidates_and_backoffs() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );
        let ver_id = store.region_with_leader.ver_id();

        let err = EpochNotMatch::default();

        let resolved = on_region_epoch_not_match(pd_client.clone(), store, err)
            .await
            .unwrap();
        assert!(!resolved);
        assert_eq!(pd_client.invalidated_region_ver_ids(), vec![ver_id]);
    }

    #[tokio::test]
    async fn test_on_region_epoch_not_match_applies_current_regions_and_retries_immediately() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );
        let ver_id = store.region_with_leader.ver_id();
        let leader_store_id = store
            .region_with_leader
            .get_store_id()
            .expect("region store must have leader");

        let mut current_region = metapb::Region::default();
        current_region.id = store.region_with_leader.id();
        current_region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: ver_id.conf_ver,
            version: ver_id.ver + 1,
        });
        current_region.peers = store.region_with_leader.region.peers.clone();

        let mut err = EpochNotMatch::default();
        err.current_regions = vec![current_region];

        let resolved = on_region_epoch_not_match(pd_client.clone(), store, err)
            .await
            .unwrap();
        assert!(resolved);

        let added = pd_client.added_regions_to_cache();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].region.id, ver_id.id);
        assert_eq!(
            added[0].leader.as_ref().map(|peer| peer.store_id),
            Some(leader_store_id)
        );
        assert_eq!(pd_client.invalidated_region_ver_ids(), vec![ver_id]);
    }

    #[tokio::test]
    async fn test_on_region_epoch_not_match_epoch_ahead_retries_with_backoff() {
        let pd_client = Arc::new(MockPdClient::default());
        let mut region = MockPdClient::region1();
        region.region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: 10,
            version: 10,
        });
        let store = RegionStore::new(
            region,
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );

        let mut current_region = metapb::Region::default();
        current_region.id = store.region_with_leader.id();
        current_region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let mut err = EpochNotMatch::default();
        err.current_regions = vec![current_region];

        let resolved = on_region_epoch_not_match(pd_client.clone(), store, err)
            .await
            .unwrap();
        assert!(!resolved);
        assert!(pd_client.invalidated_region_ver_ids().is_empty());
        assert!(pd_client.added_regions_to_cache().is_empty());
    }

    #[tokio::test]
    async fn test_handle_region_error_epoch_not_match_with_current_regions_retries_immediately() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );
        let ver_id = store.region_with_leader.ver_id();
        let leader_store_id = store
            .region_with_leader
            .get_store_id()
            .expect("expected leader store id");

        let mut current_region = metapb::Region::default();
        current_region.id = store.region_with_leader.id();
        current_region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: ver_id.conf_ver,
            version: ver_id.ver + 1,
        });
        current_region.peers = store.region_with_leader.region.peers.clone();

        let mut err = errorpb::Error::default();
        err.epoch_not_match = Some(EpochNotMatch {
            current_regions: vec![current_region],
        });

        let resolved = handle_region_error(pd_client.clone(), err, store)
            .await
            .unwrap();
        assert!(resolved);

        let added = pd_client.added_regions_to_cache();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].region.id, ver_id.id);
        assert_eq!(
            added[0].leader.as_ref().map(|peer| peer.store_id),
            Some(leader_store_id)
        );
        assert_eq!(pd_client.invalidated_region_ver_ids(), vec![ver_id]);
    }

    #[tokio::test]
    async fn test_handle_region_error_epoch_not_match_empty_current_regions_retries_with_backoff() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );
        let ver_id = store.region_with_leader.ver_id();

        let mut err = errorpb::Error::default();
        err.epoch_not_match = Some(EpochNotMatch::default());

        let resolved = handle_region_error(pd_client.clone(), err, store)
            .await
            .unwrap();
        assert!(!resolved);
        assert_eq!(pd_client.invalidated_region_ver_ids(), vec![ver_id]);
    }

    #[tokio::test]
    async fn test_handle_region_error_not_leader_with_leader_retries_immediately() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );

        let mut err = errorpb::Error::default();
        let mut not_leader = errorpb::NotLeader::default();
        not_leader.leader = Some(metapb::Peer {
            store_id: 123,
            ..Default::default()
        });
        err.not_leader = Some(not_leader);

        let resolved = handle_region_error(pd_client, err, store).await.unwrap();
        assert!(resolved);
    }

    #[tokio::test]
    async fn test_handle_region_error_disk_full_retries_with_backoff() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );

        let mut err = errorpb::Error::default();
        err.disk_full = Some(errorpb::DiskFull {
            store_id: vec![123],
            reason: "disk full".to_owned(),
        });

        let resolved = handle_region_error(pd_client, err, store).await.unwrap();
        assert!(!resolved);
    }

    #[tokio::test]
    async fn test_handle_region_error_read_index_not_ready_retries_with_backoff() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );

        let mut err = errorpb::Error::default();
        err.read_index_not_ready = Some(errorpb::ReadIndexNotReady {
            reason: "not ready".to_owned(),
            region_id: store.region_with_leader.id(),
        });

        let resolved = handle_region_error(pd_client, err, store).await.unwrap();
        assert!(!resolved);
    }

    #[tokio::test]
    async fn test_handle_region_error_store_not_match_invalidates_region_and_store_cache() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );

        let ver_id = store.region_with_leader.ver_id();
        let store_id = store
            .region_with_leader
            .get_store_id()
            .expect("expected leader store id");

        let mut err = errorpb::Error::default();
        err.store_not_match = Some(errorpb::StoreNotMatch {
            request_store_id: store_id,
            actual_store_id: store_id + 1,
        });

        let resolved = handle_region_error(pd_client.clone(), err, store)
            .await
            .unwrap();
        assert!(!resolved);

        assert!(pd_client.invalidated_region_ver_ids().contains(&ver_id));
        assert!(pd_client.invalidated_store_ids().contains(&store_id));
    }

    #[tokio::test]
    async fn test_handle_region_error_flashback_in_progress_returns_error() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );

        let mut err = errorpb::Error::default();
        err.flashback_in_progress = Some(errorpb::FlashbackInProgress {
            region_id: store.region_with_leader.id(),
            flashback_start_ts: 42,
        });

        let err = handle_region_error(pd_client, err, store)
            .await
            .expect_err("flashback_in_progress should not be retryable");
        match err {
            Error::RegionError(inner) => {
                assert!(inner.flashback_in_progress.is_some());
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handle_region_error_invalid_max_ts_update_returns_error() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                unreachable!("dispatch not expected")
            })),
            "mock://41".to_owned(),
        );

        let mut err = errorpb::Error::default();
        err.message = "invalid max_ts update".to_owned();

        let err = handle_region_error(pd_client, err, store)
            .await
            .expect_err("invalid max_ts update should not be retryable");
        match err {
            Error::RegionError(inner) => {
                assert!(inner.message.contains("invalid max_ts update"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_dispatch_execute_missing_kv_client_returns_error() {
        let plan = Dispatch {
            request: kvrpcpb::GetRequest::default(),
            kv_client: None,
        };

        let err = plan.execute().await.unwrap_err();
        match err {
            Error::InternalError { message } => {
                assert!(message.contains("kv_client has not been initialised in Dispatch"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_dispatch_execute_downcast_failure_returns_error() {
        let kv_client: Arc<dyn KvClient + Send + Sync> =
            Arc::new(MockKvClient::with_dispatch_hook(|_| Ok(Box::new(()))));
        let plan = Dispatch {
            request: kvrpcpb::GetRequest::default(),
            kv_client: Some(kv_client),
        };

        let err = plan.execute().await.unwrap_err();
        match err {
            Error::InternalError { message } => {
                assert!(message.contains("downcast failed"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_cleanup_locks_scan_lock_stops_at_shard_end_key_without_extra_scan() -> Result<()>
    {
        #[derive(Clone)]
        struct CountingScanLockPlan {
            execute_calls: Arc<AtomicUsize>,
            next_batch_calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl Plan for CountingScanLockPlan {
            type Result = kvrpcpb::ScanLockResponse;

            async fn execute(&self) -> Result<Self::Result> {
                let call = self.execute_calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    Ok(kvrpcpb::ScanLockResponse {
                        locks: vec![kvrpcpb::LockInfo {
                            key: vec![10],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                } else {
                    Ok(kvrpcpb::ScanLockResponse::default())
                }
            }
        }

        impl Shardable for CountingScanLockPlan {
            type Shard = (Vec<u8>, Vec<u8>);

            fn shards(
                &self,
                _: &Arc<impl crate::pd::PdClient>,
            ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
                Box::pin(stream::empty()).boxed()
            }

            fn apply_shard(&mut self, _: Self::Shard) {}

            fn apply_store(&mut self, _: &RegionStore) -> Result<()> {
                Ok(())
            }
        }

        impl NextBatch for CountingScanLockPlan {
            fn next_batch(&mut self, _: (Vec<u8>, Vec<u8>)) {
                self.next_batch_calls.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl HasKvContext for CountingScanLockPlan {
            fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
                None
            }
        }

        let execute_calls = Arc::new(AtomicUsize::new(0));
        let next_batch_calls = Arc::new(AtomicUsize::new(0));
        let inner = PreserveShard {
            inner: CountingScanLockPlan {
                execute_calls: execute_calls.clone(),
                next_batch_calls: next_batch_calls.clone(),
            },
            shard: Some((vec![10], vec![10, 0])),
        };

        let plan = CleanupLocks {
            inner,
            ctx: ResolveLocksContext::default(),
            options: ResolveLocksOptions {
                async_commit_only: true,
                batch_size: 1,
            },
            store: Some(RegionStore::new(
                MockPdClient::region2(),
                Arc::new(MockKvClient::default()),
                "mock://42".to_owned(),
            )),
            pd_client: Arc::new(MockPdClient::default()),
            keyspace: Keyspace::Disable,
        };

        let result = plan.execute().await?;
        assert_eq!(result.resolved_locks, 0);
        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(next_batch_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_locks_propagates_inner_kv_context_to_lock_resolver_requests() -> Result<()>
    {
        #[derive(Clone)]
        struct ContextScanLockPlan {
            context: kvrpcpb::Context,
            execute_calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl Plan for ContextScanLockPlan {
            type Result = kvrpcpb::ScanLockResponse;

            async fn execute(&self) -> Result<Self::Result> {
                let call = self.execute_calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    Ok(kvrpcpb::ScanLockResponse {
                        locks: vec![kvrpcpb::LockInfo {
                            key: vec![1],
                            primary_lock: vec![1],
                            lock_version: 7,
                            lock_ttl: 100,
                            txn_size: 1,
                            lock_type: kvrpcpb::Op::Put as i32,
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                } else {
                    Ok(kvrpcpb::ScanLockResponse::default())
                }
            }
        }

        impl Shardable for ContextScanLockPlan {
            type Shard = ();

            fn shards(
                &self,
                _: &Arc<impl crate::pd::PdClient>,
            ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
                Box::pin(stream::empty()).boxed()
            }

            fn apply_shard(&mut self, _: Self::Shard) {}

            fn apply_store(&mut self, _: &RegionStore) -> Result<()> {
                Ok(())
            }
        }

        impl NextBatch for ContextScanLockPlan {
            fn next_batch(&mut self, _: (Vec<u8>, Vec<u8>)) {}
        }

        impl HasKvContext for ContextScanLockPlan {
            fn kv_context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
                Some(&mut self.context)
            }
        }

        let mut template_context = kvrpcpb::Context::default();
        template_context.region_id = 999;
        template_context.disk_full_opt = 2;
        template_context.txn_source = 7;
        template_context.sync_log = true;
        template_context.priority = 2;
        template_context.max_execution_duration_ms = 321;
        template_context.resource_group_tag = b"rg-tag".to_vec();
        template_context.resource_control_context = Some(kvrpcpb::ResourceControlContext {
            resource_group_name: "rg-name".to_owned(),
            ..Default::default()
        });
        template_context.request_source = "request-source".to_owned();

        let expected_tag = template_context.resource_group_tag.clone();
        let expected_request_source = template_context.request_source.clone();
        let expected_resource_group_name = template_context
            .resource_control_context
            .as_ref()
            .expect("resource control context")
            .resource_group_name
            .clone();

        let kv_client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                let ctx = req.context.as_ref().expect("context");
                assert_eq!(ctx.disk_full_opt, 2);
                assert_eq!(ctx.txn_source, 7);
                assert!(ctx.sync_log);
                assert_eq!(ctx.priority, 2);
                assert_eq!(ctx.max_execution_duration_ms, 321);
                assert_eq!(ctx.request_source, expected_request_source);
                assert_eq!(ctx.resource_group_tag, expected_tag);
                assert_eq!(
                    ctx.resource_control_context
                        .as_ref()
                        .expect("resource control context")
                        .resource_group_name,
                    expected_resource_group_name
                );

                assert_eq!(
                    ctx.region_id, 1,
                    "region routing fields should come from set_leader"
                );
                assert_eq!(ctx.peer.as_ref().expect("peer").store_id, 41);

                let resp = kvrpcpb::CheckTxnStatusResponse {
                    commit_version: 5,
                    action: kvrpcpb::Action::NoAction as i32,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if req.is::<kvrpcpb::ResolveLockRequest>() {
                let req = req
                    .downcast_ref::<kvrpcpb::ResolveLockRequest>()
                    .expect("resolve lock request");
                let ctx = req.context.as_ref().expect("context");
                assert_eq!(ctx.disk_full_opt, 2);
                assert_eq!(ctx.txn_source, 7);
                assert!(ctx.sync_log);
                assert_eq!(ctx.priority, 2);
                assert_eq!(ctx.max_execution_duration_ms, 321);
                assert_eq!(ctx.request_source, expected_request_source);
                assert_eq!(ctx.resource_group_tag, expected_tag);
                assert_eq!(
                    ctx.resource_control_context
                        .as_ref()
                        .expect("resource control context")
                        .resource_group_name,
                    expected_resource_group_name
                );

                assert_eq!(
                    ctx.region_id, 1,
                    "region routing fields should come from set_leader"
                );
                assert_eq!(ctx.peer.as_ref().expect("peer").store_id, 41);

                return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(kv_client.clone()));
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(kv_client.clone()),
            "mock://41".to_owned(),
        );

        let plan = CleanupLocks {
            inner: ContextScanLockPlan {
                context: template_context,
                execute_calls: Arc::new(AtomicUsize::new(0)),
            },
            ctx: ResolveLocksContext::default(),
            options: ResolveLocksOptions {
                async_commit_only: false,
                batch_size: 1024,
            },
            store: Some(store),
            pd_client,
            keyspace: Keyspace::Disable,
        };

        let result = plan.execute().await?;
        assert_eq!(result.resolved_locks, 1);
        Ok(())
    }
}
