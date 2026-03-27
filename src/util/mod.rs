// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Utility helpers used across the client.
//!
//! Some items in this module intentionally mirror client-go `util` APIs.

pub mod r#async;
pub mod bytes;
pub mod codec;
mod exec_details;
mod failpoint;
pub mod gc_time;
#[doc(hidden)]
pub mod iter;
mod misc;
pub mod rate_limit;
pub mod redact;
pub mod ts_set;

pub use crate::request_context::build_request_source;
pub use crate::request_context::is_internal_request;
pub use crate::request_context::request_source;
pub use crate::request_context::resource_group_name;
pub use crate::request_context::with_internal_source_and_task_type;
pub use crate::request_context::with_internal_source_type;
pub use crate::request_context::with_request_source;
pub use crate::request_context::with_resource_group_name;
pub use crate::request_context::RequestSource;
pub use crate::request_context::RequestSourceKey;
pub use crate::request_context::RequestSourceKeyType;
pub use crate::request_context::RequestSourceTypeKey;
pub use crate::request_context::RequestSourceTypeKeyType;
pub use crate::request_context::EXPLICIT_TYPE_BACKGROUND;
pub use crate::request_context::EXPLICIT_TYPE_BR;
pub use crate::request_context::EXPLICIT_TYPE_DDL;
pub use crate::request_context::EXPLICIT_TYPE_DUMPLING;
pub use crate::request_context::EXPLICIT_TYPE_EMPTY;
pub use crate::request_context::EXPLICIT_TYPE_IMPORT;
pub use crate::request_context::EXPLICIT_TYPE_LIGHTNING;
pub use crate::request_context::EXPLICIT_TYPE_LIST;
pub use crate::request_context::EXPLICIT_TYPE_STATS;
pub use crate::request_context::EXTERNAL_REQUEST;
pub use crate::request_context::INTERNAL_REQUEST;
pub use crate::request_context::INTERNAL_REQUEST_PREFIX;
pub use crate::request_context::INTERNAL_TXN_GC;
pub use crate::request_context::INTERNAL_TXN_META;
pub use crate::request_context::INTERNAL_TXN_OTHERS;
pub use crate::request_context::INTERNAL_TXN_STATS;
pub use crate::request_context::SOURCE_UNKNOWN;
pub use bytes::bytes_to_string;
pub use bytes::format_bytes;
pub use exec_details::exec_details;
pub use exec_details::format_duration;
pub use exec_details::ru_details;
pub use exec_details::trace_exec_details_enabled;
pub use exec_details::with_exec_details;
pub use exec_details::with_ru_details;
pub use exec_details::with_trace_exec_details;
pub use exec_details::CommitDetailCtxKey;
pub use exec_details::CommitDetails;
pub use exec_details::CommitTSLagDetails;
pub use exec_details::ExecDetails;
pub use exec_details::ExecDetailsKey;
pub use exec_details::LockKeysDetailCtxKey;
pub use exec_details::LockKeysDetails;
pub use exec_details::RUDetails;
pub use exec_details::RUDetailsCtxKey;
pub use exec_details::ReqDetailInfo;
pub use exec_details::ScanDetail;
pub use exec_details::TiKVExecDetails;
pub use exec_details::TimeDetail;
pub use exec_details::TrafficDetails;
pub use exec_details::WriteDetail;
pub use failpoint::enable_failpoints;
pub use failpoint::eval_failpoint;
pub use failpoint::is_failpoint_active;
pub(crate) use failpoint::sleep_backoff;
pub use failpoint::FailpointsDisabledError;
pub use gc_time::compatible_parse_gc_time;
pub use gc_time::GcTimeParseError;
pub use gc_time::GC_TIME_FORMAT;
pub use misc::session_id;
pub use misc::with_recovery;
pub use misc::with_session_id;
pub use misc::SessionID;
pub use rate_limit::RateLimit;
pub use rate_limit::RateLimitError;
pub use rate_limit::RateLimitPermit;
pub use ts_set::TsSet;

pub(crate) use exec_details::record_task_local_backoff;
pub(crate) use exec_details::record_task_local_kv_traffic;
pub(crate) use exec_details::record_task_local_ru_details;
pub(crate) use exec_details::record_task_local_wait_kv_response;
pub(crate) use exec_details::record_task_local_wait_pd_response;
pub(crate) use exec_details::scope_task_exec_details;
pub(crate) use exec_details::scope_task_ru_details;
pub(crate) use exec_details::scope_task_traffic_kind;
pub(crate) use exec_details::task_traffic_kind;
pub(crate) use misc::scope_task_session_id;

pub(crate) fn spawn_with_inherited_task_locals<F, T>(future: F) -> tokio::task::JoinHandle<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let parent_trace_id = crate::trace::trace_id();
    let parent_trace_control_flags = crate::trace::trace_control_flags();
    let parent_exec_details = exec_details();
    let parent_ru_details = ru_details();
    let parent_trace_exec_details = trace_exec_details_enabled();
    let parent_request_source = crate::request_context::request_source();
    let parent_resource_group_name = crate::request_context::resource_group_name();
    let parent_shutting_down = crate::shutting_down::task_shutting_down();
    let parent_session_id = session_id();
    tokio::spawn(async move {
        let future = scope_task_session_id(parent_session_id, future);
        let future =
            scope_task_exec_details(parent_exec_details, parent_trace_exec_details, future);
        let future = scope_task_ru_details(parent_ru_details, future);
        let future = crate::request_context::scope_task_request_metadata(
            parent_request_source,
            parent_resource_group_name,
            future,
        );
        let future = match parent_trace_id {
            Some(trace_id) => {
                futures::future::Either::Left(crate::trace::with_trace_id(trace_id, future))
            }
            None => futures::future::Either::Right(future),
        };
        let future = match parent_shutting_down {
            Some(flag) => futures::future::Either::Left(crate::shutting_down::with_shutting_down(
                flag, future,
            )),
            None => futures::future::Either::Right(future),
        };
        match parent_trace_control_flags {
            Some(flags) => crate::trace::with_trace_control_flags(flags, future).await,
            None => future.await,
        }
    })
}
