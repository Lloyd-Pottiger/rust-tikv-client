// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Utility helpers used across the client.
//!
//! Some items in this module intentionally mirror client-go `util` APIs.

pub mod bytes;
mod exec_details;
pub mod gc_time;
#[doc(hidden)]
pub mod iter;
mod misc;
pub mod rate_limit;
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
pub use exec_details::trace_exec_details_enabled;
pub use exec_details::with_exec_details;
pub use exec_details::with_trace_exec_details;
pub use exec_details::CommitDetails;
pub use exec_details::CommitTSLagDetails;
pub use exec_details::ExecDetails;
pub use exec_details::LockKeysDetails;
pub use exec_details::RUDetails;
pub use exec_details::ReqDetailInfo;
pub use exec_details::ScanDetail;
pub use exec_details::TiKVExecDetails;
pub use exec_details::TimeDetail;
pub use exec_details::TrafficDetails;
pub use exec_details::WriteDetail;
pub use gc_time::compatible_parse_gc_time;
pub use gc_time::GcTimeParseError;
pub use gc_time::GC_TIME_FORMAT;
pub use misc::session_id;
pub use misc::with_recovery;
pub use misc::with_session_id;
pub use rate_limit::RateLimit;
pub use rate_limit::RateLimitError;
pub use rate_limit::RateLimitPermit;
pub use ts_set::TsSet;

pub(crate) use exec_details::record_task_local_backoff;
pub(crate) use exec_details::record_task_local_wait_kv_response;
pub(crate) use exec_details::record_task_local_wait_pd_response;
pub(crate) use exec_details::scope_task_exec_details;
pub(crate) use misc::scope_task_session_id;
