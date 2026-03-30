// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Stable wrapper types for common `kvrpcpb::Context` fields.
//!
//! TiKV RPCs attach extra metadata via `kvrpcpb::Context` (e.g. priority,
//! isolation level). We expose a small, stable set of enums so applications
//! don't need to depend on the generated protobuf types directly.

use std::future::Future;

tokio::task_local! {
    static TASK_REQUEST_SOURCE: String;
}

tokio::task_local! {
    static TASK_RESOURCE_GROUP_NAME: String;
}

/// Marker key type for request source type stored in task-local context.
///
/// This mirrors client-go `util.RequestSourceTypeKeyType` and `util.RequestSourceTypeKey`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RequestSourceTypeKey;

/// Type alias kept for client-go API parity.
pub type RequestSourceTypeKeyType = RequestSourceTypeKey;

/// Marker key type for request source stored in task-local context.
///
/// This mirrors client-go `util.RequestSourceKeyType` and `util.RequestSourceKey`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RequestSourceKey;

/// Type alias kept for client-go API parity.
pub type RequestSourceKeyType = RequestSourceKey;

/// Marker key type for resource group name stored in task-local context.
///
/// This mirrors client-go `util.ResourceGroupNameKeyType` and `util.ResourceGroupNameKey`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResourceGroupNameKey;

/// Type alias kept for client-go API parity.
pub type ResourceGroupNameKeyType = ResourceGroupNameKey;

/// The priority of commands executed by TiKV.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum CommandPriority {
    /// Normal is the default value.
    #[default]
    Normal = 0,
    /// Lower-priority work that can yield to normal traffic.
    Low = 1,
    /// Higher-priority work that should preempt normal traffic.
    High = 2,
}

/// Transaction isolation level for reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum IsolationLevel {
    /// Snapshot isolation (default).
    #[default]
    Si = 0,
    /// Read committed.
    Rc = 1,
    /// Read committed + extra check for more recent versions.
    RcCheckTs = 2,
}

/// Used to tell TiKV whether operations are allowed or not on different disk usages.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum DiskFullOpt {
    /// Disallow operations on both almost-full and already-full disks.
    #[default]
    NotAllowedOnFull = 0,
    /// Allow operations when disk is almost full.
    AllowedOnAlmostFull = 1,
    /// Allow operations when disk is already full.
    AllowedOnAlreadyFull = 2,
}

/// Control flags for trace logging behavior (`kvrpcpb::Context.trace_control_flags`).
///
/// This mirrors client-go's `trace.TraceControlFlags` bitset.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TraceControlFlags(u64);

impl TraceControlFlags {
    /// Forces immediate logging without buffering.
    pub const IMMEDIATE_LOG: Self = Self(1 << 0);
    /// Enables TiKV request category tracing.
    pub const TIKV_CATEGORY_REQUEST: Self = Self(1 << 1);
    /// Enables TiKV detailed write operation tracing.
    pub const TIKV_CATEGORY_WRITE_DETAILS: Self = Self(1 << 2);
    /// Enables TiKV detailed read operation tracing.
    pub const TIKV_CATEGORY_READ_DETAILS: Self = Self(1 << 3);

    /// Returns the raw flag bits.
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Returns true if `flag` is set.
    pub const fn has(self, flag: TraceControlFlags) -> bool {
        (self.0 & flag.0) != 0
    }

    /// Returns a new flags value with `flag` set.
    #[must_use]
    pub const fn with(self, flag: TraceControlFlags) -> TraceControlFlags {
        TraceControlFlags(self.0 | flag.0)
    }
}

impl From<u64> for TraceControlFlags {
    fn from(bits: u64) -> Self {
        TraceControlFlags(bits)
    }
}

impl From<TraceControlFlags> for u64 {
    fn from(flags: TraceControlFlags) -> Self {
        flags.bits()
    }
}

/// Internal request type used for low-resource internal tasks.
pub const INTERNAL_TXN_OTHERS: &str = "others";
/// Internal request type used by GC tasks.
pub const INTERNAL_TXN_GC: &str = "gc";
/// Internal request type used by miscellaneous meta tasks.
pub const INTERNAL_TXN_META: &str = INTERNAL_TXN_OTHERS;
/// Internal request type used by statistics tasks.
pub const INTERNAL_TXN_STATS: &str = "stats";

/// Empty explicit request-source type.
pub const EXPLICIT_TYPE_EMPTY: &str = "";
/// Deprecated explicit request-source type kept for compatibility.
pub const EXPLICIT_TYPE_LIGHTNING: &str = "lightning";
/// BR explicit request-source type.
pub const EXPLICIT_TYPE_BR: &str = "br";
/// Dumpling explicit request-source type.
pub const EXPLICIT_TYPE_DUMPLING: &str = "dumpling";
/// Background-task explicit request-source type.
pub const EXPLICIT_TYPE_BACKGROUND: &str = "background";
/// DDL explicit request-source type.
pub const EXPLICIT_TYPE_DDL: &str = "ddl";
/// Statistics explicit request-source type.
pub const EXPLICIT_TYPE_STATS: &str = "stats";
/// Import explicit request-source type.
pub const EXPLICIT_TYPE_IMPORT: &str = "import";

/// List of client-go-compatible explicit request-source types.
pub const EXPLICIT_TYPE_LIST: &[&str] = &[
    EXPLICIT_TYPE_EMPTY,
    EXPLICIT_TYPE_LIGHTNING,
    EXPLICIT_TYPE_BR,
    EXPLICIT_TYPE_DUMPLING,
    EXPLICIT_TYPE_BACKGROUND,
    EXPLICIT_TYPE_DDL,
    EXPLICIT_TYPE_STATS,
    EXPLICIT_TYPE_IMPORT,
];

/// Internal request scope label.
pub const INTERNAL_REQUEST: &str = "internal";
/// Internal request prefix label.
pub const INTERNAL_REQUEST_PREFIX: &str = "internal_";
/// External request scope label.
pub const EXTERNAL_REQUEST: &str = "external";
/// Unknown request-source label.
pub const SOURCE_UNKNOWN: &str = "unknown";

/// A structured builder for the `kvrpcpb::Context.request_source` label.
///
/// This matches the label format used by TiKV's Go client:
/// - `unknown` when neither `source_type` nor `explicit_type` is set (regardless of `internal`).
/// - Otherwise: `{internal|external}_{source|unknown}[_explicit]`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RequestSource {
    internal: bool,
    source_type: String,
    explicit_type: String,
}

impl RequestSource {
    /// Create a new request source descriptor.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether this request is considered internal.
    #[must_use]
    pub fn internal(mut self, internal: bool) -> Self {
        self.internal = internal;
        self
    }

    /// Set the primary request source type.
    #[must_use]
    pub fn source_type(mut self, source_type: impl Into<String>) -> Self {
        self.source_type = source_type.into();
        self
    }

    /// Set the explicit request source type (for example, a session or task type).
    #[must_use]
    pub fn explicit_type(mut self, explicit_type: impl Into<String>) -> Self {
        self.explicit_type = explicit_type.into();
        self
    }

    fn is_empty(&self) -> bool {
        self.source_type.is_empty() && self.explicit_type.is_empty()
    }
}

impl std::fmt::Display for RequestSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return f.write_str(SOURCE_UNKNOWN);
        }

        let origin = if self.internal {
            INTERNAL_REQUEST
        } else {
            EXTERNAL_REQUEST
        };
        let source = if self.source_type.is_empty() {
            SOURCE_UNKNOWN
        } else {
            self.source_type.as_str()
        };

        write!(f, "{origin}_{source}")?;

        if !self.explicit_type.is_empty() && self.explicit_type != self.source_type {
            write!(f, "_{}", self.explicit_type)?;
        }

        Ok(())
    }
}

/// Build a client-go-compatible request source label from its structured parts.
#[must_use]
#[doc(alias = "BuildRequestSource")]
pub fn build_request_source(
    internal: bool,
    source: impl Into<String>,
    explicit_source: impl Into<String>,
) -> String {
    RequestSource::new()
        .internal(internal)
        .source_type(source)
        .explicit_type(explicit_source)
        .to_string()
}

/// Runs `future` with a task-local request source label.
///
/// This mirrors client-go request-source context propagation, but uses Tokio
/// task-local storage instead of `context.Context`.
pub fn with_request_source<T, F>(
    request_source: impl Into<String>,
    future: F,
) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_REQUEST_SOURCE.scope(request_source.into(), future)
}

/// Runs `future` with a client-go-compatible internal request source label.
#[doc(alias = "WithInternalSourceType")]
pub fn with_internal_source_type<T, F>(
    source: impl Into<String>,
    future: F,
) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    with_request_source(
        build_request_source(true, source, EXPLICIT_TYPE_EMPTY),
        future,
    )
}

/// Runs `future` with a client-go-compatible internal request source label plus task type.
#[doc(alias = "WithInternalSourceAndTaskType")]
pub fn with_internal_source_and_task_type<T, F>(
    source: impl Into<String>,
    task_name: impl Into<String>,
    future: F,
) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    with_request_source(build_request_source(true, source, task_name), future)
}

/// Returns the task-local request source label, if present.
#[must_use]
#[doc(alias = "RequestSourceFromCtx")]
pub fn request_source() -> Option<String> {
    TASK_REQUEST_SOURCE.try_with(Clone::clone).ok()
}

/// Runs `future` with a task-local resource group name.
///
/// This mirrors client-go `util.WithResourceGroupName`, but uses Tokio
/// task-local storage instead of `context.Context`.
#[doc(alias = "WithResourceGroupName")]
pub fn with_resource_group_name<T, F>(
    resource_group_name: impl Into<String>,
    future: F,
) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_RESOURCE_GROUP_NAME.scope(resource_group_name.into(), future)
}

/// Returns the task-local resource group name, if present.
#[must_use]
#[doc(alias = "ResourceGroupNameFromCtx")]
pub fn resource_group_name() -> Option<String> {
    TASK_RESOURCE_GROUP_NAME.try_with(Clone::clone).ok()
}

pub(crate) async fn scope_task_request_metadata<T, F>(
    request_source: Option<String>,
    resource_group_name: Option<String>,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    match (request_source, resource_group_name) {
        (Some(request_source), Some(resource_group_name)) => {
            with_request_source(
                request_source,
                with_resource_group_name(resource_group_name, future),
            )
            .await
        }
        (Some(request_source), None) => with_request_source(request_source, future).await,
        (None, Some(resource_group_name)) => {
            with_resource_group_name(resource_group_name, future).await
        }
        (None, None) => future.await,
    }
}

/// Returns true if `request_source` represents an internal request.
///
/// This matches client-go's `IsInternalRequest` behavior (checks for the `"internal"` prefix).
#[must_use]
#[doc(alias = "IsInternalRequest")]
pub fn is_internal_request(request_source: &str) -> bool {
    request_source.starts_with(INTERNAL_REQUEST)
}

pub(crate) fn is_internal_request_source(request_source: &str) -> bool {
    is_internal_request(request_source)
}

#[cfg(test)]
mod tests {
    use super::{
        build_request_source, is_internal_request, is_internal_request_source, request_source,
        resource_group_name, with_internal_source_and_task_type, with_internal_source_type,
        with_request_source, with_resource_group_name, RequestSource, TraceControlFlags,
        EXPLICIT_TYPE_EMPTY,
    };

    #[test]
    fn test_request_source_formatting_matches_client_go() {
        assert_eq!(RequestSource::default().to_string(), "unknown");
        assert_eq!(RequestSource::new().internal(true).to_string(), "unknown");

        assert_eq!(
            RequestSource::new()
                .internal(true)
                .source_type("gc")
                .to_string(),
            "internal_gc"
        );
        assert_eq!(
            RequestSource::new()
                .internal(false)
                .source_type("gc")
                .to_string(),
            "external_gc"
        );

        assert_eq!(
            RequestSource::new().explicit_type("br").to_string(),
            "external_unknown_br"
        );
        assert_eq!(
            RequestSource::new()
                .source_type("br")
                .explicit_type("br")
                .to_string(),
            "external_br"
        );
        assert_eq!(
            RequestSource::new()
                .internal(true)
                .source_type("gc")
                .explicit_type("stats")
                .to_string(),
            "internal_gc_stats"
        );

        assert!(is_internal_request_source("internal_gc"));
        assert!(is_internal_request_source("internal_gc_stats"));
        assert!(!is_internal_request_source("external_gc"));
        assert!(!is_internal_request_source("unknown"));
    }

    #[test]
    fn test_build_request_source_matches_client_go() {
        assert_eq!(
            build_request_source(true, "gc", EXPLICIT_TYPE_EMPTY),
            "internal_gc"
        );
        assert_eq!(
            build_request_source(true, "gc", "stats"),
            "internal_gc_stats"
        );
        assert_eq!(
            build_request_source(false, "", "import"),
            "external_unknown_import"
        );
        assert_eq!(build_request_source(true, "", ""), "unknown");
        assert!(is_internal_request("internal_gc"));
        assert!(!is_internal_request("external_gc"));
    }

    #[test]
    fn test_trace_control_flags_bits_match_client_go() {
        assert_eq!(TraceControlFlags::IMMEDIATE_LOG.bits(), 1 << 0);
        assert_eq!(TraceControlFlags::TIKV_CATEGORY_REQUEST.bits(), 1 << 1);
        assert_eq!(
            TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS.bits(),
            1 << 2
        );
        assert_eq!(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS.bits(), 1 << 3);
    }

    #[test]
    fn test_trace_control_flags_has_and_with() {
        let flags = TraceControlFlags::default()
            .with(TraceControlFlags::IMMEDIATE_LOG)
            .with(TraceControlFlags::TIKV_CATEGORY_REQUEST);

        assert!(flags.has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(flags.has(TraceControlFlags::TIKV_CATEGORY_REQUEST));
        assert!(!flags.has(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
    }

    #[tokio::test]
    async fn test_task_local_request_metadata_scopes_restore_outer_values() {
        assert_eq!(request_source(), None);
        assert_eq!(resource_group_name(), None);

        with_request_source("external_br", async {
            assert_eq!(request_source().as_deref(), Some("external_br"));
            assert_eq!(resource_group_name(), None);

            with_resource_group_name("rg-outer", async {
                assert_eq!(request_source().as_deref(), Some("external_br"));
                assert_eq!(resource_group_name().as_deref(), Some("rg-outer"));

                with_internal_source_and_task_type("gc", "stats", async {
                    assert_eq!(request_source().as_deref(), Some("internal_gc_stats"));
                    assert_eq!(resource_group_name().as_deref(), Some("rg-outer"));
                })
                .await;

                with_internal_source_type("gc", async {
                    assert_eq!(request_source().as_deref(), Some("internal_gc"));
                    assert_eq!(resource_group_name().as_deref(), Some("rg-outer"));
                })
                .await;
            })
            .await;

            assert_eq!(request_source().as_deref(), Some("external_br"));
            assert_eq!(resource_group_name(), None);
        })
        .await;

        assert_eq!(request_source(), None);
        assert_eq!(resource_group_name(), None);
    }
}
