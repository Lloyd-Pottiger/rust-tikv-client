//! Lightweight tracing hooks and task-local propagation helpers.
//!
//! This module mirrors client-go's `trace` package at a high level. It provides task-local trace
//! ids / trace-control flags, globally configurable trace hooks, and small helpers for recording
//! trace events from request execution paths.

use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;

use lazy_static::lazy_static;

use crate::region::RegionVerId;
use crate::TraceControlFlags;

tokio::task_local! {
    static TASK_TRACE_ID: Vec<u8>;
}

tokio::task_local! {
    static TASK_TRACE_CONTROL_FLAGS: TraceControlFlags;
}

/// Runs `future` with a task-local trace id.
///
/// This mirrors client-go `trace.ContextWithTraceID`, but uses Tokio task-local storage instead of
/// Go's `context.Context`.
pub fn with_trace_id<T, F>(trace_id: impl Into<Vec<u8>>, future: F) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_TRACE_ID.scope(trace_id.into(), future)
}

/// Returns the task-local trace id, if present.
///
/// Returns `None` when no trace id is set for the current Tokio task (or when called outside a
/// Tokio task).
///
/// This mirrors client-go `trace.TraceIDFromContext`.
pub fn trace_id() -> Option<Vec<u8>> {
    TASK_TRACE_ID.try_with(|trace_id| trace_id.clone()).ok()
}

/// Runs `future` with task-local trace-control flags.
///
/// This mirrors client-go `trace.GetTraceControlFlags(ctx)` propagation, but uses Tokio
/// task-local storage instead of Go's `context.Context`.
pub fn with_trace_control_flags<T, F>(
    flags: TraceControlFlags,
    future: F,
) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_TRACE_CONTROL_FLAGS.scope(flags, future)
}

/// The function signature for providing fallback trace-control flags.
///
/// This mirrors client-go `trace.SetTraceControlExtractor`, but omits the Go `context.Context`
/// parameter and is only consulted when the current Tokio task does not have explicit task-local
/// flags.
pub type TraceControlFlagsProvider = Arc<dyn Fn() -> Option<TraceControlFlags> + Send + Sync>;

/// Returns the current trace-control flags, if present.
///
/// This first checks Tokio task-local flags. When none are set, it falls back to the registered
/// [`TraceControlFlagsProvider`], if any.
#[must_use]
pub fn trace_control_flags() -> Option<TraceControlFlags> {
    task_local_trace_control_flags().or_else(trace_control_flags_from_provider)
}

pub(crate) fn effective_trace_control_flags(configured: TraceControlFlags) -> TraceControlFlags {
    if configured.bits() != 0 {
        configured
    } else {
        trace_control_flags().unwrap_or_default()
    }
}

#[cfg(test)]
pub(crate) static TRACE_HOOK_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Category identifies a trace event family emitted by the client.
///
/// This mirrors client-go `trace.Category`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum Category {
    /// Two-phase commit prewrite and commit phases.
    Txn2Pc = 0,
    /// Lock resolution and conflict handling.
    TxnLockResolve = 1,
    /// Individual KV request send and result events.
    KvRequest = 2,
    /// Region cache operations and PD lookups.
    RegionCache = 3,
}

/// A structured trace field attached to a trace event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceField {
    /// The stable field name recorded in the trace event payload.
    pub key: &'static str,
    /// The typed field value.
    pub value: TraceValue,
}

impl TraceField {
    /// Creates a string-valued trace field.
    pub fn str(key: &'static str, value: impl Into<Cow<'static, str>>) -> Self {
        TraceField {
            key,
            value: TraceValue::Str(value.into()),
        }
    }

    /// Creates an unsigned-integer trace field.
    pub fn u64(key: &'static str, value: u64) -> Self {
        TraceField {
            key,
            value: TraceValue::U64(value),
        }
    }

    /// Creates a signed-integer trace field.
    pub fn i64(key: &'static str, value: i64) -> Self {
        TraceField {
            key,
            value: TraceValue::I64(value),
        }
    }

    /// Creates a boolean trace field.
    pub fn bool(key: &'static str, value: bool) -> Self {
        TraceField {
            key,
            value: TraceValue::Bool(value),
        }
    }

    /// Creates a bytes-valued trace field.
    pub fn bytes(key: &'static str, value: impl Into<Vec<u8>>) -> Self {
        TraceField {
            key,
            value: TraceValue::Bytes(value.into()),
        }
    }
}

/// A typed trace-field value carried by [`TraceField`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceValue {
    /// A UTF-8 string payload.
    Str(Cow<'static, str>),
    /// An unsigned integer payload.
    U64(u64),
    /// A signed integer payload.
    I64(i64),
    /// A boolean payload.
    Bool(bool),
    /// An opaque byte payload.
    Bytes(Vec<u8>),
}

/// The function signature for recording trace events.
///
/// This mirrors client-go `trace.TraceEventFunc`.
pub type TraceEventFunc = Arc<dyn Fn(Category, &str, &[TraceField]) + Send + Sync>;

/// The function signature for checking whether a category is enabled.
///
/// This mirrors client-go `trace.IsCategoryEnabledFunc`.
pub type IsCategoryEnabledFunc = Arc<dyn Fn(Category) -> bool + Send + Sync>;

fn noop_trace_event(_category: Category, _name: &str, _fields: &[TraceField]) {}
fn noop_is_category_enabled(_category: Category) -> bool {
    false
}
fn noop_trace_control_flags_provider() -> Option<TraceControlFlags> {
    None
}

fn task_local_trace_control_flags() -> Option<TraceControlFlags> {
    TASK_TRACE_CONTROL_FLAGS.try_with(|flags| *flags).ok()
}

fn trace_control_flags_from_provider() -> Option<TraceControlFlags> {
    let provider = TRACE_CONTROL_FLAGS_PROVIDER
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    provider()
}

lazy_static! {
    static ref TRACE_EVENT_FUNC: RwLock<TraceEventFunc> = RwLock::new(Arc::new(noop_trace_event));
    static ref IS_CATEGORY_ENABLED_FUNC: RwLock<IsCategoryEnabledFunc> =
        RwLock::new(Arc::new(noop_is_category_enabled));
    static ref TRACE_CONTROL_FLAGS_PROVIDER: RwLock<TraceControlFlagsProvider> =
        RwLock::new(Arc::new(noop_trace_control_flags_provider));
    static ref KV_REQUEST_REGION_RANGES: RwLock<KvRequestRegionRanges> =
        RwLock::new(HashMap::new());
}

type KvRequestRegionRange = (Vec<u8>, Vec<u8>);
type KvRequestRegionRanges = HashMap<RegionVerId, KvRequestRegionRange>;

/// Register the global trace event callback.
///
/// Passing `None` resets to a no-op implementation.
pub fn set_trace_event_func(func: Option<TraceEventFunc>) {
    let func = func.unwrap_or_else(|| Arc::new(noop_trace_event));
    let mut guard = TRACE_EVENT_FUNC.write().unwrap_or_else(|e| e.into_inner());
    *guard = func;
}

/// Register the global category enabled callback.
///
/// Passing `None` resets to a no-op implementation that returns `false`.
pub fn set_is_category_enabled_func(func: Option<IsCategoryEnabledFunc>) {
    let func = func.unwrap_or_else(|| Arc::new(noop_is_category_enabled));
    let mut guard = IS_CATEGORY_ENABLED_FUNC
        .write()
        .unwrap_or_else(|e| e.into_inner());
    *guard = func;
}

/// Register the global trace-control flags provider.
///
/// Passing `None` resets to a no-op provider that returns `None`.
pub fn set_trace_control_flags_provider(provider: Option<TraceControlFlagsProvider>) {
    let provider = provider.unwrap_or_else(|| Arc::new(noop_trace_control_flags_provider));
    let mut guard = TRACE_CONTROL_FLAGS_PROVIDER
        .write()
        .unwrap_or_else(|e| e.into_inner());
    *guard = provider;
}

/// Record a trace event (does not check `is_category_enabled`).
pub fn trace(category: Category, name: &str, fields: &[TraceField]) {
    let func = TRACE_EVENT_FUNC
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    (func)(category, name, fields);
}

/// Check whether a trace category is enabled.
pub fn is_category_enabled(category: Category) -> bool {
    let func = IS_CATEGORY_ENABLED_FUNC
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    (func)(category)
}

/// Convenience helper: checks `is_category_enabled` before recording the event.
///
/// Use this when building `fields` is non-trivial.
pub fn trace_if_enabled<F>(category: Category, name: &str, fields: F)
where
    F: FnOnce() -> Vec<TraceField>,
{
    if !is_category_enabled(category) {
        return;
    }
    let fields = fields();
    trace(category, name, &fields);
}

/// Convenience helper for `TraceControlFlags::IMMEDIATE_LOG`.
///
/// This mirrors client-go `trace.ImmediateLoggingEnabled(ctx)` but takes flags directly.
pub fn immediate_logging_enabled(flags: TraceControlFlags) -> bool {
    flags.has(TraceControlFlags::IMMEDIATE_LOG)
}

const MAX_KV_REQUEST_REGION_RANGES: usize = 4096;

pub(crate) fn record_kv_request_region_range(region: &crate::region::RegionWithLeader) {
    if !is_category_enabled(Category::KvRequest) {
        return;
    }
    let mut guard = KV_REQUEST_REGION_RANGES
        .write()
        .unwrap_or_else(|e| e.into_inner());
    if guard.len() >= MAX_KV_REQUEST_REGION_RANGES {
        guard.clear();
    }
    guard.insert(
        region.ver_id(),
        (
            region.region.start_key.clone(),
            region.region.end_key.clone(),
        ),
    );
}

pub(crate) fn kv_request_region_range(ver_id: &RegionVerId) -> Option<(Vec<u8>, Vec<u8>)> {
    let guard = KV_REQUEST_REGION_RANGES
        .read()
        .unwrap_or_else(|e| e.into_inner());
    guard.get(ver_id).cloned()
}

#[cfg(test)]
pub(crate) fn clear_kv_request_region_ranges_for_test() {
    let mut guard = KV_REQUEST_REGION_RANGES
        .write()
        .unwrap_or_else(|e| e.into_inner());
    guard.clear();
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct HookGuard {
        prev_event: TraceEventFunc,
        prev_enabled: IsCategoryEnabledFunc,
        prev_flags_provider: TraceControlFlagsProvider,
    }

    impl Drop for HookGuard {
        fn drop(&mut self) {
            set_trace_event_func(Some(self.prev_event.clone()));
            set_is_category_enabled_func(Some(self.prev_enabled.clone()));
            set_trace_control_flags_provider(Some(self.prev_flags_provider.clone()));
        }
    }

    fn set_hooks_scoped(
        event: TraceEventFunc,
        enabled: IsCategoryEnabledFunc,
        flags_provider: TraceControlFlagsProvider,
    ) -> HookGuard {
        let prev_event = TRACE_EVENT_FUNC
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let prev_enabled = IS_CATEGORY_ENABLED_FUNC
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let prev_flags_provider = TRACE_CONTROL_FLAGS_PROVIDER
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        set_trace_event_func(Some(event));
        set_is_category_enabled_func(Some(enabled));
        set_trace_control_flags_provider(Some(flags_provider));

        HookGuard {
            prev_event,
            prev_enabled,
            prev_flags_provider,
        }
    }

    #[tokio::test]
    async fn test_task_local_trace_id_scope() {
        assert_eq!(trace_id(), None);

        let outer = b"trace-outer".to_vec();
        with_trace_id(outer.clone(), async {
            assert_eq!(trace_id(), Some(outer.clone()));

            let inner = b"trace-inner".to_vec();
            with_trace_id(inner.clone(), async {
                assert_eq!(trace_id(), Some(inner.clone()));
            })
            .await;

            assert_eq!(trace_id(), Some(outer.clone()));
        })
        .await;

        assert_eq!(trace_id(), None);
    }

    #[test]
    fn test_trace_hooks_and_is_enabled() {
        let _lock = TRACE_HOOK_TEST_LOCK.blocking_lock();
        assert!(!is_category_enabled(Category::KvRequest));

        let seen = Arc::new(Mutex::new(Vec::<(Category, String, usize)>::new()));
        let seen_event = seen.clone();
        let event: TraceEventFunc = Arc::new(move |category, name, fields| {
            if category != Category::KvRequest {
                return;
            }
            if name != "send" && name != "forced" && name != "drop" {
                return;
            }
            seen_event
                .lock()
                .unwrap()
                .push((category, name.to_owned(), fields.len()));
        });
        let enabled: IsCategoryEnabledFunc = Arc::new(|category| category == Category::KvRequest);

        let _guard = set_hooks_scoped(event, enabled, Arc::new(noop_trace_control_flags_provider));

        trace_if_enabled(Category::KvRequest, "send", || {
            vec![TraceField::u64("id", 42)]
        });
        assert_eq!(seen.lock().unwrap().len(), 1);

        // `trace` does not consult the enabled hook.
        set_is_category_enabled_func(None);
        trace(Category::KvRequest, "forced", &[]);
        assert_eq!(seen.lock().unwrap().len(), 2);

        // But `trace_if_enabled` should stop emitting once disabled.
        trace_if_enabled(Category::KvRequest, "drop", Vec::new);
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[test]
    fn test_immediate_logging_enabled_helper() {
        let flags = TraceControlFlags::default().with(TraceControlFlags::IMMEDIATE_LOG);
        assert!(immediate_logging_enabled(flags));
        assert!(!immediate_logging_enabled(TraceControlFlags::default()));
    }

    #[tokio::test]
    async fn test_task_local_trace_control_flags_scopes_restore_outer_values() {
        let _lock = TRACE_HOOK_TEST_LOCK.lock().await;
        let _guard = set_hooks_scoped(
            Arc::new(noop_trace_event),
            Arc::new(noop_is_category_enabled),
            Arc::new(noop_trace_control_flags_provider),
        );
        let outer_flags =
            TraceControlFlags::IMMEDIATE_LOG.with(TraceControlFlags::TIKV_CATEGORY_REQUEST);
        let inner_flags = outer_flags.with(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS);

        assert_eq!(trace_control_flags(), None);

        with_trace_control_flags(outer_flags, async {
            assert_eq!(trace_control_flags(), Some(outer_flags));

            with_trace_control_flags(inner_flags, async {
                assert_eq!(trace_control_flags(), Some(inner_flags));
            })
            .await;

            assert_eq!(trace_control_flags(), Some(outer_flags));
        })
        .await;

        assert_eq!(trace_control_flags(), None);
    }

    #[test]
    fn test_trace_control_flags_uses_provider_when_task_local_missing() {
        let _lock = TRACE_HOOK_TEST_LOCK.blocking_lock();
        let provided_flags =
            TraceControlFlags::IMMEDIATE_LOG.with(TraceControlFlags::TIKV_CATEGORY_REQUEST);
        let _guard = set_hooks_scoped(
            Arc::new(noop_trace_event),
            Arc::new(noop_is_category_enabled),
            Arc::new(move || Some(provided_flags)),
        );

        assert_eq!(trace_control_flags(), Some(provided_flags));
        assert_eq!(
            effective_trace_control_flags(TraceControlFlags::default()),
            provided_flags
        );
    }

    #[tokio::test]
    async fn test_task_local_trace_control_flags_override_provider() {
        let _lock = TRACE_HOOK_TEST_LOCK.lock().await;
        let provided_flags = TraceControlFlags::TIKV_CATEGORY_REQUEST;
        let task_local_flags = provided_flags.with(TraceControlFlags::IMMEDIATE_LOG);
        let _guard = set_hooks_scoped(
            Arc::new(noop_trace_event),
            Arc::new(noop_is_category_enabled),
            Arc::new(move || Some(provided_flags)),
        );

        with_trace_control_flags(task_local_flags, async {
            assert_eq!(trace_control_flags(), Some(task_local_flags));
            assert_eq!(
                effective_trace_control_flags(TraceControlFlags::default()),
                task_local_flags
            );
        })
        .await;

        assert_eq!(trace_control_flags(), Some(provided_flags));
    }
}
