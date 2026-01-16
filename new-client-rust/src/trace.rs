//! Request tracing controls (TiKV-side trace flags).
//!
//! TiKV supports attaching a trace id and a set of control flags to the per-request
//! `kvrpcpb::Context`. This module provides a small, Rust-idiomatic representation of those flags.

use std::sync::OnceLock;
use std::sync::{Arc, RwLock};

/// A trace category (as used by TiKV-side tracing).
pub type Category = u32;

pub const CATEGORY_KV_REQUEST: Category = 1;
pub const CATEGORY_REGION_CACHE: Category = 2;
pub const CATEGORY_TXN_2PC: Category = 3;
pub const CATEGORY_TXN_LOCK_RESOLVE: Category = 4;

/// A per-event key/value attribute.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceField {
    pub key: String,
    pub value: TraceValue,
}

/// A trace attribute value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TraceValue {
    U64(u64),
    I64(i64),
    Bool(bool),
    Str(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceEvent {
    pub category: Category,
    pub name: String,
    pub fields: Vec<TraceField>,
}

pub type IsCategoryEnabledFn = Arc<dyn Fn(Category) -> bool + Send + Sync + 'static>;
pub type TraceEventFn = Arc<dyn Fn(&TraceEvent) + Send + Sync + 'static>;

static IS_CATEGORY_ENABLED_FN: OnceLock<RwLock<Option<IsCategoryEnabledFn>>> = OnceLock::new();
static TRACE_EVENT_FN: OnceLock<RwLock<Option<TraceEventFn>>> = OnceLock::new();

fn is_category_enabled_lock() -> &'static RwLock<Option<IsCategoryEnabledFn>> {
    IS_CATEGORY_ENABLED_FN.get_or_init(|| RwLock::new(None))
}

fn trace_event_lock() -> &'static RwLock<Option<TraceEventFn>> {
    TRACE_EVENT_FN.get_or_init(|| RwLock::new(None))
}

/// Set the global category enable hook used by [`is_category_enabled`].
pub fn set_is_category_enabled_fn(f: Option<IsCategoryEnabledFn>) {
    *is_category_enabled_lock().write().unwrap() = f;
}

/// Set the global trace event sink used by [`trace_event`].
pub fn set_trace_event_fn(f: Option<TraceEventFn>) {
    *trace_event_lock().write().unwrap() = f;
}

/// Returns whether a category is enabled for client-side trace events.
pub fn is_category_enabled(category: Category) -> bool {
    is_category_enabled_lock()
        .read()
        .unwrap()
        .as_ref()
        .map(|f| (f)(category))
        .unwrap_or(false)
}

/// Emit a client-side trace event (if the category is enabled).
pub fn trace_event(
    category: Category,
    name: impl Into<String>,
    fields: impl IntoIterator<Item = (impl Into<String>, TraceValue)>,
) {
    if !is_category_enabled(category) {
        return;
    }

    let event = TraceEvent {
        category,
        name: name.into(),
        fields: fields
            .into_iter()
            .map(|(k, v)| TraceField {
                key: k.into(),
                value: v,
            })
            .collect(),
    };

    if let Some(sink) = trace_event_lock().read().unwrap().as_ref() {
        sink(&event);
    }
}

/// Trace control flags carried by `kvrpcpb::Context.trace_control_flags`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TraceControlFlags(pub u64);

impl TraceControlFlags {
    #[inline]
    pub const fn has(self, flag: TraceControlFlags) -> bool {
        (self.0 & flag.0) != 0
    }

    #[inline]
    pub const fn with(self, flag: TraceControlFlags) -> TraceControlFlags {
        TraceControlFlags(self.0 | flag.0)
    }
}

pub const FLAG_IMMEDIATE_LOG: TraceControlFlags = TraceControlFlags(1 << 0);
pub const FLAG_TIKV_CATEGORY_REQUEST: TraceControlFlags = TraceControlFlags(1 << 1);
pub const FLAG_TIKV_CATEGORY_WRITE_DETAILS: TraceControlFlags = TraceControlFlags(1 << 2);
pub const FLAG_TIKV_CATEGORY_READ_DETAILS: TraceControlFlags = TraceControlFlags(1 << 3);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_event_is_guarded_by_category() {
        set_is_category_enabled_fn(None);
        set_trace_event_fn(None);

        trace_event(
            CATEGORY_TXN_2PC,
            "should_not_emit",
            std::iter::empty::<(String, TraceValue)>(),
        );

        let events = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let events_cloned = events.clone();
        set_is_category_enabled_fn(Some(Arc::new(|_| true)));
        set_trace_event_fn(Some(Arc::new(move |e| {
            events_cloned.lock().unwrap().push(e.name.clone());
        })));

        trace_event(
            CATEGORY_TXN_2PC,
            "prewrite.start",
            [("start_ts", TraceValue::U64(42))],
        );
        assert_eq!(events.lock().unwrap().as_slice(), ["prewrite.start"]);
    }
}
