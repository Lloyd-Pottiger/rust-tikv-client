//! Request tracing controls (TiKV-side trace flags).
//!
//! TiKV supports attaching a trace id and a set of control flags to the per-request
//! `kvrpcpb::Context`. This module provides a small, Rust-idiomatic representation of those flags.
//!
//! ## `tracing` integration
//!
//! When built with the `tracing` Cargo feature, [`enable_tracing_events`] installs a global sink
//! that forwards client-side trace events to the `tracing` ecosystem.

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

fn lock_read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poison) => poison.into_inner(),
    }
}

fn lock_write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poison) => poison.into_inner(),
    }
}

/// Set the global category enable hook used by [`is_category_enabled`].
pub fn set_is_category_enabled_fn(f: Option<IsCategoryEnabledFn>) {
    *lock_write(is_category_enabled_lock()) = f;
}

/// Set the global trace event sink used by [`trace_event`].
pub fn set_trace_event_fn(f: Option<TraceEventFn>) {
    *lock_write(trace_event_lock()) = f;
}

/// Returns whether a category is enabled for client-side trace events.
pub fn is_category_enabled(category: Category) -> bool {
    lock_read(is_category_enabled_lock())
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

    if let Some(sink) = lock_read(trace_event_lock()).as_ref() {
        sink(&event);
    }
}

/// Enable emitting client-side trace events via the [`tracing`] ecosystem.
///
/// This installs a global [`TraceEventFn`] that forwards events to `tracing::event!`.
/// If a sink is already registered via [`set_trace_event_fn`], it is preserved and invoked before
/// the `tracing` sink.
///
/// Note: this does not configure a `tracing` subscriber. Applications should install a subscriber
/// (e.g. `tracing_subscriber`) to collect/export events, optionally via OpenTelemetry.
#[cfg(feature = "tracing")]
pub fn enable_tracing_events() {
    // If the user didn't configure any category filter, enable everything so trace events
    // can be surfaced without additional plumbing.
    {
        let mut guard = lock_write(is_category_enabled_lock());
        if guard.is_none() {
            *guard = Some(Arc::new(|_| true));
        }
    }

    let tracing_sink: TraceEventFn = Arc::new(|event: &TraceEvent| {
        tracing::event!(
            target: "tikv_client::trace",
            tracing::Level::INFO,
            category = event.category,
            name = %event.name,
            fields = ?event.fields,
        );
    });

    let mut guard = lock_write(trace_event_lock());
    match guard.take() {
        None => {
            *guard = Some(tracing_sink);
        }
        Some(prev) => {
            let combined: TraceEventFn = Arc::new(move |event: &TraceEvent| {
                (prev)(event);
                (tracing_sink)(event);
            });
            *guard = Some(combined);
        }
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

    #[cfg(feature = "tracing")]
    #[test]
    fn enable_tracing_events_preserves_existing_sink() {
        let events = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let events_cloned = events.clone();
        set_is_category_enabled_fn(Some(Arc::new(|_| true)));
        set_trace_event_fn(Some(Arc::new(move |e| {
            events_cloned.lock().unwrap().push(e.name.clone());
        })));

        enable_tracing_events();
        trace_event(
            CATEGORY_TXN_2PC,
            "commit.start",
            std::iter::empty::<(String, TraceValue)>(),
        );
        assert_eq!(events.lock().unwrap().as_slice(), ["commit.start"]);
    }
}
