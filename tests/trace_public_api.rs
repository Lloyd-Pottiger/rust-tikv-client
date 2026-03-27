use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tikv_client::trace;
use tikv_client::TraceControlFlags;

static TRACE_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn trace_module_exports_types_and_hook_registration() {
    let _ = trace::Category::Txn2Pc;
    let _ = trace::Category::TxnLockResolve;
    let _ = trace::Category::KvRequest;
    let _ = trace::Category::RegionCache;

    let _ = trace::TraceField::str("k", "v");
    let _ = trace::TraceField::u64("u64", 7);
    let _ = trace::TraceField::i64("i64", -7);
    let _ = trace::TraceField::bool("b", true);
    let _ = trace::TraceField::bytes("bytes", vec![1, 2, 3]);

    let _ = trace::TraceValue::Str("v".into());
    let _ = trace::TraceValue::U64(7);
    let _ = trace::TraceValue::I64(-7);
    let _ = trace::TraceValue::Bool(true);
    let _ = trace::TraceValue::Bytes(vec![1, 2, 3]);

    let _: fn(Option<trace::TraceEventFunc>) = trace::set_trace_event_func;
    let _: fn(Option<trace::IsCategoryEnabledFunc>) = trace::set_is_category_enabled_func;
    let _: fn(Option<trace::TraceControlFlagsProvider>) = trace::set_trace_control_flags_provider;
}

#[tokio::test]
async fn trace_task_locals_roundtrip() {
    assert_eq!(trace::trace_id(), None);
    assert_eq!(trace::trace_control_flags(), None);

    let trace_id = vec![1, 2, 3];
    trace::with_trace_id(trace_id.clone(), async {
        assert_eq!(trace::trace_id(), Some(trace_id.clone()));

        trace::with_trace_control_flags(TraceControlFlags::IMMEDIATE_LOG, async {
            assert_eq!(
                trace::trace_control_flags(),
                Some(TraceControlFlags::IMMEDIATE_LOG)
            );
            assert!(trace::immediate_logging_enabled(
                TraceControlFlags::IMMEDIATE_LOG
            ));
        })
        .await;
    })
    .await;

    assert_eq!(trace::trace_id(), None);
    assert_eq!(trace::trace_control_flags(), None);
}

#[test]
fn trace_module_public_api_exposes_trace_if_enabled() {
    let _guard = TRACE_TEST_LOCK.lock().expect("trace test lock");

    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_captured = seen.clone();
    trace::set_trace_event_func(Some(Arc::new(move |_category, name, _fields| {
        seen_captured.lock().expect("seen").push(name.to_owned());
    })));
    trace::set_is_category_enabled_func(Some(Arc::new(|category| {
        matches!(category, trace::Category::KvRequest)
    })));

    trace::trace_if_enabled(trace::Category::KvRequest, "send", || {
        vec![trace::TraceField::bool("enabled", true)]
    });
    assert_eq!(seen.lock().expect("seen").as_slice(), ["send"]);
    assert!(trace::is_category_enabled(trace::Category::KvRequest));

    let called = AtomicBool::new(false);
    trace::set_is_category_enabled_func(Some(Arc::new(|_| false)));
    trace::trace_if_enabled(trace::Category::KvRequest, "drop", || {
        called.store(true, Ordering::SeqCst);
        vec![trace::TraceField::bool("enabled", false)]
    });
    assert!(!called.load(Ordering::SeqCst));

    trace::set_trace_event_func(None);
    trace::set_is_category_enabled_func(None);
}
