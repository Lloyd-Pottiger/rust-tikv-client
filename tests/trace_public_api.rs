use tikv_client::trace;
use tikv_client::TraceControlFlags;

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
