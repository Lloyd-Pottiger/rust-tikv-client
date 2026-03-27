use tikv_client::util;

#[test]
fn util_exec_details_public_api_exposes_context_key_markers() {
    let _: util::CommitDetailCtxKey = util::CommitDetailCtxKey;
    let _: util::LockKeysDetailCtxKey = util::LockKeysDetailCtxKey;
    let _: util::ExecDetailsKey = util::ExecDetailsKey;
    let _: util::RUDetailsCtxKey = util::RUDetailsCtxKey;

    let ru_details = std::sync::Arc::new(util::RUDetails::new());
    std::mem::drop(util::with_ru_details(ru_details, async {}));
    let _ = util::ru_details();

    std::mem::drop(util::with_trace_exec_details(async {}));
    let _ = util::trace_exec_details_enabled();
}

#[test]
fn util_exec_details_public_api_exposes_constructor_helpers() {
    let _: fn(Option<&tikv_client::proto::kvrpcpb::ExecDetailsV2>) -> util::TiKVExecDetails =
        util::new_tikv_exec_details;
    let _: fn() -> util::RUDetails = util::new_ru_details;
    let _: fn(f64, f64, std::time::Duration) -> util::RUDetails = util::new_ru_details_with;

    let exec_details = util::new_tikv_exec_details(None);
    assert_eq!(exec_details, util::TiKVExecDetails::default());

    let ru_details = util::new_ru_details_with(1.5, 2.5, std::time::Duration::from_millis(3));
    assert_eq!(ru_details.rru(), 1.5);
    assert_eq!(ru_details.wru(), 2.5);
    assert_eq!(
        ru_details.ru_wait_duration(),
        std::time::Duration::from_millis(3)
    );

    let _: fn(
        &util::RUDetails,
        Option<&tikv_client::ProtoResourceConsumption>,
        std::time::Duration,
    ) = util::RUDetails::record;

    let consumption = tikv_client::ProtoResourceConsumption {
        r_r_u: 0.5,
        w_r_u: 1.25,
        ..Default::default()
    };
    ru_details.record(Some(&consumption), std::time::Duration::from_millis(4));
    ru_details.record(None, std::time::Duration::from_millis(9));
    assert_eq!(ru_details.rru(), 2.0);
    assert_eq!(ru_details.wru(), 3.75);
    assert_eq!(
        ru_details.ru_wait_duration(),
        std::time::Duration::from_millis(7)
    );
}

#[test]
fn util_exec_details_public_api_exposes_types_and_format_helper() {
    let _: util::CommitDetails = Default::default();
    let _: util::CommitTSLagDetails = Default::default();
    let _: util::LockKeysDetails = Default::default();
    let _: util::ReqDetailInfo = Default::default();
    let _: util::TimeDetail = Default::default();
    let _: util::ScanDetail = Default::default();
    let _: util::WriteDetail = Default::default();

    let traffic = util::TrafficDetails::default();
    traffic.add_kv_bytes(11, 22, 3, 4);
    assert_eq!(traffic.unpacked_bytes_sent_kv_total(), 11);
    assert_eq!(traffic.unpacked_bytes_received_kv_total(), 22);
    assert_eq!(traffic.unpacked_bytes_sent_kv_cross_zone(), 3);
    assert_eq!(traffic.unpacked_bytes_received_kv_cross_zone(), 4);

    assert_eq!(
        util::format_duration(std::time::Duration::from_nanos(999)),
        "999ns"
    );
    assert_eq!(
        util::format_duration(std::time::Duration::from_nanos(1000)),
        "1us"
    );

    let mut lag_details = util::CommitTSLagDetails::default();
    lag_details.wait_time = std::time::Duration::from_millis(2);
    lag_details.backoff_count = 1;
    lag_details.first_lag_ts = 10;
    lag_details.wait_until_ts = 20;

    let mut other_lag_details = util::CommitTSLagDetails::default();
    other_lag_details.wait_time = std::time::Duration::from_millis(3);
    other_lag_details.backoff_count = 2;
    other_lag_details.first_lag_ts = 30;
    other_lag_details.wait_until_ts = 40;

    lag_details.merge(&other_lag_details);
    assert_eq!(lag_details.wait_time, std::time::Duration::from_millis(5));
    assert_eq!(lag_details.backoff_count, 3);
    assert_eq!(lag_details.first_lag_ts, 30);
    assert_eq!(lag_details.wait_until_ts, 40);

    let mut req_detail = util::ReqDetailInfo::default();
    req_detail.req_total_time = std::time::Duration::from_millis(6);
    req_detail.region = 7;
    req_detail.store_addr = "store-1".to_owned();
    req_detail.exec_details = util::TiKVExecDetails::default();
    assert_eq!(req_detail.region, 7);
    assert_eq!(req_detail.store_addr, "store-1");
    assert_eq!(
        req_detail.req_total_time,
        std::time::Duration::from_millis(6)
    );
}

#[tokio::test]
async fn util_exec_details_public_api_exposes_exec_details_scope_helper() {
    let exec_details = std::sync::Arc::new(util::ExecDetails::new());

    util::with_exec_details(exec_details.clone(), async {
        let current = util::exec_details().expect("exec details should be scoped");
        current.add_wait_pd_response(std::time::Duration::from_millis(5));
        assert_eq!(
            current.wait_pd_resp_duration(),
            std::time::Duration::from_millis(5)
        );
    })
    .await;

    assert_eq!(
        exec_details.wait_pd_resp_duration(),
        std::time::Duration::from_millis(5)
    );
}
