use tikv_client::oracle;
use tikv_client::oracle::Oracle as _;

#[test]
fn oracle_oracles_module_exports_local_oracle() {
    let _: Option<oracle::oracles::LocalOracle> = None;
    let _ = oracle::oracles::new_local_oracle();

    let _: Option<oracle::oracles::MockOracle> = None;
    let _ = oracle::oracles::new_mock_oracle();
    let _: fn(&oracle::oracles::MockOracle, i64) = oracle::oracles::MockOracle::add_offset_ms;

    let _: Option<oracle::oracles::PdOracle> = None;
    let _pd_oracle_future = oracle::oracles::new_pd_oracle(vec!["127.0.0.1:2379"]);
    let _pd_oracle_future = oracle::oracles::new_pd_oracle_with_config(
        vec!["127.0.0.1:2379"],
        tikv_client::Config::default(),
    );
}

#[tokio::test]
async fn local_oracle_get_timestamp_is_strictly_monotonic() {
    let oracle = oracle::oracles::new_local_oracle();
    let opt = oracle::OracleOption::new();

    let first = oracle.get_timestamp(&opt).await.expect("first timestamp");
    let second = oracle.get_timestamp(&opt).await.expect("second timestamp");
    assert!(second > first);
}

#[tokio::test]
async fn mock_oracle_disable_and_enable_controls_timestamp_generation() {
    let oracle = oracle::oracles::new_mock_oracle();
    let opt = oracle::OracleOption::new();

    oracle.disable();
    assert!(oracle.get_timestamp(&opt).await.is_err());

    oracle.enable();
    let first = oracle.get_timestamp(&opt).await.expect("first timestamp");
    let second = oracle.get_timestamp(&opt).await.expect("second timestamp");
    assert!(second > first);
}

#[tokio::test]
async fn mock_oracle_external_timestamp_is_monotonic_and_not_in_future() {
    let oracle = oracle::oracles::new_mock_oracle();
    let opt = oracle::OracleOption::new();

    let current = oracle.get_timestamp(&opt).await.expect("current timestamp");
    let future = current.saturating_add(60_000u64 << oracle::PHYSICAL_SHIFT_BITS);
    assert!(oracle.set_external_timestamp(future).await.is_err());

    oracle
        .set_external_timestamp(current)
        .await
        .expect("set external timestamp");
    assert_eq!(
        oracle
            .get_external_timestamp()
            .await
            .expect("get external timestamp"),
        current
    );

    assert!(oracle
        .set_external_timestamp(current.saturating_sub(1))
        .await
        .is_err());
}
