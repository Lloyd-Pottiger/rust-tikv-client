use std::time::SystemTime;

use tikv_client::oracle;

#[test]
fn oracle_module_exposes_constants_and_helpers() {
    let _ = oracle::PHYSICAL_SHIFT_BITS;
    let _ = oracle::GLOBAL_TXN_SCOPE;

    let ts = oracle::compose_ts(7, 9);
    assert_eq!(oracle::extract_physical(ts), 7);
    assert_eq!(oracle::extract_logical(ts), 9);

    let now = SystemTime::now();
    let ts = oracle::ts_from_system_time(now).expect("system time converts to ts");
    let _ = oracle::system_time_from_ts(ts).expect("ts converts to system time");

    let _ = oracle::lower_limit_start_ts(now, 1).expect("lower limit start ts");
    let _ = oracle::is_expired(ts, 1, ts);
    let _ = oracle::until_expired_ms(ts, 1, ts);
}

#[test]
fn oracle_module_exposes_oracle_traits_and_errors() {
    let opt = oracle::OracleOption::new();
    let _ = opt.txn_scope;

    fn assert_oracle<T: oracle::Oracle + Send + Sync>() {}
    assert_oracle::<tikv_client::TransactionClient>();

    let _ = oracle::ErrFutureTsRead {
        read_ts: 1,
        current_ts: 2,
    };
    let _ = oracle::ErrLatestStaleRead;
}
