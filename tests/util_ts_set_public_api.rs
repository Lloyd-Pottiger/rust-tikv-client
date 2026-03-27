use std::time::{Duration, UNIX_EPOCH};

use tikv_client::util;

#[test]
fn util_ts_set_public_api_exposes_gc_time_helpers_and_ts_set_alias() {
    assert_eq!(util::GC_TIME_FORMAT, "20060102-15:04:05.000 -0700");

    let parsed = util::compatible_parse_gc_time("20181218-19:53:37 +0800 CST")
        .expect("gc time should parse");
    assert_eq!(parsed, UNIX_EPOCH + Duration::from_secs(1_545_134_017),);

    let err: util::GcTimeParseError =
        util::compatible_parse_gc_time("not-a-gc-time").expect_err("invalid gc time");
    assert_eq!(err.value(), "not-a-gc-time");

    let _: Option<util::TSSet> = None;
    let _: Option<util::TsSet> = None;

    let set = util::TSSet::new();
    set.put([1_u64, 2, 2]);

    let mut all = set.get_all();
    all.sort_unstable();
    assert_eq!(all, vec![1, 2]);
}
