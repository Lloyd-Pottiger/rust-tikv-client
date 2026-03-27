use std::time::{Duration, UNIX_EPOCH};

use tikv_client::util;
use tikv_client::util::iter::FlatMapOkIterExt;

#[test]
fn util_submodules_public_api_exposes_bytes_helpers_via_module_path() {
    let _: fn(i64) -> String = util::bytes::format_bytes;
    let _: fn(i64) -> String = util::bytes::bytes_to_string;

    assert_eq!(util::bytes::format_bytes(1536), "1.50 KB");
    assert_eq!(util::bytes::bytes_to_string(2 * 1024 * 1024), "2 MB");
}

#[test]
fn util_submodules_public_api_exposes_gc_time_helpers_via_module_path() {
    assert_eq!(util::gc_time::GC_TIME_FORMAT, "20060102-15:04:05.000 -0700");
    let _: fn(&str) -> Result<std::time::SystemTime, util::gc_time::GcTimeParseError> =
        util::gc_time::compatible_parse_gc_time;

    let parsed = util::gc_time::compatible_parse_gc_time("20181218-19:53:37 +0800 CST")
        .expect("gc time should parse");
    assert_eq!(parsed, UNIX_EPOCH + Duration::from_secs(1_545_134_017));

    let err = util::gc_time::compatible_parse_gc_time("not-a-gc-time")
        .expect_err("invalid gc time should fail");
    assert_eq!(err.value(), "not-a-gc-time");
}

#[test]
fn util_submodules_public_api_exposes_flat_map_ok_extension_trait_and_iterator_type() {
    let iter: util::iter::FlatMapOk<_, _, std::vec::IntoIter<u8>, &str> =
        vec![Ok::<u8, &str>(1), Err("boom"), Ok(2)]
            .into_iter()
            .flat_map_ok(|value| vec![value, value + 10]);

    assert_eq!(
        iter.collect::<Vec<_>>(),
        vec![Ok(1), Ok(11), Err("boom"), Ok(2), Ok(12)]
    );
}
