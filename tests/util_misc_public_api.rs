use tikv_client::util;

#[test]
fn util_misc_public_api_exposes_generic_option_helpers() {
    let some = util::some(7_u64);
    let _: util::Option<u64> = some.clone();
    assert_eq!(some.inner(), Some(&7));

    let none: util::Option<u64> = util::none();
    assert_eq!(none.inner(), None);
}

#[test]
fn util_misc_public_api_exposes_byte_format_helpers() {
    let _: fn(i64) -> String = util::format_bytes;
    let _: fn(i64) -> String = util::bytes_to_string;

    assert_eq!(util::format_bytes(2048), "2 KB");
    assert_eq!(util::bytes_to_string(100), "100 Bytes");
}

#[tokio::test]
async fn util_misc_public_api_exposes_session_and_recovery_helpers() {
    let _: util::SessionID = util::SessionID;
    assert_eq!(util::session_id(), None);

    util::with_session_id(42, async {
        assert_eq!(util::session_id(), Some(42));
    })
    .await;

    assert_eq!(util::session_id(), None);

    let result = util::with_recovery(
        || 7_u64,
        Some(|panic: Option<&(dyn std::any::Any + Send + 'static)>| {
            assert!(panic.is_none());
        }),
    );
    assert_eq!(result, Some(7));
}
