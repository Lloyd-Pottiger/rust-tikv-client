use tikv_client::util;

#[test]
fn util_misc_public_api_exposes_generic_option_helpers() {
    let some = util::some(7_u64);
    let _: util::Option<u64> = some.clone();
    assert_eq!(some.inner(), Some(&7));

    let none: util::Option<u64> = util::none();
    assert_eq!(none.inner(), None);
}
