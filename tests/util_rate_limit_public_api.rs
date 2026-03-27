use tikv_client::util;

#[test]
fn util_rate_limit_public_api_exposes_constructor_helper() {
    let _: fn(usize) -> util::RateLimit = util::new_rate_limit;

    let limit = util::new_rate_limit(3);
    assert_eq!(limit.capacity(), 3);
    assert!(limit.try_acquire().unwrap().is_some());
}
