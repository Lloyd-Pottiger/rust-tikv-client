use tikv_client::util;

#[test]
fn util_rate_limit_public_api_exposes_constructor_helper() {
    let _: fn(usize) -> util::RateLimit = util::new_rate_limit;

    let limit = util::new_rate_limit(3);
    assert_eq!(limit.capacity(), 3);
    let permit: util::RateLimitPermit = limit.try_acquire().unwrap().expect("permit");
    drop(permit);

    let err = util::RateLimitError::Closed;
    assert_eq!(err.to_string(), "rate limit is closed");
}

#[tokio::test]
async fn util_rate_limit_public_api_exposes_async_permit_type() {
    let limit = util::new_rate_limit(1);
    let permit: util::RateLimitPermit = limit.acquire().await.unwrap();
    drop(permit);
}
