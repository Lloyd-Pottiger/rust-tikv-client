use std::time::Duration;

use tikv_client::backoff::{
    Backoff, DEFAULT_REGION_BACKOFF, DEFAULT_STORE_BACKOFF, OPTIMISTIC_BACKOFF, PESSIMISTIC_BACKOFF,
};

#[test]
fn backoff_public_api_exposes_constructors_and_constants() {
    let _: fn() -> Backoff = Backoff::no_backoff;
    let _: fn(u64, u64, u32) -> Backoff = Backoff::no_jitter_backoff;
    let _: fn(u64, u64, u32) -> Backoff = Backoff::full_jitter_backoff;
    let _: fn(u64, u64, u32) -> Backoff = Backoff::equal_jitter_backoff;
    let _: fn(u64, u64, u32) -> Backoff = Backoff::decorrelated_jitter_backoff;

    assert_eq!(
        DEFAULT_REGION_BACKOFF,
        Backoff::no_jitter_backoff(2, 500, 10)
    );
    assert_eq!(
        DEFAULT_STORE_BACKOFF,
        Backoff::no_jitter_backoff(2, 1000, 10)
    );
    assert_eq!(
        OPTIMISTIC_BACKOFF,
        Backoff::equal_jitter_backoff(2, 3000, 16)
    );
    assert_eq!(
        PESSIMISTIC_BACKOFF,
        Backoff::equal_jitter_backoff(2, 3000, 16)
    );
}

#[test]
fn backoff_public_api_exposes_deterministic_no_jitter_schedule() {
    let mut backoff = Backoff::no_jitter_backoff(2, 5, 3);
    assert!(!backoff.is_none());
    assert_eq!(backoff.current_attempts(), 0);

    assert_eq!(
        backoff.next_delay_duration(),
        Some(Duration::from_millis(2))
    );
    assert_eq!(backoff.current_attempts(), 1);
    assert_eq!(
        backoff.next_delay_duration(),
        Some(Duration::from_millis(4))
    );
    assert_eq!(backoff.current_attempts(), 2);
    assert_eq!(
        backoff.next_delay_duration(),
        Some(Duration::from_millis(5))
    );
    assert_eq!(backoff.current_attempts(), 3);
    assert_eq!(backoff.next_delay_duration(), None);
}

#[test]
fn backoff_public_api_exposes_none_and_jitter_variants() {
    let mut none = Backoff::no_backoff();
    assert!(none.is_none());
    assert_eq!(none.current_attempts(), 0);
    assert_eq!(none.next_delay_duration(), None);

    let mut full = Backoff::full_jitter_backoff(1, 3, 1);
    let full_delay = full
        .next_delay_duration()
        .expect("full jitter should retry");
    assert!(full_delay <= Duration::from_millis(1));
    assert_eq!(full.current_attempts(), 1);

    let mut equal = Backoff::equal_jitter_backoff(2, 8, 1);
    let equal_delay = equal
        .next_delay_duration()
        .expect("equal jitter should retry");
    assert!((1..=2).contains(&equal_delay.as_millis()));
    assert_eq!(equal.current_attempts(), 1);

    let mut decorrelated = Backoff::decorrelated_jitter_backoff(1, 4, 1);
    let decorrelated_delay = decorrelated
        .next_delay_duration()
        .expect("decorrelated jitter should retry");
    assert!((1..=2).contains(&decorrelated_delay.as_millis()));
    assert_eq!(decorrelated.current_attempts(), 1);
}
