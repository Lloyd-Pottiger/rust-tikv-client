// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

use std::time::Duration;

use rand::thread_rng;
use rand::Rng;

/// Default no-jitter retry policy for region-cache refreshes and similar metadata work.
pub const DEFAULT_REGION_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 500, 10);
/// Default no-jitter retry policy for store-level retries that can wait slightly longer.
pub const DEFAULT_STORE_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 1000, 10);
// Match client-go `BoTxnLockFast` defaults (base 2ms, cap 3000ms, equal jitter).
// `max_attempts` is chosen to cover typical lock TTL durations without timing out too early.
/// Equal-jitter retry policy used for optimistic transaction lock contention paths.
pub const OPTIMISTIC_BACKOFF: Backoff = Backoff::equal_jitter_backoff(2, 3000, 16);
/// Equal-jitter retry policy used for pessimistic transaction lock contention paths.
pub const PESSIMISTIC_BACKOFF: Backoff = Backoff::equal_jitter_backoff(2, 3000, 16);

/// When a request is retried, we can backoff for some time to avoid saturating the network.
///
/// `Backoff` is an object which determines how long to wait for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backoff {
    kind: BackoffKind,
    current_attempts: u32,
    max_attempts: u32,
    base_delay_ms: u64,
    current_delay_ms: u64,
    max_delay_ms: u64,
}

impl Backoff {
    /// Returns the delay for the next retry attempt.
    ///
    /// Returns `None` once the configured retry budget is exhausted. For
    /// [`Backoff::no_backoff`], this also returns `None` immediately.
    pub fn next_delay_duration(&mut self) -> Option<Duration> {
        if self.current_attempts >= self.max_attempts {
            return None;
        }
        self.current_attempts += 1;

        match self.kind {
            BackoffKind::None => None,
            BackoffKind::NoJitter => {
                let delay_ms = self.max_delay_ms.min(self.current_delay_ms);
                self.current_delay_ms <<= 1;

                Some(Duration::from_millis(delay_ms))
            }
            BackoffKind::FullJitter => {
                let delay_ms = self.max_delay_ms.min(self.current_delay_ms);

                let mut rng = thread_rng();
                let delay_ms: u64 = rng.gen_range(0..delay_ms);
                self.current_delay_ms <<= 1;

                Some(Duration::from_millis(delay_ms))
            }
            BackoffKind::EqualJitter => {
                let delay_ms = self.max_delay_ms.min(self.current_delay_ms);
                let half_delay_ms = delay_ms >> 1;

                let mut rng = thread_rng();
                let delay_ms: u64 = rng.gen_range(0..half_delay_ms) + half_delay_ms;
                self.current_delay_ms <<= 1;

                Some(Duration::from_millis(delay_ms))
            }
            BackoffKind::DecorrelatedJitter => {
                let mut rng = thread_rng();
                let delay_ms: u64 = rng
                    .gen_range(0..self.current_delay_ms * 3 - self.base_delay_ms)
                    + self.base_delay_ms;

                let delay_ms = delay_ms.min(self.max_delay_ms);
                self.current_delay_ms = delay_ms;

                Some(Duration::from_millis(delay_ms))
            }
        }
    }

    /// Returns whether this policy disables sleeping between retries.
    pub fn is_none(&self) -> bool {
        self.kind == BackoffKind::None
    }

    /// Returns how many retry attempts have already been handed out by this backoff.
    pub fn current_attempts(&self) -> u32 {
        self.current_attempts
    }

    pub(crate) fn with_base_delay_ms(mut self, base_delay_ms: u64) -> Backoff {
        let min_base_delay_ms = match self.kind {
            BackoffKind::EqualJitter => 2,
            BackoffKind::FullJitter | BackoffKind::DecorrelatedJitter => 1,
            BackoffKind::NoJitter | BackoffKind::None => 0,
        };

        let base_delay_ms = base_delay_ms.max(min_base_delay_ms);
        self.base_delay_ms = base_delay_ms;
        self.current_delay_ms = base_delay_ms;

        let min_max_delay_ms = match self.kind {
            BackoffKind::EqualJitter => 2,
            BackoffKind::FullJitter | BackoffKind::DecorrelatedJitter => 1,
            BackoffKind::NoJitter | BackoffKind::None => 0,
        };
        if self.max_delay_ms < min_max_delay_ms {
            self.max_delay_ms = min_max_delay_ms;
        }

        self
    }

    pub(crate) fn scaled_max_attempts(mut self, multiplier: u32) -> Backoff {
        let multiplier = multiplier.max(1);
        if multiplier == 1 {
            return self;
        }
        self.max_attempts = self.max_attempts.saturating_mul(multiplier);
        self
    }

    /// Returns a policy that never sleeps and yields no retry delays.
    pub const fn no_backoff() -> Backoff {
        Backoff {
            kind: BackoffKind::None,
            current_attempts: 0,
            max_attempts: 0,
            base_delay_ms: 0,
            current_delay_ms: 0,
            max_delay_ms: 0,
        }
    }

    /// Returns a deterministic exponential backoff without jitter.
    ///
    /// Each delay doubles from `base_delay_ms` until it reaches `max_delay_ms`, and the policy
    /// stops after `max_attempts` calls to [`Backoff::next_delay_duration`].
    pub const fn no_jitter_backoff(
        base_delay_ms: u64,
        max_delay_ms: u64,
        max_attempts: u32,
    ) -> Backoff {
        Backoff {
            kind: BackoffKind::NoJitter,
            current_attempts: 0,
            max_attempts,
            base_delay_ms,
            current_delay_ms: base_delay_ms,
            max_delay_ms,
        }
    }

    // Adds Jitter to the basic exponential backoff. Returns a random value between
    // zero and the calculated exponential backoff:
    //
    // temp = min(max_delay, base_delay * 2 ** attempts)
    // new_delay = random_between(0, temp)
    /// Returns an exponential backoff with full jitter.
    ///
    /// Each retry picks a random delay in `0..=min(max_delay_ms, current_delay)` and then doubles
    /// the next deterministic cap until `max_delay_ms` is reached.
    pub fn full_jitter_backoff(
        base_delay_ms: u64,
        max_delay_ms: u64,
        max_attempts: u32,
    ) -> Backoff {
        assert!(
            base_delay_ms > 0 && max_delay_ms > 0,
            "Both base_delay_ms and max_delay_ms must be positive"
        );

        Backoff {
            kind: BackoffKind::FullJitter,
            current_attempts: 0,
            max_attempts,
            base_delay_ms,
            current_delay_ms: base_delay_ms,
            max_delay_ms,
        }
    }

    /// Returns an exponential backoff with equal jitter.
    ///
    /// Each retry picks a random delay in the upper half of the current exponential window, which
    /// preserves some minimum slowdown while still spreading concurrent retries.
    pub const fn equal_jitter_backoff(
        base_delay_ms: u64,
        max_delay_ms: u64,
        max_attempts: u32,
    ) -> Backoff {
        assert!(
            base_delay_ms > 1 && max_delay_ms > 1,
            "Both base_delay_ms and max_delay_ms must be greater than 1"
        );

        Backoff {
            kind: BackoffKind::EqualJitter,
            current_attempts: 0,
            max_attempts,
            base_delay_ms,
            current_delay_ms: base_delay_ms,
            max_delay_ms,
        }
    }

    /// Returns a decorrelated-jitter backoff.
    ///
    /// Each retry samples from `base_delay_ms..=(previous_delay * 3)` and clamps the result to
    /// `max_delay_ms`, which helps avoid synchronized retry waves while still adapting upward.
    pub fn decorrelated_jitter_backoff(
        base_delay_ms: u64,
        max_delay_ms: u64,
        max_attempts: u32,
    ) -> Backoff {
        assert!(base_delay_ms > 0, "base_delay_ms must be positive");

        Backoff {
            kind: BackoffKind::DecorrelatedJitter,
            current_attempts: 0,
            max_attempts,
            base_delay_ms,
            current_delay_ms: base_delay_ms,
            max_delay_ms,
        }
    }
}

/// The pattern for computing backoff times.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BackoffKind {
    None,
    NoJitter,
    FullJitter,
    EqualJitter,
    DecorrelatedJitter,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_base_delay_ms_clamps_equal_jitter_to_two_ms() {
        let backoff = Backoff::equal_jitter_backoff(2, 10, 3).with_base_delay_ms(1);
        assert_eq!(backoff.base_delay_ms, 2);
        assert_eq!(backoff.current_delay_ms, 2);
    }

    #[test]
    fn with_base_delay_ms_resets_current_delay() {
        let mut backoff = Backoff::no_jitter_backoff(5, 10, 10);
        let _ = backoff.next_delay_duration();
        assert_eq!(backoff.current_delay_ms, 10);

        let backoff = backoff.with_base_delay_ms(7);
        assert_eq!(backoff.base_delay_ms, 7);
        assert_eq!(backoff.current_delay_ms, 7);
    }

    #[test]
    fn scaled_max_attempts_multiplies_and_saturates() {
        let backoff = Backoff::no_jitter_backoff(0, 0, 3).scaled_max_attempts(2);
        assert_eq!(backoff.max_attempts, 6);

        let backoff = Backoff::no_jitter_backoff(0, 0, u32::MAX).scaled_max_attempts(2);
        assert_eq!(backoff.max_attempts, u32::MAX);

        let backoff = Backoff::no_jitter_backoff(0, 0, 5).scaled_max_attempts(0);
        assert_eq!(backoff.max_attempts, 5);
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryInto;

    use super::*;

    #[test]
    fn test_no_jitter_backoff() {
        // Tests for zero attempts.
        let mut backoff = Backoff::no_jitter_backoff(0, 0, 0);
        assert_eq!(backoff.next_delay_duration(), None);

        let mut backoff = Backoff::no_jitter_backoff(2, 7, 3);
        assert_eq!(
            backoff.next_delay_duration(),
            Some(Duration::from_millis(2))
        );
        assert_eq!(
            backoff.next_delay_duration(),
            Some(Duration::from_millis(4))
        );
        assert_eq!(
            backoff.next_delay_duration(),
            Some(Duration::from_millis(7))
        );
        assert_eq!(backoff.next_delay_duration(), None);
    }

    #[test]
    fn test_full_jitter_backoff() {
        let mut backoff = Backoff::full_jitter_backoff(2, 7, 3);
        assert!(backoff.next_delay_duration().unwrap() <= Duration::from_millis(2));
        assert!(backoff.next_delay_duration().unwrap() <= Duration::from_millis(4));
        assert!(backoff.next_delay_duration().unwrap() <= Duration::from_millis(7));
        assert_eq!(backoff.next_delay_duration(), None);
    }

    #[test]
    #[should_panic(expected = "Both base_delay_ms and max_delay_ms must be positive")]
    fn test_full_jitter_backoff_with_invalid_base_delay_ms() {
        Backoff::full_jitter_backoff(0, 7, 3);
    }

    #[test]
    #[should_panic(expected = "Both base_delay_ms and max_delay_ms must be positive")]
    fn test_full_jitter_backoff_with_invalid_max_delay_ms() {
        Backoff::full_jitter_backoff(2, 0, 3);
    }

    #[test]
    fn test_equal_jitter_backoff() {
        let mut backoff = Backoff::equal_jitter_backoff(2, 7, 3);

        let first_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(first_delay_dur >= Duration::from_millis(1));
        assert!(first_delay_dur <= Duration::from_millis(2));

        let second_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(second_delay_dur >= Duration::from_millis(2));
        assert!(second_delay_dur <= Duration::from_millis(4));

        let third_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(third_delay_dur >= Duration::from_millis(3));
        assert!(third_delay_dur <= Duration::from_millis(6));

        assert_eq!(backoff.next_delay_duration(), None);
    }

    #[test]
    #[should_panic(expected = "Both base_delay_ms and max_delay_ms must be greater than 1")]
    fn test_equal_jitter_backoff_with_invalid_base_delay_ms() {
        Backoff::equal_jitter_backoff(1, 7, 3);
    }

    #[test]
    #[should_panic(expected = "Both base_delay_ms and max_delay_ms must be greater than 1")]
    fn test_equal_jitter_backoff_with_invalid_max_delay_ms() {
        Backoff::equal_jitter_backoff(2, 1, 3);
    }

    #[test]
    fn test_decorrelated_jitter_backoff() {
        let mut backoff = Backoff::decorrelated_jitter_backoff(2, 7, 3);

        let first_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(first_delay_dur >= Duration::from_millis(2));
        assert!(first_delay_dur <= Duration::from_millis(6));

        let second_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(second_delay_dur >= Duration::from_millis(2));
        let cap_ms = 7u64.min((first_delay_dur.as_millis() * 3).try_into().unwrap());
        assert!(second_delay_dur <= Duration::from_millis(cap_ms));

        let third_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(third_delay_dur >= Duration::from_millis(2));
        let cap_ms = 7u64.min((second_delay_dur.as_millis() * 3).try_into().unwrap());
        assert!(second_delay_dur <= Duration::from_millis(cap_ms));

        assert_eq!(backoff.next_delay_duration(), None);
    }

    #[test]
    #[should_panic(expected = "base_delay_ms must be positive")]
    fn test_decorrelated_jitter_backoff_with_invalid_base_delay_ms() {
        Backoff::decorrelated_jitter_backoff(0, 7, 3);
    }
}
