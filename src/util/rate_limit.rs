use std::sync::Arc;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// Concurrency limiter for async tasks.
///
/// This mirrors client-go `util.RateLimit`, but uses a Tokio semaphore and returns an RAII permit
/// instead of requiring an explicit `PutToken`.
#[derive(Clone, Debug)]
pub struct RateLimit {
    capacity: usize,
    semaphore: Arc<Semaphore>,
}

impl RateLimit {
    /// Create a new limiter with `capacity` concurrent permits.
    pub fn new(capacity: usize) -> Self {
        RateLimit {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity)),
        }
    }

    /// Return the configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Try to acquire a permit without waiting.
    pub fn try_acquire(&self) -> Result<Option<RateLimitPermit>, RateLimitError> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(Some(RateLimitPermit { _permit: permit })),
            Err(tokio::sync::TryAcquireError::NoPermits) => Ok(None),
            Err(tokio::sync::TryAcquireError::Closed) => Err(RateLimitError::Closed),
        }
    }

    /// Acquire a permit, waiting if necessary.
    pub async fn acquire(&self) -> Result<RateLimitPermit, RateLimitError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RateLimitError::Closed)?;
        Ok(RateLimitPermit { _permit: permit })
    }
}

/// Create a new limiter with `capacity` concurrent permits.
///
/// This mirrors client-go `util.NewRateLimit` while reusing [`RateLimit::new`].
#[doc(alias = "NewRateLimit")]
#[must_use]
pub fn new_rate_limit(capacity: usize) -> RateLimit {
    RateLimit::new(capacity)
}

/// Errors returned by [`RateLimit`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RateLimitError {
    /// The underlying semaphore has been closed and can no longer issue permits.
    #[error("rate limit is closed")]
    Closed,
}

/// A permit held from a [`RateLimit`].
///
/// Dropping the permit releases capacity back to the limiter.
#[derive(Debug)]
pub struct RateLimitPermit {
    _permit: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn test_rate_limit_try_acquire_and_drop_releases() {
        let limit = RateLimit::new(1);

        let permit = limit.acquire().await.unwrap();
        assert_eq!(1, limit.capacity());
        assert!(limit.try_acquire().unwrap().is_none());

        drop(permit);
        assert!(limit.try_acquire().unwrap().is_some());
    }

    #[tokio::test]
    async fn test_rate_limit_acquire_blocks_until_permit_dropped() {
        let limit = RateLimit::new(1);
        let permit = limit.acquire().await.unwrap();

        let acquired = Arc::new(AtomicBool::new(false));
        let acquired_flag = acquired.clone();
        let limit_task = limit.clone();
        tokio::spawn(async move {
            let _permit = limit_task.acquire().await.unwrap();
            acquired_flag.store(true, Ordering::Release);
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!acquired.load(Ordering::Acquire));

        drop(permit);

        let start = Instant::now();
        while !acquired.load(Ordering::Acquire) {
            assert!(start.elapsed() < Duration::from_secs(1));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[test]
    fn test_new_rate_limit_delegates_to_inherent_constructor() {
        let helper = new_rate_limit(2);
        let direct = RateLimit::new(2);

        assert_eq!(helper.capacity(), direct.capacity());
        assert!(helper.try_acquire().unwrap().is_some());
    }
}
