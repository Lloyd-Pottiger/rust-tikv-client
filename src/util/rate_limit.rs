use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

#[derive(Debug)]
struct RateLimitInner {
    desired_capacity: AtomicUsize,
    pending_forget: AtomicUsize,
    semaphore: Arc<Semaphore>,
}

impl RateLimitInner {
    fn try_forget_one(&self) -> bool {
        self.pending_forget
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                if pending == 0 {
                    None
                } else {
                    Some(pending - 1)
                }
            })
            .is_ok()
    }

    fn try_forget_available(&self) {
        let pending = self.pending_forget.load(Ordering::Acquire);
        if pending == 0 {
            return;
        }

        let available = self.semaphore.available_permits();
        let to_forget = pending.min(available);
        if to_forget == 0 {
            return;
        }

        let to_forget = u32::try_from(to_forget).unwrap_or(u32::MAX);
        if let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_many_owned(to_forget) {
            let to_forget_usize = usize::try_from(to_forget).unwrap_or(usize::MAX);
            let _ =
                self.pending_forget
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                        Some(pending.saturating_sub(to_forget_usize))
                    });
            permit.forget();
        }
    }
}

/// Concurrency limiter for async tasks.
///
/// This mirrors client-go `util.RateLimit`, but uses a Tokio semaphore and returns an RAII permit
/// instead of requiring an explicit `PutToken`.
#[derive(Clone, Debug)]
pub struct RateLimit {
    inner: Arc<RateLimitInner>,
}

impl RateLimit {
    /// Create a new limiter with `capacity` concurrent permits.
    pub fn new(capacity: usize) -> Self {
        RateLimit {
            inner: Arc::new(RateLimitInner {
                desired_capacity: AtomicUsize::new(capacity),
                pending_forget: AtomicUsize::new(0),
                semaphore: Arc::new(Semaphore::new(capacity)),
            }),
        }
    }

    /// Return the configured capacity.
    pub fn capacity(&self) -> usize {
        self.inner.desired_capacity.load(Ordering::Relaxed)
    }

    pub(crate) fn set_capacity(&self, capacity: usize) {
        let desired = self.inner.desired_capacity.load(Ordering::Acquire);
        let pending = self.inner.pending_forget.load(Ordering::Acquire);
        if desired == capacity && pending == 0 {
            return;
        }

        let actual = desired.saturating_add(pending);
        if capacity > actual {
            self.inner.semaphore.add_permits(capacity - actual);
            self.inner.pending_forget.store(0, Ordering::Release);
            self.inner
                .desired_capacity
                .store(capacity, Ordering::Release);
            return;
        }

        self.inner
            .desired_capacity
            .store(capacity, Ordering::Release);
        self.inner
            .pending_forget
            .store(actual - capacity, Ordering::Release);
        self.inner.try_forget_available();
    }

    /// Try to acquire a permit without waiting.
    pub fn try_acquire(&self) -> Result<Option<RateLimitPermit>, RateLimitError> {
        match self.inner.semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(Some(RateLimitPermit {
                inner: Arc::clone(&self.inner),
                permit: Some(permit),
            })),
            Err(tokio::sync::TryAcquireError::NoPermits) => Ok(None),
            Err(tokio::sync::TryAcquireError::Closed) => Err(RateLimitError::Closed),
        }
    }

    /// Acquire a permit, waiting if necessary.
    pub async fn acquire(&self) -> Result<RateLimitPermit, RateLimitError> {
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RateLimitError::Closed)?;
        Ok(RateLimitPermit {
            inner: Arc::clone(&self.inner),
            permit: Some(permit),
        })
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
    inner: Arc<RateLimitInner>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for RateLimitPermit {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };

        if self.inner.try_forget_one() {
            permit.forget();
            return;
        }

        drop(permit);
    }
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

    #[tokio::test]
    async fn test_rate_limit_set_capacity_shrinks_and_grows() {
        let limit = RateLimit::new(2);
        let permit_1 = limit.acquire().await.unwrap();
        let permit_2 = limit.acquire().await.unwrap();
        assert_eq!(limit.capacity(), 2);

        // Shrink while all permits are in use: capacity updates immediately, enforcement takes
        // effect as permits are released.
        limit.set_capacity(1);
        assert_eq!(limit.capacity(), 1);

        drop(permit_1);
        assert!(limit.try_acquire().unwrap().is_none());

        drop(permit_2);
        let permit = limit.try_acquire().unwrap().expect("permit");
        assert!(limit.try_acquire().unwrap().is_none());
        drop(permit);

        // Grow again.
        limit.set_capacity(2);
        assert_eq!(limit.capacity(), 2);
        let permit_1 = limit.try_acquire().unwrap().expect("permit");
        let permit_2 = limit.try_acquire().unwrap().expect("permit");
        assert!(limit.try_acquire().unwrap().is_none());
        drop(permit_1);
        drop(permit_2);
    }

    #[test]
    fn test_new_rate_limit_delegates_to_inherent_constructor() {
        let helper = new_rate_limit(2);
        let direct = RateLimit::new(2);

        assert_eq!(helper.capacity(), direct.capacity());
        assert!(helper.try_acquire().unwrap().is_some());
    }
}
