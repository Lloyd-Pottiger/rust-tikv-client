// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go client parity: `client-go/util/rate_limit.go`.

use tokio::sync::mpsc;
use tokio::sync::Semaphore;

/// A small token limiter used by some client-go utilities.
///
/// The limiter starts with `capacity` tokens available. A token is consumed by [`RateLimit::get_token`]
/// and returned by [`RateLimit::put_token`]. Returning a token when the limiter is already full is a
/// logic bug and will panic (mirrors client-go behavior).
#[derive(Debug)]
pub(crate) struct RateLimit {
    sem: Semaphore,
    capacity: usize,
}

impl RateLimit {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            sem: Semaphore::new(capacity),
            capacity,
        }
    }

    pub(crate) fn put_token(&self) {
        if self.sem.available_permits() >= self.capacity {
            panic!("put a redundant token");
        }
        self.sem.add_permits(1);
    }

    /// Get a token, or exit early if `done` is signaled.
    ///
    /// Returns `true` if exited, `false` if a token was acquired.
    pub(crate) async fn get_token(&self, done: &mut mpsc::Receiver<()>) -> bool {
        tokio::select! {
            permit = self.sem.acquire() => {
                let permit = permit.expect("rate limit semaphore must not be closed");
                permit.forget(); // token must be returned explicitly via `put_token`.
                false
            }
            _ = done.recv() => true,
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    fn assert_panics_with_message<F: FnOnce() + std::panic::UnwindSafe>(f: F, expected: &str) {
        let err = std::panic::catch_unwind(f).expect_err("expected panic");
        let msg = err
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| err.downcast_ref::<String>().map(|s| s.as_str()))
            .expect("panic payload must be a string");
        assert_eq!(msg, expected);
    }

    #[tokio::test]
    async fn test_rate_limit() {
        let (done_tx, mut done_rx) = mpsc::channel(1);
        let rl = Arc::new(RateLimit::new(1));

        assert_panics_with_message(|| rl.put_token(), "put a redundant token");

        let exit = rl.get_token(&mut done_rx).await;
        assert!(!exit);

        rl.put_token();
        assert_panics_with_message(|| rl.put_token(), "put a redundant token");

        let exit = rl.get_token(&mut done_rx).await;
        assert!(!exit);

        done_tx.send(()).await.unwrap();
        let exit = rl.get_token(&mut done_rx).await;
        assert!(exit);

        let (sig_tx, sig_rx) = oneshot::channel();
        let rl_task = rl.clone();
        tokio::spawn(async move {
            let mut done_rx = done_rx;
            let exit = rl_task.get_token(&mut done_rx).await;
            assert!(!exit);
            let _ = sig_tx.send(());
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        rl.put_token();
        sig_rx.await.unwrap();
    }
}
