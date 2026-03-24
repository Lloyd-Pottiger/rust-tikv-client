use std::future::Future;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

static GLOBAL_SHUTTING_DOWN: AtomicU32 = AtomicU32::new(0);

tokio::task_local! {
    static TASK_SHUTTING_DOWN: u32;
}

/// Store the global "shutting down" flag.
///
/// This mirrors client-go `tikv.StoreShuttingDown`.
pub fn store_shutting_down(v: u32) {
    GLOBAL_SHUTTING_DOWN.store(v, Ordering::Relaxed);
}

/// Load the global "shutting down" flag.
///
/// This mirrors client-go `tikv.LoadShuttingDown`.
#[must_use]
pub fn load_shutting_down() -> u32 {
    TASK_SHUTTING_DOWN
        .try_with(|value| *value)
        .unwrap_or_else(|_| GLOBAL_SHUTTING_DOWN.load(Ordering::Relaxed))
}

pub(crate) fn task_shutting_down() -> Option<u32> {
    TASK_SHUTTING_DOWN.try_with(|value| *value).ok()
}

pub(crate) fn with_shutting_down<T, F>(v: u32, future: F) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_SHUTTING_DOWN.scope(v, future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_and_load_shutting_down() {
        store_shutting_down(0);
        assert_eq!(load_shutting_down(), 0);

        store_shutting_down(1);
        assert_eq!(load_shutting_down(), 1);

        store_shutting_down(0);
        assert_eq!(load_shutting_down(), 0);
    }
}
