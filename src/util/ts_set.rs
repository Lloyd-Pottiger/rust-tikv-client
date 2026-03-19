use std::collections::HashSet;
use std::sync::RwLock;

/// A thread-safe set of timestamps.
///
/// This mirrors client-go `util.TSSet`. The backing set is lazily allocated on first insertion to
/// avoid allocations in the common case where the set is unused.
#[derive(Debug, Default)]
pub struct TsSet {
    inner: RwLock<Option<HashSet<u64>>>,
}

impl TsSet {
    pub fn new() -> TsSet {
        TsSet::default()
    }

    /// Insert timestamps into the set.
    pub fn put(&self, tss: impl IntoIterator<Item = u64>) {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let set = guard.get_or_insert_with(|| HashSet::with_capacity(5));
        for ts in tss {
            set.insert(ts);
        }
    }

    /// Return all timestamps currently in the set.
    pub fn get_all(&self) -> Vec<u64> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let Some(set) = guard.as_ref() else {
            return Vec::new();
        };
        if set.is_empty() {
            return Vec::new();
        }
        set.iter().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ts_set_put_and_get_all() {
        let set = TsSet::new();
        assert!(set.get_all().is_empty());

        set.put([1u64, 2, 3]);
        let mut got = set.get_all();
        got.sort_unstable();
        assert_eq!(vec![1, 2, 3], got);

        set.put([2u64, 4]);
        let mut got = set.get_all();
        got.sort_unstable();
        assert_eq!(vec![1, 2, 3, 4], got);
    }
}
