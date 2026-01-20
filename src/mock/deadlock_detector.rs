use std::collections::HashMap;
use std::sync::Mutex;

use thiserror::Error;

/// Go client parity: `client-go/internal/mockstore/deadlock/Detector`.
#[derive(Debug, Default)]
pub(crate) struct DeadlockDetector {
    wait_for_map: Mutex<HashMap<u64, Vec<TxnKeyHashPair>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TxnKeyHashPair {
    txn: u64,
    key_hash: u64,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("deadlock({key_hash})")]
pub(crate) struct DeadlockError {
    pub(crate) key_hash: u64,
}

impl DeadlockDetector {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Detect a wait-for edge `source_txn -> wait_for_txn` on `key_hash`.
    pub(crate) fn detect(
        &self,
        source_txn: u64,
        wait_for_txn: u64,
        key_hash: u64,
    ) -> Result<(), DeadlockError> {
        let mut map = self.wait_for_map.lock().unwrap();
        if let Some(err) = do_detect(&map, source_txn, wait_for_txn) {
            return Err(err);
        }
        register(&mut map, source_txn, wait_for_txn, key_hash);
        Ok(())
    }

    pub(crate) fn clean_up(&self, txn: u64) {
        self.wait_for_map.lock().unwrap().remove(&txn);
    }

    pub(crate) fn clean_up_wait_for(&self, txn: u64, wait_for_txn: u64, key_hash: u64) {
        let pair = TxnKeyHashPair {
            txn: wait_for_txn,
            key_hash,
        };
        let mut map = self.wait_for_map.lock().unwrap();
        let Some(list) = map.get_mut(&txn) else {
            return;
        };

        if let Some(pos) = list.iter().position(|p| *p == pair) {
            list.remove(pos);
        }

        if list.is_empty() {
            map.remove(&txn);
        }
    }

    pub(crate) fn expire(&self, min_ts: u64) {
        let mut map = self.wait_for_map.lock().unwrap();
        map.retain(|&ts, _| ts >= min_ts);
    }

    #[cfg(test)]
    fn wait_for_list_len(&self, txn: u64) -> Option<usize> {
        self.wait_for_map.lock().unwrap().get(&txn).map(|l| l.len())
    }

    #[cfg(test)]
    fn contains_txn(&self, txn: u64) -> bool {
        self.wait_for_map.lock().unwrap().contains_key(&txn)
    }

    #[cfg(test)]
    fn map_len(&self) -> usize {
        self.wait_for_map.lock().unwrap().len()
    }
}

fn do_detect(
    map: &HashMap<u64, Vec<TxnKeyHashPair>>,
    source_txn: u64,
    wait_for_txn: u64,
) -> Option<DeadlockError> {
    let list = map.get(&wait_for_txn)?;
    for next in list {
        if next.txn == source_txn {
            return Some(DeadlockError {
                key_hash: next.key_hash,
            });
        }
        if let Some(err) = do_detect(map, source_txn, next.txn) {
            return Some(err);
        }
    }
    None
}

fn register(
    map: &mut HashMap<u64, Vec<TxnKeyHashPair>>,
    source_txn: u64,
    wait_for_txn: u64,
    key_hash: u64,
) {
    let pair = TxnKeyHashPair {
        txn: wait_for_txn,
        key_hash,
    };
    let list = map.entry(source_txn).or_default();
    if list.iter().any(|p| *p == pair) {
        return;
    }
    list.push(pair);
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deadlock_detector() {
        let detector = DeadlockDetector::new();

        detector.detect(1, 2, 100).unwrap();
        detector.detect(2, 3, 200).unwrap();
        let err = detector.detect(3, 1, 300).unwrap_err();
        assert_eq!(err.to_string(), "deadlock(200)");

        detector.clean_up(2);
        assert!(!detector.contains_txn(2));

        // After cycle is broken, no deadlock now.
        detector.detect(3, 1, 300).unwrap();
        assert_eq!(detector.wait_for_list_len(3), Some(1));

        // Different keyHash grows the list.
        detector.detect(3, 1, 400).unwrap();
        assert_eq!(detector.wait_for_list_len(3), Some(2));

        // Same waitFor and key hash doesn't grow the list.
        detector.detect(3, 1, 400).unwrap();
        assert_eq!(detector.wait_for_list_len(3), Some(2));

        detector.clean_up_wait_for(3, 1, 300);
        assert_eq!(detector.wait_for_list_len(3), Some(1));
        detector.clean_up_wait_for(3, 1, 400);
        assert!(!detector.contains_txn(3));

        detector.expire(1);
        assert_eq!(detector.map_len(), 1);
        detector.expire(2);
        assert_eq!(detector.map_len(), 0);
    }
}
