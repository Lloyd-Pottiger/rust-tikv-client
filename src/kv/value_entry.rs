// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use crate::Value;

/// A value paired with its commit timestamp (when requested).
///
/// This is a small, self-contained port of `client-go/kv.ValueEntry`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ValueEntry {
    pub value: Value,
    /// Commit timestamp of this value. `0` means "unknown / not requested".
    pub commit_ts: u64,
}

impl ValueEntry {
    pub fn new(value: impl Into<Value>, commit_ts: u64) -> ValueEntry {
        ValueEntry {
            value: value.into(),
            commit_ts,
        }
    }

    pub fn is_value_empty(&self) -> bool {
        self.value.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_entry_is_value_empty_matches_client_go_test() {
        assert!(ValueEntry::default().is_value_empty());
        assert!(ValueEntry::new(Vec::<u8>::new(), 123).is_value_empty());
        assert!(ValueEntry::new(Vec::<u8>::new(), 0).is_value_empty());
        assert!(!ValueEntry::new(vec![b'x'], 123).is_value_empty());
        assert!(!ValueEntry::new(vec![b'x'], 0).is_value_empty());
    }
}

