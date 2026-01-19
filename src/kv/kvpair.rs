// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::str;

#[cfg(test)]
use proptest_derive::Arbitrary;

use super::HexRepr;
use super::Key;
use super::Value;
use crate::proto::kvrpcpb;

/// A key/value pair.
///
/// # Examples
/// ```rust
/// # use tikv_client::{Key, Value, KvPair};
/// let key = "key".to_owned();
/// let value = "value".to_owned();
/// let constructed = KvPair::new(key.clone(), value.clone());
/// let from_tuple = KvPair::from((key, value));
/// assert_eq!(constructed, from_tuple);
/// ```
///
/// Many functions which accept a `KvPair` accept an `Into<KvPair>`, which means all of the above
/// types (Like a `(Key, Value)`) can be passed directly to those functions.
#[derive(Default, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct KvPair {
    pub key: Key,
    pub value: Value,
    /// Commit timestamp of this value (when requested). `0` means "unknown / not requested".
    pub commit_ts: u64,
}

impl KvPair {
    /// Create a new `KvPair`.
    #[inline]
    pub fn new(key: impl Into<Key>, value: impl Into<Value>) -> Self {
        Self::new_with_commit_ts(key, value, 0)
    }

    /// Create a new `KvPair` with the given commit timestamp.
    #[inline]
    pub fn new_with_commit_ts(
        key: impl Into<Key>,
        value: impl Into<Value>,
        commit_ts: u64,
    ) -> Self {
        KvPair {
            key: key.into(),
            value: value.into(),
            commit_ts,
        }
    }

    /// Immutably borrow the `Key` part of the `KvPair`.
    #[inline]
    pub fn key(&self) -> &Key {
        &self.key
    }

    /// Immutably borrow the `Value` part of the `KvPair`.
    #[inline]
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Consume `self` and return the `Key` part.
    #[inline]
    pub fn into_key(self) -> Key {
        self.key
    }

    /// Consume `self` and return the `Value` part.
    #[inline]
    pub fn into_value(self) -> Value {
        self.value
    }

    /// Mutably borrow the `Key` part of the `KvPair`.
    #[inline]
    pub fn key_mut(&mut self) -> &mut Key {
        &mut self.key
    }

    /// Mutably borrow the `Value` part of the `KvPair`.
    #[inline]
    pub fn value_mut(&mut self) -> &mut Value {
        &mut self.value
    }

    /// Set the `Key` part of the `KvPair`.
    #[inline]
    pub fn set_key(&mut self, k: impl Into<Key>) {
        self.key = k.into();
    }

    /// Set the `Value` part of the `KvPair`.
    #[inline]
    pub fn set_value(&mut self, v: impl Into<Value>) {
        self.value = v.into();
    }
}

impl<K, V> From<(K, V)> for KvPair
where
    K: Into<Key>,
    V: Into<Value>,
{
    fn from((k, v): (K, V)) -> Self {
        KvPair::new(k, v)
    }
}

impl From<KvPair> for (Key, Value) {
    fn from(pair: KvPair) -> Self {
        (pair.key, pair.value)
    }
}

impl From<KvPair> for Key {
    fn from(pair: KvPair) -> Self {
        pair.key
    }
}

impl From<kvrpcpb::KvPair> for KvPair {
    fn from(pair: kvrpcpb::KvPair) -> Self {
        KvPair::new_with_commit_ts(Key::from(pair.key), pair.value, pair.commit_ts)
    }
}

impl From<KvPair> for kvrpcpb::KvPair {
    fn from(pair: KvPair) -> Self {
        let mut result = kvrpcpb::KvPair::default();
        result.key = pair.key.into();
        result.value = pair.value;
        result.commit_ts = pair.commit_ts;
        result
    }
}

impl AsRef<Key> for KvPair {
    fn as_ref(&self) -> &Key {
        &self.key
    }
}

impl AsRef<Value> for KvPair {
    fn as_ref(&self) -> &Value {
        &self.value
    }
}

impl fmt::Debug for KvPair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match str::from_utf8(&self.value) {
            Ok(s) => write!(
                f,
                "KvPair({}, {:?}, commit_ts={})",
                HexRepr(&self.key.0),
                s,
                self.commit_ts
            ),
            Err(_) => write!(
                f,
                "KvPair({}, {}, commit_ts={})",
                HexRepr(&self.key.0),
                HexRepr(&self.value),
                self.commit_ts
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_and_mutators() {
        let mut pair = KvPair::new("k".to_owned(), "v".to_owned());
        assert_eq!(pair.key(), &Key::from("k".to_owned()));
        assert_eq!(pair.value(), &Value::from("v".to_owned()));

        pair.set_key("k2".to_owned());
        pair.set_value("v2".to_owned());
        assert_eq!(pair.key(), &Key::from("k2".to_owned()));
        assert_eq!(pair.value(), &Value::from("v2".to_owned()));

        pair.key_mut().0.push(b'X');
        pair.value_mut().push(b'Y');
        assert!(pair.key().0.ends_with(b"X"));
        assert!(pair.value().ends_with(b"Y"));

        let key: Key = pair.clone().into();
        assert_eq!(key, pair.key);

        let (k, v): (Key, Value) = pair.clone().into();
        assert_eq!(k, pair.key);
        assert_eq!(v, pair.value);
    }

    #[test]
    fn debug_formats_utf8_and_non_utf8_values() {
        let pair = KvPair::new("key".to_owned(), "hello".to_owned());
        let s = format!("{pair:?}");
        assert!(s.contains("KvPair("));
        assert!(s.contains("\"hello\""));

        let pair = KvPair::new("key".to_owned(), vec![0xFF, 0x00, 0xAA]);
        let s = format!("{pair:?}");
        // value printed as hex when not valid UTF-8
        assert!(s.contains("FF00AA"), "{s}");
    }

    #[test]
    fn proto_round_trip() {
        let pair = KvPair::new_with_commit_ts("k".to_owned(), vec![1, 2, 3], 9);
        let proto: kvrpcpb::KvPair = pair.clone().into();
        let back: KvPair = proto.into();
        assert_eq!(back, pair);
    }
}
