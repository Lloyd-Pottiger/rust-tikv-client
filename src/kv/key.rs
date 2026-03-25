// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::ops::Bound;

#[allow(unused_imports)]
#[cfg(test)]
use proptest::arbitrary::any_with;
#[allow(unused_imports)]
#[cfg(test)]
use proptest::collection::size_range;
#[cfg(test)]
use proptest_derive::Arbitrary;

use super::HexRepr;
use crate::kv::codec::BytesEncoder;
use crate::kv::codec::{self};
use crate::proto::kvrpcpb;
use crate::proto::kvrpcpb::KvPair;

const _PROPTEST_KEY_MAX: usize = 1024 * 2; // 2 KB

/// Compare two keys using byte-wise lexicographic ordering.
///
/// This maps to client-go `kv.CmpKey` (which returns `-1/0/1`).
#[doc(alias = "CmpKey")]
#[must_use]
pub fn cmp_key(key: &[u8], another: &[u8]) -> std::cmp::Ordering {
    key.cmp(another)
}

/// Returns the next key in byte-order.
///
/// This maps to client-go `kv.NextKey`.
#[doc(alias = "NextKey")]
#[must_use]
pub fn next_key(key: impl Into<Key>) -> Key {
    key.into().next_key()
}

/// Returns the next prefix key.
///
/// This maps to client-go `kv.PrefixNextKey`.
#[doc(alias = "PrefixNextKey")]
#[must_use]
pub fn prefix_next_key(key: impl Into<Key>) -> Key {
    key.into().prefix_next_key()
}

/// The key part of a key/value pair.
///
/// In TiKV, keys are an ordered sequence of bytes. This has an advantage over choosing `String` as
/// valid `UTF-8` is not required. This means that the user is permitted to store any data they wish,
/// as long as it can be represented by bytes. (Which is to say, pretty much anything!)
///
/// This type wraps around an owned value, so it should be treated it like `String` or `Vec<u8>`.
///
/// # Examples
/// ```rust
/// use tikv_client::Key;
///
/// let static_str: &'static str = "TiKV";
/// let from_static_str = Key::from(static_str.to_owned());
///
/// let string: String = String::from(static_str);
/// let from_string = Key::from(string);
/// assert_eq!(from_static_str, from_string);
///
/// let vec: Vec<u8> = static_str.as_bytes().to_vec();
/// let from_vec = Key::from(vec);
/// assert_eq!(from_static_str, from_vec);
///
/// let bytes = static_str.as_bytes().to_vec();
/// let from_bytes = Key::from(bytes);
/// assert_eq!(from_static_str, from_bytes);
/// ```
///
/// While `.into()` is usually sufficient for obtaining the buffer itself, sometimes type inference
/// isn't able to determine the correct type. Notably in the `assert_eq!()` and `==` cases. In
/// these cases using the fully-qualified-syntax is useful:
///
/// # Examples
/// ```rust
/// use tikv_client::Key;
///
/// let buf = "TiKV".as_bytes().to_owned();
/// let key = Key::from(buf.clone());
/// assert_eq!(Into::<Vec<u8>>::into(key), buf);
/// ```
///
/// Many functions which accept a `Key` accept an `Into<Key>`, which means all of the above types
/// can be passed directly to those functions.
#[derive(Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(test, derive(Arbitrary))]
#[repr(transparent)]
pub struct Key(
    #[cfg_attr(
        test,
        proptest(strategy = "any_with::<Vec<u8>>((size_range(_PROPTEST_KEY_MAX), ()))")
    )]
    pub(crate) Vec<u8>,
);

impl AsRef<Key> for kvrpcpb::Mutation {
    fn as_ref(&self) -> &Key {
        self.key.as_ref()
    }
}

pub struct KvPairTTL(pub KvPair, pub u64);

impl AsRef<Key> for KvPairTTL {
    fn as_ref(&self) -> &Key {
        self.0.key.as_ref()
    }
}

impl From<KvPairTTL> for (KvPair, u64) {
    fn from(value: KvPairTTL) -> Self {
        (value.0, value.1)
    }
}

impl Key {
    /// The empty key.
    pub const EMPTY: Self = Key(Vec::new());

    /// Return whether the key is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return whether the last byte of key is 0.
    #[inline]
    pub(super) fn zero_terminated(&self) -> bool {
        self.0.last().map(|i| *i == 0).unwrap_or(false)
    }

    /// Push a zero to the end of the key.
    ///
    /// Extending a zero makes the new key the smallest key that is greater than than the original one.
    ///
    /// This maps to client-go `kv.NextKey` (as an in-place, consuming operation).
    #[doc(alias = "NextKey")]
    #[inline]
    pub fn next_key(mut self) -> Self {
        self.0.push(0);
        self
    }

    /// Return the next prefix key.
    ///
    /// This maps to client-go `kv.PrefixNextKey` (as an in-place, consuming operation).
    #[doc(alias = "PrefixNextKey")]
    #[inline]
    #[must_use]
    pub fn prefix_next_key(mut self) -> Self {
        for i in (0..self.0.len()).rev() {
            self.0[i] = self.0[i].wrapping_add(1);
            if self.0[i] != 0 {
                return self;
            }
        }
        self.0.clear();
        self
    }

    /// Convert the key to a lower bound. The key is treated as inclusive.
    #[inline]
    pub(super) fn into_lower_bound(mut self) -> Bound<Key> {
        if self.zero_terminated() {
            self.0.pop();
            Bound::Excluded(self)
        } else {
            Bound::Included(self)
        }
    }

    /// Convert the key to an upper bound. The key is treated as exclusive.
    #[inline]
    pub(super) fn into_upper_bound(mut self) -> Bound<Key> {
        if self.zero_terminated() {
            self.0.pop();
            Bound::Included(self)
        } else {
            Bound::Excluded(self)
        }
    }

    /// Return the MVCC-encoded representation of the key.
    #[inline]
    #[must_use]
    pub fn to_encoded(&self) -> Key {
        let len = codec::max_encoded_bytes_size(self.0.len());
        let mut encoded = Vec::with_capacity(len);
        if let Err(err) = encoded.encode_bytes(&self.0, false) {
            debug_assert!(
                false,
                "encode_bytes should be infallible for Vec<u8>: {err:?}"
            );
        }
        Key(encoded)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// A range where `start_key <= key < end_key`.
///
/// This maps to client-go `kv.KeyRange`.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct KeyRange {
    pub start_key: Key,
    pub end_key: Key,
}

impl KeyRange {
    #[must_use]
    pub fn new(start_key: impl Into<Key>, end_key: impl Into<Key>) -> Self {
        Self {
            start_key: start_key.into(),
            end_key: end_key.into(),
        }
    }
}

impl From<KeyRange> for super::BoundRange {
    fn from(range: KeyRange) -> Self {
        (range.start_key..range.end_key).into()
    }
}

impl From<KeyRange> for kvrpcpb::KeyRange {
    fn from(range: KeyRange) -> Self {
        let bound: super::BoundRange = range.into();
        bound.into()
    }
}

impl From<Vec<u8>> for Key {
    fn from(v: Vec<u8>) -> Self {
        Key(v)
    }
}

impl From<String> for Key {
    fn from(v: String) -> Key {
        Key(v.into_bytes())
    }
}

impl From<Key> for Vec<u8> {
    fn from(key: Key) -> Self {
        key.0
    }
}

impl<'a> From<&'a Key> for &'a [u8] {
    fn from(key: &'a Key) -> Self {
        &key.0
    }
}

impl<'a> From<&'a Vec<u8>> for &'a Key {
    fn from(key: &'a Vec<u8>) -> Self {
        // SAFETY: `Key` is `#[repr(transparent)]` over `Vec<u8>` and has no additional
        // invariants, so the references are layout-compatible.
        unsafe { &*(key as *const Vec<u8> as *const Key) }
    }
}
impl AsRef<Key> for Key {
    fn as_ref(&self) -> &Key {
        self
    }
}

impl AsRef<Key> for Vec<u8> {
    fn as_ref(&self) -> &Key {
        // SAFETY: `Key` is `#[repr(transparent)]` over `Vec<u8>` and has no additional
        // invariants, so the references are layout-compatible.
        unsafe { &*(self as *const Vec<u8> as *const Key) }
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Key({})", HexRepr(&self.0))
    }
}
