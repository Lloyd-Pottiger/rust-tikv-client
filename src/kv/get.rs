// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use async_trait::async_trait;

use super::Key;
use super::KvPair;
use super::Value;
use crate::Result;

/// A value returned by a point read, optionally carrying the commit timestamp.
///
/// This mirrors client-go `kv.ValueEntry` as used by `Getter` / `BatchGetter`, but follows Rust
/// conventions:
/// - missing keys are represented as `value: None` rather than an `ErrNotExist`;
/// - `commit_ts == 0` means "unknown / not returned".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ValueEntry {
    /// Value returned by the read, or `None` when the key does not exist.
    pub value: Option<Value>,
    /// Commit timestamp returned by the server, or `0` when it was not requested.
    pub commit_ts: u64,
}

impl ValueEntry {
    /// Create a new [`ValueEntry`].
    #[must_use]
    pub fn new(value: Option<Value>, commit_ts: u64) -> ValueEntry {
        ValueEntry { value, commit_ts }
    }

    /// Returns `true` if the key exists.
    #[must_use]
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    /// Returns `true` if the value is empty.
    ///
    /// This follows client-go `ValueEntry.IsValueEmpty`: missing keys also return `true`.
    #[must_use]
    pub fn is_value_empty(&self) -> bool {
        match self.value.as_ref() {
            None => true,
            Some(value) => value.is_empty(),
        }
    }

    /// Return an approximate in-memory size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.value.as_ref().map_or(0, Vec::len)
    }
}

/// A get or batch_get option.
///
/// This mirrors the shared option set used by client-go `kv.GetOption` / `kv.BatchGetOption`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GetOrBatchGetOption {
    /// Require TiKV to return commit timestamps for returned values (when supported).
    ///
    /// This mirrors client-go `kv.WithReturnCommitTS`.
    ReturnCommitTs,
}

/// Shared option alias for single-key get operations.
///
/// This currently aliases [`GetOrBatchGetOption`] so single-key and batch-get
/// requests stay on the same stable option surface.
pub type GetOption = GetOrBatchGetOption;
/// Shared option alias for batch-get operations.
///
/// This currently aliases [`GetOrBatchGetOption`] so single-key and batch-get
/// requests stay on the same stable option surface.
pub type BatchGetOption = GetOrBatchGetOption;

/// Returns an option indicating commit timestamps should be returned (when supported).
///
/// This mirrors client-go `kv.WithReturnCommitTS`.
#[must_use]
pub fn with_return_commit_ts() -> GetOrBatchGetOption {
    GetOrBatchGetOption::ReturnCommitTs
}

/// Converts batch-get options into the equivalent single-get options.
///
/// This mirrors client-go `kv.BatchGetToGetOptions`. In the Rust client both option types are
/// aliases of [`GetOrBatchGetOption`], so this preserves order and simply copies the shared enum
/// values into a new [`Vec`].
#[doc(alias = "BatchGetToGetOptions")]
#[must_use]
pub fn batch_get_to_get_options(options: &[BatchGetOption]) -> Vec<GetOption> {
    options.to_vec()
}

/// Options passed to get operations.
///
/// This mirrors client-go `kv.GetOptions`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct GetOptions {
    return_commit_ts: bool,
}

impl GetOptions {
    /// Apply get options.
    pub fn apply(&mut self, opts: &[GetOption]) {
        for opt in opts {
            match opt {
                GetOrBatchGetOption::ReturnCommitTs => self.return_commit_ts = true,
            }
        }
    }

    /// Whether to require commit timestamps for returned values.
    #[must_use]
    pub fn return_commit_ts(&self) -> bool {
        self.return_commit_ts
    }
}

/// Options passed to batch get operations.
///
/// This mirrors client-go `kv.BatchGetOptions`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchGetOptions {
    return_commit_ts: bool,
}

impl BatchGetOptions {
    /// Apply batch get options.
    pub fn apply(&mut self, opts: &[BatchGetOption]) {
        for opt in opts {
            match opt {
                GetOrBatchGetOption::ReturnCommitTs => self.return_commit_ts = true,
            }
        }
    }

    /// Whether to require commit timestamps for returned values.
    #[must_use]
    pub fn return_commit_ts(&self) -> bool {
        self.return_commit_ts
    }
}

/// A point-read interface.
///
/// This mirrors client-go `kv.Getter`.
#[async_trait]
pub trait Getter {
    /// Get the value for the given key.
    ///
    /// When `with_return_commit_ts()` is present in `options`, this calls the underlying
    /// `*_with_commit_ts` APIs (when supported).
    async fn get(&mut self, key: Key, options: &[GetOption]) -> Result<ValueEntry>;
}

/// A batch point-read interface.
///
/// This mirrors client-go `kv.BatchGetter`.
#[async_trait]
pub trait BatchGetter {
    /// Batch get values for the given keys.
    ///
    /// Non-existent keys will not appear in the returned map. This follows the behavior of TiKV's
    /// batch get RPCs and matches client-go's `BatchGet` map shape.
    async fn batch_get(
        &mut self,
        keys: Vec<Key>,
        options: &[BatchGetOption],
    ) -> Result<HashMap<Key, ValueEntry>>;
}

#[async_trait]
impl Getter for crate::Snapshot {
    async fn get(&mut self, key: Key, options: &[GetOption]) -> Result<ValueEntry> {
        let mut opts = GetOptions::default();
        opts.apply(options);
        if opts.return_commit_ts() {
            let value = crate::Snapshot::get_with_commit_ts(self, key).await?;
            Ok(match value {
                Some((value, commit_ts)) => ValueEntry::new(Some(value), commit_ts),
                None => ValueEntry::new(None, 0),
            })
        } else {
            Ok(ValueEntry::new(crate::Snapshot::get(self, key).await?, 0))
        }
    }
}

#[async_trait]
impl BatchGetter for crate::Snapshot {
    async fn batch_get(
        &mut self,
        keys: Vec<Key>,
        options: &[BatchGetOption],
    ) -> Result<HashMap<Key, ValueEntry>> {
        let capacity = keys.len();
        let mut opts = BatchGetOptions::default();
        opts.apply(options);

        let mut out = HashMap::with_capacity(capacity);
        if opts.return_commit_ts() {
            for (pair, commit_ts) in crate::Snapshot::batch_get_with_commit_ts(self, keys).await? {
                let KvPair(key, value) = pair;
                out.insert(key, ValueEntry::new(Some(value), commit_ts));
            }
        } else {
            for pair in crate::Snapshot::batch_get(self, keys).await? {
                let KvPair(key, value) = pair;
                out.insert(key, ValueEntry::new(Some(value), 0));
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl Getter for crate::Transaction {
    async fn get(&mut self, key: Key, options: &[GetOption]) -> Result<ValueEntry> {
        let mut opts = GetOptions::default();
        opts.apply(options);
        if opts.return_commit_ts() {
            let value = crate::Transaction::get_with_commit_ts(self, key).await?;
            Ok(match value {
                Some((value, commit_ts)) => ValueEntry::new(Some(value), commit_ts),
                None => ValueEntry::new(None, 0),
            })
        } else {
            Ok(ValueEntry::new(
                crate::Transaction::get(self, key).await?,
                0,
            ))
        }
    }
}

#[async_trait]
impl BatchGetter for crate::Transaction {
    async fn batch_get(
        &mut self,
        keys: Vec<Key>,
        options: &[BatchGetOption],
    ) -> Result<HashMap<Key, ValueEntry>> {
        let capacity = keys.len();
        let mut opts = BatchGetOptions::default();
        opts.apply(options);

        let mut out = HashMap::with_capacity(capacity);
        if opts.return_commit_ts() {
            for (pair, commit_ts) in
                crate::Transaction::batch_get_with_commit_ts(self, keys).await?
            {
                let KvPair(key, value) = pair;
                out.insert(key, ValueEntry::new(Some(value), commit_ts));
            }
        } else {
            for pair in crate::Transaction::batch_get(self, keys).await? {
                let KvPair(key, value) = pair;
                out.insert(key, ValueEntry::new(Some(value), 0));
            }
        }
        Ok(out)
    }
}
