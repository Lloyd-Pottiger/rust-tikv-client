// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use crate::proto::kvrpcpb;
use crate::BoundRange;
use crate::Error;
use crate::Key;
use crate::KvPair;
use crate::Result;
use crate::Value;

use super::transaction::Mutation;

type MemoryFootprintChangeHook = Arc<dyn Fn(u64) + Send + Sync>;

#[derive(Debug)]
struct BufferStaging {
    handle: u64,
    primary_key_at_start: Option<Key>,
    undo_entries: HashMap<Key, Option<BufferEntry>>,
}

/// A caching layer which buffers reads and writes in a transaction.
pub struct Buffer {
    primary_key: Option<Key>,
    entry_map: BTreeMap<Key, BufferEntry>,
    is_pessimistic: bool,
    mutation_count: usize,
    mutation_size: u64,
    memory_footprint_change_hook: Option<MemoryFootprintChangeHook>,
    staging: Vec<BufferStaging>,
    next_staging_handle: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BufferedLockKind {
    Exclusive,
    Shared,
}

impl BufferedLockKind {
    fn to_mutation_op(self) -> kvrpcpb::Op {
        match self {
            BufferedLockKind::Exclusive => kvrpcpb::Op::Lock,
            BufferedLockKind::Shared => kvrpcpb::Op::SharedLock,
        }
    }
}

impl Buffer {
    pub fn new(is_pessimistic: bool) -> Buffer {
        Buffer {
            primary_key: None,
            entry_map: BTreeMap::new(),
            is_pessimistic,
            mutation_count: 0,
            mutation_size: 0,
            memory_footprint_change_hook: None,
            staging: Vec::new(),
            next_staging_handle: 0,
        }
    }

    pub(crate) fn is_staging(&self) -> bool {
        !self.staging.is_empty()
    }

    fn record_undo_entry(&mut self, key: &Key) {
        let Some(staging) = self.staging.last_mut() else {
            return;
        };
        if staging.undo_entries.contains_key(key) {
            return;
        }
        let prev_entry = self.entry_map.get(key).cloned();
        staging.undo_entries.insert(key.clone(), prev_entry);
    }

    fn remove_entry(&mut self, key: &Key) {
        let Some(entry) = self.entry_map.remove(key) else {
            return;
        };

        let prev_mem = self.mutation_size;
        if !matches!(entry, BufferEntry::Cached(_)) {
            self.mutation_count = self.mutation_count.saturating_sub(1);
            let size = mutation_size_for_entry(key.len(), &entry);
            self.mutation_size = self.mutation_size.saturating_sub(size);
        }
        if prev_mem != self.mutation_size {
            self.report_memory_footprint();
        }
    }

    pub(crate) fn staging(&mut self) -> u64 {
        let handle = self.next_staging_handle;
        self.next_staging_handle = self.next_staging_handle.saturating_add(1);
        self.staging.push(BufferStaging {
            handle,
            primary_key_at_start: self.primary_key.clone(),
            undo_entries: HashMap::new(),
        });
        handle
    }

    pub(crate) fn release_staging(&mut self, handle: u64) -> Result<()> {
        let staging = self
            .staging
            .pop()
            .ok_or_else(|| Error::StringError("staging handle not found".to_owned()))?;
        if staging.handle != handle {
            self.staging.push(staging);
            return Err(Error::StringError(format!(
                "staging handle mismatch: expected {handle}, found {}",
                self.staging.last().map(|s| s.handle).unwrap_or_default()
            )));
        }

        if let Some(parent) = self.staging.last_mut() {
            for (key, entry) in staging.undo_entries {
                parent.undo_entries.entry(key).or_insert(entry);
            }
        }
        Ok(())
    }

    pub(crate) fn cleanup_staging(&mut self, handle: u64) -> Result<()> {
        let staging = self
            .staging
            .pop()
            .ok_or_else(|| Error::StringError("staging handle not found".to_owned()))?;
        if staging.handle != handle {
            self.staging.push(staging);
            return Err(Error::StringError(format!(
                "staging handle mismatch: expected {handle}, found {}",
                self.staging.last().map(|s| s.handle).unwrap_or_default()
            )));
        }

        for (key, entry) in staging.undo_entries {
            match entry {
                Some(entry) => self.insert_entry(key, entry),
                None => self.remove_entry(&key),
            }
        }
        self.primary_key = staging.primary_key_at_start;
        Ok(())
    }

    pub(crate) fn set_is_pessimistic(&mut self, is_pessimistic: bool) {
        self.is_pessimistic = is_pessimistic;
    }

    pub(crate) fn mutation_count(&self) -> usize {
        self.mutation_count
    }

    pub(crate) fn mutation_size(&self) -> u64 {
        self.mutation_size
    }

    pub(crate) fn set_memory_footprint_change_hook(&mut self, hook: MemoryFootprintChangeHook) {
        self.memory_footprint_change_hook = Some(hook);
    }

    pub(crate) fn clear_memory_footprint_change_hook(&mut self) {
        self.memory_footprint_change_hook = None;
    }

    pub(crate) fn mem_hook_set(&self) -> bool {
        self.memory_footprint_change_hook.is_some()
    }

    pub(crate) fn mem(&self) -> u64 {
        self.mutation_size
    }

    /// Get the primary key of the buffer.
    pub fn get_primary_key(&self) -> Option<Key> {
        self.primary_key.clone()
    }

    /// Set the primary key if it is not set
    pub fn primary_key_or(&mut self, key: &Key) {
        self.primary_key.get_or_insert_with(|| key.clone());
    }

    pub(crate) fn clear_primary_key(&mut self) {
        self.primary_key = None;
    }

    /// Get a value from the buffer.
    /// If the returned value is None, it means the key doesn't exist in buffer yet.
    pub fn get(&self, key: &Key) -> Option<Value> {
        match self.get_from_mutations(key) {
            MutationValue::Determined(value) => value,
            MutationValue::Undetermined => None,
        }
    }

    /// Get a value from the buffer. If the value is not present, run `f` to get
    /// the value.
    pub async fn get_or_else<F, Fut>(&mut self, key: Key, f: F) -> Result<Option<Value>>
    where
        F: FnOnce(Key) -> Fut,
        Fut: Future<Output = Result<Option<Value>>>,
    {
        match self.get_from_mutations(&key) {
            MutationValue::Determined(value) => Ok(value),
            MutationValue::Undetermined => {
                let value = f(key.clone()).await?;
                self.update_cache(key, value.clone());
                Ok(value)
            }
        }
    }

    /// Get multiple values from the buffer. If any are not present, run `f` to
    /// get the missing values.
    ///
    /// only used for snapshot read (i.e. not for `batch_get_for_update`)
    pub async fn batch_get_or_else<F, Fut>(
        &mut self,
        keys: impl Iterator<Item = Key>,
        f: F,
    ) -> Result<impl Iterator<Item = KvPair>>
    where
        F: FnOnce(Box<dyn Iterator<Item = Key> + Send>) -> Fut,
        Fut: Future<Output = Result<Vec<KvPair>>>,
    {
        let (cached_results, undetermined_keys) = {
            // Partition the keys into those we have buffered and those we have to
            // get from the store.
            let (undetermined_keys, cached_results): (Vec<_>, Vec<_>) = keys
                .map(|key| {
                    let value = self
                        .entry_map
                        .get(&key)
                        .map(BufferEntry::get_value)
                        .unwrap_or(MutationValue::Undetermined);
                    (key, value)
                })
                .partition(|(_, v)| *v == MutationValue::Undetermined);

            let cached_results = cached_results.into_iter().filter_map(|(k, v)| match v {
                MutationValue::Determined(Some(value)) => Some(KvPair(k, value)),
                MutationValue::Determined(None) | MutationValue::Undetermined => None,
            });

            let undetermined_keys = undetermined_keys.into_iter().map(|(k, _)| k);
            (cached_results, undetermined_keys)
        };

        let undetermined_keys: Vec<Key> = undetermined_keys.collect();
        let fetched_results = f(Box::new(undetermined_keys.clone().into_iter())).await?;

        let fetched_keys: HashSet<&Key> = fetched_results.iter().map(|pair| &pair.0).collect();
        for kvpair in &fetched_results {
            let key = kvpair.0.clone();
            let value = Some(kvpair.1.clone());
            self.update_cache(key, value);
        }
        for key in undetermined_keys {
            if !fetched_keys.contains(&key) {
                self.update_cache(key, None);
            }
        }

        let results = cached_results.chain(fetched_results.into_iter());
        Ok(results)
    }

    pub(crate) fn clean_cached_reads(&mut self, keys: impl IntoIterator<Item = Key>) {
        for key in keys {
            let Entry::Occupied(mut occupied) = self.entry_map.entry(key) else {
                continue;
            };

            match occupied.get_mut() {
                BufferEntry::Cached(_) => {
                    occupied.remove();
                }
                BufferEntry::Locked(_kind, cached_value) => {
                    // Keep the lock, but drop the cached read result.
                    if cached_value.is_some() {
                        *cached_value = None;
                    }
                }
                _ => {}
            }
        }
    }

    /// Run `f` to fetch entries in `range` from TiKV. Combine them with mutations in local buffer. Returns the results.
    pub async fn scan_and_fetch<F, Fut>(
        &mut self,
        range: BoundRange,
        limit: u32,
        update_cache: bool,
        reverse: bool,
        f: F,
    ) -> Result<impl Iterator<Item = KvPair>>
    where
        F: FnOnce(BoundRange, u32) -> Fut,
        Fut: Future<Output = Result<Vec<KvPair>>>,
    {
        // read from local buffer
        let mutation_range = self.entry_map.range(range.clone());

        // fetch from TiKV
        // fetch more entries because some of them may be deleted.
        let deleted_count = u32::try_from(
            mutation_range
                .clone()
                .filter(|(_, m)| matches!(m, BufferEntry::Del))
                .count(),
        )
        .unwrap_or(u32::MAX);
        let redundant_limit = limit.saturating_add(deleted_count);

        let mut results = f(range, redundant_limit)
            .await?
            .into_iter()
            .map(|pair| pair.into())
            .collect::<BTreeMap<Key, Value>>();

        // override using local data
        for (k, m) in mutation_range {
            match m {
                BufferEntry::Put(v) => {
                    results.insert(k.clone(), v.clone());
                }
                BufferEntry::Del => {
                    results.remove(k);
                }
                _ => {}
            }
        }

        // update local buffer
        if update_cache {
            for (k, v) in &results {
                self.update_cache(k.clone(), Some(v.clone()));
            }
        }

        let mut res = Vec::new();
        if reverse {
            for (k, v) in results.into_iter().rev().take(limit as usize) {
                res.push(KvPair::new(k, v));
            }
        } else {
            for (k, v) in results.into_iter().take(limit as usize) {
                res.push(KvPair::new(k, v));
            }
        }

        Ok(res.into_iter())
    }

    /// Lock the given key if necessary.
    pub fn lock(&mut self, key: Key) {
        self.record_undo_entry(&key);
        self.primary_key.get_or_insert_with(|| key.clone());
        let key_len = u64::try_from(key.len()).unwrap_or(u64::MAX);
        let prev_mem = self.mutation_size;

        let mut should_report = false;
        match self.entry_map.entry(key) {
            Entry::Vacant(vacant) => {
                vacant.insert(BufferEntry::Locked(BufferedLockKind::Exclusive, None));
                self.mutation_count = self.mutation_count.saturating_add(1);
                self.mutation_size = self.mutation_size.saturating_add(key_len);
                should_report = prev_mem != self.mutation_size;
            }
            Entry::Occupied(mut occupied) => {
                if matches!(occupied.get(), BufferEntry::Cached(_)) {
                    let entry = occupied.get_mut();
                    let BufferEntry::Cached(cached_value) = entry else {
                        return;
                    };
                    let cached_value = cached_value.take();
                    *entry = BufferEntry::Locked(BufferedLockKind::Exclusive, Some(cached_value));
                    self.mutation_count = self.mutation_count.saturating_add(1);
                    self.mutation_size = self.mutation_size.saturating_add(key_len);
                    should_report = prev_mem != self.mutation_size;
                }
            }
        }

        if should_report {
            self.report_memory_footprint();
        }
    }

    pub(crate) fn lock_shared(&mut self, key: Key) {
        self.record_undo_entry(&key);
        let key_len = u64::try_from(key.len()).unwrap_or(u64::MAX);
        let prev_mem = self.mutation_size;

        let mut should_report = false;
        match self.entry_map.entry(key) {
            Entry::Vacant(vacant) => {
                vacant.insert(BufferEntry::Locked(BufferedLockKind::Shared, None));
                self.mutation_count = self.mutation_count.saturating_add(1);
                self.mutation_size = self.mutation_size.saturating_add(key_len);
                should_report = prev_mem != self.mutation_size;
            }
            Entry::Occupied(mut occupied) => match occupied.get() {
                BufferEntry::Cached(_) => {
                    let entry = occupied.get_mut();
                    let BufferEntry::Cached(cached_value) = entry else {
                        return;
                    };
                    let cached_value = cached_value.take();
                    *entry = BufferEntry::Locked(BufferedLockKind::Shared, Some(cached_value));
                    self.mutation_count = self.mutation_count.saturating_add(1);
                    self.mutation_size = self.mutation_size.saturating_add(key_len);
                    should_report = prev_mem != self.mutation_size;
                }
                BufferEntry::Locked(BufferedLockKind::Shared, _) => {}
                // Keep the stronger lock mode.
                BufferEntry::Locked(BufferedLockKind::Exclusive, _) => {}
                _ => {}
            },
        }

        if should_report {
            self.report_memory_footprint();
        }
    }

    pub(crate) fn is_locked_in_share_mode(&self, key: &Key) -> bool {
        matches!(
            self.entry_map.get(key),
            Some(BufferEntry::Locked(BufferedLockKind::Shared, _))
        )
    }

    pub(crate) fn has_mutation_or_lock(&self, key: &Key) -> bool {
        match self.entry_map.get(key) {
            Some(BufferEntry::Cached(_)) | None => false,
            Some(_) => true,
        }
    }

    /// Unlock the given key if locked.
    pub fn unlock(&mut self, key: &Key) {
        let was_locked = matches!(self.entry_map.get(key), Some(BufferEntry::Locked(_, _)));
        if !was_locked {
            return;
        }

        self.record_undo_entry(key);

        let prev_mem = self.mutation_size;
        let key_len = u64::try_from(key.len()).unwrap_or(u64::MAX);

        let old_entry = self.entry_map.remove(key);
        if let Some(BufferEntry::Locked(_kind, cached_value)) = old_entry {
            if let Some(cached_value) = cached_value {
                self.entry_map
                    .insert(key.clone(), BufferEntry::Cached(cached_value));
            }
            self.mutation_count = self.mutation_count.saturating_sub(1);
            self.mutation_size = self.mutation_size.saturating_sub(key_len);
            if prev_mem != self.mutation_size {
                self.report_memory_footprint();
            }
        } else if let Some(entry) = old_entry {
            self.entry_map.insert(key.clone(), entry);
        }
    }

    /// Put a value into the buffer (does not write through).
    pub fn put(&mut self, key: Key, value: Value) {
        self.record_undo_entry(&key);
        if matches!(
            self.entry_map.get(&key),
            Some(BufferEntry::Insert(_)) | Some(BufferEntry::CheckNotExist)
        ) {
            self.insert_entry(key, BufferEntry::Insert(value));
            return;
        }
        self.insert_entry(key, BufferEntry::Put(value));
    }

    /// Mark a value as Insert mutation into the buffer (does not write through).
    pub fn insert(&mut self, key: Key, value: Value) {
        self.record_undo_entry(&key);
        if matches!(self.entry_map.get(&key), Some(BufferEntry::Del)) {
            self.insert_entry(key, BufferEntry::Put(value));
            return;
        }
        self.insert_entry(key, BufferEntry::Insert(value));
    }

    /// Mark a value as deleted.
    pub fn delete(&mut self, key: Key) {
        self.record_undo_entry(&key);
        if !self.is_pessimistic
            && matches!(
                self.entry_map.get(&key),
                Some(BufferEntry::Insert(_)) | Some(BufferEntry::CheckNotExist)
            )
        {
            self.insert_entry(key, BufferEntry::CheckNotExist);
            return;
        }
        self.insert_entry(key, BufferEntry::Del);
    }

    pub(crate) fn mutate(&mut self, m: Mutation) {
        match m {
            Mutation::Put(key, value) => self.put(key, value),
            Mutation::Delete(key) => self.delete(key),
        };
    }

    /// Converts the buffered mutations to the proto buffer version
    pub fn to_proto_mutations(&self) -> Vec<kvrpcpb::Mutation> {
        self.entry_map
            .iter()
            .filter_map(|(key, mutation)| mutation.to_proto_with_key(key))
            .collect()
    }

    /// Take all non-cached mutations from the buffer and return them as proto mutations.
    ///
    /// This is used by pipelined transactions that flush buffered mutations during execution.
    pub(crate) fn take_mutations(&mut self) -> Vec<kvrpcpb::Mutation> {
        debug_assert!(
            self.staging.is_empty(),
            "pipelined take_mutations is not compatible with staging"
        );
        let prev_mem = self.mutation_size;
        let mut cached_entries = BTreeMap::new();
        let mut mutations = Vec::new();

        for (key, entry) in std::mem::take(&mut self.entry_map) {
            match entry {
                BufferEntry::Cached(value) => {
                    cached_entries.insert(key, BufferEntry::Cached(value));
                }
                entry => {
                    if let Some(mutation) = entry.into_proto_with_key(key) {
                        mutations.push(mutation);
                    }
                }
            }
        }

        self.entry_map = cached_entries;
        self.mutation_count = 0;
        self.mutation_size = 0;
        if prev_mem != self.mutation_size {
            self.report_memory_footprint();
        }
        mutations
    }

    pub fn get_write_size(&self) -> usize {
        self.entry_map
            .iter()
            .map(|(k, v)| match v {
                BufferEntry::Put(val) | BufferEntry::Insert(val) => val.len() + k.len(),
                BufferEntry::Del => k.len(),
                _ => 0,
            })
            .sum()
    }

    fn get_from_mutations(&self, key: &Key) -> MutationValue {
        self.entry_map
            .get(key)
            .map(BufferEntry::get_value)
            .unwrap_or(MutationValue::Undetermined)
    }

    fn update_cache(&mut self, key: Key, value: Option<Value>) {
        match self.entry_map.get(&key) {
            Some(BufferEntry::Locked(kind, None)) => {
                self.entry_map
                    .insert(key, BufferEntry::Locked(*kind, Some(value)));
            }
            None => {
                self.entry_map.insert(key, BufferEntry::Cached(value));
            }
            Some(BufferEntry::Cached(v)) | Some(BufferEntry::Locked(_, Some(v))) => {
                assert!(&value == v);
            }
            Some(BufferEntry::Put(v)) => assert!(value.as_ref() == Some(v)),
            Some(BufferEntry::Del) => {
                assert!(value.is_none());
            }
            Some(BufferEntry::Insert(v)) => assert!(value.as_ref() == Some(v)),
            Some(BufferEntry::CheckNotExist) => {
                assert!(value.is_none());
            }
        }
    }

    fn insert_entry(&mut self, key: impl Into<Key>, entry: BufferEntry) {
        let key = key.into();
        if !matches!(
            entry,
            BufferEntry::Cached(_)
                | BufferEntry::CheckNotExist
                | BufferEntry::Locked(BufferedLockKind::Shared, _)
        ) {
            self.primary_key.get_or_insert_with(|| key.clone());
        }
        let key_len = key.len();
        let prev_mem = self.mutation_size;
        let new_is_mutation = !matches!(entry, BufferEntry::Cached(_));
        let new_size = mutation_size_for_entry(key_len, &entry);

        if let Some(old_entry) = self.entry_map.insert(key, entry) {
            if !matches!(old_entry, BufferEntry::Cached(_)) {
                self.mutation_count = self.mutation_count.saturating_sub(1);
                let old_size = mutation_size_for_entry(key_len, &old_entry);
                self.mutation_size = self.mutation_size.saturating_sub(old_size);
            }
        }

        if new_is_mutation {
            self.mutation_count = self.mutation_count.saturating_add(1);
            self.mutation_size = self.mutation_size.saturating_add(new_size);
        }

        if prev_mem != self.mutation_size {
            self.report_memory_footprint();
        }
    }

    fn report_memory_footprint(&self) {
        let Some(hook) = self.memory_footprint_change_hook.as_ref() else {
            return;
        };
        hook(self.mutation_size);
    }
}

fn mutation_size_for_entry(key_len: usize, entry: &BufferEntry) -> u64 {
    match entry {
        BufferEntry::Cached(_) => 0,
        BufferEntry::Put(value) | BufferEntry::Insert(value) => {
            u64::try_from(key_len.saturating_add(value.len())).unwrap_or(u64::MAX)
        }
        BufferEntry::Del | BufferEntry::Locked(_, _) | BufferEntry::CheckNotExist => {
            u64::try_from(key_len).unwrap_or(u64::MAX)
        }
    }
}

// The state of a key-value pair in the buffer.
// It includes two kinds of state:
//
// Mutations:
//   - `Put`
//   - `Del`
//   - `Insert`
//   - `CheckNotExist`, a constraint to ensure the key doesn't exist. See https://github.com/pingcap/tidb/pull/14968.
// Cache of read requests:
//   - `Cached`, generated by normal read requests
//   - `ReadLockCached`, generated by lock commands (`lock_keys`, `get_for_update`) and optionally read requests
//
#[derive(Debug, Clone)]
enum BufferEntry {
    // The value has been read from the server. None means there is no entry.
    // Also means the entry isn't locked.
    Cached(Option<Value>),
    // Key is locked.
    //
    // Cached value:
    //   - Outer Option: Whether there is cached value
    //   - Inner Option: Whether the value is empty
    //   - Note: The cache is not what the lock request reads, but what normal read (`get`) requests read.
    //
    // In optimistic transaction:
    //   The key is locked by `lock_keys`.
    //   It means letting the server check for conflicts when committing
    //
    // In pessimistic transaction:
    //   The key is locked by `get_for_update` or `batch_get_for_update`
    Locked(BufferedLockKind, Option<Option<Value>>),
    // Value has been written.
    Put(Value),
    // Value has been deleted.
    Del,
    // Key should be check not exists before.
    Insert(Value),
    // Key should be check not exists before.
    CheckNotExist,
}

impl BufferEntry {
    fn to_proto_with_key(&self, key: &Key) -> Option<kvrpcpb::Mutation> {
        let mut pb = kvrpcpb::Mutation::default();
        match self {
            BufferEntry::Cached(_) => return None,
            BufferEntry::Put(v) => {
                pb.op = kvrpcpb::Op::Put.into();
                pb.value.clone_from(v);
            }
            BufferEntry::Del => pb.op = kvrpcpb::Op::Del.into(),
            BufferEntry::Locked(kind, _) => pb.op = kind.to_mutation_op().into(),
            BufferEntry::Insert(v) => {
                pb.op = kvrpcpb::Op::Insert.into();
                pb.value.clone_from(v);
            }
            BufferEntry::CheckNotExist => pb.op = kvrpcpb::Op::CheckNotExists.into(),
        };
        pb.key = key.clone().into();
        Some(pb)
    }

    fn into_proto_with_key(self, key: Key) -> Option<kvrpcpb::Mutation> {
        let mut pb = kvrpcpb::Mutation::default();
        match self {
            BufferEntry::Cached(_) => return None,
            BufferEntry::Put(value) => {
                pb.op = kvrpcpb::Op::Put.into();
                pb.value = value;
            }
            BufferEntry::Del => pb.op = kvrpcpb::Op::Del.into(),
            BufferEntry::Locked(kind, _) => pb.op = kind.to_mutation_op().into(),
            BufferEntry::Insert(value) => {
                pb.op = kvrpcpb::Op::Insert.into();
                pb.value = value;
            }
            BufferEntry::CheckNotExist => pb.op = kvrpcpb::Op::CheckNotExists.into(),
        }
        pb.key = key.into();
        Some(pb)
    }

    fn get_value(&self) -> MutationValue {
        match self {
            BufferEntry::Cached(value) => MutationValue::Determined(value.clone()),
            BufferEntry::Put(value) => MutationValue::Determined(Some(value.clone())),
            BufferEntry::Del => MutationValue::Determined(None),
            BufferEntry::Locked(_kind, None) => MutationValue::Undetermined,
            BufferEntry::Locked(_kind, Some(value)) => MutationValue::Determined(value.clone()),
            BufferEntry::Insert(value) => MutationValue::Determined(Some(value.clone())),
            BufferEntry::CheckNotExist => MutationValue::Determined(None),
        }
    }
}

// The state of a value as known by the buffer.
#[derive(Eq, PartialEq, Debug)]
enum MutationValue {
    // The buffer can determine the value.
    Determined(Option<Value>),
    // The buffer cannot determine the value.
    Undetermined,
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::future::ready;

    use super::*;
    use crate::internal_err;

    #[test]
    fn set_and_get_from_buffer() {
        let mut buffer = Buffer::new(false);
        buffer.put(b"key1".to_vec().into(), b"value1".to_vec());
        buffer.put(b"key2".to_vec().into(), b"value2".to_vec());
        assert_eq!(
            block_on(
                buffer.get_or_else(b"key1".to_vec().into(), move |_| ready(Err(internal_err!(
                    ""
                ))))
            )
            .unwrap()
            .unwrap(),
            b"value1".to_vec()
        );

        buffer.delete(b"key2".to_vec().into());
        buffer.put(b"key1".to_vec().into(), b"value".to_vec());
        assert_eq!(
            block_on(buffer.batch_get_or_else(
                vec![b"key2".to_vec().into(), b"key1".to_vec().into()].into_iter(),
                move |_| ready(Ok(vec![])),
            ))
            .unwrap()
            .collect::<Vec<_>>(),
            vec![KvPair(Key::from(b"key1".to_vec()), b"value".to_vec(),),]
        );
    }

    #[test]
    fn insert_and_get_from_buffer() {
        let mut buffer = Buffer::new(false);
        buffer.insert(b"key1".to_vec().into(), b"value1".to_vec());
        buffer.insert(b"key2".to_vec().into(), b"value2".to_vec());
        assert_eq!(
            block_on(
                buffer.get_or_else(b"key1".to_vec().into(), move |_| ready(Err(internal_err!(
                    ""
                ))))
            )
            .unwrap()
            .unwrap(),
            b"value1".to_vec()
        );

        buffer.delete(b"key2".to_vec().into());
        buffer.insert(b"key1".to_vec().into(), b"value".to_vec());
        assert_eq!(
            block_on(buffer.batch_get_or_else(
                vec![b"key2".to_vec().into(), b"key1".to_vec().into()].into_iter(),
                move |_| ready(Ok(vec![])),
            ))
            .unwrap()
            .collect::<Vec<_>>(),
            vec![KvPair(Key::from(b"key1".to_vec()), b"value".to_vec()),]
        );
    }

    #[test]
    fn staging_cleanup_rolls_back_changes() {
        let k1: Key = b"key1".to_vec().into();
        let k2: Key = b"key2".to_vec().into();

        let mut buffer = Buffer::new(false);
        buffer.put(k1.clone(), b"value1".to_vec());
        let handle = buffer.staging();

        buffer.put(k1.clone(), b"value2".to_vec());
        buffer.put(k2.clone(), b"value3".to_vec());
        buffer.delete(k1.clone());

        buffer.cleanup_staging(handle).unwrap();

        assert!(matches!(
            buffer.entry_map.get(&k1),
            Some(BufferEntry::Put(v)) if v == b"value1"
        ));
        assert!(!buffer.entry_map.contains_key(&k2));
        assert_eq!(buffer.get_primary_key(), Some(k1));
    }

    #[test]
    fn staging_cleanup_restores_nested_state() {
        let k1: Key = b"key1".to_vec().into();

        let mut buffer = Buffer::new(false);
        buffer.put(k1.clone(), b"value1".to_vec());
        let outer = buffer.staging();
        buffer.put(k1.clone(), b"value2".to_vec());

        let inner = buffer.staging();
        buffer.put(k1.clone(), b"value3".to_vec());
        buffer.cleanup_staging(inner).unwrap();

        assert!(matches!(
            buffer.entry_map.get(&k1),
            Some(BufferEntry::Put(v)) if v == b"value2"
        ));

        buffer.cleanup_staging(outer).unwrap();
        assert!(matches!(
            buffer.entry_map.get(&k1),
            Some(BufferEntry::Put(v)) if v == b"value1"
        ));
    }

    #[test]
    fn staging_release_merges_undo_into_parent() {
        let k1: Key = b"key1".to_vec().into();
        let k2: Key = b"key2".to_vec().into();

        let mut buffer = Buffer::new(false);
        buffer.put(k1.clone(), b"value1".to_vec());
        let outer = buffer.staging();
        buffer.put(k2.clone(), b"value2".to_vec());

        let inner = buffer.staging();
        buffer.put(k1.clone(), b"value3".to_vec());
        buffer.release_staging(inner).unwrap();

        buffer.cleanup_staging(outer).unwrap();
        assert!(matches!(
            buffer.entry_map.get(&k1),
            Some(BufferEntry::Put(v)) if v == b"value1"
        ));
        assert!(!buffer.entry_map.contains_key(&k2));
    }

    #[test]
    fn staging_handle_mismatch_errors_and_preserves_stack() {
        let mut buffer = Buffer::new(false);
        let outer = buffer.staging();
        let inner = buffer.staging();

        let err = buffer.cleanup_staging(outer).unwrap_err();
        assert!(matches!(err, Error::StringError(msg) if msg.contains("mismatch")));
        assert_eq!(buffer.staging.len(), 2);

        buffer.cleanup_staging(inner).unwrap();
        buffer.cleanup_staging(outer).unwrap();
        assert!(buffer.staging.is_empty());
    }

    #[test]
    fn repeat_reads_are_cached() {
        let k1: Key = b"key1".to_vec().into();
        let k1_ = k1.clone();
        let k2: Key = b"key2".to_vec().into();
        let k2_ = k2.clone();
        let v1: Value = b"value1".to_vec();
        let v1_ = v1.clone();
        let v1__ = v1.clone();
        let v2: Value = b"value2".to_vec();
        let v2_ = v2.clone();

        let mut buffer = Buffer::new(false);
        let r1 = block_on(buffer.get_or_else(k1.clone(), move |_| ready(Ok(Some(v1_)))));
        let r2 = block_on(buffer.get_or_else(k1.clone(), move |_| ready(Err(internal_err!("")))));
        assert_eq!(r1.unwrap().unwrap(), v1);
        assert_eq!(r2.unwrap().unwrap(), v1);

        let mut buffer = Buffer::new(false);
        let r1 = block_on(
            buffer.batch_get_or_else(vec![k1.clone(), k2.clone()].into_iter(), move |_| {
                ready(Ok(vec![(k1_, v1__).into(), (k2_, v2_).into()]))
            }),
        );
        let r2 = block_on(buffer.get_or_else(k2.clone(), move |_| ready(Err(internal_err!("")))));
        let r3 = block_on(
            buffer.batch_get_or_else(vec![k1.clone(), k2.clone()].into_iter(), move |_| {
                ready(Ok(vec![]))
            }),
        );
        assert_eq!(
            r1.unwrap().collect::<Vec<_>>(),
            vec![
                KvPair(k1.clone(), v1.clone()),
                KvPair(k2.clone(), v2.clone())
            ]
        );
        assert_eq!(r2.unwrap().unwrap(), v2);
        assert_eq!(
            r3.unwrap().collect::<Vec<_>>(),
            vec![KvPair(k1, v1), KvPair(k2, v2)]
        );
    }

    #[test]
    fn scan_and_fetch_redundant_limit_does_not_overflow() {
        let mut buffer = Buffer::new(false);
        buffer.delete(b"key1".to_vec().into());

        let range: BoundRange = (..).into();
        let res =
            block_on(
                buffer.scan_and_fetch(range, u32::MAX, false, false, |_, redundant_limit| {
                    assert_eq!(redundant_limit, u32::MAX);
                    ready(Ok(Vec::<KvPair>::new()))
                }),
            )
            .unwrap()
            .collect::<Vec<_>>();

        assert!(res.is_empty());
    }

    #[test]
    fn scan_and_fetch_returns_ordered_results() {
        let mut buffer = Buffer::new(false);
        buffer.delete(vec![1].into());
        buffer.put(vec![2].into(), b"local-2".to_vec());
        buffer.put(vec![4].into(), b"local-4".to_vec());

        let forward_remote = vec![
            KvPair::new(vec![3], b"remote-3".to_vec()),
            KvPair::new(vec![1], b"remote-1".to_vec()),
            KvPair::new(vec![2], b"remote-2".to_vec()),
        ];
        let forward = block_on(
            buffer.scan_and_fetch((..).into(), 10, true, false, move |_, _| {
                ready(Ok(forward_remote))
            }),
        )
        .unwrap()
        .collect::<Vec<_>>();
        assert_eq!(
            forward,
            vec![
                KvPair::new(vec![2], b"local-2".to_vec()),
                KvPair::new(vec![3], b"remote-3".to_vec()),
                KvPair::new(vec![4], b"local-4".to_vec()),
            ]
        );

        let reverse_remote = vec![
            KvPair::new(vec![1], b"remote-1".to_vec()),
            KvPair::new(vec![2], b"remote-2".to_vec()),
            KvPair::new(vec![3], b"remote-3".to_vec()),
        ];
        let reverse = block_on(
            buffer.scan_and_fetch((..).into(), 2, true, true, move |_, _| {
                ready(Ok(reverse_remote))
            }),
        )
        .unwrap()
        .collect::<Vec<_>>();
        assert_eq!(
            reverse,
            vec![
                KvPair::new(vec![4], b"local-4".to_vec()),
                KvPair::new(vec![3], b"remote-3".to_vec()),
            ]
        );
    }

    // Check that multiple writes to the same key combine in the correct way.
    #[test]
    fn state_machine() {
        let mut buffer = Buffer::new(false);

        macro_rules! assert_entry {
            ($key: ident, $p: pat) => {
                assert!(matches!(buffer.entry_map.get(&$key), Some(&$p),))
            };
        }

        macro_rules! assert_entry_none {
            ($key: ident) => {
                assert!(buffer.entry_map.get(&$key).is_none())
            };
        }

        // Insert + Delete = CheckNotExists
        let key: Key = b"key1".to_vec().into();
        buffer.insert(key.clone(), b"value1".to_vec());
        buffer.delete(key.clone());
        assert_entry!(key, BufferEntry::CheckNotExist);

        // CheckNotExists + Delete = CheckNotExists
        buffer.delete(key.clone());
        assert_entry!(key, BufferEntry::CheckNotExist);

        // CheckNotExists + Put = Insert
        buffer.put(key.clone(), b"value2".to_vec());
        assert_entry!(key, BufferEntry::Insert(_));

        // Insert + Put = Insert
        let key: Key = b"key2".to_vec().into();
        buffer.insert(key.clone(), b"value1".to_vec());
        buffer.put(key.clone(), b"value2".to_vec());
        assert_entry!(key, BufferEntry::Insert(_));

        // Delete + Insert = Put
        let key: Key = b"key3".to_vec().into();
        buffer.delete(key.clone());
        buffer.insert(key.clone(), b"value1".to_vec());
        assert_entry!(key, BufferEntry::Put(_));

        // Lock + Unlock = None
        let key: Key = b"key4".to_vec().into();
        buffer.lock(key.clone());
        buffer.unlock(&key);
        assert_entry_none!(key);

        // Cached + Lock + Unlock = Cached
        let key: Key = b"key5".to_vec().into();
        let val: Value = b"value5".to_vec();
        let val_ = val.clone();
        let r = block_on(buffer.get_or_else(key.clone(), move |_| ready(Ok(Some(val_)))));
        assert_eq!(r.unwrap().unwrap(), val);
        buffer.lock(key.clone());
        buffer.unlock(&key);
        assert_entry!(key, BufferEntry::Cached(Some(_)));
        assert_eq!(
            block_on(buffer.get_or_else(key, move |_| ready(Err(internal_err!("")))))
                .unwrap()
                .unwrap(),
            val
        );
    }
}
