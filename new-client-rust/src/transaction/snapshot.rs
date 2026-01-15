// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use derive_new::new;
use log::{debug, trace};

use crate::interceptor::RpcContextInfo;
use crate::interceptor::RpcInterceptor;
use crate::BoundRange;
use crate::CommandPriority;
use crate::Key;
use crate::KvPair;
use crate::Result;
use crate::Transaction;
use crate::Value;

/// A read-only transaction which reads at the given timestamp.
///
/// It behaves as if the snapshot was taken at the given timestamp,
/// i.e. it can read operations happened before the timestamp,
/// but ignores operations after the timestamp.
///
/// See the [Transaction](struct@crate::Transaction) docs for more information on the methods.
#[derive(new)]
pub struct Snapshot {
    transaction: Transaction,
}

impl Snapshot {
    /// Set `kvrpcpb::Context.request_source` for all requests sent by this snapshot.
    pub fn set_request_source(&mut self, source: impl Into<String>) {
        self.transaction.set_request_source(source);
    }

    /// Set `kvrpcpb::Context.resource_group_tag` for all requests sent by this snapshot.
    pub fn set_resource_group_tag(&mut self, tag: impl Into<Vec<u8>>) {
        self.transaction.set_resource_group_tag(tag);
    }

    /// Set `kvrpcpb::Context.resource_control_context.resource_group_name` for all requests sent by this snapshot.
    pub fn set_resource_group_name(&mut self, name: impl Into<String>) {
        self.transaction.set_resource_group_name(name);
    }

    /// Set `kvrpcpb::Context.priority` for all requests sent by this snapshot.
    pub fn set_priority(&mut self, priority: CommandPriority) {
        self.transaction.set_priority(priority);
    }

    /// Set `kvrpcpb::Context.resource_control_context.override_priority` for all requests sent by this snapshot.
    pub fn set_resource_control_override_priority(&mut self, override_priority: u64) {
        self.transaction
            .set_resource_control_override_priority(override_priority);
    }

    /// Set `kvrpcpb::Context.resource_control_context.penalty` for all requests sent by this snapshot.
    pub fn set_resource_control_penalty(
        &mut self,
        penalty: impl Into<crate::resource_manager::Consumption>,
    ) {
        self.transaction.set_resource_control_penalty(penalty);
    }

    /// Set a resource group tagger for all requests sent by this snapshot.
    ///
    /// If a fixed resource group tag is set via [`set_resource_group_tag`](Self::set_resource_group_tag),
    /// it takes precedence over this tagger.
    pub fn set_resource_group_tagger(
        &mut self,
        tagger: impl Fn(&RpcContextInfo, &crate::kvrpcpb::Context) -> Vec<u8> + Send + Sync + 'static,
    ) {
        self.transaction.set_resource_group_tagger(tagger);
    }

    /// Replace the RPC interceptor chain for all requests sent by this snapshot.
    pub fn set_rpc_interceptor(&mut self, interceptor: Arc<dyn RpcInterceptor>) {
        self.transaction.set_rpc_interceptor(interceptor);
    }

    /// Add an RPC interceptor for all requests sent by this snapshot.
    ///
    /// If another interceptor with the same name exists, it is replaced.
    pub fn add_rpc_interceptor(&mut self, interceptor: Arc<dyn RpcInterceptor>) {
        self.transaction.add_rpc_interceptor(interceptor);
    }

    /// Get the value associated with the given key.
    pub async fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        trace!("invoking get request on snapshot");
        self.transaction.get(key).await
    }

    /// Check whether the key exists.
    pub async fn key_exists(&mut self, key: impl Into<Key>) -> Result<bool> {
        debug!("invoking key_exists request on snapshot");
        self.transaction.key_exists(key).await
    }

    /// Get the values associated with the given keys.
    pub async fn batch_get(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking batch_get request on snapshot");
        self.transaction.batch_get(keys).await
    }

    /// Scan a range, return at most `limit` key-value pairs that lying in the range.
    pub async fn scan(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking scan request on snapshot");
        self.transaction.scan(range, limit).await
    }

    /// Scan a range, return at most `limit` keys that lying in the range.
    pub async fn scan_keys(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking scan_keys request on snapshot");
        self.transaction.scan_keys(range, limit).await
    }

    /// Similar to scan, but in the reverse direction.
    pub async fn scan_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking scan_reverse request on snapshot");
        self.transaction.scan_reverse(range, limit).await
    }

    /// Similar to scan_keys, but in the reverse direction.
    pub async fn scan_keys_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking scan_keys_reverse request on snapshot");
        self.transaction.scan_keys_reverse(range, limit).await
    }
}
