// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use derive_new::new;
use log::{debug, trace};

use crate::interceptor::RpcContextInfo;
use crate::interceptor::RpcInterceptor;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::BatchGetOption;
use crate::BoundRange;
use crate::CommandPriority;
use crate::GetOption;
use crate::Key;
use crate::KvPair;
use crate::Result;
use crate::Transaction;
use crate::Value;
use crate::ValueEntry;

/// A read-only transaction which reads at the given timestamp.
///
/// It behaves as if the snapshot was taken at the given timestamp,
/// i.e. it can read operations happened before the timestamp,
/// but ignores operations after the timestamp.
///
/// See the [Transaction](struct@crate::Transaction) docs for more information on the methods.
#[derive(new)]
pub struct Snapshot<PdC: PdClient = PdRpcClient> {
    transaction: Transaction<PdC>,
}

impl<PdC: PdClient> Snapshot<PdC> {
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

    /// Get the value associated with the given key with read options.
    pub async fn get_with_options(
        &mut self,
        key: impl Into<Key>,
        options: &[GetOption],
    ) -> Result<Option<ValueEntry>> {
        trace!("invoking get_with_options request on snapshot");
        self.transaction.get_with_options(key, options).await
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

    /// Get the values associated with the given keys with read options.
    pub async fn batch_get_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        options: &[BatchGetOption],
    ) -> Result<HashMap<Key, ValueEntry>> {
        debug!("invoking batch_get_with_options request on snapshot");
        self.transaction.batch_get_with_options(keys, options).await
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

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::interceptor::rpc_interceptor;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb;
    use crate::request::Keyspace;
    use crate::transaction::CheckLevel;
    use crate::TransactionOptions;

    #[tokio::test]
    async fn snapshot_context_setters_and_read_methods_are_reachable() -> Result<()> {
        let stage = Arc::new(AtomicUsize::new(0));

        let interceptor1_calls = Arc::new(AtomicUsize::new(0));
        let interceptor2_calls = Arc::new(AtomicUsize::new(0));

        let stage_for_hook = stage.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let expected_tag = if stage_for_hook.load(Ordering::SeqCst) == 0 {
                    vec![9_u8]
                } else {
                    vec![1_u8, 2, 3]
                };

                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let ctx = req.context.as_ref().expect("context should be set");
                    assert!(
                        ctx.request_source.ends_with("snap-src"),
                        "unexpected request_source: {}",
                        ctx.request_source
                    );
                    assert_eq!(ctx.resource_group_tag, expected_tag);
                    assert_eq!(ctx.priority, i32::from(CommandPriority::High));

                    let resource_ctl_ctx = ctx
                        .resource_control_context
                        .as_ref()
                        .expect("resource_control_context should be set");
                    assert_eq!(resource_ctl_ctx.resource_group_name, "snap-rg");
                    assert_eq!(resource_ctl_ctx.override_priority, 16);
                    assert!(resource_ctl_ctx.penalty.is_some());

                    return Ok(Box::new(kvrpcpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    }));
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    let ctx = req.context.as_ref().expect("context should be set");
                    assert!(
                        ctx.request_source.ends_with("snap-src"),
                        "unexpected request_source: {}",
                        ctx.request_source
                    );
                    assert_eq!(ctx.resource_group_tag, expected_tag);
                    return Ok(Box::new(kvrpcpb::BatchGetResponse::default()));
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::ScanRequest>() {
                    let ctx = req.context.as_ref().expect("context should be set");
                    assert!(
                        ctx.request_source.ends_with("snap-src"),
                        "unexpected request_source: {}",
                        ctx.request_source
                    );
                    assert_eq!(ctx.resource_group_tag, expected_tag);
                    return Ok(Box::new(kvrpcpb::ScanResponse::default()));
                }

                unreachable!("unexpected request type");
            },
        )));

        let transaction = Transaction::new(
            crate::Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
            crate::RequestContext::default(),
            None,
        );
        let mut snapshot = Snapshot::new(transaction);
        snapshot.set_request_source("snap-src");
        snapshot.set_resource_group_name("snap-rg");
        snapshot.set_priority(CommandPriority::High);
        snapshot.set_resource_control_override_priority(16);
        snapshot.set_resource_control_penalty(crate::resource_manager::Consumption::default());

        snapshot.set_resource_group_tagger(|_, _| vec![9_u8]);

        let interceptor1_calls_cloned = interceptor1_calls.clone();
        snapshot.set_rpc_interceptor(rpc_interceptor(
            "snapshot.test.interceptor1",
            move |_, _| {
                interceptor1_calls_cloned.fetch_add(1, Ordering::SeqCst);
            },
        ));
        let interceptor2_calls_cloned = interceptor2_calls.clone();
        snapshot.add_rpc_interceptor(rpc_interceptor(
            "snapshot.test.interceptor2",
            move |_, _| {
                interceptor2_calls_cloned.fetch_add(1, Ordering::SeqCst);
            },
        ));

        assert!(snapshot.get("k".to_owned()).await?.is_none());
        stage.store(1, Ordering::SeqCst);

        snapshot.set_resource_group_tag(vec![1_u8, 2, 3]);
        assert!(!snapshot.key_exists("k".to_owned()).await?);

        let got: Vec<_> = snapshot
            .batch_get(vec!["a".to_owned(), "b".to_owned()])
            .await?
            .collect();
        assert!(got.is_empty());

        let scan: Vec<_> = snapshot
            .scan("a".to_owned().."b".to_owned(), 10)
            .await?
            .collect();
        assert!(scan.is_empty());

        let scan_keys: Vec<_> = snapshot
            .scan_keys("a".to_owned().."b".to_owned(), 10)
            .await?
            .collect();
        assert!(scan_keys.is_empty());

        let scan_rev: Vec<_> = snapshot
            .scan_reverse("a".to_owned().."b".to_owned(), 10)
            .await?
            .collect();
        assert!(scan_rev.is_empty());

        let scan_keys_rev: Vec<_> = snapshot
            .scan_keys_reverse("a".to_owned().."b".to_owned(), 10)
            .await?
            .collect();
        assert!(scan_keys_rev.is_empty());

        assert!(interceptor1_calls.load(Ordering::SeqCst) > 0);
        assert!(interceptor2_calls.load(Ordering::SeqCst) > 0);
        Ok(())
    }
}
