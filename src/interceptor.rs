//! RPC context interception hooks.
//!
//! This module provides a lightweight mechanism to mutate `kvrpcpb::Context` right before a TiKV
//! RPC is sent.
//!
//! It is primarily used to support client-go v2 "control plane" features:
//! - resource group tags (aka resource control tags)
//! - resource control context fields (`penalty`, `override_priority`)
//! - request classification hooks (e.g. priority overrides)
//!
//! Notes:
//! - Interceptors are executed in the order they are linked.
//! - The interceptor chain is applied on the request execution path, after region/store has been
//!   selected and the request `Context` has been populated with region metadata.

use std::sync::Arc;

use crate::kvrpcpb;

/// The resolved target replica kind for a single RPC attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaKind {
    Leader,
    Follower,
    Learner,
}

/// Metadata for a single outgoing TiKV RPC attempt.
#[derive(Clone, Debug, Default)]
pub struct RpcContextInfo {
    /// The request label (stable within this crate).
    pub label: &'static str,
    /// 0 for the first attempt, increasing for retries.
    pub attempt: usize,
    /// The target region id, if this is a region-scoped request.
    pub region_id: Option<u64>,
    /// The target store id, if this is a region-scoped request.
    pub store_id: Option<u64>,
    /// The chosen replica kind (leader/follower/learner), if applicable.
    pub replica_kind: Option<ReplicaKind>,
    /// Whether this RPC uses `Context.replica_read`.
    pub replica_read: bool,
    /// Whether this RPC uses `Context.stale_read`.
    pub stale_read: bool,
}

impl RpcContextInfo {
    #[must_use]
    pub fn is_retry(&self) -> bool {
        self.attempt > 0
    }
}

/// An RPC interceptor which can mutate the outgoing `kvrpcpb::Context`.
pub trait RpcInterceptor: Send + Sync + 'static {
    /// A stable name used for de-duplication in [`RpcInterceptorChain`].
    fn name(&self) -> &str;

    /// Called right before sending a request to a TiKV store.
    ///
    /// Implementations may mutate `ctx` (e.g. set resource control fields).
    fn before_send(&self, info: &RpcContextInfo, ctx: &mut kvrpcpb::Context);

    /// Wrap another interceptor function (onion model).
    ///
    /// The default implementation runs [`before_send`](Self::before_send) and then calls `next`.
    fn wrap(self: Arc<Self>, next: RpcInterceptorFunc) -> RpcInterceptorFunc {
        let it = self;
        Arc::new(move |info, ctx| {
            it.before_send(info, ctx);
            next(info, ctx);
        })
    }
}

/// A composable interceptor function.
pub type RpcInterceptorFunc =
    Arc<dyn Fn(&RpcContextInfo, &mut kvrpcpb::Context) + Send + Sync + 'static>;

type BeforeSendFn = dyn Fn(&RpcContextInfo, &mut kvrpcpb::Context) + Send + Sync + 'static;

struct FnRpcInterceptor {
    name: Arc<str>,
    f: Box<BeforeSendFn>,
}

impl RpcInterceptor for FnRpcInterceptor {
    fn name(&self) -> &str {
        &self.name
    }

    fn before_send(&self, info: &RpcContextInfo, ctx: &mut kvrpcpb::Context) {
        (self.f)(info, ctx);
    }
}

/// Create an interceptor from a function.
#[must_use]
pub fn rpc_interceptor(
    name: &'static str,
    f: impl Fn(&RpcContextInfo, &mut kvrpcpb::Context) + Send + Sync + 'static,
) -> Arc<dyn RpcInterceptor> {
    Arc::new(FnRpcInterceptor {
        name: Arc::<str>::from(name),
        f: Box::new(f),
    })
}

/// A chain of RPC interceptors (deduplicated by name).
#[derive(Clone, Default)]
pub struct RpcInterceptorChain {
    chain: Vec<Arc<dyn RpcInterceptor>>,
}

impl RpcInterceptorChain {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn name(&self) -> &'static str {
        "interceptor-chain"
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.chain.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chain.is_empty()
    }

    /// Link an interceptor.
    ///
    /// If another interceptor with the same name exists, the old one is replaced.
    pub fn link(&mut self, it: Arc<dyn RpcInterceptor>) {
        if let Some(pos) = self.chain.iter().position(|x| x.name() == it.name()) {
            self.chain.remove(pos);
        }
        self.chain.push(it);
    }

    #[must_use]
    pub fn wrap(&self, mut next: RpcInterceptorFunc) -> RpcInterceptorFunc {
        for it in self.chain.iter().rev() {
            next = it.clone().wrap(next);
        }
        next
    }

    pub(crate) fn apply(&self, info: &RpcContextInfo, ctx: &mut kvrpcpb::Context) {
        let noop: RpcInterceptorFunc = Arc::new(|_, _| {});
        let wrapped = self.wrap(noop);
        wrapped(info, ctx);
    }
}

impl RpcInterceptor for RpcInterceptorChain {
    fn name(&self) -> &str {
        self.name()
    }

    fn before_send(&self, info: &RpcContextInfo, ctx: &mut kvrpcpb::Context) {
        self.apply(info, ctx);
    }

    fn wrap(self: Arc<Self>, next: RpcInterceptorFunc) -> RpcInterceptorFunc {
        RpcInterceptorChain::wrap(&self, next)
    }
}

/// Chain multiple interceptors into one (onion model).
pub fn chain_rpc_interceptors(
    first: Arc<dyn RpcInterceptor>,
    rest: impl IntoIterator<Item = Arc<dyn RpcInterceptor>>,
) -> Arc<dyn RpcInterceptor> {
    let mut chain = RpcInterceptorChain::new();
    chain.link(first);
    for it in rest {
        chain.link(it);
    }
    Arc::new(chain)
}

/// Create mock interceptors and keep execution counters/logs.
#[derive(Clone, Default)]
pub struct MockInterceptorManager {
    inner: Arc<MockInterceptorManagerInner>,
}

#[derive(Default)]
struct MockInterceptorManagerInner {
    begin: std::sync::atomic::AtomicUsize,
    end: std::sync::atomic::AtomicUsize,
    exec_log: std::sync::Mutex<Vec<String>>,
}

impl MockInterceptorManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn create_mock_interceptor(&self, name: impl Into<String>) -> Arc<dyn RpcInterceptor> {
        Arc::new(MockInterceptor {
            name: Arc::<str>::from(name.into()),
            inner: self.inner.clone(),
        })
    }

    pub fn reset(&self) {
        use std::sync::atomic::Ordering;

        self.inner.begin.store(0, Ordering::SeqCst);
        self.inner.end.store(0, Ordering::SeqCst);
        *self
            .inner
            .exec_log
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Vec::new();
    }

    #[must_use]
    pub fn begin_count(&self) -> usize {
        use std::sync::atomic::Ordering;

        self.inner.begin.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn end_count(&self) -> usize {
        use std::sync::atomic::Ordering;

        self.inner.end.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn exec_log(&self) -> Vec<String> {
        self.inner
            .exec_log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

struct MockInterceptor {
    name: Arc<str>,
    inner: Arc<MockInterceptorManagerInner>,
}

impl RpcInterceptor for MockInterceptor {
    fn name(&self) -> &str {
        &self.name
    }

    fn before_send(&self, _info: &RpcContextInfo, _ctx: &mut kvrpcpb::Context) {}

    fn wrap(self: Arc<Self>, next: RpcInterceptorFunc) -> RpcInterceptorFunc {
        let name = self.name.to_string();
        let inner = self.inner.clone();
        Arc::new(move |info, ctx| {
            use std::sync::atomic::Ordering;

            inner
                .exec_log
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(name.clone());
            inner.begin.fetch_add(1, Ordering::SeqCst);
            next(info, ctx);
            inner.end.fetch_add(1, Ordering::SeqCst);
        })
    }
}

/// A request-scoped resource group tagger.
///
/// If a fixed resource group tag is set on the client, it takes precedence over this tagger.
pub type ResourceGroupTagger =
    Arc<dyn Fn(&RpcContextInfo, &kvrpcpb::Context) -> Vec<u8> + Send + Sync + 'static>;

/// Create an interceptor which sets `Context.resource_control_context.override_priority` if it is unset.
///
/// This matches client-go's behavior of only providing a default override priority when the caller
/// didn't set one explicitly (e.g. for runaway query deprioritization).
#[must_use]
pub fn override_priority_if_unset(override_priority: u64) -> Arc<dyn RpcInterceptor> {
    rpc_interceptor(
        "resource_control.override_priority_if_unset",
        move |_, ctx| {
            let Some(resource_control_context) = ctx.resource_control_context.as_mut() else {
                return;
            };
            if resource_control_context.resource_group_name.is_empty() {
                return;
            }
            if resource_control_context.override_priority == 0 {
                resource_control_context.override_priority = override_priority;
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interceptor_chain_wrap_is_onion_order() {
        let mgr = MockInterceptorManager::new();
        let i1 = mgr.create_mock_interceptor("i1");
        let i2 = mgr.create_mock_interceptor("i2");

        let mut chain = RpcInterceptorChain::new();
        chain.link(i1);
        chain.link(i2);

        let base_called = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let base_called_cloned = base_called.clone();
        let base: RpcInterceptorFunc = Arc::new(move |_, _| {
            base_called_cloned.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let wrapped = chain.wrap(base);

        let info = RpcContextInfo {
            label: "unit-test",
            ..Default::default()
        };
        let mut ctx = kvrpcpb::Context::default();
        wrapped(&info, &mut ctx);

        assert_eq!(base_called.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(mgr.begin_count(), 2);
        assert_eq!(mgr.end_count(), 2);
        assert_eq!(mgr.exec_log(), vec!["i1".to_owned(), "i2".to_owned()]);
    }

    #[test]
    fn chain_rpc_interceptors_dedup_by_name() {
        let it1 = rpc_interceptor("dup", |_, ctx| {
            ctx.request_source = "should_be_dropped".to_owned();
        });
        let it2 = rpc_interceptor("dup", |_, ctx| {
            ctx.resource_group_tag = vec![9_u8];
        });
        let chained = chain_rpc_interceptors(it1, vec![it2]);

        let base: RpcInterceptorFunc = Arc::new(|_, ctx| {
            assert_eq!(ctx.request_source, "");
            assert_eq!(ctx.resource_group_tag, vec![9_u8]);
        });
        let wrapped = chained.wrap(base);

        let info = RpcContextInfo::default();
        let mut ctx = kvrpcpb::Context::default();
        wrapped(&info, &mut ctx);
    }
}
