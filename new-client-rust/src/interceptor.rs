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
    fn name(&self) -> &'static str;

    /// Called right before sending a request to a TiKV store.
    ///
    /// Implementations may mutate `ctx` (e.g. set resource control fields).
    fn before_send(&self, info: &RpcContextInfo, ctx: &mut kvrpcpb::Context);
}

struct FnRpcInterceptor {
    name: &'static str,
    f: Box<dyn Fn(&RpcContextInfo, &mut kvrpcpb::Context) + Send + Sync + 'static>,
}

impl RpcInterceptor for FnRpcInterceptor {
    fn name(&self) -> &'static str {
        self.name
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
        name,
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

    pub(crate) fn apply(&self, info: &RpcContextInfo, ctx: &mut kvrpcpb::Context) {
        for it in &self.chain {
            it.before_send(info, ctx);
        }
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
