//! TiKV store RPC interceptors.
//!
//! This mirrors client-go `tikvrpc/interceptor`: an interceptor runs before and after each RPC
//! request initiated by the client.

use std::sync::Arc;

use crate::request_context::CommandPriority;
use crate::tikvrpc::CmdType;
use crate::Error;

use crate::proto::kvrpcpb;

pub(crate) type RpcInterceptors = Arc<Vec<Arc<dyn RpcInterceptor>>>;

/// The outcome of an intercepted RPC call.
#[derive(Clone, Copy, Debug)]
pub enum RpcCallResult<'a> {
    Ok,
    Err(&'a Error),
}

/// An interceptor that can observe and modify TiKV RPC requests.
///
/// Interceptors are executed in an "onion model":
/// - `before` hooks run in registration order.
/// - `after` hooks run in reverse order.
///
/// This maps to client-go `interceptor.RPCInterceptor`.
pub trait RpcInterceptor: Send + Sync + 'static {
    /// A stable name used for deduplicating interceptors.
    ///
    /// If multiple interceptors with the same name are added, only the last one is kept.
    fn name(&self) -> &str;

    /// Called right before the RPC is dispatched.
    fn before(&self, _request: &mut RpcRequest<'_>) {}

    /// Called right after the RPC finishes (success or error).
    fn after(&self, _request: &RpcRequest<'_>, _result: RpcCallResult<'_>) {}
}

/// A stable view of a TiKV RPC request passed to an interceptor.
pub struct RpcRequest<'a> {
    target: &'a str,
    label: &'static str,
    context: Option<&'a mut kvrpcpb::Context>,
}

impl<'a> RpcRequest<'a> {
    pub(crate) fn new(
        target: &'a str,
        label: &'static str,
        context: Option<&'a mut kvrpcpb::Context>,
    ) -> Self {
        Self {
            target,
            label,
            context,
        }
    }

    /// The TiKV store address the request is sent to.
    #[must_use]
    pub fn target(&self) -> &str {
        self.target
    }

    /// A stable request label (for example, `"kv_get"` or `"kv_commit"`).
    #[must_use]
    pub fn label(&self) -> &'static str {
        self.label
    }

    /// The stable command type derived from [`Self::label`].
    #[must_use]
    pub fn cmd_type(&self) -> CmdType {
        CmdType::from_label(self.label)
    }

    /// Returns whether TiKV should treat this request as a replica-read request.
    ///
    /// This maps to `kvrpcpb::Context.replica_read`.
    #[must_use]
    pub fn replica_read(&self) -> bool {
        self.context.as_ref().is_some_and(|ctx| ctx.replica_read)
    }

    /// Set whether TiKV should treat this request as a replica-read request.
    ///
    /// This maps to `kvrpcpb::Context.replica_read`.
    pub fn set_replica_read(&mut self, replica_read: bool) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.replica_read = replica_read;
        }
    }

    /// Returns whether the request is marked as stale read.
    ///
    /// This maps to `kvrpcpb::Context.stale_read`.
    #[must_use]
    pub fn stale_read(&self) -> bool {
        self.context.as_ref().is_some_and(|ctx| ctx.stale_read)
    }

    /// Set whether the request is marked as stale read.
    ///
    /// This maps to `kvrpcpb::Context.stale_read`.
    pub fn set_stale_read(&mut self, stale_read: bool) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.stale_read = stale_read;
        }
    }

    /// Returns whether this request should avoid filling TiKV block cache.
    ///
    /// This maps to `kvrpcpb::Context.not_fill_cache`.
    #[must_use]
    pub fn not_fill_cache(&self) -> bool {
        self.context.as_ref().is_some_and(|ctx| ctx.not_fill_cache)
    }

    /// Set whether this request should avoid filling TiKV block cache.
    ///
    /// This maps to `kvrpcpb::Context.not_fill_cache`.
    pub fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.not_fill_cache = not_fill_cache;
        }
    }

    /// Returns the command priority configured for the request.
    ///
    /// This maps to `kvrpcpb::Context.priority`.
    #[must_use]
    pub fn priority(&self) -> CommandPriority {
        match self.context.as_ref().map(|ctx| ctx.priority) {
            Some(value) if value == CommandPriority::Low as i32 => CommandPriority::Low,
            Some(value) if value == CommandPriority::High as i32 => CommandPriority::High,
            _ => CommandPriority::Normal,
        }
    }

    /// Set the command priority configured for the request.
    ///
    /// This maps to `kvrpcpb::Context.priority`.
    pub fn set_priority(&mut self, priority: CommandPriority) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.priority = priority as i32;
        }
    }

    /// Returns the request source label attached to this request, if any.
    ///
    /// This maps to `kvrpcpb::Context.request_source`.
    #[must_use]
    pub fn request_source(&self) -> Option<&str> {
        self.context
            .as_ref()
            .map(|ctx| ctx.request_source.as_str())
            .filter(|value| !value.is_empty())
    }

    /// Set the request source label attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.request_source`.
    pub fn set_request_source(&mut self, request_source: impl Into<String>) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.request_source = request_source.into();
        }
    }

    /// Returns the resource group name attached to this request, if any.
    ///
    /// This maps to `kvrpcpb::Context.resource_control_context.resource_group_name`.
    #[must_use]
    pub fn resource_group_name(&self) -> Option<&str> {
        self.context
            .as_ref()
            .and_then(|ctx| ctx.resource_control_context.as_ref())
            .map(|ctx| ctx.resource_group_name.as_str())
            .filter(|value| !value.is_empty())
    }

    /// Set the resource group name attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.resource_control_context.resource_group_name`.
    pub fn set_resource_group_name(&mut self, resource_group_name: impl Into<String>) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.resource_control_context
                .get_or_insert_with(kvrpcpb::ResourceControlContext::default)
                .resource_group_name = resource_group_name.into();
        }
    }
}

type BeforeHook = dyn for<'a> Fn(&mut RpcRequest<'a>) + Send + Sync + 'static;
type AfterHook = dyn for<'a> Fn(&RpcRequest<'a>, RpcCallResult<'a>) + Send + Sync + 'static;

/// A closure-based [`RpcInterceptor`] implementation.
///
/// This is a convenience helper for lightweight interceptors that don't need their own struct.
pub struct FnRpcInterceptor {
    name: String,
    before: Option<Box<BeforeHook>>,
    after: Option<Box<AfterHook>>,
}

impl FnRpcInterceptor {
    /// Create a new interceptor with no hooks.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            before: None,
            after: None,
        }
    }

    /// Set the `before` hook.
    ///
    /// The hook can observe and mutate the outgoing request (for example, by setting
    /// `kvrpcpb::Context.priority` via [`RpcRequest::set_priority`]).
    #[must_use]
    pub fn on_before<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(&mut RpcRequest<'a>) + Send + Sync + 'static,
    {
        self.before = Some(Box::new(hook));
        self
    }

    /// Set the `after` hook.
    ///
    /// The hook can observe the request and the outcome (`Ok`/`Err`) after the RPC finishes.
    #[must_use]
    pub fn on_after<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(&RpcRequest<'a>, RpcCallResult<'a>) + Send + Sync + 'static,
    {
        self.after = Some(Box::new(hook));
        self
    }
}

impl RpcInterceptor for FnRpcInterceptor {
    fn name(&self) -> &str {
        &self.name
    }

    fn before(&self, request: &mut RpcRequest<'_>) {
        if let Some(hook) = self.before.as_ref() {
            hook(request);
        }
    }

    fn after(&self, request: &RpcRequest<'_>, result: RpcCallResult<'_>) {
        if let Some(hook) = self.after.as_ref() {
            hook(request, result);
        }
    }
}

/// A chain of interceptors that can be executed as a single [`RpcInterceptor`].
///
/// Interceptors are executed in an "onion model" when invoked through this chain:
/// - `before` hooks run in link order.
/// - `after` hooks run in reverse link order.
///
/// This mirrors client-go's `RPCInterceptorChain`.
#[derive(Default)]
pub struct RpcInterceptorChain {
    chain: Vec<Arc<dyn RpcInterceptor>>,
}

impl RpcInterceptorChain {
    /// Create an empty interceptor chain.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an interceptor.
    ///
    /// Interceptors are executed in the order they are linked. If multiple interceptors with the
    /// same name are linked, only the last one is kept.
    pub fn link<I>(&mut self, interceptor: I) -> &mut Self
    where
        I: RpcInterceptor,
    {
        self.link_arc(Arc::new(interceptor));
        self
    }

    /// Add an interceptor stored in an [`Arc`].
    ///
    /// This is useful when building chains from mixed concrete types.
    pub fn link_arc(&mut self, interceptor: Arc<dyn RpcInterceptor>) -> &mut Self {
        let name = interceptor.name().to_owned();
        self.chain
            .retain(|existing| existing.name() != name.as_str());
        self.chain.push(interceptor);
        self
    }
}

impl RpcInterceptor for RpcInterceptorChain {
    fn name(&self) -> &str {
        "interceptor-chain"
    }

    fn before(&self, request: &mut RpcRequest<'_>) {
        for interceptor in self.chain.iter() {
            interceptor.before(request);
        }
    }

    fn after(&self, request: &RpcRequest<'_>, result: RpcCallResult<'_>) {
        for interceptor in self.chain.iter().rev() {
            interceptor.after(request, result);
        }
    }
}

/// Chain multiple interceptors into an [`RpcInterceptorChain`].
#[must_use]
pub fn chain_rpc_interceptors(
    first: Arc<dyn RpcInterceptor>,
    rest: impl IntoIterator<Item = Arc<dyn RpcInterceptor>>,
) -> RpcInterceptorChain {
    let mut chain = RpcInterceptorChain::new();
    chain.link_arc(first);
    for interceptor in rest {
        chain.link_arc(interceptor);
    }
    chain
}

#[cfg(test)]
mod tests {
    use super::{FnRpcInterceptor, RpcCallResult, RpcInterceptor, RpcInterceptorChain, RpcRequest};
    use crate::proto::kvrpcpb;
    use crate::tikvrpc::CmdType;
    use crate::CommandPriority;
    use crate::Error;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[test]
    fn test_fn_rpc_interceptor_hooks_run_and_can_mutate_context() {
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let calls_before = calls.clone();
        let calls_after = calls.clone();

        let interceptor = FnRpcInterceptor::new("fn")
            .on_before(move |req| {
                calls_before.lock().unwrap().push("before".to_owned());
                req.set_priority(CommandPriority::High);
            })
            .on_after(move |_req, result| {
                let suffix = match result {
                    RpcCallResult::Ok => "ok",
                    RpcCallResult::Err(_) => "err",
                };
                calls_after.lock().unwrap().push(format!("after:{suffix}"));
            });

        let mut ctx = kvrpcpb::Context::default();
        {
            let mut req = RpcRequest::new("target", "kv_get", Some(&mut ctx));
            interceptor.before(&mut req);
        }
        assert_eq!(ctx.priority, CommandPriority::High as i32);

        let err = Error::StringError("interceptor-test".to_owned());
        {
            let req = RpcRequest::new("target", "kv_get", Some(&mut ctx));
            interceptor.after(&req, RpcCallResult::Err(&err));
        }

        assert_eq!(
            calls.lock().unwrap().clone(),
            vec!["before".to_owned(), "after:err".to_owned()]
        );
    }

    #[test]
    fn test_rpc_interceptor_chain_onion_order_and_dedup() {
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let calls_a_before = calls.clone();
        let calls_a_after = calls.clone();
        let calls_b_before = calls.clone();
        let calls_b_after = calls.clone();
        let calls_a2_before = calls.clone();
        let calls_a2_after = calls.clone();

        let a = FnRpcInterceptor::new("a")
            .on_before(move |_req| calls_a_before.lock().unwrap().push("before:a".to_owned()))
            .on_after(move |_req, _result| {
                calls_a_after.lock().unwrap().push("after:a".to_owned())
            });
        let b = FnRpcInterceptor::new("b")
            .on_before(move |_req| calls_b_before.lock().unwrap().push("before:b".to_owned()))
            .on_after(move |_req, _result| {
                calls_b_after.lock().unwrap().push("after:b".to_owned())
            });

        let a_replacement = FnRpcInterceptor::new("a")
            .on_before(move |_req| calls_a2_before.lock().unwrap().push("before:a2".to_owned()))
            .on_after(move |_req, _result| {
                calls_a2_after.lock().unwrap().push("after:a2".to_owned())
            });

        let mut chain = RpcInterceptorChain::new();
        chain.link(a);
        chain.link(b);
        chain.link(a_replacement);

        let mut ctx = kvrpcpb::Context::default();
        {
            let mut req = RpcRequest::new("target", "kv_get", Some(&mut ctx));
            chain.before(&mut req);
        }
        {
            let req = RpcRequest::new("target", "kv_get", Some(&mut ctx));
            chain.after(&req, RpcCallResult::Ok);
        }

        assert_eq!(
            calls.lock().unwrap().clone(),
            vec![
                "before:b".to_owned(),
                "before:a2".to_owned(),
                "after:a2".to_owned(),
                "after:b".to_owned(),
            ]
        );
    }

    #[test]
    fn test_cmd_type_from_label_and_as_str() {
        assert_eq!(CmdType::from_label("kv_get"), CmdType::Get);
        assert_eq!(CmdType::Get.as_str(), "Get");

        assert_eq!(
            CmdType::from_label("kv_check_secondary_locks_request"),
            CmdType::CheckSecondaryLocks
        );
        assert_eq!(CmdType::CheckSecondaryLocks.as_str(), "CheckSecondaryLocks");

        assert_eq!(CmdType::from_label("not_a_real_command"), CmdType::Unknown);
        assert_eq!(CmdType::Unknown.as_str(), "Unknown");
    }

    #[test]
    fn test_rpc_request_exposes_cmd_type() {
        let mut ctx = kvrpcpb::Context::default();
        let req = RpcRequest::new("target", "raw_compare_and_swap", Some(&mut ctx));
        assert_eq!(req.cmd_type(), CmdType::RawCompareAndSwap);
    }

    #[test]
    fn test_rpc_request_exposes_request_source() {
        let mut ctx = kvrpcpb::Context {
            request_source: "external_gc".to_owned(),
            ..Default::default()
        };
        let mut req = RpcRequest::new("target", "kv_get", Some(&mut ctx));

        assert_eq!(req.request_source(), Some("external_gc"));
        req.set_request_source("internal_gc_stats");
        assert_eq!(req.request_source(), Some("internal_gc_stats"));
        assert_eq!(ctx.request_source, "internal_gc_stats");
    }

    #[test]
    fn test_rpc_request_exposes_resource_group_name() {
        let mut ctx = kvrpcpb::Context {
            resource_control_context: Some(kvrpcpb::ResourceControlContext {
                resource_group_name: "rg-a".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut req = RpcRequest::new("target", "kv_get", Some(&mut ctx));

        assert_eq!(req.resource_group_name(), Some("rg-a"));
        req.set_resource_group_name("rg-b");
        assert_eq!(req.resource_group_name(), Some("rg-b"));
        assert_eq!(
            ctx.resource_control_context
                .as_ref()
                .map(|ctx| ctx.resource_group_name.as_str()),
            Some("rg-b")
        );
    }

    #[test]
    fn test_rpc_request_set_resource_group_name_initializes_context() {
        let mut ctx = kvrpcpb::Context::default();
        let mut req = RpcRequest::new("target", "kv_get", Some(&mut ctx));

        assert_eq!(req.resource_group_name(), None);
        req.set_resource_group_name("rg-c");
        assert_eq!(req.resource_group_name(), Some("rg-c"));
        assert_eq!(
            ctx.resource_control_context
                .as_ref()
                .map(|ctx| ctx.resource_group_name.as_str()),
            Some("rg-c")
        );
    }
}
