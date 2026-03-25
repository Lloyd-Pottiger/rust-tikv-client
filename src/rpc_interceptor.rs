//! TiKV store RPC interceptors.
//!
//! This mirrors client-go `tikvrpc/interceptor`: an interceptor runs before and after each RPC
//! request initiated by the client.

use std::sync::Arc;

use crate::request_context::{CommandPriority, DiskFullOpt, TraceControlFlags};
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

    /// Returns the task ID attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.task_id`.
    #[must_use]
    pub fn task_id(&self) -> u64 {
        self.context.as_ref().map_or(0, |ctx| ctx.task_id)
    }

    /// Set the task ID attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.task_id`.
    pub fn set_task_id(&mut self, task_id: u64) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.task_id = task_id;
        }
    }

    /// Returns the busy-threshold attached to this request, in milliseconds.
    ///
    /// This maps to `kvrpcpb::Context.busy_threshold_ms`.
    #[must_use]
    pub fn busy_threshold_ms(&self) -> u32 {
        self.context.as_ref().map_or(0, |ctx| ctx.busy_threshold_ms)
    }

    /// Set the busy-threshold attached to this request, in milliseconds.
    ///
    /// This maps to `kvrpcpb::Context.busy_threshold_ms`.
    pub fn set_busy_threshold_ms(&mut self, busy_threshold_ms: u32) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.busy_threshold_ms = busy_threshold_ms;
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

    /// Returns the resource control penalty attached to this request, if any.
    ///
    /// This maps to `kvrpcpb::Context.resource_control_context.penalty`.
    #[must_use]
    pub fn resource_control_penalty(&self) -> Option<&crate::ProtoResourceConsumption> {
        self.context
            .as_ref()
            .and_then(|ctx| ctx.resource_control_context.as_ref())
            .and_then(|ctx| ctx.penalty.as_ref())
    }

    /// Set the resource control penalty attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.resource_control_context.penalty`.
    pub fn set_resource_control_penalty(
        &mut self,
        penalty: Option<crate::ProtoResourceConsumption>,
    ) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.resource_control_context
                .get_or_insert_with(kvrpcpb::ResourceControlContext::default)
                .penalty = penalty;
        }
    }

    /// Returns the resource control override priority attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.resource_control_context.override_priority`.
    #[must_use]
    pub fn resource_control_override_priority(&self) -> u64 {
        self.context
            .as_ref()
            .and_then(|ctx| ctx.resource_control_context.as_ref())
            .map_or(0, |ctx| ctx.override_priority)
    }

    /// Set the resource control override priority attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.resource_control_context.override_priority`.
    pub fn set_resource_control_override_priority(&mut self, override_priority: u64) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.resource_control_context
                .get_or_insert_with(kvrpcpb::ResourceControlContext::default)
                .override_priority = override_priority;
        }
    }

    /// Returns the transaction source attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.txn_source`.
    #[must_use]
    pub fn txn_source(&self) -> u64 {
        self.context.as_ref().map_or(0, |ctx| ctx.txn_source)
    }

    /// Set the transaction source attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.txn_source`.
    pub fn set_txn_source(&mut self, txn_source: u64) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.txn_source = txn_source;
        }
    }

    /// Returns the disk-full option attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.disk_full_opt`.
    #[must_use]
    pub fn disk_full_opt(&self) -> DiskFullOpt {
        match self.context.as_ref().map(|ctx| ctx.disk_full_opt) {
            Some(value) if value == DiskFullOpt::AllowedOnAlmostFull as i32 => {
                DiskFullOpt::AllowedOnAlmostFull
            }
            Some(value) if value == DiskFullOpt::AllowedOnAlreadyFull as i32 => {
                DiskFullOpt::AllowedOnAlreadyFull
            }
            _ => DiskFullOpt::NotAllowedOnFull,
        }
    }

    /// Set the disk-full option attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.disk_full_opt`.
    pub fn set_disk_full_opt(&mut self, disk_full_opt: DiskFullOpt) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.disk_full_opt = disk_full_opt as i32;
        }
    }

    /// Returns whether synchronous log writing is enabled for this request.
    ///
    /// This maps to `kvrpcpb::Context.sync_log`.
    #[must_use]
    pub fn sync_log(&self) -> bool {
        self.context.as_ref().is_some_and(|ctx| ctx.sync_log)
    }

    /// Set whether synchronous log writing is enabled for this request.
    ///
    /// This maps to `kvrpcpb::Context.sync_log`.
    pub fn set_sync_log(&mut self, sync_log: bool) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.sync_log = sync_log;
        }
    }

    /// Returns the max execution duration attached to this request, in milliseconds.
    ///
    /// This maps to `kvrpcpb::Context.max_execution_duration_ms`.
    #[must_use]
    pub fn max_execution_duration_ms(&self) -> u64 {
        self.context
            .as_ref()
            .map_or(0, |ctx| ctx.max_execution_duration_ms)
    }

    /// Set the max execution duration attached to this request, in milliseconds.
    ///
    /// This maps to `kvrpcpb::Context.max_execution_duration_ms`.
    pub fn set_max_execution_duration_ms(&mut self, max_execution_duration_ms: u64) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.max_execution_duration_ms = max_execution_duration_ms;
        }
    }

    /// Returns the trace-control flags attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.trace_control_flags`.
    #[must_use]
    pub fn trace_control_flags(&self) -> TraceControlFlags {
        TraceControlFlags::from(
            self.context
                .as_ref()
                .map_or(0, |ctx| ctx.trace_control_flags),
        )
    }

    /// Set the trace-control flags attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.trace_control_flags`.
    pub fn set_trace_control_flags(&mut self, flags: TraceControlFlags) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.trace_control_flags = flags.bits();
        }
    }

    /// Returns the resource group tag attached to this request, if any.
    ///
    /// This maps to `kvrpcpb::Context.resource_group_tag`.
    #[must_use]
    pub fn resource_group_tag(&self) -> Option<&[u8]> {
        self.context
            .as_ref()
            .map(|ctx| ctx.resource_group_tag.as_slice())
            .filter(|value| !value.is_empty())
    }

    /// Set the resource group tag attached to this request.
    ///
    /// This maps to `kvrpcpb::Context.resource_group_tag`.
    pub fn set_resource_group_tag(&mut self, resource_group_tag: impl Into<Vec<u8>>) {
        if let Some(ctx) = self.context.as_deref_mut() {
            ctx.resource_group_tag = resource_group_tag.into();
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
    use crate::Error;
    use crate::{CommandPriority, DiskFullOpt, TraceControlFlags};
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

    #[test]
    fn test_rpc_request_exposes_resource_control_penalty_and_override_priority() {
        let mut ctx = kvrpcpb::Context::default();
        let mut req = RpcRequest::new("target", "kv_get", Some(&mut ctx));

        assert_eq!(req.resource_control_penalty(), None);
        assert_eq!(req.resource_control_override_priority(), 0);

        let penalty = crate::ProtoResourceConsumption {
            r_r_u: 1.0,
            ..Default::default()
        };
        req.set_resource_control_penalty(Some(penalty.clone()));
        req.set_resource_control_override_priority(7);

        assert_eq!(req.resource_control_penalty(), Some(&penalty));
        assert_eq!(req.resource_control_override_priority(), 7);

        assert_eq!(
            ctx.resource_control_context
                .as_ref()
                .and_then(|ctx| ctx.penalty.as_ref()),
            Some(&penalty)
        );
        assert_eq!(
            ctx.resource_control_context
                .as_ref()
                .map(|ctx| ctx.override_priority),
            Some(7)
        );

        let mut req_without_context = RpcRequest::new("target", "kv_get", None);
        assert_eq!(req_without_context.resource_control_penalty(), None);
        assert_eq!(req_without_context.resource_control_override_priority(), 0);
        req_without_context.set_resource_control_penalty(Some(penalty));
        req_without_context.set_resource_control_override_priority(9);
        assert_eq!(req_without_context.resource_control_penalty(), None);
        assert_eq!(req_without_context.resource_control_override_priority(), 0);
    }

    #[test]
    fn test_rpc_request_exposes_write_context_scalars() {
        let mut ctx = kvrpcpb::Context {
            txn_source: 7,
            disk_full_opt: DiskFullOpt::AllowedOnAlmostFull as i32,
            sync_log: true,
            max_execution_duration_ms: 99,
            ..Default::default()
        };
        let mut req = RpcRequest::new("target", "kv_prewrite", Some(&mut ctx));

        assert_eq!(req.txn_source(), 7);
        assert_eq!(req.disk_full_opt(), DiskFullOpt::AllowedOnAlmostFull);
        assert!(req.sync_log());
        assert_eq!(req.max_execution_duration_ms(), 99);

        req.set_txn_source(42);
        req.set_disk_full_opt(DiskFullOpt::AllowedOnAlreadyFull);
        req.set_sync_log(false);
        req.set_max_execution_duration_ms(321);

        assert_eq!(req.txn_source(), 42);
        assert_eq!(req.disk_full_opt(), DiskFullOpt::AllowedOnAlreadyFull);
        assert!(!req.sync_log());
        assert_eq!(req.max_execution_duration_ms(), 321);
        assert_eq!(ctx.txn_source, 42);
        assert_eq!(ctx.disk_full_opt, DiskFullOpt::AllowedOnAlreadyFull as i32);
        assert!(!ctx.sync_log);
        assert_eq!(ctx.max_execution_duration_ms, 321);
    }

    #[test]
    fn test_rpc_request_exposes_task_id() {
        let mut ctx = kvrpcpb::Context {
            task_id: 42,
            ..Default::default()
        };
        let mut req = RpcRequest::new("target", "kv_get", Some(&mut ctx));

        assert_eq!(req.task_id(), 42);
        req.set_task_id(7);
        assert_eq!(req.task_id(), 7);
        assert_eq!(ctx.task_id, 7);

        let mut req_without_context = RpcRequest::new("target", "kv_get", None);
        assert_eq!(req_without_context.task_id(), 0);
        req_without_context.set_task_id(9);
        assert_eq!(req_without_context.task_id(), 0);
    }

    #[test]
    fn test_rpc_request_exposes_busy_threshold_ms() {
        let mut ctx = kvrpcpb::Context {
            busy_threshold_ms: 123,
            ..Default::default()
        };
        let mut req = RpcRequest::new("target", "kv_get", Some(&mut ctx));

        assert_eq!(req.busy_threshold_ms(), 123);
        req.set_busy_threshold_ms(0);
        assert_eq!(req.busy_threshold_ms(), 0);
        assert_eq!(ctx.busy_threshold_ms, 0);

        let mut req_without_context = RpcRequest::new("target", "kv_get", None);
        assert_eq!(req_without_context.busy_threshold_ms(), 0);
        req_without_context.set_busy_threshold_ms(42);
        assert_eq!(req_without_context.busy_threshold_ms(), 0);
    }

    #[test]
    fn test_rpc_request_exposes_trace_control_flags() {
        let initial =
            TraceControlFlags::IMMEDIATE_LOG.with(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS);
        let mut ctx = kvrpcpb::Context {
            trace_control_flags: initial.bits(),
            ..Default::default()
        };
        let mut req = RpcRequest::new("target", "kv_get", Some(&mut ctx));

        assert!(req
            .trace_control_flags()
            .has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(req
            .trace_control_flags()
            .has(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));

        req.set_trace_control_flags(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS);
        assert!(!req
            .trace_control_flags()
            .has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(req
            .trace_control_flags()
            .has(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS));
        assert_eq!(
            ctx.trace_control_flags,
            TraceControlFlags::TIKV_CATEGORY_READ_DETAILS.bits()
        );

        let mut req_without_context = RpcRequest::new("target", "kv_get", None);
        assert_eq!(req_without_context.trace_control_flags().bits(), 0);
        req_without_context.set_trace_control_flags(TraceControlFlags::IMMEDIATE_LOG);
        assert_eq!(req_without_context.trace_control_flags().bits(), 0);
    }

    #[test]
    fn test_rpc_request_exposes_resource_group_tag() {
        let mut ctx = kvrpcpb::Context {
            resource_group_tag: b"tag-a".to_vec(),
            ..Default::default()
        };
        let mut req = RpcRequest::new("target", "kv_prewrite", Some(&mut ctx));

        assert_eq!(req.resource_group_tag(), Some(&b"tag-a"[..]));
        req.set_resource_group_tag(b"tag-b".to_vec());
        assert_eq!(req.resource_group_tag(), Some(&b"tag-b"[..]));
        assert_eq!(ctx.resource_group_tag, b"tag-b".to_vec());
    }
}
