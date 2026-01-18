use std::fmt;
use std::sync::Arc;

use crate::interceptor::ResourceGroupTagger;
use crate::interceptor::RpcInterceptorChain;
use crate::proto::resource_manager;
use crate::store::Request;
use crate::CommandPriority;
use crate::DiskFullOpt;

/// Per-request `kvrpcpb::Context` fields injected into every outgoing TiKV RPC.
///
/// This is a small, cloneable bundle of optional fields which are applied to requests right before
/// sending them.
#[derive(Clone)]
pub(crate) struct RequestContext {
    request_source: Option<Arc<str>>,
    resource_group_tag: Option<Arc<[u8]>>,
    resource_group_name: Option<Arc<str>>,
    priority: Option<CommandPriority>,
    disk_full_opt: Option<DiskFullOpt>,
    txn_source: Option<u64>,
    trace_id: Option<Arc<[u8]>>,
    trace_control_flags: Option<u64>,
    resource_control_penalty: Option<Arc<resource_manager::Consumption>>,
    resource_control_override_priority: Option<u64>,
    resource_group_tagger: Option<ResourceGroupTagger>,
    rpc_interceptors: RpcInterceptorChain,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self {
            request_source: Some(Arc::<str>::from("unknown")),
            resource_group_tag: None,
            resource_group_name: None,
            priority: None,
            disk_full_opt: None,
            txn_source: None,
            trace_id: None,
            trace_control_flags: None,
            resource_control_penalty: None,
            resource_control_override_priority: None,
            resource_group_tagger: None,
            rpc_interceptors: RpcInterceptorChain::default(),
        }
    }
}

impl fmt::Debug for RequestContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestContext")
            .field("request_source", &self.request_source.as_deref())
            .field(
                "resource_group_tag",
                &self.resource_group_tag.as_deref().map(|t| t.len()),
            )
            .field("resource_group_name", &self.resource_group_name.as_deref())
            .field("priority", &self.priority)
            .field("disk_full_opt", &self.disk_full_opt)
            .field("txn_source", &self.txn_source)
            .field("trace_id", &self.trace_id.as_deref().map(|t| t.len()))
            .field("trace_control_flags", &self.trace_control_flags)
            .field(
                "resource_control_penalty",
                &self.resource_control_penalty.is_some(),
            )
            .field(
                "resource_control_override_priority",
                &self.resource_control_override_priority,
            )
            .field(
                "resource_group_tagger",
                &self.resource_group_tagger.is_some(),
            )
            .field("rpc_interceptors", &self.rpc_interceptors.len())
            .finish()
    }
}

impl RequestContext {
    #[must_use]
    pub(crate) fn with_request_source(&self, source: impl Into<String>) -> Self {
        let mut cloned = self.clone();
        cloned.request_source = Some(Arc::<str>::from(source.into()));
        cloned
    }

    #[must_use]
    pub(crate) fn with_resource_group_tag(&self, tag: impl Into<Vec<u8>>) -> Self {
        let mut cloned = self.clone();
        cloned.resource_group_tag = Some(Arc::<[u8]>::from(tag.into()));
        cloned
    }

    #[must_use]
    pub(crate) fn with_resource_group_name(&self, name: impl Into<String>) -> Self {
        let mut cloned = self.clone();
        cloned.resource_group_name = Some(Arc::<str>::from(name.into()));
        cloned
    }

    #[must_use]
    pub(crate) fn with_priority(&self, priority: CommandPriority) -> Self {
        let mut cloned = self.clone();
        cloned.priority = Some(priority);
        cloned
    }

    #[must_use]
    pub(crate) fn with_disk_full_opt(&self, disk_full_opt: DiskFullOpt) -> Self {
        let mut cloned = self.clone();
        cloned.disk_full_opt = Some(disk_full_opt);
        cloned
    }

    #[must_use]
    pub(crate) fn with_txn_source(&self, txn_source: u64) -> Self {
        let mut cloned = self.clone();
        cloned.txn_source = Some(txn_source);
        cloned
    }

    #[must_use]
    pub(crate) fn with_trace_id(&self, trace_id: impl Into<Vec<u8>>) -> Self {
        let mut cloned = self.clone();
        cloned.trace_id = Some(Arc::<[u8]>::from(trace_id.into()));
        cloned
    }

    #[must_use]
    pub(crate) fn with_trace_control_flags(&self, flags: u64) -> Self {
        let mut cloned = self.clone();
        cloned.trace_control_flags = Some(flags);
        cloned
    }

    #[must_use]
    pub(crate) fn with_trace_control(&self, flags: crate::trace::TraceControlFlags) -> Self {
        self.with_trace_control_flags(flags.0)
    }

    #[must_use]
    pub(crate) fn with_resource_control_penalty(
        &self,
        penalty: impl Into<resource_manager::Consumption>,
    ) -> Self {
        let mut cloned = self.clone();
        cloned.resource_control_penalty = Some(Arc::new(penalty.into()));
        cloned
    }

    #[must_use]
    pub(crate) fn with_resource_control_override_priority(&self, override_priority: u64) -> Self {
        let mut cloned = self.clone();
        cloned.resource_control_override_priority = Some(override_priority);
        cloned
    }

    #[must_use]
    pub(crate) fn with_resource_group_tagger(&self, tagger: ResourceGroupTagger) -> Self {
        let mut cloned = self.clone();
        cloned.resource_group_tagger = Some(tagger);
        cloned
    }

    #[must_use]
    pub(crate) fn with_rpc_interceptors(&self, interceptors: RpcInterceptorChain) -> Self {
        let mut cloned = self.clone();
        cloned.rpc_interceptors = interceptors;
        cloned
    }

    #[must_use]
    pub(crate) fn add_rpc_interceptor(
        &self,
        interceptor: Arc<dyn crate::interceptor::RpcInterceptor>,
    ) -> Self {
        let mut cloned = self.clone();
        cloned.rpc_interceptors.link(interceptor);
        cloned
    }

    pub(crate) fn resource_group_tagger(&self) -> Option<&ResourceGroupTagger> {
        self.resource_group_tagger.as_ref()
    }

    pub(crate) fn has_resource_group_tag(&self) -> bool {
        self.resource_group_tag.is_some()
    }

    pub(crate) fn rpc_interceptors(&self) -> &RpcInterceptorChain {
        &self.rpc_interceptors
    }

    pub(crate) fn request_source(&self) -> Option<Arc<str>> {
        self.request_source.clone()
    }

    pub(crate) fn apply_to<R: Request>(&self, mut request: R) -> R {
        let ctx = request.context_mut();
        if let Some(source) = self.request_source.as_deref() {
            ctx.request_source = source.to_owned();
        }
        if let Some(tag) = self.resource_group_tag.as_deref() {
            ctx.resource_group_tag = tag.to_vec();
        }
        if let Some(name) = self.resource_group_name.as_deref() {
            let resource_ctl_ctx = ctx
                .resource_control_context
                .get_or_insert(crate::kvrpcpb::ResourceControlContext::default());
            resource_ctl_ctx.resource_group_name = name.to_owned();
        }
        if let Some(priority) = self.priority {
            ctx.priority = priority.into();
        }
        if let Some(disk_full_opt) = self.disk_full_opt {
            ctx.disk_full_opt = disk_full_opt.into();
        }
        if let Some(txn_source) = self.txn_source {
            ctx.txn_source = txn_source;
        }
        if let Some(trace_id) = self.trace_id.as_deref() {
            ctx.trace_id = trace_id.to_vec();
        }
        if let Some(flags) = self.trace_control_flags {
            ctx.trace_control_flags = flags;
        }
        if let Some(override_priority) = self.resource_control_override_priority {
            let resource_ctl_ctx = ctx
                .resource_control_context
                .get_or_insert(crate::kvrpcpb::ResourceControlContext::default());
            resource_ctl_ctx.override_priority = override_priority;
        }
        if let Some(penalty) = self.resource_control_penalty.as_deref() {
            let resource_ctl_ctx = ctx
                .resource_control_context
                .get_or_insert(crate::kvrpcpb::ResourceControlContext::default());
            resource_ctl_ctx.penalty = Some(penalty.clone());
        }
        request
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interceptor::rpc_interceptor;
    use crate::proto::kvrpcpb;
    use crate::trace::TraceControlFlags;
    use crate::CommandPriority;
    use crate::DiskFullOpt;

    #[test]
    fn apply_to_sets_all_supported_context_fields() {
        let penalty = resource_manager::Consumption::default();
        let mut ctx = RequestContext::default()
            .with_request_source("src")
            .with_resource_group_tag(vec![1, 2, 3])
            .with_resource_group_name("rg")
            .with_priority(CommandPriority::Low)
            .with_disk_full_opt(DiskFullOpt::AllowedOnAlreadyFull)
            .with_txn_source(7)
            .with_trace_id(vec![9, 9, 9])
            .with_trace_control_flags(123)
            .with_resource_control_override_priority(456)
            .with_resource_control_penalty(penalty.clone());

        // Make sure the interceptor chain is cloneable and link-able.
        ctx = ctx.add_rpc_interceptor(rpc_interceptor("noop", |_, _| {}));
        assert_eq!(ctx.rpc_interceptors.len(), 1);

        let req = kvrpcpb::GetRequest::default();
        let req = ctx.apply_to(req);
        let applied = req.context.unwrap_or_default();

        assert_eq!(applied.request_source, "src");
        assert_eq!(applied.resource_group_tag, vec![1, 2, 3]);
        assert_eq!(applied.priority, i32::from(CommandPriority::Low));
        assert_eq!(
            applied.disk_full_opt,
            i32::from(DiskFullOpt::AllowedOnAlreadyFull)
        );
        assert_eq!(applied.txn_source, 7);
        assert_eq!(applied.trace_id, vec![9, 9, 9]);
        assert_eq!(applied.trace_control_flags, 123);

        let ctl = applied.resource_control_context.unwrap_or_default();
        assert_eq!(ctl.resource_group_name, "rg");
        assert_eq!(ctl.override_priority, 456);
        assert_eq!(ctl.penalty, Some(penalty));
    }

    #[test]
    fn debug_includes_key_fields() {
        let ctx = RequestContext::default()
            .with_request_source("src")
            .with_resource_group_tag(vec![1, 2, 3])
            .with_resource_group_name("rg")
            .with_priority(CommandPriority::High)
            .with_disk_full_opt(DiskFullOpt::AllowedOnAlmostFull)
            .with_txn_source(7);

        let s = format!("{ctx:?}");
        assert!(s.contains("RequestContext"), "{s}");
        assert!(s.contains("src"), "{s}");
        assert!(s.contains("rpc_interceptors"), "{s}");
    }

    #[test]
    fn with_trace_control_sets_flags() {
        let ctx = RequestContext::default().with_trace_control(TraceControlFlags(42));
        let req = ctx.apply_to(kvrpcpb::GetRequest::default());
        let applied = req.context.unwrap_or_default();
        assert_eq!(applied.trace_control_flags, 42);
    }
}
