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
        if let Some(source) = self.request_source.as_deref() {
            request.set_request_source(source);
        }
        if let Some(tag) = self.resource_group_tag.as_deref() {
            request.set_resource_group_tag(tag);
        }
        if let Some(name) = self.resource_group_name.as_deref() {
            request.set_resource_group_name(name);
        }
        if let Some(priority) = self.priority {
            request.set_priority(priority);
        }
        if let Some(disk_full_opt) = self.disk_full_opt {
            request.set_disk_full_opt(disk_full_opt);
        }
        if let Some(txn_source) = self.txn_source {
            request.set_txn_source(txn_source);
        }
        if let Some(override_priority) = self.resource_control_override_priority {
            request.set_resource_control_override_priority(override_priority);
        }
        if let Some(penalty) = self.resource_control_penalty.as_deref() {
            request.set_resource_control_penalty(penalty);
        }
        request
    }
}
