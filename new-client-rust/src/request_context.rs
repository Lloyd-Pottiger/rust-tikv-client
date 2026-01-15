use std::sync::Arc;

use crate::store::Request;

/// Per-request `kvrpcpb::Context` fields injected into every outgoing TiKV RPC.
///
/// This is a small, cloneable bundle of optional fields which are applied to requests right before
/// sending them.
#[derive(Clone, Default, Debug)]
pub(crate) struct RequestContext {
    request_source: Option<Arc<str>>,
    resource_group_tag: Option<Arc<[u8]>>,
    resource_group_name: Option<Arc<str>>,
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
        request
    }
}
