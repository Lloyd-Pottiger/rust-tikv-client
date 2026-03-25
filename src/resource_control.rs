use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use lazy_static::lazy_static;

use crate::Result;

static RESOURCE_CONTROL_ENABLED: AtomicBool = AtomicBool::new(false);

lazy_static! {
    static ref RESOURCE_CONTROL_INTERCEPTOR: RwLock<Option<Arc<dyn ResourceGroupKvInterceptor>>> =
        RwLock::new(None);
}

/// Enables the global resource control hook.
///
/// This mirrors client-go `tikv.EnableResourceControl`.
pub fn enable_resource_control() {
    RESOURCE_CONTROL_ENABLED.store(true, Ordering::Relaxed);
}

/// Disables the global resource control hook.
///
/// This mirrors client-go `tikv.DisableResourceControl`.
pub fn disable_resource_control() {
    RESOURCE_CONTROL_ENABLED.store(false, Ordering::Relaxed);
}

/// Installs the global resource control interceptor.
///
/// This mirrors client-go `tikv.SetResourceControlInterceptor`.
pub fn set_resource_control_interceptor(interceptor: Arc<dyn ResourceGroupKvInterceptor>) {
    *RESOURCE_CONTROL_INTERCEPTOR
        .write()
        .expect("resource control interceptor lock poisoned") = Some(interceptor);
}

/// Removes the global resource control interceptor.
///
/// This mirrors client-go `tikv.UnsetResourceControlInterceptor`.
pub fn unset_resource_control_interceptor() {
    *RESOURCE_CONTROL_INTERCEPTOR
        .write()
        .expect("resource control interceptor lock poisoned") = None;
}

#[derive(Clone, Debug, Default)]
pub struct ResourceControlRequestWaitResult {
    pub consumption: Option<crate::ProtoResourceConsumption>,
    pub penalty: Option<crate::ProtoResourceConsumption>,
    pub wait_duration: Duration,
    pub priority: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ResourceControlResponseWaitResult {
    pub consumption: Option<crate::ProtoResourceConsumption>,
    pub wait_duration: Duration,
}

/// A lightweight view of a TiKV RPC request for resource control.
///
/// This intentionally contains only stable fields that are cheap to compute in the hot path.
#[derive(Clone, Copy, Debug)]
pub struct ResourceControlRequestInfo {
    label: &'static str,
    cmd_type: crate::CmdType,
    request_size: u64,
    store_id: u64,
}

impl ResourceControlRequestInfo {
    #[must_use]
    pub fn new(label: &'static str, request_size: u64, store_id: u64) -> Self {
        Self {
            label,
            cmd_type: crate::CmdType::from_label(label),
            request_size,
            store_id,
        }
    }

    /// A stable request label (for example, `"kv_get"` or `"kv_commit"`).
    #[must_use]
    pub const fn label(self) -> &'static str {
        self.label
    }

    /// The stable command type derived from [`Self::label`].
    #[must_use]
    pub const fn cmd_type(self) -> crate::CmdType {
        self.cmd_type
    }

    /// The size of the request message (in bytes), as sent over gRPC.
    #[must_use]
    pub const fn request_size(self) -> u64 {
        self.request_size
    }

    /// The target store ID of this request when known (0 when unavailable).
    #[must_use]
    pub const fn store_id(self) -> u64 {
        self.store_id
    }
}

/// A lightweight view of a TiKV RPC response for resource control.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResourceControlResponseInfo {
    response_size: u64,
}

impl ResourceControlResponseInfo {
    #[must_use]
    pub const fn new(response_size: u64) -> Self {
        Self { response_size }
    }

    /// The size of the response message (in bytes), as received over gRPC.
    #[must_use]
    pub const fn response_size(self) -> u64 {
        self.response_size
    }
}

/// A resource control interceptor that can apply request/response waits based on resource groups.
///
/// This mirrors client-go's `ResourceGroupKVInterceptor` interface (from PD resource group
/// controller), but is modeled as a Rust trait.
#[async_trait]
pub trait ResourceGroupKvInterceptor: Send + Sync + 'static {
    /// Returns true when this request should bypass resource control because it is a background
    /// request whose consumption is handled elsewhere.
    fn is_background_request(&self, _resource_group_name: &str, _request_source: &str) -> bool {
        false
    }

    /// Called before sending an RPC to TiKV to apply resource group waits/penalties.
    async fn on_request_wait(
        &self,
        resource_group_name: &str,
        request: &ResourceControlRequestInfo,
    ) -> Result<ResourceControlRequestWaitResult>;

    /// Called after receiving the RPC response to apply any follow-up waits.
    async fn on_response_wait(
        &self,
        resource_group_name: &str,
        request: &ResourceControlRequestInfo,
        response: &ResourceControlResponseInfo,
    ) -> Result<ResourceControlResponseWaitResult>;
}

#[derive(Clone)]
pub(crate) struct ResourceControlHook {
    resource_group_name: String,
    interceptor: Arc<dyn ResourceGroupKvInterceptor>,
}

const INTERNAL_OTHERS_REQUEST_SOURCE_MARKER: &str = "internal_others";

pub(crate) fn hook_for_context(
    context: Option<&crate::proto::kvrpcpb::Context>,
) -> Option<ResourceControlHook> {
    if !RESOURCE_CONTROL_ENABLED.load(Ordering::Relaxed) {
        return None;
    }

    let interceptor = RESOURCE_CONTROL_INTERCEPTOR
        .read()
        .expect("resource control interceptor lock poisoned")
        .clone()?;

    let ctx = context?;
    let resource_group_name = ctx
        .resource_control_context
        .as_ref()
        .map(|rc| rc.resource_group_name.as_str())
        .filter(|name| !name.is_empty())?;

    let request_source = ctx.request_source.as_str();
    if request_source.contains(INTERNAL_OTHERS_REQUEST_SOURCE_MARKER) {
        return None;
    }

    if interceptor.is_background_request(resource_group_name, request_source) {
        return None;
    }

    Some(ResourceControlHook {
        resource_group_name: resource_group_name.to_owned(),
        interceptor,
    })
}

impl ResourceControlHook {
    pub(crate) async fn on_request_wait(
        &self,
        request: &ResourceControlRequestInfo,
    ) -> Result<ResourceControlRequestWaitResult> {
        self.interceptor
            .on_request_wait(&self.resource_group_name, request)
            .await
    }

    pub(crate) async fn on_response_wait(
        &self,
        request: &ResourceControlRequestInfo,
        response: &ResourceControlResponseInfo,
    ) -> Result<ResourceControlResponseWaitResult> {
        self.interceptor
            .on_response_wait(&self.resource_group_name, request, response)
            .await
    }
}
