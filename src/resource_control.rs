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
    // This is a library crate: avoid panicking on lock poisoning and keep the best-effort
    // semantics (the interceptor can still be installed/uninstalled).
    let mut guard = RESOURCE_CONTROL_INTERCEPTOR
        .write()
        .unwrap_or_else(|poison| poison.into_inner());
    *guard = Some(interceptor);
}

/// Removes the global resource control interceptor.
///
/// This mirrors client-go `tikv.UnsetResourceControlInterceptor`.
pub fn unset_resource_control_interceptor() {
    let mut guard = RESOURCE_CONTROL_INTERCEPTOR
        .write()
        .unwrap_or_else(|poison| poison.into_inner());
    *guard = None;
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
    write_bytes: u64,
    replica_number: u64,
    access_location_type: crate::AccessLocationType,
    bypass: bool,
}

impl ResourceControlRequestInfo {
    #[must_use]
    pub fn new(label: &'static str, request_size: u64, store_id: u64) -> Self {
        Self {
            label,
            cmd_type: crate::CmdType::from_label(label),
            request_size,
            store_id,
            write_bytes: 0,
            replica_number: 0,
            access_location_type: crate::AccessLocationType::Unknown,
            bypass: false,
        }
    }

    #[must_use]
    pub(crate) const fn with_write_bytes(mut self, write_bytes: u64) -> Self {
        self.write_bytes = write_bytes;
        self
    }

    #[must_use]
    pub(crate) const fn with_replica_number(mut self, replica_number: u64) -> Self {
        self.replica_number = replica_number;
        self
    }

    #[must_use]
    pub(crate) const fn with_access_location_type(
        mut self,
        access_location_type: crate::AccessLocationType,
    ) -> Self {
        self.access_location_type = access_location_type;
        self
    }

    #[must_use]
    pub(crate) const fn with_bypass(mut self, bypass: bool) -> Self {
        self.bypass = bypass;
        self
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

    /// The actual bytes written by this request when it is a transactional write request.
    ///
    /// Returns `0` for read requests or when the client cannot determine the write payload size.
    #[must_use]
    pub const fn write_bytes(self) -> u64 {
        self.write_bytes
    }

    /// The number of voter/learner replicas in the target region when known.
    #[must_use]
    pub const fn replica_number(self) -> u64 {
        self.replica_number
    }

    /// Whether the request targets the local zone or crosses zones.
    #[must_use]
    pub const fn access_location_type(self) -> crate::AccessLocationType {
        self.access_location_type
    }

    /// Whether this request should bypass resource control accounting/waits.
    #[must_use]
    pub const fn bypass(self) -> bool {
        self.bypass
    }
}

/// A lightweight view of a TiKV RPC response for resource control.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResourceControlResponseInfo {
    response_size: u64,
    read_bytes: u64,
    kv_cpu: Duration,
}

impl ResourceControlResponseInfo {
    #[must_use]
    pub const fn new(response_size: u64) -> Self {
        Self {
            response_size,
            read_bytes: 0,
            kv_cpu: Duration::ZERO,
        }
    }

    #[must_use]
    pub(crate) const fn with_read_bytes(mut self, read_bytes: u64) -> Self {
        self.read_bytes = read_bytes;
        self
    }

    #[must_use]
    pub(crate) const fn with_kv_cpu(mut self, kv_cpu: Duration) -> Self {
        self.kv_cpu = kv_cpu;
        self
    }

    /// The size of the response message (in bytes), as received over gRPC.
    #[must_use]
    pub const fn response_size(self) -> u64 {
        self.response_size
    }

    /// The bytes read on the TiKV side when execution details expose scan statistics.
    ///
    /// Returns `0` when TiKV does not report scan details for this response type.
    #[must_use]
    pub const fn read_bytes(self) -> u64 {
        self.read_bytes
    }

    /// The TiKV-side processing CPU/wall time reported by execution details.
    ///
    /// Returns `Duration::ZERO` when TiKV does not report execution timing for this response.
    #[must_use]
    pub const fn kv_cpu(self) -> Duration {
        self.kv_cpu
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

pub(crate) fn hook_for_context(
    context: Option<&crate::proto::kvrpcpb::Context>,
) -> Option<ResourceControlHook> {
    if !RESOURCE_CONTROL_ENABLED.load(Ordering::Relaxed) {
        return None;
    }

    let interceptor = RESOURCE_CONTROL_INTERCEPTOR
        .read()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()?;

    let ctx = context?;
    let resource_group_name = ctx
        .resource_control_context
        .as_ref()
        .map(|rc| rc.resource_group_name.as_str())
        .filter(|name| !name.is_empty())?;

    // Keep the hook for `internal_others` requests so dispatch can surface `bypass=true`,
    // matching client-go's "hook runs, interceptor decides to skip accounting/waits" behavior.
    let request_source = ctx.request_source.as_str();
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

#[cfg(test)]
mod test {
    use std::panic;

    use serial_test::serial;

    use super::*;

    struct ResetGuard;

    impl Drop for ResetGuard {
        fn drop(&mut self) {
            disable_resource_control();
            unset_resource_control_interceptor();
        }
    }

    struct TestInterceptor;

    #[async_trait]
    impl ResourceGroupKvInterceptor for TestInterceptor {
        async fn on_request_wait(
            &self,
            _resource_group_name: &str,
            _request: &ResourceControlRequestInfo,
        ) -> Result<ResourceControlRequestWaitResult> {
            Ok(ResourceControlRequestWaitResult::default())
        }

        async fn on_response_wait(
            &self,
            _resource_group_name: &str,
            _request: &ResourceControlRequestInfo,
            _response: &ResourceControlResponseInfo,
        ) -> Result<ResourceControlResponseWaitResult> {
            Ok(ResourceControlResponseWaitResult::default())
        }
    }

    #[test]
    #[serial]
    fn test_resource_control_interceptor_lock_poison_is_tolerated() {
        let _reset = ResetGuard;

        let _ = panic::catch_unwind(|| {
            let _guard = RESOURCE_CONTROL_INTERCEPTOR.write().unwrap();
            panic!("poison resource control interceptor lock");
        });

        set_resource_control_interceptor(Arc::new(TestInterceptor));
        enable_resource_control();

        let ctx = crate::proto::kvrpcpb::Context {
            request_source: "external_test".to_owned(),
            resource_control_context: Some(crate::proto::kvrpcpb::ResourceControlContext {
                resource_group_name: "rg".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(hook_for_context(Some(&ctx)).is_some());
    }
}
