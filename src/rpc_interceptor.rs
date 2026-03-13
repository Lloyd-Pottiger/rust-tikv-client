//! TiKV store RPC interceptors.
//!
//! This mirrors client-go `tikvrpc/interceptor`: an interceptor runs before and after each RPC
//! request initiated by the client.

use std::sync::Arc;

use crate::request_context::CommandPriority;
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
}
