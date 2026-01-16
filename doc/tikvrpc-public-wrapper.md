# tikvrpc (client-go) public layer: Rust replacement design

Goal: cover the **public capabilities** of `client-go/tikvrpc` without porting Go's `CmdType/Request/Response`
"mega-wrapper" API 1:1.

## Decision

- Rust-side low-level API is `tikv_client::request::PlanBuilder` + kvproto request/response types.
- `tikvrpc/interceptor` maps to `tikv_client::interceptor` (already implemented).
- Per-request context injection (request_source / resource group / resource control / priority / etc)
  is provided as **PlanBuilder builder methods**, matching the high-level client builders.

Rationale:
- Rust already has strong typing for kvproto requests; wrapping everything into a single enum erases
  type information and becomes a maintenance burden.
- `PlanBuilder` already represents "how to send this request" (sharding, retry, lock resolve, merge).
  This maps better to Rust than Go's `CmdType` switch.

## Mapping (Go → Rust)

- `tikvrpc.NewRequest(...)` / `NewReplicaReadRequest(...)`
  - Rust: create a typed kvproto request (e.g. via `tikv_client::raw_lowering` /
    `tikv_client::transaction_lowering`), then:
    - `PlanBuilder::new(pd, keyspace, req)`
    - optional: `PlanBuilder::{replica_read,stale_read,replica_read_seed}`

- `tikvrpc.AttachContext(req, ctx)` / `SetContext*`
  - Rust: `PlanBuilder` context builder methods:
    - `with_request_source`
    - `with_resource_group_tag` / `with_resource_group_name` / `with_resource_group_tagger`
    - `with_priority` / `with_disk_full_opt` / `with_txn_source`
    - `with_resource_control_override_priority` / `with_resource_control_penalty`

- `tikvrpc/interceptor.*`
  - Rust: `tikv_client::interceptor::{RpcInterceptor,RpcInterceptorChain,rpc_interceptor,...}`
  - `PlanBuilder::{with_rpc_interceptor,with_added_rpc_interceptor}` mirrors client-level builders.

## Example (low-level RawGet)

```rust,no_run
use std::sync::Arc;
use tikv_client::request::PlanBuilder;
use tikv_client::{Backoff, Config, Keyspace, RawClient};
use tikv_client::kvrpcpb;

# async fn example(pd: Arc<tikv_client::pd::PdRpcClient>) -> tikv_client::Result<()> {
let mut req = kvrpcpb::RawGetRequest::default();
req.key = b"hello".to_vec();

let plan = PlanBuilder::new(pd, Keyspace::Disable, req)
    .with_request_source("app")
    .with_resource_group_name("rg1")
    .with_added_rpc_interceptor(tikv_client::interceptor::override_priority_if_unset(123))
    .retry_multi_region(Backoff::no_backoff())
    .merge(tikv_client::request::CollectSingle)
    .post_process_default()
    .plan();

let _val = plan.execute().await?;
Ok(())
# }
```

Notes:
- This intentionally keeps kvproto request/response types visible at the lowest level.
- Custom request types are still "sealed": only the built-in kvproto request types implement the
  internal request traits used by `PlanBuilder`.

