# client-go(v2) → tikv-client（Rust）映射草案（高层）

目标：以 `client-go` v2 的**功能**与**对外 API 能力**为基准，规划 Rust crate 的 module 边界；Rust 侧 API 命名/构造方式遵循 Rust 风格（避免 Go 式全局 config / 可变单例）。

## Scope policy（Rust 侧暴露策略）

> `.codex/progress/parity-checklist.md` 以 Go 导出符号为“工作清单入口”，但不强制 Rust 必须 1:1 复刻每个 Go package 的 API 形态。
>
> 每条 checklist item 最终必须落入以下三类之一（用 `Rust:` 字段明确标注）：
> 1) **Rust public API**：有明确的 Rust `pub` 符号/路径映射；
> 2) **Capability-only**：能力已覆盖，但实现作为 Rust crate 内部细节（Go 因缺少 visibility 粒度而导出）；
> 3) **Out-of-scope**：仅测试/过时/与 Rust 生态不相容（例如 Go 全局可变配置），用 `Rust: N/A (out-of-scope: …)` 标注并解释替代方案。

约定：
- **核心入口必须 public**：`tikv_client::{RawClient, TransactionClient, Transaction, Snapshot, Config, TransactionOptions}` 等。
- **控制面能力优先**：request-source、resource control、replica/stale read、txn 选项等应以 Rust-y API 暴露（builder/显式 opts）。
- **低层 API 不做 Go 式“类型枚举”**：优先暴露 Rust 的 request plan 抽象（`tikv_client::request`）+ 必要 hooks（interceptor/context），而不是把所有 protobuf 请求包装成一个巨大 `enum`。
- **测试/观测能力可 feature-gate**：metrics/trace/testutils 走 `cargo feature`（默认关闭或最小化），避免污染核心依赖图。

## 核心入口

- Go `rawkv.Client` → Rust `tikv_client::RawClient`（crate 根 re-export；内部实现位于 `raw` 模块）
- Go `txnkv.Client` + `tikv.KVStore`/`txnkv/transaction.KVTxn` → Rust `tikv_client::{TransactionClient, Transaction, Snapshot}`（内部 store/region cache/lock resolver 不直接 public）

## 模块映射（按 Go package）

- `client-go/config` + `config/retry`
  - Rust: `tikv_client::Config`（显式配置；避免全局 config 单例）
  - Rust: `tikv_client::backoff::Backoff` + `tikv_client::RetryOptions`（替代 Go backoffer；以 opts 驱动 retry）

- `client-go/tikvrpc` + `tikvrpc/interceptor`
  - Rust: `tikv_client::request`（Plan/PlanBuilder/KvRequest 抽象；更 Rust-y 的低层 API）
  - Rust: `tikv_client::interceptor`（RPC Context 拦截链；与 request/clients 集成）
  - Rust: `tikv_client::proto`（kvproto 生成代码）
  - 说明：不计划对齐 Go `tikvrpc::{CmdType,Request,Response}` 的“全量枚举式” public API；以 request/plan + interceptor hooks 覆盖能力。

- `client-go/tikv`（region cache / request sender / lock resolver / GC / safepoint / resource control 等组合体）
  - Rust: `tikv_client::store`（连接管理、region cache、请求路由）+ `tikv_client::transaction::lock_resolver` + `tikv_client::gc`（若对外暴露）
  - 背景任务（safe ts / txn safepoint）由 `store::KvStore` 内部驱动，公共 API 只暴露必要控制面。

- `client-go/oracle` + `oracle/oracles`
  - Rust: `tikv_client::timestamp`（TSO/ReadTS 相关类型）+（内部）`tikv_client::pd`

- `client-go/kv`
  - Rust: `tikv_client::kv::{Key, Value, KvPair, BoundRange}`（client-rust 现有类型可复用/迁移）

- `client-go/util`（TSSet、rate limit、pd interceptor、redact 等）
  - Rust: `tikv_client::util`（算法/小工具；redaction 与 Debug/Display 约束）

- `client-go/metrics` / `trace`
  - Rust: 计划追加 `tikv_client::metrics`（prometheus feature-gated）+ `tikv_client::trace`（建议基于 `tracing`/OpenTelemetry）

## 当前差异 / 后续迭代点

> 说明：这里的条目多数是 **Rust 侧刻意不复刻** 的 client-go public surface（见 scope policy）。

- `config`/`retry`：Rust 侧仅暴露显式 `Config/TransactionOptions/RetryOptions`；client-go 的全局可变 config 与大量 knobs 在 checklist 中已标注为 out-of-scope/capability-only。
- `tikvrpc`：不复刻 Go 的 `CmdType/Request/Response` 枚举式 wrapper；低层能力以 `request::PlanBuilder` + kvproto request/response 覆盖。
- `metrics`/`trace`：保持最小 public surface + feature-gate；`tracing` 集成通过 `tikv_client::trace::enable_tracing_events()`（feature `tracing`）对接生态。
- `util/*` 与 `txnkv/*`：多数属于实现细节/调试工具；映射与取舍已在 checklist 标注。

其余工程性 hardening（集成测、doc/bench、CI guardrail）以 `.codex/progress/daemon.md` 为准。
