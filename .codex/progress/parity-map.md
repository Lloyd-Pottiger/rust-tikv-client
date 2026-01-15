# client-go(v2) → new-client-rust 映射草案（高层）

目标：以 `client-go` v2 的**功能**与**对外 API 能力**为基准，规划 `new-client-rust` 的 crate/module 边界；Rust 侧 API 命名/构造方式遵循 Rust 风格（避免 Go 式全局 config / 可变单例）。

## 核心入口

- Go `rawkv.Client` → Rust `tikv_client::raw::RawClient`（已有 client-rust 形态可复用/迁移）
- Go `txnkv.Client` + `tikv.KVStore`/`txnkv/transaction.KVTxn` → Rust `tikv_client::transaction::{TransactionClient, Transaction, Snapshot}` +（内部）`store::KvStore`

## 模块映射（按 Go package）

- `client-go/config` + `config/retry`
  - Rust: `tikv_client::config`（显式 `Config` + builder）
  - Rust: `tikv_client::backoff`（统一 Backoff/Retry 配置；避免全局可变配置）

- `client-go/tikvrpc` + `tikvrpc/interceptor`
  - Rust: `tikv_client::rpc`（请求构造/分类/flags）+ `tikv_client::interceptor`（可选：请求拦截链）
  - Rust: `tikv_client::proto`（kvproto 生成代码；prost/tonic）

- `client-go/tikv`（region cache / request sender / lock resolver / GC / safepoint / resource control 等组合体）
  - Rust: `tikv_client::store`（连接管理、region cache、请求路由）+ `tikv_client::transaction::lock_resolver` + `tikv_client::gc`（若对外暴露）
  - 背景任务（safe ts / txn safepoint）由 `store::KvStore` 内部驱动，公共 API 只暴露必要控制面。

- `client-go/oracle` + `oracle/oracles`
  - Rust: `tikv_client::timestamp`（TSO/ReadTS validator）+ `tikv_client::pd`（PD 交互）

- `client-go/kv`
  - Rust: `tikv_client::kv::{Key, Value, KvPair, BoundRange}`（client-rust 现有类型可复用/迁移）

- `client-go/util`（TSSet、rate limit、pd interceptor、redact 等）
  - Rust: `tikv_client::util`（算法/小工具；redaction 与 Debug/Display 约束）

- `client-go/metrics` / `trace`
  - Rust: `tikv_client::metrics`（prometheus feature-gated）+ `tikv_client::trace`（建议基于 `tracing`/OpenTelemetry；保留必要开关/事件）

## 缺口提示（待 Phase 1/2 完成后补齐）

- `client-go` v2 宣传特性：Follower Read / 1PC / Async Commit（需逐代码对齐到 txn 协议实现与选项）。
- `client-go` 的公开包数量较多（含 `util/*`, `txnkv/*` 子包）；Rust 侧需要决定哪些能力作为 public module 暴露，哪些仅作为实现细节（但要覆盖能力）。

