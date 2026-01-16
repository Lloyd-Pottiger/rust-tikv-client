# client-rust / new-client-rust vs client-go(v2) 缺口分析（滚动更新）

> 目的：决定 `new-client-rust` 的复用策略与优先补齐的能力点。  
> 说明：本文件只记录“能力/协议/对外可用性”，不是逐函数签名级 diff（后续会基于 `.codex/progress/client-go-api-inventory.md` 继续细化）。

## new-client-rust 已具备（从代码确认）

- 基础：PD 交互、region cache、Backoff/Retry、请求 plan 执行框架（`client-rust/src/request` + `region_cache.rs` + `pd/*`）。
- Raw API：`RawClient` 支持 get/put/batch/scan/delete_range/TTL/CAS、以及 raw coprocessor（`client-rust/src/raw/client.rs`）。
- Txn API：`TransactionClient`/`Transaction` 支持 optimistic/pessimistic、2PC、**try 1PC**、**async commit**（存在 TODO/FIXME）、lock resolver、scan locks、GC safepoint 更新（`client-rust/src/transaction/*` + `pd/*`）。
- Keyspace：`Config::with_keyspace` + `PdRpcClient::load_keyspace`（`client-rust/src/config.rs` + `raw/client.rs`/`pd/*`）。
- Replica Read / Stale Read：请求路由支持 leader/follower/learner + stale read，并在 `kvrpcpb::Context` 填充对应字段（`new-client-rust/src/request/*` + `new-client-rust/src/replica_read.rs`）。
- Resource Control：支持 request_source / resource group tag / resource control ctx 字段 + interceptor 链（`new-client-rust/src/request_context.rs` + `new-client-rust/src/interceptor.rs`）。
- Pipelined txn + local latches：实现 flush pipeline + 乐观事务 local latches（`new-client-rust/src/transaction/*`）。
- Raw checksum：支持 raw range checksum（`new-client-rust/src/raw/client.rs`）。

## 相对 client-go(v2) 仍缺（按能力域，粗粒度）

### Public API surface
- `config`/`retry`：Go 侧大量配置项/全局 config API 未在 Rust 侧暴露（Rust 计划用显式 `Config`/`TransactionOptions`/builders 取代全局可变 config）。
- `tikvrpc`：Go 暴露的 `CmdType/Request/Response` “枚举式请求构造层”在 Rust 侧尚未设计 public wrapper；当前低层能力由 `tikv_client::request` + proto 类型覆盖。
- `metrics`/`trace`：Rust 侧目前使用 prometheus/log，但缺少对齐 client-go 的导出 metrics/trace public API 与 feature-gate 策略。
- `util/*`、`txnkv/*` 子包：大量 Go 导出符号属于实现细节/调试/测试工具（Rust 需逐项判定 public vs capability-only，并在 checklist 标注）。

### Protocol/detail gaps（可直接落任务）
- async-commit safe window 目前常量化（`new-client-rust/src/transaction/transaction.rs`），需要可配置并对齐 client-go。
- lite resolve lock 目前 disabled（`new-client-rust/src/transaction/requests.rs`）。
- lock resolver/backoff 等实现细节存在 TODO/FIXME（见 `rg TODO|FIXME new-client-rust/src`）。

## 已知实现不完整点（从代码注释确认）

- 若干参数/算法仍带 “cargo-culted” TODO，需要逐步以 client-go 行为与压测结果校准（例如 `pd/retry.rs`、region cache 相关）。

## 结论（策略建议）

- `new-client-rust` 建议**迁移/复用** `client-rust` 的核心实现作为起点（PD/region cache/request plan/raw/txn），再针对 client-go(v2) 缺口逐步补齐；
  - 理由：已覆盖事务核心协议（含 1PC/async commit 的雏形），能显著降低重写成本，并减少“猜测实现”风险。
  - 后续以 `.codex/progress/parity-checklist.md` 为签名级跟踪入口，并在 `.codex/progress/parity-map.md` 明确哪些为 Rust public / capability-only / out-of-scope。
