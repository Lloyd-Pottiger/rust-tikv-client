# client-rust / tikv-client（Rust）vs client-go(v2) 缺口分析（滚动更新）

> 目的：决定 Rust client（repo 根目录 crate）的复用策略与优先补齐的能力点。  
> 说明：本文件只记录“能力/协议/对外可用性”，不是逐函数签名级 diff（签名级跟踪见 `.codex/progress/parity-checklist.md`）。

## Rust client 已具备（从代码确认）

- 基础：PD 交互、region cache、Backoff/Retry、请求 plan 执行框架（`src/{pd,store,request,region_cache}.rs`）。
- Raw API：`RawClient` 支持 get/put/batch/scan/delete_range/TTL/CAS、raw coprocessor、raw checksum（`src/raw/*`）。
- Txn API：`TransactionClient`/`Transaction` 支持 optimistic/pessimistic、2PC、try-1PC、async-commit、pipelined flush、txn local latches、lock resolver、scan locks、GC（`src/transaction/*`）。
- Keyspace：`Config::with_keyspace` + `PdRpcClient::load_keyspace`（`src/{config.rs,pd/*}`）。
- Replica Read / Stale Read：leader/follower/learner + stale read 路由，`kvrpcpb::Context` 字段注入（`src/{request/*,replica_read.rs}`）。
- Resource Control：request_source / resource group tag / override priority + interceptor 链（`src/{request_context.rs,interceptor.rs}`）。
- Trace/Metrics：trace hooks（最小 public surface）+ prometheus feature-gate 的 metrics（`src/{trace.rs,metrics.rs,stats.rs}`）。
- parity artifacts：inventory + checklist + scope policy（`.codex/progress/{client-go-api-inventory.md,parity-checklist.md,parity-map.md}`）。

## 相对 client-go(v2) 的差异（按能力域，粗粒度）

> 多数差异为 **Rust 侧刻意不复刻** 的 client-go public surface（见 checklist 的 out-of-scope/capability-only 标注）。

### Public API surface
- `config`/`retry`：Rust 侧仅暴露最小必要配置（`Config`/`TransactionOptions`/builders）；client-go 的全局可变 config 与大量 knobs 多数被标注为 out-of-scope/capability-only。
- `tikvrpc`：不复刻 Go 的 `CmdType/Request/Response` 枚举式 wrapper；低层能力由 `PlanBuilder` + kvproto request 直接覆盖。
- `trace/metrics`：目前为最小 public surface；尚未提供更 Rust 生态的 `tracing`/OpenTelemetry 对接与 client-go 级别的 label/handle 兼容层（已在 checklist 标注为 out-of-scope）。
- `util/*`、`txnkv/*` 子包：多数属于实现细节/调试工具；映射与取舍已在 checklist 标注。

### Protocol/detail gaps（可直接落任务）
- resolve-lock-lite 已实现并按 client-go 语义做 gating（region retry attempt>0 禁用等）；细节以单测与集成测试行为为准。
- 非 generated 代码 TODO/FIXME：已清零（仅 kvproto 生成代码中仍含 TODO 注释，不作为实现缺口跟踪）。

## 已知实现不完整点（从代码注释确认）

- integration tests 需要真实 PD/TiKV 集群（`cargo test --features integration-tests` + `$PD_ADDRS`）；当前已保证 `--all-features --no-run` 可编译。
- 若需进一步校准 backoff/region cache 等性能行为，优先用 benchmark/压测驱动落任务（避免“cargo-culted knobs”）。

## 结论（策略建议）

- 以 `client-rust` 迁移为起点的策略成立：能力/协议已覆盖；Rust public surface 维持“前门小、实现细节隐藏”。
- 后续迭代重点应放在 hardening（integration/doc/bench/CI guardrail），以 `.codex/progress/daemon.md` 为准。
