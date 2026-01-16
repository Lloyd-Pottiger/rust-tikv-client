# client-rust / new-client-rust vs client-go(v2) 缺口分析（滚动更新）

> 目的：决定 `new-client-rust` 的复用策略与优先补齐的能力点。  
> 说明：本文件只记录“能力/协议/对外可用性”，不是逐函数签名级 diff（签名级跟踪见 `.codex/progress/parity-checklist.md`）。

## new-client-rust 已具备（从代码确认）

- 基础：PD 交互、region cache、Backoff/Retry、请求 plan 执行框架（`new-client-rust/src/{pd,store,request,region_cache}.rs`）。
- Raw API：`RawClient` 支持 get/put/batch/scan/delete_range/TTL/CAS、raw coprocessor、raw checksum（`new-client-rust/src/raw/*`）。
- Txn API：`TransactionClient`/`Transaction` 支持 optimistic/pessimistic、2PC、try-1PC、async-commit、pipelined flush、txn local latches、lock resolver、scan locks、GC（`new-client-rust/src/transaction/*`）。
- Keyspace：`Config::with_keyspace` + `PdRpcClient::load_keyspace`（`new-client-rust/src/{config.rs,pd/*}`）。
- Replica Read / Stale Read：leader/follower/learner + stale read 路由，`kvrpcpb::Context` 字段注入（`new-client-rust/src/{request/*,replica_read.rs}`）。
- Resource Control：request_source / resource group tag / override priority + interceptor 链（`new-client-rust/src/{request_context.rs,interceptor.rs}`）。
- Trace/Metrics：trace hooks（最小 public surface）+ prometheus feature-gate 的 metrics（`new-client-rust/src/{trace.rs,metrics.rs,stats.rs}`）。
- parity artifacts：inventory + checklist + scope policy（`.codex/progress/{client-go-api-inventory.md,parity-checklist.md,parity-map.md}`）。

## 相对 client-go(v2) 仍缺（按能力域，粗粒度）

### Public API surface
- `config`/`retry`：Rust 侧仅暴露最小必要配置（`Config`/`TransactionOptions`/builders）；client-go 的全局可变 config 与大量 knobs 多数被标注为 out-of-scope/capability-only。
- `tikvrpc`：不复刻 Go 的 `CmdType/Request/Response` 枚举式 wrapper；低层能力由 `PlanBuilder` + kvproto request 直接覆盖。
- `trace/metrics`：目前为最小 public surface；尚未提供更 Rust 生态的 `tracing`/OpenTelemetry 对接与 client-go 级别的 label/handle 兼容层（已在 checklist 标注为 out-of-scope）。
- `util/*`、`txnkv/*` 子包：导出符号已在 checklist 完成 public vs capability-only vs out-of-scope 标注；后续缺口以 TODO/FIXME 形式跟踪（见下节）。

### Protocol/detail gaps（可直接落任务）
- resolve-lock-lite 已实现并按 client-go 语义做 gating（region retry attempt>0 禁用等）；剩余缺口以代码 TODO/FIXME 为准。
- 非 generated 代码 TODO/FIXME（当前快照，`rg TODO|FIXME new-client-rust/src --glob '!*/generated/*'`）：
  - `new-client-rust/src/pd/retry.rs`：retry/backoff 参数仍是 cargo-cult，需要对齐 client-go 行为与压测结果。
  - `new-client-rust/src/request/plan.rs`：backoff/细粒度处理仍有 TODO。
  - `new-client-rust/src/region_cache.rs`：TTL 策略与性能点（锁/数据结构）仍有 TODO/FIXME。
  - `new-client-rust/src/transaction/{buffer.rs,lock.rs,requests.rs}`：buffer 数据结构优化、LockResolver 结构收敛、ScanLock next-batch 边界优化。
  - `new-client-rust/src/{kv/bound_range.rs,pd/{cluster.rs,timestamp.rs},raw/requests.rs,util/iter.rs}`：若干 correctness/性能/测试 TODO。

## 已知实现不完整点（从代码注释确认）

- 若干参数/算法仍带 “cargo-culted” TODO，需要逐步以 client-go 行为与压测结果校准（优先 `pd/retry.rs` 与 region cache 相关）。
- integration tests 需要真实 PD/TiKV 集群（`cargo test --features integration-tests` + `$PD_ADDRS`）；当前已保证 `--all-features --no-run` 可编译。

## 结论（策略建议）

- `new-client-rust` 以 `client-rust` 迁移为起点的策略仍成立（PD/region cache/request plan/raw/txn），后续以 client-go(v2) 缺口逐步补齐；
  - 理由：已覆盖事务核心协议（含 1PC/async commit 的雏形），能显著降低重写成本，并减少“猜测实现”风险。
  - 后续以 `.codex/progress/parity-checklist.md` 为签名级跟踪入口，并在 `.codex/progress/parity-map.md` 明确哪些为 Rust public / capability-only / out-of-scope。
