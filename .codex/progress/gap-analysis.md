# client-rust vs client-go(v2) 缺口分析（初稿）

> 目的：决定 `new-client-rust` 的复用策略与优先补齐的能力点。  
> 说明：本文件只记录“能力/协议/对外可用性”，不是逐函数签名级 diff（后续会基于 `.codex/progress/client-go-api-inventory.md` 继续细化）。

## client-rust 已具备（从代码确认）

- 基础：PD 交互、region cache、Backoff/Retry、请求 plan 执行框架（`client-rust/src/request` + `region_cache.rs` + `pd/*`）。
- Raw API：`RawClient` 支持 get/put/batch/scan/delete_range/TTL/CAS、以及 raw coprocessor（`client-rust/src/raw/client.rs`）。
- Txn API：`TransactionClient`/`Transaction` 支持 optimistic/pessimistic、2PC、**try 1PC**、**async commit**（存在 TODO/FIXME）、lock resolver、scan locks、GC safepoint 更新（`client-rust/src/transaction/*` + `pd/*`）。
- Keyspace：`Config::with_keyspace` + `PdRpcClient::load_keyspace`（`client-rust/src/config.rs` + `raw/client.rs`/`pd/*`）。

## 相对 client-go(v2) 仍缺（按能力域）

- Replica Read / Stale Read / Follower Read
  - client-go 暴露 `kv.ReplicaReadType` 等并在请求层支持多副本读策略；client-rust 当前未见相应实现/选项（源码 grep 无匹配）。

- Resource Control / Resource Group Tag
  - client-go 有 `tikvrpc.ResourceGroupTagger`、resource control interceptor/开关等；client-rust 仅有 proto 结构体生成代码，缺少上层逻辑与对外 API。

- RawKV: checksum 等请求
  - client-go `rawkv.Client` 暴露 `Checksum`；client-rust raw client 未实现 raw checksum。

- Txn: pipelined txn / 本地 latch / TiDB 衍生能力
  - client-go v2 有 `PipelinedTxnOptions`、txn local latches、以及一批 TiDB 场景相关的对外结构（如 `tikv/unionstore_export.go` 重导出）。
  - client-rust 当前没有 pipelined txn 与 latch 相关实现。

- tikvrpc 层的“请求类型枚举 + 拦截链 + 批量 RPC 管线”能力
  - client-go `tikvrpc` 作为公共包对外暴露 request/response 分类、批量/流式、以及拦截器；client-rust 目前主要通过 `request` 模块抽象执行，缺少与 client-go 同等粒度的公共 API 对齐。

## 已知实现不完整点（从代码注释确认）

- async commit: `Committer::prewrite` 里标注 `FIXME set max_commit_ts and min_commit_ts`；需要对齐 TiKV/PD 的 async commit 语义与 client-go 的实现。

## 结论（策略建议）

- `new-client-rust` 建议**迁移/复用** `client-rust` 的核心实现作为起点（PD/region cache/request plan/raw/txn），再针对 client-go(v2) 缺口逐步补齐；
  - 理由：已覆盖事务核心协议（含 1PC/async commit 的雏形），能显著降低重写成本，并减少“猜测实现”风险。

