# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- Pessimistic lock ctx/options：补齐 lock request 关键字段与对外配置（对齐 client-go v2）
  - 计划：
    - 对齐 `PessimisticLockRequest`：`min_commit_ts/check_existence/lock_only_if_exists/wake_up_mode/return_values/is_first_lock` 等
    - Rust：补齐 `Transaction::{lock_keys,get_for_update,batch_get_for_update}` 可配置项（可能新增 `LockOptions`/`LockCtx` 风格 builder）
    - UT：构造 mock server 检查请求字段 + error 透传（含部分成功 keys + rollback）

# 待做工作

- Txn buffer flags：补齐 `KeyFlags/FlagsOp` 风格的 per-key 元数据（含 assertion/presumeKNE/locked 等），并与 mutation lowering 对齐
  - 计划：
    - 设计 Rust 侧 key-flag 存储结构（避免额外 clone；保证可扩展）
    - 对齐 client-go flags 语义（至少覆盖 assertion + presumeKNE + prewrite-only）
    - UT：状态机测试 + prewrite/lock request 字段透传

# 已完成工作

- 维护追踪：回填 `.codex/progress/parity-checklist.md` 已完成项（Rust path + Tests）
  - 完成：为 error 模型 / RequestContext setters / Txn assertions / pipelined txn / replica read / interceptor 等条目补齐 Rust 映射与测试索引，便于后续按清单推进
  - 改动文件：`.codex/progress/parity-checklist.md`、`.codex/progress/daemon.md`

- Txn assertions：补齐 `AssertionLevel` 对外 API，并贯穿 prewrite/flush/pessimistic lock
  - 完成：新增 `TransactionOptions::assertion_level` + `Transaction::set_assertion_level`；新增 `Transaction::set_key_assertion` 并在 Buffer 里持久化 per-key assertion；Prewrite/Flush request 写入 `assertion_level` 且 mutation 透传 assertion（assertion_level=Off 时不发送）
  - 关键决策：commit/prewrite 返回单个 KeyError 时解包 `MultipleKeyErrors`，避免把“单个 assertion failed/insert conflict”包装成 Vec 影响上层判断
  - 测试：新增 UT 覆盖 Prewrite/Flush request 字段与 `AssertionFailed/KeyExists` 错误透传；`cd new-client-rust && cargo test` 通过
  - 改动文件：`new-client-rust/src/transaction/{transaction.rs,buffer.rs,pipelined.rs}`、`.codex/progress/daemon.md`

- 错误模型对齐：`kvrpcpb::KeyError` → 结构化 Rust `Error`
  - 完成：新增 `Error::{WriteConflict,Deadlock,KeyExists,AssertionFailed,Retryable,TxnAborted,CommitTsTooLarge,TxnNotFound}` + `Error::{is_write_conflict,is_deadlock,is_key_exists,is_assertion_failed}`；保留 `Error::KeyError` 作为兜底
  - 测试：新增 UT 覆盖 KeyError→Error 映射；`cd new-client-rust && cargo test` 通过
  - 改动文件：`new-client-rust/src/common/errors.rs`、`.codex/progress/daemon.md`

- RequestContext 补齐：`disk_full_opt` / `txn_source`（并贯穿 Raw/Txn/resolve lock/flush）
  - 完成：新增 crate-level `DiskFullOpt`；`RequestContext` 支持 `disk_full_opt/txn_source` 并通过 `store::Request` 透传到所有 TiKV RPC；补齐 RawClient/TxnClient/Transaction setters；pipelined flush/resolve-lock 仍能保留这两个字段（仅覆盖 request_source）
  - 测试：扩展 Raw/TXN/Flush/ResolveLock UT 覆盖 context 透传；`cd new-client-rust && cargo test` 通过
  - 改动文件：`new-client-rust/src/{disk_full_opt.rs,request_context.rs,store/request.rs,raw/client.rs,transaction/{client.rs,transaction.rs},lib.rs}`、`.codex/progress/daemon.md`

- 事务协议补齐：pipelined txn / txn local latches（对齐 client-go v2）
  - 完成：实现 `kvrpcpb::FlushRequest` 管线（generation+min_commit_ts+TTL）与 Txn pipelined commit/rollback（commit primary + 异步 resolve-lock range；rollback cancel+flush_wait+resolve-lock）；实现进程内 local latches（murmur3 slot + stale=max_commit_ts）并接入 optimistic commit（pessimistic/pipelined bypass）
  - 关键决策：pipelined flush/resolve-lock 固定 `Context.request_source="external_pdml"`；flushed range 记录为 `[min_key, max_key.next())`；ResolveLockRequest 仍保持“上层手动处理 region error”的约束
  - 测试：新增 pipelined flush/rollback + latch UT；`cd new-client-rust && cargo test` 通过
  - 改动文件：`new-client-rust/src/transaction/{pipelined.rs,latch.rs,transaction.rs,requests.rs,client.rs,buffer.rs,mod.rs}`、`new-client-rust/src/store/{request.rs,errors.rs}`、`new-client-rust/src/request/{plan.rs,plan_builder.rs}`、`new-client-rust/src/common/errors.rs`、`new-client-rust/src/pd/timestamp.rs`、`new-client-rust/proto/*`、`new-client-rust/src/generated/*`、`.codex/progress/daemon.md`

- Parity 规划与清单（client-go v2 / client-rust 现状）
  - 关键产物：`.codex/progress/client-go-api-inventory.md`、`.codex/progress/parity-map.md`、`.codex/progress/gap-analysis.md`、`.codex/progress/parity-checklist.md`、`tools/client-go-api-inventory/`
  - 关键决策：用源码自动提取导出符号与签名，避免人工漏项；new-client-rust 以迁移 client-rust 为起点再补齐缺口

- new-client-rust 基线能力落地（Raw/Txn + RequestContext + async commit 语义）
  - 覆盖：工程骨架可编译可测试；RawKV `checksum`；Txn async commit/1PC `min_commit_ts/max_commit_ts` + fallback；RequestContext 贯穿 Raw/Txn/resolve lock（request_source/resource_group_tag/resource_group_name）
  - 主要文件：`new-client-rust/src/{raw,transaction,request,store,request_context}.rs`、`new-client-rust/doc/client-go-v2-parity-roadmap.md`

- 读路径 Replica Read / Stale Read（对齐 client-go v2）
  - 实现：新增 `ReplicaReadType` + `TransactionOptions::{replica_read,stale_read}`；计划层按 policy 选择 region peer/store 写入 `kvrpcpb::Context.{peer,replica_read,stale_read}`；stale-read meet-lock 后强制 leader reread
  - 测试：新增 UT 覆盖 leader/follower/learner/mixed/prefer-leader 与 stale-read fallback；`cd new-client-rust && cargo test` 通过
  - 改动文件：`new-client-rust/src/{replica_read.rs,request/{read_routing,plan,plan_builder,mod,shard}.rs,store/{mod,errors,request}.rs,transaction/transaction.rs,lib.rs}`、`.codex/progress/daemon.md`

- Resource Control（扩展项：penalty/override_priority + tagger/interceptor）
  - 完成：`ResourceGroupTagger` 对齐 TiDB 逻辑（tagger 可读取最终 `Context.request_source`，含 retry 拼接）；新增 `RpcInterceptor` hook（发送前可改写 `Context` 字段：priority/override_priority/penalty/tag），默认不启用
  - 关键决策：tagger 签名带 `&kvrpcpb::Context`，避免额外拷贝并保证能读到 replica-read/stale-read 的 request_source 补丁；提供 `override_priority_if_unset` helper 对齐 client-go “仅在未设置时注入”语义
  - 测试：新增 UT 覆盖 tagger 优先级（fixed tag > tagger）、retry request_source 拼接、interceptor 覆盖 priority/penalty/tag、override_priority 规则；`cd new-client-rust && cargo test` 通过
  - 改动文件：`new-client-rust/src/{interceptor.rs,priority.rs,request_context.rs,store/{mod,request}.rs,raw/client.rs,raw/requests.rs,transaction/{client,transaction,snapshot,requests}.rs,request/{plan,plan_builder,read_routing,shard,mod}.rs,lib.rs}`
