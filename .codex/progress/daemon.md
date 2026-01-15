# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- Resource Control（扩展项：penalty/override_priority + tagger/interceptor）
  - 计划：
    - 对齐 `client-go/tikvrpc.ResourceGroupTagger` 与 TiDB 侧 tagger 逻辑（含 retry 的 request_source 拼接规则）
    - Rust 侧提供可插拔 hook：发送前可改写 `kvrpcpb::Context`（priority/override_priority/penalty/tag），默认关闭
    - UT：hook 覆盖 + 优先级/override 规则
  - 验证：`cd new-client-rust && cargo test`

# 待做工作

- 事务协议补齐：pipelined txn / txn local latches（按 client-go v2 行为）
  - 计划：
    - 对齐 `client-go/txnkv/transaction` 下 pipelined 相关实现与公开选项
    - 评估 Rust 侧实现方式（lock table/latch 抽象 + 任务驱动），并先做最小可用版本
    - UT：并发冲突/写冲突/回滚路径

# 已完成工作

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
