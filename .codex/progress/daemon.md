# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

# 待做工作

- Replica Read / Stale Read / Follower Read（读路径对齐到 client-go v2）
  - 计划：
    - 梳理 `client-go/kv.ReplicaReadType` + `stale_read` / `replica_read` 在 `kvrpcpb.Context` 的写入点
    - 设计 Rust 侧 public API（尽量小的 surface）并实现 region peer 选择/回退策略
    - UT 覆盖：leader/follower/learner/mixed/prefer-leader 的 peer 选择与 context 字段写入

- Resource Control（扩展项：penalty/override_priority + tagger/interceptor）
  - 计划：
    - 对齐 `client-go/tikvrpc.ResourceGroupTagger` 与 TiDB 侧 tagger 逻辑
    - Rust 侧提供可插拔 hook（发送前可改写 context/tag），默认关闭
    - UT：hook 覆盖与优先级规则

- 事务协议补齐：pipelined txn / txn local latches（按 client-go v2 行为）
  - 计划：
    - 对齐 `client-go/txnkv/transaction` 下 pipelined 相关实现与公开选项
    - 评估 Rust 侧实现方式（lock table/latch 抽象 + 任务驱动），并先做最小可用版本
    - UT：并发冲突/写冲突/回滚路径

# 已完成工作

- 梳理 client-go(v2) Public API + 功能清单（以代码为准）
  - 关键产物：`.codex/progress/client-go-api-inventory.md`（导出符号 name-only 清单）、`.codex/progress/parity-map.md`（Go→Rust 高层模块映射草案）
  - 决策：用源码自动提取导出符号，避免人工漏项；方法目前 receiver/signature 未精确归属，后续需要迭代到“签名级”对齐
  - 改动文件：`.codex/progress/client-go-api-inventory.md`、`.codex/progress/parity-map.md`、`.codex/progress/daemon.md`、`task_plan.md`、`findings.md`、`progress.md`

- 梳理 client-rust 现有能力与缺口（对齐到 client-go v2）
  - 关键产物：`.codex/progress/gap-analysis.md`（能力域缺口 + 已知不完整点），并在 `.codex/progress/parity-map.md` 形成复用方向
  - 关键决策：`new-client-rust` 以迁移/复用 `client-rust` 为起点（PD/region cache/request plan/raw/txn），再补齐 client-go(v2) 缺口
  - 注意：async commit 在 `client-rust` 里存在 `FIXME set max_commit_ts and min_commit_ts`，后续需要对齐协议语义与 client-go 实现
  - 改动文件：`.codex/progress/gap-analysis.md`、`.codex/progress/daemon.md`、`findings.md`、`progress.md`

- 初始化 new-client-rust Rust 工程骨架（可编译/可测试）
  - 关键决策：以 `client-rust` 代码为起点迁移到 `new-client-rust/`（保留 workspace + 生成 proto 方式）
  - 验证：`new-client-rust/` 下 `cargo test` 通过（基础单测可运行）
  - 改动文件：`new-client-rust/**`（从 `client-rust` 复制，不含 `.git`）、`.codex/progress/daemon.md`、`findings.md`、`progress.md`

- 实现 RawKV Checksum（对齐 client-go `rawkv.Client.Checksum` 能力）
  - 新增：`RawClient::checksum` + `RawChecksum`（crc64_xor/total_kvs/total_bytes），按 region 分片并聚合（xor + sum）
  - 增补：`RawChecksum{Request,Response}` 的 dispatch、region/key error 抽取、`HasLocks` 空实现、merge 逻辑
  - 测试：新增单测覆盖聚合；`cargo test` 通过
  - 改动文件：`new-client-rust/src/raw/client.rs`、`new-client-rust/src/raw/mod.rs`、`new-client-rust/src/raw/requests.rs`、`new-client-rust/src/raw/lowering.rs`、`new-client-rust/src/store/request.rs`、`new-client-rust/src/store/errors.rs`、`new-client-rust/src/lib.rs`、`.codex/progress/daemon.md`

- 明确 new-client-rust 的对外 API 规划（按 parity-map 分阶段落地）
  - 约定：维持“最小 public surface”（Raw/Txn/Config/Key types），其余能力先作为实现细节，按 parity 逐步对外暴露
  - 产物：`new-client-rust/doc/client-go-v2-parity-roadmap.md` + 更新 `new-client-rust/README.md`/crate docs 指向 roadmap
  - 改动文件：`new-client-rust/doc/client-go-v2-parity-roadmap.md`、`new-client-rust/README.md`、`new-client-rust/src/lib.rs`、`.codex/progress/daemon.md`

- 基于 kvrpcpb.Context 补齐基础“可观测/资源控制”字段（RawClient）
  - 新增：`RawClient::{with_request_source,with_resource_group_tag}`（clone-style）
  - 实现：通过 `store::Request` trait 在请求发送前写入 `kvrpcpb::Context.{request_source,resource_group_tag}`
  - 测试：新增 UT 校验 dispatch 时 context 字段已设置；`cargo test` 通过
  - 改动文件：`new-client-rust/src/store/request.rs`、`new-client-rust/src/raw/client.rs`、`new-client-rust/src/raw/requests.rs`、`.codex/progress/daemon.md`

- 对齐事务 async commit/1PC 的 `min_commit_ts`/`max_commit_ts` 语义 + `min_commit_ts==0` fallback
  - 实现：
    - `Committer::prewrite` 写入 `PrewriteRequest.{min_commit_ts,max_commit_ts}`；pessimistic async commit 额外修正 `lock_ttl`（参照 TiDB #33641）
    - `Committer::commit` 在 prewrite 返回 `min_commit_ts==0` 时自动 fallback 到普通 2PC（从 PD 获取 commit ts）
  - 测试：新增 UT 覆盖 async commit 成功/ fallback；补齐 failpoint 相关 UT 的 `#[serial]`，避免并发 teardown 互相干扰；`cargo test` 通过
  - 关键决策：`max_commit_ts` safe window 暂按 client-go 默认 2s（常量），后续需要做成可配置项
  - 改动文件：`new-client-rust/src/transaction/transaction.rs`、`.codex/progress/daemon.md`

- `client-go` Public API 对齐迭代到“签名级” parity（自动化产物）
  - 关键产物：
    - `.codex/progress/client-go-api-inventory.md`：按 package 输出 `type/func/var/const/method` 的签名级清单（含 receiver）
    - `.codex/progress/parity-checklist.md`：Go→Rust parity checklist 骨架（可逐项填充 Rust path + Tests）
  - 实现：新增可重复生成的本地工具 `tools/client-go-api-inventory/`（用 `go list -json` + AST 提取，避免手工漏项）
  - 验证：`go run ./tools/client-go-api-inventory` 可重复生成（重复运行内容稳定）
  - 改动文件：`tools/client-go-api-inventory/main.go`、`go.mod`、`.codex/progress/client-go-api-inventory.md`、`.codex/progress/parity-checklist.md`、`.codex/progress/daemon.md`

- Resource Control（RawClient 支持 `resource_group_name` / `ResourceControlContext` 写入）
  - 实现：扩展 `store::Request` trait，统一写入 `kvrpcpb::Context.resource_control_context.resource_group_name`
  - 新增：`RawClient::with_resource_group_name`（clone-style），并在 raw 请求发送前写入 context
  - 测试：扩展 RawClient UT 校验 `RawGetRequest.context.resource_control_context.resource_group_name`；`cargo test` 通过
  - 改动文件：`new-client-rust/src/store/request.rs`、`new-client-rust/src/raw/client.rs`、`new-client-rust/src/raw/requests.rs`、`.codex/progress/daemon.md`

- Resource Control（Txn 路径：贯穿 request_source/resource_group_tag/resource_group_name，含 resolve lock）
  - 实现：
    - 引入 crate 内部 `RequestContext`，TransactionClient/Transaction/2PC/lock resolver 统一复用
    - PlanBuilder 增加 `with_request_context`，ResolveLock/CleanupLocks 内部请求（Cleanup/ResolveLock/CheckTxnStatus…）继承同一 context
    - TransactionClient：new txn / cleanup_locks / unsafe_destroy_range 写入 context
  - 测试：新增 UT 抽样校验 txn get/prewrite/commit + lock resolve 的 context 字段；`cd new-client-rust && cargo test` 通过
  - 改动文件：`new-client-rust/src/request_context.rs`、`new-client-rust/src/request/plan_builder.rs`、`new-client-rust/src/request/plan.rs`、`new-client-rust/src/transaction/{client,transaction,lock}.rs`、`new-client-rust/src/lib.rs`、`.codex/progress/daemon.md`
