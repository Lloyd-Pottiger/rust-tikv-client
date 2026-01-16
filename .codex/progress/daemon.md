# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- txn/gc：校准并注释 `SCAN_LOCK_BATCH_SIZE=1024`（来源 client-go `GCScanLockLimit=ResolvedCacheSize/2`），并评估是否应下沉到 `ResolveLocksOptions` 默认值
  - 计划：
    - 在 `transaction/client.rs` / `transaction/lock.rs` 注释来源与语义（与 client-go 一致）
    - 去重：优先以 `ResolveLocksOptions::default().batch_size` 作为默认值，避免重复常量
    - `cargo test` 验证

# 待做工作

- progress：刷新 `.codex/progress/gap-analysis.md`（resolve-lock-lite、pipelined、resource control 等已完成项）并整理剩余 TODO/FIXME（仅保留真实缺口）
- build：增加 `cargo test --all-features --no-run` 作为 feature 组合编译验证（覆盖 `integration-tests`）

# 已完成工作

- config/kv/util：收敛剩余导出符号（Rust mapping / capability-only / out-of-scope），清空 checklist 未标注项
  - 关键决策：Go `config/kv/util` 大量导出项为实现细节/Go context helpers；Rust 侧以 `Config` + builder/typed API 替代，统一标注 out-of-scope/capability-only
  - 文件：`.codex/progress/parity-checklist.md`，`.codex/progress/daemon.md`

- rawkv：补齐剩余 public API（Scan/ReverseScan/ClusterID）+ option/probe 取舍，并更新 checklist
  - 关键决策：不复刻 Go `RawOption`/functional options；改为 `Config` + chainable builder（`with_cf/with_request_source/...`）；`GetPDClient/Probe` 等保持 out-of-scope
  - 代码：增加 `RawClient::cluster_id()`（初始化时从 PD 缓存），并补齐单测
  - 文件：`new-client-rust/src/{raw/client.rs,pd/{client,retry,cluster}.rs}`，`.codex/progress/parity-checklist.md`，`.codex/progress/daemon.md`

- tikv/tikvrpc/txnkv：public API mapping + out-of-scope（按 checklist 渐进，优先入口与核心类型）
  - 关键决策：`tikvrpc.{CmdType,Request,Response}` 不做 Go 风格 wrapper；以 `request::PlanBuilder`/typed kvproto request 覆盖能力；`txnkv` 不复刻 Go 包结构，统一映射到 `TransactionClient`/`Transaction`
  - 文件：`.codex/progress/parity-checklist.md`，`.codex/progress/daemon.md`
  - 备注：后续继续收敛剩余包（`config/rawkv/kv/util`）的 public vs capability-only vs out-of-scope

- new-client-rust 基线 + API 轮廓：Raw/Txn 客户端、PD/region cache、request plan/PlanBuilder、Keyspace、Error/RequestContext、proto/gen（可编译可测试）
  - 关键决策：以 `client-rust` 为起点迁移；对外 API Rust-idiomatic（`Config`/`TransactionOptions`/`RetryOptions` 显式配置，替代 Go 全局 config/ctx）
  - 文件：`new-client-rust/src/{lib.rs,config.rs,raw/*,transaction/*,request/*,store/*,pd/*,region_cache.rs,request_context.rs,common/*,timestamp.rs}` + `new-client-rust/src/generated/*`，`.codex/progress/parity-checklist.md`
  - 测试：`new-client-rust/src/timestamp.rs (tests)`，`new-client-rust/src/common/errors.rs (undetermined_error_query)`

- Txn/协议对齐：async-commit/1PC、pipelined txn + local latches、replica/stale read、assertion/lock options、resolve-lock-lite + retry/backoff 校准、resource control tagger/interceptor
  - 关键决策：pipelined flush/resolve-lock 固定 request_source；stale-read meet-lock fallback leader；prewrite-only 不进入 commit/rollback；region retry attempt>0 时禁用 resolve-lock-lite
  - 测试：覆盖 flush/rollback/latch、replica/stale routing、assertion/lock options、lite txn_size、lock resolver retry/backoff、resource control 透传

- 横切能力 + parity artifacts：interceptor chain(wrap)/mock、trace hooks + PlanBuilder/RequestContext 注入、metrics(feature-gate)+最小 API、checklist scope policy + inventory 工具
  - 关键决策：trace ctx helper（ContextWithTraceID/TraceIDFromContext/GetTraceControlFlags）不引入 Go-style context，统一映射到 `PlanBuilder::{with_trace_id,with_trace_control}` 并标注 out-of-scope；Prometheus 可选依赖，禁用 `prometheus` 时 stats no-op；metrics 不 1:1 暴露 Go 的 label/handle（统一标注 out-of-scope），仅保留 `metrics::{register,gather_as_text}`
  - 文件：`new-client-rust/src/{interceptor.rs,trace.rs,metrics.rs,stats.rs,request/plan.rs,request/plan_builder.rs,request_context.rs}`，`.codex/progress/{parity-checklist.md,parity-map.md,gap-analysis.md,client-go-api-inventory.md}`，`tools/client-go-api-inventory/main.go`
  - 测试：`new-client-rust/src/metrics.rs (gather_contains_core_metrics)` 先打点再 gather（避免空 vec 不输出导致 flaky），`--no-default-features` 可编译可测试

- 验证与收尾：fmt/clippy + feature 组合（no-default-features）构建/测试
  - 关键决策：clippy 仅做最小修复；递归 handler 保留签名用 allow 标注；同时覆盖 default 与 `--no-default-features` 的 clippy
  - 结果：`cargo fmt --check`，`cargo test`，`cargo test --no-default-features`，`cargo clippy --all-targets`，`cargo clippy --all-targets --no-default-features` 均通过
  - 文件：`new-client-rust/src/{common/errors.rs,interceptor.rs,lib.rs,request/plan.rs,request/plan_builder.rs,transaction/{latch.rs,pipelined.rs,transaction.rs}}`，`.codex/progress/daemon.md`

- txn/cleanup_locks：修复 `ScanLockRequest` range shard（apply_shard 设置 end_key）并补齐跨 region 的单测
  - 关键决策：按 `region_stream_for_range` 的 intersection 设置 per-region `end_key`（避免 ScanLock 跨 region 触发 RegionError）
  - 测试：新增单测覆盖 multi-region range 的 shard 结果（end_key 被截断到 region end）
  - 文件：`new-client-rust/src/transaction/requests.rs`，`.codex/progress/daemon.md`
