# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- trace：补齐 trace package ctx helper（按 checklist 渐进）
  - 计划：
    - 明确 `ContextWithTraceID/TraceIDFromContext` 在 Rust 的映射（PlanBuilder / out-of-scope）
    - 视需要增加兼容 helper（不引入 Go-style context）

# 待做工作

# 已完成工作

- new-client-rust 基线 + API 轮廓：Raw/Txn 客户端、PD/region cache、request plan/PlanBuilder、Keyspace、Error/RequestContext、proto/gen（可编译可测试）
  - 关键决策：以 `client-rust` 为起点迁移；对外 API Rust-idiomatic（`Config`/`TransactionOptions`/`RetryOptions` 显式配置，替代 Go 全局 config/ctx）
  - 文件：`new-client-rust/src/{lib.rs,config.rs,raw/*,transaction/*,request/*,store/*,pd/*,region_cache.rs,request_context.rs,common/*}` + `new-client-rust/src/generated/*`

- Txn/协议对齐：async-commit/1PC、pipelined txn + local latches、replica/stale read、assertion/lock options、resolve-lock-lite + retry/backoff 校准、resource control tagger/interceptor
  - 关键决策：pipelined flush/resolve-lock 固定 request_source；stale-read meet-lock fallback leader；prewrite-only 不进入 commit/rollback；region retry attempt>0 时禁用 resolve-lock-lite
  - 测试：覆盖 flush/rollback/latch、replica/stale routing、assertion/lock options、lite txn_size、lock resolver retry/backoff、resource control 透传

- 横切能力 + parity artifacts：interceptor chain(wrap)/mock、trace hooks + PlanBuilder/RequestContext 注入、metrics(feature-gate)+最小 API、checklist scope policy + inventory 工具
  - 关键决策：Prometheus 可选依赖，禁用 `prometheus` 时 stats no-op；metrics 不 1:1 暴露 Go 的 label/handle（统一标注 out-of-scope），仅保留 `metrics::{register,gather_as_text}`
  - 文件：`new-client-rust/src/{interceptor.rs,trace.rs,metrics.rs,stats.rs,request/plan.rs,request/plan_builder.rs,request_context.rs}`，`.codex/progress/{parity-checklist.md,parity-map.md,gap-analysis.md,client-go-api-inventory.md}`，`tools/client-go-api-inventory/main.go`
  - 测试：`new-client-rust/src/metrics.rs (gather_contains_core_metrics)` 先打点再 gather（避免空 vec 不输出导致 flaky），`--no-default-features` 可编译可测试
