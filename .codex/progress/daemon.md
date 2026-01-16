# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- metrics：补齐 metrics package 最小可用集（Init/Register + 核心 counters/histograms）
  - 计划：
    - 提供 `metrics::register()`/`metrics::gather_as_text()` 等稳定 API
    - 渐进补齐关键 backoff/txn 指标（优先 lock/region/rpc）
    - 补单测：feature off 编译 + 指标可 gather

# 待做工作

- Parity checklist：metrics/trace/interceptor 区域回填
  - 计划：
    - `go run ./tools/client-go-api-inventory` 刷新 checklist
    - 逐条标注 public/capability-only/out-of-scope（按 scope policy）

# 已完成工作

- Core crate 基线（new-client-rust）：Raw/Txn 客户端、PD/Region 路由、request plan、Keyspace、基础 Error/RequestContext（可编译可测试）
  - 关键决策：以 `client-rust` 为起点迁移实现；public API 以 Rust 风格组织（`Config`/`TransactionOptions`/`RetryOptions` 显式配置）
  - 主要文件：`new-client-rust/src/{lib.rs,config.rs,raw/*,transaction/*,request/*,store/*,pd/*,region_cache.rs,request_context.rs,common/errors.rs}` + proto/generated

- Txn/读写路径对齐：async commit/1PC、pipelined txn + local latches、replica/stale read、resource control interceptor/tagger
  - 关键决策：pipelined flush/resolve-lock 固定 request_source；latches 仅用于 optimistic non-pipelined；stale-read meet-lock fallback leader
  - 测试：覆盖 flush/rollback/latch、replica/stale routing、resource control/interceptor、request context 透传

- Txn 细节 parity + 工具链：assertion API 与 KeyError→Error 映射、pessimistic lock options、KeyFlags(prewrite-only)、checklist regen merge
  - 关键决策：prewrite-only(`Op_CheckNotExists`) 不进入 commit/rollback；commit primary 必须来自需 commit keys；inventory 工具 regen 不覆盖已回填 checklist
  - 测试：覆盖 assertion/lock options/force-lock、prewrite-only no-commit、primary 重新选择等

- Parity 范围收敛：补齐 scope policy + 刷新 gap（仅保留 still-relevant 能力点）
  - 关键决策：checklist 每条落入 public / capability-only / out-of-scope 三类；Go `testutils` 不纳入 parity 追踪（工具层面剔除）
  - 文件：`.codex/progress/{parity-map.md,gap-analysis.md,client-go-api-inventory.md}`，`tools/client-go-api-inventory/main.go`

- Parity checklist 回填（第 1 批）：`config` / `config-retry` / `rawkv` + `tikvrpc(interceptor)` 关键 hook
  - 关键决策：全局 config/ctx 注入类 API 标注 out-of-scope（Rust 以显式 opts + client builder 取代）
  - 文件：`.codex/progress/parity-checklist.md`

- Config parity：async-commit/1PC `max_commit_ts` safe window 可配置（默认 2s）
  - 关键决策：以 `TransactionOptions::max_commit_ts_safe_window(Duration)` 暴露；`max_commit_ts` 计算改用该值（ms 转换做饱和）
  - 文件：`new-client-rust/src/transaction/transaction.rs`，`.codex/progress/{parity-checklist.md,gap-analysis.md,parity-map.md}`
  - 测试：复用 async-commit fallback 单测覆盖自定义 safe window

- tikvrpc public wrapper：以 `request::PlanBuilder` 作为 Rust 侧低层 API（替代 Go `CmdType/Request/Response`）
  - 关键决策：不引入巨型 enum；提供 PlanBuilder 的 context 注入 + replica/stale read builder，使低层调用具备与 client-go `tikvrpc` 等价的控制面能力
  - 文件：`new-client-rust/src/request/{plan_builder.rs,read_routing.rs}`，`new-client-rust/doc/tikvrpc-public-wrapper.md`
  - 测试：新增 PlanBuilder request_context/interceptor 注入测试

- Parity checklist 回填（第 2 批）：`tikvrpc` entrypoints + `tikv` 核心入口标注
  - 关键决策：`tikvrpc.{CmdType,Request,Response}` 标注 out-of-scope（以 PlanBuilder + typed requests 替代）；`tikv.NewKVStore` 标注 out-of-scope（Rust 不暴露内部 store wiring）
  - 文件：`.codex/progress/parity-checklist.md`

- Protocol TODO（第 1 项）：启用 lite resolve lock（对齐 client-go 的 PrewriteRequest.txn_size 语义）
  - 关键决策：txn_size=“同一 region 内涉及的 key 数”（与 batching 无关）；region error 重试 attempt>0 时强制 txn_size=u64::MAX 以避免意外 resolve-lock-lite
  - 文件：`new-client-rust/src/transaction/requests.rs`
  - 测试：新增 prewrite txn_size + retry fallback + async-commit/1PC/pessimistic 覆盖单测

- Protocol TODO（后续）：lock resolver backoff / retry 参数校准
  - 关键决策：`resolve_lock_with_retry` 对 region error 引入 backoff + 复用 `handle_region_error` 做 cache/leader 更新；store.attempt 透传到 request context
  - 文件：`new-client-rust/src/transaction/lock.rs`
  - 测试：lock resolver 重试单测改用 0ms backoff（避免 sleep 放大测试耗时）

- metrics/trace：补齐 feature-gate 策略与最小 public API（对齐 client-go 导出包语义）
  - 关键决策：Prometheus 作为可选依赖；禁用 `prometheus` feature 时 stats 退化为 no-op；提供最小 `metrics::gather_as_text()` 与 `PlanBuilder` trace 注入入口
  - 文件：`new-client-rust/{Cargo.toml,src/{metrics.rs,trace.rs,stats.rs,lib.rs,request_context.rs,request/plan_builder.rs}}`
  - 测试：补 PlanBuilder trace_id/trace_control_flags 注入断言；`--no-default-features` 可编译可测试

- interceptor：补齐 RPCInterceptorChain/MockInterceptorManager parity
  - 关键决策：chain 引入 `wrap`（onion model）以对齐 client-go 执行顺序；`MockInterceptorManager` 记录 begin/end/log 便于测试
  - 文件：`new-client-rust/src/interceptor.rs`
  - 测试：新增 interceptor chain wrap 顺序单测

- trace：补齐 trace package 最小可用集（category/flags + hooks + RequestContext 注入）
  - 关键决策：提供全局 hook（IsCategoryEnabled/TraceEventFn）；txn 关键路径发出少量 `trace_event`（默认关闭）
  - 文件：`new-client-rust/src/{trace.rs,transaction/{transaction.rs,lock.rs}}`
  - 测试：新增 trace hook 单测；复用 PlanBuilder trace 字段透传断言
