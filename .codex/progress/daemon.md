# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- Config parity：将 async-commit safe window 做成可配置项（对齐 client-go 默认 2s）
  - 计划：
    - `TransactionOptions` 新增 `async_commit_safe_window`（默认 2s）
    - 2PC `max_commit_ts` 计算使用该值；补齐单测断言
    - checklist 回填对应条目并补文档备注

# 待做工作

- Parity checklist 回填（继续）：补齐 `tikv` / `tikvrpc` 其余 entrypoints（标注 capability-only vs public）
  - 计划：
    - 优先 `tikvrpc/interceptor` 与 `ResourceGroupTagger` 周边；其余 `tikvrpc::{Request,CmdType,...}` 先给出 Rust 侧替代策略并标注
    - `tikv` 包中明显为调试/探针/Go 可见性产物的符号，按 scope policy 标注为 capability-only 或 out-of-scope

- tikvrpc public wrapper：决定是否需要对外暴露 “按命令构造请求/响应” 的 Rust 层（或坚持 `request::PlanBuilder` 作为低层 API）
  - 计划：
    - 明确最小 public surface（不引入巨型 enum/trait 层级）
    - 为 interceptor/metrics/trace 预留稳定 hook（label + context）

- metrics/trace：补齐 feature-gate 策略与最小 public API（对齐 client-go 导出包语义）
  - 计划：
    - prometheus/tracing 依赖改为可选 feature
    - 先对齐核心 counters/histograms（可渐进补齐）

- Protocol TODO：lite resolve lock / lock resolver backoff / retry 参数校准
  - 计划：
    - 逐 TODO/FIXME 立项（从 `.codex/progress/gap-analysis.md` + `rg TODO|FIXME new-client-rust/src`）
    - 每项补单测与行为对齐说明

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
