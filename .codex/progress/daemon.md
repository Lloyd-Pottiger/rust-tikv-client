# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- 整体目标复审：按最新 `client-go/` 刷新 parity 状态并列出剩余 gap（仅保留 still-relevant 的 public API/能力点）
  - 计划：
    - 基于 `go run ./tools/client-go-api-inventory` 输出核对 `.codex/progress/{client-go-api-inventory,parity-checklist}.md`
    - 给出 Rust 侧 scope policy（哪些坚持能力即可、哪些需要 1:1 public API）
    - 将剩余 gap 拆成小任务，补齐到本文件「待做工作」，并开始最高优先级项

# 待做工作

- Parity 范围收敛：明确哪些 `client-go` public package/符号需要在 Rust 侧暴露（哪些仅保证能力但不做 1:1 API）
  - 计划：
    - 按 package 粗分：core entrypoints / 事务协议 / 工具包（metrics/trace/util）
    - 给出 “Rust-y” 替代策略（例如避免全局 config；builder/显式 opts；feature-gate metrics）
    - 更新 `.codex/progress/parity-map.md` 的 scope policy，并同步修订 checklist（标注/删去 out-of-scope）

- Parity checklist 回填（核心包优先）：`config` / `config/retry` / `rawkv` / `tikv` / `tikvrpc`
  - 计划：
    - 逐包把已实现的 Rust path + Tests 回填到 `.codex/progress/parity-checklist.md`（优先 entrypoints + txn/read/write/options）
    - 对仍缺失但确定需要的 API，拆成可实现的小任务并移到「正在进行的工作」

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
