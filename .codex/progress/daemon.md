# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- （无）

# 待做工作

- （无）

# 已完成工作

- cleanup：清理剩余非 generated TODO/FIXME（LockResolver pd_client、PD members 字段校验），并补齐测试
  - 关键决策：`LockResolver` 泛型化并持有 `pd_client/keyspace`（避免 trait object 不可用）；PD 连接阶段显式校验 members/header/leader 关键字段避免 unwrap/panic
  - 测试：新增 `LockResolver::cleanup_locks` 单测；新增 PD members response 校验单测；修复 `DiskFullOpt` 断言的 `.into()` 推导歧义
  - 文件：`new-client-rust/src/{transaction/lock.rs,request/plan.rs,pd/cluster.rs,raw/client.rs,transaction/transaction.rs}`，`.codex/progress/daemon.md`
  - 验证：`cargo fmt`，`cargo test`，`cargo clippy --all-targets`

- new-client-rust 基线 + parity artifacts：Raw/Txn 轮廓、Plan/PlanBuilder、Keyspace、Error/RequestContext、proto/gen；并建立 scope policy/out-of-scope 口径与进度产物
  - 关键决策：对外 API Rust-idiomatic（显式 `Config/Options`）；metrics/trace/interceptor 以能力导向映射，不复刻 Go context/helpers
  - 文件：`new-client-rust/src/{lib.rs,config.rs,raw/*,transaction/*,request/*,store/*,pd/*,timestamp.rs,metrics.rs,trace.rs,interceptor.rs,stats.rs,request_context.rs}`，`.codex/progress/{parity-checklist.md,parity-map.md,gap-analysis.md,client-go-api-inventory.md,daemon.md}`，`tools/client-go-api-inventory/main.go`
  - 验证：default/`--no-default-features` 的 fmt/test/clippy；`cargo test --all-features --no-run`

- txn：协议对齐 + correctness 修复（async-commit/1PC、pipelined+local latches、replica/stale、assertion/lock options、resolve-lock-lite/backoff 校准）
  - 关键决策：pipelined 固定 request_source；stale-read meet-lock fallback leader；region retry attempt>0 禁用 resolve-lock-lite；ScanLock shard 按 region intersection 截断 end_key；ScanLock batch_size 对齐 client-go
  - 文件：`new-client-rust/src/transaction/*`，`.codex/progress/daemon.md`

- region_cache：TTL/失效策略 + 锁粒度（移除 TODO/FIXME，避免全局锁瓶颈）
  - 关键决策：region entry 持有原子 TTL（idle TTL，近过期才 refresh，含 jitter）；`get_region_by_id` in-flight 标记从 region index 锁拆出；empty end_key 视为 +inf 修复 overlap；TTL 通过 `Config::{region_cache_ttl,region_cache_ttl_jitter}` 可控
  - 测试：新增 overlap(tail region) 与强制 TTL 过期 reload 用例；`cargo test` 通过
  - 文件：`new-client-rust/src/{region_cache.rs,config.rs,pd/client.rs}`，`.codex/progress/daemon.md`

- request/plan：清理 TODO（unknown region error 日志、EpochNotMatch 立即 retry、CleanupLocks ResolveLockError backoff）并补齐单测
  - 关键决策：EpochNotMatch 仅在“本地 epoch ahead of TiKV”时触发 backoff；CleanupLocks 遇到 `ResolveLockError` 不推进 next_batch，sleep 后重扫当前 range
  - 测试：新增单测覆盖 CleanupLocks 的 backoff-retry 语义；`cargo test` 通过
  - 文件：`new-client-rust/src/request/plan.rs`，`.codex/progress/daemon.md`

- transaction/requests：收敛 ScanLock next-batch 的 end_key 语义，避免尾部空扫/越界
  - 关键决策：为 `NextBatch` 增加 `end_key()`，由 CleanupLocks 在计算 next_range 时过滤 `start_key >= end_key`；`ScanLockResponse::has_next_batch()` 仅负责生成 next start_key
  - 测试：新增单测覆盖 end_key 边界（next start == end_key 时不再发起下一次 ScanLock）
  - 文件：`new-client-rust/src/{request/shard.rs,transaction/requests.rs,request/plan.rs}`，`.codex/progress/daemon.md`
