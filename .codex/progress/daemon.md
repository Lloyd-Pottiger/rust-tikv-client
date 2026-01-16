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

- core：new-client-rust 基线 + client-go(v2) 能力对齐（Raw/Txn/PD/RegionCache/Plan）
  - 关键决策：Rust public API 走显式 `Config/Options`；低层以 `request::PlanBuilder` + typed kvproto 组合；不复刻 Go tikvrpc mega-wrapper
  - 覆盖：2PC/async-commit/1PC/pipelined/local-latches；replica+stale read 路由；resource control/trace/metrics hooks
  - 文件：`new-client-rust/src/{raw,transaction,request,pd,store}/*`，`new-client-rust/src/{region_cache.rs,replica_read.rs,request_context.rs}`
  - 跟踪：`.codex/progress/{parity-checklist.md,parity-map.md,gap-analysis.md,client-go-api-inventory.md}`，`new-client-rust/doc/client-go-v2-parity-roadmap.md`

- infra：docs + CI + playground harness + benchmarks
  - 产出：`doc/architecture.md` / `doc/client-go-v2-migration.md` / `doc/bench.md`；Makefile tiup-up/down + integration smoke；CI feature 编译覆盖（含 tracing + plan bench）
  - 文件：`new-client-rust/doc/{architecture,client-go-v2-migration,bench}.md`，`new-client-rust/README.md`，`new-client-rust/Makefile`，`.github/workflows/new-client-rust.yml`，`new-client-rust/benches/*`，`new-client-rust/Cargo.toml`

- correctness：hardening + bugfix + tests（避免“猜测正确性/性能”）
  - hardening：去 panic 化、Backoff/TS helpers 校验、unsafe 合同化；补齐关键单测
  - bugfix：txn get retry 遵循 `no_resolve_regions`；raw batch_scan each_limit 语义 + backoff 一致；stale-read 增加安全时间戳 API `TransactionClient::current_min_timestamp()`（PD GetMinTs）
  - tests：补齐 txn 协议关键路径集成测（1PC/pipelined/replica/stale），并用 GetMinTs 等待 safe-ts 使 stale-read 用例稳定
  - 文件：`new-client-rust/src/{transaction/transaction.rs,raw/client.rs,trace.rs}`，`new-client-rust/src/pd/{cluster.rs,retry.rs,client.rs}`，`new-client-rust/src/transaction/client.rs`，`new-client-rust/src/region_cache.rs`，`new-client-rust/tests/integration_tests.rs`

- correctness：pessimistic snapshot 读路径 lock-check（移除 tests TODO #235）
  - 关键决策：用 failpoint `after-prewrite` 构造 prewrite lock；snapshot 侧用 `no_resolve_locks()` 断言“会报锁而不是读旧值”
  - 改动：`txn_crud` snapshot 改为 `TransactionOptions::new_pessimistic()`；新增 `txn_pessimistic_snapshot_checks_locks`
  - 文件：`new-client-rust/tests/integration_tests.rs`
