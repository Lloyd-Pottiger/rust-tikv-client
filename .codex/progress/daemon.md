# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- integration-tests：补齐“可重复运行”的真实集群集成测试指引/脚手架
  - 步骤：
    - Makefile：提供 `tiup-up/tiup-down/tiup-clean`（一键启动/停止/清理 playground）
    - Makefile：提供 `integration-test-smoke`（cargo test 跑少量 raw/txn）
    - README/doc：补齐对应命令与注意事项

# 待做工作

- observability：补齐 Rust 生态 `tracing`/OpenTelemetry 对接（feature-gate）
  - 步骤：
    - 提供 `tracing` feature：将 `trace::trace_event` 以 span/event 的方式输出（保留现有 hook）
    - 文档：如何配置 trace id / category flags / 与 TiKV-side trace 联动

- bench：增加基准测试与压测指引（避免“猜测性能”）
  - 步骤：
    - 增加 `benches/`（criterion）：keyspace encode/truncate、plan shard/merge 开销（mock 驱动）
    - 文档：如何跑 bench、如何解读、如何做 flamegraph（可选）

# 已完成工作

- txn/retry：`Transaction::get` 遵循 `TransactionOptions::no_resolve_regions`
  - 关键决策：`retry_multi_region` 使用 `retry_options.region_backoff`（而不是固定常量），使 `no_resolve_regions()` 对 read-path 生效
  - 测试：新增单测（region error 第一次失败/第二次成功，`no_resolve_regions` 不重试且不触发 drop panic）
  - 文件：`new-client-rust/src/transaction/transaction.rs`

- raw/scan：修复 `RawClient::{batch_scan,batch_scan_keys}` 的 `each_limit` 语义 + scan backoff 一致性
  - 关键决策：batch_scan 语义收敛为“对每个 range 调用 scan/scan_keys 并按输入顺序拼接”；raw scan 使用 `RawClient::with_backoff` 的 backoff（而不是固定常量）
  - 测试：新增单测（跨 region range + 多 range，总返回条数 <= `each_limit * ranges`）
  - 文件：`new-client-rust/src/raw/client.rs`

- review：复盘当前实现状态并建立下一阶段工程任务
  - 验证：`make -C new-client-rust check`；`cargo test`（含 `--no-default-features`）通过
  - 发现：`Transaction::get` 未使用 `retry_options.region_backoff`；raw `batch_scan(each_limit)` 语义不符合预期（按 region 分片限流）
  - 文件：`.codex/progress/daemon.md`

- core：new-client-rust 基线 + client-go(v2) 关键能力对齐（Raw/Txn/PD/RegionCache/Plan）
  - 关键决策：对外 API Rust-idiomatic（显式 `Config/Options`）；低层能力用 `request::PlanBuilder` + typed kvproto request 组合，不复刻 Go tikvrpc mega-wrapper；协议侧覆盖 async-commit/1PC/pipelined/local-latches/replica+stale/read-routing 等
  - 测试：单测/性质测试齐全；集成测试框架已具备（feature `integration-tests`，需真实集群）
  - 文件：`new-client-rust/src/{raw/*,transaction/*,request/*,pd/*,store/*,region_cache.rs,replica_read.rs,request_context.rs}`，`.codex/progress/{parity-checklist.md,parity-map.md,gap-analysis.md,client-go-api-inventory.md}`，`new-client-rust/doc/client-go-v2-parity-roadmap.md`

- robustness：hardening + safety（去 panic 化 + unsafe 合同化）
  - 关键决策：锁中毒/字段缺失/配置异常不再 panic（返回 `Error`）；Backoff jitter 构造参数 clamp；timestamp helpers 改为 `Result`；补齐 `internal_err!` 宏递归作用域；unsafe blocks 统一补齐 `// SAFETY: ...` 并增加边界单测
  - 文件：`new-client-rust/src/{common/{errors,security}.rs,backoff.rs,timestamp.rs,pd/retry.rs,request/{keyspace,plan,shard}.rs,transaction/{buffer,requests,transaction}.rs,kv/{key,codec}.rs,compat.rs,region_cache.rs}`，`new-client-rust/Cargo.toml`

- ops：docs + CI（架构文档 + 基础流水线）
  - 产出：`new-client-rust/doc/architecture.md`；README/crate docs 修正旧引用并加入口；`.github/workflows/new-client-rust.yml`（fmt/test/clippy + feature matrix + integration-tests compile-only）
