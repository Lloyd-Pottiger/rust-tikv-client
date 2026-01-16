# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- （无）

# 待做工作

- stale-read：补齐“安全时间戳”获取 API（避免用户手写 PD RPC）
  - 步骤：
    - `TransactionClient` 增加 `get_min_ts`/`stale_read_timestamp`（调用 PD GetMinTs）
    - 增加单测/集成测：stale-read snapshot 使用该 ts 必须稳定通过
    - 文档：迁移指南补齐 stale-read 的正确用法

- correctness：修复 pessimistic snapshot 不做 lock 检查（tests 里已有 TODO #235）
  - 步骤：
    - 明确语义：pessimistic snapshot/read-path 是否应 resolve/check locks
    - 补齐实现与单测（对齐 optimistic 行为或按 client-go 语义）
    - 集成测：复现并防回归（锁存在时读/写行为）

# 已完成工作

- integration-tests：补齐关键协议路径的集成测试覆盖（真实 TiKV/PD）
  - 产出：新增 txn 关键路径用例（1PC / pipelined flush / replica read / stale read）；pipelined 增加 test-only `min_flush_keys_for_test`（feature `integration-tests` 下可用）以便小事务触发 FlushRequest；smoke suite 覆盖 1PC/pipelined/replica read（stale read 用例保留但不纳入 smoke，避免耗时）
  - 验证：`make -C new-client-rust check`；TiUP playground 跑 `make -C new-client-rust integration-test-smoke`；手动跑 `txn_stale_read_snapshot_get` 通过
  - 文件：`new-client-rust/src/transaction/transaction.rs`，`new-client-rust/tests/integration_tests.rs`，`new-client-rust/Makefile`，`new-client-rust/doc/client-go-v2-migration.md`

- docs：补齐 “client-go v2 → new-client-rust” 迁移指南（Rust public API 视角）
  - 产出：迁移文档（核心入口/配置/观测/低层 request plan）；明确 capability-only/out-of-scope 的差异与替代方式
  - 文件：`new-client-rust/doc/client-go-v2-migration.md`，`new-client-rust/README.md`

- ci：CI 增加 `test-util` bench 编译覆盖（避免 plan bench 漂移）
  - 产出：workflow 增加 `cargo check --features test-util --bench plan`，保证 plan bench 变更能在 CI 里被编译捕获
  - 文件：`.github/workflows/new-client-rust.yml`

- bench：增加基准测试与压测指引（避免“猜测性能”）
  - 产出：criterion microbench（keyspace encode/truncate；plan shard/dispatch/merge，mock 驱动、无需 TiKV 集群）；`doc/bench.md`（运行方式 + 可选 flamegraph）；README 增加入口
  - 关键决策：引入 feature `test-util` 暴露 mock 给 bench 使用，同时保持生产构建不编译 mock
  - 文件：`new-client-rust/benches/{keyspace,plan}.rs`，`new-client-rust/doc/bench.md`，`new-client-rust/Cargo.toml`，`new-client-rust/src/{lib,mock}.rs`，`new-client-rust/README.md`

- observability：提供 `tracing` feature，将 client-side trace events 输出到 tracing
  - 关键决策：保留现有 hook API；`enable_tracing_events()` 在不覆盖用户 sink 的前提下追加 tracing sink；默认无 filter 时启用全部 category
  - 文档：README 增加启用方式；`src/trace.rs` 增加 feature 说明；CI 增加 `--features tracing --no-run` 编译覆盖
  - 文件：`new-client-rust/src/trace.rs`，`new-client-rust/Cargo.toml`，`new-client-rust/README.md`，`.github/workflows/new-client-rust.yml`

- integration-tests：补齐可重复运行的 TiUP playground 脚手架 + smoke suite
  - 产出：`make tiup-up/tiup-down/tiup-clean`（后台启动 + 日志落盘）；`make integration-test-smoke`（cargo test 跑 txn/raw 各 1 个）
  - 文档：README 增加一键命令（启动/停止/清理 + smoke）
  - 文件：`new-client-rust/Makefile`，`new-client-rust/README.md`

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
