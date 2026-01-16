# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- （无）

# 待做工作

- docs：补齐 crate-level “how to test/benchmark/tiup” 开发者指南（README/doc 分散，整理到单一入口）
  - 计划：新增 `new-client-rust/doc/development.md`（fmt/clippy/unit/integration/doc/bench/tiup）；README 加链接；CI 不改

- safety：梳理/注释关键 unsafe 合同并加回归单测（仅覆盖当前仍存在的 unsafe）
  - 计划：枚举 `unsafe {}` 点；为每处补齐 `// SAFETY:` 说明（若缺）；挑 1-2 个关键点加单测/断言

# 已完成工作

- hardening：清理 new-client-rust 残留的 `todo!/unimplemented!`（避免隐藏 panic）
  - 关键决策：mock 层用显式 `Ok/Err(Error::Unimplemented)` 替代 panic；keyspace mock 返回 Enabled meta 以便 test-util 可用
  - 改动：MockPdClient/ReplicaReadPdClient/RegionCache test mock 补齐 `update_safepoint/update_leader/load_keyspace/get_store/...` 的返回
  - 文件：`new-client-rust/src/mock.rs`，`new-client-rust/src/region_cache.rs`，`new-client-rust/src/transaction/transaction.rs`

- tests：清零 integration/failpoint 剩余 TODO/FIXME，补齐缺失用例并稳定断言
  - 关键决策：cleanup_locks 的 rollback/region-error 走 MockPd/MockKv 单测覆盖（避免依赖真实集群触发 region error 的不稳定）
  - 改动：`raw_write_million` 恢复 batch_scan(each_limit) 断言并校验等价 `scan`；`txn_pessimistic_delete` 恢复 insert 场景并用非保留 key；`txn_bank_transfer` 增加更稳健的 RetryOptions；移除 failpoint tests TODO；新增 cleanup_locks rollback/region-error 单测
  - 文件：`new-client-rust/tests/integration_tests.rs`，`new-client-rust/tests/failpoint_tests.rs`，`new-client-rust/src/request/plan.rs`
  - 备注：用 tiup playground 实测 `txn_pessimistic_delete/raw_write_million/txn_bank_transfer` 通过

- review：对齐「整体工作目标」vs 当前实现，识别剩余缺口并拆解任务
  - 发现：core/infra/parity 已覆盖；剩余缺口集中在 tests 文件内的 TODO/FIXME（未断言的 batch_scan each_limit、pessimistic delete 的 insert 行为、cleanup_locks async-commit 的 rollback/region-error 覆盖）
  - 决策：优先用确定性的 unit tests 覆盖 rollback/region-error（避免依赖真实集群触发 region error 的不稳定）；integration tests 只保留稳定、端到端语义校验
  - 文件：`.codex/progress/daemon.md`

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

- infra：Makefile 在未安装 `cargo nextest` 时回退到 `cargo test`
  - 关键决策：保持 nextest 优先；无 nextest 时用 `cargo test --workspace` 跑 unit/integration 的等价子集（不引入额外工具依赖）
  - 改动：`unit-test`/`integration-test-{txn,raw}` 增加 nextest 探测与 fallback
  - 文件：`new-client-rust/Makefile`

- docs：rustdoc build 清零 warnings + CI guardrail
  - 关键决策：修复 crate 自身的 rustdoc warnings；kvproto 生成代码的 bare URL 通过局部 allow 隔离
  - 改动：修复 broken intra-doc link / invalid HTML tag / redundant links；Makefile `doc` exclude 正确化；CI 增加 `RUSTDOCFLAGS=-Dwarnings cargo doc --no-deps`
  - 文件：`new-client-rust/src/{trace.rs,raw/client.rs,transaction/client.rs,kv/bound_range.rs,timestamp.rs,proto.rs}`，`new-client-rust/Makefile`，`.github/workflows/new-client-rust.yml`

- hardening：清理禁用的 proptests 模块（避免“死代码”误导）
  - 关键决策：删除长期禁用且依赖真实集群的 proptests，测试覆盖由 unit/integration tests 承担
  - 改动：移除 `#[cfg(test)] mod proptests;` 与 `new-client-rust/src/proptests/*`
  - 文件：`new-client-rust/src/lib.rs`

- docs：刷新 parity artifacts，使其与当前 scope policy/实现一致
  - 关键决策：把“剩余 gap”改为“差异/后续迭代点”，避免误导为功能缺口；结论聚焦 hardening 与 daemon.md
  - 文件：`.codex/progress/parity-map.md`，`.codex/progress/gap-analysis.md`
