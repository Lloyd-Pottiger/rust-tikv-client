# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 repo 根目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

当 client-go v2 所有功能，所有 Public API 都已经实现，你需要再次仔细阅读所有代码，review 它们的实现原理和细节，确保代码有良好的质量，足够的测试覆盖 80% 以上的代码，关键路径/易出错逻辑一定要有测试覆盖，没有 bug 等。

并且你应该 Port client-go 的所有测试到新的 rust client 实现中，如果测试失败了，你要尝试修复它们。

---

# 正在进行的工作

# 待做工作

- infra/coverage：调查 llvm-cov “mismatched data” warning（尽量降噪/定位原因）
  - 计划：用 `cargo llvm-cov -v`/`--failure-mode`/分目标运行定位；若不可消除则在 `doc/development.md` 解释来源与影响
  - 验证：`make coverage-integration`
  - 文件：`Makefile`，`doc/development.md`

# 已完成工作

- core/parity+quality：完成 client-go(v2) 关键能力与 Public API 对齐（Rust-idiomatic），并补齐关键测试/文档
  - 决策：显式 `Config/TransactionOptions`；低层 `request::PlanBuilder` + typed kvproto（不复刻 Go `tikvrpc` mega-wrapper）
  - 覆盖：Raw/Txn/PD/RegionCache/Backoff；2PC/1PC/async-commit/pipelined；replica/stale read；resource control；keyspace；raw checksum
  - 文件：`src/*`，`tests/*`，`doc/*`

- infra/devex：统一本地/CI 验证入口（Makefile/CI/doc），并固化 coverage/doc-test/workflow
  - 变更：修复/整理 `make all`（新增 `integration-test-if-ready`：PD 可达才跑 integration tests）；coverage 增加 profraw 清理 + 失败正确透传（支持 `COVERAGE_FAIL_UNDER`）；补充 `make check-all-features`；文档同步（development/README/AGENTS）
  - 文件：`Makefile`，`.github/workflows/ci.yml`，`doc/development.md`，`getting-started.md`，`README.md`，`AGENTS.md`，`.gitignore`

- final/review：复核整体目标达成 + 全量验证
  - 核对：`.codex/progress/parity-checklist.md` 无未勾选项；`rg TODO|todo!|unimplemented!|FIXME` 仅命中 `src/generated/**`
  - 验证：`make all` / `make coverage` / `make coverage-integration`（llvm-cov “mismatched data” warning 仍可能出现，但不影响 fail-under/报告）
  - 文件：`.codex/progress/daemon.md`

- style/tests：清理集成测试的 wildcard import
  - 变更：移除 `tests/integration_tests.rs` 的 `use futures::prelude::*;`；改为显式 `futures::future::join_all`
  - 验证：`make check` / `make unit-test`
  - 文件：`tests/integration_tests.rs`
