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
# 已完成工作

- tests/port-keyspace-endkey：补齐 apicodec v2 的 keyspace endKey/range 编码边界测试并修正实现
  - 变更：新增 `keyspace_end_prefix`（4-byte prefix carry-increment）；修正空 range end：不再 `keyspace_id+1`，改为 byte-level 进位（覆盖 mode byte 溢出）
  - 覆盖：补 `1<<8-1`/`1<<16-1`/`1<<24-1` endKey；补 v2 key ranges 的空 key/start/end 编码断言
  - 验证：`cargo test`；`make check`
  - 文件：`src/request/keyspace.rs`

- tests/port-unit-more：迁移可独立运行的 client-go 单测（trace flags + keyspace apicodec parse/decode）
  - 决策：Rust crate 未暴露 apicodec decode API，先用 `cfg(test)` 的最小 helper 覆盖语义，避免 `dead_code`/clippy
  - 变更：`TraceControlFlags` 支持 `|`/`|=`；补齐 flags chaining 测试；补 keyspace ParseKeyspaceID/DecodeKey 与 prefixes sorted 测试
  - 验证：`cargo test`；`make check`
  - 文件：`src/trace.rs`，`src/request/keyspace.rs`

- tests/port-client-go：盘点 client-go 全量测试并做 Rust 覆盖映射（out-of-scope 明确化 + 缺口列表）
  - 决策：遵循 parity checklist 的 out-of-scope 约束（Go context/unionstore/mockstore 等不做 1:1）；以“等价语义覆盖”替代文件级对齐
  - 文件：`.codex/progress/client-go-tests-port.md`

- tests/port-unit：补齐可迁移的单元/逻辑测试覆盖（不依赖 TiKV 集群）
  - 变更：增强 read routing/replica read 用例；补 plan 并发 semaphore 用例；补 transaction buffer scan/delete overlay 用例
  - 验证：`cargo test`；`make check`
  - 文件：`src/request/read_routing.rs`，`src/request/plan.rs`，`src/transaction/buffer.rs`

- tests/port-mockstore：对 Go mockstore/unionstore 测试做语义迁移（不做 1:1 mock server）
  - 决策：Go unionstore/memdb/mocktikv 属于实现细节；Rust 用更小的 mock + 关键语义单测替代（聚焦 buffer/plan/routing）
  - 文件：`src/request/plan.rs`，`src/transaction/buffer.rs`

- tests/port-integration：对齐 client-go `integration_tests/` 的关键 E2E 语义并补缺口
  - 变更：新增 raw delete-range E2E（覆盖 `start==end` 空区间与 `\\0` 边界技巧）；补齐 delete-range 空区间为 no-op（避免 TiKV 报 invalid range）
  - 映射：`.codex/progress/client-go-integration-tests-port.md`
  - 验证：`make integration-test-if-ready`
  - 文件：`tests/integration_tests.rs`，`src/raw/client.rs`

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

- infra/coverage：解释 llvm-cov “mismatched data” warning 的来源与影响（降噪）
  - 结论：warning 出现在合并多个 test binary 的 profraw（单个 `--test integration_tests` 不触发）；对本 crate lines 覆盖率阈值/报告无影响
  - 验证：`make coverage-integration`
  - 文件：`doc/development.md`
