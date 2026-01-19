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

- build/make-all：修复 Makefile 丢失的 `all` target（恢复 `make all` 入口）
  - 验证：`make check`
  - 文件：`Makefile`

- makefile/cleanup：移除重复的 `all:` target 声明（保持 Makefile 简洁）
  - 验证：`make check`
  - 文件：`Makefile`

- docs/development：补齐 `doc-test` 工作流说明，并让 `make all` 覆盖 doc tests
  - 决策：`make all` 显式包含 `doc-test`，本地一条命令更接近 CI
  - 验证：`make check`
  - 文件：`Makefile`，`doc/development.md`

- docs/getting-started：更新 getting-started.md（依当前 API/版本），避免示例过时
  - 决策：示例改为可直接运行的 Tokio `main`；同步修正依赖版本和 `RawClient/TransactionClient::new` 签名（移除旧的 `None` 参数）
  - 验证：`make doc-test` / `make check`
  - 文件：`getting-started.md`

- core/parity：Rust TiKV client 对齐 client-go(v2) Public API + 关键能力（Rust-idiomatic）
  - 架构：显式 `Config/TransactionOptions`；低层 `request::PlanBuilder` + typed kvproto；不复刻 Go `tikvrpc` mega-wrapper
  - 覆盖：Raw/Txn/PD/RegionCache/Backoff；2PC/1PC/async-commit/pipelined；replica/stale read；resource control；keyspace；raw checksum
  - 复核：`.codex/progress/parity-checklist.md` 全勾选；`rg TODO|todo!|unimplemented!|FIXME` 无非 generated 命中
  - 文件：`src/*`，`tests/*`，`doc/*`

- infra/devex：可复现验证入口 + CI/Makefile 统一（含 coverage/doc-test）
  - CI：`make check`/`doc`/`unit-test`/集成测（tiup）统一；生成物变更用 `git diff --exit-code` guard
  - coverage：排除 `src/generated/**`；lines fail-under=80；集成 coverage 入口；`cargo-llvm-cov` 版本固定
  - doc：新增 `make doc-test` 并纳入 CI（编译-only）
  - 文件：`Makefile`，`.github/workflows/ci.yml`，`doc/*`，`Cargo.lock`

- quality/tests/docs：质量修复 + 覆盖率提升 + 文档/测试运行时一致性
  - correctness/unsafe：修 raw checksum CRC64；txn heartbeat 降级噪音；移除可替换 unsafe 并用单测约束
  - tests/coverage：补齐 request/plan、raw/client、txn client/snapshot、store client 等单测；TOTAL lines≈87%（排除 generated）
  - docs/runtime：rustdoc 示例 Tokio-only；测试去掉 `futures::executor::*`；生产代码去掉 `futures::prelude::*`
  - 验证：`make check` / `make unit-test` / `make doc` / `make coverage`

- final/review：复核「整体工作目标」达成情况 + 全量验证入口
  - 核对：`.codex/progress/parity-checklist.md` 无未勾选项；`rg TODO|todo!|unimplemented!|FIXME` 仅命中 `src/generated/**`
  - 验证：`make all` / `make coverage` / `make coverage-integration`
  - 备注：`make coverage-integration` 仍可能打印 llvm-cov "mismatched data" warning，但不影响 fail-under/报告产出
  - 文件：`.codex/progress/daemon.md`
