# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 repo 根目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

当 client-go v2 所有功能，所有 Public API 都已经实现，你需要再次仔细阅读所有代码，review 它们的实现原理和细节，确保代码有良好的质量，足够的测试覆盖 80% 以上的代码，关键路径/易出错逻辑一定要有测试覆盖，没有 bug 等。

并且你应该 Port client-go 的所有测试到新的 rust client 实现中，如果测试失败了，你要尝试修复它们。

---

# 正在进行的工作

（空）

# 待做工作

（空）

# 已完成工作

- tests/parity/client-go：client-go 可迁移测试语义迁移闭环（101 `_test.go` 归因 + 关键路径补齐）
  - 关键：StoreNotMatch->invalidate cache+刷新 leader 重试；stale-read DataIsNotReady wait+retry；async-commit 二级锁恢复对齐 client-go（仅缺锁时采信 `commit_ts`，否则用 `max(min_commit_ts)`）
  - 文件：`src/{request,raw,transaction,store}/*`，`tests/{integration_tests.rs,failpoint_tests.rs}`，`.codex/progress/{client-go-tests-file-map.md,client-go-tests-port.md,client-go-integration-tests-port.md,parity-checklist.md,daemon.md}`
  - 验证：`make all`；`make coverage`；`make coverage-integration`

- transport/store：实现 BatchCommands stream + health feedback 等价语义
  - 关键：stream prime `Empty(request_id=0)`；request_id 从 1 起；recv 按 request_id 解复用/乱序；断线重连；timeout -> DeadlineExceeded；batch unimplemented fallback unary
  - 文件：`src/store/{batch_commands.rs,request.rs,client.rs,health.rs}`，`tests/integration_tests.rs`
  - 验证：`make all`

- quality/ci：覆盖率/feature matrix 收尾 + 稳定性收敛
  - 关键：CI 增加 `make check-all-features`；`make coverage-integration` 固定 `RUST_MIN_STACK=32MiB`；`raw_get_health_feedback` 不再假设 slow_score 恒为 1；复核 `make check-all-features`/`make coverage`/`make all` 均通过
  - 文件：`Makefile`，`.github/workflows/ci.yml`，`tests/integration_tests.rs`，`.codex/progress/daemon.md`
  - 验证：`make check-all-features`；`make coverage`；`make coverage-integration`；`make all`
