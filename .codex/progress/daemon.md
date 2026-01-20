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

- tests/parity/client-go：client-go tests 迁移闭环（raw/txn/request/config/safe-ts），补齐 rawkv store replace StoreNotMatch->invalidate->leader switch 单测
  - 关键：store_not_match 无 backoff 也必须 invalidate region+store cache 并刷新 leader 后重试；stale-read DataIsNotReady wait+retry 等
  - 文件：`src/request/plan.rs`，`src/raw/client.rs`，`src/config.rs`，`src/store/safe_ts.rs`，`tests/{integration_tests.rs,failpoint_tests.rs}`，`.codex/progress/{client-go-tests-file-map.md,client-go-tests-port.md,daemon.md}`
  - 验证：`cargo test test_retryable_multi_region_store_not_match_switches_leader_after_invalidate --features test-util`；`make all`

- transport/store：实现 BatchCommands stream + health feedback 等价语义
  - 关键：stream prime `Empty(request_id=0)`；request_id 从 1 起；recv 按 request_id 解复用/乱序；断线重连；timeout -> DeadlineExceeded
  - 文件：`src/store/{batch_commands.rs,request.rs,client.rs,health.rs}`，`tests/integration_tests.rs`，`Cargo.toml`，`Cargo.lock`
  - 验证：`make all`

- parity/review-backlog：复核 `.codex/progress/parity-checklist.md` 并修正 raw/txn/pd/region_cache/store 的 Rust/Tests 映射
  - 关键：补充 rawkv/kv flags/pd/region_cache/store 的单测/集成测引用，便于后续定位覆盖点
  - 文件：`.codex/progress/parity-checklist.md`，`.codex/progress/daemon.md`
  - 验证：`make all`

- tests/port/internal-client-async：收敛 Go `internal/client/{client_test,client_async_test}.go` 可迁移语义映射（batch stream 关键路径 + KvRpcClient fallback）
  - 关键：batch cmd unimplemented（如 Empty）必须 fallback unary；其余 conn-pool/forward/metadata/trace 等 Go-only 细节 N/A
  - 文件：`src/store/client.rs`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/daemon.md`
  - 验证：`make check` + `make unit-test` + `make all`

- tests/port/integration-mock-scan-raw：收敛 Go mockstore 集成用例（raw/txn scan）到 Rust real-cluster E2E 覆盖
  - 关键：mockstore harness N/A，但 raw CRUD/batch/scan + txn scan/reverse 多 region 语义由 Rust E2E + unit tests 覆盖
  - 文件：`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-integration-tests-port.md`，`.codex/progress/daemon.md`
  - 验证：`make all`

- quality/coverage-80：验证 `make coverage` lines>=80%（生成 html 报告）
  - 关键：当前覆盖率阈值通过（`COVERAGE_FAIL_UNDER=80`）；报告输出到 `target/llvm-cov/html/index.html`
  - 文件：`.codex/progress/daemon.md`
  - 验证：`make coverage`
