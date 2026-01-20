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

# 已完成工作

- tests/port/mockstore-minimal：从 Go mocktikv 用例挑选可迁移子集，落到 Rust `MockKvClient` 单测（不做 1:1 mock server）
  - 完成：补齐 raw 侧可迁移纯逻辑用例：checksum 多 region merge、checksum keyspace 编码、CAS swapped=false/prev_not_exist、batch_get key 排序+按 region 分组、delete_range empty range no-op
  - 关键：新增 `MockKvClient::with_typed_dispatch_hook` 降低 downcast/boxing 噪音，便于批量写 mock 用例
  - 验证：`make unit-test`
  - 文件：`src/mock.rs`，`src/raw/client.rs`

- tests/parity-mapping+backoffer+util：完成 client-go tests 迁移/映射维护闭环（101 `_test.go` 逐文件清单；integration-tests 映射；parity-checklist 对齐；清零 `partial`）
  - 关键：新增 `src/backoffer.rs`（client-go Backoffer：maxSleep/excludedSleep/longestSleep/clone+fork/update + MayBackoffForRegionError）+ 单测；补齐 util/mock parity：`src/util/request_source.rs`（Build/GetRequestSource）、`src/util/rate_limit.rs`（token limiter）、`src/util/async_util.rs`（Callback/RunLoop）、`src/mock/deadlock_detector.rs`（deadlock detector）；对 mocktikv/BatchCommands/forwarding 等不可移植用例统一标注 N/A，但在 notes 指向 Rust 可迁移语义覆盖点
  - 验证：`cargo test` + `cargo test --features integration-tests --no-run`
  - 文件：`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-tests-port.md`，`.codex/progress/client-go-integration-tests-port.md`，`.codex/progress/parity-checklist.md`，`src/backoffer.rs`，`src/util/request_source.rs`，`src/util/rate_limit.rs`，`src/util/async_util.rs`，`src/mock/deadlock_detector.rs`

- request+pd/core-suite：迁移 client-go 核心可迁移单测语义（region/lock/oracle/keyspace/gc-time/resource-control + pd kv_client conn 缓存/dial 去重 + replica selector fast-retry/pending-backoff）
  - 关键：resolved lock cache 命中不触发 secondary-check；EpochNotMatch(empty CurrentRegions)->backoff；ServerIsBusy replica fast-retry + pending-backoff；PdRpcClient per-address kv_client cache + OnceCell 并发 dial 去重
  - 验证：`cargo test`
  - 文件：`src/request/*`，`src/region_cache.rs`，`src/transaction/lock.rs`，`src/pd/client.rs`，`src/timestamp/*`，`src/util/gc_time.rs`

- txn/options：补齐 ReturnCommitTS + commit-wait TSO（含错误类型与单测），并确保 integration-tests feature 可编译
  - 关键：新增 `Transaction`/`Snapshot` *with_options API（返回 `ValueEntry`）；commit 使用 `fetch_commit_timestamp()` 轮询等待 commit_ts>=tso（timeout）；新增 `Error::CommitTsLag`
  - 验证：`cargo test` + `cargo test --features integration-tests --no-run` + `cargo clippy`
  - 文件：`src/transaction/transaction.rs`，`src/transaction/snapshot.rs`，`src/kv/kvpair.rs`，`src/common/errors.rs`，`tests/integration_tests.rs`

- infra/make-all：修复 clippy::all / `-D warnings` 触发点，确保 `make all` 通过（含 `--no-default-features` unit-test）
  - 关键：按 clippy 建议做语义等价重构（enum variant 命名、nonminimal_bool、type_complexity、unnecessary_*）；仅对 `#[cfg(test)]` 且 `not(feature=\"prometheus\")` 路径的测试 helper 加 `#[allow(dead_code)]` 避免 `-D warnings` 误伤
  - 验证：`make all`
  - 文件：`src/backoffer.rs`，`src/util/async_util.rs`，`src/util/gc_time.rs`，`src/transaction/transaction.rs`，`src/store/client.rs`，`src/stats.rs`，`.codex/progress/daemon.md`

- tests/coverage：确认 unit-test 侧行覆盖率满足阈值（>=80%）
  - 关键：使用 `cargo llvm-cov`；报告输出 `target/llvm-cov/html/index.html`
  - 验证：`make coverage`
  - 文件：`.codex/progress/daemon.md`

- transport/batch-commands：引入 BatchCommands stream（multiplex/reconnect/request_id 映射/timeout），解锁 Go batch-client 相关用例迁移
  - 关键：新增 `src/store/batch_commands.rs` + `KvRpcClient` batch/unary 分流；stream 建连前发送 `Empty(request_id=0)` 破除 TiKV/tonic 首包握手死锁；recv 侧按 request_id 解复用并支持乱序响应；stream 断线后可自动重连用于后续请求；超时返回 `DeadlineExceeded("batch commands request timeout")`
  - 测试：新增 in-process tonic server 单测覆盖 prime/乱序/断线重连/health_feedback；`make all`
  - 文件：`Cargo.toml`，`Cargo.lock`，`src/store/batch_commands.rs`，`src/store/client.rs`，`src/store/request.rs`，`src/store/mod.rs`，`src/request/plan.rs`，`src/transaction/transaction.rs`，`src/transaction/lock.rs`

- tests/port/health-feedback：迁移 Go `integration_tests/health_feedback_test.go`（store slow-score / feedback_seq_no）
  - 关键：新增 `src/store/health.rs`（seq_no 单调）+ 解析 `BatchCommandsResponse.health_feedback` 并落盘到 `StoreHealthMap`；补 `RawClient::__test_get_health_feedback/__test_tikv_side_slow_score`（仅 `integration-tests` feature）用于 E2E 断言
  - 测试：`tests/integration_tests.rs` 新增 `raw_get_health_feedback`；`make integration-test-if-ready` + `make all`
  - 文件：`src/store/health.rs`，`src/store/batch_commands.rs`，`src/pd/client.rs`，`src/request/plan_builder.rs`，`src/raw/client.rs`，`tests/integration_tests.rs`
