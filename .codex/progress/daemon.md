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

- tests/parity-mapping+backoffer+util：完成 client-go tests 迁移/映射维护闭环（101 `_test.go` 逐文件清单；integration-tests 映射；parity-checklist 对齐；清零 `partial`）
  - 关键：新增 `src/backoffer.rs`（client-go Backoffer：maxSleep/excludedSleep/longestSleep/clone+fork/update + MayBackoffForRegionError）+ 单测；补齐 util parity：`src/util/request_source.rs`（Build/GetRequestSource）与 `src/util/rate_limit.rs`（token limiter）；对 mocktikv/BatchCommands/forwarding 等不可移植用例统一标注 N/A，但在 notes 指向 Rust 可迁移语义覆盖点
  - 验证：`cargo test` + `cargo test --features integration-tests --no-run`
  - 文件：`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-tests-port.md`，`.codex/progress/client-go-integration-tests-port.md`，`.codex/progress/parity-checklist.md`，`src/backoffer.rs`，`src/util/request_source.rs`，`src/util/rate_limit.rs`

- request+pd/core-suite：迁移 client-go 核心可迁移单测语义（region/lock/oracle/keyspace/gc-time/resource-control + pd kv_client conn 缓存/dial 去重 + replica selector fast-retry/pending-backoff）
  - 关键：resolved lock cache 命中不触发 secondary-check；EpochNotMatch(empty CurrentRegions)->backoff；ServerIsBusy replica fast-retry + pending-backoff；PdRpcClient per-address kv_client cache + OnceCell 并发 dial 去重
  - 验证：`cargo test`
  - 文件：`src/request/*`，`src/region_cache.rs`，`src/transaction/lock.rs`，`src/pd/client.rs`，`src/timestamp/*`，`src/util/gc_time.rs`

- txn/options：补齐 ReturnCommitTS + commit-wait TSO（含错误类型与单测），并确保 integration-tests feature 可编译
  - 关键：新增 `Transaction`/`Snapshot` *with_options API（返回 `ValueEntry`）；commit 使用 `fetch_commit_timestamp()` 轮询等待 commit_ts>=tso（timeout）；新增 `Error::CommitTsLag`
  - 验证：`cargo test` + `cargo test --features integration-tests --no-run` + `cargo clippy`
  - 文件：`src/transaction/transaction.rs`，`src/transaction/snapshot.rs`，`src/kv/kvpair.rs`，`src/common/errors.rs`，`tests/integration_tests.rs`
