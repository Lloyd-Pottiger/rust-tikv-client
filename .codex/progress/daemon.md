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

- tests/port-replica-selector-fast-retry：移植 client-go fast retry/avoid slow store 的等价语义（最小可测覆盖）
  - 关键：PreferLeader retry path 走 score 选点，基于 StoreHealthMap 的 slow 标记避开失败 store；gRPC transport error 触发 mark slow 后可换点重试
  - 覆盖：MockKvClient 第一次打到 replica->gRPC error->标 slow；第二次避开 slow replica；同时断言 `Context.is_retry_request` 与 patched request_source
  - 验证：`cargo test`；`make check`
  - 文件：`src/request/read_routing.rs`，`src/request/plan.rs`，`.codex/progress/daemon.md`

- feat+tests/replica-selection-score：实现最小 score 选择并移植 client-go `TestReplicaSelectorCalculateScore` 的等价语义
  - 关键：引入 score bits（NotSlow/LabelMatches/PreferLeader/NormalPeer/NotAttempted）；Mixed/PreferLeader attempt=0 走 score 选点；同 score 用 seed/attempt 做 deterministic tie-break
  - 覆盖：leader/follower/slow/label match/tryLeader/preferLeader 关键 case；补齐「replicas 全 slow -> Mixed 回落 leader」用例
  - 验证：`cargo test`；`cargo fmt -- --check`
  - 文件：`src/request/read_routing.rs`，`src/store/mod.rs`，`.codex/progress/daemon.md`

- tests/keyspace-apicodec-v2-port：移植 client-go keyspace apicodec v2 关键语义与用例（prefix + region error + encode request）
  - 决策：不暴露 apicodec 对外 API；用 `cfg(test)` helper + `TruncateKeyspace` 做“等价语义覆盖”，避免泄露 encoded key/range；CommitRequest 用新增 ctor 支持 primary_key prefix（不破坏原 ctor）
  - 覆盖：decodeKey/ParseKeyspaceID/endKey carry；EpochNotMatch decode+range intersect+strip；bucket keys decode+range filter+`{}` 边界；RawGet/CommitRequest keys/primary_key prefix
  - 验证：`cargo test`；`make check`
  - 文件：`src/request/keyspace.rs`，`src/raw/client.rs`，`src/transaction/client.rs`，`src/transaction/transaction.rs`，`src/transaction/lowering.rs`，`src/transaction/requests.rs`，`src/trace.rs`，`.codex/progress/daemon.md`

- tests/routing+retry-observability：补齐 read routing/plan/store dispatch 单测（retry attempt/request_source/slow-store）
  - 覆盖：Mixed retry fallback-to-leader；PreferLeader retry 打散；RegionStore::apply_to_request（retry flag/request_source/tagger/interceptors）；Plan retry attempt->Context.is_retry_request；gRPC transport error -> mark store slow(500ms)；PreferLeader attempt=0 避开 slow leader
  - 决策：Backoff::no_backoff 不会触发重试；需要重试的用例用 `Backoff::no_jitter_backoff(0, 0, N)`（0ms sleep + N attempts）
  - 验证：`cargo test`
  - 文件：`src/request/read_routing.rs`，`src/request/plan.rs`，`src/request/plan_builder.rs`，`src/request/store_health.rs`，`src/request/mod.rs`，`src/store/mod.rs`，`.codex/progress/daemon.md`

- infra/test-mapping+replica-read-design：client-go 测试盘点映射 + 验证入口/文档同步 + replica_selector 迁移设计
  - 覆盖：`.codex/progress/client-go-*-port.md` 映射；`make all`/`integration-test-if-ready`/`check-all-features`；CI/doc 同步；integration tests import 清理；replica_selector health/score/fast-retry 集成点设计与拆解
  - 文件：`.codex/progress/client-go-tests-port.md`，`.codex/progress/client-go-integration-tests-port.md`，`.codex/progress/replica-read-health-selection.md`，`Makefile`，`.github/workflows/ci.yml`，`doc/development.md`，`README.md`，`getting-started.md`，`AGENTS.md`，`.gitignore`，`tests/integration_tests.rs`，`.codex/progress/daemon.md`
