# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 repo 根目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

当 client-go v2 所有功能，所有 Public API 都已经实现，你需要再次仔细阅读所有代码，review 它们的实现原理和细节，确保代码有良好的质量，足够的测试覆盖 80% 以上的代码，关键路径/易出错逻辑一定要有测试覆盖，没有 bug 等。

并且你应该 Port client-go 的所有测试到新的 rust client 实现中，如果测试失败了，你要尝试修复它们。

---

# 正在进行的工作

- tests/port-client-go-integration-missing-cases：对照 `client-go/integration_tests/*.go`，补齐 Rust `tests/integration_tests.rs` 缺失的 E2E 语义（优先小而关键的）
  - 实施：逐文件 diff；能用现有 public API 覆盖的补集成用例；无法覆盖的明确原因（例如缺少对应 public API）
  - 验证：`cargo test --features integration-tests --no-run`；cluster ready 时跑 `make integration-test-if-ready`
  - 文件：`tests/integration_tests.rs`，`.codex/progress/client-go-integration-tests-port.md`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-tests-port.md`

# 待做工作

# 已完成工作

- tests/port-client-go-core-suite：迁移 client-go 可迁移单测语义（kv/options+ValueEntry，lock resolver cache，backoff/backoffer，keyspace codec，gc time，oracle，core request/retry/resource-control 等）
  - 关键：resolved cache 命中不触发 secondary-check RPC；EpochNotMatch(empty CurrentRegions)->backoff；Keyspace::try_enable 限制 24-bit id；GC time 兼容解析（最多容忍 1 个尾随字段）；oracle(singleflight+lowres cache + local oracle time hook)；Key.next_prefix_key 边界 + region cache/retry fast-path + stale-read metrics + resource-control bypass
  - 验证：`cargo test`
  - 文件：`src/kv/*`，`src/transaction/lock.rs`，`src/request/*`，`src/request/keyspace.rs`，`src/util/gc_time.rs`，`src/pd/*`，`src/timestamp/*`，`src/store/*`，`src/region_cache.rs`，`src/resource_control.rs`

- infra/test-mapping+integration-docs：维护 go tests 覆盖映射 + integration_tests 映射；校验 integration-tests feature gate 可编译
  - 结果：go tests inventory 101 files/294 cases；`cargo test --features integration-tests --no-run` 通过
  - 文件：`.codex/progress/client-go-tests-port.md`，`.codex/progress/client-go-integration-tests-port.md`，`.codex/progress/daemon.md`

- feature/txn-read-return-commit-ts：txn get/batch_get 支持 ReturnCommitTS（need_commit_ts + commit_ts 透传）
  - 关键：新增 `Transaction::get_with_options`/`Snapshot::get_with_options`（返回 `ValueEntry`）与 `Transaction::batch_get_with_options`/`Snapshot::batch_get_with_options`（返回 key->`ValueEntry`）；Get/BatchGet request 支持 `need_commit_ts`；`KvPair.commit_ts` 公开并从 proto 透传；keyspace 下 batch_get 返回 key decode 正常；单测 mock kv 校验 need_commit_ts+commit_ts（含 cached->refetch）
  - 验证：`cargo test`
  - 文件：`src/kv/kvpair.rs`，`src/request/keyspace.rs`，`src/transaction/transaction.rs`，`src/transaction/snapshot.rs`，`src/transaction/buffer.rs`，`src/transaction/requests.rs`，`src/transaction/lowering.rs`，`examples/raw.rs`，`.codex/progress/daemon.md`

- review/overall-goals-audit：复核整体目标与测试/集成用例映射；修复因 `KvPair` API 调整导致的 integration-tests 编译回归
  - 结果：`cargo test` + `cargo test --features integration-tests --no-run` + `cargo clippy` 通过（integration-tests 仅编译，不依赖 cluster）
  - 文件：`tests/integration_tests.rs`，`.codex/progress/daemon.md`

- tests/port-client-go-replica-selector-fast-retry：补齐 replica selector 的 `ServerIsBusy` fast-retry + pending-backoff 可迁移语义
  - 关键：replica-read 模式下 `ServerIsBusy` 优先切换 peer（bounded by non-witness peers），不消耗 backoff；回到该 store 前先应用 pending backoff；其他 keep-peer region errors 仍 pin 同 store
  - 验证：`cargo test`
  - 文件：`src/request/pending_backoff.rs`，`src/request/read_routing.rs`，`src/request/plan.rs`，`src/request/mod.rs`

- infra/update-test-mapping：更新 go tests 覆盖映射（fast-retry/pending-backoff 已补）
  - 文件：`.codex/progress/client-go-tests-port.md`，`.codex/progress/daemon.md`

- infra/full-test-file-mapping：把 client-go 101 个 `_test.go` 文件逐一映射到 Rust 覆盖点（或标注 N/A），形成可持续维护的清单
  - 关键：新增逐文件清单 `.codex/progress/client-go-tests-file-map.md`（101 rows；covered/partial/n-a + notes），并在 `client-go-tests-port.md` 标注其为 source-of-truth
  - 文件：`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-tests-port.md`，`.codex/progress/daemon.md`

- tests/port-client-go-internal-client-suite：处理 `client-go/internal/client/*_test.go`（batch-client/conn-pool/priority-queue 等）在 Rust 侧的等价覆盖或明确 N/A
  - 关键：补齐 Rust 侧“conn-pool 等价语义”——PdRpcClient 的 per-address kv_client cache + 并发 dial 去重（`OnceCell`）；同步更新 go test file map 将 `client_test.go` 标注为 partial（其余 BatchCommands/forwarding 维持 N/A）
  - 验证：`cargo test`
  - 文件：`src/pd/client.rs`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-tests-port.md`，`.codex/progress/daemon.md`
