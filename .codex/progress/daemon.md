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

- tests/expand-read-routing：继续补齐 replica read selection 单测覆盖（对齐 client-go `replica_selector_test.go` 的关键语义）
  - 范围：`src/request/read_routing.rs`
  - 计划：
    - 补齐 “pending-backoff/avoid-slow-store” 等分支（若 Rust 逻辑缺失则先实现最小等价行为再加测）
    - 覆盖 Mixed/Learner/Witness 组合的稳定选择（deterministic seed）
  - 验证：`cargo test`；`make check`

- tests/expand-store-client：扩展 store/rpc dispatch 行为单测（timeout/context/label + 与 PlanBuilder/Retry 组合）
  - 范围：`src/store/client.rs`，`src/request/plan_builder.rs`
  - 计划：
    - 增加 mock request/response 覆盖 timeout 透传、context 注入顺序、retry attempt 递增语义
    - 覆盖 interceptor 在重试链条中的可观测性（attempt/region/store id）
  - 验证：`cargo test`；`make check`

# 已完成工作

- tests/store-regionstore-apply：补齐 `RegionStore::apply_to_request` 语义单测（request_source/resource_group_tag/interceptors/retry flag）
  - 覆盖：`attempt>0` -> `is_retry_request`；patched request_source 覆盖；tagger 仅在未显式设置 tag 时触发；interceptors 可观测 `RpcContextInfo`（label/attempt/region/store/flags）
  - 验证：`cargo test`
  - 文件：`src/store/mod.rs`，`.codex/progress/daemon.md`

- tests/port-keyspace-encode-request：对齐 apicodec v2 EncodeRequest（key fields prefixing）
  - 覆盖：RawGet key prefix；CommitRequest.keys/primary_key prefix（PrimaryKey 非空场景）
  - 决策：commit request 通过新增 ctor 支持设置 primary_key（保持原 ctor 不破坏兼容）；测试用 mock dispatch 截获 kvproto request
  - 验证：`cargo test`
  - 文件：`src/raw/client.rs`，`src/transaction/transaction.rs`，`src/transaction/lowering.rs`，`src/transaction/requests.rs`，`.codex/progress/daemon.md`

- tests/port-keyspace-bucket-keys：对齐 apicodec v2 DecodeBucketKeys（decode bytes + keyspace range filter）
  - 覆盖：bucket keys memcomparable decode；keyspace range filter；`{}` 边界规范化（prev-prefix/endKey -> `{}`，inside strip prefix）
  - 决策：作为 `cfg(test)` helper 仅做语义覆盖（当前代码路径未消费 buckets）；避免引入对外 API
  - 验证：`cargo test`
  - 文件：`src/request/keyspace.rs`，`.codex/progress/daemon.md`

- tests/port-keyspace-epoch-not-match：对齐 apicodec v2 decodeRegionError(EpochNotMatch)（decode bytes + keyspace range intersect + prefix stripping）
  - 决策：对外不暴露 apicodec API；通过 `TruncateKeyspace` 在 region-error 返回路径做 best-effort decode+裁剪，避免泄露 encoded key/range
  - 覆盖：`EpochNotMatch.current_regions` memcomparable decode；按 keyspace range 裁剪并去 prefix；空交集 region 过滤
  - 验证：`cargo test`
  - 文件：`src/request/keyspace.rs`，`.codex/progress/daemon.md`

- tests/keyspace-apicodec-v2：对齐 keyspace codec v2 的 prefix 语义（encode/decode/error stripping）
  - 覆盖：ParseKeyspaceID/DecodeKey；prefixes sorted；endKey 4-byte carry-increment；decodeKeyError strip prefix；keyspace enabled 的 error path 统一 truncate（避免对外暴露 encoded key）
  - 决策：Rust crate 未暴露 apicodec decode API，用 `cfg(test)` 的最小 helper 覆盖语义；对外以“等价语义覆盖”替代 Go 文件级对齐
  - 验证：`cargo test`；`make check`
  - 文件：`src/request/keyspace.rs`，`src/raw/client.rs`，`src/transaction/client.rs`，`src/transaction/transaction.rs`，`src/trace.rs`

- tests/client-go-port：盘点 client-go 全量测试并做 Rust 覆盖映射 + 迁移可独立单测/关键 E2E 语义
  - 映射：`.codex/progress/client-go-tests-port.md`，`.codex/progress/client-go-integration-tests-port.md`
  - 覆盖：read routing/replica read；plan 并发 semaphore；txn buffer overlay；raw delete-range E2E（含空区间与 `\\0` 边界）
  - 验证：`cargo test`；`make check`；`make integration-test-if-ready`
  - 文件：`src/request/read_routing.rs`，`src/request/plan.rs`，`src/transaction/buffer.rs`，`tests/integration_tests.rs`，`src/raw/client.rs`

- infra/devex+coverage：统一验证入口 + 解释 coverage warning（降噪）
  - 覆盖：`make all`/`integration-test-if-ready`/`check-all-features`；CI/doc 同步；integration tests import 清理；llvm-cov “mismatched data” 来源与影响说明
  - 文件：`Makefile`，`.github/workflows/ci.yml`，`doc/development.md`，`README.md`，`getting-started.md`，`AGENTS.md`，`.gitignore`，`tests/integration_tests.rs`，`.codex/progress/daemon.md`
