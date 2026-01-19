# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 repo 根目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

当 client-go v2 所有功能，所有 Public API 都已经实现，你需要再次仔细阅读所有代码，review 它们的实现原理和细节，确保代码有良好的质量，足够的测试覆盖 80% 以上的代码，关键路径/易出错逻辑一定要有测试覆盖，没有 bug 等。

并且你应该 Port client-go 的所有测试到新的 rust client 实现中，如果测试失败了，你要尝试修复它们。

---

# 正在进行的工作

- tests/port-region-cache-inflight-dedup：补齐 region cache 并发 load 语义（对齐 client-go region_cache 的“in-flight singleflight”行为）
  - 计划：并发 `get_region_by_id`/`get_region_by_key` 时只触发一次 PD fetch；waiters 正确唤醒；失败也必须清理 in-flight 标记避免永久挂死
  - 验证：`cargo test`
  - 文件：`src/region_cache.rs`，`.codex/progress/daemon.md`

# 待做工作

- tests/port-region-request-error-retry-semantics：扩展 region_request sender 的 region error retry/backoff 覆盖面（对齐 client-go `internal/locate/region_request*_test.go`）
  - 计划：补齐 StaleCommand/RegionNotFound/StoreNotMatch/EpochNotMatch 的 retry vs backoff 语义（resolved 不消耗 backoff；unresolved 才 backoff）
  - 验证：`cargo test`
  - 文件：`src/request/plan.rs`，`.codex/progress/daemon.md`

# 已完成工作

- tests/port-region-request-and-cache：扩展 region request sender 对 NotLeader 的等价语义覆盖（对齐 client-go `internal/locate/region_request*_test.go`）
  - 关键：NotLeader(leader present) -> `update_leader` + 立即重试；NotLeader(no leader) -> invalidate region + backoff 后重试
  - 决策：resolved region error（`handle_region_error -> Ok(true)`）不消耗 backoff attempt（对齐 client-go：不调用 bo.Backoff）
  - 覆盖：新增 `RetryableMultiRegion` NotLeader with/without leader 行为单测（含 start_paused backoff 断言），补齐 `handle_region_error` NotLeader 单测
  - 验证：`cargo test`
  - 文件：`src/request/plan.rs`，`Cargo.toml`，`.codex/progress/daemon.md`

- tests/port-store-client-behavior-timeout-cancel：修复并覆盖 plan 并发执行的 cancel 行为（避免脱管 tokio task）
  - 关键：`RetryableMultiRegion`/`RetryableAllStores` 用 `FuturesUnordered` 执行并发 shard/store handler，替代 `tokio::spawn + try_join_all`；外层 future drop/abort 会连带取消内层 handler
  - 覆盖：新增 `RetryableAllStores::execute` cancel regression（abort 外层后 notify gate，不应再完成内层计划）
  - 验证：`cargo test`；`make check`
  - 文件：`src/request/plan.rs`，`.codex/progress/daemon.md`

- tests/port-util-misc-time-detail：移植 client-go util.TimeDetail 的字符串格式化语义（映射到 kvproto TimeDetail/TimeDetailV2）
  - 关键：为 `kvrpcpb::TimeDetail`/`TimeDetailV2` 实现 `Display`（只输出非零字段；字段名/顺序对齐 client-go；ms/ns 转换 + 轻量 duration 格式化）
  - 决策：`CompatibleParseGCTime` 在 client-go 内部无引用，Rust 无对应入口 -> out-of-scope（不迁移）
  - 验证：`cargo test`；`make check`
  - 文件：`src/util/time_detail.rs`，`src/util/mod.rs`，`.codex/progress/daemon.md`

- tests/port-interceptor-chain-flatten-dedup：对齐 client-go `RPCInterceptorChain.Link`/`ChainRPCInterceptors` 的 chain 拼接语义
  - 关键：`RpcInterceptorChain::link` 支持递归 flatten 嵌套 chain；dedup by name 时保留最后一个并移动到队尾（执行顺序可预期）
  - 覆盖：新增单测对齐 Go `TestAppendChainedInterceptor`（chain 递归拼接 + duplicated name move-to-end）
  - 验证：`cargo test`；`make check`
  - 文件：`src/interceptor.rs`，`.codex/progress/daemon.md`

- tests/port-error-debug-info-json-redaction：移植 client-go `error/error_test.go` ExtractDebugInfoStrFromKeyErr 等价语义
  - 决策：不扩张 public API；用 `cfg(test)` helper 生成 DebugInfo JSON 串（bytes base64）并用 JSON 结构断言；redaction 开关也仅测试内使用
  - 覆盖：无 debug_info -> 空串；redaction on/off（bytes -> `b"?"` / base64=`Pw==`）
  - 验证：`cargo test`；`make check`
  - 文件：`src/common/errors.rs`，`Cargo.toml`，`Cargo.lock`，`.codex/progress/daemon.md`

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
