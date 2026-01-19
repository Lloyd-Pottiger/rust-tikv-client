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

- tests/port-pd-oracle-validate-read-ts：迁移 PD oracle ValidateReadTS/stale-read read-ts 校验语义（Go `oracle/oracles/pd_test.go`）
  - 计划：补齐 validate_read_ts 相关接口/缓存/interval 调整；先做 unit-test + mock PD
  - 步骤：梳理 Rust `src/pd/*` timestamp/oracle；实现 validate_read_ts API（含不同 source 的隔离/复用）；按 go 用例拆小测试
  - 验证：`cargo test`
  - 文件：`src/pd/*`，`src/timestamp.rs`，`.codex/progress/daemon.md`

# 已完成工作

- tests/core-request-retry-parity：对齐 client-go region/cache/request retry + replica selection 关键语义（region errors 分类、backoff budget、cache invalidate、TTL/inflight、pinned store/slow-store score）
  - 关键：replica-read stale-command 不 backoff 直接切 peer（加 non-witness fast-retry 上限）；unknown region errors -> invalidate region cache + unresolved backoff；KeyNotInRegion resolved 立即重试
  - 覆盖：RegionCache TTL refresh/expired read-through；Plan handle_region_error（NotLeader/ServerIsBusy/MaxTsNotSynced/...）；KvRpcClient dispatch error 透传 + gRPC invalidation
  - 验证：`cargo test`
  - 文件：`src/request/plan.rs`，`src/request/read_routing.rs`，`src/region_cache.rs`，`src/store/client.rs`，`.codex/progress/daemon.md`

- tests/telemetry+misc-parity：补齐 client-go 可迁移杂项语义（metrics/time_detail/interceptors/errors/keyspace/resource-control）
  - 关键：stale-read bytes metrics 在 gRPC dispatch 按 `Context.stale_read` 统计（暂 local-zone）；resourcecontrol bypass: request_source 包含 `internal_others`
  - 覆盖：network stale-read out/in bytes + req counter；RequestInfo write_bytes(prewrite/commit)+store_id；TimeDetail Display；InterceptorChain dedup/flatten；KeyError debug_info redaction；keyspace v2 codec
  - 验证：`cargo test`
  - 文件：`src/stats.rs`，`src/store/request.rs`，`src/request/metrics_collector.rs`，`src/resource_control.rs`，`src/util/time_detail.rs`，`src/interceptor.rs`，`src/common/errors.rs`，`src/request/keyspace.rs`，`.codex/progress/daemon.md`

- infra/mapping+docs：维护 client-go 测试映射与文档/CI 入口
  - 覆盖：`.codex/progress/client-go-*-port.md`/design docs；CI/文档/Makefile；integration tests scaffold
  - 文件：`Makefile`，`.github/workflows/ci.yml`，`doc/*`，`README.md`，`getting-started.md`，`tests/*`，`.codex/progress/*.md`
