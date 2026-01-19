# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 repo 根目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

当 client-go v2 所有功能，所有 Public API 都已经实现，你需要再次仔细阅读所有代码，review 它们的实现原理和细节，确保代码有良好的质量，足够的测试覆盖 80% 以上的代码，关键路径/易出错逻辑一定要有测试覆盖，没有 bug 等。

并且你应该 Port client-go 的所有测试到新的 rust client 实现中，如果测试失败了，你要尝试修复它们。

---

# 正在进行的工作

- tests/port-metrics-collector：迁移 locate/network collector 的可迁移指标语义（Go `internal/locate/metrics_collector_test.go`）
  - 计划：对齐 request/response bytes 统计、stale-read metrics；缺失的 prom 指标先用 Rust stats 等价覆盖
  - 步骤：定位 Rust 对应 stats/metrics 模块；补单测验证 counter 增量；必要时加 feature-gated prometheus 集成
  - 验证：`cargo test`
  - 文件：`src/stats/*`，`src/request/*`，`.codex/progress/daemon.md`

# 待做工作

# 已完成工作

- tests/port-store-client-tests：补齐 store/rpc client 并发/错误路径单测（对齐 go `internal/client/*_test.go` 的可迁移部分）
  - 覆盖：KvRpcClient dispatch error 透传；RetryableMultiRegion 遇到 gRPC Status（unavailable/deadline_exceeded）会 invalidate region+store cache 且依赖 backoff budget 重试
  - 验证：`cargo test`
  - 文件：`src/store/client.rs`，`src/request/plan.rs`，`.codex/progress/daemon.md`

- tests/port-region-cache-tests-more：继续迁移 client-go `internal/locate/region_cache_test.go` 缺失语义（重点：invalidate/TTL/并发/region-split/resolve-loop）
  - 覆盖：补 RegionCache TTL check_and_refresh 单测；补 expired TTL 触发 read-through refetch 的集成单测
  - 验证：`cargo test`
  - 文件：`src/region_cache.rs`，`.codex/progress/daemon.md`

- tests/port-read-routing-score-tests：扩展 `ReadRouting`/replica selection 的 score/seed/slow-store 语义覆盖（对齐 go `replica_selector_test.go` 的可迁移部分）
  - 覆盖：补齐 calculate_score（tryLeader/preferLeader/learnerOnly/slow-store）关键 flag 断言；补齐 pinned store override + witness 排除等路径的单测
  - 验证：`cargo test`
  - 文件：`src/request/read_routing.rs`，`.codex/progress/daemon.md`

- tests/port-region-request-replica-selector-errors：迁移/等价覆盖 replica selector 在 region error 下的重试/切 peer 语义（Go `region_request3_test.go`）
  - 关键：ReadRouting 增加 per-region pinned store；replica-read 下 StaleCommand 不 backoff 直接切 peer（并用 non-witness peer 数限制 fast-retry 防止无限递归）
  - 覆盖：replica-read stale-command 切 peer；ServerIsBusy/MaxTsNotSynced/ReadIndexNotReady/ProposalInMergingMode backoff+重试同 store；handle_region_error 对上述错误改为 backoff(不 invalidate)
  - 验证：`cargo test`
  - 文件：`src/request/read_routing.rs`，`src/request/plan.rs`，`.codex/progress/daemon.md`

- tests/port-region-request-unknown-region-error：对齐 unknown region error 的 invalidate/backoff 语义（对齐 client-go `internal/locate/region_request*_test.go`）
  - 关键：unknown region error -> invalidate region cache（不 invalidate store）；归类为 unresolved，走 backoff budget（避免无限递归/stack overflow）
  - 覆盖：handle_region_error 单测 + RetryableMultiRegion（backoff=0 直接返回 RegionError；backoff=1 可重试成功）
  - 验证：`cargo test`
  - 文件：`src/request/plan.rs`，`.codex/progress/daemon.md`

- tests/port-region-request-key-not-in-region：对齐 KeyNotInRegion 的 invalidate/retry 语义（对齐 client-go `internal/locate/region_request*_test.go`）
  - 关键：KeyNotInRegion -> 仅 invalidate region cache（不 invalidate store）+ 立即重试（resolved，不消耗 backoff）
  - 覆盖：handle_region_error 单测 + RetryableMultiRegion 端到端重试不 backoff 断言（backoff=0 也能成功）
  - 验证：`cargo test`
  - 文件：`src/request/plan.rs`，`.codex/progress/daemon.md`

- core/request+cache-retry-parity：对齐 client-go region/cache/request retry 关键语义（plan retry、NotLeader/backoff、region cache inflight）
  - 关键：resolved region errors（NotLeader(with leader)/StoreNotMatch/RegionNotFound/EpochNotMatch when behind）立即重试不消耗 backoff；需要等待的错误（NotLeader(no leader)/StaleCommand/EpochNotMatch when cache ahead）才 backoff；RegionCache `get_region_by_{id,key}` in-flight singleflight（成功/失败都清理并唤醒）
  - 覆盖：新增/扩展 plan/region_cache 单测（tokio start_paused backoff 断言；并发 get_region* 只触发一次 PD fetch）
  - 验证：`cargo test`
  - 文件：`src/request/plan.rs`，`src/region_cache.rs`，`Cargo.toml`，`.codex/progress/daemon.md`

- parity/misc-utils：移植 client-go 杂项语义与单测（time_detail、interceptor chain、error debug_info redaction、keyspace apicodec v2）
  - 关键：TimeDetail Display 输出字段/顺序对齐；InterceptorChain flatten+dedup；KeyError debug_info JSON redaction on/off；keyspace v2 prefix/epoch-not-match decode+range；CommitRequest primary_key prefix ctor
  - 验证：`cargo test`；必要时 `make check`
  - 文件：`src/util/time_detail.rs`，`src/interceptor.rs`，`src/common/errors.rs`，`src/request/keyspace.rs`，`src/raw/client.rs`，`src/transaction/*`，`Cargo.toml`，`Cargo.lock`，`.codex/progress/daemon.md`

- infra/mapping+docs：维护 client-go 测试映射、CI/doc/Makefile 入口与 replica-read 设计拆解
  - 覆盖：`.codex/progress/client-go-*-port.md`/design docs；CI/文档/Makefile；integration tests scaffold
  - 文件：`Makefile`，`.github/workflows/ci.yml`，`doc/development.md`，`README.md`，`getting-started.md`，`tests/integration_tests.rs`，`.codex/progress/*.md`
