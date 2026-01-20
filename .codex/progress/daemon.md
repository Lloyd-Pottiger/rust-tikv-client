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

- parity/review-backlog：对照 `.codex/progress/parity-checklist.md` 复核未完成项，拆解成可实现的小任务并补回本文件的「待做工作」
  - 步骤：按模块（raw/txn/pd/region_cache/store）扫一遍；标注缺口/API；每项给最小实现步骤与验证命令

# 已完成工作

- tests/port/partial-audit：补齐 `region_request_state_test.go` 可迁移纯逻辑语义（stale-read `DataIsNotReady` wait+retry + retry fallback-to-leader）
  - 关键：stale-read 遇到 `data_is_not_ready` 不 invalidate region/store cache；固定 sleep 后重试；重试选路回退 leader 但保持 `Context.stale_read=true`
  - 文件：`src/request/plan.rs`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/daemon.md`
  - 验证：`make check` + `make unit-test`

- tests/port/locate-region-request-more：补齐 Go `client-go/internal/locate/region_request*_test.go` 可迁移 send-fail/stale-command 语义到 Rust 单测
  - 关键：引入 `Error::ResourceGroupThrottled` 并确保不触发 cache invalidation/mark slow；gRPC send-fail 触发 mark store slow + invalidate_store_cache；replica-read stale-command 无 backoff、按非 witness peer 数量有界重试，耗尽后 invalidate region 并返回 RegionError
  - 文件：`src/common/errors.rs`，`src/request/plan.rs`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/daemon.md`
  - 验证：`make all`

- transport/batch-commands+health：实现 TiKV BatchCommands stream + health feedback 落盘，并用单测/集成测对齐 Go batch-client 关键语义
  - 关键：stream prime `Empty(request_id=0)` 防首包死锁；真实 request_id 从 1 开始；recv 按 request_id 解复用支持乱序；断线后自动重连；超时返回 `DeadlineExceeded("batch commands request timeout")`
  - 覆盖：对齐 Go `internal/client/client_fail_test.go` / `tikvrpc/tikvrpc_test.go` / `integration_tests/health_feedback_test.go` 等价语义
  - 文件：`src/store/batch_commands.rs`，`src/store/request.rs`，`src/store/client.rs`，`src/store/health.rs`，`src/raw/client.rs`，`tests/integration_tests.rs`，`Cargo.toml`，`Cargo.lock`

- tests/parity-mapping+util：完成 client-go tests 迁移/映射闭环（101 `_test.go`），补齐可迁移 util/mock 语义单测
  - 关键：实现 Backoffer/token limiter/Callback+RunLoop/request_source/deadlock detector；从 mocktikv 挑选 raw 可迁移子集落到 `MockKvClient` 单测；其余 Go-only/mockstore 标注 N/A 并给等价覆盖点
  - 文件：`src/backoffer.rs`，`src/util/{request_source,rate_limit,async_util}.rs`，`src/mock/deadlock_detector.rs`，`src/mock.rs`，`src/raw/client.rs`，`.codex/progress/client-go-tests-file-map.md`

- core/txn+pd+infra：补齐 request/pd/txn 核心可迁移语义与工程门槛，确保 `make all`/覆盖率阈值通过
  - 关键：PdRpcClient kv_client cache + 并发 dial 去重；replica selector fast-retry/pending-backoff；resolved lock cache；ReturnCommitTS + commit-wait TSO；clippy/-Dwarnings 修复；`cargo llvm-cov` 行覆盖率>=80%
  - 文件：`src/request/*`，`src/pd/client.rs`，`src/region_cache.rs`，`src/transaction/*`，`src/common/errors.rs`，`.codex/progress/daemon.md`

- tests/port/config-config_test：迁移 Go `client-go/config/config_test.go`（ParsePath / TxnScope 注入 / gRPC keepalive timeout 校验）
  - 关键：新增 `parse_path`（`tikv://...` DSN-like）+ `txn_scope_from_config`（failpoint `tikvclient/injectTxnScope`）；在 `SecurityManager` 上补 grpc keepalive timeout 配置与最小值校验（>=50ms）
  - 验证：`make check` + `make unit-test`
  - 文件：`src/config.rs`，`src/common/security.rs`，`src/lib.rs`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-tests-port.md`，`.codex/progress/daemon.md`

- tests/port/tikv-min-safe-ts：迁移 Go `client-go/tikv/kv_test.go` 的 MinSafeTS/StoreSafeTS/PD fallback 语义到 Rust 单测，并提供 TransactionClient 入口
  - 关键：新增 `SafeTsManager`（store safe_ts + per-scope min_safe_ts；0/max 处理；PD per-store map 优先，invalid->StoreSafeTS fallback；混合路径对齐 Go）；新增 `TransactionClient::{refresh_safe_ts_cache,min_safe_ts}`（store-side StoreSafeTS 刷新）
  - 验证：`make check` + `make unit-test`
  - 文件：`src/store/safe_ts.rs`，`src/pd/client.rs`，`src/transaction/client.rs`，`src/store/mod.rs`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-tests-port.md`，`.codex/progress/daemon.md`

- tests/port/locate-region-request：补齐 Go `internal/locate/region_request*_test.go` 可迁移语义覆盖的映射与关键分支单测
  - 完成：将 `region_request{,3,_state}_test.go` 从 N/A 收敛为 partial（明确对应 Rust 覆盖点：`src/request/plan.rs`/`src/request/plan_builder.rs`/`src/request/read_routing.rs`/integration stale-read）；补 `StoreNotMatch` 但 leader 缺失时不触发 store invalidation 的分支单测
  - 验证：`make check` + `make unit-test`
  - 文件：`src/request/plan.rs`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/daemon.md`

- tests/port/rawkv-rawkv_test-minimal：从 Go `client-go/rawkv/rawkv_test.go` 挑选可迁移纯逻辑子集，落到 Rust mock/unit tests
  - 完成：补 raw CF 透传/隔离单测；将 raw `Client::{with_cf,with_backoff,with_atomic_for_cas}` 下放到泛型 impl，便于 `Client<MockPdClient>` 使用；更新 Go tests mapping（rawkv 从 N/A -> partial）
  - 验证：`make all`
  - 文件：`src/raw/client.rs`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-tests-port.md`，`.codex/progress/daemon.md`

- tests/port/rawkv-store-addr-update：补齐 Go `client-go/rawkv/rawkv_test.go` 的 store addr swap / StoreNotMatch->reload->retry 可迁移语义
  - 完成：新增 RawClient 单测 `test_store_not_match_reloads_store_addr_and_retries`（模拟 PD store addr 更新 + client-side store cache 失效；先 StoreNotMatch，再 invalidate_store_cache 后用新 addr 重试成功）；更新 mapping notes
  - 验证：`make check` + `make unit-test`
  - 文件：`src/raw/client.rs`，`.codex/progress/client-go-tests-file-map.md`，`.codex/progress/client-go-tests-port.md`，`.codex/progress/daemon.md`
