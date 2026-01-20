# client-go Tests -> Rust Coverage Map (High-Level)

目标：把 client-go 的「可迁移测试语义」迁移到本仓库 Rust 实现；对 Go-only/internal-structure-only 的测试标注 out-of-scope，并用 Rust 的等价语义测试覆盖关键路径。

## Inventory
- client-go `_test.go` files: 101
- `func Test*` cases: ~294
- Top dirs by test-file count: `internal/` (38), `integration_tests/` (32), `util/` (6), `config/` (5), `tikvrpc/` (4), `oracle/` (4)

## File-Level Map (Source Of Truth)
逐文件（101 个 `_test.go`）的覆盖/归因清单见：`.codex/progress/client-go-tests-file-map.md`

## Out-Of-Scope (Skip; Rust Has No Equivalent Abstraction)
- `client-go/tikvrpc/*/main_test.go`: Go `goleak` harness
- `client-go/config/config_test.go`: DSN ParsePath / failpoint 注入 TxnScope / gRPC keepalive timeout 细节（Rust 入口是 PD endpoints + `Config`）
- `client-go/internal/unionstore/**`: Go memdb/art/rbt/staging/snapshot/memory-footprint 细节；Rust 事务 buffer 采用不同结构（只迁移“事务本地 buffer 语义”相关测试）
- `client-go/internal/mockstore/**`: Go mocktikv/mockstore 生态；Rust 使用 `src/mock.rs`（不做 1:1 mock server 迁移）
- `client-go/rawkv/rawkv_test.go` + `client-go/tikv/*_test.go` + `client-go/txnkv/**`(mocktikv parts): 强依赖 mocktikv cluster/PD HTTP mocks；Rust 以 `tests/integration_tests.rs`(real cluster) + unit-test mocks 覆盖等价语义

## Mostly Covered (Existing Rust Tests)
- TLS/security: `client-go/config/security_test.go` -> `src/common/security.rs#L123`
- Retry/backoff algorithms: Go jitter backoff -> `src/backoff.rs#L207`；Go Backoffer（maxSleep/excludedSleep/longestSleep/clone+fork+update + `MayBackoffForRegionError`）-> `src/backoffer.rs`
- Util/request_source: `client-go/util/request_source*_test.go` -> `src/util/request_source.rs`
- Util/async runloop: `client-go/util/async/*_test.go` -> `src/util/async_util.rs`
- Util/rate limit: `client-go/util/rate_limit_test.go` -> `src/util/rate_limit.rs`
- Latch: `client-go/internal/latch/*` -> `src/transaction/latch.rs#L483`
- LockResolver cache: `client-go/txnkv/txnlock/lock_resolver_test.go` -> `src/transaction/lock.rs`（resolved cache 命中不触发 secondary-check RPC）
- Region cache core invariants: `client-go/internal/locate/region_cache_test.go`(部分语义) -> `src/region_cache.rs#L423`
- Interceptor chain basics: `client-go/tikvrpc/interceptor/*` -> `src/interceptor.rs#L309`
- BatchCommands wrappers: `client-go/tikvrpc/tikvrpc_test.go` -> `src/store/request.rs` + `src/store/batch_commands.rs`
- KV primitives: `client-go/kv/*` -> `src/kv/*`（Key/BoundRange/codec + Get/BatchGet options + ValueEntry 单测）
- Trace flags/events: `client-go/trace/*` -> `src/trace.rs#L189`
- KeyError debug-info redaction: `client-go/error/error_test.go` -> `src/common/errors.rs#L905`
- Raw/Txn request context injection: 覆盖在 `src/raw/client.rs` / `src/transaction/transaction.rs` 的单元测试 + `tests/integration_tests.rs`

## Gaps / Needs Expansion
- Replica read selection: `client-go/internal/locate/replica_selector_test.go` 覆盖面远大于 Rust 的 `src/request/read_routing.rs#L229`
  - 已补：Mixed/Learner 选择、stale-read retry fallback、witness 排除、seed deterministic selection、force-leader override vs request-source 选择
  - 已补：score + `ServerIsBusy` fast-retry + pending-backoff（`src/request/read_routing.rs`/`src/request/plan.rs`/`src/request/pending_backoff.rs` 单测）
  - 仍缺：Go proxy/forwarding/flashback 等高级路径（Rust 架构不同，按 N/A 或等价语义覆盖）
- Store/RPC client behavior: Go `client-go/internal/client/*_test.go` 有不少并发/错误路径；Rust 目前 `src/store/client.rs#L62` 仅覆盖最小 dispatch 语义
  - 已补：store error traits 单测（Vec region_errors 聚合 + SetRegionError）；TikvConnect invalid addr fast-fail；PdRpcClient kv_client cache + 并发 dial 去重（singleflight）；PlanBuilder retry attempt/patch request_source/interceptor 组合
  - 仍缺：Go batch-client/forwarding/conn-pool 的并发/重连细节（Rust 架构不同，按 N/A 或等价语义覆盖）

## Next (Implementation Order)
1. 引入 BatchCommands send-loop + health feedback（解锁 `integration_tests/health_feedback_test.go` 等 batch-client 相关用例迁移）
2. 评估 mocktikv 用例可迁移子集，用 `src/mock.rs`/`MockKvClient` 覆盖更多纯逻辑/错误路径单测
3. 对照 `.codex/progress/client-go-tests-file-map.md` 定期收敛 N/A 的归因与等价语义覆盖点

## Integration Mapping
- Go `integration_tests/` 的高层映射见：`.codex/progress/client-go-integration-tests-port.md`
