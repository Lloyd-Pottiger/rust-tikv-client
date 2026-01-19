# client-go Tests -> Rust Coverage Map (High-Level)

目标：把 client-go 的「可迁移测试语义」迁移到本仓库 Rust 实现；对 Go-only/internal-structure-only 的测试标注 out-of-scope，并用 Rust 的等价语义测试覆盖关键路径。

## Inventory
- client-go `_test.go` files: 101
- `func Test*` cases: ~294
- Top dirs by test-file count: `internal/` (38), `integration_tests/` (32), `util/` (6), `config/` (5), `tikvrpc/` (4), `oracle/` (4)

## Out-Of-Scope (Skip; Rust Has No Equivalent Abstraction)
- `client-go/util/request_source*_test.go`: Go-style `context.Context` keys/RequestSource struct/BuildRequestSource (parity checklist already标注 N/A)
- `client-go/util/async/*_test.go`: Go-specific runloop/goroutine utilities
- `client-go/util/rate_limit_test.go`: Go channel token limiter utility
- `client-go/config/config_test.go`: DSN ParsePath / failpoint 注入 TxnScope / gRPC keepalive timeout 细节（Rust 入口是 PD endpoints + `Config`）
- `client-go/internal/unionstore/**`: Go memdb/art/rbt/staging/snapshot/memory-footprint 细节；Rust 事务 buffer 采用不同结构（只迁移“事务本地 buffer 语义”相关测试）
- `client-go/internal/mockstore/**`: Go mocktikv/mockstore 生态；Rust 使用 `src/mock.rs`（不做 1:1 mock server 迁移）

## Mostly Covered (Existing Rust Tests)
- TLS/security: `client-go/config/security_test.go` -> `src/common/security.rs#L123`
- Retry/backoff algorithms: Go backoff jitter / attempt cap -> `src/backoff.rs#L207`（注：Go Backoffer 的 “error-type longest sleep / excluded sleep” 语义不完全等价）
- Latch: `client-go/internal/latch/*` -> `src/transaction/latch.rs#L483`
- Region cache core invariants: `client-go/internal/locate/region_cache_test.go`(部分语义) -> `src/region_cache.rs#L423`
- Interceptor chain basics: `client-go/tikvrpc/interceptor/*` -> `src/interceptor.rs#L309`
- Raw/Txn request context injection: 覆盖在 `src/raw/client.rs` / `src/transaction/transaction.rs` 的单元测试 + `tests/integration_tests.rs`

## Gaps / Needs Expansion
- Replica read selection: `client-go/internal/locate/replica_selector_test.go` 覆盖面远大于 Rust 的 `src/request/read_routing.rs#L229`
  - 已补：Mixed/Learner 选择、stale-read retry fallback、witness 排除、seed deterministic selection、force-leader override vs request-source 选择
  - 仍缺：Go 的 score/fast-retry/pending-backoff/avoid-slow-store/proxy/flashback 等高级路径（Rust 目前未实现同构逻辑）
- Store/RPC client behavior: Go `client-go/internal/client/*_test.go` 有不少并发/错误路径；Rust 目前 `src/store/client.rs#L62` 仅覆盖最小 dispatch 语义
  - 计划：补齐 timeout/label/context 应用、重试链条中 interceptor/patch request_source 的交互等

## Next (Implementation Order)
1. 扩展 `src/request/read_routing.rs` 的单元测试（ReplicaReadType::Mixed/Learner + deterministic selection）
2. 扩展 `src/store/**` 的单元测试（dispatch/context/timeout + 与 PlanBuilder/Retry 的组合）
3. 对照 Go `integration_tests/`，补齐 Rust `tests/integration_tests.rs` 缺失的 E2E case（仅在 cluster ready 时验证）

## Integration Mapping
- Go `integration_tests/` 的高层映射见：`.codex/progress/client-go-integration-tests-port.md`
