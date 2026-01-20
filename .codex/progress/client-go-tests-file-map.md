# client-go `_test.go` File Map -> Rust Coverage

状态：
- `covered`: Rust 已有等价语义测试覆盖
- `partial`: 仅覆盖可迁移语义；其余属于 Go-only/架构差异（在 Notes 里说明）
- `n/a`: Rust 无对应抽象/纯 Go harness（明确原因）

| Go test file | Status | Rust coverage / notes |
|---|---|---|
| `client-go/config/config_test.go` | n/a | Go DSN/txn-scope/failpoint 注入；Rust `Config` 入口不同 |
| `client-go/config/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/config/retry/backoff_test.go` | partial | Backoffer 内部计数/clone/fork N/A；可迁移 region backoff 语义见 `src/request/plan.rs` + `src/backoff.rs` |
| `client-go/config/retry/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/config/security_test.go` | covered | `src/common/security.rs` |
| `client-go/error/error_test.go` | covered | `src/common/errors.rs`（KeyError debug-info redact 等） |
| `client-go/integration_tests/1pc_test.go` | covered | `tests/integration_tests.rs` `txn_try_one_pc` |
| `client-go/integration_tests/2pc_test.go` | covered | `tests/integration_tests.rs` `txn_crud`/`txn_pessimistic*`/`txn_bank_transfer`/`txn_batch_mutate_*`/`txn_scan*` |
| `client-go/integration_tests/assertion_test.go` | covered | `src/transaction/transaction.rs`（assertion level 单测）+ `src/common/errors.rs`（映射） |
| `client-go/integration_tests/async_commit_fail_test.go` | covered | `tests/failpoint_tests.rs` |
| `client-go/integration_tests/async_commit_test.go` | covered | `tests/integration_tests.rs` + `tests/failpoint_tests.rs` |
| `client-go/integration_tests/client_fp_test.go` | covered | `tests/failpoint_tests.rs` |
| `client-go/integration_tests/delete_range_test.go` | covered | `tests/integration_tests.rs` `raw_delete_range` |
| `client-go/integration_tests/gc_test.go` | covered | `tests/integration_tests.rs` `txn_update_safepoint` |
| `client-go/integration_tests/health_feedback_test.go` | n/a | Go batch-client health feedback；Rust 无 BatchCommands stream（仅保留等价语义测试） |
| `client-go/integration_tests/interceptor_test.go` | covered | `tests/integration_tests.rs` `txn_snapshot_api_and_request_context` |
| `client-go/integration_tests/isolation_test.go` | covered | `tests/integration_tests.rs` `txn_read`/`txn_snapshot*` |
| `client-go/integration_tests/lock_test.go` | covered | `tests/integration_tests.rs` `txn_lock_keys*`/`txn_get_for_update` + `tests/failpoint_tests.rs` |
| `client-go/integration_tests/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/integration_tests/option_test.go` | covered | `src/transaction/transaction.rs`/`src/config.rs` 单测 |
| `client-go/integration_tests/pd_api_test.go` | covered | `tests/integration_tests.rs` `txn_get_timestamp`/`txn_update_safepoint` + `tests/common/ctl.rs` |
| `client-go/integration_tests/pipelined_memdb_test.go` | covered | `tests/integration_tests.rs` `txn_pipelined_flush` + `src/transaction/transaction.rs` 单测 |
| `client-go/integration_tests/prewrite_test.go` | covered | `src/transaction/transaction.rs` 单测 + txn E2E 用例 |
| `client-go/integration_tests/range_task_test.go` | covered | `src/request/*`（plan/shard 单测） |
| `client-go/integration_tests/raw/api_mock_test.go` | n/a | Go mockstore；Rust 选择 real-cluster E2E + unit-test mocks |
| `client-go/integration_tests/raw/api_test.go` | covered | `tests/integration_tests.rs` `raw_req`/`raw_write_million`/`raw_large_batch_put`/`raw_ttl`/`raw_checksum`/`raw_cas` |
| `client-go/integration_tests/raw/util_test.go` | covered | `src/kv/bound_range.rs` 单测 |
| `client-go/integration_tests/resource_group_test.go` | covered | `tests/integration_tests.rs` `txn_snapshot_api_and_request_context` |
| `client-go/integration_tests/resource_tag_test.go` | covered | 同上 |
| `client-go/integration_tests/safepoint_test.go` | covered | `tests/integration_tests.rs` `txn_update_safepoint` |
| `client-go/integration_tests/scan_mock_test.go` | n/a | Go mock 扫描；Rust 用 real-cluster scan + 单测覆盖 range 语义 |
| `client-go/integration_tests/scan_test.go` | covered | `tests/integration_tests.rs` `txn_scan*` + raw scan 覆盖 |
| `client-go/integration_tests/snapshot_fail_test.go` | covered | `tests/failpoint_tests.rs` |
| `client-go/integration_tests/snapshot_test.go` | covered | `tests/integration_tests.rs` `txn_snapshot_api_and_request_context`/`txn_pessimistic_snapshot_checks_locks` |
| `client-go/integration_tests/split_test.go` | covered | `tests/integration_tests.rs` `txn_split_batch` |
| `client-go/integration_tests/store_test.go` | covered | `tests/integration_tests.rs` `raw_client_new_and_with_cf_smoke` |
| `client-go/integration_tests/ticlient_test.go` | covered | `tests/integration_tests.rs` `txn_get_timestamp` |
| `client-go/integration_tests/util_test.go` | covered | `tests/common/mod.rs` |
| `client-go/internal/apicodec/codec_test.go` | covered | `src/request/keyspace.rs`（parse/decode keyspace + prefixes sorted） |
| `client-go/internal/apicodec/codec_v1_test.go` | n/a | 空测试（Go 侧占位） |
| `client-go/internal/apicodec/codec_v2_test.go` | covered | `src/request/keyspace.rs`（encode ranges/decode epoch/bucket）+ `src/transaction/transaction.rs`（commit primary key encode） |
| `client-go/internal/client/client_async_test.go` | n/a | Go async/batch client 发送环；Rust 无同构 BatchCommands |
| `client-go/internal/client/client_fail_test.go` | n/a | 同上（mockserver/batch-client 行为） |
| `client-go/internal/client/client_interceptor_test.go` | covered | `src/interceptor.rs`（interceptor chain 语义） |
| `client-go/internal/client/client_test.go` | partial | Rust 侧连接复用/并发 dial 去重：`src/pd/client.rs`（`test_kv_client_caching` + `test_kv_client_concurrent_connect_is_deduped_per_address`）；其余 BatchCommands/forwarding/metadata/health-feedback 等 N/A |
| `client-go/internal/client/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/internal/client/priority_queue_test.go` | n/a | Go priority queue 内存/引用清理；Rust 无对应实现 |
| `client-go/internal/latch/latch_test.go` | covered | `src/transaction/latch.rs` |
| `client-go/internal/latch/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/internal/latch/scheduler_test.go` | covered | `src/transaction/latch.rs` |
| `client-go/internal/locate/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/internal/locate/metrics_collector_test.go` | covered | `src/request/metrics_collector.rs` + `src/store/request.rs`（stale-read req/resp metrics） |
| `client-go/internal/locate/region_cache_test.go` | covered | `src/region_cache.rs` |
| `client-go/internal/locate/region_request3_test.go` | partial | 大量 mocktikv/forwarding/conn-pool；可迁移 region error/backoff/replica routing 语义见 `src/request/plan.rs` |
| `client-go/internal/locate/region_request_state_test.go` | partial | mocktikv FSM；等价语义由 `src/request/plan.rs` + `tests/integration_tests.rs`（stale/replica read）覆盖 |
| `client-go/internal/locate/region_request_test.go` | partial | mocktikv；关键 retry/patch request_source/backoff 见 `src/request/plan.rs`/`src/request/plan_builder.rs` |
| `client-go/internal/locate/replica_selector_test.go` | covered | `src/request/read_routing.rs` + `src/request/plan.rs`（stale-command/keep-peer/fast-retry） |
| `client-go/internal/mockstore/deadlock/deadlock_test.go` | n/a | Go mockstore deadlock server；Rust 未做 1:1 mock server 迁移 |
| `client-go/internal/mockstore/deadlock/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/internal/mockstore/mocktikv/main_test.go` | n/a | Go mocktikv harness |
| `client-go/internal/mockstore/mocktikv/marshal_test.go` | n/a | Go mocktikv 编码/批处理回归；Rust 无 BatchCommands 发送环 |
| `client-go/internal/mockstore/mocktikv/mock_tikv_test.go` | n/a | Go mocktikv MVCC/region 模拟 |
| `client-go/internal/mockstore/mocktikv/mvcc_test.go` | n/a | 同上 |
| `client-go/internal/resourcecontrol/resource_control_test.go` | covered | `src/resource_control.rs` |
| `client-go/internal/unionstore/arena/arena_test.go` | n/a | Go memdb/arena 结构；Rust 事务 buffer 架构不同 |
| `client-go/internal/unionstore/art/art_iterator_test.go` | n/a | 同上 |
| `client-go/internal/unionstore/art/art_node_test.go` | n/a | 同上 |
| `client-go/internal/unionstore/art/art_snapshot_test.go` | n/a | 同上 |
| `client-go/internal/unionstore/art/art_test.go` | n/a | 同上 |
| `client-go/internal/unionstore/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/internal/unionstore/memdb_bench_test.go` | n/a | Go benchmark/test harness |
| `client-go/internal/unionstore/memdb_norace_test.go` | n/a | Go memdb |
| `client-go/internal/unionstore/memdb_test.go` | n/a | Go memdb |
| `client-go/internal/unionstore/pipelined_memdb_test.go` | n/a | Go memdb；Rust pipelined txn 用 E2E + 单测覆盖语义 |
| `client-go/internal/unionstore/rbt/rbt_test.go` | n/a | Go rbt impl |
| `client-go/internal/unionstore/union_store_test.go` | n/a | Go unionstore impl |
| `client-go/kv/key_test.go` | covered | `src/kv/key.rs` / `src/kv/key.rs` 单测 |
| `client-go/kv/kv_test.go` | covered | `src/kv/*`（options + ValueEntry 等） |
| `client-go/kv/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/oracle/oracles/export_test.go` | n/a | Go test-only exports；Rust 以内部 hook/Mock 覆盖 |
| `client-go/oracle/oracles/local_test.go` | covered | `src/timestamp/local_oracle.rs` |
| `client-go/oracle/oracles/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/oracle/oracles/pd_test.go` | covered | `src/pd/low_resolution_ts.rs` + `src/pd/stale_timestamp.rs` + `src/pd/read_ts_validation.rs` |
| `client-go/rawkv/rawkv_test.go` | n/a | 强依赖 mocktikv cluster；Rust 用 `tests/integration_tests.rs` 覆盖 raw E2E |
| `client-go/tikv/kv_test.go` | n/a | Go mocktikv parts（Rust 无 1:1） |
| `client-go/tikv/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/tikvrpc/interceptor/interceptor_test.go` | covered | `src/interceptor.rs` |
| `client-go/tikvrpc/interceptor/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/tikvrpc/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/tikvrpc/tikvrpc_test.go` | n/a | Go BatchCommands wrappers；Rust 无 BatchCommands 发送环 |
| `client-go/trace/flags_test.go` | covered | `src/trace.rs` |
| `client-go/trace/trace_test.go` | covered | `src/trace.rs` |
| `client-go/txnkv/transaction/2pc_test.go` | n/a | Go 内部 `minCommitTsManager`/并发 helper；Rust async-commit/2PC 走不同实现（语义由 txn 单测/E2E 覆盖） |
| `client-go/txnkv/transaction/batch_getter_test.go` | covered | `src/transaction/transaction.rs`（ReturnCommitTS + buffer cache/overlay） |
| `client-go/txnkv/txnlock/lock_resolver_test.go` | covered | `src/transaction/lock.rs`（resolved cache 等） |
| `client-go/util/async/core_test.go` | n/a | Go async/runloop 工具；Rust async 模型不同 |
| `client-go/util/async/runloop_test.go` | n/a | 同上 |
| `client-go/util/main_test.go` | n/a | Go `TestMain` harness |
| `client-go/util/misc_test.go` | covered | `src/util/gc_time.rs` + `src/util/time_detail.rs` |
| `client-go/util/rate_limit_test.go` | n/a | Go channel limiter；Rust 无对应 util |
| `client-go/util/request_source_test.go` | n/a | Go `context.Context` key/request_source builder；Rust 无对应抽象 |
