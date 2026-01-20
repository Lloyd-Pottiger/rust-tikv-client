# client-go integration_tests -> Rust Coverage Map

Rust 侧集成测试集中在：
- `tests/integration_tests.rs`（feature `integration-tests`）
- `tests/failpoint_tests.rs`（feature `integration-tests` + failpoints）

下表以“等价语义覆盖”为目标：能在 Rust 侧以 unit-test 或 E2E 覆盖的就标注对应位置；Go 侧依赖 mockstore/batch-client 的用例标注为 N/A（Rust 架构不同）。

| client-go file | Rust coverage | Notes |
|---|---|---|
| `integration_tests/1pc_test.go` | `tests/integration_tests.rs` `txn_try_one_pc` | 1PC try-one-pc flow |
| `integration_tests/2pc_test.go` | `tests/integration_tests.rs` `txn_crud`/`txn_pessimistic*`/`txn_bank_transfer`/`txn_batch_mutate_*`/`txn_scan*` | 覆盖 2PC/并发/scan 等主路径 |
| `integration_tests/assertion_test.go` | `src/transaction/transaction.rs`（assertion level/mutation assertions 单测）+ `src/common/errors.rs`（assertion failed 映射） | Go 用例大量依赖 failpoints/mockstore；Rust 用单测验证请求构造/错误映射 |
| `integration_tests/async_commit_test.go` | `tests/integration_tests.rs`（async-commit usage）+ `tests/failpoint_tests.rs` | 主要语义：async-commit 选项/锁清理/异常路径 |
| `integration_tests/async_commit_fail_test.go` | `tests/failpoint_tests.rs` | 覆盖 partial-commit / cleanup / heartbeat 等失败路径 |
| `integration_tests/client_fp_test.go` | `tests/failpoint_tests.rs` | failpoint 驱动的事务异常路径 |
| `integration_tests/delete_range_test.go` | `tests/integration_tests.rs` `raw_delete_range` | 新增：覆盖 raw delete-range 的 range 语义（含 `\\0` 边界技巧） |
| `integration_tests/gc_test.go` | `tests/integration_tests.rs` `txn_update_safepoint` | GC/safepoint 相关主路径 |
| `integration_tests/health_feedback_test.go` | N/A | Go batch client health feedback；Rust 未实现 BatchCommands 同构机制 |
| `integration_tests/interceptor_test.go` | `tests/integration_tests.rs` `txn_snapshot_api_and_request_context` | request context setters + rpc interceptors |
| `integration_tests/isolation_test.go` | `tests/integration_tests.rs` `txn_read`/`txn_snapshot*` | 读一致性/快照相关 |
| `integration_tests/lock_test.go` | `tests/integration_tests.rs` `txn_lock_keys*`/`txn_get_for_update` + `tests/failpoint_tests.rs` | pessimistic lock / resolve locks |
| `integration_tests/option_test.go` | `src/transaction/transaction.rs`/`src/config.rs` 单测 | option builder/validation + commit-wait TSO（`set_commit_wait_until_tso` + timeout） |
| `integration_tests/pd_api_test.go` | `tests/integration_tests.rs` `txn_get_timestamp`/`txn_update_safepoint` + `tests/common/ctl.rs` | PD TSO/HTTP API 辅助 |
| `integration_tests/pipelined_memdb_test.go` | `tests/integration_tests.rs` `txn_pipelined_flush` + `src/transaction/transaction.rs` 单测 | pipelined txn flush/resolve locks |
| `integration_tests/prewrite_test.go` | `src/transaction/transaction.rs` 单测 + 多个 txn E2E 用例 | prewrite/commit 请求构造与重试 |
| `integration_tests/range_task_test.go` | `src/request/*`（plan/shard 单测） | Go RangeTask 抽象 Rust 侧对应 plan/shard；以单测验证拆分/合并/重试 |
| `integration_tests/raw/api_test.go` | `tests/integration_tests.rs` `raw_req`/`raw_write_million`/`raw_large_batch_put`/`raw_ttl`/`raw_checksum`/`raw_cas` | raw CRUD/scan/ttl/checksum/cas |
| `integration_tests/raw/api_mock_test.go` | N/A | Go 使用 mockstore；Rust 选择 real-cluster E2E + unit-test mocks |
| `integration_tests/raw/util_test.go` | `src/kv/bound_range.rs` 单测 | range/`\\0` 语义 |
| `integration_tests/resource_group_test.go` | `tests/integration_tests.rs` `txn_snapshot_api_and_request_context` | resource group name/tag/penalty/override priority |
| `integration_tests/resource_tag_test.go` | 同上 | tagger/interceptor 组合 |
| `integration_tests/safepoint_test.go` | `tests/integration_tests.rs` `txn_update_safepoint` | safepoint 更新 |
| `integration_tests/scan_mock_test.go` | N/A | Go mock 扫描；Rust 用 real-cluster scan + 单测覆盖 range 语义 |
| `integration_tests/scan_test.go` | `tests/integration_tests.rs` `txn_scan*` + raw scan 覆盖 | scan/scan_reverse 多 region |
| `integration_tests/snapshot_fail_test.go` | `tests/failpoint_tests.rs` | snapshot/commit 失败路径 |
| `integration_tests/snapshot_test.go` | `tests/integration_tests.rs` `txn_snapshot_api_and_request_context`/`txn_pessimistic_snapshot_checks_locks` | snapshot 语义 |
| `integration_tests/split_test.go` | `tests/integration_tests.rs` `txn_split_batch` | batch split & region boundary 相关 |
| `integration_tests/store_test.go` | `tests/integration_tests.rs` `raw_client_new_and_with_cf_smoke` | client 创建 + CF |
| `integration_tests/ticlient_test.go` | `tests/integration_tests.rs` `txn_get_timestamp` | basic client usage |
| `integration_tests/util_test.go` | `tests/common/mod.rs` | Rust 侧用 `init()/clear_tikv()` 做等价准备工作 |
