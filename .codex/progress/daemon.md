# tikv-client daemon.md

## 整体工作目标
- 修复当前 Rust `tikv-client` 的失败用例，使 `make integration-test-txn` 通过（至少覆盖当前失败的 `txn_batch_mutate_pessimistic` / `txn_stale_read_snapshot_get`）。
- 保持改动聚焦：优先修复根因（错误传播/重试策略），避免扩大接口破坏面。

## 正在进行的工作
(无)

## 待做工作
(无)

## 已完成工作
- 修复 `txn_batch_mutate_pessimistic`：悲观锁部分成功/部分失败时，保留并对外返回 `Error::PessimisticLockError`（仍会先回滚 `success_keys` 释放锁）；避免把 wrapper 展开成 inner 导致测试期望不匹配。改动：`src/transaction/transaction.rs`。
- 修复 `txn_stale_read_snapshot_get`：stale read 遇到 `RegionError.data_is_not_ready` 不再用默认 region backoff 过早失败；增加 stale-read 重试时优先 leader 的路由回退（仍保持 `Context.stale_read=true`），并在 `data_is_not_ready` 场景下做 200ms 间隔、最多 120 次的等待重试且不刷 region/store cache。改动：`src/request/plan.rs`、`src/request/read_routing.rs`。
- 验证：启动 `make tiup-up`，运行 `make integration-test-txn` 通过（含上述两条用例）。
