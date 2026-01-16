# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 ./new-client-rust 目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- region_cache：明确 TTL/失效策略与锁粒度（解决 TODO/FIXME，避免全局锁成为瓶颈）
  - 计划：
    - 梳理 cache 命中/失效路径（invalidate_region/store、update_leader）
    - 评估是否需要 TTL；若需要，引入可控的过期策略（不影响正确性）
    - 针对热点路径做 micro 优化并补齐单测
    - `cargo test` 验证

# 待做工作

- cleanup：清理剩余非 generated TODO/FIXME（小项为主），并补齐对应测试
  - 范围：`request/plan.rs`（backoff TODO）、`pd/timestamp.rs`（可调参数/stream 结束语义）

# 已完成工作

- config/kv/util：收敛剩余导出符号（Rust mapping / capability-only / out-of-scope），清空 checklist 未标注项
  - 关键决策：Go `config/kv/util` 大量导出项为实现细节/Go context helpers；Rust 侧以 `Config` + builder/typed API 替代，统一标注 out-of-scope/capability-only
  - 文件：`.codex/progress/parity-checklist.md`，`.codex/progress/daemon.md`

- rawkv：补齐剩余 public API（Scan/ReverseScan/ClusterID）+ option/probe 取舍，并更新 checklist
  - 关键决策：不复刻 Go `RawOption`/functional options；改为 `Config` + chainable builder（`with_cf/with_request_source/...`）；`GetPDClient/Probe` 等保持 out-of-scope
  - 代码：增加 `RawClient::cluster_id()`（初始化时从 PD 缓存），并补齐单测
  - 文件：`new-client-rust/src/{raw/client.rs,pd/{client,retry,cluster}.rs}`，`.codex/progress/parity-checklist.md`，`.codex/progress/daemon.md`

- tikv/tikvrpc/txnkv：public API mapping + out-of-scope（按 checklist 渐进，优先入口与核心类型）
  - 关键决策：`tikvrpc.{CmdType,Request,Response}` 不做 Go 风格 wrapper；以 `request::PlanBuilder`/typed kvproto request 覆盖能力；`txnkv` 不复刻 Go 包结构，统一映射到 `TransactionClient`/`Transaction`
  - 文件：`.codex/progress/parity-checklist.md`，`.codex/progress/daemon.md`
  - 备注：后续继续收敛剩余包（`config/rawkv/kv/util`）的 public vs capability-only vs out-of-scope

- new-client-rust 基线 + API 轮廓：Raw/Txn 客户端、PD/region cache、request plan/PlanBuilder、Keyspace、Error/RequestContext、proto/gen（可编译可测试）
  - 关键决策：以 `client-rust` 为起点迁移；对外 API Rust-idiomatic（`Config`/`TransactionOptions`/`RetryOptions` 显式配置，替代 Go 全局 config/ctx）
  - 文件：`new-client-rust/src/{lib.rs,config.rs,raw/*,transaction/*,request/*,store/*,pd/*,region_cache.rs,request_context.rs,common/*,timestamp.rs}` + `new-client-rust/src/generated/*`，`.codex/progress/parity-checklist.md`
  - 测试：`new-client-rust/src/timestamp.rs (tests)`，`new-client-rust/src/common/errors.rs (undetermined_error_query)`

- Txn/协议对齐：async-commit/1PC、pipelined txn + local latches、replica/stale read、assertion/lock options、resolve-lock-lite + retry/backoff 校准、resource control tagger/interceptor
  - 关键决策：pipelined flush/resolve-lock 固定 request_source；stale-read meet-lock fallback leader；prewrite-only 不进入 commit/rollback；region retry attempt>0 时禁用 resolve-lock-lite
  - 测试：覆盖 flush/rollback/latch、replica/stale routing、assertion/lock options、lite txn_size、lock resolver retry/backoff、resource control 透传

- 横切能力 + parity artifacts：interceptor chain(wrap)/mock、trace hooks + PlanBuilder/RequestContext 注入、metrics(feature-gate)+最小 API、checklist scope policy + inventory 工具
  - 关键决策：trace ctx helper（ContextWithTraceID/TraceIDFromContext/GetTraceControlFlags）不引入 Go-style context，统一映射到 `PlanBuilder::{with_trace_id,with_trace_control}` 并标注 out-of-scope；Prometheus 可选依赖，禁用 `prometheus` 时 stats no-op；metrics 不 1:1 暴露 Go 的 label/handle（统一标注 out-of-scope），仅保留 `metrics::{register,gather_as_text}`
  - 文件：`new-client-rust/src/{interceptor.rs,trace.rs,metrics.rs,stats.rs,request/plan.rs,request/plan_builder.rs,request_context.rs}`，`.codex/progress/{parity-checklist.md,parity-map.md,gap-analysis.md,client-go-api-inventory.md}`，`tools/client-go-api-inventory/main.go`
  - 测试：`new-client-rust/src/metrics.rs (gather_contains_core_metrics)` 先打点再 gather（避免空 vec 不输出导致 flaky），`--no-default-features` 可编译可测试

- 验证与收尾：fmt/clippy + feature 组合（no-default-features）构建/测试
  - 关键决策：clippy 仅做最小修复；递归 handler 保留签名用 allow 标注；同时覆盖 default 与 `--no-default-features` 的 clippy
  - 结果：`cargo fmt --check`，`cargo test`，`cargo test --no-default-features`，`cargo clippy --all-targets`，`cargo clippy --all-targets --no-default-features` 均通过
  - 文件：`new-client-rust/src/{common/errors.rs,interceptor.rs,lib.rs,request/plan.rs,request/plan_builder.rs,transaction/{latch.rs,pipelined.rs,transaction.rs}}`，`.codex/progress/daemon.md`

- txn/cleanup_locks：修复 `ScanLockRequest` range shard（apply_shard 设置 end_key）并补齐跨 region 的单测
  - 关键决策：按 `region_stream_for_range` 的 intersection 设置 per-region `end_key`（避免 ScanLock 跨 region 触发 RegionError）
  - 测试：新增单测覆盖 multi-region range 的 shard 结果（end_key 被截断到 region end）
  - 文件：`new-client-rust/src/transaction/requests.rs`，`.codex/progress/daemon.md`

- txn/gc：校准并注释 ScanLock batch size（对齐 client-go `GCScanLockLimit=ResolvedCacheSize/2`），并去重默认值来源
  - 关键决策：以 `ResolveLocksOptions::default().batch_size` 作为唯一默认值，并在 `transaction/lock.rs` 注释 client-go 来源
  - 文件：`new-client-rust/src/transaction/{client.rs,lock.rs}`，`.codex/progress/daemon.md`

- build：增加 `cargo test --all-features --no-run` 作为 feature 组合编译验证（覆盖 `integration-tests`）
  - 结果：`cargo test --all-features --no-run` 通过（integration/failpoint test bin 可编译）
  - 文件：`.codex/progress/daemon.md`

- progress：刷新 `.codex/progress/gap-analysis.md`（清理过期陈述并提炼非 generated TODO/FIXME 作为缺口）
  - 关键决策：gap-analysis 只跟踪“能力/协议/对外可用性”，签名级跟踪仍以 checklist 为准
  - 文件：`.codex/progress/{gap-analysis.md,daemon.md}`

- txn/buffer：`scan_and_fetch` 使用 `BTreeMap` 去重并保持有序，移除排序 TODO
  - 关键决策：以有序 map 保证 scan 结果 determinism，避免额外排序开销
  - 文件：`new-client-rust/src/transaction/buffer.rs`，`.codex/progress/daemon.md`

- raw/requests：补齐 `test_raw_scan` 返回 keys 的断言，清理 FIXME
  - 测试：覆盖 keyspace disable/enable 两种 case 的 key 序列断言
  - 文件：`new-client-rust/src/raw/requests.rs`，`.codex/progress/daemon.md`

- util/iter：完善 `FlatMapOk` 的 Iterator 语义（size_hint/FusedIterator）并补齐单测
  - 测试：新增 size_hint 行为用例
  - 文件：`new-client-rust/src/util/iter.rs`，`.codex/progress/daemon.md`

- kv/bound_range：澄清 `PartialEq<(Bound<T>, Bound<T>)>` 的 clone 约束，移除 FIXME（保持 API 不变）
  - 关键决策：此 impl 主要服务 doc/test；tuple 借用导致需要 clone（不做无意义优化）
  - 文件：`new-client-rust/src/kv/bound_range.rs`，`.codex/progress/daemon.md`

- pd/client(test)：澄清 `group_keys_by_region` 的输入约束，移除测试中的 FIXME
  - 关键决策：强调 batching 最优输入约束，而非 correctness 依赖
  - 文件：`new-client-rust/src/pd/client.rs`，`.codex/progress/daemon.md`

- pd/timestamp：澄清 MAX_PENDING_COUNT 与 TSO stream termination 语义，移除 TODO
  - 关键决策：MAX_PENDING_COUNT 作为经验值用于 backpressure；TSO stream 结束视为 shutdown/reconnect 的 debug 日志
  - 文件：`new-client-rust/src/pd/timestamp.rs`，`.codex/progress/daemon.md`

- pd/retry：将 PD 重试参数从 magic number 收敛为 `Config::pd_retry`（`PdRetryConfig`），并补齐单测覆盖可配置行为
  - 关键决策：默认值保持不变，但通过 `Config` 暴露可调参数（reconnect_interval/max_reconnect_attempts/leader_change_retry）
  - 文件：`new-client-rust/src/{config.rs,lib.rs,mock.rs,pd/{client.rs,retry.rs}}`，`.codex/progress/daemon.md`
