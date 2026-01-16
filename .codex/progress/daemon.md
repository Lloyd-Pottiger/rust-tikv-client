# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 repo 根目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

---

# 正在进行的工作

- （无）

# 待做工作

- （无）

# 已完成工作

- tests：扩展 raw_checksum 集成测试，覆盖子区间范围语义（start/end bound 编码回归）
  - 覆盖：`[start,end)` / `[start,end]` / 空区间（start==end）语义
  - 文件：`tests/integration_tests.rs`，`.codex/progress/daemon.md`

- infra：把 raw_checksum 纳入 integration-test-smoke（确保端到端 checksum 覆盖可一键跑）
  - 文件：`Makefile`，`.codex/progress/daemon.md`

- docs：补齐 RawChecksum/RawClient::checksum 文档语义，并刷新 README 状态描述
  - 关键点：明确 checksum=CRC64-ECMA(encoded_key||value) xor 聚合；API v2 下 encoded_key 含 keyspace 前缀
  - 文件：`src/raw/{mod.rs,client.rs}`，`README.md`，`.codex/progress/daemon.md`

- tests：补齐 RawClient::checksum 端到端集成测试（crc64_xor/total_kvs/total_bytes）并用 tiup 验证
  - 关键点：对齐 TiKV/Go 的 CRC64-ECMA 语义；API v2 keyspace 下 checksum 计算包含 raw key 前缀 `[b'r',0,0,0]`
  - 验证：`tiup playground` + `cargo test --features integration-tests raw_checksum` 通过
  - 文件：`tests/integration_tests.rs`，`.codex/progress/daemon.md`

- core：Rust TiKV client（repo 根目录 crate）对齐 client-go(v2) 能力与对外 API 能力
  - 关键决策：Rust public API 以显式 `Config/TransactionOptions` 为入口；低层以 `request::PlanBuilder` + typed kvproto 覆盖能力；不复刻 Go `tikvrpc` mega-wrapper
  - 覆盖：Raw/Txn/PD/RegionCache/Plan；2PC/async-commit/1PC/pipelined/local-latches；replica+stale read；resource control/trace/metrics hooks；keyspace
  - 文件：`src/{raw,transaction,request,pd,store}/*`，`src/{region_cache.rs,replica_read.rs,request_context.rs,lib.rs}`
  - 跟踪：`.codex/progress/{client-go-api-inventory.md,parity-checklist.md,parity-map.md,gap-analysis.md}`，`doc/{architecture.md,client-go-v2-parity-roadmap.md}`

- correctness：hardening + 协议行为回归测试（避免“猜测正确性”）
  - 关键点：stale-read 引入 PD `GetMinTs` + `TransactionClient::current_min_timestamp()`；pessimistic snapshot 读路径 lock-check；cleanup_locks rollback/region-error 单测；清理残留 `todo!/unimplemented!` 与死测试模块
  - 文件：`src/{transaction/*.rs,request/plan.rs,request/keyspace.rs,raw/client.rs,mock.rs}`，`tests/*`

- infra：CI/docs/bench/devex（可复现的验证与开发入口）
  - 产出：`doc/{architecture,bench,client-go-v2-migration,development}.md`；Makefile tiup playground + integration smoke；nextest 可选 fallback；rustdoc warnings 清零 + CI guardrail
  - 文件：`doc/*`，`Makefile`，`README.md`，`.github/workflows/ci.yml`，`benches/*`

- repo：将 Rust client 从 `new-client-rust/` 上移到 repo 根目录，删除 `new-client-rust/`，并修复所有路径/文档/CI 引用
  - 关键决策：crate 根目录化（统一构建/CI/文档入口）；移除仅对旧子目录有意义的 repo hygiene 文件（`OWNERS*`/`.gitignore` 等）；CI 统一为 `.github/workflows/ci.yml`（push `main`）
  - 校验：`make check`，`make unit-test`
  - 文件：`Cargo.toml`，`Makefile`，`README.md`，`.github/workflows/ci.yml`，`doc/*`，`src/*`，`tests/*`，`proto*`，`proto-build/*`，`.codex/progress/*`，并删除 `task_plan.md/findings.md/progress.md`

- infra/docs：强化 rustdoc guardrail（Makefile `RUSTDOCFLAGS=-Dwarnings` + CI 运行 `make doc`），并修正文档中的 Rust toolchain 要求
  - 校验：`make doc`（-Dwarnings）通过
  - 文件：`Makefile`，`.github/workflows/ci.yml`，`README.md`，`.codex/progress/daemon.md`
