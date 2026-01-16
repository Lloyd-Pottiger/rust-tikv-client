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

- core/tests：Rust TiKV client（crate at repo root）对齐 client-go(v2) 能力与对外 API（Rust-idiomatic）
  - 关键决策：以显式 `Config/TransactionOptions` 作为入口；低层以 `request::PlanBuilder` + typed kvproto 覆盖能力；不复刻 Go `tikvrpc` mega-wrapper
  - 覆盖：Raw/Txn/PD/RegionCache/Backoff；2PC/1PC/async-commit/pipelined/local-latches；replica/stale read；resource control（request source/resource group tag）；keyspace；raw checksum（CRC64-ECMA xor，含 keyspace prefix）
  - 测试：单测覆盖关键协议分支；集成测（feature-gated）覆盖 raw_checksum/txn smoke
  - 文件：`src/*`，`tests/*`，`doc/{architecture,client-go-v2-parity-roadmap}.md`，`.codex/progress/{client-go-api-inventory,parity-checklist,parity-map,gap-analysis}.md`

- infra/repo：整理目录结构 + CI/devex（可复现验证入口）
  - 关键决策：将 crate 上移到 repo 根目录并删除 `new-client-rust/`；CI 统一为 `.github/workflows/ci.yml`（push `main`）；Makefile 统一入口（check/unit-test/doc/tiup）
  - guardrail：`RUSTFLAGS=-Dwarnings` + `RUSTDOCFLAGS=-Dwarnings`；CI 运行 `make check`/`make doc`
  - 文件：`Cargo.toml`，`Makefile`，`README.md`，`doc/*`，`.github/workflows/ci.yml`，`proto*`，`proto-build/*`，`.codex/progress/daemon.md`

- safety：移除可替换的 `unsafe`（保留必要的零拷贝点）
  - 实现：`compat::stream_fn` 改为 `futures::stream::unfold`；`kv::codec::decode_bytes_in_place` 改为 `copy_within`+`truncate`；keyspace 前缀拼接/截断改为 `resize`+`copy_within`+`truncate`
  - 说明：当前仅保留 `kv::Key` 的 `#[repr(transparent)]` 引用转换 `unsafe`（零拷贝；已用单测约束）
  - 校验：`make check`，`make unit-test`
  - 文件：`src/{compat.rs,kv/codec.rs,request/keyspace.rs}`，`.codex/progress/daemon.md`

- infra/tests：按 feature gate integration test binaries（避免 unit-test 构建/运行空测试）
  - 关键点：为 `tests/{integration_tests,failpoint_tests}.rs` 增加/修正 `Cargo.toml [[test]] required-features=["integration-tests"]`
  - 校验：`make check`，`make unit-test`
  - 文件：`Cargo.toml`，`.codex/progress/daemon.md`

- infra：提交 `Cargo.lock` + 记录 MSRV（提升可复现构建；避免依赖/编译器漂移）
  - 关键决策：对本 repo（带 CI/bench/integration）视为应用型工程，锁定依赖版本以稳定 Rust toolchain 兼容性
  - 校验：`make check`，`make unit-test`
  - 文件：`Cargo.lock`，`.gitignore`，`Cargo.toml`，`.codex/progress/daemon.md`

- tests/docs：修复 raw_checksum 集成测试期望值（CRC64 init/xorout 与 TiKV 对齐）
  - 根因：TiKV raw checksum 的 `CRC64(key||value)` 使用 init=~0 且 xorout=~0（否则 crc64_xor 会偏差）
  - 校验：`make tiup-up` + `cargo test --features integration-tests raw_checksum` + `make tiup-down`
  - 文件：`tests/integration_tests.rs`，`src/raw/{mod.rs,client.rs}`，`.codex/progress/daemon.md`
