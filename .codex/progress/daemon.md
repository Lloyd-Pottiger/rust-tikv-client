# 整体工作目标
client-go (https://github.com/tikv/client-go) 是 TiKV 的 Go 客户端。尽管存在 https://github.com/tikv/client-rust（即 TiKV 的 Rust 客户端），但 client-rust 缺少许多功能。
作为一名高级分布式系统工程师和 Rust 专家。将 TiKV 的 Go 客户端 client-go 重写为符合 Rust 语言风格、高性能的 Rust 版本。目标是支持 client-go v2 所有功能，所有 Public API，同时充分利用 Rust 的内存安全性和零成本抽象。
但无需考虑兼容性，这个库是为了给最新版本的 tikv 使用的，无需兼容老版本，也无需支持已经废弃的 API 和特性。你需要确保你的代码有足够的注释，以便其他开发者理解你的代码。并且，你需要确保你的代码有足够的测试，以便在将来进行维护和扩展时，能够快速定位问题。

client-go 和 client-rust 我都已经 clone 到当前目录下，新的 rust client 实现在 repo 根目录下。你需要阅读这两个项目，不要靠猜，要仔细阅读代码，理解它们的实现原理和细节。

当 client-go v2 所有功能，所有 Public API 都已经实现，你需要再次仔细阅读所有代码，review 它们的实现原理和细节，确保代码有良好的质量，足够的测试覆盖 80% 以上的代码，关键路径/易出错逻辑一定要有测试覆盖，没有 bug 等。

并且你应该 Port client-go 的所有测试到新的 rust client 实现中，如果测试失败了，你要尝试修复它们。

---

# 正在进行的工作

# 待做工作

# 已完成工作

- tests/oracle-parity：迁移 client-go oracle 相关关键测试语义（pd oracle + local oracle）
  - 关键：per-txn-scope singleflight GetTS + low-res ts cache；stale-ts/UntilExpired 用 physical 差值；local oracle 用 SystemTime + per-ms logical counter + time hook；测试用 tokio paused time 避免 flake
  - 验证：`cargo test`
  - 文件：`src/pd/*`，`src/timestamp.rs`，`src/timestamp/local_oracle.rs`，`.codex/progress/daemon.md`

- tests/core-parity：迁移 client-go 核心请求/重试/kv/misc 可迁移测试语义
  - 关键：`Key::next_prefix_key` 边界（全 0xFF -> empty）；region/cache/retry 的 fast-retry+invalidate；stale-read bytes/req metrics；resource-control bypass request_source `internal_others`
  - 验证：`cargo test`
  - 文件：`src/kv/key.rs`，`src/store/*`，`src/request/*`，`src/region_cache.rs`，`src/stats.rs`，`src/resource_control.rs`，`.codex/progress/daemon.md`

- infra/test-mapping+docs：维护 client-go 测试清单/覆盖映射与集成测试文档入口
  - 关键：统计 go tests（101 files/294 cases）并标注 N/A（mockstore/mocktikv 强绑定）；维护 integration_tests 高层映射
  - 文件：`.codex/progress/client-go-tests-port.md`，`.codex/progress/client-go-integration-tests-port.md`，`.codex/progress/daemon.md`
