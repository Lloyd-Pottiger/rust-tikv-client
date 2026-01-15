# Findings & Decisions

## Requirements
- New implementation lives in `./new-client-rust`.
- Target: feature + public-API parity with `./client-go` v2 (not legacy/deprecated APIs).
- Rust-idiomatic + high performance + memory safety; sufficient comments + tests.
- Must read `client-go` and `client-rust` code; avoid guessing.
- Track work in `.codex/progress/daemon.md` (required by `AGENTS.md`).

## Research Findings
- `client-go` v2 explicitly advertises features: Follower Read, 1PC, Async Commit (`client-go/README.md`).
- Existing `client-rust` provides async `RawClient` and `TransactionClient`, plus internal modules for PD, region cache, backoff, raw, and transaction (`client-rust/src/lib.rs`).
- `client-go` v2 的主要入口类型（初步）：`rawkv.Client`/`txnkv.Client`/`tikv.KVStore`，以及 `tikvrpc::{Request,Response}`（导出类型扫描 + `New*` 构造函数定位）。
- `client-go/tikv/kv.go` 提供 `NewKVStore`（内部组合：PD client + region cache + oracle + lock resolver + safe ts/txn safepoint 后台任务等）。

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| Create `task_plan.md/findings.md/progress.md` | planning-with-files workflow for large multi-step work |
| Keep `client-go/` and `client-rust/` as read-only references | New implementation isolated in `new-client-rust/` |
| Bootstrap `new-client-rust/` by migrating `client-rust/` | Reuse proven PD/region cache/raw/txn building blocks; reduces “guessing” risk |

## Issues Encountered
| Issue | Resolution |
|-------|------------|
|       |            |

## Resources
- `client-go/README.md` (v2 scope + feature list)
- `client-go/go.mod` (module path `github.com/tikv/client-go/v2`)
- `.codex/progress/client-go-api-inventory.md` (从源码自动提取的导出符号清单；用于对齐 Rust 侧工作拆解)
- `.codex/progress/parity-map.md` (Go→Rust 高层模块映射草案)
- `.codex/progress/gap-analysis.md` (client-rust vs client-go(v2) 缺口初稿)
- `client-rust/src/lib.rs` (existing Rust client public surface + module structure)

## Visual/Browser Findings
- N/A (local repo exploration only)
