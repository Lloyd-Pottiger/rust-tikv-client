# Task Plan: Rewrite TiKV client-go v2 to Rust

## Goal
Implement a new Rust TiKV client in `./new-client-rust` with feature + public-API parity with `./client-go` (v2), while keeping a Rust-idiomatic, high-performance, well-tested design.

> Note: `.codex/progress/daemon.md` is the canonical task tracker required by `AGENTS.md`.  
> This file is my per-session working memory (planning-with-files skill).

## Current Phase
Phase 1

## Phases

### Phase 1: Requirements & Discovery
- [ ] Inventory `client-go` v2 exported APIs (by package) and map to feature areas.
- [ ] Audit `client-rust` capabilities and identify gaps vs `client-go` v2.
- [ ] Record findings in `findings.md` and update `.codex/progress/daemon.md`.
- **Status:** in_progress

### Phase 2: Architecture & Crate Layout
- [ ] Decide crate/workspace structure under `new-client-rust/`.
- [ ] Define core abstractions: PD, region cache, request routing, backoff, raw, txn, error model.
- [ ] Define public API surface and naming consistent with Rust guidelines.
- **Status:** pending

### Phase 3: Implementation (Iterative)
- [ ] Bootstrap workspace + proto build pipeline.
- [ ] Implement core transport (gRPC), PD client, region cache.
- [ ] Implement raw client parity.
- [ ] Implement transactional client parity (2PC/1PC/async-commit, lock resolve, etc.).
- **Status:** pending

### Phase 4: Testing & Verification
- [ ] Port/author unit tests for key algorithms (region cache, backoff, lock resolver).
- [ ] Add integration tests (feature-gated) against a TiKV+PD cluster.
- [ ] Run `cargo fmt`, `cargo clippy`, `cargo test`.
- **Status:** pending

### Phase 5: Delivery
- [ ] Ensure docs for all `pub` APIs (rustdoc examples).
- [ ] Update `.codex/progress/daemon.md` and commit logically-scoped changes.
- **Status:** pending

## Key Questions
1. What are the complete exported APIs in `client-go` v2 (packages + types + constructors)?
2. Which `client-go` v2 features are absent or different in `client-rust`, and why?
3. What crate boundaries best preserve performance and keep public APIs simple?

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Maintain `.codex/progress/daemon.md` as canonical tracker | Required by repo workflow (`AGENTS.md`) |
| Use `client-rust` as a reference implementation | Avoid guessing; reuse proven building blocks where possible |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
|       | 1       |            |

