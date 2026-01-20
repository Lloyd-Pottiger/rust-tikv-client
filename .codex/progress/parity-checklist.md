# client-go v2 parity checklist (auto-generated skeleton)

This file is generated from `client-go/` source and is used to track Go→Rust parity work.

Regenerate:
- `go run ./tools/client-go-api-inventory`

Conventions:
- `Rust:` fill with target Rust path/symbol(s)
- `Tests:` fill with relevant unit/integration tests (or `N/A`)

## config (package config)

### Types
- [x] `type AsyncCommit struct` | Rust: `tikv_client::TransactionOptions::{use_async_commit,max_commit_ts_safe_window}` (src/transaction/transaction.rs) | Tests: `src/transaction/transaction.rs (test_async_commit_fallback_to_2pc_when_min_commit_ts_is_zero, test_async_commit_uses_min_commit_ts_when_available)`
- [x] `type Config struct` | Rust: `tikv_client::Config` (src/config.rs) | Tests: N/A
- [x] `type CoprocessorCache struct` | Rust: N/A (out-of-scope: client-go internal cache config not exposed) | Tests: N/A
- [x] `type PDClient struct` | Rust: N/A (out-of-scope: PD client config is internal; use `Config`/defaults) | Tests: N/A
- [x] `type PessimisticTxn struct` | Rust: `tikv_client::TransactionClient::begin_pessimistic` + `tikv_client::TransactionOptions::new_pessimistic` (src/transaction/{client,transaction}.rs) | Tests: N/A
- [x] `type Security struct` | Rust: `tikv_client::Config::with_security` + `tikv_client::SecurityManager` (src/{config.rs,common/security.rs}) | Tests: `src/common/security.rs (test_security)`
- [x] `type TiKVClient struct` | Rust: N/A (out-of-scope: TiKV gRPC client config is internal; use `Config`/defaults) | Tests: N/A
- [x] `type TxnLocalLatches struct` | Rust: `tikv_client::TransactionClient::with_txn_local_latches` (src/transaction/client.rs) | Tests: `src/transaction/latch.rs (tests)`

### Functions
- [x] `func DefaultConfig() Config` | Rust: `Config::default()` (src/config.rs) | Tests: N/A
- [x] `func DefaultPDClient() PDClient` | Rust: N/A (out-of-scope: PD client config is internal; use `Config`/defaults) | Tests: N/A
- [x] `func DefaultTiKVClient() TiKVClient` | Rust: N/A (out-of-scope: TiKV gRPC client config is internal; use `Config`/defaults) | Tests: N/A
- [x] `func DefaultTxnLocalLatches() TxnLocalLatches` | Rust: N/A (out-of-scope: configure via `TransactionClient::with_txn_local_latches`) | Tests: N/A
- [x] `func GetGlobalConfig() *Config` | Rust: N/A (out-of-scope: explicit `Config` passed to `*_with_config`) | Tests: N/A
- [x] `func GetTxnScopeFromConfig() string` | Rust: N/A (out-of-scope: no global config; use `GLOBAL_TXN_SCOPE` and explicit options) | Tests: N/A
- [x] `func NewSecurity(sslCA, sslCert, sslKey string, verityCN []string) Security` | Rust: `Config::with_security` + `SecurityManager::load` (src/{config.rs,common/security.rs}) | Tests: `src/common/security.rs (test_security)`
- [x] `func ParsePath(path string) (etcdAddrs []string, disableGC bool, keyspaceName string, err error)` | Rust: N/A (out-of-scope: no DSN parser; pass PD endpoints + `Config`) | Tests: N/A
- [x] `func StoreGlobalConfig(config *Config)` | Rust: N/A (out-of-scope: explicit `Config` passed to `*_with_config`) | Tests: N/A
- [x] `func UpdateGlobal(f func(conf *Config)) func()` | Rust: N/A (out-of-scope: explicit `Config` passed to `*_with_config`) | Tests: N/A

### Consts
- [x] `BatchPolicyBasic` | Rust: N/A (out-of-scope: internal batching policy not exposed) | Tests: N/A
- [x] `BatchPolicyCustom` | Rust: N/A (out-of-scope: internal batching policy not exposed) | Tests: N/A
- [x] `BatchPolicyPositive` | Rust: N/A (out-of-scope: internal batching policy not exposed) | Tests: N/A
- [x] `BatchPolicyStandard` | Rust: N/A (out-of-scope: internal batching policy not exposed) | Tests: N/A
- [x] `DefBatchPolicy` | Rust: N/A (out-of-scope: internal batching policy not exposed) | Tests: N/A
- [x] `DefGrpcInitialConnWindowSize` | Rust: N/A (out-of-scope: internal gRPC tuning not exposed) | Tests: N/A
- [x] `DefGrpcInitialWindowSize` | Rust: N/A (out-of-scope: internal gRPC tuning not exposed) | Tests: N/A
- [x] `DefMaxConcurrencyRequestLimit` | Rust: N/A (out-of-scope: internal request limit not exposed) | Tests: N/A
- [x] `DefStoreLivenessTimeout` | Rust: N/A (out-of-scope: internal store liveness tuning not exposed) | Tests: N/A
- [x] `DefStoresRefreshInterval` | Rust: N/A (out-of-scope: internal refresh interval not exposed) | Tests: N/A
- [x] `NextGen` | Rust: N/A (out-of-scope) | Tests: N/A

### Vars
- (none)

### Methods
- [x] `func (c *TxnLocalLatches) Valid() error` | Rust: N/A (out-of-scope: validated by constructors/options) | Tests: N/A
- [x] `func (config *TiKVClient) GetGrpcKeepAliveTimeout() time.Duration` | Rust: N/A (out-of-scope: internal gRPC tuning not exposed) | Tests: N/A
- [x] `func (config *TiKVClient) Valid() error` | Rust: N/A (out-of-scope: internal gRPC tuning not exposed) | Tests: N/A
- [x] `func (p *PDClient) Valid() error` | Rust: N/A (out-of-scope: internal PD tuning not exposed) | Tests: N/A
- [x] `func (s *Security) ToTLSConfig() (tlsConfig *tls.Config, err error)` | Rust: `SecurityManager::load` (src/common/security.rs) | Tests: `src/common/security.rs (test_security)`

## config/retry (package retry)

### Types
- [x] `type BackoffFnCfg struct` | Rust: `tikv_client::Backoff` (src/backoff.rs) | Tests: `src/backoff.rs (test_*)`
- [x] `type Backoffer struct` | Rust: N/A (capability: request retries via `tikv_client::request::PlanBuilder` + `RetryOptions`) | Tests: `src/request/mod.rs (test_region_retry)`
- [x] `type Config struct` | Rust: `tikv_client::RetryOptions` (src/request/mod.rs) | Tests: `src/request/mod.rs (test_region_retry)`

### Functions
- [x] `func IsFakeRegionError(err *errorpb.Error) bool` | Rust: N/A (capability: region error retry handled by `PlanBuilder` internal region handler) | Tests: `src/request/mod.rs (test_region_retry)`
- [x] `func MayBackoffForRegionError(regionErr *errorpb.Error, bo *Backoffer) error` | Rust: N/A (capability: region error retry handled by `PlanBuilder` + `RetryOptions`) | Tests: `src/request/mod.rs (test_region_retry)`
- [x] `func NewBackoffFnCfg(base, cap, jitter int) *BackoffFnCfg` | Rust: `Backoff::{no_jitter_backoff,full_jitter_backoff,equal_jitter_backoff,decorrelated_jitter_backoff}` (src/backoff.rs) | Tests: `src/backoff.rs (tests)`
- [x] `func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer` | Rust: N/A (out-of-scope: no Go-style Backoffer; use `RetryOptions` + `Backoff`) | Tests: N/A
- [x] `func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer` | Rust: N/A (out-of-scope: no Go-style Backoffer/Variables) | Tests: N/A
- [x] `func NewConfig(name string, metric *prometheus.Observer, backoffFnCfg *BackoffFnCfg, err error) *Config` | Rust: N/A (out-of-scope: no per-error backoff config object; use `RetryOptions`) | Tests: N/A
- [x] `func NewNoopBackoff(ctx context.Context) *Backoffer` | Rust: `Backoff::no_backoff` (src/backoff.rs) | Tests: `src/backoff.rs (test_no_jitter_backoff)`

### Consts
- [x] `DecorrJitter` | Rust: `Backoff::decorrelated_jitter_backoff` (src/backoff.rs) | Tests: `src/backoff.rs (tests)`
- [x] `EqualJitter` | Rust: `Backoff::equal_jitter_backoff` (src/backoff.rs) | Tests: `src/backoff.rs (tests)`
- [x] `FullJitter` | Rust: `Backoff::full_jitter_backoff` (src/backoff.rs) | Tests: `src/backoff.rs (tests)`
- [x] `NoJitter` | Rust: `Backoff::no_jitter_backoff` (src/backoff.rs) | Tests: `src/backoff.rs (test_no_jitter_backoff)`

### Vars
- [x] `BoCommitTSLag` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoIsWitness` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoMaxRegionNotInitialized` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoMaxTsNotSynced` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoPDRPC` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoRegionMiss` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoRegionRecoveryInProgress` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoRegionScheduling` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoStaleCmd` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoTiFlashRPC` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoTiFlashServerBusy` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoTiKVDiskFull` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoTiKVRPC` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoTiKVServerBusy` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoTxnLock` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoTxnLockFast` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `BoTxnNotFound` | Rust: N/A (out-of-scope: no exported per-error backoff configs; use `RetryOptions`) | Tests: N/A
- [x] `TxnStartKey` | Rust: N/A (out-of-scope: no Go-style context variables) | Tests: N/A

### Methods
- [x] `func (b *Backoffer) Backoff(cfg *Config, err error) error` | Rust: N/A (out-of-scope: no Go-style Backoffer; use `PlanBuilder` retries) | Tests: N/A
- [x] `func (b *Backoffer) BackoffWithCfgAndMaxSleep(cfg *Config, maxSleepMs int, err error) error` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) BackoffWithMaxSleepTxnLockFast(maxSleepMs int, err error) error` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) CheckKilled() error` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) Clone() *Backoffer` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) ErrorsNum() int` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) Fork() (*Backoffer, context.CancelFunc)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) GetBackoffSleepMS() map[string]int` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) GetBackoffTimes() map[string]int` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) GetCtx() context.Context` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) GetTotalBackoffTimes() int` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) GetTotalSleep() int` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) GetTypes() []string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) GetVars() *kv.Variables` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) Reset()` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) ResetMaxSleep(maxSleep int)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) SetCtx(ctx context.Context)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) String() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (b *Backoffer) UpdateUsingForked(forked *Backoffer)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (c *Config) Base() int` | Rust: N/A (out-of-scope: no Go-style `retry::Config` object) | Tests: N/A
- [x] `func (c *Config) SetBackoffFnCfg(fnCfg *BackoffFnCfg)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (c *Config) SetErrors(err error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (c *Config) String() string` | Rust: N/A (out-of-scope) | Tests: N/A

## error (package error)

### Types
- [x] `type ErrAssertionFailed struct` | Rust: `src/common/errors.rs (AssertionFailedError, Error::AssertionFailed)` | Tests: `src/common/errors.rs (key_error_assertion_failed_maps_to_assertion_failed)`
- [x] `type ErrDeadlock struct` | Rust: `src/common/errors.rs (DeadlockError, Error::Deadlock)` | Tests: `src/common/errors.rs (key_error_deadlock_maps_to_deadlock)`
- [x] `type ErrEntryTooLarge struct` | Rust: N/A (out-of-scope: typed size-limit errors not exposed; surfaced as `Error`) | Tests: N/A
- [x] `type ErrGCTooEarly struct` | Rust: N/A (out-of-scope: deprecated in Go; not exposed) | Tests: N/A
- [x] `type ErrKeyExist struct` | Rust: `src/common/errors.rs (KeyExistsError, Error::KeyExists)` | Tests: `src/common/errors.rs (key_error_already_exist_maps_to_key_exists)`
- [x] `type ErrKeyTooLarge struct` | Rust: N/A (out-of-scope: typed size-limit errors not exposed; surfaced as `Error`) | Tests: N/A
- [x] `type ErrLockOnlyIfExistsNoPrimaryKey struct` | Rust: N/A (out-of-scope: Go lock-ctx validation error not exposed) | Tests: N/A
- [x] `type ErrLockOnlyIfExistsNoReturnValue struct` | Rust: N/A (out-of-scope: Go lock-ctx validation error not exposed) | Tests: N/A
- [x] `type ErrPDServerTimeout struct` | Rust: N/A (out-of-scope: PD timeouts surfaced as `Error::Grpc*` / internal errors) | Tests: N/A
- [x] `type ErrQueryInterruptedWithSignal struct` | Rust: N/A (out-of-scope: TiDB integration error) | Tests: N/A
- [x] `type ErrRetryable struct` | Rust: `src/common/errors.rs (Error::Retryable)` | Tests: `src/common/errors.rs (key_error_retryable_maps_to_retryable)`
- [x] `type ErrTokenLimit struct` | Rust: N/A (out-of-scope: token limiter error not exposed) | Tests: N/A
- [x] `type ErrTxnAbortedByGC struct` | Rust: `src/common/errors.rs (Error::TxnAborted)` | Tests: `src/common/errors.rs (key_error_abort_maps_to_txn_aborted)`
- [x] `type ErrTxnTooLarge struct` | Rust: N/A (out-of-scope: typed txn-too-large error not exposed; surfaced as `Error`) | Tests: N/A
- [x] `type ErrWriteConflict struct` | Rust: `src/common/errors.rs (WriteConflictError, Error::WriteConflict)` | Tests: `src/common/errors.rs (key_error_conflict_maps_to_write_conflict)`
- [x] `type ErrWriteConflictInLatch struct` | Rust: `src/common/errors.rs (Error::WriteConflictInLatch)` | Tests: `src/transaction/transaction.rs (latch tests)`
- [x] `type PDError struct` | Rust: N/A (out-of-scope: does not expose PD error wrapper types) | Tests: N/A

### Functions
- [x] `func ExtractDebugInfoStrFromKeyErr(keyErr *kvrpcpb.KeyError) string` | Rust: N/A (out-of-scope: no JSON/redaction debug-info helper) | Tests: N/A
- [x] `func ExtractKeyErr(keyErr *kvrpcpb.KeyError) error` | Rust: `tikv_client::Error::from(kvrpcpb::KeyError)` (src/common/errors.rs) | Tests: `src/common/errors.rs (key_error_*_maps_*)`
- [x] `func IsErrKeyExist(err error) bool` | Rust: `src/common/errors.rs (Error::is_key_exists)` | Tests: `src/common/errors.rs (key_error_already_exist_maps_to_key_exists)`
- [x] `func IsErrNotFound(err error) bool` | Rust: N/A (out-of-scope: not-found represented as `Option` in Rust APIs) | Tests: N/A
- [x] `func IsErrWriteConflict(err error) bool` | Rust: `src/common/errors.rs (Error::is_write_conflict)` | Tests: `src/common/errors.rs (key_error_conflict_maps_to_write_conflict)`
- [x] `func IsErrorCommitTSLag(err error) bool` | Rust: N/A (out-of-scope: no explicit commit-ts-lag error) | Tests: N/A
- [x] `func IsErrorUndetermined(err error) bool` | Rust: `tikv_client::Error::is_undetermined` (src/common/errors.rs) | Tests: `src/common/errors.rs (undetermined_error_query)`
- [x] `func Log(err error)` | Rust: N/A (out-of-scope: use application logging/tracing) | Tests: N/A
- [x] `func NewErrPDServerTimeout(msg string) error` | Rust: N/A (out-of-scope: PD timeouts surfaced as `Error::Grpc*` / internal errors) | Tests: N/A
- [x] `func NewErrWriteConflict(conflict *kvrpcpb.WriteConflict) *ErrWriteConflict` | Rust: `WriteConflictError::from(kvrpcpb::WriteConflict)` + `Error::WriteConflict` (src/common/errors.rs) | Tests: `src/common/errors.rs (key_error_conflict_maps_to_write_conflict)`
- [x] `func NewErrWriteConflictWithArgs(startTs, conflictTs, conflictCommitTs uint64, key []byte, reason kvrpcpb.WriteConflict_Reason) *ErrWriteConflict` | Rust: N/A (out-of-scope: construct `WriteConflictError` directly) | Tests: N/A

### Consts
- [x] `MismatchClusterID` | Rust: N/A (out-of-scope: no error string sentinel; surfaced as `Error`) | Tests: N/A

### Vars
- [x] `ErrBodyMissing` | Rust: N/A (out-of-scope: does not expose Go-style error sentinels) | Tests: N/A
- [x] `ErrCannotSetNilValue` | Rust: N/A (out-of-scope: does not expose Go-style error sentinels) | Tests: N/A
- [x] `ErrCommitTSLag` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrInvalidTxn` | Rust: N/A (out-of-scope: does not expose Go-style error sentinels) | Tests: N/A
- [x] `ErrIsWitness` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrLockAcquireFailAndNoWaitSet` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrLockWaitTimeout` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrNotExist` | Rust: N/A (out-of-scope: not-found represented as `Option`) | Tests: N/A
- [x] `ErrQueryInterrupted` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrRegionDataNotReady` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrRegionFlashbackInProgress` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrRegionFlashbackNotPrepared` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrRegionNotInitialized` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrRegionRecoveryInProgress` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrRegionUnavailable` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrResolveLockTimeout` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrResultUndetermined` | Rust: `Error::UndeterminedError` (src/common/errors.rs) | Tests: `src/common/errors.rs (undetermined_error_query)`
- [x] `ErrTiDBShuttingDown` | Rust: N/A (out-of-scope: TiDB integration error) | Tests: N/A
- [x] `ErrTiFlashServerBusy` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrTiFlashServerTimeout` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrTiKVDiskFull` | Rust: N/A (out-of-scope: disk-full surfaced as `Error`/gRPC status) | Tests: N/A
- [x] `ErrTiKVMaxTimestampNotSynced` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrTiKVServerBusy` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrTiKVServerTimeout` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrTiKVStaleCommand` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ErrUnknown` | Rust: N/A (out-of-scope) | Tests: N/A

### Methods
- [x] `func (d *ErrDeadlock) Error() string` | Rust: `src/common/errors.rs (impl Display for DeadlockError)` | Tests: `src/common/errors.rs (key_error_deadlock_maps_to_deadlock)`
- [x] `func (d *PDError) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e *ErrAssertionFailed) Error() string` | Rust: `src/common/errors.rs (impl Display for AssertionFailedError)` | Tests: `src/common/errors.rs (key_error_assertion_failed_maps_to_assertion_failed)`
- [x] `func (e *ErrEntryTooLarge) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e *ErrGCTooEarly) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e *ErrKeyTooLarge) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e *ErrLockOnlyIfExistsNoPrimaryKey) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e *ErrLockOnlyIfExistsNoReturnValue) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e *ErrPDServerTimeout) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e *ErrTokenLimit) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e *ErrTxnAbortedByGC) Error() string` | Rust: `src/common/errors.rs (Error::TxnAborted Display)` | Tests: `src/common/errors.rs (key_error_abort_maps_to_txn_aborted)`
- [x] `func (e *ErrTxnTooLarge) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e *ErrWriteConflictInLatch) Error() string` | Rust: `src/common/errors.rs (Error::WriteConflictInLatch Display)` | Tests: `src/transaction/latch.rs (tests)`
- [x] `func (e ErrQueryInterruptedWithSignal) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (k *ErrKeyExist) Error() string` | Rust: `src/common/errors.rs (impl Display for KeyExistsError)` | Tests: `src/common/errors.rs (key_error_already_exist_maps_to_key_exists)`
- [x] `func (k *ErrRetryable) Error() string` | Rust: `src/common/errors.rs (Error::Retryable Display)` | Tests: `src/common/errors.rs (key_error_retryable_maps_to_retryable)`
- [x] `func (k *ErrWriteConflict) Error() string` | Rust: `src/common/errors.rs (impl Display for WriteConflictError)` | Tests: `src/common/errors.rs (key_error_conflict_maps_to_write_conflict)`

## kv (package kv)

### Types
- [x] `type AccessLocationType byte` | Rust: N/A (out-of-scope: client-go internal locality hint not exposed) | Tests: N/A
- [x] `type BatchGetOption interface` | Rust: N/A (out-of-scope: no option-interfaces; use typed params/structs) | Tests: N/A
- [x] `type BatchGetOptions struct` | Rust: N/A (out-of-scope: no option-interfaces; use typed params/structs) | Tests: N/A
- [x] `type BatchGetter interface` | Rust: N/A (out-of-scope: no Go-style getter interfaces) | Tests: N/A
- [x] `type FlagsOp uint32` | Rust: `tikv_client::transaction::FlagsOp` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `type GetOption interface` | Rust: N/A (out-of-scope: no option-interfaces; use typed params/structs) | Tests: N/A
- [x] `type GetOptions struct` | Rust: N/A (out-of-scope: no option-interfaces; use typed params/structs) | Tests: N/A
- [x] `type GetOrBatchGetOption interface` | Rust: N/A (out-of-scope: no option-interfaces; use typed params/structs) | Tests: N/A
- [x] `type Getter interface` | Rust: N/A (out-of-scope: no Go-style getter interfaces) | Tests: N/A
- [x] `type KeyFlags uint16` | Rust: `tikv_client::transaction::KeyFlags` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `type KeyRange struct` | Rust: `tikv_client::BoundRange` (src/kv/bound_range.rs) | Tests: `src/kv/bound_range.rs (doctests)`
- [x] `type LockCtx struct` | Rust: N/A (out-of-scope: use `tikv_client::transaction::LockOptions`) | Tests: N/A
- [x] `type ReplicaReadType byte` | Rust: `src/replica_read.rs (ReplicaReadType)` | Tests: `src/transaction/transaction.rs (test_replica_read_peer_selection_and_context_fields)`
- [x] `type ReturnedValue struct` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `type ValueEntry struct` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `type Variables struct` | Rust: N/A (out-of-scope) | Tests: N/A

### Functions
- [x] `func ApplyFlagsOps(origin KeyFlags, ops ...FlagsOp) KeyFlags` | Rust: N/A (capability: `KeyFlags::apply` in `transaction::key_flags`) | Tests: N/A
- [x] `func BatchGetToGetOptions(options []BatchGetOption) []GetOption` | Rust: N/A (out-of-scope: no option-interfaces) | Tests: N/A
- [x] `func CmpKey(k, another []byte) int` | Rust: N/A (out-of-scope: use `Key`/byte slice ordering) | Tests: N/A
- [x] `func NewLockCtx(forUpdateTS uint64, lockWaitTime int64, waitStartTime time.Time) *LockCtx` | Rust: N/A (out-of-scope: use `transaction::LockOptions`) | Tests: N/A
- [x] `func NewValueEntry(value []byte, commitTS uint64) ValueEntry` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func NewVariables(killed *uint32) *Variables` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func NextKey(k []byte) []byte` | Rust: N/A (capability: range handling via `BoundRange`) | Tests: N/A
- [x] `func PrefixNextKey(k []byte) []byte` | Rust: N/A (capability: range handling via `BoundRange`) | Tests: N/A
- [x] `func WithReturnCommitTS() GetOrBatchGetOption` | Rust: N/A (out-of-scope) | Tests: N/A

### Consts
- [x] `AccessCrossZone` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `AccessLocalZone` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `AccessUnknown` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `DefBackOffWeight` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `DefBackoffLockFast` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `DefTxnCommitBatchSize` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `DelKeyLocked` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `DelNeedCheckExists` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `DelNeedConstraintCheckInPrewrite` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `DelNeedLocked` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `DelPresumeKeyNotExists` | Rust: `FlagsOp::DelPresumeKeyNotExists` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `FlagBytes` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `LockAlwaysWait` | Rust: N/A (capability: `transaction::LockOptions::wait_timeout_ms`) | Tests: N/A
- [x] `LockNoWait` | Rust: N/A (capability: `transaction::LockOptions::wait_timeout_ms`) | Tests: N/A
- [x] `ReplicaReadFollower` | Rust: `ReplicaReadType::Follower` (src/replica_read.rs) | Tests: N/A
- [x] `ReplicaReadLeader` | Rust: `ReplicaReadType::Leader` (src/replica_read.rs) | Tests: N/A
- [x] `ReplicaReadLearner` | Rust: `ReplicaReadType::Learner` (src/replica_read.rs) | Tests: N/A
- [x] `ReplicaReadMixed` | Rust: `ReplicaReadType::Mixed` (src/replica_read.rs) | Tests: N/A
- [x] `ReplicaReadPreferLeader` | Rust: `ReplicaReadType::PreferLeader` (src/replica_read.rs) | Tests: N/A
- [x] `SetAssertExist` | Rust: `FlagsOp::SetAssertExist` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `SetAssertNone` | Rust: `FlagsOp::SetAssertNone` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `SetAssertNotExist` | Rust: `FlagsOp::SetAssertNotExist` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `SetAssertUnknown` | Rust: `FlagsOp::SetAssertUnknown` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `SetIgnoredIn2PC` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `SetKeyLocked` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `SetKeyLockedValueExists` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `SetKeyLockedValueNotExists` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `SetNeedConstraintCheckInPrewrite` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `SetNeedLocked` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `SetNewlyInserted` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `SetPresumeKeyNotExists` | Rust: `FlagsOp::SetPresumeKeyNotExists` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `SetPreviousPresumeKNE` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `SetPrewriteOnly` | Rust: `FlagsOp::SetPrewriteOnly` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `SetReadable` | Rust: N/A (out-of-scope) | Tests: N/A

### Vars
- [x] `DefaultVars` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `StoreLimit` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `TxnCommitBatchSize` | Rust: N/A (out-of-scope) | Tests: N/A

### Methods
- [x] `func (ctx *LockCtx) GetValueNotLocked(key []byte) ([]byte, bool)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (ctx *LockCtx) InitCheckExistence(capacity int)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (ctx *LockCtx) InitReturnValues(capacity int)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (ctx *LockCtx) IterateValuesNotLocked(f func([]byte, []byte))` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (ctx *LockCtx) LockWaitTime() int64` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e ValueEntry) IsValueEmpty() bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e ValueEntry) Size() int` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (f KeyFlags) AndPersistent() KeyFlags` | Rust: N/A (out-of-scope: persistent flags are internal in Rust) | Tests: N/A
- [x] `func (f KeyFlags) HasAssertExist() bool` | Rust: `KeyFlags::has_assert_exist` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `func (f KeyFlags) HasAssertNotExist() bool` | Rust: `KeyFlags::has_assert_not_exist` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `func (f KeyFlags) HasAssertUnknown() bool` | Rust: `KeyFlags::has_assert_unknown` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `func (f KeyFlags) HasAssertionFlags() bool` | Rust: `KeyFlags::has_assertion_flags` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `func (f KeyFlags) HasIgnoredIn2PC() bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (f KeyFlags) HasLocked() bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (f KeyFlags) HasLockedValueExists() bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (f KeyFlags) HasNeedCheckExists() bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (f KeyFlags) HasNeedConstraintCheckInPrewrite() bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (f KeyFlags) HasNeedLocked() bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (f KeyFlags) HasNewlyInserted() bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (f KeyFlags) HasPresumeKeyNotExists() bool` | Rust: `KeyFlags::has_presume_key_not_exists` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `func (f KeyFlags) HasPrewriteOnly() bool` | Rust: `KeyFlags::has_prewrite_only` (src/transaction/key_flags.rs) | Tests: N/A
- [x] `func (f KeyFlags) HasReadable() bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *BatchGetOptions) Apply(opts []BatchGetOption)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *GetOptions) Apply(opts []GetOption)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (r ReplicaReadType) IsFollowerRead() bool` | Rust: `ReplicaReadType::is_follower_read` (src/replica_read.rs) | Tests: N/A
- [x] `func (r ReplicaReadType) String() string` | Rust: `impl Display for ReplicaReadType` (src/replica_read.rs) | Tests: N/A

## metrics (package metrics)

### Types
- [x] `type LabelPair = dto.LabelPair` | Rust: N/A (out-of-scope: does not expose Prometheus DTO types) | Tests: N/A
- [x] `type MetricVec interface` | Rust: N/A (out-of-scope: does not expose per-metric handles) | Tests: N/A
- [x] `type TxnCommitCounter struct` | Rust: N/A (out-of-scope: does not expose per-metric handles) | Tests: N/A

### Functions
- [x] `func FindNextStaleStoreID(collector prometheus.Collector, validStoreIDs map[uint64]struct{}) uint64` | Rust: N/A (out-of-scope: no per-store metric vec list) | Tests: N/A
- [x] `func GetStoreMetricVecList() []MetricVec` | Rust: N/A (out-of-scope: no per-metric handle list) | Tests: N/A
- [x] `func GetTxnCommitCounter() TxnCommitCounter` | Rust: N/A (out-of-scope: no per-metric handle exposure) | Tests: N/A
- [x] `func InitMetrics(namespace, subsystem string)` | Rust: N/A (out-of-scope: fixed metric names; use `metrics::register`) | Tests: N/A
- [x] `func InitMetricsWithConstLabels(namespace, subsystem string, constLabels prometheus.Labels)` | Rust: N/A (out-of-scope: fixed metric names; use `metrics::register`) | Tests: N/A
- [x] `func ObserveReadSLI(readKeys uint64, readTime float64, readSize float64)` | Rust: N/A (out-of-scope: SLI set not ported yet; can be re-added behind `prometheus`) | Tests: N/A
- [x] `func RegisterMetrics()` | Rust: `tikv_client::metrics::register` (src/metrics.rs) | Tests: `src/metrics.rs (gather_contains_core_metrics)`

### Consts
- [x] `LabelBatchRecvLoop` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LabelBatchSendLoop` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblAbort` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblAddress` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblBatchGet` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblCommit` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblDirection` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblFromStore` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblGeneral` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblGet` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblInternal` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblLockKeys` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblReason` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblResult` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblRollback` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblScope` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblSource` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblStaleRead` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblStore` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblTarget` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblToStore` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A
- [x] `LblType` | Rust: N/A (out-of-scope: does not expose metrics label consts) | Tests: N/A

### Vars
- [x] `AggressiveLockedKeysDerived` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AggressiveLockedKeysLockedWithConflict` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AggressiveLockedKeysNew` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AggressiveLockedKeysNonForceLock` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncBatchGetCounterWithLockError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncBatchGetCounterWithOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncBatchGetCounterWithOtherError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncBatchGetCounterWithRegionError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncCommitTxnCounterError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncCommitTxnCounterOk` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncSendReqCounterWithOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncSendReqCounterWithOtherError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncSendReqCounterWithRPCError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncSendReqCounterWithRegionError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `AsyncSendReqCounterWithSendError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramDataNotReady` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramEmpty` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramIsWitness` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramLock` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramLockFast` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramPD` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramRPC` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramRegionMiss` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramRegionRecoveryInProgress` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramRegionScheduling` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramServerBusy` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramStaleCmd` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BackoffHistogramTiKVDiskFull` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BatchRecvHistogramError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BatchRecvHistogramOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BatchRequestDurationDone` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BatchRequestDurationRecv` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `BatchRequestDurationSend` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LagCommitTSAttemptHistogramWithError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LagCommitTSAttemptHistogramWithOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LagCommitTSWaitHistogramWithError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LagCommitTSWaitHistogramWithOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LoadRegionCacheHistogramWhenCacheMiss` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LoadRegionCacheHistogramWithBatchScanRegions` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LoadRegionCacheHistogramWithGetStore` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LoadRegionCacheHistogramWithRegionByID` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LoadRegionCacheHistogramWithRegions` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithBatchResolve` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithExpired` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithNotExpired` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithQueryCheckSecondaryLocks` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithQueryTxnStatus` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithQueryTxnStatusCommitted` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithQueryTxnStatusRolledBack` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithResolve` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithResolveAsync` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithResolveForWrite` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithResolveLockLite` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithResolveLocks` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `LockResolverCountWithWaitExpired` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `OnePCTxnCounterError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `OnePCTxnCounterFallback` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `OnePCTxnCounterOk` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `PrewriteAssertionUsageCounterExist` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `PrewriteAssertionUsageCounterNone` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `PrewriteAssertionUsageCounterNotExist` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `PrewriteAssertionUsageCounterUnknown` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvCmdHistogramWithBatchDelete` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvCmdHistogramWithBatchGet` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvCmdHistogramWithBatchPut` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvCmdHistogramWithDelete` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvCmdHistogramWithGet` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvCmdHistogramWithRawChecksum` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvCmdHistogramWithRawReversScan` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvCmdHistogramWithRawScan` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvSizeHistogramWithKey` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RawkvSizeHistogramWithValue` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `ReadRequestFollowerLocalBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `ReadRequestFollowerRemoteBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `ReadRequestLeaderLocalBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `ReadRequestLeaderRemoteBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithBatchScanRegionsError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithBatchScanRegionsOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithGetCacheMissError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithGetCacheMissOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithGetRegionByIDError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithGetRegionByIDOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithGetStoreError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithGetStoreOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithInvalidateRegionFromCacheOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithInvalidateStoreRegionsOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithScanRegionsError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithScanRegionsOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `RegionCacheCounterWithSendFail` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `SecondaryLockCleanupFailureCounterCommit` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `SecondaryLockCleanupFailureCounterRollback` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StaleReadHitCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StaleReadLocalInBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StaleReadLocalOutBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StaleReadMissCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StaleReadRemoteInBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StaleReadRemoteOutBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StaleReadReqCrossZoneCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StaleReadReqLocalCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StatusCountWithError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `StatusCountWithOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVAggressiveLockedKeysCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVAsyncBatchGetCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVAsyncCommitTxnCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVAsyncSendReqCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBackoffHistogram` | Rust: `tikv_client::stats::observe_backoff_sleep` → `tikv_backoff_sleep_duration_seconds` (src/stats.rs) | Tests: `src/metrics.rs (gather_contains_core_metrics)`
- [x] `TiKVBatchBestSize` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchClientRecycle` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchClientUnavailable` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchClientWaitEstablish` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchHeadArrivalInterval` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchMoreRequests` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchPendingRequests` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchRecvLoopDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchRecvTailLatency` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchRequestDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchRequests` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchSendLoopDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchSendTailLatency` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBatchWaitOverLoad` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVBucketClampedCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVFeedbackSlowScoreGauge` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVForwardRequestCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVGRPCConnTransientFailureCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVGrpcConnectionState` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVHealthFeedbackOpsCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVLoadRegionCacheHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVLoadRegionCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVLoadTxnSafePointCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVLocalLatchWaitTimeHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVLockResolverCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVLowResolutionTSOUpdateIntervalSecondsGauge` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVMinSafeTSGapSeconds` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVNoAvailableConnectionCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVOnePCTxnCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVPanicCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVPessimisticLockKeysDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVPipelinedFlushDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVPipelinedFlushLenHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVPipelinedFlushSizeHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVPipelinedFlushThrottleSecondsHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVPreferLeaderFlowsGauge` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVPrewriteAssertionUsageCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVRPCErrorCounter` | Rust: N/A (capability: aggregate failures via `tikv_failed_request_total`) (src/stats.rs) | Tests: `src/metrics.rs (gather_contains_core_metrics)`
- [x] `TiKVRPCNetLatencyHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVRangeTaskPushDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVRangeTaskStats` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVRawkvCmdHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVRawkvSizeHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVReadRequestBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVReadThroughput` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVRegionCacheCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVRegionErrorCounter` | Rust: N/A (capability: aggregate failures via `tikv_failed_request_total`) (src/stats.rs) | Tests: `src/metrics.rs (gather_contains_core_metrics)`
- [x] `TiKVReplicaSelectorFailureCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVRequestRetryTimesHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVSafeTSUpdateCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVSecondaryLockCleanupFailureCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVSendReqBySourceSummary` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVSendReqHistogram` | Rust: `tikv_client::stats::tikv_stats` → `tikv_request_total` + `tikv_request_duration_seconds` (src/stats.rs) | Tests: `src/metrics.rs (gather_contains_core_metrics)`
- [x] `TiKVSmallReadDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVStaleReadBytes` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVStaleReadCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVStaleReadReqCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVStaleRegionFromPDCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVStatusCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVStatusDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVStoreLimitErrorCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVStoreLivenessGauge` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVStoreSlowScoreGauge` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTSFutureWaitDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTTLLifeTimeReachCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTTLManagerHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTokenWaitDuration` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTwoPCTxnCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnCmdHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnCommitBackoffCount` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnCommitBackoffSeconds` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnHeartBeatHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnLagCommitTSAttemptHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnLagCommitTSWaitHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnRegionsNumHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnWriteConflictCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnWriteKVCountHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVTxnWriteSizeHistogram` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVUnsafeDestroyRangeFailuresCounterVec` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TiKVValidateReadTSFromPDCount` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TwoPCTxnCounterError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TwoPCTxnCounterOk` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithBatchGetGeneral` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithBatchGetInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithCommitGeneral` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithCommitInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithGetGeneral` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithGetInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithLockKeysGeneral` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithLockKeysInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithRollbackGeneral` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnCmdHistogramWithRollbackInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnHeartBeatHistogramError` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnHeartBeatHistogramOK` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramCleanup` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramCleanupInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramCommit` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramCommitInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramPessimisticLock` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramPessimisticLockInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramPessimisticRollback` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramPessimisticRollbackInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramPrewrite` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramPrewriteInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramWithBatchCoprocessor` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramWithBatchCoprocessorInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramWithCoprocessor` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramWithCoprocessorInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramWithSnapshot` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnRegionsNumHistogramWithSnapshotInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnWriteKVCountHistogramGeneral` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnWriteKVCountHistogramInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnWriteSizeHistogramGeneral` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A
- [x] `TxnWriteSizeHistogramInternal` | Rust: N/A (out-of-scope: does not expose per-metric handles/shortcuts) | Tests: N/A

### Methods
- [x] `func (c TxnCommitCounter) Sub(rhs TxnCommitCounter) TxnCommitCounter` | Rust: N/A (out-of-scope: does not expose per-metric handles) | Tests: N/A

## oracle (package oracle)

### Types
- [x] `type ErrFutureTSRead struct` | Rust: N/A (out-of-scope: oracle read-ts validation errors not exposed) | Tests: N/A
- [x] `type ErrLatestStaleRead struct` | Rust: N/A (out-of-scope: oracle read-ts validation errors not exposed) | Tests: N/A
- [x] `type Future interface` | Rust: N/A (out-of-scope: Rust uses `async`/`await` futures directly) | Tests: N/A
- [x] `type NoopReadTSValidator struct` | Rust: N/A (out-of-scope: read-ts validation not exposed) | Tests: N/A
- [x] `type Option struct` | Rust: N/A (out-of-scope: no txn-scope Oracle options; latest TiKV only) | Tests: N/A
- [x] `type Oracle interface` | Rust: N/A (out-of-scope: timestamp oracle is internal; use `TransactionClient::current_timestamp`) | Tests: N/A
- [x] `type ReadTSValidator interface` | Rust: N/A (out-of-scope: read-ts validation not exposed) | Tests: N/A

### Functions
- [x] `func ComposeTS(physical, logical int64) uint64` | Rust: `tikv_client::compose_ts` (src/timestamp.rs) | Tests: `src/timestamp.rs (ts_parts_round_trip)`
- [x] `func ExtractLogical(ts uint64) int64` | Rust: `tikv_client::extract_logical` (src/timestamp.rs) | Tests: `src/timestamp.rs (ts_parts_round_trip)`
- [x] `func ExtractPhysical(ts uint64) int64` | Rust: `tikv_client::extract_physical` (src/timestamp.rs) | Tests: `src/timestamp.rs (ts_parts_round_trip)`
- [x] `func GetPhysical(t time.Time) int64` | Rust: `tikv_client::get_physical(SystemTime)` (src/timestamp.rs) | Tests: `src/timestamp.rs (system_time_round_trip_is_ms_precision)`
- [x] `func GetTimeFromTS(ts uint64) time.Time` | Rust: `tikv_client::get_time_from_ts` (src/timestamp.rs) | Tests: `src/timestamp.rs (system_time_round_trip_is_ms_precision)`
- [x] `func GoTimeToLowerLimitStartTS(now time.Time, maxTxnTimeUse int64) uint64` | Rust: `tikv_client::lower_limit_start_ts(now, Duration)` (src/timestamp.rs) | Tests: `src/timestamp.rs (lower_limit_matches_subtracted_time)`
- [x] `func GoTimeToTS(t time.Time) uint64` | Rust: `tikv_client::time_to_ts` (src/timestamp.rs) | Tests: `src/timestamp.rs (system_time_round_trip_is_ms_precision)`

### Consts
- [x] `GlobalTxnScope` | Rust: `tikv_client::GLOBAL_TXN_SCOPE` (src/timestamp.rs) | Tests: N/A

### Vars
- (none)

### Methods
- [x] `func (ErrLatestStaleRead) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (NoopReadTSValidator) ValidateReadTS(ctx context.Context, readTS uint64, isStaleRead bool, opt *Option) error` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (e ErrFutureTSRead) Error() string` | Rust: N/A (out-of-scope) | Tests: N/A

## oracle/oracles (package oracles)

### Types
- [x] `type MockOracle struct` | Rust: N/A (out-of-scope: oracle impls are internal) | Tests: N/A
- [x] `type PDOracleOptions struct` | Rust: N/A (out-of-scope: oracle impls are internal) | Tests: N/A
- [x] `type ValidateReadTSForTidbSnapshot struct` | Rust: N/A (out-of-scope: TiDB snapshot validator not exposed) | Tests: N/A

### Functions
- [x] `func NewLocalOracle() oracle.Oracle` | Rust: N/A (out-of-scope: local oracle not exposed) | Tests: N/A
- [x] `func NewPdOracle(pdClient pd.Client, options *PDOracleOptions) (oracle.Oracle, error)` | Rust: N/A (out-of-scope: PD oracle is internal to clients) | Tests: N/A

### Consts
- (none)

### Vars
- [x] `EnableTSValidation` | Rust: N/A (out-of-scope: TiDB integration knob not exposed) | Tests: N/A

### Methods
- [x] `func (o *MockOracle) AddOffset(d time.Duration)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) Close()` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) Disable()` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) Enable()` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) GetAllTSOKeyspaceGroupMinTS(ctx context.Context) (uint64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) GetExternalTimestamp(ctx context.Context) (uint64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) GetLowResolutionTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) GetLowResolutionTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) GetStaleTimestamp(ctx context.Context, txnScope string, prevSecond uint64) (ts uint64, err error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) GetTimestamp(ctx context.Context, _ *oracle.Option) (uint64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) GetTimestampAsync(ctx context.Context, _ *oracle.Option) oracle.Future` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) IsExpired(lockTimestamp, TTL uint64, _ *oracle.Option) bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) SetExternalTimestamp(ctx context.Context, newTimestamp uint64) error` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) SetLowResolutionTimestampUpdateInterval(time.Duration) error` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) UntilExpired(lockTimeStamp, TTL uint64, _ *oracle.Option) int64` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (o *MockOracle) ValidateReadTS(ctx context.Context, readTS uint64, isStaleRead bool, opt *oracle.Option) error` | Rust: N/A (out-of-scope) | Tests: N/A

## rawkv (package rawkv)

### Types
- [x] `type Client struct` | Rust: `tikv_client::RawClient` (src/raw/client.rs) | Tests: `src/raw/client.rs (tests)`
- [x] `type ClientOpt func(*option)` | Rust: N/A (out-of-scope: Rust uses explicit `Config` on `RawClient::new_with_config`) | Tests: N/A
- [x] `type ClientProbe struct` | Rust: N/A (out-of-scope: Go probe/test hooks not exposed) | Tests: N/A
- [x] `type ConfigProbe struct` | Rust: N/A (out-of-scope: Go probe/test hooks not exposed) | Tests: N/A
- [x] `type RawChecksum struct` | Rust: `tikv_client::RawChecksum` (src/raw/mod.rs) | Tests: `src/raw/requests.rs (test_raw_checksum_merge)`
- [x] `type RawOption interface` | Rust: N/A (out-of-scope: Rust uses typed methods like `with_cf`/`scan_keys`) | Tests: N/A

### Functions
- [x] `func NewClient(ctx context.Context, pdAddrs []string, security config.Security, opts ...opt.ClientOption) (*Client, error)` | Rust: `RawClient::new_with_config` + `Config::with_security` (src/{raw/client.rs,config.rs}) | Tests: `src/common/security.rs (test_security)`
- [x] `func NewClientWithOpts(ctx context.Context, pdAddrs []string, opts ...ClientOpt) (*Client, error)` | Rust: `RawClient::new` / `RawClient::new_with_config` (src/raw/client.rs) | Tests: `src/raw/client.rs (tests)`
- [x] `func ScanKeyOnly() RawOption` | Rust: `RawClient::{scan_keys,scan_keys_reverse}` (src/raw/client.rs) | Tests: N/A
- [x] `func SetColumnFamily(cf string) RawOption` | Rust: `RawClient::with_cf` + `ColumnFamily::try_from` (src/raw/{client.rs,mod.rs}) | Tests: N/A
- [x] `func WithAPIVersion(apiVersion kvrpcpb.APIVersion) ClientOpt` | Rust: N/A (out-of-scope: target TiKV uses API v2; use `Config::with_keyspace`) | Tests: N/A
- [x] `func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOpt` | Rust: N/A (out-of-scope: no gRPC dial option passthrough; use `Config`/defaults) | Tests: N/A
- [x] `func WithKeyspace(name string) ClientOpt` | Rust: `Config::with_keyspace` / `Config::with_default_keyspace` (src/config.rs) | Tests: N/A
- [x] `func WithPDOptions(opts ...opt.ClientOption) ClientOpt` | Rust: N/A (out-of-scope: no PD option passthrough; use `Config`/defaults) | Tests: N/A
- [x] `func WithSecurity(security config.Security) ClientOpt` | Rust: `Config::with_security` (src/config.rs) | Tests: `src/common/security.rs (test_security)`

### Consts
- (none)

### Vars
- [x] `ErrMaxScanLimitExceeded` | Rust: `Error::MaxScanLimitExceeded` (src/common/errors.rs) | Tests: N/A
- [x] `MaxRawKVScanLimit` | Rust: `MAX_RAW_KV_SCAN_LIMIT` (src/raw/client.rs) | Tests: N/A

### Methods
- [x] `func (c *Client) BatchDelete(ctx context.Context, keys [][]byte, options ...RawOption) error` | Rust: `RawClient::batch_delete` (src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) BatchGet(ctx context.Context, keys [][]byte, options ...RawOption) ([][]byte, error)` | Rust: `RawClient::batch_get` (src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) BatchPut(ctx context.Context, keys, values [][]byte, options ...RawOption) error` | Rust: `RawClient::batch_put` (src/raw/client.rs) | Tests: `src/raw/client.rs (test_batch_put_with_ttl)`
- [x] `func (c *Client) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, options ...RawOption) error` | Rust: `RawClient::batch_put_with_ttl` (src/raw/client.rs) | Tests: `src/raw/client.rs (test_batch_put_with_ttl)`
- [x] `func (c *Client) Checksum(ctx context.Context, startKey, endKey []byte, options ...RawOption, ) (check RawChecksum, err error)` | Rust: `RawClient::checksum` (src/raw/client.rs) | Tests: `src/raw/requests.rs (test_raw_checksum_merge)`
- [x] `func (c *Client) Close() error` | Rust: N/A (out-of-scope: drop closes client) | Tests: N/A
- [x] `func (c *Client) ClusterID() uint64` | Rust: `RawClient::cluster_id` (src/raw/client.rs) | Tests: `src/raw/client.rs (tests)`
- [x] `func (c *Client) CompareAndSwap(ctx context.Context, key, previousValue, newValue []byte, options ...RawOption) ([]byte, bool, error)` | Rust: `RawClient::compare_and_swap` (src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) Delete(ctx context.Context, key []byte, options ...RawOption) error` | Rust: `RawClient::delete` (src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) DeleteRange(ctx context.Context, startKey []byte, endKey []byte, options ...RawOption) error` | Rust: `RawClient::delete_range` (src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) Get(ctx context.Context, key []byte, options ...RawOption) ([]byte, error)` | Rust: `RawClient::get` (src/raw/client.rs) | Tests: `src/raw/client.rs (test_request_source_and_resource_group_tag)`
- [x] `func (c *Client) GetKeyTTL(ctx context.Context, key []byte, options ...RawOption) (*uint64, error)` | Rust: `RawClient::get_key_ttl_secs` (src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) GetPDClient() pd.Client` | Rust: N/A (out-of-scope: PD client is internal; use `request::PlanBuilder`/clients) | Tests: N/A
- [x] `func (c *Client) Put(ctx context.Context, key, value []byte, options ...RawOption) error` | Rust: `RawClient::put` (src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64, options ...RawOption) error` | Rust: `RawClient::put_with_ttl` (src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int, options ...RawOption) (keys [][]byte, values [][]byte, err error)` | Rust: `RawClient::scan_reverse` (src/raw/client.rs) | Tests: `src/raw/requests.rs (test_raw_scan)`
- [x] `func (c *Client) Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...RawOption, ) (keys [][]byte, values [][]byte, err error)` | Rust: `RawClient::scan` (src/raw/client.rs) | Tests: `src/raw/requests.rs (test_raw_scan)`
- [x] `func (c *Client) SetAtomicForCAS(b bool) *Client` | Rust: `RawClient::with_atomic_for_cas` (src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) SetColumnFamily(columnFamily string) *Client` | Rust: `RawClient::with_cf` + `ColumnFamily::try_from` (src/raw/{client.rs,mod.rs}) | Tests: N/A
- [x] `func (c ClientProbe) GetRegionCache() *locate.RegionCache` | Rust: N/A (out-of-scope: Go probe/test hooks not exposed) | Tests: N/A
- [x] `func (c ClientProbe) SetPDClient(client pd.Client)` | Rust: N/A (out-of-scope: Go probe/test hooks not exposed) | Tests: N/A
- [x] `func (c ClientProbe) SetRPCClient(client client.Client)` | Rust: N/A (out-of-scope: Go probe/test hooks not exposed) | Tests: N/A
- [x] `func (c ClientProbe) SetRegionCache(regionCache *locate.RegionCache)` | Rust: N/A (out-of-scope: Go probe/test hooks not exposed) | Tests: N/A
- [x] `func (c ConfigProbe) GetRawBatchPutSize() int` | Rust: N/A (out-of-scope: Go probe/test hooks not exposed) | Tests: N/A

## tikv (package tikv)

### Types
- [x] `type BackoffConfig = retry.Config` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Backoffer = retry.Backoffer` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type BaseRegionLockResolver struct` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type BatchLocateKeyRangesOpt = locate.BatchLocateKeyRangesOpt` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type BinlogWriteResult = transaction.BinlogWriteResult` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Client = client.Client` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type ClientEventListener = client.ClientEventListener` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type ClientOpt = client.Opt` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Codec = apicodec.Codec` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type CodecClient struct` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type CodecPDClient = locate.CodecPDClient` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type ConfigProbe struct` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type EtcdSafePointKV struct` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type GCOpt func(*gcOption)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Getter = kv.Getter` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Iterator = unionstore.Iterator` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type KVFilter = transaction.KVFilter` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type KVStore struct` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type KVTxn = transaction.KVTxn` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type KeyLocation = locate.KeyLocation` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type KeyRange = kv.KeyRange` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type KeyspaceID = apicodec.KeyspaceID` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type LabelFilter = locate.LabelFilter` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type LockResolverProbe struct` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type MemBuffer = unionstore.MemBuffer` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type MemBufferSnapshot = unionstore.MemBufferSnapshot` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type MemDB = unionstore.MemDB` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type MemDBCheckpoint = unionstore.MemDBCheckpoint` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Metrics = unionstore.Metrics` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type MockSafePointKV struct` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Mode = apicodec.Mode` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Option func(*KVStore)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Pool interface` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type RPCCanceller = locate.RPCCanceller` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type RPCCancellerCtxKey = locate.RPCCancellerCtxKey` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type RPCContext = locate.RPCContext` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type RPCRuntimeStats = locate.RPCRuntimeStats` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Region = locate.Region` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type RegionCache = locate.RegionCache` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type RegionLockResolver interface` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type RegionRequestRuntimeStats = locate.RegionRequestRuntimeStats` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type RegionRequestSender = locate.RegionRequestSender` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type RegionVerID = locate.RegionVerID` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type SafePointKV interface` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type SafePointKVOpt func(*option)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type SchemaLeaseChecker = transaction.SchemaLeaseChecker` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type SchemaVer = transaction.SchemaVer` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Spool struct` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Storage interface` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Store = locate.Store` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type StoreProbe struct` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type StoreSelectorOption = locate.StoreSelectorOption` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type TxnOption func(*transaction.TxnOptions)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `type Variables = kv.Variables` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 

### Functions
- [x] `func BoPDRPC() *BackoffConfig` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func BoRegionMiss() *BackoffConfig` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func BoTiFlashRPC() *BackoffConfig` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func BoTiKVRPC() *BackoffConfig` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func BoTxnLock() *BackoffConfig` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func ChangePDRegionMetaCircuitBreakerSettings(apply func(config *circuitbreaker.Settings))` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func CodecV1ExcludePrefixes() [][]byte` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func CodecV2Prefixes() [][]byte` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func DisableResourceControl()` | Rust: N/A (out-of-scope: no global toggle; use per-client/plan context + interceptors) | Tests: N/A
- [x] `func EnableResourceControl()` | Rust: N/A (out-of-scope: no global toggle; use per-client/plan context + interceptors) | Tests: N/A
- [x] `func GetStoreTypeByMeta(store *metapb.Store) tikvrpc.EndpointType` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func LoadShuttingDown() uint32` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewEtcdSafePointKV(addrs []string, tlsConfig *tls.Config, opts ...SafePointKVOpt) (*EtcdSafePointKV, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewGcResolveLockMaxBackoffer(ctx context.Context) *Backoffer` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewKVStore( uuid string, pdClient pd.Client, spkv SafePointKV, tikvclient Client, opt ...Option, ) (_ *KVStore, retErr error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`, internal store wiring is not public) | Tests: N/A
- [x] `func NewLockResolver(etcdAddrs []string, security config.Security, opts ...opt.ClientOption) ( *txnlock.LockResolver, error, )` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewLockResolverProb(r *txnlock.LockResolver) *LockResolverProbe` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewMockSafePointKV(opts ...SafePointKVOpt) *MockSafePointKV` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewPDClient(pdAddrs []string) (pd.Client, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewRPCClient(opts ...ClientOpt) *client.RPCClient` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewRPCanceller() *RPCCanceller` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewRegionCache(pdClient pd.Client) *locate.RegionCache` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewRegionLockResolver(identifier string, store Storage) *BaseRegionLockResolver` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewRegionRequestRuntimeStats() *RegionRequestRuntimeStats` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewRegionRequestSender(regionCache *RegionCache, client client.Client, readTSValidator oracle.ReadTSValidator) *RegionRequestSender` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewRegionVerID(id, confVer, ver uint64) RegionVerID` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewSpool(n int, dur time.Duration) *Spool` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewTestKeyspaceTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client, txnLocalLatches uint, keyspaceMeta keyspacepb.KeyspaceMeta, opt ...Option) (*KVStore, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func NewTestTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client, txnLocalLatches uint, opt ...Option) (*KVStore, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func ResolveLocksForRange( ctx context.Context, resolver RegionLockResolver, maxVersion uint64, startKey []byte, endKey []byte, createBackoffFn func(context.Context) *Backoffer, scanLimit uint32, ) (rangetask.TaskStat, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func SetLogContextKey(key interface{})` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func SetRegionCacheTTLSec(t int64)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func SetRegionCacheTTLWithJitter(base int64, jitter int64)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func SetResourceControlInterceptor(interceptor resourceControlClient.ResourceGroupKVInterceptor)` | Rust: `interceptor::RpcInterceptor` + `RawClient/TransactionClient/PlanBuilder::{with_rpc_interceptor,with_added_rpc_interceptor}` | Tests: `src/request/plan_builder.rs (test_plan_builder_request_context_and_interceptors)`
- [x] `func SetStoreLivenessTimeout(t time.Duration)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func StoreShuttingDown(v uint32)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func TxnStartKey() interface{}` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func UnsetResourceControlInterceptor()` | Rust: N/A (out-of-scope: no global interceptor; configure on client/plan) | Tests: N/A
- [x] `func WithCodec(codec apicodec.Codec) ClientOpt` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithConcurrency(concurrency int) GCOpt` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithDefaultPipelinedTxn() TxnOption` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithLogContext(ctx context.Context, logger *zap.Logger) context.Context` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithMatchLabels(labels []*metapb.StoreLabel) StoreSelectorOption` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithMatchStores(stores []uint64) StoreSelectorOption` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithPDHTTPClient( source string, pdAddrs []string, opts ...pdhttp.ClientOption, ) Option` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithPipelinedTxn( flushConcurrency, resolveLockConcurrency int, writeThrottleRatio float64, ) TxnOption` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithPool(gp Pool) Option` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithPrefix(prefix string) SafePointKVOpt` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithSecurity(security config.Security) ClientOpt` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithStartTS(startTS uint64) TxnOption` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithTxnScope(txnScope string) TxnOption` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func WithUpdateInterval(updateInterval time.Duration) Option` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 

### Consts
- [x] `CodecV2RawKeyspacePrefix` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `CodecV2TxnKeyspacePrefix` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `DCLabelKey` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `EpochNotMatch` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `GCScanLockLimit` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `GcSavedSafePoint` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `GcStateCacheInterval` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `MaxTxnTimeUse` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `MaxWriteExecutionTime` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `ModeRaw` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `ModeTxn` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `NullspaceID` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `ReadTimeoutMedium` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `ReadTimeoutShort` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 

### Vars
- [x] `DecodeKey` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `DefaultKeyspaceID` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `DefaultKeyspaceName` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `EnableFailpoints` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `LabelFilterAllNode` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `LabelFilterAllTiFlashNode` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `LabelFilterNoTiFlashWriteNode` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `LabelFilterOnlyTiFlashWriteNode` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `NewCodecPDClient` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `NewCodecPDClientWithKeyspace` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `NewCodecV1` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `NewCodecV2` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `NewNoopBackoff` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `WithNeedBuckets` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `WithNeedRegionHasLeaderPeer` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 

### Methods
- [x] `func (c *CodecClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c *CodecClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response])` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) GetBigTxnThreshold() int` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) GetDefaultLockTTL() uint64` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) GetGetMaxBackoff() int` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) GetScanBatchSize() int` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) GetTTLFactor() int` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) GetTxnCommitBatchSize() uint64` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) LoadPreSplitDetectThreshold() uint32` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) LoadPreSplitSizeThreshold() uint32` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) SetOracleUpdateInterval(v int)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) StorePreSplitDetectThreshold(v uint32)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (c ConfigProbe) StorePreSplitSizeThreshold(v uint32)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (l *BaseRegionLockResolver) GetStore() Storage` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (l *BaseRegionLockResolver) Identifier() string` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (l *BaseRegionLockResolver) ResolveLocksInOneRegion(bo *Backoffer, locks []*txnlock.Lock, loc *locate.KeyLocation) (*locate.KeyLocation, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (l *BaseRegionLockResolver) ScanLocksInOneRegion(bo *Backoffer, key []byte, endKey []byte, maxVersion uint64, scanLimit uint32) ([]*txnlock.Lock, *locate.KeyLocation, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (l LockResolverProbe) ForceResolveLock(ctx context.Context, lock *txnlock.Lock) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (l LockResolverProbe) ResolveLock(ctx context.Context, lock *txnlock.Lock) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (l LockResolverProbe) ResolvePessimisticLock(ctx context.Context, lock *txnlock.Lock) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (p *Spool) Run(fn func()) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) Begin(opts ...TxnOption) (txn *transaction.KVTxn, err error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) CheckRegionInScattering(regionID uint64) (bool, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) CheckVisibility(startTS uint64) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) Close() error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) Closed() <-chan struct{}` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) Ctx() context.Context` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) CurrentAllTSOKeyspaceGroupMinTs() (uint64, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) CurrentTimestamp(txnScope string) (uint64, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) DeleteRange( ctx context.Context, startKey []byte, endKey []byte, concurrency int, ) (completedRegions int, err error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) EnableTxnLocalLatches(size uint)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GC(ctx context.Context, expectedSafePoint uint64, opts ...GCOpt) (newGCSafePoint uint64, err error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetClusterID() uint64` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetLockResolver() *txnlock.LockResolver` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetMinSafeTS(txnScope string) uint64` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetOracle() oracle.Oracle` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetPDClient() pd.Client` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetPDHTTPClient() pdhttp.Client` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetRegionCache() *locate.RegionCache` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetSafePointKV() SafePointKV` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetSnapshot(ts uint64) *txnsnapshot.KVSnapshot` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetTiKVClient() (client Client)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) GetTimestampWithRetry(bo *Backoffer, scope string) (uint64, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) Go(f func()) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) IsClose() bool` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) IsLatchEnabled() bool` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) SendReq( bo *Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, ) (*tikvrpc.Response, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) SetOracle(oracle oracle.Oracle)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) SetTiKVClient(client Client)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) SplitRegions(ctx context.Context, splitKeys [][]byte, scatter bool, tableID *int64) (regionIDs []uint64, err error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) SupportDeleteRange() (supported bool)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) TxnLatches() *latch.LatchesScheduler` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) UUID() string` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) UnsafeDestroyRange(ctx context.Context, startKey []byte, endKey []byte) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) UpdateTxnSafePointCache(txnSafePoint uint64, now time.Time)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) WaitGroup() *sync.WaitGroup` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s *KVStore) WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) Begin(opts ...TxnOption) (transaction.TxnProbe, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) ClearTxnLatches()` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) GCResolveLockPhase(ctx context.Context, safepoint uint64, concurrency int) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) GetCompatibleTxnSafePointLoaderUnderlyingEtcdClient() *clientv3.Client` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) GetGCStatesClient() pdgc.GCStatesClient` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) GetSnapshot(ts uint64) txnsnapshot.SnapshotProbe` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) LoadSafePointFromSafePointKV() (uint64, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) LoadTxnSafePoint(ctx context.Context) (uint64, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) NewLockResolver() LockResolverProbe` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) ReplaceGCStatesClient(c pdgc.GCStatesClient)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) SaveSafePointToSafePointKV(v uint64) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) ScanLocks(ctx context.Context, startKey, endKey []byte, maxVersion uint64) ([]*txnlock.Lock, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) SendTxnHeartbeat(ctx context.Context, key []byte, startTS uint64, ttl uint64) (uint64, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) SetRegionCachePDClient(client pd.Client)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) SetRegionCacheStore(id uint64, storeType tikvrpc.EndpointType, state uint64, labels []*metapb.StoreLabel)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) SetSafeTS(storeID, safeTS uint64)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (s StoreProbe) UpdateTxnSafePointCache(txnSafePoint uint64, now time.Time)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (w *EtcdSafePointKV) Close() error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (w *EtcdSafePointKV) Get(k string) (string, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (w *EtcdSafePointKV) GetWithPrefix(k string) ([]*mvccpb.KeyValue, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (w *EtcdSafePointKV) Put(k string, v string) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (w *MockSafePointKV) Close() error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (w *MockSafePointKV) Get(k string) (string, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (w *MockSafePointKV) GetWithPrefix(prefix string) ([]*mvccpb.KeyValue, error)` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 
- [x] `func (w *MockSafePointKV) Put(k string, v string) error` | Rust: N/A (out-of-scope: Rust exposes `RawClient`/`TransactionClient`; internal store/locate/unionstore wiring is not public) | Tests: N/A 

## tikvrpc (package tikvrpc)

### Types
- [x] `type BatchCopStreamResponse struct` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `type CmdType uint16` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`, no `CmdType` switch layer) | Tests: N/A
- [x] `type CopStreamResponse struct` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `type EndpointType uint8` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `type Lease struct` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `type MPPStreamResponse struct` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `type Request struct` | Rust: N/A (capability: kvproto request types + `tikv_client::request::PlanBuilder`) | Tests: `src/request/plan_builder.rs (test_plan_builder_request_context_and_interceptors)`
- [x] `type ResourceGroupTagger func(req *Request)` | Rust: `tikv_client::interceptor::ResourceGroupTagger` (src/interceptor.rs) | Tests: `src/raw/client.rs (test_request_source_and_resource_group_tag)`
- [x] `type Response struct` | Rust: N/A (capability: typed kvproto response types + `request::{Merge,Process}`) | Tests: `src/request/plan.rs (tests)`
- [x] `type ResponseExt struct` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 

### Functions
- [x] `func AttachContext(req *Request, rpcCtx kvrpcpb.Context) bool` | Rust: `request::PlanBuilder::{with_request_source,with_resource_group_tag,with_resource_group_name,...}` (src/request/plan_builder.rs) | Tests: `src/request/plan_builder.rs (test_plan_builder_request_context_and_interceptors)`
- [x] `func CallDebugRPC(ctx context.Context, client debugpb.DebugClient, req *Request) (*Response, error)` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func CallRPC(ctx context.Context, client tikvpb.TikvClient, req *Request) (*Response, error)` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func CheckStreamTimeoutLoop(ch <-chan *Lease, done <-chan struct{})` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func FromBatchCommandsResponse(res *tikvpb.BatchCommandsResponse_Response) (*Response, error)` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error)` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func GetStoreTypeByMeta(store *metapb.Store) EndpointType` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func NewReplicaReadRequest(typ CmdType, pointer interface{}, replicaReadType kv.ReplicaReadType, replicaReadSeed *uint32, ctxs ...kvrpcpb.Context) *Request` | Rust: `PlanBuilder::{replica_read,stale_read,replica_read_seed}` + `ReplicaReadType` (src/request/plan_builder.rs) | Tests: `src/request/plan_builder.rs (test_plan_builder_request_context_and_interceptors)`
- [x] `func NewRequest(typ CmdType, pointer interface{}, ctxs ...kvrpcpb.Context) *Request` | Rust: kvproto request constructors (`raw_lowering`/`transaction_lowering`) + `PlanBuilder::new` | Tests: `src/request/plan_builder.rs (test_plan_builder_request_context_and_interceptors)`
- [x] `func SetContext(req *Request, region *metapb.Region, peer *metapb.Peer) error` | Rust: N/A (capability: PlanBuilder resolves regions and sets context automatically) | Tests: `src/request/plan_builder.rs (test_plan_builder_request_context_and_interceptors)`
- [x] `func SetContextNoAttach(req *Request, region *metapb.Region, peer *metapb.Peer) error` | Rust: N/A (capability: PlanBuilder resolves regions and sets context automatically) | Tests: `src/request/plan_builder.rs (test_plan_builder_request_context_and_interceptors)`

### Consts
- [x] `CmdBatchCop` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdBatchGet` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdBatchRollback` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdBroadcastTxnStatus` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdBufferBatchGet` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdCheckLockObserver` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdCheckSecondaryLocks` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdCheckTxnStatus` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdCleanup` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdCommit` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdCompact` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdCop` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdCopStream` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdDebugGetRegionProperties` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdDeleteRange` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdEmpty` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdFlashbackToVersion` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdFlush` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdGC` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdGet` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdGetHealthFeedback` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdGetKeyTTL` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdGetTiFlashSystemTable` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdLockWaitInfo` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdMPPAlive` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdMPPCancel` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdMPPConn` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdMPPTask` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdMvccGetByKey` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdMvccGetByStartTs` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdPessimisticLock` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdPessimisticRollback` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdPhysicalScanLock` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdPrepareFlashbackToVersion` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdPrewrite` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawBatchDelete` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawBatchGet` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawBatchPut` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawChecksum` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawCompareAndSwap` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawDelete` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawDeleteRange` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawGet` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawGetKeyTTL` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawPut` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRawScan` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRegisterLockObserver` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdRemoveLockObserver` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdResolveLock` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdScan` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdScanLock` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdSplitRegion` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdStoreSafeTS` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdTxnHeartBeat` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `CmdUnsafeDestroyRange` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `EngineLabelKey` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `EngineLabelTiFlash` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `EngineLabelTiFlashCompute` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `EngineRoleLabelKey` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `EngineRoleWrite` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `TiDB` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `TiFlash` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `TiFlashCompute` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `TiKV` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 

### Vars
- (none)

### Methods
- [x] `func (req *Request) BatchCop() *coprocessor.BatchRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) BatchGet() *kvrpcpb.BatchGetRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) BatchRollback() *kvrpcpb.BatchRollbackRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) BroadcastTxnStatus() *kvrpcpb.BroadcastTxnStatusRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) BufferBatchGet() *kvrpcpb.BufferBatchGetRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) CancelMPPTask() *mpp.CancelTaskRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) CheckLockObserver() *kvrpcpb.CheckLockObserverRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) CheckSecondaryLocks() *kvrpcpb.CheckSecondaryLocksRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) CheckTxnStatus() *kvrpcpb.CheckTxnStatusRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) Cleanup() *kvrpcpb.CleanupRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) Commit() *kvrpcpb.CommitRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) Compact() *kvrpcpb.CompactRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) Cop() *coprocessor.Request` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) DebugGetRegionProperties() *debugpb.GetRegionPropertiesRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) DeleteRange() *kvrpcpb.DeleteRangeRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) DisableStaleReadMeetLock()` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) DispatchMPPTask() *mpp.DispatchTaskRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) Empty() *tikvpb.BatchCommandsEmptyRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) EnableStaleWithMixedReplicaRead()` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) EstablishMPPConn() *mpp.EstablishMPPConnectionRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) FlashbackToVersion() *kvrpcpb.FlashbackToVersionRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) Flush() *kvrpcpb.FlushRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) GC() *kvrpcpb.GCRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) Get() *kvrpcpb.GetRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) GetHealthFeedback() *kvrpcpb.GetHealthFeedbackRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) GetReplicaReadSeed() *uint32` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) GetSize() int` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) GetStartTS() uint64` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) GetTiFlashSystemTable() *kvrpcpb.TiFlashSystemTableRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) IsDebugReq() bool` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) IsGlobalStaleRead() bool` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) IsGreenGCRequest() bool` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) IsInterruptible() bool` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) IsMPPAlive() *mpp.IsAliveRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) IsRawWriteRequest() bool` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) IsTxnWriteRequest() bool` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) LockWaitInfo() *kvrpcpb.GetLockWaitInfoRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) MvccGetByKey() *kvrpcpb.MvccGetByKeyRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) MvccGetByStartTs() *kvrpcpb.MvccGetByStartTsRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) PessimisticLock() *kvrpcpb.PessimisticLockRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) PessimisticRollback() *kvrpcpb.PessimisticRollbackRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) PhysicalScanLock() *kvrpcpb.PhysicalScanLockRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) PrepareFlashbackToVersion() *kvrpcpb.PrepareFlashbackToVersionRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) Prewrite() *kvrpcpb.PrewriteRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawBatchDelete() *kvrpcpb.RawBatchDeleteRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawBatchGet() *kvrpcpb.RawBatchGetRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawBatchPut() *kvrpcpb.RawBatchPutRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawChecksum() *kvrpcpb.RawChecksumRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawCompareAndSwap() *kvrpcpb.RawCASRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawDelete() *kvrpcpb.RawDeleteRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawDeleteRange() *kvrpcpb.RawDeleteRangeRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawGet() *kvrpcpb.RawGetRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawGetKeyTTL() *kvrpcpb.RawGetKeyTTLRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawPut() *kvrpcpb.RawPutRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RawScan() *kvrpcpb.RawScanRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RegisterLockObserver() *kvrpcpb.RegisterLockObserverRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) RemoveLockObserver() *kvrpcpb.RemoveLockObserverRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) ResolveLock() *kvrpcpb.ResolveLockRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) Scan() *kvrpcpb.ScanRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) ScanLock() *kvrpcpb.ScanLockRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) SetReplicaReadType(replicaReadType kv.ReplicaReadType)` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) SplitRegion() *kvrpcpb.SplitRegionRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) StoreSafeTS() *kvrpcpb.StoreSafeTSRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) ToBatchCommandsRequest() *tikvpb.BatchCommandsRequest_Request` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) TxnHeartBeat() *kvrpcpb.TxnHeartBeatRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (req *Request) UnsafeDestroyRange() *kvrpcpb.UnsafeDestroyRangeRequest` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (resp *BatchCopStreamResponse) Close()` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (resp *BatchCopStreamResponse) Recv() (*coprocessor.BatchResponse, error)` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (resp *CopStreamResponse) Close()` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (resp *CopStreamResponse) Recv() (*coprocessor.Response, error)` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (resp *MPPStreamResponse) Close()` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (resp *MPPStreamResponse) Recv() (*mpp.MPPDataPacket, error)` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (resp *Response) GetExecDetailsV2() *kvrpcpb.ExecDetailsV2` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (resp *Response) GetRegionError() (*errorpb.Error, error)` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (resp *Response) GetSize() int` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (t CmdType) String() string` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (t EndpointType) IsTiFlashRelatedType() bool` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 
- [x] `func (t EndpointType) Name() string` | Rust: N/A (out-of-scope: Rust uses typed kvproto requests + `request::PlanBuilder`; no `tikvrpc` wrapper layer) | Tests: N/A 

## tikvrpc/interceptor (package interceptor)

### Types
- [x] `type MockInterceptorManager struct` | Rust: `tikv_client::interceptor::MockInterceptorManager` (src/interceptor.rs) | Tests: `src/interceptor.rs (interceptor_chain_wrap_is_onion_order)`
- [x] `type RPCInterceptor interface` | Rust: `tikv_client::interceptor::RpcInterceptor` (src/interceptor.rs) | Tests: `src/raw/client.rs (test_request_source_and_resource_group_tag)`
- [x] `type RPCInterceptorChain struct` | Rust: `tikv_client::interceptor::RpcInterceptorChain` (src/interceptor.rs) | Tests: N/A
- [x] `type RPCInterceptorFunc func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error)` | Rust: `tikv_client::interceptor::RpcInterceptorFunc` (src/interceptor.rs) | Tests: `src/interceptor.rs (interceptor_chain_wrap_is_onion_order)`

### Functions
- [x] `func ChainRPCInterceptors(first RPCInterceptor, rest ...RPCInterceptor) RPCInterceptor` | Rust: `tikv_client::interceptor::chain_rpc_interceptors` (src/interceptor.rs) | Tests: `src/interceptor.rs (chain_rpc_interceptors_dedup_by_name)`
- [x] `func GetRPCInterceptorFromCtx(ctx context.Context) RPCInterceptor` | Rust: N/A (out-of-scope: no Go-style context; configure on PlanBuilder/client) | Tests: N/A
- [x] `func NewMockInterceptorManager() *MockInterceptorManager` | Rust: `MockInterceptorManager::new` (src/interceptor.rs) | Tests: `src/interceptor.rs (interceptor_chain_wrap_is_onion_order)`
- [x] `func NewRPCInterceptor(name string, fn func(next RPCInterceptorFunc) RPCInterceptorFunc) RPCInterceptor` | Rust: `tikv_client::interceptor::rpc_interceptor` (src/interceptor.rs) | Tests: N/A
- [x] `func NewRPCInterceptorChain() *RPCInterceptorChain` | Rust: `RpcInterceptorChain::new` (src/interceptor.rs) | Tests: N/A
- [x] `func WithRPCInterceptor(ctx context.Context, interceptor RPCInterceptor) context.Context` | Rust: N/A (out-of-scope: configure on client via `with_rpc_interceptor`) | Tests: N/A

### Consts
- (none)

### Vars
- (none)

### Methods
- [x] `func (c *RPCInterceptorChain) Len() int` | Rust: `RpcInterceptorChain::len` (src/interceptor.rs) | Tests: N/A
- [x] `func (c *RPCInterceptorChain) Link(it RPCInterceptor) *RPCInterceptorChain` | Rust: `RpcInterceptorChain::link` (src/interceptor.rs) | Tests: N/A
- [x] `func (c *RPCInterceptorChain) Name() string` | Rust: `RpcInterceptorChain::name` (src/interceptor.rs) | Tests: N/A
- [x] `func (c *RPCInterceptorChain) Wrap(next RPCInterceptorFunc) RPCInterceptorFunc` | Rust: `RpcInterceptorChain::wrap` (src/interceptor.rs) | Tests: `src/interceptor.rs (interceptor_chain_wrap_is_onion_order)`
- [x] `func (m *MockInterceptorManager) BeginCount() int` | Rust: `MockInterceptorManager::begin_count` (src/interceptor.rs) | Tests: `src/interceptor.rs (interceptor_chain_wrap_is_onion_order)`
- [x] `func (m *MockInterceptorManager) CreateMockInterceptor(name string) RPCInterceptor` | Rust: `MockInterceptorManager::create_mock_interceptor` (src/interceptor.rs) | Tests: `src/interceptor.rs (interceptor_chain_wrap_is_onion_order)`
- [x] `func (m *MockInterceptorManager) EndCount() int` | Rust: `MockInterceptorManager::end_count` (src/interceptor.rs) | Tests: `src/interceptor.rs (interceptor_chain_wrap_is_onion_order)`
- [x] `func (m *MockInterceptorManager) ExecLog() []string` | Rust: `MockInterceptorManager::exec_log` (src/interceptor.rs) | Tests: `src/interceptor.rs (interceptor_chain_wrap_is_onion_order)`
- [x] `func (m *MockInterceptorManager) Reset()` | Rust: `MockInterceptorManager::reset` (src/interceptor.rs) | Tests: `src/interceptor.rs (interceptor_chain_wrap_is_onion_order)`

## trace (package trace)

### Types
- [x] `type Category uint32` | Rust: `tikv_client::trace::Category` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`
- [x] `type IsCategoryEnabledFunc func(category Category) bool` | Rust: `tikv_client::trace::IsCategoryEnabledFn` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`
- [x] `type TraceControlExtractorFunc func(ctx context.Context) TraceControlFlags` | Rust: N/A (out-of-scope: no Go-style context; set flags explicitly via `PlanBuilder::with_trace_control`) | Tests: N/A
- [x] `type TraceControlFlags uint64` | Rust: `tikv_client::trace::TraceControlFlags` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`
- [x] `type TraceEventFunc func(ctx context.Context, category Category, name string, fields ...zap.Field)` | Rust: `tikv_client::trace::TraceEventFn` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`

### Functions
- [x] `func ContextWithTraceID(ctx context.Context, traceID []byte) context.Context` | Rust: `tikv_client::request::PlanBuilder::with_trace_id` (src/request/plan_builder.rs) | Tests: `src/request/plan_builder.rs (test_plan_builder_request_context_and_interceptors)`
- [x] `func GetTraceControlFlags(ctx context.Context) TraceControlFlags` | Rust: N/A (out-of-scope: no Go-style context; pass flags explicitly via `PlanBuilder::with_trace_control`) | Tests: N/A
- [x] `func ImmediateLoggingEnabled(ctx context.Context) bool` | Rust: N/A (capability: `TraceControlFlags::has(FLAG_IMMEDIATE_LOG)`) | Tests: N/A
- [x] `func IsCategoryEnabled(category Category) bool` | Rust: `tikv_client::trace::is_category_enabled` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`
- [x] `func SetIsCategoryEnabledFunc(fn IsCategoryEnabledFunc)` | Rust: `tikv_client::trace::set_is_category_enabled_fn` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`
- [x] `func SetTraceControlExtractor(fn TraceControlExtractorFunc)` | Rust: N/A (out-of-scope: no Go-style context) | Tests: N/A
- [x] `func SetTraceEventFunc(fn TraceEventFunc)` | Rust: `tikv_client::trace::set_trace_event_fn` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`
- [x] `func TraceEvent(ctx context.Context, category Category, name string, fields ...zap.Field)` | Rust: `tikv_client::trace::trace_event` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`
- [x] `func TraceIDFromContext(ctx context.Context) []byte` | Rust: N/A (out-of-scope: no Go-style context; use `PlanBuilder::with_trace_id`) | Tests: N/A

### Consts
- [x] `CategoryKVRequest` | Rust: `trace::CATEGORY_KV_REQUEST` (src/trace.rs) | Tests: N/A
- [x] `CategoryRegionCache` | Rust: `trace::CATEGORY_REGION_CACHE` (src/trace.rs) | Tests: N/A
- [x] `CategoryTxn2PC` | Rust: `trace::CATEGORY_TXN_2PC` (src/trace.rs) | Tests: N/A
- [x] `CategoryTxnLockResolve` | Rust: `trace::CATEGORY_TXN_LOCK_RESOLVE` (src/trace.rs) | Tests: N/A
- [x] `FlagImmediateLog` | Rust: `trace::FLAG_IMMEDIATE_LOG` (src/trace.rs) | Tests: N/A
- [x] `FlagTiKVCategoryReadDetails` | Rust: `trace::FLAG_TIKV_CATEGORY_READ_DETAILS` (src/trace.rs) | Tests: N/A
- [x] `FlagTiKVCategoryRequest` | Rust: `trace::FLAG_TIKV_CATEGORY_REQUEST` (src/trace.rs) | Tests: N/A
- [x] `FlagTiKVCategoryWriteDetails` | Rust: `trace::FLAG_TIKV_CATEGORY_WRITE_DETAILS` (src/trace.rs) | Tests: N/A

### Vars
- (none)

### Methods
- [x] `func (f TraceControlFlags) Has(flag TraceControlFlags) bool` | Rust: `TraceControlFlags::has` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`
- [x] `func (f TraceControlFlags) With(flag TraceControlFlags) TraceControlFlags` | Rust: `TraceControlFlags::with` (src/trace.rs) | Tests: `src/trace.rs (trace_event_is_guarded_by_category)`

## txnkv (package txnkv)

### Types
- [x] `type BinlogWriteResult = transaction.BinlogWriteResult` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type Client struct` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type ClientOpt func(*option)` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type IsoLevel = txnsnapshot.IsoLevel` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type KVFilter = transaction.KVFilter` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type KVSnapshot = txnsnapshot.KVSnapshot` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type KVTxn = transaction.KVTxn` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type Lock = txnlock.Lock` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type LockResolver = txnlock.LockResolver` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type Priority = txnutil.Priority` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type ReplicaReadAdjuster = txnsnapshot.ReplicaReadAdjuster` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type Scanner = txnsnapshot.Scanner` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type SchemaLeaseChecker = transaction.SchemaLeaseChecker` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type SchemaVer = transaction.SchemaVer` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type SnapshotRuntimeStats = txnsnapshot.SnapshotRuntimeStats` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `type TxnStatus = txnlock.TxnStatus` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 

### Functions
- [x] `func NewClient(pdAddrs []string, opts ...ClientOpt) (*Client, error)` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `func NewLock(l *kvrpcpb.LockInfo) *Lock` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `func WithAPIVersion(apiVersion kvrpcpb.APIVersion) ClientOpt` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `func WithKeyspace(keyspaceName string) ClientOpt` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `func WithSafePointKVPrefix(prefix string) ClientOpt` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 

### Consts
- [x] `MaxTxnTimeUse` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `PriorityHigh` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `PriorityLow` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `PriorityNormal` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `RC` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `RCCheckTS` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 
- [x] `SI` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 

### Vars
- (none)

### Methods
- [x] `func (c *Client) GetTimestamp(ctx context.Context) (uint64, error)` | Rust: N/A (out-of-scope: Rust does not mirror Go package split; use `TransactionClient`/`Transaction`) | Tests: N/A 

## txnkv/rangetask (package rangetask)

### Types
- [x] `type DeleteRangeTask struct` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `type Runner struct` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `type TaskHandler = func(ctx context.Context, r kv.KeyRange) (TaskStat, error)` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `type TaskStat struct` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 

### Functions
- [x] `func NewDeleteRangeTask(store storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func NewLocateRegionBackoffer(ctx context.Context) *retry.Backoffer` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func NewNotifyDeleteRangeTask(store storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func NewRangeTaskRunner( name string, store storage, concurrency int, handler TaskHandler, ) *Runner` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func NewRangeTaskRunnerWithID( name string, identifier string, store storage, concurrency int, handler TaskHandler, ) *Runner` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 

### Consts
- (none)

### Vars
- (none)

### Methods
- [x] `func (s *Runner) CompletedRegions() int` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func (s *Runner) FailedRegions() int` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func (s *Runner) RunOnRange(ctx context.Context, startKey, endKey []byte) error` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func (s *Runner) SetRegionsPerTask(regionsPerTask int)` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func (s *Runner) SetStatLogInterval(interval time.Duration)` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func (t *DeleteRangeTask) CompletedRegions() int` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 
- [x] `func (t *DeleteRangeTask) Execute(ctx context.Context) error` | Rust: N/A (out-of-scope: Rust does not expose Go rangetask helpers) | Tests: N/A 

## txnkv/transaction (package transaction)

### Types
- [x] `type AggressiveLockedKeyInfo struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type BatchBufferGetter interface` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type BatchSnapshotBufferGetter interface` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type BinlogExecutor interface` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type BinlogWriteResult interface` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type BufferBatchGetter struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type BufferSnapshotBatchGetter struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type CommitterMutationFlags uint8` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type CommitterMutations interface` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type CommitterProbe struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type ConfigProbe struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type KVFilter interface` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type KVTxn struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type LifecycleHooks struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type MemBufferMutationsProbe struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type PipelinedTxnOptions struct` | Rust: `src/transaction/transaction.rs (PipelinedTxnOptions)` | Tests: `src/transaction/transaction.rs (pipelined tests)`
- [x] `type PlainMutation struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type PlainMutations struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type PrewriteEncounterLockPolicy int` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type RelatedSchemaChange struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type SchemaLeaseChecker interface` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type SchemaVer interface` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type TxnInfo struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type TxnOptions struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type TxnProbe struct` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `type WriteAccessLevel int` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 

### Functions
- [x] `func NewBufferBatchGetter(buffer BatchBufferGetter, snapshot kv.BatchGetter) *BufferBatchGetter` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func NewBufferSnapshotBatchGetter(buffer BatchSnapshotBufferGetter, snapshot kv.BatchGetter) *BufferSnapshotBatchGetter` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func NewMemBufferMutationsProbe(sizeHint int, storage *unionstore.MemDB) MemBufferMutationsProbe` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func NewPlainMutations(sizeHint int) PlainMutations` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func NewTiKVTxn(store kvstore, snapshot *txnsnapshot.KVSnapshot, startTS uint64, options *TxnOptions) (*KVTxn, error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func SendTxnHeartBeat(bo *retry.Backoffer, store kvstore, primary []byte, startTS, ttl uint64) (newTTL uint64, stopHeartBeat bool, err error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 

### Consts
- [x] `CommitSecondaryMaxBackoff` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `MaxExecTimeExceededSignal` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `MaxTxnTimeUse` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `MutationFlagIsAssertExists` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `MutationFlagIsAssertNotExists` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `MutationFlagIsPessimisticLock` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `MutationFlagNeedConstraintCheckInPrewrite` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `NoResolvePolicy` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `PipelinedRequestSource` | Rust: `src/transaction/pipelined.rs (PIPELINED_REQUEST_SOURCE)` | Tests: `src/transaction/transaction.rs (test_pipelined_flush_commit_and_resolve_locks)`
- [x] `TryResolvePolicy` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `TsoMaxBackoff` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 

### Vars
- [x] `CommitMaxBackoff` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `CtxInGetTimestampForCommitKey` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `ManagedLockTTL` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `MaxPipelinedTxnTTL` | Rust: `src/transaction/pipelined.rs (PIPELINED_LOCK_TTL_MS)` | Tests: `src/transaction/transaction.rs (pipelined tests)`
- [x] `PrewriteMaxBackoff` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `SetSuccess` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 

### Methods
- [x] `func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (b *BufferSnapshotBatchGetter) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) AppendMutation(mutation PlainMutation)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) GetFlags() []CommitterMutationFlags` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) GetKey(i int) []byte` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) GetKeys() [][]byte` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) GetOp(i int) kvrpcpb.Op` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) GetOps() []kvrpcpb.Op` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) GetValue(i int) []byte` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) GetValues() [][]byte` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) IsAssertExists(i int) bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) IsAssertNotExist(i int) bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) IsPessimisticLock(i int) bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) Len() int` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) MergeMutations(mutations PlainMutations)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) NeedConstraintCheckInPrewrite(i int) bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) Push(op kvrpcpb.Op, key []byte, value []byte, isPessimisticLock, assertExist, assertNotExist, NeedConstraintCheckInPrewrite bool)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c *PlainMutations) Slice(from, to int) CommitterMutations` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) BuildPrewriteRequest(regionID, regionConf, regionVersion uint64, mutations CommitterMutations, txnSize uint64) *tikvrpc.Request` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) CheckAsyncCommit() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) Cleanup(ctx context.Context)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) CleanupMutations(ctx context.Context) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) CleanupWithoutWait(ctx context.Context)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) CloseTTLManager()` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) CommitMutations(ctx context.Context) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) Execute(ctx context.Context) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) GetCommitTS() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) GetForUpdateTS() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) GetLockTTL() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) GetMinCommitTS() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) GetMutations() CommitterMutations` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) GetOnePCCommitTS() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) GetPrimaryKey() []byte` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) GetStartTS() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) GetUndeterminedErr() error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) InitKeysAndMutations() error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) IsAsyncCommit() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) IsNil() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) IsOnePC() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) IsTTLRunning() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) IsTTLUninitialized() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) MutationsOfKeys(keys [][]byte) CommitterMutations` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) PessimisticRollbackMutations(ctx context.Context, muts CommitterMutations) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) PrewriteAllMutations(ctx context.Context) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) PrewriteMutations(ctx context.Context, mutations CommitterMutations) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) ResolveFlushedLocks(bo *retry.Backoffer, start, end []byte, commit bool)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetCommitTS(ts uint64)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetForUpdateTS(ts uint64)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetLockTTL(ttl uint64)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetLockTTLByTimeAndSize(start time.Time, size int)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetMaxCommitTS(ts uint64)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetMinCommitTS(ts uint64)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetMutations(muts CommitterMutations)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetNoFallBack()` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetPrimaryKey(key []byte)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetPrimaryKeyBlocker(ac, bk chan struct{})` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetSessionID(id uint64)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetTxnSize(sz int)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) SetUseAsyncCommit()` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c CommitterProbe) WaitCleanup()` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c ConfigProbe) GetDefaultLockTTL() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c ConfigProbe) GetPessimisticLockMaxBackoff() int` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c ConfigProbe) GetTTLFactor() int` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c ConfigProbe) GetTxnCommitBatchSize() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c ConfigProbe) LoadPreSplitDetectThreshold() uint32` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c ConfigProbe) LoadPreSplitSizeThreshold() uint32` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c ConfigProbe) StorePreSplitDetectThreshold(v uint32)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (c ConfigProbe) StorePreSplitSizeThreshold(v uint32)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (i *AggressiveLockedKeyInfo) ActualLockForUpdateTS() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (i *AggressiveLockedKeyInfo) HasCheckExistence() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (i *AggressiveLockedKeyInfo) HasReturnValues() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (i *AggressiveLockedKeyInfo) Key() []byte` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (i *AggressiveLockedKeyInfo) Value() kv.ReturnedValue` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (p PrewriteEncounterLockPolicy) String() string` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) AddRPCInterceptor(it interceptor.RPCInterceptor)` | Rust: `src/transaction/transaction.rs (Transaction::add_rpc_interceptor)` | Tests: `src/transaction/transaction.rs (test_rpc_interceptor_can_override_priority_penalty_and_tag)`
- [x] `func (txn *KVTxn) BatchGet(ctx context.Context, keys [][]byte, options ...tikv.BatchGetOption) (map[string]tikv.ValueEntry, error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) CancelAggressiveLocking(ctx context.Context)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) ClearDiskFullOpt()` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Commit(ctx context.Context) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) CommitTS() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Delete(k []byte) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) DoneAggressiveLocking(ctx context.Context)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) EnableForceSyncLog()` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Get(ctx context.Context, k []byte, options ...tikv.GetOption) (tikv.ValueEntry, error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) GetClusterID() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) GetCommitWaitUntilTSO() uint64` | Rust: `src/transaction/transaction.rs (Transaction::commit_wait_until_tso)` | Tests: `src/transaction/transaction.rs (test_commit_wait_until_tso_retries_until_target_is_reached, test_commit_wait_until_tso_times_out)` 
- [x] `func (txn *KVTxn) GetCommitWaitUntilTSOTimeout() time.Duration` | Rust: `src/transaction/transaction.rs (Transaction::commit_wait_until_tso_timeout)` | Tests: `src/transaction/transaction.rs (test_commit_wait_until_tso_retries_until_target_is_reached, test_commit_wait_until_tso_times_out)` 
- [x] `func (txn *KVTxn) GetDiskFullOpt() kvrpcpb.DiskFullOpt` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) GetMemBuffer() unionstore.MemBuffer` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) GetScope() string` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) GetSnapshot() *txnsnapshot.KVSnapshot` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) GetTimestampForCommit(bo *retry.Backoffer, scope string) (_ uint64, err error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) GetUnionStore() *unionstore.KVUnionStore` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) GetVars() *tikv.Variables` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) InitPipelinedMemDB() error` | Rust: `src/transaction/transaction.rs (TransactionOptions::use_pipelined_txn)` | Tests: `src/transaction/transaction.rs (test_pipelined_flush_commit_and_resolve_locks)`
- [x] `func (txn *KVTxn) IsCasualConsistency() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) IsInAggressiveLockingMode() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) IsInAggressiveLockingStage(key []byte) bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) IsPessimistic() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) IsPipelined() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) IsReadOnly() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) IterReverse(k, lowerBound []byte) (unionstore.Iterator, error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Len() int` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) LockKeys(ctx context.Context, lockCtx *tikv.LockCtx, keysInput ...[]byte) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) LockKeysFunc(ctx context.Context, lockCtx *tikv.LockCtx, fn func(), keysInput ...[]byte) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) LockKeysWithWaitTime(ctx context.Context, lockWaitTime int64, keysInput ...[]byte) (err error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Mem() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) MemHookSet() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) RetryAggressiveLocking(ctx context.Context)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Rollback() error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Set(k []byte, v []byte) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetAssertionLevel(assertionLevel kvrpcpb.AssertionLevel)` | Rust: `src/transaction/transaction.rs (Transaction::set_assertion_level, TransactionOptions::assertion_level)` | Tests: `src/transaction/transaction.rs (test_prewrite_propagates_assertion_level_and_mutation_assertions)`
- [x] `func (txn *KVTxn) SetBackgroundGoroutineLifecycleHooks(hooks LifecycleHooks)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetBinlogExecutor(binlog BinlogExecutor)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetCausalConsistency(b bool)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetCommitCallback(f func(string, error))` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetCommitTSUpperBoundCheck(f func(commitTS uint64) bool)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetCommitWaitUntilTSO(commitWaitUntilTSO uint64)` | Rust: `src/transaction/transaction.rs (Transaction::set_commit_wait_until_tso)` | Tests: `src/transaction/transaction.rs (test_commit_wait_until_tso_retries_until_target_is_reached, test_commit_wait_until_tso_times_out)` 
- [x] `func (txn *KVTxn) SetCommitWaitUntilTSOTimeout(val time.Duration)` | Rust: `src/transaction/transaction.rs (Transaction::set_commit_wait_until_tso_timeout)` | Tests: `src/transaction/transaction.rs (test_commit_wait_until_tso_retries_until_target_is_reached, test_commit_wait_until_tso_times_out)` 
- [x] `func (txn *KVTxn) SetDiskFullOpt(level kvrpcpb.DiskFullOpt)` | Rust: `src/transaction/transaction.rs (Transaction::set_disk_full_opt)` | Tests: `src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (txn *KVTxn) SetEnable1PC(b bool)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetEnableAsyncCommit(b bool)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetExplicitRequestSourceType(tp string)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetKVFilter(filter KVFilter)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetMemoryFootprintChangeHook(hook func(uint64))` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetPessimistic(b bool)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetPrewriteEncounterLockPolicy(policy PrewriteEncounterLockPolicy)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetPriority(pri txnutil.Priority)` | Rust: `src/transaction/transaction.rs (Transaction::set_priority)` | Tests: `src/transaction/transaction.rs (request_context tests)`
- [x] `func (txn *KVTxn) SetRPCInterceptor(it interceptor.RPCInterceptor)` | Rust: `src/transaction/transaction.rs (Transaction::set_rpc_interceptor)` | Tests: `src/transaction/transaction.rs (test_rpc_interceptor_can_override_priority_penalty_and_tag)`
- [x] `func (txn *KVTxn) SetRequestSourceInternal(internal bool)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetRequestSourceType(tp string)` | Rust: `src/transaction/transaction.rs (Transaction::set_request_source)` | Tests: `src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (txn *KVTxn) SetResourceGroupName(name string)` | Rust: `src/transaction/transaction.rs (Transaction::set_resource_group_name)` | Tests: `src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (txn *KVTxn) SetResourceGroupTag(tag []byte)` | Rust: `src/transaction/transaction.rs (Transaction::set_resource_group_tag)` | Tests: `src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (txn *KVTxn) SetResourceGroupTagger(tagger tikvrpc.ResourceGroupTagger)` | Rust: `src/transaction/transaction.rs (Transaction::set_resource_group_tagger)` | Tests: `src/transaction/transaction.rs (test_fixed_resource_group_tag_takes_precedence_over_tagger)`
- [x] `func (txn *KVTxn) SetSchemaLeaseChecker(checker SchemaLeaseChecker)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetSchemaVer(schemaVer SchemaVer)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetScope(scope string)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetSessionID(sessionID uint64)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) SetTxnSource(txnSource uint64)` | Rust: `src/transaction/transaction.rs (Transaction::set_txn_source)` | Tests: `src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (txn *KVTxn) SetVars(vars *tikv.Variables)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Size() int` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) StartAggressiveLocking()` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) StartTS() uint64` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) String() string` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn *KVTxn) Valid() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) BatchGetSingleRegion(bo *retry.Backoffer, region locate.RegionVerID, keys [][]byte, collect func([]byte, []byte)) error` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) CollectLockedKeys() [][]byte` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) GetAggressiveLockingKeys() []string` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) GetAggressiveLockingKeysInfo() []AggressiveLockedKeyInfo` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) GetAggressiveLockingPreviousKeys() []string` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) GetAggressiveLockingPreviousKeysInfo() []AggressiveLockedKeyInfo` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) GetCommitter() CommitterProbe` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) GetLockedCount() int` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) GetStartTime() time.Time` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) GetUnionStore() *unionstore.KVUnionStore` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) IsAsyncCommit() bool` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) NewCommitter(sessionID uint64) (CommitterProbe, error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) NewScanner(start, end []byte, batchSize int, reverse bool) (*txnsnapshot.Scanner, error)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) SetCommitter(committer CommitterProbe)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 
- [x] `func (txn TxnProbe) SetStartTS(ts uint64)` | Rust: N/A (out-of-scope: Rust exposes `TransactionClient`/`Transaction` directly) | Tests: N/A 

## txnkv/txnlock (package txnlock)

### Types
- [x] `type Lock struct` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `type LockProbe struct` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `type LockResolver struct` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `type LockResolverProbe struct` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `type ResolveLockResult struct` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `type ResolveLocksOptions struct` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `type ResolvingLock struct` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `type TxnStatus struct` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 

### Functions
- [x] `func ExtractLockFromKeyErr(keyErr *kvrpcpb.KeyError) (*Lock, error)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func NewLock(l *kvrpcpb.LockInfo) *Lock` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func NewLockResolver(store storage) *LockResolver` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 

### Consts
- [x] `ResolvedCacheSize` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 

### Vars
- (none)

### Methods
- [x] `func (l *Lock) String() string` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockProbe) GetPrimaryKeyFromTxnStatus(s TxnStatus) []byte` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockProbe) NewLockStatus(keys [][]byte, useAsyncCommit bool, minCommitTS uint64) TxnStatus` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) CheckAllSecondaries(bo *retry.Backoffer, lock *Lock, status *TxnStatus) error` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) GetSecondariesFromTxnStatus(status TxnStatus) [][]byte` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) GetTxnStatus(bo *retry.Backoffer, txnID uint64, primary []byte, callerStartTS, currentTS uint64, rollbackIfNotExist bool, forceSyncCommit bool, lockInfo *Lock) (TxnStatus, error)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) GetTxnStatusFromLock(bo *retry.Backoffer, lock *Lock, callerStartTS uint64, forceSyncCommit bool) (TxnStatus, error)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) IsErrorNotFound(err error) bool` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) IsNonAsyncCommitLock(err error) bool` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) ResolveAsyncCommitLock(bo *retry.Backoffer, lock *Lock, status TxnStatus) error` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) ResolveLock(bo *retry.Backoffer, lock *Lock) error` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) ResolvePessimisticLock(bo *retry.Backoffer, lock *Lock) error` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) SetMeetLockCallback(f func([]*Lock))` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (l LockResolverProbe) SetResolving(currentStartTS uint64, locks []Lock)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) BatchResolveLocks(bo *retry.Backoffer, locks []*Lock, loc locate.RegionVerID) (bool, error)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) Close()` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) GetTxnStatus(txnID uint64, callerStartTS uint64, primary []byte) (TxnStatus, error)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) RecordResolvingLocks(locks []*Lock, callerStartTS uint64) int` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) ResolveLocks(bo *retry.Backoffer, callerStartTS uint64, locks []*Lock) (int64, error)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) ResolveLocksDone(callerStartTS uint64, token int)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) ResolveLocksForRead(bo *retry.Backoffer, callerStartTS uint64, locks []*Lock, lite bool) (int64, []uint64, []uint64, error)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) ResolveLocksWithOpts(bo *retry.Backoffer, opts ResolveLocksOptions) (ResolveLockResult, error)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) Resolving() []ResolvingLock` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (lr *LockResolver) UpdateResolvingLocks(locks []*Lock, callerStartTS uint64, token int)` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (s TxnStatus) Action() kvrpcpb.Action` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (s TxnStatus) CommitTS() uint64` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (s TxnStatus) HasSameDeterminedStatus(other TxnStatus) bool` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (s TxnStatus) IsCommitted() bool` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (s TxnStatus) IsRolledBack() bool` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (s TxnStatus) IsStatusDetermined() bool` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (s TxnStatus) StatusCacheable() bool` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (s TxnStatus) String() string` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 
- [x] `func (s TxnStatus) TTL() uint64` | Rust: N/A (out-of-scope: lock resolver APIs are internal; use high-level txn APIs) | Tests: N/A 

## txnkv/txnsnapshot (package txnsnapshot)

### Types
- [x] `type ClientHelper struct` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `type ConfigProbe struct` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `type IsoLevel kvrpcpb.IsolationLevel` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `type KVSnapshot struct` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `type ReplicaReadAdjuster func(int) (locate.StoreSelectorOption, kv.ReplicaReadType)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `type Scanner struct` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `type SnapshotProbe struct` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `type SnapshotRuntimeStats struct` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 

### Functions
- [x] `func NewClientHelper(store kvstore, resolvedLocks *util.TSSet, committedLocks *util.TSSet, resolveLite bool) *ClientHelper` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func NewTiKVSnapshot(store kvstore, ts uint64, replicaReadSeed uint32) *KVSnapshot` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 

### Consts
- [x] `BatchGetBufferTier` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `BatchGetSnapshotTier` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `DefaultScanBatchSize` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `RC` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `RCCheckTS` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `SI` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 

### Vars
- (none)

### Methods
- [x] `func (c ConfigProbe) GetGetMaxBackoff() int` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (c ConfigProbe) GetScanBatchSize() int` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (ch *ClientHelper) RecordResolvingLocks(locks []*txnlock.Lock, callerStartTS uint64) int` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (ch *ClientHelper) ResolveLocks(bo *retry.Backoffer, callerStartTS uint64, locks []*txnlock.Lock) (int64, error)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (ch *ClientHelper) ResolveLocksDone(callerStartTS uint64, token int)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (ch *ClientHelper) ResolveLocksWithOpts(bo *retry.Backoffer, opts txnlock.ResolveLocksOptions) (txnlock.ResolveLockResult, error)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (ch *ClientHelper) SendReqAsync( bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, cb async.Callback[*tikvrpc.ResponseExt], opts ...locate.StoreSelectorOption, )` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (ch *ClientHelper) SendReqCtx(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, et tikvrpc.EndpointType, directStoreAddr string, opts ...locate.StoreSelectorOption) (*tikvrpc.Response, *locate.RPCContext, string, error)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (ch *ClientHelper) UpdateResolvingLocks(locks []*txnlock.Lock, callerStartTS uint64, token int)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (l IsoLevel) ToPB() kvrpcpb.IsolationLevel` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (rs *SnapshotRuntimeStats) Clone() *SnapshotRuntimeStats` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (rs *SnapshotRuntimeStats) GetCmdRPCCount(cmd tikvrpc.CmdType) int64` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (rs *SnapshotRuntimeStats) GetTimeDetail() *util.TimeDetail` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (rs *SnapshotRuntimeStats) Merge(other *SnapshotRuntimeStats)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (rs *SnapshotRuntimeStats) String() string` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) AddRPCInterceptor(it interceptor.RPCInterceptor)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) BatchGetWithTier(ctx context.Context, keys [][]byte, readTier int, opt kv.BatchGetOptions) (m map[string]kv.ValueEntry, err error)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) CleanCache(keys [][]byte)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) Get(ctx context.Context, k []byte, options ...kv.GetOption) (entry kv.ValueEntry, err error)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) GetKVReadTimeout() time.Duration` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) GetResolveLockDetail() *util.ResolveLockDetail` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) IsInternal() bool` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) IterReverse(k, lowerBound []byte) (unionstore.Iterator, error)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetIsStalenessReadOnly(b bool)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetIsolationLevel(level IsoLevel)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetKVReadTimeout(readTimeout time.Duration)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetKeyOnly(b bool)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetLoadBasedReplicaReadThreshold(busyThreshold time.Duration)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetMatchStoreLabels(labels []*metapb.StoreLabel)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetNotFillCache(b bool)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetPipelined(ts uint64)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetPriority(pri txnutil.Priority)` | Rust: `src/transaction/snapshot.rs (Snapshot::set_priority)` | Tests: `src/transaction/transaction.rs (request_context tests)`
- [x] `func (s *KVSnapshot) SetRPCInterceptor(it interceptor.RPCInterceptor)` | Rust: `src/transaction/snapshot.rs (Snapshot::set_rpc_interceptor)` | Tests: `src/transaction/transaction.rs (test_rpc_interceptor_can_override_priority_penalty_and_tag)`
- [x] `func (s *KVSnapshot) SetReadReplicaScope(scope string)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetReplicaRead(readType kv.ReplicaReadType)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetReplicaReadAdjuster(f ReplicaReadAdjuster)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetResourceGroupName(name string)` | Rust: `src/transaction/snapshot.rs (Snapshot::set_resource_group_name)` | Tests: `src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (s *KVSnapshot) SetResourceGroupTag(tag []byte)` | Rust: `src/transaction/snapshot.rs (Snapshot::set_resource_group_tag)` | Tests: `src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (s *KVSnapshot) SetResourceGroupTagger(tagger tikvrpc.ResourceGroupTagger)` | Rust: `src/transaction/snapshot.rs (Snapshot::set_resource_group_tagger)` | Tests: `src/transaction/transaction.rs (test_fixed_resource_group_tag_takes_precedence_over_tagger)`
- [x] `func (s *KVSnapshot) SetRuntimeStats(stats *SnapshotRuntimeStats)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetSampleStep(step uint32)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetScanBatchSize(batchSize int)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetSnapshotTS(ts uint64)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetTaskID(id uint64)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetTxnScope(scope string)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SetVars(vars *kv.Variables)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SnapCache() map[string]kv.ValueEntry` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SnapCacheHitCount() int` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) SnapCacheSize() int` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *KVSnapshot) UpdateSnapshotCache(keys [][]byte, m map[string]kv.ValueEntry)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *Scanner) Close()` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *Scanner) Key() []byte` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *Scanner) Next() error` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *Scanner) Valid() bool` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s *Scanner) Value() []byte` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s SnapshotProbe) BatchGetSingleRegion(bo *retry.Backoffer, region locate.RegionVerID, keys [][]byte, collectF func(k, v []byte)) error` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s SnapshotProbe) FormatStats() string` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s SnapshotProbe) MergeExecDetail(detail *kvrpcpb.ExecDetailsV2)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s SnapshotProbe) MergeRegionRequestStats(rpcStats *locate.RegionRequestRuntimeStats)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s SnapshotProbe) NewScanner(start, end []byte, batchSize int, reverse bool) (*Scanner, error)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 
- [x] `func (s SnapshotProbe) RecordBackoffInfo(bo *retry.Backoffer)` | Rust: N/A (out-of-scope: snapshot APIs exposed as `TransactionClient::snapshot` / `Snapshot`) | Tests: N/A 

## txnkv/txnutil (package txnutil)

### Types
- [x] `type Priority kvrpcpb.CommandPri` | Rust: N/A (out-of-scope: Rust does not expose Go txnutil helpers) | Tests: N/A 

### Functions
- (none)

### Consts
- [x] `PriorityHigh` | Rust: N/A (out-of-scope: Rust does not expose Go txnutil helpers) | Tests: N/A 
- [x] `PriorityLow` | Rust: N/A (out-of-scope: Rust does not expose Go txnutil helpers) | Tests: N/A 
- [x] `PriorityNormal` | Rust: N/A (out-of-scope: Rust does not expose Go txnutil helpers) | Tests: N/A 

### Vars
- (none)

### Methods
- [x] `func (p Priority) ToPB() kvrpcpb.CommandPri` | Rust: N/A (out-of-scope: Rust does not expose Go txnutil helpers) | Tests: N/A 

## util (package util)

### Types
- [x] `type CommitDetails struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type CommitTSLagDetails struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type ExecDetails struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type InterceptedPDClient struct` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `type LockKeysDetails struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type Option struct` | Rust: N/A (out-of-scope: use `Option<T>`) | Tests: N/A
- [x] `type RUDetails struct` | Rust: N/A (out-of-scope: resource usage details not exposed) | Tests: N/A
- [x] `type RateLimit struct` | Rust: N/A (out-of-scope: use `tokio::sync::Semaphore`) | Tests: N/A
- [x] `type ReqDetailInfo struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type RequestSource struct` | Rust: N/A (out-of-scope: no Go-style context; use `PlanBuilder/RawClient/TransactionClient::with_request_source`) | Tests: N/A
- [x] `type RequestSourceKeyType struct` | Rust: N/A (out-of-scope: no Go-style context) | Tests: N/A
- [x] `type RequestSourceTypeKeyType struct` | Rust: N/A (out-of-scope: no Go-style context) | Tests: N/A
- [x] `type ResolveLockDetail struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type ScanDetail struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type TSSet struct` | Rust: N/A (out-of-scope: internal helper; use `HashSet<u64>` + lock) | Tests: N/A
- [x] `type TiKVExecDetails struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type TimeDetail struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type TrafficDetails struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A
- [x] `type WriteDetail struct` | Rust: N/A (out-of-scope: internal exec-details aggregation not exposed) | Tests: N/A

### Functions
- [x] `func BuildRequestSource(internal bool, source, explicitSource string) string` | Rust: N/A (out-of-scope: use `PlanBuilder::with_request_source`/pass explicit string) | Tests: N/A
- [x] `func BytesToString(numBytes int64) string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func CompatibleParseGCTime(value string) (time.Time, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func ContextWithTraceExecDetails(ctx context.Context) context.Context` | Rust: N/A (out-of-scope: no Go-style context) | Tests: N/A
- [x] `func EnableFailpoints()` | Rust: N/A (out-of-scope: failpoints are behind Rust `fail` crate) | Tests: N/A
- [x] `func EvalFailpoint(name string) (interface{}, error)` | Rust: N/A (out-of-scope: failpoints are behind Rust `fail` crate) | Tests: N/A
- [x] `func FormatBytes(numBytes int64) string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func FormatDuration(d time.Duration) string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func GetCustomDNSDialer(dnsServer, dnsDomain string) dnsF` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func IsInternalRequest(source string) bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func IsRequestSourceInternal(reqSrc *RequestSource) bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func NewInterceptedPDClient(client pd.Client) *InterceptedPDClient` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `func NewRUDetails() *RUDetails` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func NewRUDetailsWith(rru, wru float64, waitDur time.Duration) *RUDetails` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func NewRateLimit(n int) *RateLimit` | Rust: N/A (out-of-scope: use `tokio::sync::Semaphore`) | Tests: N/A
- [x] `func NewTiKVExecDetails(pb *kvrpcpb.ExecDetailsV2) TiKVExecDetails` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func None[T interface{}]() Option[T]` | Rust: N/A (out-of-scope: use `Option::None`) | Tests: N/A
- [x] `func RequestSourceFromCtx(ctx context.Context) string` | Rust: N/A (out-of-scope: no Go-style context; configure on client/plan) | Tests: N/A
- [x] `func ResourceGroupNameFromCtx(ctx context.Context) string` | Rust: N/A (out-of-scope: no Go-style context; configure on client/plan) | Tests: N/A
- [x] `func SetSessionID(ctx context.Context, sessionID uint64) context.Context` | Rust: N/A (out-of-scope: no Go-style context) | Tests: N/A
- [x] `func Some[T interface{}](inner T) Option[T]` | Rust: N/A (out-of-scope: use `Some(inner)`) | Tests: N/A
- [x] `func TraceExecDetailsEnabled(ctx context.Context) bool` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func WithInternalSourceAndTaskType(ctx context.Context, source, taskName string) context.Context` | Rust: N/A (out-of-scope: no Go-style context) | Tests: N/A
- [x] `func WithInternalSourceType(ctx context.Context, source string) context.Context` | Rust: N/A (out-of-scope: no Go-style context) | Tests: N/A
- [x] `func WithRecovery(exec func(), recoverFn func(r interface{}))` | Rust: N/A (out-of-scope: Rust panics use `catch_unwind` explicitly) | Tests: N/A
- [x] `func WithResourceGroupName(ctx context.Context, groupName string) context.Context` | Rust: N/A (out-of-scope: no Go-style context; configure on client/plan) | Tests: N/A

### Consts
- [x] `ExplicitTypeBR` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ExplicitTypeBackground` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ExplicitTypeDDL` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ExplicitTypeDumpling` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ExplicitTypeEmpty` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ExplicitTypeLightning` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ExplicitTypeStats` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `ExternalRequest` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `GCTimeFormat` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `InternalRequest` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `InternalRequestPrefix` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `InternalTxnGC` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `InternalTxnMeta` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `InternalTxnOthers` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `InternalTxnStats` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `SourceUnknown` | Rust: N/A (out-of-scope) | Tests: N/A

### Vars
- [x] `CommitDetailCtxKey` | Rust: N/A (out-of-scope: no Go-style context keys) | Tests: N/A
- [x] `ExecDetailsKey` | Rust: N/A (out-of-scope: no Go-style context keys) | Tests: N/A
- [x] `ExplicitTypeList` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `LockKeysDetailCtxKey` | Rust: N/A (out-of-scope: no Go-style context keys) | Tests: N/A
- [x] `RUDetailsCtxKey` | Rust: N/A (out-of-scope: no Go-style context keys) | Tests: N/A
- [x] `RequestSourceKey` | Rust: N/A (out-of-scope: no Go-style context keys) | Tests: N/A
- [x] `RequestSourceTypeKey` | Rust: N/A (out-of-scope: no Go-style context keys) | Tests: N/A
- [x] `SessionID` | Rust: N/A (out-of-scope: no Go-style context keys) | Tests: N/A

### Methods
- [x] `func (cd *CommitDetails) Clone() *CommitDetails` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (cd *CommitDetails) Merge(other *CommitDetails)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (cd *CommitDetails) MergeCommitReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (cd *CommitDetails) MergeFlushReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (cd *CommitDetails) MergePrewriteReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (d *CommitTSLagDetails) Merge(other *CommitTSLagDetails)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (ed *TiKVExecDetails) String() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (ld *LockKeysDetails) Clone() *LockKeysDetails` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (ld *LockKeysDetails) Merge(lockKey *LockKeysDetails)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (ld *LockKeysDetails) MergeReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (m InterceptedPDClient) GetPrevRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error)` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `func (m InterceptedPDClient) GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error)` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `func (m InterceptedPDClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...opt.GetRegionOption) (*router.Region, error)` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `func (m InterceptedPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `func (m InterceptedPDClient) GetTS(ctx context.Context) (int64, int64, error)` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `func (m InterceptedPDClient) GetTSAsync(ctx context.Context) tso.TSFuture` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `func (m InterceptedPDClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...opt.GetRegionOption) ([]*router.Region, error)` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `func (m InterceptedPDClient) WithCallerComponent(component caller.Component) pd.Client` | Rust: N/A (out-of-scope: PD client is internal) | Tests: N/A
- [x] `func (o Option[T]) Inner() *T` | Rust: N/A (out-of-scope: use `Option<T>` API) | Tests: N/A
- [x] `func (r *RateLimit) GetCapacity() int` | Rust: N/A (out-of-scope: use `tokio::sync::Semaphore`) | Tests: N/A
- [x] `func (r *RateLimit) GetToken(done <-chan struct{}) (exit bool)` | Rust: N/A (out-of-scope: use `tokio::sync::Semaphore`) | Tests: N/A
- [x] `func (r *RateLimit) PutToken()` | Rust: N/A (out-of-scope: use `tokio::sync::Semaphore`) | Tests: N/A
- [x] `func (r *RequestSource) GetRequestSource() string` | Rust: N/A (out-of-scope: no Go-style context; pass request source string explicitly) | Tests: N/A
- [x] `func (r *RequestSource) SetExplicitRequestSourceType(tp string)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (r *RequestSource) SetRequestSourceInternal(internal bool)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (r *RequestSource) SetRequestSourceType(tp string)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (rd *RUDetails) Clone() *RUDetails` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (rd *RUDetails) Merge(other *RUDetails)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (rd *RUDetails) RRU() float64` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (rd *RUDetails) RUWaitDuration() time.Duration` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (rd *RUDetails) String() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (rd *RUDetails) Update(consumption *rmpb.Consumption, waitDuration time.Duration)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (rd *RUDetails) WRU() float64` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (rd *ResolveLockDetail) Merge(resolveLock *ResolveLockDetail)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (s *TSSet) GetAll() []uint64` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (s *TSSet) Put(tss ...uint64)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (sd *ScanDetail) Merge(scanDetail *ScanDetail)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (sd *ScanDetail) MergeFromScanDetailV2(scanDetail *kvrpcpb.ScanDetailV2)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (sd *ScanDetail) String() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (td *TimeDetail) Merge(detail *TimeDetail)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (td *TimeDetail) MergeFromTimeDetail(timeDetailV2 *kvrpcpb.TimeDetailV2, timeDetail *kvrpcpb.TimeDetail)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (td *TimeDetail) String() string` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (wd *WriteDetail) Merge(writeDetail *WriteDetail)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (wd *WriteDetail) MergeFromWriteDetailPb(pb *kvrpcpb.WriteDetail)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (wd *WriteDetail) String() string` | Rust: N/A (out-of-scope) | Tests: N/A

## util/async (package async)

### Types
- [x] `type Callback interface` | Rust: N/A (out-of-scope: Rust uses `Future`/async closures) | Tests: N/A
- [x] `type Executor interface` | Rust: N/A (out-of-scope: use Tokio executor) | Tests: N/A
- [x] `type Pool interface` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `type RunLoop struct` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `type State uint32` | Rust: N/A (out-of-scope) | Tests: N/A

### Functions
- [x] `func NewCallback[T any](e Executor, f func(T, error)) Callback[T]` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func NewRunLoop() *RunLoop` | Rust: N/A (out-of-scope) | Tests: N/A

### Consts
- [x] `StateIdle` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `StateRunning` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `StateWaiting` | Rust: N/A (out-of-scope) | Tests: N/A

### Vars
- (none)

### Methods
- [x] `func (l *RunLoop) Append(fs ...func())` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (l *RunLoop) Exec(ctx context.Context) (int, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (l *RunLoop) Go(f func())` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (l *RunLoop) NumRunnable() int` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func (l *RunLoop) State() State` | Rust: N/A (out-of-scope) | Tests: N/A

## util/codec (package codec)

### Types
- (none)

### Functions
- [x] `func DecodeBytes(b []byte, buf []byte) ([]byte, []byte, error)` | Rust: N/A (out-of-scope: internal encoding helpers not exposed) | Tests: N/A
- [x] `func DecodeCmpUintToInt(u uint64) int64` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func DecodeComparableUvarint(b []byte) ([]byte, uint64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func DecodeComparableVarint(b []byte) ([]byte, int64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func DecodeInt(b []byte) ([]byte, int64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func DecodeIntDesc(b []byte) ([]byte, int64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func DecodeUint(b []byte) ([]byte, uint64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func DecodeUintDesc(b []byte) ([]byte, uint64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func DecodeUvarint(b []byte) ([]byte, uint64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func DecodeVarint(b []byte) ([]byte, int64, error)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func EncodeBytes(b []byte, data []byte) []byte` | Rust: N/A (capability: memcomparable bytes encoding in `kv::codec`) | Tests: N/A
- [x] `func EncodeComparableUvarint(b []byte, v uint64) []byte` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func EncodeComparableVarint(b []byte, v int64) []byte` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func EncodeInt(b []byte, v int64) []byte` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func EncodeIntDesc(b []byte, v int64) []byte` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func EncodeIntToCmpUint(v int64) uint64` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func EncodeUint(b []byte, v uint64) []byte` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func EncodeUintDesc(b []byte, v uint64) []byte` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func EncodeUvarint(b []byte, v uint64) []byte` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func EncodeVarint(b []byte, v int64) []byte` | Rust: N/A (out-of-scope) | Tests: N/A

### Consts
- (none)

### Vars
- (none)

### Methods
- (none)

## util/intest (package intest)

### Types
- (none)

### Functions
- (none)

### Consts
- (none)

### Vars
- [x] `InTest` | Rust: N/A (out-of-scope: Rust tests use `cfg(test)`/features) | Tests: N/A

### Methods
- (none)

## util/israce (package israce)

### Types
- (none)

### Functions
- (none)

### Consts
- [x] `RaceEnabled` | Rust: N/A (out-of-scope: Rust has no data race mode switch like Go) | Tests: N/A

### Vars
- (none)

### Methods
- (none)

## util/redact (package redact)

### Types
- (none)

### Functions
- [x] `func Key(key []byte) string` | Rust: N/A (out-of-scope: use structured logging + avoid logging raw keys) | Tests: N/A
- [x] `func KeyBytes(key []byte) []byte` | Rust: N/A (out-of-scope: use structured logging + avoid logging raw keys) | Tests: N/A
- [x] `func NeedRedact() bool` | Rust: N/A (out-of-scope: no global redact toggle) | Tests: N/A
- [x] `func RedactKeyErrIfNecessary(err *kvrpcpb.KeyError)` | Rust: N/A (out-of-scope) | Tests: N/A
- [x] `func String(b []byte) (s string)` | Rust: N/A (out-of-scope: avoid logging raw key bytes) | Tests: N/A

### Consts
- (none)

### Vars
- (none)

### Methods
- (none)
