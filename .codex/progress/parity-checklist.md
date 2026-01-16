# client-go v2 parity checklist (auto-generated skeleton)

This file is generated from `client-go/` source and is used to track Go→Rust parity work.

Regenerate:
- `go run ./tools/client-go-api-inventory`

Conventions:
- `Rust:` fill with target Rust path/symbol(s)
- `Tests:` fill with relevant unit/integration tests (or `N/A`)

## config (package config)

### Types
- [x] `type AsyncCommit struct` | Rust: `tikv_client::TransactionOptions::use_async_commit` (new-client-rust/src/transaction/transaction.rs) | Tests: `new-client-rust/src/transaction/transaction.rs (test_async_commit_fallback_to_2pc_when_min_commit_ts_is_zero, test_async_commit_uses_min_commit_ts_when_available)`
- [x] `type Config struct` | Rust: `tikv_client::Config` (new-client-rust/src/config.rs) | Tests: N/A
- [ ] `type CoprocessorCache struct` | Rust:  | Tests: 
- [ ] `type PDClient struct` | Rust:  | Tests: 
- [x] `type PessimisticTxn struct` | Rust: `tikv_client::TransactionClient::begin_pessimistic` + `tikv_client::TransactionOptions::new_pessimistic` (new-client-rust/src/transaction/{client,transaction}.rs) | Tests: N/A
- [x] `type Security struct` | Rust: `tikv_client::Config::with_security` + `tikv_client::SecurityManager` (new-client-rust/src/{config.rs,common/security.rs}) | Tests: `new-client-rust/src/common/security.rs (test_security)`
- [ ] `type TiKVClient struct` | Rust:  | Tests: 
- [x] `type TxnLocalLatches struct` | Rust: `tikv_client::TransactionClient::with_txn_local_latches` (new-client-rust/src/transaction/client.rs) | Tests: `new-client-rust/src/transaction/latch.rs (tests)`

### Functions
- [x] `func DefaultConfig() Config` | Rust: `Config::default()` (new-client-rust/src/config.rs) | Tests: N/A
- [ ] `func DefaultPDClient() PDClient` | Rust:  | Tests: 
- [ ] `func DefaultTiKVClient() TiKVClient` | Rust:  | Tests: 
- [ ] `func DefaultTxnLocalLatches() TxnLocalLatches` | Rust:  | Tests: 
- [x] `func GetGlobalConfig() *Config` | Rust: N/A (out-of-scope: explicit `Config` passed to `*_with_config`) | Tests: N/A
- [ ] `func GetTxnScopeFromConfig() string` | Rust:  | Tests: 
- [x] `func NewSecurity(sslCA, sslCert, sslKey string, verityCN []string) Security` | Rust: `Config::with_security` + `SecurityManager::load` (new-client-rust/src/{config.rs,common/security.rs}) | Tests: `new-client-rust/src/common/security.rs (test_security)`
- [ ] `func ParsePath(path string) (etcdAddrs []string, disableGC bool, keyspaceName string, err error)` | Rust:  | Tests: 
- [x] `func StoreGlobalConfig(config *Config)` | Rust: N/A (out-of-scope: explicit `Config` passed to `*_with_config`) | Tests: N/A
- [x] `func UpdateGlobal(f func(conf *Config)) func()` | Rust: N/A (out-of-scope: explicit `Config` passed to `*_with_config`) | Tests: N/A

### Consts
- [ ] `BatchPolicyBasic` | Rust:  | Tests: 
- [ ] `BatchPolicyCustom` | Rust:  | Tests: 
- [ ] `BatchPolicyPositive` | Rust:  | Tests: 
- [ ] `BatchPolicyStandard` | Rust:  | Tests: 
- [ ] `DefBatchPolicy` | Rust:  | Tests: 
- [ ] `DefGrpcInitialConnWindowSize` | Rust:  | Tests: 
- [ ] `DefGrpcInitialWindowSize` | Rust:  | Tests: 
- [ ] `DefMaxConcurrencyRequestLimit` | Rust:  | Tests: 
- [ ] `DefStoreLivenessTimeout` | Rust:  | Tests: 
- [ ] `DefStoresRefreshInterval` | Rust:  | Tests: 
- [ ] `NextGen` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- [ ] `func (c *TxnLocalLatches) Valid() error` | Rust:  | Tests: 
- [ ] `func (config *TiKVClient) GetGrpcKeepAliveTimeout() time.Duration` | Rust:  | Tests: 
- [ ] `func (config *TiKVClient) Valid() error` | Rust:  | Tests: 
- [ ] `func (p *PDClient) Valid() error` | Rust:  | Tests: 
- [x] `func (s *Security) ToTLSConfig() (tlsConfig *tls.Config, err error)` | Rust: `SecurityManager::load` (new-client-rust/src/common/security.rs) | Tests: `new-client-rust/src/common/security.rs (test_security)`

## config/retry (package retry)

### Types
- [x] `type BackoffFnCfg struct` | Rust: `tikv_client::Backoff` (new-client-rust/src/backoff.rs) | Tests: `new-client-rust/src/backoff.rs (test_*)`
- [x] `type Backoffer struct` | Rust: N/A (capability: request retries via `tikv_client::request::PlanBuilder` + `RetryOptions`) | Tests: `new-client-rust/src/request/mod.rs (test_region_retry)`
- [x] `type Config struct` | Rust: `tikv_client::RetryOptions` (new-client-rust/src/request/mod.rs) | Tests: `new-client-rust/src/request/mod.rs (test_region_retry)`

### Functions
- [ ] `func IsFakeRegionError(err *errorpb.Error) bool` | Rust:  | Tests: 
- [ ] `func MayBackoffForRegionError(regionErr *errorpb.Error, bo *Backoffer) error` | Rust:  | Tests: 
- [x] `func NewBackoffFnCfg(base, cap, jitter int) *BackoffFnCfg` | Rust: `Backoff::{no_jitter_backoff,full_jitter_backoff,equal_jitter_backoff,decorrelated_jitter_backoff}` (new-client-rust/src/backoff.rs) | Tests: `new-client-rust/src/backoff.rs (tests)`
- [ ] `func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer` | Rust:  | Tests: 
- [ ] `func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer` | Rust:  | Tests: 
- [ ] `func NewConfig(name string, metric *prometheus.Observer, backoffFnCfg *BackoffFnCfg, err error) *Config` | Rust:  | Tests: 
- [x] `func NewNoopBackoff(ctx context.Context) *Backoffer` | Rust: `Backoff::no_backoff` (new-client-rust/src/backoff.rs) | Tests: `new-client-rust/src/backoff.rs (test_no_jitter_backoff)`

### Consts
- [x] `DecorrJitter` | Rust: `Backoff::decorrelated_jitter_backoff` (new-client-rust/src/backoff.rs) | Tests: `new-client-rust/src/backoff.rs (tests)`
- [x] `EqualJitter` | Rust: `Backoff::equal_jitter_backoff` (new-client-rust/src/backoff.rs) | Tests: `new-client-rust/src/backoff.rs (tests)`
- [x] `FullJitter` | Rust: `Backoff::full_jitter_backoff` (new-client-rust/src/backoff.rs) | Tests: `new-client-rust/src/backoff.rs (tests)`
- [x] `NoJitter` | Rust: `Backoff::no_jitter_backoff` (new-client-rust/src/backoff.rs) | Tests: `new-client-rust/src/backoff.rs (test_no_jitter_backoff)`

### Vars
- [ ] `BoCommitTSLag` | Rust:  | Tests: 
- [ ] `BoIsWitness` | Rust:  | Tests: 
- [ ] `BoMaxRegionNotInitialized` | Rust:  | Tests: 
- [ ] `BoMaxTsNotSynced` | Rust:  | Tests: 
- [ ] `BoPDRPC` | Rust:  | Tests: 
- [ ] `BoRegionMiss` | Rust:  | Tests: 
- [ ] `BoRegionRecoveryInProgress` | Rust:  | Tests: 
- [ ] `BoRegionScheduling` | Rust:  | Tests: 
- [ ] `BoStaleCmd` | Rust:  | Tests: 
- [ ] `BoTiFlashRPC` | Rust:  | Tests: 
- [ ] `BoTiFlashServerBusy` | Rust:  | Tests: 
- [ ] `BoTiKVDiskFull` | Rust:  | Tests: 
- [ ] `BoTiKVRPC` | Rust:  | Tests: 
- [ ] `BoTiKVServerBusy` | Rust:  | Tests: 
- [ ] `BoTxnLock` | Rust:  | Tests: 
- [ ] `BoTxnLockFast` | Rust:  | Tests: 
- [ ] `BoTxnNotFound` | Rust:  | Tests: 
- [ ] `TxnStartKey` | Rust:  | Tests: 

### Methods
- [ ] `func (b *Backoffer) Backoff(cfg *Config, err error) error` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) BackoffWithCfgAndMaxSleep(cfg *Config, maxSleepMs int, err error) error` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) BackoffWithMaxSleepTxnLockFast(maxSleepMs int, err error) error` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) CheckKilled() error` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) Clone() *Backoffer` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) ErrorsNum() int` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) Fork() (*Backoffer, context.CancelFunc)` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) GetBackoffSleepMS() map[string]int` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) GetBackoffTimes() map[string]int` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) GetCtx() context.Context` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) GetTotalBackoffTimes() int` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) GetTotalSleep() int` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) GetTypes() []string` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) GetVars() *kv.Variables` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) Reset()` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) ResetMaxSleep(maxSleep int)` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) SetCtx(ctx context.Context)` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) String() string` | Rust:  | Tests: 
- [ ] `func (b *Backoffer) UpdateUsingForked(forked *Backoffer)` | Rust:  | Tests: 
- [ ] `func (c *Config) Base() int` | Rust:  | Tests: 
- [ ] `func (c *Config) SetBackoffFnCfg(fnCfg *BackoffFnCfg)` | Rust:  | Tests: 
- [ ] `func (c *Config) SetErrors(err error)` | Rust:  | Tests: 
- [ ] `func (c *Config) String() string` | Rust:  | Tests: 

## error (package error)

### Types
- [x] `type ErrAssertionFailed struct` | Rust: `new-client-rust/src/common/errors.rs (AssertionFailedError, Error::AssertionFailed)` | Tests: `new-client-rust/src/common/errors.rs (key_error_assertion_failed_maps_to_assertion_failed)`
- [x] `type ErrDeadlock struct` | Rust: `new-client-rust/src/common/errors.rs (DeadlockError, Error::Deadlock)` | Tests: `new-client-rust/src/common/errors.rs (key_error_deadlock_maps_to_deadlock)`
- [ ] `type ErrEntryTooLarge struct` | Rust:  | Tests: 
- [ ] `type ErrGCTooEarly struct` | Rust:  | Tests: 
- [x] `type ErrKeyExist struct` | Rust: `new-client-rust/src/common/errors.rs (KeyExistsError, Error::KeyExists)` | Tests: `new-client-rust/src/common/errors.rs (key_error_already_exist_maps_to_key_exists)`
- [ ] `type ErrKeyTooLarge struct` | Rust:  | Tests: 
- [ ] `type ErrLockOnlyIfExistsNoPrimaryKey struct` | Rust:  | Tests: 
- [ ] `type ErrLockOnlyIfExistsNoReturnValue struct` | Rust:  | Tests: 
- [ ] `type ErrPDServerTimeout struct` | Rust:  | Tests: 
- [ ] `type ErrQueryInterruptedWithSignal struct` | Rust:  | Tests: 
- [x] `type ErrRetryable struct` | Rust: `new-client-rust/src/common/errors.rs (Error::Retryable)` | Tests: `new-client-rust/src/common/errors.rs (key_error_retryable_maps_to_retryable)`
- [ ] `type ErrTokenLimit struct` | Rust:  | Tests: 
- [x] `type ErrTxnAbortedByGC struct` | Rust: `new-client-rust/src/common/errors.rs (Error::TxnAborted)` | Tests: `new-client-rust/src/common/errors.rs (key_error_abort_maps_to_txn_aborted)`
- [ ] `type ErrTxnTooLarge struct` | Rust:  | Tests: 
- [x] `type ErrWriteConflict struct` | Rust: `new-client-rust/src/common/errors.rs (WriteConflictError, Error::WriteConflict)` | Tests: `new-client-rust/src/common/errors.rs (key_error_conflict_maps_to_write_conflict)`
- [x] `type ErrWriteConflictInLatch struct` | Rust: `new-client-rust/src/common/errors.rs (Error::WriteConflictInLatch)` | Tests: `new-client-rust/src/transaction/transaction.rs (latch tests)`
- [ ] `type PDError struct` | Rust:  | Tests: 

### Functions
- [ ] `func ExtractDebugInfoStrFromKeyErr(keyErr *kvrpcpb.KeyError) string` | Rust:  | Tests: 
- [ ] `func ExtractKeyErr(keyErr *kvrpcpb.KeyError) error` | Rust:  | Tests: 
- [x] `func IsErrKeyExist(err error) bool` | Rust: `new-client-rust/src/common/errors.rs (Error::is_key_exists)` | Tests: `new-client-rust/src/common/errors.rs (key_error_already_exist_maps_to_key_exists)`
- [ ] `func IsErrNotFound(err error) bool` | Rust:  | Tests: 
- [x] `func IsErrWriteConflict(err error) bool` | Rust: `new-client-rust/src/common/errors.rs (Error::is_write_conflict)` | Tests: `new-client-rust/src/common/errors.rs (key_error_conflict_maps_to_write_conflict)`
- [ ] `func IsErrorCommitTSLag(err error) bool` | Rust:  | Tests: 
- [ ] `func IsErrorUndetermined(err error) bool` | Rust:  | Tests: 
- [ ] `func Log(err error)` | Rust:  | Tests: 
- [ ] `func NewErrPDServerTimeout(msg string) error` | Rust:  | Tests: 
- [ ] `func NewErrWriteConflict(conflict *kvrpcpb.WriteConflict) *ErrWriteConflict` | Rust:  | Tests: 
- [ ] `func NewErrWriteConflictWithArgs(startTs, conflictTs, conflictCommitTs uint64, key []byte, reason kvrpcpb.WriteConflict_Reason) *ErrWriteConflict` | Rust:  | Tests: 

### Consts
- [ ] `MismatchClusterID` | Rust:  | Tests: 

### Vars
- [ ] `ErrBodyMissing` | Rust:  | Tests: 
- [ ] `ErrCannotSetNilValue` | Rust:  | Tests: 
- [ ] `ErrCommitTSLag` | Rust:  | Tests: 
- [ ] `ErrInvalidTxn` | Rust:  | Tests: 
- [ ] `ErrIsWitness` | Rust:  | Tests: 
- [ ] `ErrLockAcquireFailAndNoWaitSet` | Rust:  | Tests: 
- [ ] `ErrLockWaitTimeout` | Rust:  | Tests: 
- [ ] `ErrNotExist` | Rust:  | Tests: 
- [ ] `ErrQueryInterrupted` | Rust:  | Tests: 
- [ ] `ErrRegionDataNotReady` | Rust:  | Tests: 
- [ ] `ErrRegionFlashbackInProgress` | Rust:  | Tests: 
- [ ] `ErrRegionFlashbackNotPrepared` | Rust:  | Tests: 
- [ ] `ErrRegionNotInitialized` | Rust:  | Tests: 
- [ ] `ErrRegionRecoveryInProgress` | Rust:  | Tests: 
- [ ] `ErrRegionUnavailable` | Rust:  | Tests: 
- [ ] `ErrResolveLockTimeout` | Rust:  | Tests: 
- [ ] `ErrResultUndetermined` | Rust:  | Tests: 
- [ ] `ErrTiDBShuttingDown` | Rust:  | Tests: 
- [ ] `ErrTiFlashServerBusy` | Rust:  | Tests: 
- [ ] `ErrTiFlashServerTimeout` | Rust:  | Tests: 
- [ ] `ErrTiKVDiskFull` | Rust:  | Tests: 
- [ ] `ErrTiKVMaxTimestampNotSynced` | Rust:  | Tests: 
- [ ] `ErrTiKVServerBusy` | Rust:  | Tests: 
- [ ] `ErrTiKVServerTimeout` | Rust:  | Tests: 
- [ ] `ErrTiKVStaleCommand` | Rust:  | Tests: 
- [ ] `ErrUnknown` | Rust:  | Tests: 

### Methods
- [x] `func (d *ErrDeadlock) Error() string` | Rust: `new-client-rust/src/common/errors.rs (impl Display for DeadlockError)` | Tests: `new-client-rust/src/common/errors.rs (key_error_deadlock_maps_to_deadlock)`
- [ ] `func (d *PDError) Error() string` | Rust:  | Tests: 
- [x] `func (e *ErrAssertionFailed) Error() string` | Rust: `new-client-rust/src/common/errors.rs (impl Display for AssertionFailedError)` | Tests: `new-client-rust/src/common/errors.rs (key_error_assertion_failed_maps_to_assertion_failed)`
- [ ] `func (e *ErrEntryTooLarge) Error() string` | Rust:  | Tests: 
- [ ] `func (e *ErrGCTooEarly) Error() string` | Rust:  | Tests: 
- [ ] `func (e *ErrKeyTooLarge) Error() string` | Rust:  | Tests: 
- [ ] `func (e *ErrLockOnlyIfExistsNoPrimaryKey) Error() string` | Rust:  | Tests: 
- [ ] `func (e *ErrLockOnlyIfExistsNoReturnValue) Error() string` | Rust:  | Tests: 
- [ ] `func (e *ErrPDServerTimeout) Error() string` | Rust:  | Tests: 
- [ ] `func (e *ErrTokenLimit) Error() string` | Rust:  | Tests: 
- [x] `func (e *ErrTxnAbortedByGC) Error() string` | Rust: `new-client-rust/src/common/errors.rs (Error::TxnAborted Display)` | Tests: `new-client-rust/src/common/errors.rs (key_error_abort_maps_to_txn_aborted)`
- [ ] `func (e *ErrTxnTooLarge) Error() string` | Rust:  | Tests: 
- [x] `func (e *ErrWriteConflictInLatch) Error() string` | Rust: `new-client-rust/src/common/errors.rs (Error::WriteConflictInLatch Display)` | Tests: `new-client-rust/src/transaction/latch.rs (tests)`
- [ ] `func (e ErrQueryInterruptedWithSignal) Error() string` | Rust:  | Tests: 
- [x] `func (k *ErrKeyExist) Error() string` | Rust: `new-client-rust/src/common/errors.rs (impl Display for KeyExistsError)` | Tests: `new-client-rust/src/common/errors.rs (key_error_already_exist_maps_to_key_exists)`
- [x] `func (k *ErrRetryable) Error() string` | Rust: `new-client-rust/src/common/errors.rs (Error::Retryable Display)` | Tests: `new-client-rust/src/common/errors.rs (key_error_retryable_maps_to_retryable)`
- [x] `func (k *ErrWriteConflict) Error() string` | Rust: `new-client-rust/src/common/errors.rs (impl Display for WriteConflictError)` | Tests: `new-client-rust/src/common/errors.rs (key_error_conflict_maps_to_write_conflict)`

## kv (package kv)

### Types
- [ ] `type AccessLocationType byte` | Rust:  | Tests: 
- [ ] `type BatchGetOption interface` | Rust:  | Tests: 
- [ ] `type BatchGetOptions struct` | Rust:  | Tests: 
- [ ] `type BatchGetter interface` | Rust:  | Tests: 
- [ ] `type FlagsOp uint32` | Rust:  | Tests: 
- [ ] `type GetOption interface` | Rust:  | Tests: 
- [ ] `type GetOptions struct` | Rust:  | Tests: 
- [ ] `type GetOrBatchGetOption interface` | Rust:  | Tests: 
- [ ] `type Getter interface` | Rust:  | Tests: 
- [ ] `type KeyFlags uint16` | Rust:  | Tests: 
- [ ] `type KeyRange struct` | Rust:  | Tests: 
- [ ] `type LockCtx struct` | Rust:  | Tests: 
- [x] `type ReplicaReadType byte` | Rust: `new-client-rust/src/replica_read.rs (ReplicaReadType)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_replica_read_peer_selection_and_context_fields)`
- [ ] `type ReturnedValue struct` | Rust:  | Tests: 
- [ ] `type ValueEntry struct` | Rust:  | Tests: 
- [ ] `type Variables struct` | Rust:  | Tests: 

### Functions
- [ ] `func ApplyFlagsOps(origin KeyFlags, ops ...FlagsOp) KeyFlags` | Rust:  | Tests: 
- [ ] `func BatchGetToGetOptions(options []BatchGetOption) []GetOption` | Rust:  | Tests: 
- [ ] `func CmpKey(k, another []byte) int` | Rust:  | Tests: 
- [ ] `func NewLockCtx(forUpdateTS uint64, lockWaitTime int64, waitStartTime time.Time) *LockCtx` | Rust:  | Tests: 
- [ ] `func NewValueEntry(value []byte, commitTS uint64) ValueEntry` | Rust:  | Tests: 
- [ ] `func NewVariables(killed *uint32) *Variables` | Rust:  | Tests: 
- [ ] `func NextKey(k []byte) []byte` | Rust:  | Tests: 
- [ ] `func PrefixNextKey(k []byte) []byte` | Rust:  | Tests: 
- [ ] `func WithReturnCommitTS() GetOrBatchGetOption` | Rust:  | Tests: 

### Consts
- [ ] `AccessCrossZone` | Rust:  | Tests: 
- [ ] `AccessLocalZone` | Rust:  | Tests: 
- [ ] `AccessUnknown` | Rust:  | Tests: 
- [ ] `DefBackOffWeight` | Rust:  | Tests: 
- [ ] `DefBackoffLockFast` | Rust:  | Tests: 
- [ ] `DefTxnCommitBatchSize` | Rust:  | Tests: 
- [ ] `DelKeyLocked` | Rust:  | Tests: 
- [ ] `DelNeedCheckExists` | Rust:  | Tests: 
- [ ] `DelNeedConstraintCheckInPrewrite` | Rust:  | Tests: 
- [ ] `DelNeedLocked` | Rust:  | Tests: 
- [ ] `DelPresumeKeyNotExists` | Rust:  | Tests: 
- [ ] `FlagBytes` | Rust:  | Tests: 
- [ ] `LockAlwaysWait` | Rust:  | Tests: 
- [ ] `LockNoWait` | Rust:  | Tests: 
- [ ] `ReplicaReadFollower` | Rust:  | Tests: 
- [ ] `ReplicaReadLeader` | Rust:  | Tests: 
- [ ] `ReplicaReadLearner` | Rust:  | Tests: 
- [ ] `ReplicaReadMixed` | Rust:  | Tests: 
- [ ] `ReplicaReadPreferLeader` | Rust:  | Tests: 
- [ ] `SetAssertExist` | Rust:  | Tests: 
- [ ] `SetAssertNone` | Rust:  | Tests: 
- [ ] `SetAssertNotExist` | Rust:  | Tests: 
- [ ] `SetAssertUnknown` | Rust:  | Tests: 
- [ ] `SetIgnoredIn2PC` | Rust:  | Tests: 
- [ ] `SetKeyLocked` | Rust:  | Tests: 
- [ ] `SetKeyLockedValueExists` | Rust:  | Tests: 
- [ ] `SetKeyLockedValueNotExists` | Rust:  | Tests: 
- [ ] `SetNeedConstraintCheckInPrewrite` | Rust:  | Tests: 
- [ ] `SetNeedLocked` | Rust:  | Tests: 
- [ ] `SetNewlyInserted` | Rust:  | Tests: 
- [ ] `SetPresumeKeyNotExists` | Rust:  | Tests: 
- [ ] `SetPreviousPresumeKNE` | Rust:  | Tests: 
- [ ] `SetPrewriteOnly` | Rust:  | Tests: 
- [ ] `SetReadable` | Rust:  | Tests: 

### Vars
- [ ] `DefaultVars` | Rust:  | Tests: 
- [ ] `StoreLimit` | Rust:  | Tests: 
- [ ] `TxnCommitBatchSize` | Rust:  | Tests: 

### Methods
- [ ] `func (ctx *LockCtx) GetValueNotLocked(key []byte) ([]byte, bool)` | Rust:  | Tests: 
- [ ] `func (ctx *LockCtx) InitCheckExistence(capacity int)` | Rust:  | Tests: 
- [ ] `func (ctx *LockCtx) InitReturnValues(capacity int)` | Rust:  | Tests: 
- [ ] `func (ctx *LockCtx) IterateValuesNotLocked(f func([]byte, []byte))` | Rust:  | Tests: 
- [ ] `func (ctx *LockCtx) LockWaitTime() int64` | Rust:  | Tests: 
- [ ] `func (e ValueEntry) IsValueEmpty() bool` | Rust:  | Tests: 
- [ ] `func (e ValueEntry) Size() int` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) AndPersistent() KeyFlags` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasAssertExist() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasAssertNotExist() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasAssertUnknown() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasAssertionFlags() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasIgnoredIn2PC() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasLocked() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasLockedValueExists() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasNeedCheckExists() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasNeedConstraintCheckInPrewrite() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasNeedLocked() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasNewlyInserted() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasPresumeKeyNotExists() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasPrewriteOnly() bool` | Rust:  | Tests: 
- [ ] `func (f KeyFlags) HasReadable() bool` | Rust:  | Tests: 
- [ ] `func (o *BatchGetOptions) Apply(opts []BatchGetOption)` | Rust:  | Tests: 
- [ ] `func (o *GetOptions) Apply(opts []GetOption)` | Rust:  | Tests: 
- [ ] `func (r ReplicaReadType) IsFollowerRead() bool` | Rust:  | Tests: 
- [ ] `func (r ReplicaReadType) String() string` | Rust:  | Tests: 

## metrics (package metrics)

### Types
- [ ] `type LabelPair = dto.LabelPair` | Rust:  | Tests: 
- [ ] `type MetricVec interface` | Rust:  | Tests: 
- [ ] `type TxnCommitCounter struct` | Rust:  | Tests: 

### Functions
- [ ] `func FindNextStaleStoreID(collector prometheus.Collector, validStoreIDs map[uint64]struct{}) uint64` | Rust:  | Tests: 
- [ ] `func GetStoreMetricVecList() []MetricVec` | Rust:  | Tests: 
- [ ] `func GetTxnCommitCounter() TxnCommitCounter` | Rust:  | Tests: 
- [ ] `func InitMetrics(namespace, subsystem string)` | Rust:  | Tests: 
- [ ] `func InitMetricsWithConstLabels(namespace, subsystem string, constLabels prometheus.Labels)` | Rust:  | Tests: 
- [ ] `func ObserveReadSLI(readKeys uint64, readTime float64, readSize float64)` | Rust:  | Tests: 
- [ ] `func RegisterMetrics()` | Rust:  | Tests: 

### Consts
- [ ] `LabelBatchRecvLoop` | Rust:  | Tests: 
- [ ] `LabelBatchSendLoop` | Rust:  | Tests: 
- [ ] `LblAbort` | Rust:  | Tests: 
- [ ] `LblAddress` | Rust:  | Tests: 
- [ ] `LblBatchGet` | Rust:  | Tests: 
- [ ] `LblCommit` | Rust:  | Tests: 
- [ ] `LblDirection` | Rust:  | Tests: 
- [ ] `LblFromStore` | Rust:  | Tests: 
- [ ] `LblGeneral` | Rust:  | Tests: 
- [ ] `LblGet` | Rust:  | Tests: 
- [ ] `LblInternal` | Rust:  | Tests: 
- [ ] `LblLockKeys` | Rust:  | Tests: 
- [ ] `LblReason` | Rust:  | Tests: 
- [ ] `LblResult` | Rust:  | Tests: 
- [ ] `LblRollback` | Rust:  | Tests: 
- [ ] `LblScope` | Rust:  | Tests: 
- [ ] `LblSource` | Rust:  | Tests: 
- [ ] `LblStaleRead` | Rust:  | Tests: 
- [ ] `LblStore` | Rust:  | Tests: 
- [ ] `LblTarget` | Rust:  | Tests: 
- [ ] `LblToStore` | Rust:  | Tests: 
- [ ] `LblType` | Rust:  | Tests: 

### Vars
- [ ] `AggressiveLockedKeysDerived` | Rust:  | Tests: 
- [ ] `AggressiveLockedKeysLockedWithConflict` | Rust:  | Tests: 
- [ ] `AggressiveLockedKeysNew` | Rust:  | Tests: 
- [ ] `AggressiveLockedKeysNonForceLock` | Rust:  | Tests: 
- [ ] `AsyncBatchGetCounterWithLockError` | Rust:  | Tests: 
- [ ] `AsyncBatchGetCounterWithOK` | Rust:  | Tests: 
- [ ] `AsyncBatchGetCounterWithOtherError` | Rust:  | Tests: 
- [ ] `AsyncBatchGetCounterWithRegionError` | Rust:  | Tests: 
- [ ] `AsyncCommitTxnCounterError` | Rust:  | Tests: 
- [ ] `AsyncCommitTxnCounterOk` | Rust:  | Tests: 
- [ ] `AsyncSendReqCounterWithOK` | Rust:  | Tests: 
- [ ] `AsyncSendReqCounterWithOtherError` | Rust:  | Tests: 
- [ ] `AsyncSendReqCounterWithRPCError` | Rust:  | Tests: 
- [ ] `AsyncSendReqCounterWithRegionError` | Rust:  | Tests: 
- [ ] `AsyncSendReqCounterWithSendError` | Rust:  | Tests: 
- [ ] `BackoffHistogramDataNotReady` | Rust:  | Tests: 
- [ ] `BackoffHistogramEmpty` | Rust:  | Tests: 
- [ ] `BackoffHistogramIsWitness` | Rust:  | Tests: 
- [ ] `BackoffHistogramLock` | Rust:  | Tests: 
- [ ] `BackoffHistogramLockFast` | Rust:  | Tests: 
- [ ] `BackoffHistogramPD` | Rust:  | Tests: 
- [ ] `BackoffHistogramRPC` | Rust:  | Tests: 
- [ ] `BackoffHistogramRegionMiss` | Rust:  | Tests: 
- [ ] `BackoffHistogramRegionRecoveryInProgress` | Rust:  | Tests: 
- [ ] `BackoffHistogramRegionScheduling` | Rust:  | Tests: 
- [ ] `BackoffHistogramServerBusy` | Rust:  | Tests: 
- [ ] `BackoffHistogramStaleCmd` | Rust:  | Tests: 
- [ ] `BackoffHistogramTiKVDiskFull` | Rust:  | Tests: 
- [ ] `BatchRecvHistogramError` | Rust:  | Tests: 
- [ ] `BatchRecvHistogramOK` | Rust:  | Tests: 
- [ ] `BatchRequestDurationDone` | Rust:  | Tests: 
- [ ] `BatchRequestDurationRecv` | Rust:  | Tests: 
- [ ] `BatchRequestDurationSend` | Rust:  | Tests: 
- [ ] `LagCommitTSAttemptHistogramWithError` | Rust:  | Tests: 
- [ ] `LagCommitTSAttemptHistogramWithOK` | Rust:  | Tests: 
- [ ] `LagCommitTSWaitHistogramWithError` | Rust:  | Tests: 
- [ ] `LagCommitTSWaitHistogramWithOK` | Rust:  | Tests: 
- [ ] `LoadRegionCacheHistogramWhenCacheMiss` | Rust:  | Tests: 
- [ ] `LoadRegionCacheHistogramWithBatchScanRegions` | Rust:  | Tests: 
- [ ] `LoadRegionCacheHistogramWithGetStore` | Rust:  | Tests: 
- [ ] `LoadRegionCacheHistogramWithRegionByID` | Rust:  | Tests: 
- [ ] `LoadRegionCacheHistogramWithRegions` | Rust:  | Tests: 
- [ ] `LockResolverCountWithBatchResolve` | Rust:  | Tests: 
- [ ] `LockResolverCountWithExpired` | Rust:  | Tests: 
- [ ] `LockResolverCountWithNotExpired` | Rust:  | Tests: 
- [ ] `LockResolverCountWithQueryCheckSecondaryLocks` | Rust:  | Tests: 
- [ ] `LockResolverCountWithQueryTxnStatus` | Rust:  | Tests: 
- [ ] `LockResolverCountWithQueryTxnStatusCommitted` | Rust:  | Tests: 
- [ ] `LockResolverCountWithQueryTxnStatusRolledBack` | Rust:  | Tests: 
- [ ] `LockResolverCountWithResolve` | Rust:  | Tests: 
- [ ] `LockResolverCountWithResolveAsync` | Rust:  | Tests: 
- [ ] `LockResolverCountWithResolveForWrite` | Rust:  | Tests: 
- [ ] `LockResolverCountWithResolveLockLite` | Rust:  | Tests: 
- [ ] `LockResolverCountWithResolveLocks` | Rust:  | Tests: 
- [ ] `LockResolverCountWithWaitExpired` | Rust:  | Tests: 
- [ ] `OnePCTxnCounterError` | Rust:  | Tests: 
- [ ] `OnePCTxnCounterFallback` | Rust:  | Tests: 
- [ ] `OnePCTxnCounterOk` | Rust:  | Tests: 
- [ ] `PrewriteAssertionUsageCounterExist` | Rust:  | Tests: 
- [ ] `PrewriteAssertionUsageCounterNone` | Rust:  | Tests: 
- [ ] `PrewriteAssertionUsageCounterNotExist` | Rust:  | Tests: 
- [ ] `PrewriteAssertionUsageCounterUnknown` | Rust:  | Tests: 
- [ ] `RawkvCmdHistogramWithBatchDelete` | Rust:  | Tests: 
- [ ] `RawkvCmdHistogramWithBatchGet` | Rust:  | Tests: 
- [ ] `RawkvCmdHistogramWithBatchPut` | Rust:  | Tests: 
- [ ] `RawkvCmdHistogramWithDelete` | Rust:  | Tests: 
- [ ] `RawkvCmdHistogramWithGet` | Rust:  | Tests: 
- [ ] `RawkvCmdHistogramWithRawChecksum` | Rust:  | Tests: 
- [ ] `RawkvCmdHistogramWithRawReversScan` | Rust:  | Tests: 
- [ ] `RawkvCmdHistogramWithRawScan` | Rust:  | Tests: 
- [ ] `RawkvSizeHistogramWithKey` | Rust:  | Tests: 
- [ ] `RawkvSizeHistogramWithValue` | Rust:  | Tests: 
- [ ] `ReadRequestFollowerLocalBytes` | Rust:  | Tests: 
- [ ] `ReadRequestFollowerRemoteBytes` | Rust:  | Tests: 
- [ ] `ReadRequestLeaderLocalBytes` | Rust:  | Tests: 
- [ ] `ReadRequestLeaderRemoteBytes` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithBatchScanRegionsError` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithBatchScanRegionsOK` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithGetCacheMissError` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithGetCacheMissOK` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithGetRegionByIDError` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithGetRegionByIDOK` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithGetStoreError` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithGetStoreOK` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithInvalidateRegionFromCacheOK` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithInvalidateStoreRegionsOK` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithScanRegionsError` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithScanRegionsOK` | Rust:  | Tests: 
- [ ] `RegionCacheCounterWithSendFail` | Rust:  | Tests: 
- [ ] `SecondaryLockCleanupFailureCounterCommit` | Rust:  | Tests: 
- [ ] `SecondaryLockCleanupFailureCounterRollback` | Rust:  | Tests: 
- [ ] `StaleReadHitCounter` | Rust:  | Tests: 
- [ ] `StaleReadLocalInBytes` | Rust:  | Tests: 
- [ ] `StaleReadLocalOutBytes` | Rust:  | Tests: 
- [ ] `StaleReadMissCounter` | Rust:  | Tests: 
- [ ] `StaleReadRemoteInBytes` | Rust:  | Tests: 
- [ ] `StaleReadRemoteOutBytes` | Rust:  | Tests: 
- [ ] `StaleReadReqCrossZoneCounter` | Rust:  | Tests: 
- [ ] `StaleReadReqLocalCounter` | Rust:  | Tests: 
- [ ] `StatusCountWithError` | Rust:  | Tests: 
- [ ] `StatusCountWithOK` | Rust:  | Tests: 
- [ ] `TiKVAggressiveLockedKeysCounter` | Rust:  | Tests: 
- [ ] `TiKVAsyncBatchGetCounter` | Rust:  | Tests: 
- [ ] `TiKVAsyncCommitTxnCounter` | Rust:  | Tests: 
- [ ] `TiKVAsyncSendReqCounter` | Rust:  | Tests: 
- [ ] `TiKVBackoffHistogram` | Rust:  | Tests: 
- [ ] `TiKVBatchBestSize` | Rust:  | Tests: 
- [ ] `TiKVBatchClientRecycle` | Rust:  | Tests: 
- [ ] `TiKVBatchClientUnavailable` | Rust:  | Tests: 
- [ ] `TiKVBatchClientWaitEstablish` | Rust:  | Tests: 
- [ ] `TiKVBatchHeadArrivalInterval` | Rust:  | Tests: 
- [ ] `TiKVBatchMoreRequests` | Rust:  | Tests: 
- [ ] `TiKVBatchPendingRequests` | Rust:  | Tests: 
- [ ] `TiKVBatchRecvLoopDuration` | Rust:  | Tests: 
- [ ] `TiKVBatchRecvTailLatency` | Rust:  | Tests: 
- [ ] `TiKVBatchRequestDuration` | Rust:  | Tests: 
- [ ] `TiKVBatchRequests` | Rust:  | Tests: 
- [ ] `TiKVBatchSendLoopDuration` | Rust:  | Tests: 
- [ ] `TiKVBatchSendTailLatency` | Rust:  | Tests: 
- [ ] `TiKVBatchWaitOverLoad` | Rust:  | Tests: 
- [ ] `TiKVBucketClampedCounter` | Rust:  | Tests: 
- [ ] `TiKVFeedbackSlowScoreGauge` | Rust:  | Tests: 
- [ ] `TiKVForwardRequestCounter` | Rust:  | Tests: 
- [ ] `TiKVGRPCConnTransientFailureCounter` | Rust:  | Tests: 
- [ ] `TiKVGrpcConnectionState` | Rust:  | Tests: 
- [ ] `TiKVHealthFeedbackOpsCounter` | Rust:  | Tests: 
- [ ] `TiKVLoadRegionCacheHistogram` | Rust:  | Tests: 
- [ ] `TiKVLoadRegionCounter` | Rust:  | Tests: 
- [ ] `TiKVLoadTxnSafePointCounter` | Rust:  | Tests: 
- [ ] `TiKVLocalLatchWaitTimeHistogram` | Rust:  | Tests: 
- [ ] `TiKVLockResolverCounter` | Rust:  | Tests: 
- [ ] `TiKVLowResolutionTSOUpdateIntervalSecondsGauge` | Rust:  | Tests: 
- [ ] `TiKVMinSafeTSGapSeconds` | Rust:  | Tests: 
- [ ] `TiKVNoAvailableConnectionCounter` | Rust:  | Tests: 
- [ ] `TiKVOnePCTxnCounter` | Rust:  | Tests: 
- [ ] `TiKVPanicCounter` | Rust:  | Tests: 
- [ ] `TiKVPessimisticLockKeysDuration` | Rust:  | Tests: 
- [ ] `TiKVPipelinedFlushDuration` | Rust:  | Tests: 
- [ ] `TiKVPipelinedFlushLenHistogram` | Rust:  | Tests: 
- [ ] `TiKVPipelinedFlushSizeHistogram` | Rust:  | Tests: 
- [ ] `TiKVPipelinedFlushThrottleSecondsHistogram` | Rust:  | Tests: 
- [ ] `TiKVPreferLeaderFlowsGauge` | Rust:  | Tests: 
- [ ] `TiKVPrewriteAssertionUsageCounter` | Rust:  | Tests: 
- [ ] `TiKVRPCErrorCounter` | Rust:  | Tests: 
- [ ] `TiKVRPCNetLatencyHistogram` | Rust:  | Tests: 
- [ ] `TiKVRangeTaskPushDuration` | Rust:  | Tests: 
- [ ] `TiKVRangeTaskStats` | Rust:  | Tests: 
- [ ] `TiKVRawkvCmdHistogram` | Rust:  | Tests: 
- [ ] `TiKVRawkvSizeHistogram` | Rust:  | Tests: 
- [ ] `TiKVReadRequestBytes` | Rust:  | Tests: 
- [ ] `TiKVReadThroughput` | Rust:  | Tests: 
- [ ] `TiKVRegionCacheCounter` | Rust:  | Tests: 
- [ ] `TiKVRegionErrorCounter` | Rust:  | Tests: 
- [ ] `TiKVReplicaSelectorFailureCounter` | Rust:  | Tests: 
- [ ] `TiKVRequestRetryTimesHistogram` | Rust:  | Tests: 
- [ ] `TiKVSafeTSUpdateCounter` | Rust:  | Tests: 
- [ ] `TiKVSecondaryLockCleanupFailureCounter` | Rust:  | Tests: 
- [ ] `TiKVSendReqBySourceSummary` | Rust:  | Tests: 
- [ ] `TiKVSendReqHistogram` | Rust:  | Tests: 
- [ ] `TiKVSmallReadDuration` | Rust:  | Tests: 
- [ ] `TiKVStaleReadBytes` | Rust:  | Tests: 
- [ ] `TiKVStaleReadCounter` | Rust:  | Tests: 
- [ ] `TiKVStaleReadReqCounter` | Rust:  | Tests: 
- [ ] `TiKVStaleRegionFromPDCounter` | Rust:  | Tests: 
- [ ] `TiKVStatusCounter` | Rust:  | Tests: 
- [ ] `TiKVStatusDuration` | Rust:  | Tests: 
- [ ] `TiKVStoreLimitErrorCounter` | Rust:  | Tests: 
- [ ] `TiKVStoreLivenessGauge` | Rust:  | Tests: 
- [ ] `TiKVStoreSlowScoreGauge` | Rust:  | Tests: 
- [ ] `TiKVTSFutureWaitDuration` | Rust:  | Tests: 
- [ ] `TiKVTTLLifeTimeReachCounter` | Rust:  | Tests: 
- [ ] `TiKVTTLManagerHistogram` | Rust:  | Tests: 
- [ ] `TiKVTokenWaitDuration` | Rust:  | Tests: 
- [ ] `TiKVTwoPCTxnCounter` | Rust:  | Tests: 
- [ ] `TiKVTxnCmdHistogram` | Rust:  | Tests: 
- [ ] `TiKVTxnCommitBackoffCount` | Rust:  | Tests: 
- [ ] `TiKVTxnCommitBackoffSeconds` | Rust:  | Tests: 
- [ ] `TiKVTxnHeartBeatHistogram` | Rust:  | Tests: 
- [ ] `TiKVTxnLagCommitTSAttemptHistogram` | Rust:  | Tests: 
- [ ] `TiKVTxnLagCommitTSWaitHistogram` | Rust:  | Tests: 
- [ ] `TiKVTxnRegionsNumHistogram` | Rust:  | Tests: 
- [ ] `TiKVTxnWriteConflictCounter` | Rust:  | Tests: 
- [ ] `TiKVTxnWriteKVCountHistogram` | Rust:  | Tests: 
- [ ] `TiKVTxnWriteSizeHistogram` | Rust:  | Tests: 
- [ ] `TiKVUnsafeDestroyRangeFailuresCounterVec` | Rust:  | Tests: 
- [ ] `TiKVValidateReadTSFromPDCount` | Rust:  | Tests: 
- [ ] `TwoPCTxnCounterError` | Rust:  | Tests: 
- [ ] `TwoPCTxnCounterOk` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithBatchGetGeneral` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithBatchGetInternal` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithCommitGeneral` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithCommitInternal` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithGetGeneral` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithGetInternal` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithLockKeysGeneral` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithLockKeysInternal` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithRollbackGeneral` | Rust:  | Tests: 
- [ ] `TxnCmdHistogramWithRollbackInternal` | Rust:  | Tests: 
- [ ] `TxnHeartBeatHistogramError` | Rust:  | Tests: 
- [ ] `TxnHeartBeatHistogramOK` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramCleanup` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramCleanupInternal` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramCommit` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramCommitInternal` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramPessimisticLock` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramPessimisticLockInternal` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramPessimisticRollback` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramPessimisticRollbackInternal` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramPrewrite` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramPrewriteInternal` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramWithBatchCoprocessor` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramWithBatchCoprocessorInternal` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramWithCoprocessor` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramWithCoprocessorInternal` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramWithSnapshot` | Rust:  | Tests: 
- [ ] `TxnRegionsNumHistogramWithSnapshotInternal` | Rust:  | Tests: 
- [ ] `TxnWriteKVCountHistogramGeneral` | Rust:  | Tests: 
- [ ] `TxnWriteKVCountHistogramInternal` | Rust:  | Tests: 
- [ ] `TxnWriteSizeHistogramGeneral` | Rust:  | Tests: 
- [ ] `TxnWriteSizeHistogramInternal` | Rust:  | Tests: 

### Methods
- [ ] `func (c TxnCommitCounter) Sub(rhs TxnCommitCounter) TxnCommitCounter` | Rust:  | Tests: 

## oracle (package oracle)

### Types
- [ ] `type ErrFutureTSRead struct` | Rust:  | Tests: 
- [ ] `type ErrLatestStaleRead struct` | Rust:  | Tests: 
- [ ] `type Future interface` | Rust:  | Tests: 
- [ ] `type NoopReadTSValidator struct` | Rust:  | Tests: 
- [ ] `type Option struct` | Rust:  | Tests: 
- [ ] `type Oracle interface` | Rust:  | Tests: 
- [ ] `type ReadTSValidator interface` | Rust:  | Tests: 

### Functions
- [ ] `func ComposeTS(physical, logical int64) uint64` | Rust:  | Tests: 
- [ ] `func ExtractLogical(ts uint64) int64` | Rust:  | Tests: 
- [ ] `func ExtractPhysical(ts uint64) int64` | Rust:  | Tests: 
- [ ] `func GetPhysical(t time.Time) int64` | Rust:  | Tests: 
- [ ] `func GetTimeFromTS(ts uint64) time.Time` | Rust:  | Tests: 
- [ ] `func GoTimeToLowerLimitStartTS(now time.Time, maxTxnTimeUse int64) uint64` | Rust:  | Tests: 
- [ ] `func GoTimeToTS(t time.Time) uint64` | Rust:  | Tests: 

### Consts
- [ ] `GlobalTxnScope` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- [ ] `func (ErrLatestStaleRead) Error() string` | Rust:  | Tests: 
- [ ] `func (NoopReadTSValidator) ValidateReadTS(ctx context.Context, readTS uint64, isStaleRead bool, opt *Option) error` | Rust:  | Tests: 
- [ ] `func (e ErrFutureTSRead) Error() string` | Rust:  | Tests: 

## oracle/oracles (package oracles)

### Types
- [ ] `type MockOracle struct` | Rust:  | Tests: 
- [ ] `type PDOracleOptions struct` | Rust:  | Tests: 
- [ ] `type ValidateReadTSForTidbSnapshot struct` | Rust:  | Tests: 

### Functions
- [ ] `func NewLocalOracle() oracle.Oracle` | Rust:  | Tests: 
- [ ] `func NewPdOracle(pdClient pd.Client, options *PDOracleOptions) (oracle.Oracle, error)` | Rust:  | Tests: 

### Consts
- (none)

### Vars
- [ ] `EnableTSValidation` | Rust:  | Tests: 

### Methods
- [ ] `func (o *MockOracle) AddOffset(d time.Duration)` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) Close()` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) Disable()` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) Enable()` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) GetAllTSOKeyspaceGroupMinTS(ctx context.Context) (uint64, error)` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) GetExternalTimestamp(ctx context.Context) (uint64, error)` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) GetLowResolutionTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error)` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) GetLowResolutionTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) GetStaleTimestamp(ctx context.Context, txnScope string, prevSecond uint64) (ts uint64, err error)` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) GetTimestamp(ctx context.Context, _ *oracle.Option) (uint64, error)` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) GetTimestampAsync(ctx context.Context, _ *oracle.Option) oracle.Future` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) IsExpired(lockTimestamp, TTL uint64, _ *oracle.Option) bool` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) SetExternalTimestamp(ctx context.Context, newTimestamp uint64) error` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) SetLowResolutionTimestampUpdateInterval(time.Duration) error` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) UntilExpired(lockTimeStamp, TTL uint64, _ *oracle.Option) int64` | Rust:  | Tests: 
- [ ] `func (o *MockOracle) ValidateReadTS(ctx context.Context, readTS uint64, isStaleRead bool, opt *oracle.Option) error` | Rust:  | Tests: 

## rawkv (package rawkv)

### Types
- [x] `type Client struct` | Rust: `tikv_client::RawClient` (new-client-rust/src/raw/client.rs) | Tests: `new-client-rust/src/raw/client.rs (tests)`
- [ ] `type ClientOpt func(*option)` | Rust:  | Tests: 
- [ ] `type ClientProbe struct` | Rust:  | Tests: 
- [ ] `type ConfigProbe struct` | Rust:  | Tests: 
- [x] `type RawChecksum struct` | Rust: `tikv_client::RawChecksum` (new-client-rust/src/raw/mod.rs) | Tests: `new-client-rust/src/raw/requests.rs (test_raw_checksum_merge)`
- [ ] `type RawOption interface` | Rust:  | Tests: 

### Functions
- [x] `func NewClient(ctx context.Context, pdAddrs []string, security config.Security, opts ...opt.ClientOption) (*Client, error)` | Rust: `RawClient::new_with_config` + `Config::with_security` (new-client-rust/src/{raw/client.rs,config.rs}) | Tests: `new-client-rust/src/common/security.rs (test_security)`
- [x] `func NewClientWithOpts(ctx context.Context, pdAddrs []string, opts ...ClientOpt) (*Client, error)` | Rust: `RawClient::new` / `RawClient::new_with_config` (new-client-rust/src/raw/client.rs) | Tests: `new-client-rust/src/raw/client.rs (tests)`
- [x] `func ScanKeyOnly() RawOption` | Rust: `RawClient::{scan_keys,scan_keys_reverse}` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [x] `func SetColumnFamily(cf string) RawOption` | Rust: `RawClient::with_cf` + `ColumnFamily::try_from` (new-client-rust/src/raw/{client.rs,mod.rs}) | Tests: N/A
- [ ] `func WithAPIVersion(apiVersion kvrpcpb.APIVersion) ClientOpt` | Rust:  | Tests: 
- [ ] `func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOpt` | Rust:  | Tests: 
- [x] `func WithKeyspace(name string) ClientOpt` | Rust: `Config::with_keyspace` / `Config::with_default_keyspace` (new-client-rust/src/config.rs) | Tests: N/A
- [ ] `func WithPDOptions(opts ...opt.ClientOption) ClientOpt` | Rust:  | Tests: 
- [x] `func WithSecurity(security config.Security) ClientOpt` | Rust: `Config::with_security` (new-client-rust/src/config.rs) | Tests: `new-client-rust/src/common/security.rs (test_security)`

### Consts
- (none)

### Vars
- [x] `ErrMaxScanLimitExceeded` | Rust: `Error::MaxScanLimitExceeded` (new-client-rust/src/common/errors.rs) | Tests: N/A
- [x] `MaxRawKVScanLimit` | Rust: `MAX_RAW_KV_SCAN_LIMIT` (new-client-rust/src/raw/client.rs) | Tests: N/A

### Methods
- [x] `func (c *Client) BatchDelete(ctx context.Context, keys [][]byte, options ...RawOption) error` | Rust: `RawClient::batch_delete` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) BatchGet(ctx context.Context, keys [][]byte, options ...RawOption) ([][]byte, error)` | Rust: `RawClient::batch_get` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) BatchPut(ctx context.Context, keys, values [][]byte, options ...RawOption) error` | Rust: `RawClient::batch_put` (new-client-rust/src/raw/client.rs) | Tests: `new-client-rust/src/raw/client.rs (test_batch_put_with_ttl)`
- [x] `func (c *Client) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, options ...RawOption) error` | Rust: `RawClient::batch_put_with_ttl` (new-client-rust/src/raw/client.rs) | Tests: `new-client-rust/src/raw/client.rs (test_batch_put_with_ttl)`
- [x] `func (c *Client) Checksum(ctx context.Context, startKey, endKey []byte, options ...RawOption, ) (check RawChecksum, err error)` | Rust: `RawClient::checksum` (new-client-rust/src/raw/client.rs) | Tests: `new-client-rust/src/raw/requests.rs (test_raw_checksum_merge)`
- [x] `func (c *Client) Close() error` | Rust: N/A (out-of-scope: drop closes client) | Tests: N/A
- [ ] `func (c *Client) ClusterID() uint64` | Rust:  | Tests: 
- [x] `func (c *Client) CompareAndSwap(ctx context.Context, key, previousValue, newValue []byte, options ...RawOption) ([]byte, bool, error)` | Rust: `RawClient::compare_and_swap` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) Delete(ctx context.Context, key []byte, options ...RawOption) error` | Rust: `RawClient::delete` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) DeleteRange(ctx context.Context, startKey []byte, endKey []byte, options ...RawOption) error` | Rust: `RawClient::delete_range` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) Get(ctx context.Context, key []byte, options ...RawOption) ([]byte, error)` | Rust: `RawClient::get` (new-client-rust/src/raw/client.rs) | Tests: `new-client-rust/src/raw/client.rs (test_request_source_and_resource_group_tag)`
- [x] `func (c *Client) GetKeyTTL(ctx context.Context, key []byte, options ...RawOption) (*uint64, error)` | Rust: `RawClient::get_key_ttl_secs` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [ ] `func (c *Client) GetPDClient() pd.Client` | Rust:  | Tests: 
- [x] `func (c *Client) Put(ctx context.Context, key, value []byte, options ...RawOption) error` | Rust: `RawClient::put` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64, options ...RawOption) error` | Rust: `RawClient::put_with_ttl` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [ ] `func (c *Client) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int, options ...RawOption) (keys [][]byte, values [][]byte, err error)` | Rust:  | Tests: 
- [ ] `func (c *Client) Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...RawOption, ) (keys [][]byte, values [][]byte, err error)` | Rust:  | Tests: 
- [x] `func (c *Client) SetAtomicForCAS(b bool) *Client` | Rust: `RawClient::with_atomic_for_cas` (new-client-rust/src/raw/client.rs) | Tests: N/A
- [x] `func (c *Client) SetColumnFamily(columnFamily string) *Client` | Rust: `RawClient::with_cf` + `ColumnFamily::try_from` (new-client-rust/src/raw/{client.rs,mod.rs}) | Tests: N/A
- [ ] `func (c ClientProbe) GetRegionCache() *locate.RegionCache` | Rust:  | Tests: 
- [ ] `func (c ClientProbe) SetPDClient(client pd.Client)` | Rust:  | Tests: 
- [ ] `func (c ClientProbe) SetRPCClient(client client.Client)` | Rust:  | Tests: 
- [ ] `func (c ClientProbe) SetRegionCache(regionCache *locate.RegionCache)` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetRawBatchPutSize() int` | Rust:  | Tests: 

## tikv (package tikv)

### Types
- [ ] `type BackoffConfig = retry.Config` | Rust:  | Tests: 
- [ ] `type Backoffer = retry.Backoffer` | Rust:  | Tests: 
- [ ] `type BaseRegionLockResolver struct` | Rust:  | Tests: 
- [ ] `type BatchLocateKeyRangesOpt = locate.BatchLocateKeyRangesOpt` | Rust:  | Tests: 
- [ ] `type BinlogWriteResult = transaction.BinlogWriteResult` | Rust:  | Tests: 
- [ ] `type Client = client.Client` | Rust:  | Tests: 
- [ ] `type ClientEventListener = client.ClientEventListener` | Rust:  | Tests: 
- [ ] `type ClientOpt = client.Opt` | Rust:  | Tests: 
- [ ] `type Codec = apicodec.Codec` | Rust:  | Tests: 
- [ ] `type CodecClient struct` | Rust:  | Tests: 
- [ ] `type CodecPDClient = locate.CodecPDClient` | Rust:  | Tests: 
- [ ] `type ConfigProbe struct` | Rust:  | Tests: 
- [ ] `type EtcdSafePointKV struct` | Rust:  | Tests: 
- [ ] `type GCOpt func(*gcOption)` | Rust:  | Tests: 
- [ ] `type Getter = kv.Getter` | Rust:  | Tests: 
- [ ] `type Iterator = unionstore.Iterator` | Rust:  | Tests: 
- [ ] `type KVFilter = transaction.KVFilter` | Rust:  | Tests: 
- [ ] `type KVStore struct` | Rust:  | Tests: 
- [ ] `type KVTxn = transaction.KVTxn` | Rust:  | Tests: 
- [ ] `type KeyLocation = locate.KeyLocation` | Rust:  | Tests: 
- [ ] `type KeyRange = kv.KeyRange` | Rust:  | Tests: 
- [ ] `type KeyspaceID = apicodec.KeyspaceID` | Rust:  | Tests: 
- [ ] `type LabelFilter = locate.LabelFilter` | Rust:  | Tests: 
- [ ] `type LockResolverProbe struct` | Rust:  | Tests: 
- [ ] `type MemBuffer = unionstore.MemBuffer` | Rust:  | Tests: 
- [ ] `type MemBufferSnapshot = unionstore.MemBufferSnapshot` | Rust:  | Tests: 
- [ ] `type MemDB = unionstore.MemDB` | Rust:  | Tests: 
- [ ] `type MemDBCheckpoint = unionstore.MemDBCheckpoint` | Rust:  | Tests: 
- [ ] `type Metrics = unionstore.Metrics` | Rust:  | Tests: 
- [ ] `type MockSafePointKV struct` | Rust:  | Tests: 
- [ ] `type Mode = apicodec.Mode` | Rust:  | Tests: 
- [ ] `type Option func(*KVStore)` | Rust:  | Tests: 
- [ ] `type Pool interface` | Rust:  | Tests: 
- [ ] `type RPCCanceller = locate.RPCCanceller` | Rust:  | Tests: 
- [ ] `type RPCCancellerCtxKey = locate.RPCCancellerCtxKey` | Rust:  | Tests: 
- [ ] `type RPCContext = locate.RPCContext` | Rust:  | Tests: 
- [ ] `type RPCRuntimeStats = locate.RPCRuntimeStats` | Rust:  | Tests: 
- [ ] `type Region = locate.Region` | Rust:  | Tests: 
- [ ] `type RegionCache = locate.RegionCache` | Rust:  | Tests: 
- [ ] `type RegionLockResolver interface` | Rust:  | Tests: 
- [ ] `type RegionRequestRuntimeStats = locate.RegionRequestRuntimeStats` | Rust:  | Tests: 
- [ ] `type RegionRequestSender = locate.RegionRequestSender` | Rust:  | Tests: 
- [ ] `type RegionVerID = locate.RegionVerID` | Rust:  | Tests: 
- [ ] `type SafePointKV interface` | Rust:  | Tests: 
- [ ] `type SafePointKVOpt func(*option)` | Rust:  | Tests: 
- [ ] `type SchemaLeaseChecker = transaction.SchemaLeaseChecker` | Rust:  | Tests: 
- [ ] `type SchemaVer = transaction.SchemaVer` | Rust:  | Tests: 
- [ ] `type Spool struct` | Rust:  | Tests: 
- [ ] `type Storage interface` | Rust:  | Tests: 
- [ ] `type Store = locate.Store` | Rust:  | Tests: 
- [ ] `type StoreProbe struct` | Rust:  | Tests: 
- [ ] `type StoreSelectorOption = locate.StoreSelectorOption` | Rust:  | Tests: 
- [ ] `type TxnOption func(*transaction.TxnOptions)` | Rust:  | Tests: 
- [ ] `type Variables = kv.Variables` | Rust:  | Tests: 

### Functions
- [ ] `func BoPDRPC() *BackoffConfig` | Rust:  | Tests: 
- [ ] `func BoRegionMiss() *BackoffConfig` | Rust:  | Tests: 
- [ ] `func BoTiFlashRPC() *BackoffConfig` | Rust:  | Tests: 
- [ ] `func BoTiKVRPC() *BackoffConfig` | Rust:  | Tests: 
- [ ] `func BoTxnLock() *BackoffConfig` | Rust:  | Tests: 
- [ ] `func ChangePDRegionMetaCircuitBreakerSettings(apply func(config *circuitbreaker.Settings))` | Rust:  | Tests: 
- [ ] `func CodecV1ExcludePrefixes() [][]byte` | Rust:  | Tests: 
- [ ] `func CodecV2Prefixes() [][]byte` | Rust:  | Tests: 
- [ ] `func DisableResourceControl()` | Rust:  | Tests: 
- [ ] `func EnableResourceControl()` | Rust:  | Tests: 
- [ ] `func GetStoreTypeByMeta(store *metapb.Store) tikvrpc.EndpointType` | Rust:  | Tests: 
- [ ] `func LoadShuttingDown() uint32` | Rust:  | Tests: 
- [ ] `func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer` | Rust:  | Tests: 
- [ ] `func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer` | Rust:  | Tests: 
- [ ] `func NewEtcdSafePointKV(addrs []string, tlsConfig *tls.Config, opts ...SafePointKVOpt) (*EtcdSafePointKV, error)` | Rust:  | Tests: 
- [ ] `func NewGcResolveLockMaxBackoffer(ctx context.Context) *Backoffer` | Rust:  | Tests: 
- [ ] `func NewKVStore( uuid string, pdClient pd.Client, spkv SafePointKV, tikvclient Client, opt ...Option, ) (_ *KVStore, retErr error)` | Rust:  | Tests: 
- [ ] `func NewLockResolver(etcdAddrs []string, security config.Security, opts ...opt.ClientOption) ( *txnlock.LockResolver, error, )` | Rust:  | Tests: 
- [ ] `func NewLockResolverProb(r *txnlock.LockResolver) *LockResolverProbe` | Rust:  | Tests: 
- [ ] `func NewMockSafePointKV(opts ...SafePointKVOpt) *MockSafePointKV` | Rust:  | Tests: 
- [ ] `func NewPDClient(pdAddrs []string) (pd.Client, error)` | Rust:  | Tests: 
- [ ] `func NewRPCClient(opts ...ClientOpt) *client.RPCClient` | Rust:  | Tests: 
- [ ] `func NewRPCanceller() *RPCCanceller` | Rust:  | Tests: 
- [ ] `func NewRegionCache(pdClient pd.Client) *locate.RegionCache` | Rust:  | Tests: 
- [ ] `func NewRegionLockResolver(identifier string, store Storage) *BaseRegionLockResolver` | Rust:  | Tests: 
- [ ] `func NewRegionRequestRuntimeStats() *RegionRequestRuntimeStats` | Rust:  | Tests: 
- [ ] `func NewRegionRequestSender(regionCache *RegionCache, client client.Client, readTSValidator oracle.ReadTSValidator) *RegionRequestSender` | Rust:  | Tests: 
- [ ] `func NewRegionVerID(id, confVer, ver uint64) RegionVerID` | Rust:  | Tests: 
- [ ] `func NewSpool(n int, dur time.Duration) *Spool` | Rust:  | Tests: 
- [ ] `func NewTestKeyspaceTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client, txnLocalLatches uint, keyspaceMeta keyspacepb.KeyspaceMeta, opt ...Option) (*KVStore, error)` | Rust:  | Tests: 
- [ ] `func NewTestTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client, txnLocalLatches uint, opt ...Option) (*KVStore, error)` | Rust:  | Tests: 
- [ ] `func ResolveLocksForRange( ctx context.Context, resolver RegionLockResolver, maxVersion uint64, startKey []byte, endKey []byte, createBackoffFn func(context.Context) *Backoffer, scanLimit uint32, ) (rangetask.TaskStat, error)` | Rust:  | Tests: 
- [ ] `func SetLogContextKey(key interface{})` | Rust:  | Tests: 
- [ ] `func SetRegionCacheTTLSec(t int64)` | Rust:  | Tests: 
- [ ] `func SetRegionCacheTTLWithJitter(base int64, jitter int64)` | Rust:  | Tests: 
- [ ] `func SetResourceControlInterceptor(interceptor resourceControlClient.ResourceGroupKVInterceptor)` | Rust:  | Tests: 
- [ ] `func SetStoreLivenessTimeout(t time.Duration)` | Rust:  | Tests: 
- [ ] `func StoreShuttingDown(v uint32)` | Rust:  | Tests: 
- [ ] `func TxnStartKey() interface{}` | Rust:  | Tests: 
- [ ] `func UnsetResourceControlInterceptor()` | Rust:  | Tests: 
- [ ] `func WithCodec(codec apicodec.Codec) ClientOpt` | Rust:  | Tests: 
- [ ] `func WithConcurrency(concurrency int) GCOpt` | Rust:  | Tests: 
- [ ] `func WithDefaultPipelinedTxn() TxnOption` | Rust:  | Tests: 
- [ ] `func WithLogContext(ctx context.Context, logger *zap.Logger) context.Context` | Rust:  | Tests: 
- [ ] `func WithMatchLabels(labels []*metapb.StoreLabel) StoreSelectorOption` | Rust:  | Tests: 
- [ ] `func WithMatchStores(stores []uint64) StoreSelectorOption` | Rust:  | Tests: 
- [ ] `func WithPDHTTPClient( source string, pdAddrs []string, opts ...pdhttp.ClientOption, ) Option` | Rust:  | Tests: 
- [ ] `func WithPipelinedTxn( flushConcurrency, resolveLockConcurrency int, writeThrottleRatio float64, ) TxnOption` | Rust:  | Tests: 
- [ ] `func WithPool(gp Pool) Option` | Rust:  | Tests: 
- [ ] `func WithPrefix(prefix string) SafePointKVOpt` | Rust:  | Tests: 
- [ ] `func WithSecurity(security config.Security) ClientOpt` | Rust:  | Tests: 
- [ ] `func WithStartTS(startTS uint64) TxnOption` | Rust:  | Tests: 
- [ ] `func WithTxnScope(txnScope string) TxnOption` | Rust:  | Tests: 
- [ ] `func WithUpdateInterval(updateInterval time.Duration) Option` | Rust:  | Tests: 

### Consts
- [ ] `CodecV2RawKeyspacePrefix` | Rust:  | Tests: 
- [ ] `CodecV2TxnKeyspacePrefix` | Rust:  | Tests: 
- [ ] `DCLabelKey` | Rust:  | Tests: 
- [ ] `EpochNotMatch` | Rust:  | Tests: 
- [ ] `GCScanLockLimit` | Rust:  | Tests: 
- [ ] `GcSavedSafePoint` | Rust:  | Tests: 
- [ ] `GcStateCacheInterval` | Rust:  | Tests: 
- [ ] `MaxTxnTimeUse` | Rust:  | Tests: 
- [ ] `MaxWriteExecutionTime` | Rust:  | Tests: 
- [ ] `ModeRaw` | Rust:  | Tests: 
- [ ] `ModeTxn` | Rust:  | Tests: 
- [ ] `NullspaceID` | Rust:  | Tests: 
- [ ] `ReadTimeoutMedium` | Rust:  | Tests: 
- [ ] `ReadTimeoutShort` | Rust:  | Tests: 

### Vars
- [ ] `DecodeKey` | Rust:  | Tests: 
- [ ] `DefaultKeyspaceID` | Rust:  | Tests: 
- [ ] `DefaultKeyspaceName` | Rust:  | Tests: 
- [ ] `EnableFailpoints` | Rust:  | Tests: 
- [ ] `LabelFilterAllNode` | Rust:  | Tests: 
- [ ] `LabelFilterAllTiFlashNode` | Rust:  | Tests: 
- [ ] `LabelFilterNoTiFlashWriteNode` | Rust:  | Tests: 
- [ ] `LabelFilterOnlyTiFlashWriteNode` | Rust:  | Tests: 
- [ ] `NewCodecPDClient` | Rust:  | Tests: 
- [ ] `NewCodecPDClientWithKeyspace` | Rust:  | Tests: 
- [ ] `NewCodecV1` | Rust:  | Tests: 
- [ ] `NewCodecV2` | Rust:  | Tests: 
- [ ] `NewNoopBackoff` | Rust:  | Tests: 
- [ ] `WithNeedBuckets` | Rust:  | Tests: 
- [ ] `WithNeedRegionHasLeaderPeer` | Rust:  | Tests: 

### Methods
- [ ] `func (c *CodecClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)` | Rust:  | Tests: 
- [ ] `func (c *CodecClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response])` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetBigTxnThreshold() int` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetDefaultLockTTL() uint64` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetGetMaxBackoff() int` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetScanBatchSize() int` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetTTLFactor() int` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetTxnCommitBatchSize() uint64` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) LoadPreSplitDetectThreshold() uint32` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) LoadPreSplitSizeThreshold() uint32` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) SetOracleUpdateInterval(v int)` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) StorePreSplitDetectThreshold(v uint32)` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) StorePreSplitSizeThreshold(v uint32)` | Rust:  | Tests: 
- [ ] `func (l *BaseRegionLockResolver) GetStore() Storage` | Rust:  | Tests: 
- [ ] `func (l *BaseRegionLockResolver) Identifier() string` | Rust:  | Tests: 
- [ ] `func (l *BaseRegionLockResolver) ResolveLocksInOneRegion(bo *Backoffer, locks []*txnlock.Lock, loc *locate.KeyLocation) (*locate.KeyLocation, error)` | Rust:  | Tests: 
- [ ] `func (l *BaseRegionLockResolver) ScanLocksInOneRegion(bo *Backoffer, key []byte, endKey []byte, maxVersion uint64, scanLimit uint32) ([]*txnlock.Lock, *locate.KeyLocation, error)` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) ForceResolveLock(ctx context.Context, lock *txnlock.Lock) error` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) ResolveLock(ctx context.Context, lock *txnlock.Lock) error` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) ResolvePessimisticLock(ctx context.Context, lock *txnlock.Lock) error` | Rust:  | Tests: 
- [ ] `func (p *Spool) Run(fn func()) error` | Rust:  | Tests: 
- [ ] `func (s *KVStore) Begin(opts ...TxnOption) (txn *transaction.KVTxn, err error)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) CheckRegionInScattering(regionID uint64) (bool, error)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) CheckVisibility(startTS uint64) error` | Rust:  | Tests: 
- [ ] `func (s *KVStore) Close() error` | Rust:  | Tests: 
- [ ] `func (s *KVStore) Closed() <-chan struct{}` | Rust:  | Tests: 
- [ ] `func (s *KVStore) Ctx() context.Context` | Rust:  | Tests: 
- [ ] `func (s *KVStore) CurrentAllTSOKeyspaceGroupMinTs() (uint64, error)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) CurrentTimestamp(txnScope string) (uint64, error)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) DeleteRange( ctx context.Context, startKey []byte, endKey []byte, concurrency int, ) (completedRegions int, err error)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) EnableTxnLocalLatches(size uint)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GC(ctx context.Context, expectedSafePoint uint64, opts ...GCOpt) (newGCSafePoint uint64, err error)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetClusterID() uint64` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetLockResolver() *txnlock.LockResolver` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetMinSafeTS(txnScope string) uint64` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetOracle() oracle.Oracle` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetPDClient() pd.Client` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetPDHTTPClient() pdhttp.Client` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetRegionCache() *locate.RegionCache` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetSafePointKV() SafePointKV` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetSnapshot(ts uint64) *txnsnapshot.KVSnapshot` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetTiKVClient() (client Client)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) GetTimestampWithRetry(bo *Backoffer, scope string) (uint64, error)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) Go(f func()) error` | Rust:  | Tests: 
- [ ] `func (s *KVStore) IsClose() bool` | Rust:  | Tests: 
- [ ] `func (s *KVStore) IsLatchEnabled() bool` | Rust:  | Tests: 
- [ ] `func (s *KVStore) SendReq( bo *Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, ) (*tikvrpc.Response, error)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) SetOracle(oracle oracle.Oracle)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) SetTiKVClient(client Client)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) SplitRegions(ctx context.Context, splitKeys [][]byte, scatter bool, tableID *int64) (regionIDs []uint64, err error)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) SupportDeleteRange() (supported bool)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) TxnLatches() *latch.LatchesScheduler` | Rust:  | Tests: 
- [ ] `func (s *KVStore) UUID() string` | Rust:  | Tests: 
- [ ] `func (s *KVStore) UnsafeDestroyRange(ctx context.Context, startKey []byte, endKey []byte) error` | Rust:  | Tests: 
- [ ] `func (s *KVStore) UpdateTxnSafePointCache(txnSafePoint uint64, now time.Time)` | Rust:  | Tests: 
- [ ] `func (s *KVStore) WaitGroup() *sync.WaitGroup` | Rust:  | Tests: 
- [ ] `func (s *KVStore) WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) Begin(opts ...TxnOption) (transaction.TxnProbe, error)` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) ClearTxnLatches()` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) GCResolveLockPhase(ctx context.Context, safepoint uint64, concurrency int) error` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) GetCompatibleTxnSafePointLoaderUnderlyingEtcdClient() *clientv3.Client` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) GetGCStatesClient() pdgc.GCStatesClient` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) GetSnapshot(ts uint64) txnsnapshot.SnapshotProbe` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) LoadSafePointFromSafePointKV() (uint64, error)` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) LoadTxnSafePoint(ctx context.Context) (uint64, error)` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) NewLockResolver() LockResolverProbe` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) ReplaceGCStatesClient(c pdgc.GCStatesClient)` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) SaveSafePointToSafePointKV(v uint64) error` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) ScanLocks(ctx context.Context, startKey, endKey []byte, maxVersion uint64) ([]*txnlock.Lock, error)` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) SendTxnHeartbeat(ctx context.Context, key []byte, startTS uint64, ttl uint64) (uint64, error)` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) SetRegionCachePDClient(client pd.Client)` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) SetRegionCacheStore(id uint64, storeType tikvrpc.EndpointType, state uint64, labels []*metapb.StoreLabel)` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) SetSafeTS(storeID, safeTS uint64)` | Rust:  | Tests: 
- [ ] `func (s StoreProbe) UpdateTxnSafePointCache(txnSafePoint uint64, now time.Time)` | Rust:  | Tests: 
- [ ] `func (w *EtcdSafePointKV) Close() error` | Rust:  | Tests: 
- [ ] `func (w *EtcdSafePointKV) Get(k string) (string, error)` | Rust:  | Tests: 
- [ ] `func (w *EtcdSafePointKV) GetWithPrefix(k string) ([]*mvccpb.KeyValue, error)` | Rust:  | Tests: 
- [ ] `func (w *EtcdSafePointKV) Put(k string, v string) error` | Rust:  | Tests: 
- [ ] `func (w *MockSafePointKV) Close() error` | Rust:  | Tests: 
- [ ] `func (w *MockSafePointKV) Get(k string) (string, error)` | Rust:  | Tests: 
- [ ] `func (w *MockSafePointKV) GetWithPrefix(prefix string) ([]*mvccpb.KeyValue, error)` | Rust:  | Tests: 
- [ ] `func (w *MockSafePointKV) Put(k string, v string) error` | Rust:  | Tests: 

## tikvrpc (package tikvrpc)

### Types
- [ ] `type BatchCopStreamResponse struct` | Rust:  | Tests: 
- [ ] `type CmdType uint16` | Rust:  | Tests: 
- [ ] `type CopStreamResponse struct` | Rust:  | Tests: 
- [ ] `type EndpointType uint8` | Rust:  | Tests: 
- [ ] `type Lease struct` | Rust:  | Tests: 
- [ ] `type MPPStreamResponse struct` | Rust:  | Tests: 
- [ ] `type Request struct` | Rust:  | Tests: 
- [x] `type ResourceGroupTagger func(req *Request)` | Rust: `tikv_client::interceptor::ResourceGroupTagger` (new-client-rust/src/interceptor.rs) | Tests: `new-client-rust/src/raw/client.rs (test_request_source_and_resource_group_tag)`
- [ ] `type Response struct` | Rust:  | Tests: 
- [ ] `type ResponseExt struct` | Rust:  | Tests: 

### Functions
- [ ] `func AttachContext(req *Request, rpcCtx kvrpcpb.Context) bool` | Rust:  | Tests: 
- [ ] `func CallDebugRPC(ctx context.Context, client debugpb.DebugClient, req *Request) (*Response, error)` | Rust:  | Tests: 
- [ ] `func CallRPC(ctx context.Context, client tikvpb.TikvClient, req *Request) (*Response, error)` | Rust:  | Tests: 
- [ ] `func CheckStreamTimeoutLoop(ch <-chan *Lease, done <-chan struct{})` | Rust:  | Tests: 
- [ ] `func FromBatchCommandsResponse(res *tikvpb.BatchCommandsResponse_Response) (*Response, error)` | Rust:  | Tests: 
- [ ] `func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error)` | Rust:  | Tests: 
- [ ] `func GetStoreTypeByMeta(store *metapb.Store) EndpointType` | Rust:  | Tests: 
- [ ] `func NewReplicaReadRequest(typ CmdType, pointer interface{}, replicaReadType kv.ReplicaReadType, replicaReadSeed *uint32, ctxs ...kvrpcpb.Context) *Request` | Rust:  | Tests: 
- [ ] `func NewRequest(typ CmdType, pointer interface{}, ctxs ...kvrpcpb.Context) *Request` | Rust:  | Tests: 
- [ ] `func SetContext(req *Request, region *metapb.Region, peer *metapb.Peer) error` | Rust:  | Tests: 
- [ ] `func SetContextNoAttach(req *Request, region *metapb.Region, peer *metapb.Peer) error` | Rust:  | Tests: 

### Consts
- [ ] `CmdBatchCop` | Rust:  | Tests: 
- [ ] `CmdBatchGet` | Rust:  | Tests: 
- [ ] `CmdBatchRollback` | Rust:  | Tests: 
- [ ] `CmdBroadcastTxnStatus` | Rust:  | Tests: 
- [ ] `CmdBufferBatchGet` | Rust:  | Tests: 
- [ ] `CmdCheckLockObserver` | Rust:  | Tests: 
- [ ] `CmdCheckSecondaryLocks` | Rust:  | Tests: 
- [ ] `CmdCheckTxnStatus` | Rust:  | Tests: 
- [ ] `CmdCleanup` | Rust:  | Tests: 
- [ ] `CmdCommit` | Rust:  | Tests: 
- [ ] `CmdCompact` | Rust:  | Tests: 
- [ ] `CmdCop` | Rust:  | Tests: 
- [ ] `CmdCopStream` | Rust:  | Tests: 
- [ ] `CmdDebugGetRegionProperties` | Rust:  | Tests: 
- [ ] `CmdDeleteRange` | Rust:  | Tests: 
- [ ] `CmdEmpty` | Rust:  | Tests: 
- [ ] `CmdFlashbackToVersion` | Rust:  | Tests: 
- [ ] `CmdFlush` | Rust:  | Tests: 
- [ ] `CmdGC` | Rust:  | Tests: 
- [ ] `CmdGet` | Rust:  | Tests: 
- [ ] `CmdGetHealthFeedback` | Rust:  | Tests: 
- [ ] `CmdGetKeyTTL` | Rust:  | Tests: 
- [ ] `CmdGetTiFlashSystemTable` | Rust:  | Tests: 
- [ ] `CmdLockWaitInfo` | Rust:  | Tests: 
- [ ] `CmdMPPAlive` | Rust:  | Tests: 
- [ ] `CmdMPPCancel` | Rust:  | Tests: 
- [ ] `CmdMPPConn` | Rust:  | Tests: 
- [ ] `CmdMPPTask` | Rust:  | Tests: 
- [ ] `CmdMvccGetByKey` | Rust:  | Tests: 
- [ ] `CmdMvccGetByStartTs` | Rust:  | Tests: 
- [ ] `CmdPessimisticLock` | Rust:  | Tests: 
- [ ] `CmdPessimisticRollback` | Rust:  | Tests: 
- [ ] `CmdPhysicalScanLock` | Rust:  | Tests: 
- [ ] `CmdPrepareFlashbackToVersion` | Rust:  | Tests: 
- [ ] `CmdPrewrite` | Rust:  | Tests: 
- [ ] `CmdRawBatchDelete` | Rust:  | Tests: 
- [ ] `CmdRawBatchGet` | Rust:  | Tests: 
- [ ] `CmdRawBatchPut` | Rust:  | Tests: 
- [ ] `CmdRawChecksum` | Rust:  | Tests: 
- [ ] `CmdRawCompareAndSwap` | Rust:  | Tests: 
- [ ] `CmdRawDelete` | Rust:  | Tests: 
- [ ] `CmdRawDeleteRange` | Rust:  | Tests: 
- [ ] `CmdRawGet` | Rust:  | Tests: 
- [ ] `CmdRawGetKeyTTL` | Rust:  | Tests: 
- [ ] `CmdRawPut` | Rust:  | Tests: 
- [ ] `CmdRawScan` | Rust:  | Tests: 
- [ ] `CmdRegisterLockObserver` | Rust:  | Tests: 
- [ ] `CmdRemoveLockObserver` | Rust:  | Tests: 
- [ ] `CmdResolveLock` | Rust:  | Tests: 
- [ ] `CmdScan` | Rust:  | Tests: 
- [ ] `CmdScanLock` | Rust:  | Tests: 
- [ ] `CmdSplitRegion` | Rust:  | Tests: 
- [ ] `CmdStoreSafeTS` | Rust:  | Tests: 
- [ ] `CmdTxnHeartBeat` | Rust:  | Tests: 
- [ ] `CmdUnsafeDestroyRange` | Rust:  | Tests: 
- [ ] `EngineLabelKey` | Rust:  | Tests: 
- [ ] `EngineLabelTiFlash` | Rust:  | Tests: 
- [ ] `EngineLabelTiFlashCompute` | Rust:  | Tests: 
- [ ] `EngineRoleLabelKey` | Rust:  | Tests: 
- [ ] `EngineRoleWrite` | Rust:  | Tests: 
- [ ] `TiDB` | Rust:  | Tests: 
- [ ] `TiFlash` | Rust:  | Tests: 
- [ ] `TiFlashCompute` | Rust:  | Tests: 
- [ ] `TiKV` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- [ ] `func (req *Request) BatchCop() *coprocessor.BatchRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) BatchGet() *kvrpcpb.BatchGetRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) BatchRollback() *kvrpcpb.BatchRollbackRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) BroadcastTxnStatus() *kvrpcpb.BroadcastTxnStatusRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) BufferBatchGet() *kvrpcpb.BufferBatchGetRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) CancelMPPTask() *mpp.CancelTaskRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) CheckLockObserver() *kvrpcpb.CheckLockObserverRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) CheckSecondaryLocks() *kvrpcpb.CheckSecondaryLocksRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) CheckTxnStatus() *kvrpcpb.CheckTxnStatusRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) Cleanup() *kvrpcpb.CleanupRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) Commit() *kvrpcpb.CommitRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) Compact() *kvrpcpb.CompactRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) Cop() *coprocessor.Request` | Rust:  | Tests: 
- [ ] `func (req *Request) DebugGetRegionProperties() *debugpb.GetRegionPropertiesRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) DeleteRange() *kvrpcpb.DeleteRangeRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) DisableStaleReadMeetLock()` | Rust:  | Tests: 
- [ ] `func (req *Request) DispatchMPPTask() *mpp.DispatchTaskRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) Empty() *tikvpb.BatchCommandsEmptyRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) EnableStaleWithMixedReplicaRead()` | Rust:  | Tests: 
- [ ] `func (req *Request) EstablishMPPConn() *mpp.EstablishMPPConnectionRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) FlashbackToVersion() *kvrpcpb.FlashbackToVersionRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) Flush() *kvrpcpb.FlushRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) GC() *kvrpcpb.GCRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) Get() *kvrpcpb.GetRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) GetHealthFeedback() *kvrpcpb.GetHealthFeedbackRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) GetReplicaReadSeed() *uint32` | Rust:  | Tests: 
- [ ] `func (req *Request) GetSize() int` | Rust:  | Tests: 
- [ ] `func (req *Request) GetStartTS() uint64` | Rust:  | Tests: 
- [ ] `func (req *Request) GetTiFlashSystemTable() *kvrpcpb.TiFlashSystemTableRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) IsDebugReq() bool` | Rust:  | Tests: 
- [ ] `func (req *Request) IsGlobalStaleRead() bool` | Rust:  | Tests: 
- [ ] `func (req *Request) IsGreenGCRequest() bool` | Rust:  | Tests: 
- [ ] `func (req *Request) IsInterruptible() bool` | Rust:  | Tests: 
- [ ] `func (req *Request) IsMPPAlive() *mpp.IsAliveRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) IsRawWriteRequest() bool` | Rust:  | Tests: 
- [ ] `func (req *Request) IsTxnWriteRequest() bool` | Rust:  | Tests: 
- [ ] `func (req *Request) LockWaitInfo() *kvrpcpb.GetLockWaitInfoRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) MvccGetByKey() *kvrpcpb.MvccGetByKeyRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) MvccGetByStartTs() *kvrpcpb.MvccGetByStartTsRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) PessimisticLock() *kvrpcpb.PessimisticLockRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) PessimisticRollback() *kvrpcpb.PessimisticRollbackRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) PhysicalScanLock() *kvrpcpb.PhysicalScanLockRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) PrepareFlashbackToVersion() *kvrpcpb.PrepareFlashbackToVersionRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) Prewrite() *kvrpcpb.PrewriteRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawBatchDelete() *kvrpcpb.RawBatchDeleteRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawBatchGet() *kvrpcpb.RawBatchGetRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawBatchPut() *kvrpcpb.RawBatchPutRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawChecksum() *kvrpcpb.RawChecksumRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawCompareAndSwap() *kvrpcpb.RawCASRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawDelete() *kvrpcpb.RawDeleteRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawDeleteRange() *kvrpcpb.RawDeleteRangeRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawGet() *kvrpcpb.RawGetRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawGetKeyTTL() *kvrpcpb.RawGetKeyTTLRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawPut() *kvrpcpb.RawPutRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RawScan() *kvrpcpb.RawScanRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RegisterLockObserver() *kvrpcpb.RegisterLockObserverRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) RemoveLockObserver() *kvrpcpb.RemoveLockObserverRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) ResolveLock() *kvrpcpb.ResolveLockRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) Scan() *kvrpcpb.ScanRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) ScanLock() *kvrpcpb.ScanLockRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) SetReplicaReadType(replicaReadType kv.ReplicaReadType)` | Rust:  | Tests: 
- [ ] `func (req *Request) SplitRegion() *kvrpcpb.SplitRegionRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) StoreSafeTS() *kvrpcpb.StoreSafeTSRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) ToBatchCommandsRequest() *tikvpb.BatchCommandsRequest_Request` | Rust:  | Tests: 
- [ ] `func (req *Request) TxnHeartBeat() *kvrpcpb.TxnHeartBeatRequest` | Rust:  | Tests: 
- [ ] `func (req *Request) UnsafeDestroyRange() *kvrpcpb.UnsafeDestroyRangeRequest` | Rust:  | Tests: 
- [ ] `func (resp *BatchCopStreamResponse) Close()` | Rust:  | Tests: 
- [ ] `func (resp *BatchCopStreamResponse) Recv() (*coprocessor.BatchResponse, error)` | Rust:  | Tests: 
- [ ] `func (resp *CopStreamResponse) Close()` | Rust:  | Tests: 
- [ ] `func (resp *CopStreamResponse) Recv() (*coprocessor.Response, error)` | Rust:  | Tests: 
- [ ] `func (resp *MPPStreamResponse) Close()` | Rust:  | Tests: 
- [ ] `func (resp *MPPStreamResponse) Recv() (*mpp.MPPDataPacket, error)` | Rust:  | Tests: 
- [ ] `func (resp *Response) GetExecDetailsV2() *kvrpcpb.ExecDetailsV2` | Rust:  | Tests: 
- [ ] `func (resp *Response) GetRegionError() (*errorpb.Error, error)` | Rust:  | Tests: 
- [ ] `func (resp *Response) GetSize() int` | Rust:  | Tests: 
- [ ] `func (t CmdType) String() string` | Rust:  | Tests: 
- [ ] `func (t EndpointType) IsTiFlashRelatedType() bool` | Rust:  | Tests: 
- [ ] `func (t EndpointType) Name() string` | Rust:  | Tests: 

## tikvrpc/interceptor (package interceptor)

### Types
- [ ] `type MockInterceptorManager struct` | Rust:  | Tests: 
- [x] `type RPCInterceptor interface` | Rust: `tikv_client::interceptor::RpcInterceptor` (new-client-rust/src/interceptor.rs) | Tests: `new-client-rust/src/raw/client.rs (test_request_source_and_resource_group_tag)`
- [x] `type RPCInterceptorChain struct` | Rust: `tikv_client::interceptor::RpcInterceptorChain` (new-client-rust/src/interceptor.rs) | Tests: N/A
- [ ] `type RPCInterceptorFunc func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error)` | Rust:  | Tests: 

### Functions
- [ ] `func ChainRPCInterceptors(first RPCInterceptor, rest ...RPCInterceptor) RPCInterceptor` | Rust:  | Tests: 
- [ ] `func GetRPCInterceptorFromCtx(ctx context.Context) RPCInterceptor` | Rust:  | Tests: 
- [ ] `func NewMockInterceptorManager() *MockInterceptorManager` | Rust:  | Tests: 
- [x] `func NewRPCInterceptor(name string, fn func(next RPCInterceptorFunc) RPCInterceptorFunc) RPCInterceptor` | Rust: `tikv_client::interceptor::rpc_interceptor` (new-client-rust/src/interceptor.rs) | Tests: N/A
- [x] `func NewRPCInterceptorChain() *RPCInterceptorChain` | Rust: `RpcInterceptorChain::new` (new-client-rust/src/interceptor.rs) | Tests: N/A
- [x] `func WithRPCInterceptor(ctx context.Context, interceptor RPCInterceptor) context.Context` | Rust: N/A (out-of-scope: configure on client via `with_rpc_interceptor`) | Tests: N/A

### Consts
- (none)

### Vars
- (none)

### Methods
- [x] `func (c *RPCInterceptorChain) Len() int` | Rust: `RpcInterceptorChain::len` (new-client-rust/src/interceptor.rs) | Tests: N/A
- [x] `func (c *RPCInterceptorChain) Link(it RPCInterceptor) *RPCInterceptorChain` | Rust: `RpcInterceptorChain::link` (new-client-rust/src/interceptor.rs) | Tests: N/A
- [ ] `func (c *RPCInterceptorChain) Name() string` | Rust:  | Tests: 
- [ ] `func (c *RPCInterceptorChain) Wrap(next RPCInterceptorFunc) RPCInterceptorFunc` | Rust:  | Tests: 
- [ ] `func (m *MockInterceptorManager) BeginCount() int` | Rust:  | Tests: 
- [ ] `func (m *MockInterceptorManager) CreateMockInterceptor(name string) RPCInterceptor` | Rust:  | Tests: 
- [ ] `func (m *MockInterceptorManager) EndCount() int` | Rust:  | Tests: 
- [ ] `func (m *MockInterceptorManager) ExecLog() []string` | Rust:  | Tests: 
- [ ] `func (m *MockInterceptorManager) Reset()` | Rust:  | Tests: 

## trace (package trace)

### Types
- [ ] `type Category uint32` | Rust:  | Tests: 
- [ ] `type IsCategoryEnabledFunc func(category Category) bool` | Rust:  | Tests: 
- [ ] `type TraceControlExtractorFunc func(ctx context.Context) TraceControlFlags` | Rust:  | Tests: 
- [ ] `type TraceControlFlags uint64` | Rust:  | Tests: 
- [ ] `type TraceEventFunc func(ctx context.Context, category Category, name string, fields ...zap.Field)` | Rust:  | Tests: 

### Functions
- [ ] `func ContextWithTraceID(ctx context.Context, traceID []byte) context.Context` | Rust:  | Tests: 
- [ ] `func GetTraceControlFlags(ctx context.Context) TraceControlFlags` | Rust:  | Tests: 
- [ ] `func ImmediateLoggingEnabled(ctx context.Context) bool` | Rust:  | Tests: 
- [ ] `func IsCategoryEnabled(category Category) bool` | Rust:  | Tests: 
- [ ] `func SetIsCategoryEnabledFunc(fn IsCategoryEnabledFunc)` | Rust:  | Tests: 
- [ ] `func SetTraceControlExtractor(fn TraceControlExtractorFunc)` | Rust:  | Tests: 
- [ ] `func SetTraceEventFunc(fn TraceEventFunc)` | Rust:  | Tests: 
- [ ] `func TraceEvent(ctx context.Context, category Category, name string, fields ...zap.Field)` | Rust:  | Tests: 
- [ ] `func TraceIDFromContext(ctx context.Context) []byte` | Rust:  | Tests: 

### Consts
- [ ] `CategoryKVRequest` | Rust:  | Tests: 
- [ ] `CategoryRegionCache` | Rust:  | Tests: 
- [ ] `CategoryTxn2PC` | Rust:  | Tests: 
- [ ] `CategoryTxnLockResolve` | Rust:  | Tests: 
- [ ] `FlagImmediateLog` | Rust:  | Tests: 
- [ ] `FlagTiKVCategoryReadDetails` | Rust:  | Tests: 
- [ ] `FlagTiKVCategoryRequest` | Rust:  | Tests: 
- [ ] `FlagTiKVCategoryWriteDetails` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- [ ] `func (f TraceControlFlags) Has(flag TraceControlFlags) bool` | Rust:  | Tests: 
- [ ] `func (f TraceControlFlags) With(flag TraceControlFlags) TraceControlFlags` | Rust:  | Tests: 

## txnkv (package txnkv)

### Types
- [ ] `type BinlogWriteResult = transaction.BinlogWriteResult` | Rust:  | Tests: 
- [ ] `type Client struct` | Rust:  | Tests: 
- [ ] `type ClientOpt func(*option)` | Rust:  | Tests: 
- [ ] `type IsoLevel = txnsnapshot.IsoLevel` | Rust:  | Tests: 
- [ ] `type KVFilter = transaction.KVFilter` | Rust:  | Tests: 
- [ ] `type KVSnapshot = txnsnapshot.KVSnapshot` | Rust:  | Tests: 
- [ ] `type KVTxn = transaction.KVTxn` | Rust:  | Tests: 
- [ ] `type Lock = txnlock.Lock` | Rust:  | Tests: 
- [ ] `type LockResolver = txnlock.LockResolver` | Rust:  | Tests: 
- [ ] `type Priority = txnutil.Priority` | Rust:  | Tests: 
- [ ] `type ReplicaReadAdjuster = txnsnapshot.ReplicaReadAdjuster` | Rust:  | Tests: 
- [ ] `type Scanner = txnsnapshot.Scanner` | Rust:  | Tests: 
- [ ] `type SchemaLeaseChecker = transaction.SchemaLeaseChecker` | Rust:  | Tests: 
- [ ] `type SchemaVer = transaction.SchemaVer` | Rust:  | Tests: 
- [ ] `type SnapshotRuntimeStats = txnsnapshot.SnapshotRuntimeStats` | Rust:  | Tests: 
- [ ] `type TxnStatus = txnlock.TxnStatus` | Rust:  | Tests: 

### Functions
- [ ] `func NewClient(pdAddrs []string, opts ...ClientOpt) (*Client, error)` | Rust:  | Tests: 
- [ ] `func NewLock(l *kvrpcpb.LockInfo) *Lock` | Rust:  | Tests: 
- [ ] `func WithAPIVersion(apiVersion kvrpcpb.APIVersion) ClientOpt` | Rust:  | Tests: 
- [ ] `func WithKeyspace(keyspaceName string) ClientOpt` | Rust:  | Tests: 
- [ ] `func WithSafePointKVPrefix(prefix string) ClientOpt` | Rust:  | Tests: 

### Consts
- [ ] `MaxTxnTimeUse` | Rust:  | Tests: 
- [ ] `PriorityHigh` | Rust:  | Tests: 
- [ ] `PriorityLow` | Rust:  | Tests: 
- [ ] `PriorityNormal` | Rust:  | Tests: 
- [ ] `RC` | Rust:  | Tests: 
- [ ] `RCCheckTS` | Rust:  | Tests: 
- [ ] `SI` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- [ ] `func (c *Client) GetTimestamp(ctx context.Context) (uint64, error)` | Rust:  | Tests: 

## txnkv/rangetask (package rangetask)

### Types
- [ ] `type DeleteRangeTask struct` | Rust:  | Tests: 
- [ ] `type Runner struct` | Rust:  | Tests: 
- [ ] `type TaskHandler = func(ctx context.Context, r kv.KeyRange) (TaskStat, error)` | Rust:  | Tests: 
- [ ] `type TaskStat struct` | Rust:  | Tests: 

### Functions
- [ ] `func NewDeleteRangeTask(store storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask` | Rust:  | Tests: 
- [ ] `func NewLocateRegionBackoffer(ctx context.Context) *retry.Backoffer` | Rust:  | Tests: 
- [ ] `func NewNotifyDeleteRangeTask(store storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask` | Rust:  | Tests: 
- [ ] `func NewRangeTaskRunner( name string, store storage, concurrency int, handler TaskHandler, ) *Runner` | Rust:  | Tests: 
- [ ] `func NewRangeTaskRunnerWithID( name string, identifier string, store storage, concurrency int, handler TaskHandler, ) *Runner` | Rust:  | Tests: 

### Consts
- (none)

### Vars
- (none)

### Methods
- [ ] `func (s *Runner) CompletedRegions() int` | Rust:  | Tests: 
- [ ] `func (s *Runner) FailedRegions() int` | Rust:  | Tests: 
- [ ] `func (s *Runner) RunOnRange(ctx context.Context, startKey, endKey []byte) error` | Rust:  | Tests: 
- [ ] `func (s *Runner) SetRegionsPerTask(regionsPerTask int)` | Rust:  | Tests: 
- [ ] `func (s *Runner) SetStatLogInterval(interval time.Duration)` | Rust:  | Tests: 
- [ ] `func (t *DeleteRangeTask) CompletedRegions() int` | Rust:  | Tests: 
- [ ] `func (t *DeleteRangeTask) Execute(ctx context.Context) error` | Rust:  | Tests: 

## txnkv/transaction (package transaction)

### Types
- [ ] `type AggressiveLockedKeyInfo struct` | Rust:  | Tests: 
- [ ] `type BatchBufferGetter interface` | Rust:  | Tests: 
- [ ] `type BatchSnapshotBufferGetter interface` | Rust:  | Tests: 
- [ ] `type BinlogExecutor interface` | Rust:  | Tests: 
- [ ] `type BinlogWriteResult interface` | Rust:  | Tests: 
- [ ] `type BufferBatchGetter struct` | Rust:  | Tests: 
- [ ] `type BufferSnapshotBatchGetter struct` | Rust:  | Tests: 
- [ ] `type CommitterMutationFlags uint8` | Rust:  | Tests: 
- [ ] `type CommitterMutations interface` | Rust:  | Tests: 
- [ ] `type CommitterProbe struct` | Rust:  | Tests: 
- [ ] `type ConfigProbe struct` | Rust:  | Tests: 
- [ ] `type KVFilter interface` | Rust:  | Tests: 
- [ ] `type KVTxn struct` | Rust:  | Tests: 
- [ ] `type LifecycleHooks struct` | Rust:  | Tests: 
- [ ] `type MemBufferMutationsProbe struct` | Rust:  | Tests: 
- [x] `type PipelinedTxnOptions struct` | Rust: `new-client-rust/src/transaction/transaction.rs (PipelinedTxnOptions)` | Tests: `new-client-rust/src/transaction/transaction.rs (pipelined tests)`
- [ ] `type PlainMutation struct` | Rust:  | Tests: 
- [ ] `type PlainMutations struct` | Rust:  | Tests: 
- [ ] `type PrewriteEncounterLockPolicy int` | Rust:  | Tests: 
- [ ] `type RelatedSchemaChange struct` | Rust:  | Tests: 
- [ ] `type SchemaLeaseChecker interface` | Rust:  | Tests: 
- [ ] `type SchemaVer interface` | Rust:  | Tests: 
- [ ] `type TxnInfo struct` | Rust:  | Tests: 
- [ ] `type TxnOptions struct` | Rust:  | Tests: 
- [ ] `type TxnProbe struct` | Rust:  | Tests: 
- [ ] `type WriteAccessLevel int` | Rust:  | Tests: 

### Functions
- [ ] `func NewBufferBatchGetter(buffer BatchBufferGetter, snapshot kv.BatchGetter) *BufferBatchGetter` | Rust:  | Tests: 
- [ ] `func NewBufferSnapshotBatchGetter(buffer BatchSnapshotBufferGetter, snapshot kv.BatchGetter) *BufferSnapshotBatchGetter` | Rust:  | Tests: 
- [ ] `func NewMemBufferMutationsProbe(sizeHint int, storage *unionstore.MemDB) MemBufferMutationsProbe` | Rust:  | Tests: 
- [ ] `func NewPlainMutations(sizeHint int) PlainMutations` | Rust:  | Tests: 
- [ ] `func NewTiKVTxn(store kvstore, snapshot *txnsnapshot.KVSnapshot, startTS uint64, options *TxnOptions) (*KVTxn, error)` | Rust:  | Tests: 
- [ ] `func SendTxnHeartBeat(bo *retry.Backoffer, store kvstore, primary []byte, startTS, ttl uint64) (newTTL uint64, stopHeartBeat bool, err error)` | Rust:  | Tests: 

### Consts
- [ ] `CommitSecondaryMaxBackoff` | Rust:  | Tests: 
- [ ] `MaxExecTimeExceededSignal` | Rust:  | Tests: 
- [ ] `MaxTxnTimeUse` | Rust:  | Tests: 
- [ ] `MutationFlagIsAssertExists` | Rust:  | Tests: 
- [ ] `MutationFlagIsAssertNotExists` | Rust:  | Tests: 
- [ ] `MutationFlagIsPessimisticLock` | Rust:  | Tests: 
- [ ] `MutationFlagNeedConstraintCheckInPrewrite` | Rust:  | Tests: 
- [ ] `NoResolvePolicy` | Rust:  | Tests: 
- [x] `PipelinedRequestSource` | Rust: `new-client-rust/src/transaction/pipelined.rs (PIPELINED_REQUEST_SOURCE)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_pipelined_flush_commit_and_resolve_locks)`
- [ ] `TryResolvePolicy` | Rust:  | Tests: 
- [ ] `TsoMaxBackoff` | Rust:  | Tests: 

### Vars
- [ ] `CommitMaxBackoff` | Rust:  | Tests: 
- [ ] `CtxInGetTimestampForCommitKey` | Rust:  | Tests: 
- [ ] `ManagedLockTTL` | Rust:  | Tests: 
- [x] `MaxPipelinedTxnTTL` | Rust: `new-client-rust/src/transaction/pipelined.rs (PIPELINED_LOCK_TTL_MS)` | Tests: `new-client-rust/src/transaction/transaction.rs (pipelined tests)`
- [ ] `PrewriteMaxBackoff` | Rust:  | Tests: 
- [ ] `SetSuccess` | Rust:  | Tests: 

### Methods
- [ ] `func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error)` | Rust:  | Tests: 
- [ ] `func (b *BufferSnapshotBatchGetter) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error)` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) AppendMutation(mutation PlainMutation)` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) GetFlags() []CommitterMutationFlags` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) GetKey(i int) []byte` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) GetKeys() [][]byte` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) GetOp(i int) kvrpcpb.Op` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) GetOps() []kvrpcpb.Op` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) GetValue(i int) []byte` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) GetValues() [][]byte` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) IsAssertExists(i int) bool` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) IsAssertNotExist(i int) bool` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) IsPessimisticLock(i int) bool` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) Len() int` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) MergeMutations(mutations PlainMutations)` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) NeedConstraintCheckInPrewrite(i int) bool` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) Push(op kvrpcpb.Op, key []byte, value []byte, isPessimisticLock, assertExist, assertNotExist, NeedConstraintCheckInPrewrite bool)` | Rust:  | Tests: 
- [ ] `func (c *PlainMutations) Slice(from, to int) CommitterMutations` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) BuildPrewriteRequest(regionID, regionConf, regionVersion uint64, mutations CommitterMutations, txnSize uint64) *tikvrpc.Request` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) CheckAsyncCommit() bool` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) Cleanup(ctx context.Context)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) CleanupMutations(ctx context.Context) error` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) CleanupWithoutWait(ctx context.Context)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) CloseTTLManager()` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) CommitMutations(ctx context.Context) error` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) Execute(ctx context.Context) error` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) GetCommitTS() uint64` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) GetForUpdateTS() uint64` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) GetLockTTL() uint64` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) GetMinCommitTS() uint64` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) GetMutations() CommitterMutations` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) GetOnePCCommitTS() uint64` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) GetPrimaryKey() []byte` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) GetStartTS() uint64` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) GetUndeterminedErr() error` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) InitKeysAndMutations() error` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) IsAsyncCommit() bool` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) IsNil() bool` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) IsOnePC() bool` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) IsTTLRunning() bool` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) IsTTLUninitialized() bool` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) MutationsOfKeys(keys [][]byte) CommitterMutations` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) PessimisticRollbackMutations(ctx context.Context, muts CommitterMutations) error` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) PrewriteAllMutations(ctx context.Context) error` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) PrewriteMutations(ctx context.Context, mutations CommitterMutations) error` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) ResolveFlushedLocks(bo *retry.Backoffer, start, end []byte, commit bool)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetCommitTS(ts uint64)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetForUpdateTS(ts uint64)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetLockTTL(ttl uint64)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetLockTTLByTimeAndSize(start time.Time, size int)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetMaxCommitTS(ts uint64)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetMinCommitTS(ts uint64)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetMutations(muts CommitterMutations)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetNoFallBack()` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetPrimaryKey(key []byte)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetPrimaryKeyBlocker(ac, bk chan struct{})` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetSessionID(id uint64)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetTxnSize(sz int)` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) SetUseAsyncCommit()` | Rust:  | Tests: 
- [ ] `func (c CommitterProbe) WaitCleanup()` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetDefaultLockTTL() uint64` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetPessimisticLockMaxBackoff() int` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetTTLFactor() int` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetTxnCommitBatchSize() uint64` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) LoadPreSplitDetectThreshold() uint32` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) LoadPreSplitSizeThreshold() uint32` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) StorePreSplitDetectThreshold(v uint32)` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) StorePreSplitSizeThreshold(v uint32)` | Rust:  | Tests: 
- [ ] `func (i *AggressiveLockedKeyInfo) ActualLockForUpdateTS() uint64` | Rust:  | Tests: 
- [ ] `func (i *AggressiveLockedKeyInfo) HasCheckExistence() bool` | Rust:  | Tests: 
- [ ] `func (i *AggressiveLockedKeyInfo) HasReturnValues() bool` | Rust:  | Tests: 
- [ ] `func (i *AggressiveLockedKeyInfo) Key() []byte` | Rust:  | Tests: 
- [ ] `func (i *AggressiveLockedKeyInfo) Value() kv.ReturnedValue` | Rust:  | Tests: 
- [ ] `func (p PrewriteEncounterLockPolicy) String() string` | Rust:  | Tests: 
- [x] `func (txn *KVTxn) AddRPCInterceptor(it interceptor.RPCInterceptor)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::add_rpc_interceptor)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_rpc_interceptor_can_override_priority_penalty_and_tag)`
- [ ] `func (txn *KVTxn) BatchGet(ctx context.Context, keys [][]byte, options ...tikv.BatchGetOption) (map[string]tikv.ValueEntry, error)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) CancelAggressiveLocking(ctx context.Context)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) ClearDiskFullOpt()` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Commit(ctx context.Context) error` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) CommitTS() uint64` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Delete(k []byte) error` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) DoneAggressiveLocking(ctx context.Context)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) EnableForceSyncLog()` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Get(ctx context.Context, k []byte, options ...tikv.GetOption) (tikv.ValueEntry, error)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetClusterID() uint64` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetCommitWaitUntilTSO() uint64` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetCommitWaitUntilTSOTimeout() time.Duration` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetDiskFullOpt() kvrpcpb.DiskFullOpt` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetMemBuffer() unionstore.MemBuffer` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetScope() string` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetSnapshot() *txnsnapshot.KVSnapshot` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetTimestampForCommit(bo *retry.Backoffer, scope string) (_ uint64, err error)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetUnionStore() *unionstore.KVUnionStore` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) GetVars() *tikv.Variables` | Rust:  | Tests: 
- [x] `func (txn *KVTxn) InitPipelinedMemDB() error` | Rust: `new-client-rust/src/transaction/transaction.rs (TransactionOptions::use_pipelined_txn)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_pipelined_flush_commit_and_resolve_locks)`
- [ ] `func (txn *KVTxn) IsCasualConsistency() bool` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) IsInAggressiveLockingMode() bool` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) IsInAggressiveLockingStage(key []byte) bool` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) IsPessimistic() bool` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) IsPipelined() bool` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) IsReadOnly() bool` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) IterReverse(k, lowerBound []byte) (unionstore.Iterator, error)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Len() int` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) LockKeys(ctx context.Context, lockCtx *tikv.LockCtx, keysInput ...[]byte) error` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) LockKeysFunc(ctx context.Context, lockCtx *tikv.LockCtx, fn func(), keysInput ...[]byte) error` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) LockKeysWithWaitTime(ctx context.Context, lockWaitTime int64, keysInput ...[]byte) (err error)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Mem() uint64` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) MemHookSet() bool` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) RetryAggressiveLocking(ctx context.Context)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Rollback() error` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Set(k []byte, v []byte) error` | Rust:  | Tests: 
- [x] `func (txn *KVTxn) SetAssertionLevel(assertionLevel kvrpcpb.AssertionLevel)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::set_assertion_level, TransactionOptions::assertion_level)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_prewrite_propagates_assertion_level_and_mutation_assertions)`
- [ ] `func (txn *KVTxn) SetBackgroundGoroutineLifecycleHooks(hooks LifecycleHooks)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetBinlogExecutor(binlog BinlogExecutor)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetCausalConsistency(b bool)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetCommitCallback(f func(string, error))` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetCommitTSUpperBoundCheck(f func(commitTS uint64) bool)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetCommitWaitUntilTSO(commitWaitUntilTSO uint64)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetCommitWaitUntilTSOTimeout(val time.Duration)` | Rust:  | Tests: 
- [x] `func (txn *KVTxn) SetDiskFullOpt(level kvrpcpb.DiskFullOpt)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::set_disk_full_opt)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [ ] `func (txn *KVTxn) SetEnable1PC(b bool)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetEnableAsyncCommit(b bool)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetExplicitRequestSourceType(tp string)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetKVFilter(filter KVFilter)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetMemoryFootprintChangeHook(hook func(uint64))` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetPessimistic(b bool)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetPrewriteEncounterLockPolicy(policy PrewriteEncounterLockPolicy)` | Rust:  | Tests: 
- [x] `func (txn *KVTxn) SetPriority(pri txnutil.Priority)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::set_priority)` | Tests: `new-client-rust/src/transaction/transaction.rs (request_context tests)`
- [x] `func (txn *KVTxn) SetRPCInterceptor(it interceptor.RPCInterceptor)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::set_rpc_interceptor)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_rpc_interceptor_can_override_priority_penalty_and_tag)`
- [ ] `func (txn *KVTxn) SetRequestSourceInternal(internal bool)` | Rust:  | Tests: 
- [x] `func (txn *KVTxn) SetRequestSourceType(tp string)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::set_request_source)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (txn *KVTxn) SetResourceGroupName(name string)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::set_resource_group_name)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (txn *KVTxn) SetResourceGroupTag(tag []byte)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::set_resource_group_tag)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (txn *KVTxn) SetResourceGroupTagger(tagger tikvrpc.ResourceGroupTagger)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::set_resource_group_tagger)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_fixed_resource_group_tag_takes_precedence_over_tagger)`
- [ ] `func (txn *KVTxn) SetSchemaLeaseChecker(checker SchemaLeaseChecker)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetSchemaVer(schemaVer SchemaVer)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetScope(scope string)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) SetSessionID(sessionID uint64)` | Rust:  | Tests: 
- [x] `func (txn *KVTxn) SetTxnSource(txnSource uint64)` | Rust: `new-client-rust/src/transaction/transaction.rs (Transaction::set_txn_source)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [ ] `func (txn *KVTxn) SetVars(vars *tikv.Variables)` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Size() int` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) StartAggressiveLocking()` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) StartTS() uint64` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) String() string` | Rust:  | Tests: 
- [ ] `func (txn *KVTxn) Valid() bool` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) BatchGetSingleRegion(bo *retry.Backoffer, region locate.RegionVerID, keys [][]byte, collect func([]byte, []byte)) error` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) CollectLockedKeys() [][]byte` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) GetAggressiveLockingKeys() []string` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) GetAggressiveLockingKeysInfo() []AggressiveLockedKeyInfo` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) GetAggressiveLockingPreviousKeys() []string` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) GetAggressiveLockingPreviousKeysInfo() []AggressiveLockedKeyInfo` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) GetCommitter() CommitterProbe` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) GetLockedCount() int` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) GetStartTime() time.Time` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) GetUnionStore() *unionstore.KVUnionStore` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) IsAsyncCommit() bool` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) NewCommitter(sessionID uint64) (CommitterProbe, error)` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) NewScanner(start, end []byte, batchSize int, reverse bool) (*txnsnapshot.Scanner, error)` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) SetCommitter(committer CommitterProbe)` | Rust:  | Tests: 
- [ ] `func (txn TxnProbe) SetStartTS(ts uint64)` | Rust:  | Tests: 

## txnkv/txnlock (package txnlock)

### Types
- [ ] `type Lock struct` | Rust:  | Tests: 
- [ ] `type LockProbe struct` | Rust:  | Tests: 
- [ ] `type LockResolver struct` | Rust:  | Tests: 
- [ ] `type LockResolverProbe struct` | Rust:  | Tests: 
- [ ] `type ResolveLockResult struct` | Rust:  | Tests: 
- [ ] `type ResolveLocksOptions struct` | Rust:  | Tests: 
- [ ] `type ResolvingLock struct` | Rust:  | Tests: 
- [ ] `type TxnStatus struct` | Rust:  | Tests: 

### Functions
- [ ] `func ExtractLockFromKeyErr(keyErr *kvrpcpb.KeyError) (*Lock, error)` | Rust:  | Tests: 
- [ ] `func NewLock(l *kvrpcpb.LockInfo) *Lock` | Rust:  | Tests: 
- [ ] `func NewLockResolver(store storage) *LockResolver` | Rust:  | Tests: 

### Consts
- [ ] `ResolvedCacheSize` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- [ ] `func (l *Lock) String() string` | Rust:  | Tests: 
- [ ] `func (l LockProbe) GetPrimaryKeyFromTxnStatus(s TxnStatus) []byte` | Rust:  | Tests: 
- [ ] `func (l LockProbe) NewLockStatus(keys [][]byte, useAsyncCommit bool, minCommitTS uint64) TxnStatus` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) CheckAllSecondaries(bo *retry.Backoffer, lock *Lock, status *TxnStatus) error` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) GetSecondariesFromTxnStatus(status TxnStatus) [][]byte` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) GetTxnStatus(bo *retry.Backoffer, txnID uint64, primary []byte, callerStartTS, currentTS uint64, rollbackIfNotExist bool, forceSyncCommit bool, lockInfo *Lock) (TxnStatus, error)` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) GetTxnStatusFromLock(bo *retry.Backoffer, lock *Lock, callerStartTS uint64, forceSyncCommit bool) (TxnStatus, error)` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) IsErrorNotFound(err error) bool` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) IsNonAsyncCommitLock(err error) bool` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) ResolveAsyncCommitLock(bo *retry.Backoffer, lock *Lock, status TxnStatus) error` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) ResolveLock(bo *retry.Backoffer, lock *Lock) error` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) ResolvePessimisticLock(bo *retry.Backoffer, lock *Lock) error` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) SetMeetLockCallback(f func([]*Lock))` | Rust:  | Tests: 
- [ ] `func (l LockResolverProbe) SetResolving(currentStartTS uint64, locks []Lock)` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) BatchResolveLocks(bo *retry.Backoffer, locks []*Lock, loc locate.RegionVerID) (bool, error)` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) Close()` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) GetTxnStatus(txnID uint64, callerStartTS uint64, primary []byte) (TxnStatus, error)` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) RecordResolvingLocks(locks []*Lock, callerStartTS uint64) int` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) ResolveLocks(bo *retry.Backoffer, callerStartTS uint64, locks []*Lock) (int64, error)` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) ResolveLocksDone(callerStartTS uint64, token int)` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) ResolveLocksForRead(bo *retry.Backoffer, callerStartTS uint64, locks []*Lock, lite bool) (int64, []uint64, []uint64, error)` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) ResolveLocksWithOpts(bo *retry.Backoffer, opts ResolveLocksOptions) (ResolveLockResult, error)` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) Resolving() []ResolvingLock` | Rust:  | Tests: 
- [ ] `func (lr *LockResolver) UpdateResolvingLocks(locks []*Lock, callerStartTS uint64, token int)` | Rust:  | Tests: 
- [ ] `func (s TxnStatus) Action() kvrpcpb.Action` | Rust:  | Tests: 
- [ ] `func (s TxnStatus) CommitTS() uint64` | Rust:  | Tests: 
- [ ] `func (s TxnStatus) HasSameDeterminedStatus(other TxnStatus) bool` | Rust:  | Tests: 
- [ ] `func (s TxnStatus) IsCommitted() bool` | Rust:  | Tests: 
- [ ] `func (s TxnStatus) IsRolledBack() bool` | Rust:  | Tests: 
- [ ] `func (s TxnStatus) IsStatusDetermined() bool` | Rust:  | Tests: 
- [ ] `func (s TxnStatus) StatusCacheable() bool` | Rust:  | Tests: 
- [ ] `func (s TxnStatus) String() string` | Rust:  | Tests: 
- [ ] `func (s TxnStatus) TTL() uint64` | Rust:  | Tests: 

## txnkv/txnsnapshot (package txnsnapshot)

### Types
- [ ] `type ClientHelper struct` | Rust:  | Tests: 
- [ ] `type ConfigProbe struct` | Rust:  | Tests: 
- [ ] `type IsoLevel kvrpcpb.IsolationLevel` | Rust:  | Tests: 
- [ ] `type KVSnapshot struct` | Rust:  | Tests: 
- [ ] `type ReplicaReadAdjuster func(int) (locate.StoreSelectorOption, kv.ReplicaReadType)` | Rust:  | Tests: 
- [ ] `type Scanner struct` | Rust:  | Tests: 
- [ ] `type SnapshotProbe struct` | Rust:  | Tests: 
- [ ] `type SnapshotRuntimeStats struct` | Rust:  | Tests: 

### Functions
- [ ] `func NewClientHelper(store kvstore, resolvedLocks *util.TSSet, committedLocks *util.TSSet, resolveLite bool) *ClientHelper` | Rust:  | Tests: 
- [ ] `func NewTiKVSnapshot(store kvstore, ts uint64, replicaReadSeed uint32) *KVSnapshot` | Rust:  | Tests: 

### Consts
- [ ] `BatchGetBufferTier` | Rust:  | Tests: 
- [ ] `BatchGetSnapshotTier` | Rust:  | Tests: 
- [ ] `DefaultScanBatchSize` | Rust:  | Tests: 
- [ ] `RC` | Rust:  | Tests: 
- [ ] `RCCheckTS` | Rust:  | Tests: 
- [ ] `SI` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- [ ] `func (c ConfigProbe) GetGetMaxBackoff() int` | Rust:  | Tests: 
- [ ] `func (c ConfigProbe) GetScanBatchSize() int` | Rust:  | Tests: 
- [ ] `func (ch *ClientHelper) RecordResolvingLocks(locks []*txnlock.Lock, callerStartTS uint64) int` | Rust:  | Tests: 
- [ ] `func (ch *ClientHelper) ResolveLocks(bo *retry.Backoffer, callerStartTS uint64, locks []*txnlock.Lock) (int64, error)` | Rust:  | Tests: 
- [ ] `func (ch *ClientHelper) ResolveLocksDone(callerStartTS uint64, token int)` | Rust:  | Tests: 
- [ ] `func (ch *ClientHelper) ResolveLocksWithOpts(bo *retry.Backoffer, opts txnlock.ResolveLocksOptions) (txnlock.ResolveLockResult, error)` | Rust:  | Tests: 
- [ ] `func (ch *ClientHelper) SendReqAsync( bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, cb async.Callback[*tikvrpc.ResponseExt], opts ...locate.StoreSelectorOption, )` | Rust:  | Tests: 
- [ ] `func (ch *ClientHelper) SendReqCtx(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, et tikvrpc.EndpointType, directStoreAddr string, opts ...locate.StoreSelectorOption) (*tikvrpc.Response, *locate.RPCContext, string, error)` | Rust:  | Tests: 
- [ ] `func (ch *ClientHelper) UpdateResolvingLocks(locks []*txnlock.Lock, callerStartTS uint64, token int)` | Rust:  | Tests: 
- [ ] `func (l IsoLevel) ToPB() kvrpcpb.IsolationLevel` | Rust:  | Tests: 
- [ ] `func (rs *SnapshotRuntimeStats) Clone() *SnapshotRuntimeStats` | Rust:  | Tests: 
- [ ] `func (rs *SnapshotRuntimeStats) GetCmdRPCCount(cmd tikvrpc.CmdType) int64` | Rust:  | Tests: 
- [ ] `func (rs *SnapshotRuntimeStats) GetTimeDetail() *util.TimeDetail` | Rust:  | Tests: 
- [ ] `func (rs *SnapshotRuntimeStats) Merge(other *SnapshotRuntimeStats)` | Rust:  | Tests: 
- [ ] `func (rs *SnapshotRuntimeStats) String() string` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) AddRPCInterceptor(it interceptor.RPCInterceptor)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) BatchGetWithTier(ctx context.Context, keys [][]byte, readTier int, opt kv.BatchGetOptions) (m map[string]kv.ValueEntry, err error)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) CleanCache(keys [][]byte)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) Get(ctx context.Context, k []byte, options ...kv.GetOption) (entry kv.ValueEntry, err error)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) GetKVReadTimeout() time.Duration` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) GetResolveLockDetail() *util.ResolveLockDetail` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) IsInternal() bool` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) IterReverse(k, lowerBound []byte) (unionstore.Iterator, error)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetIsStalenessReadOnly(b bool)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetIsolationLevel(level IsoLevel)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetKVReadTimeout(readTimeout time.Duration)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetKeyOnly(b bool)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetLoadBasedReplicaReadThreshold(busyThreshold time.Duration)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetMatchStoreLabels(labels []*metapb.StoreLabel)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetNotFillCache(b bool)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetPipelined(ts uint64)` | Rust:  | Tests: 
- [x] `func (s *KVSnapshot) SetPriority(pri txnutil.Priority)` | Rust: `new-client-rust/src/transaction/snapshot.rs (Snapshot::set_priority)` | Tests: `new-client-rust/src/transaction/transaction.rs (request_context tests)`
- [x] `func (s *KVSnapshot) SetRPCInterceptor(it interceptor.RPCInterceptor)` | Rust: `new-client-rust/src/transaction/snapshot.rs (Snapshot::set_rpc_interceptor)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_rpc_interceptor_can_override_priority_penalty_and_tag)`
- [ ] `func (s *KVSnapshot) SetReadReplicaScope(scope string)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetReplicaRead(readType kv.ReplicaReadType)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetReplicaReadAdjuster(f ReplicaReadAdjuster)` | Rust:  | Tests: 
- [x] `func (s *KVSnapshot) SetResourceGroupName(name string)` | Rust: `new-client-rust/src/transaction/snapshot.rs (Snapshot::set_resource_group_name)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (s *KVSnapshot) SetResourceGroupTag(tag []byte)` | Rust: `new-client-rust/src/transaction/snapshot.rs (Snapshot::set_resource_group_tag)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_txn_request_context_applied_to_get_prewrite_commit)`
- [x] `func (s *KVSnapshot) SetResourceGroupTagger(tagger tikvrpc.ResourceGroupTagger)` | Rust: `new-client-rust/src/transaction/snapshot.rs (Snapshot::set_resource_group_tagger)` | Tests: `new-client-rust/src/transaction/transaction.rs (test_fixed_resource_group_tag_takes_precedence_over_tagger)`
- [ ] `func (s *KVSnapshot) SetRuntimeStats(stats *SnapshotRuntimeStats)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetSampleStep(step uint32)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetScanBatchSize(batchSize int)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetSnapshotTS(ts uint64)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetTaskID(id uint64)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetTxnScope(scope string)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SetVars(vars *kv.Variables)` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SnapCache() map[string]kv.ValueEntry` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SnapCacheHitCount() int` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) SnapCacheSize() int` | Rust:  | Tests: 
- [ ] `func (s *KVSnapshot) UpdateSnapshotCache(keys [][]byte, m map[string]kv.ValueEntry)` | Rust:  | Tests: 
- [ ] `func (s *Scanner) Close()` | Rust:  | Tests: 
- [ ] `func (s *Scanner) Key() []byte` | Rust:  | Tests: 
- [ ] `func (s *Scanner) Next() error` | Rust:  | Tests: 
- [ ] `func (s *Scanner) Valid() bool` | Rust:  | Tests: 
- [ ] `func (s *Scanner) Value() []byte` | Rust:  | Tests: 
- [ ] `func (s SnapshotProbe) BatchGetSingleRegion(bo *retry.Backoffer, region locate.RegionVerID, keys [][]byte, collectF func(k, v []byte)) error` | Rust:  | Tests: 
- [ ] `func (s SnapshotProbe) FormatStats() string` | Rust:  | Tests: 
- [ ] `func (s SnapshotProbe) MergeExecDetail(detail *kvrpcpb.ExecDetailsV2)` | Rust:  | Tests: 
- [ ] `func (s SnapshotProbe) MergeRegionRequestStats(rpcStats *locate.RegionRequestRuntimeStats)` | Rust:  | Tests: 
- [ ] `func (s SnapshotProbe) NewScanner(start, end []byte, batchSize int, reverse bool) (*Scanner, error)` | Rust:  | Tests: 
- [ ] `func (s SnapshotProbe) RecordBackoffInfo(bo *retry.Backoffer)` | Rust:  | Tests: 

## txnkv/txnutil (package txnutil)

### Types
- [ ] `type Priority kvrpcpb.CommandPri` | Rust:  | Tests: 

### Functions
- (none)

### Consts
- [ ] `PriorityHigh` | Rust:  | Tests: 
- [ ] `PriorityLow` | Rust:  | Tests: 
- [ ] `PriorityNormal` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- [ ] `func (p Priority) ToPB() kvrpcpb.CommandPri` | Rust:  | Tests: 

## util (package util)

### Types
- [ ] `type CommitDetails struct` | Rust:  | Tests: 
- [ ] `type CommitTSLagDetails struct` | Rust:  | Tests: 
- [ ] `type ExecDetails struct` | Rust:  | Tests: 
- [ ] `type InterceptedPDClient struct` | Rust:  | Tests: 
- [ ] `type LockKeysDetails struct` | Rust:  | Tests: 
- [ ] `type Option struct` | Rust:  | Tests: 
- [ ] `type RUDetails struct` | Rust:  | Tests: 
- [ ] `type RateLimit struct` | Rust:  | Tests: 
- [ ] `type ReqDetailInfo struct` | Rust:  | Tests: 
- [ ] `type RequestSource struct` | Rust:  | Tests: 
- [ ] `type RequestSourceKeyType struct` | Rust:  | Tests: 
- [ ] `type RequestSourceTypeKeyType struct` | Rust:  | Tests: 
- [ ] `type ResolveLockDetail struct` | Rust:  | Tests: 
- [ ] `type ScanDetail struct` | Rust:  | Tests: 
- [ ] `type TSSet struct` | Rust:  | Tests: 
- [ ] `type TiKVExecDetails struct` | Rust:  | Tests: 
- [ ] `type TimeDetail struct` | Rust:  | Tests: 
- [ ] `type TrafficDetails struct` | Rust:  | Tests: 
- [ ] `type WriteDetail struct` | Rust:  | Tests: 

### Functions
- [ ] `func BuildRequestSource(internal bool, source, explicitSource string) string` | Rust:  | Tests: 
- [ ] `func BytesToString(numBytes int64) string` | Rust:  | Tests: 
- [ ] `func CompatibleParseGCTime(value string) (time.Time, error)` | Rust:  | Tests: 
- [ ] `func ContextWithTraceExecDetails(ctx context.Context) context.Context` | Rust:  | Tests: 
- [ ] `func EnableFailpoints()` | Rust:  | Tests: 
- [ ] `func EvalFailpoint(name string) (interface{}, error)` | Rust:  | Tests: 
- [ ] `func FormatBytes(numBytes int64) string` | Rust:  | Tests: 
- [ ] `func FormatDuration(d time.Duration) string` | Rust:  | Tests: 
- [ ] `func GetCustomDNSDialer(dnsServer, dnsDomain string) dnsF` | Rust:  | Tests: 
- [ ] `func IsInternalRequest(source string) bool` | Rust:  | Tests: 
- [ ] `func IsRequestSourceInternal(reqSrc *RequestSource) bool` | Rust:  | Tests: 
- [ ] `func NewInterceptedPDClient(client pd.Client) *InterceptedPDClient` | Rust:  | Tests: 
- [ ] `func NewRUDetails() *RUDetails` | Rust:  | Tests: 
- [ ] `func NewRUDetailsWith(rru, wru float64, waitDur time.Duration) *RUDetails` | Rust:  | Tests: 
- [ ] `func NewRateLimit(n int) *RateLimit` | Rust:  | Tests: 
- [ ] `func NewTiKVExecDetails(pb *kvrpcpb.ExecDetailsV2) TiKVExecDetails` | Rust:  | Tests: 
- [ ] `func None[T interface{}]() Option[T]` | Rust:  | Tests: 
- [ ] `func RequestSourceFromCtx(ctx context.Context) string` | Rust:  | Tests: 
- [ ] `func ResourceGroupNameFromCtx(ctx context.Context) string` | Rust:  | Tests: 
- [ ] `func SetSessionID(ctx context.Context, sessionID uint64) context.Context` | Rust:  | Tests: 
- [ ] `func Some[T interface{}](inner T) Option[T]` | Rust:  | Tests: 
- [ ] `func TraceExecDetailsEnabled(ctx context.Context) bool` | Rust:  | Tests: 
- [ ] `func WithInternalSourceAndTaskType(ctx context.Context, source, taskName string) context.Context` | Rust:  | Tests: 
- [ ] `func WithInternalSourceType(ctx context.Context, source string) context.Context` | Rust:  | Tests: 
- [ ] `func WithRecovery(exec func(), recoverFn func(r interface{}))` | Rust:  | Tests: 
- [ ] `func WithResourceGroupName(ctx context.Context, groupName string) context.Context` | Rust:  | Tests: 

### Consts
- [ ] `ExplicitTypeBR` | Rust:  | Tests: 
- [ ] `ExplicitTypeBackground` | Rust:  | Tests: 
- [ ] `ExplicitTypeDDL` | Rust:  | Tests: 
- [ ] `ExplicitTypeDumpling` | Rust:  | Tests: 
- [ ] `ExplicitTypeEmpty` | Rust:  | Tests: 
- [ ] `ExplicitTypeLightning` | Rust:  | Tests: 
- [ ] `ExplicitTypeStats` | Rust:  | Tests: 
- [ ] `ExternalRequest` | Rust:  | Tests: 
- [ ] `GCTimeFormat` | Rust:  | Tests: 
- [ ] `InternalRequest` | Rust:  | Tests: 
- [ ] `InternalRequestPrefix` | Rust:  | Tests: 
- [ ] `InternalTxnGC` | Rust:  | Tests: 
- [ ] `InternalTxnMeta` | Rust:  | Tests: 
- [ ] `InternalTxnOthers` | Rust:  | Tests: 
- [ ] `InternalTxnStats` | Rust:  | Tests: 
- [ ] `SourceUnknown` | Rust:  | Tests: 

### Vars
- [ ] `CommitDetailCtxKey` | Rust:  | Tests: 
- [ ] `ExecDetailsKey` | Rust:  | Tests: 
- [ ] `ExplicitTypeList` | Rust:  | Tests: 
- [ ] `LockKeysDetailCtxKey` | Rust:  | Tests: 
- [ ] `RUDetailsCtxKey` | Rust:  | Tests: 
- [ ] `RequestSourceKey` | Rust:  | Tests: 
- [ ] `RequestSourceTypeKey` | Rust:  | Tests: 
- [ ] `SessionID` | Rust:  | Tests: 

### Methods
- [ ] `func (cd *CommitDetails) Clone() *CommitDetails` | Rust:  | Tests: 
- [ ] `func (cd *CommitDetails) Merge(other *CommitDetails)` | Rust:  | Tests: 
- [ ] `func (cd *CommitDetails) MergeCommitReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)` | Rust:  | Tests: 
- [ ] `func (cd *CommitDetails) MergeFlushReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)` | Rust:  | Tests: 
- [ ] `func (cd *CommitDetails) MergePrewriteReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)` | Rust:  | Tests: 
- [ ] `func (d *CommitTSLagDetails) Merge(other *CommitTSLagDetails)` | Rust:  | Tests: 
- [ ] `func (ed *TiKVExecDetails) String() string` | Rust:  | Tests: 
- [ ] `func (ld *LockKeysDetails) Clone() *LockKeysDetails` | Rust:  | Tests: 
- [ ] `func (ld *LockKeysDetails) Merge(lockKey *LockKeysDetails)` | Rust:  | Tests: 
- [ ] `func (ld *LockKeysDetails) MergeReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)` | Rust:  | Tests: 
- [ ] `func (m InterceptedPDClient) GetPrevRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error)` | Rust:  | Tests: 
- [ ] `func (m InterceptedPDClient) GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error)` | Rust:  | Tests: 
- [ ] `func (m InterceptedPDClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...opt.GetRegionOption) (*router.Region, error)` | Rust:  | Tests: 
- [ ] `func (m InterceptedPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)` | Rust:  | Tests: 
- [ ] `func (m InterceptedPDClient) GetTS(ctx context.Context) (int64, int64, error)` | Rust:  | Tests: 
- [ ] `func (m InterceptedPDClient) GetTSAsync(ctx context.Context) tso.TSFuture` | Rust:  | Tests: 
- [ ] `func (m InterceptedPDClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...opt.GetRegionOption) ([]*router.Region, error)` | Rust:  | Tests: 
- [ ] `func (m InterceptedPDClient) WithCallerComponent(component caller.Component) pd.Client` | Rust:  | Tests: 
- [ ] `func (o Option[T]) Inner() *T` | Rust:  | Tests: 
- [ ] `func (r *RateLimit) GetCapacity() int` | Rust:  | Tests: 
- [ ] `func (r *RateLimit) GetToken(done <-chan struct{}) (exit bool)` | Rust:  | Tests: 
- [ ] `func (r *RateLimit) PutToken()` | Rust:  | Tests: 
- [ ] `func (r *RequestSource) GetRequestSource() string` | Rust:  | Tests: 
- [ ] `func (r *RequestSource) SetExplicitRequestSourceType(tp string)` | Rust:  | Tests: 
- [ ] `func (r *RequestSource) SetRequestSourceInternal(internal bool)` | Rust:  | Tests: 
- [ ] `func (r *RequestSource) SetRequestSourceType(tp string)` | Rust:  | Tests: 
- [ ] `func (rd *RUDetails) Clone() *RUDetails` | Rust:  | Tests: 
- [ ] `func (rd *RUDetails) Merge(other *RUDetails)` | Rust:  | Tests: 
- [ ] `func (rd *RUDetails) RRU() float64` | Rust:  | Tests: 
- [ ] `func (rd *RUDetails) RUWaitDuration() time.Duration` | Rust:  | Tests: 
- [ ] `func (rd *RUDetails) String() string` | Rust:  | Tests: 
- [ ] `func (rd *RUDetails) Update(consumption *rmpb.Consumption, waitDuration time.Duration)` | Rust:  | Tests: 
- [ ] `func (rd *RUDetails) WRU() float64` | Rust:  | Tests: 
- [ ] `func (rd *ResolveLockDetail) Merge(resolveLock *ResolveLockDetail)` | Rust:  | Tests: 
- [ ] `func (s *TSSet) GetAll() []uint64` | Rust:  | Tests: 
- [ ] `func (s *TSSet) Put(tss ...uint64)` | Rust:  | Tests: 
- [ ] `func (sd *ScanDetail) Merge(scanDetail *ScanDetail)` | Rust:  | Tests: 
- [ ] `func (sd *ScanDetail) MergeFromScanDetailV2(scanDetail *kvrpcpb.ScanDetailV2)` | Rust:  | Tests: 
- [ ] `func (sd *ScanDetail) String() string` | Rust:  | Tests: 
- [ ] `func (td *TimeDetail) Merge(detail *TimeDetail)` | Rust:  | Tests: 
- [ ] `func (td *TimeDetail) MergeFromTimeDetail(timeDetailV2 *kvrpcpb.TimeDetailV2, timeDetail *kvrpcpb.TimeDetail)` | Rust:  | Tests: 
- [ ] `func (td *TimeDetail) String() string` | Rust:  | Tests: 
- [ ] `func (wd *WriteDetail) Merge(writeDetail *WriteDetail)` | Rust:  | Tests: 
- [ ] `func (wd *WriteDetail) MergeFromWriteDetailPb(pb *kvrpcpb.WriteDetail)` | Rust:  | Tests: 
- [ ] `func (wd *WriteDetail) String() string` | Rust:  | Tests: 

## util/async (package async)

### Types
- [ ] `type Callback interface` | Rust:  | Tests: 
- [ ] `type Executor interface` | Rust:  | Tests: 
- [ ] `type Pool interface` | Rust:  | Tests: 
- [ ] `type RunLoop struct` | Rust:  | Tests: 
- [ ] `type State uint32` | Rust:  | Tests: 

### Functions
- [ ] `func NewCallback[T any](e Executor, f func(T, error)) Callback[T]` | Rust:  | Tests: 
- [ ] `func NewRunLoop() *RunLoop` | Rust:  | Tests: 

### Consts
- [ ] `StateIdle` | Rust:  | Tests: 
- [ ] `StateRunning` | Rust:  | Tests: 
- [ ] `StateWaiting` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- [ ] `func (l *RunLoop) Append(fs ...func())` | Rust:  | Tests: 
- [ ] `func (l *RunLoop) Exec(ctx context.Context) (int, error)` | Rust:  | Tests: 
- [ ] `func (l *RunLoop) Go(f func())` | Rust:  | Tests: 
- [ ] `func (l *RunLoop) NumRunnable() int` | Rust:  | Tests: 
- [ ] `func (l *RunLoop) State() State` | Rust:  | Tests: 

## util/codec (package codec)

### Types
- (none)

### Functions
- [ ] `func DecodeBytes(b []byte, buf []byte) ([]byte, []byte, error)` | Rust:  | Tests: 
- [ ] `func DecodeCmpUintToInt(u uint64) int64` | Rust:  | Tests: 
- [ ] `func DecodeComparableUvarint(b []byte) ([]byte, uint64, error)` | Rust:  | Tests: 
- [ ] `func DecodeComparableVarint(b []byte) ([]byte, int64, error)` | Rust:  | Tests: 
- [ ] `func DecodeInt(b []byte) ([]byte, int64, error)` | Rust:  | Tests: 
- [ ] `func DecodeIntDesc(b []byte) ([]byte, int64, error)` | Rust:  | Tests: 
- [ ] `func DecodeUint(b []byte) ([]byte, uint64, error)` | Rust:  | Tests: 
- [ ] `func DecodeUintDesc(b []byte) ([]byte, uint64, error)` | Rust:  | Tests: 
- [ ] `func DecodeUvarint(b []byte) ([]byte, uint64, error)` | Rust:  | Tests: 
- [ ] `func DecodeVarint(b []byte) ([]byte, int64, error)` | Rust:  | Tests: 
- [ ] `func EncodeBytes(b []byte, data []byte) []byte` | Rust:  | Tests: 
- [ ] `func EncodeComparableUvarint(b []byte, v uint64) []byte` | Rust:  | Tests: 
- [ ] `func EncodeComparableVarint(b []byte, v int64) []byte` | Rust:  | Tests: 
- [ ] `func EncodeInt(b []byte, v int64) []byte` | Rust:  | Tests: 
- [ ] `func EncodeIntDesc(b []byte, v int64) []byte` | Rust:  | Tests: 
- [ ] `func EncodeIntToCmpUint(v int64) uint64` | Rust:  | Tests: 
- [ ] `func EncodeUint(b []byte, v uint64) []byte` | Rust:  | Tests: 
- [ ] `func EncodeUintDesc(b []byte, v uint64) []byte` | Rust:  | Tests: 
- [ ] `func EncodeUvarint(b []byte, v uint64) []byte` | Rust:  | Tests: 
- [ ] `func EncodeVarint(b []byte, v int64) []byte` | Rust:  | Tests: 

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
- [ ] `InTest` | Rust:  | Tests: 

### Methods
- (none)

## util/israce (package israce)

### Types
- (none)

### Functions
- (none)

### Consts
- [ ] `RaceEnabled` | Rust:  | Tests: 

### Vars
- (none)

### Methods
- (none)

## util/redact (package redact)

### Types
- (none)

### Functions
- [ ] `func Key(key []byte) string` | Rust:  | Tests: 
- [ ] `func KeyBytes(key []byte) []byte` | Rust:  | Tests: 
- [ ] `func NeedRedact() bool` | Rust:  | Tests: 
- [ ] `func RedactKeyErrIfNecessary(err *kvrpcpb.KeyError)` | Rust:  | Tests: 
- [ ] `func String(b []byte) (s string)` | Rust:  | Tests: 

### Consts
- (none)

### Vars
- (none)

### Methods
- (none)

