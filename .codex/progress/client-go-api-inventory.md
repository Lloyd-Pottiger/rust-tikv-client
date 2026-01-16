# client-go v2 API inventory (signature-level, auto-generated)

Module: `github.com/tikv/client-go/v2`

Generated from local source under `client-go/` (excluding `internal/`, `integration_tests/`, `examples/`, `testutils/`).
This is a signature-level inventory used to drive parity work in this repo's Rust client crate (repo root).

Regenerate:
- `go run ./tools/client-go-api-inventory`

## config (package config)

### Types
- `type AsyncCommit struct`
- `type Config struct`
- `type CoprocessorCache struct`
- `type PDClient struct`
- `type PessimisticTxn struct`
- `type Security struct`
- `type TiKVClient struct`
- `type TxnLocalLatches struct`

### Functions
- `func DefaultConfig() Config`
- `func DefaultPDClient() PDClient`
- `func DefaultTiKVClient() TiKVClient`
- `func DefaultTxnLocalLatches() TxnLocalLatches`
- `func GetGlobalConfig() *Config`
- `func GetTxnScopeFromConfig() string`
- `func NewSecurity(sslCA, sslCert, sslKey string, verityCN []string) Security`
- `func ParsePath(path string) (etcdAddrs []string, disableGC bool, keyspaceName string, err error)`
- `func StoreGlobalConfig(config *Config)`
- `func UpdateGlobal(f func(conf *Config)) func()`

### Consts
- `BatchPolicyBasic`
- `BatchPolicyCustom`
- `BatchPolicyPositive`
- `BatchPolicyStandard`
- `DefBatchPolicy`
- `DefGrpcInitialConnWindowSize`
- `DefGrpcInitialWindowSize`
- `DefMaxConcurrencyRequestLimit`
- `DefStoreLivenessTimeout`
- `DefStoresRefreshInterval`
- `NextGen`

### Vars
- (none)

### Methods
- `func (c *TxnLocalLatches) Valid() error`
- `func (config *TiKVClient) GetGrpcKeepAliveTimeout() time.Duration`
- `func (config *TiKVClient) Valid() error`
- `func (p *PDClient) Valid() error`
- `func (s *Security) ToTLSConfig() (tlsConfig *tls.Config, err error)`

## config/retry (package retry)

### Types
- `type BackoffFnCfg struct`
- `type Backoffer struct`
- `type Config struct`

### Functions
- `func IsFakeRegionError(err *errorpb.Error) bool`
- `func MayBackoffForRegionError(regionErr *errorpb.Error, bo *Backoffer) error`
- `func NewBackoffFnCfg(base, cap, jitter int) *BackoffFnCfg`
- `func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer`
- `func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer`
- `func NewConfig(name string, metric *prometheus.Observer, backoffFnCfg *BackoffFnCfg, err error) *Config`
- `func NewNoopBackoff(ctx context.Context) *Backoffer`

### Consts
- `DecorrJitter`
- `EqualJitter`
- `FullJitter`
- `NoJitter`

### Vars
- `BoCommitTSLag`
- `BoIsWitness`
- `BoMaxRegionNotInitialized`
- `BoMaxTsNotSynced`
- `BoPDRPC`
- `BoRegionMiss`
- `BoRegionRecoveryInProgress`
- `BoRegionScheduling`
- `BoStaleCmd`
- `BoTiFlashRPC`
- `BoTiFlashServerBusy`
- `BoTiKVDiskFull`
- `BoTiKVRPC`
- `BoTiKVServerBusy`
- `BoTxnLock`
- `BoTxnLockFast`
- `BoTxnNotFound`
- `TxnStartKey`

### Methods
- `func (b *Backoffer) Backoff(cfg *Config, err error) error`
- `func (b *Backoffer) BackoffWithCfgAndMaxSleep(cfg *Config, maxSleepMs int, err error) error`
- `func (b *Backoffer) BackoffWithMaxSleepTxnLockFast(maxSleepMs int, err error) error`
- `func (b *Backoffer) CheckKilled() error`
- `func (b *Backoffer) Clone() *Backoffer`
- `func (b *Backoffer) ErrorsNum() int`
- `func (b *Backoffer) Fork() (*Backoffer, context.CancelFunc)`
- `func (b *Backoffer) GetBackoffSleepMS() map[string]int`
- `func (b *Backoffer) GetBackoffTimes() map[string]int`
- `func (b *Backoffer) GetCtx() context.Context`
- `func (b *Backoffer) GetTotalBackoffTimes() int`
- `func (b *Backoffer) GetTotalSleep() int`
- `func (b *Backoffer) GetTypes() []string`
- `func (b *Backoffer) GetVars() *kv.Variables`
- `func (b *Backoffer) Reset()`
- `func (b *Backoffer) ResetMaxSleep(maxSleep int)`
- `func (b *Backoffer) SetCtx(ctx context.Context)`
- `func (b *Backoffer) String() string`
- `func (b *Backoffer) UpdateUsingForked(forked *Backoffer)`
- `func (c *Config) Base() int`
- `func (c *Config) SetBackoffFnCfg(fnCfg *BackoffFnCfg)`
- `func (c *Config) SetErrors(err error)`
- `func (c *Config) String() string`

## error (package error)

### Types
- `type ErrAssertionFailed struct`
- `type ErrDeadlock struct`
- `type ErrEntryTooLarge struct`
- `type ErrGCTooEarly struct`
- `type ErrKeyExist struct`
- `type ErrKeyTooLarge struct`
- `type ErrLockOnlyIfExistsNoPrimaryKey struct`
- `type ErrLockOnlyIfExistsNoReturnValue struct`
- `type ErrPDServerTimeout struct`
- `type ErrQueryInterruptedWithSignal struct`
- `type ErrRetryable struct`
- `type ErrTokenLimit struct`
- `type ErrTxnAbortedByGC struct`
- `type ErrTxnTooLarge struct`
- `type ErrWriteConflict struct`
- `type ErrWriteConflictInLatch struct`
- `type PDError struct`

### Functions
- `func ExtractDebugInfoStrFromKeyErr(keyErr *kvrpcpb.KeyError) string`
- `func ExtractKeyErr(keyErr *kvrpcpb.KeyError) error`
- `func IsErrKeyExist(err error) bool`
- `func IsErrNotFound(err error) bool`
- `func IsErrWriteConflict(err error) bool`
- `func IsErrorCommitTSLag(err error) bool`
- `func IsErrorUndetermined(err error) bool`
- `func Log(err error)`
- `func NewErrPDServerTimeout(msg string) error`
- `func NewErrWriteConflict(conflict *kvrpcpb.WriteConflict) *ErrWriteConflict`
- `func NewErrWriteConflictWithArgs(startTs, conflictTs, conflictCommitTs uint64, key []byte, reason kvrpcpb.WriteConflict_Reason) *ErrWriteConflict`

### Consts
- `MismatchClusterID`

### Vars
- `ErrBodyMissing`
- `ErrCannotSetNilValue`
- `ErrCommitTSLag`
- `ErrInvalidTxn`
- `ErrIsWitness`
- `ErrLockAcquireFailAndNoWaitSet`
- `ErrLockWaitTimeout`
- `ErrNotExist`
- `ErrQueryInterrupted`
- `ErrRegionDataNotReady`
- `ErrRegionFlashbackInProgress`
- `ErrRegionFlashbackNotPrepared`
- `ErrRegionNotInitialized`
- `ErrRegionRecoveryInProgress`
- `ErrRegionUnavailable`
- `ErrResolveLockTimeout`
- `ErrResultUndetermined`
- `ErrTiDBShuttingDown`
- `ErrTiFlashServerBusy`
- `ErrTiFlashServerTimeout`
- `ErrTiKVDiskFull`
- `ErrTiKVMaxTimestampNotSynced`
- `ErrTiKVServerBusy`
- `ErrTiKVServerTimeout`
- `ErrTiKVStaleCommand`
- `ErrUnknown`

### Methods
- `func (d *ErrDeadlock) Error() string`
- `func (d *PDError) Error() string`
- `func (e *ErrAssertionFailed) Error() string`
- `func (e *ErrEntryTooLarge) Error() string`
- `func (e *ErrGCTooEarly) Error() string`
- `func (e *ErrKeyTooLarge) Error() string`
- `func (e *ErrLockOnlyIfExistsNoPrimaryKey) Error() string`
- `func (e *ErrLockOnlyIfExistsNoReturnValue) Error() string`
- `func (e *ErrPDServerTimeout) Error() string`
- `func (e *ErrTokenLimit) Error() string`
- `func (e *ErrTxnAbortedByGC) Error() string`
- `func (e *ErrTxnTooLarge) Error() string`
- `func (e *ErrWriteConflictInLatch) Error() string`
- `func (e ErrQueryInterruptedWithSignal) Error() string`
- `func (k *ErrKeyExist) Error() string`
- `func (k *ErrRetryable) Error() string`
- `func (k *ErrWriteConflict) Error() string`

## kv (package kv)

### Types
- `type AccessLocationType byte`
- `type BatchGetOption interface`
- `type BatchGetOptions struct`
- `type BatchGetter interface`
- `type FlagsOp uint32`
- `type GetOption interface`
- `type GetOptions struct`
- `type GetOrBatchGetOption interface`
- `type Getter interface`
- `type KeyFlags uint16`
- `type KeyRange struct`
- `type LockCtx struct`
- `type ReplicaReadType byte`
- `type ReturnedValue struct`
- `type ValueEntry struct`
- `type Variables struct`

### Functions
- `func ApplyFlagsOps(origin KeyFlags, ops ...FlagsOp) KeyFlags`
- `func BatchGetToGetOptions(options []BatchGetOption) []GetOption`
- `func CmpKey(k, another []byte) int`
- `func NewLockCtx(forUpdateTS uint64, lockWaitTime int64, waitStartTime time.Time) *LockCtx`
- `func NewValueEntry(value []byte, commitTS uint64) ValueEntry`
- `func NewVariables(killed *uint32) *Variables`
- `func NextKey(k []byte) []byte`
- `func PrefixNextKey(k []byte) []byte`
- `func WithReturnCommitTS() GetOrBatchGetOption`

### Consts
- `AccessCrossZone`
- `AccessLocalZone`
- `AccessUnknown`
- `DefBackOffWeight`
- `DefBackoffLockFast`
- `DefTxnCommitBatchSize`
- `DelKeyLocked`
- `DelNeedCheckExists`
- `DelNeedConstraintCheckInPrewrite`
- `DelNeedLocked`
- `DelPresumeKeyNotExists`
- `FlagBytes`
- `LockAlwaysWait`
- `LockNoWait`
- `ReplicaReadFollower`
- `ReplicaReadLeader`
- `ReplicaReadLearner`
- `ReplicaReadMixed`
- `ReplicaReadPreferLeader`
- `SetAssertExist`
- `SetAssertNone`
- `SetAssertNotExist`
- `SetAssertUnknown`
- `SetIgnoredIn2PC`
- `SetKeyLocked`
- `SetKeyLockedValueExists`
- `SetKeyLockedValueNotExists`
- `SetNeedConstraintCheckInPrewrite`
- `SetNeedLocked`
- `SetNewlyInserted`
- `SetPresumeKeyNotExists`
- `SetPreviousPresumeKNE`
- `SetPrewriteOnly`
- `SetReadable`

### Vars
- `DefaultVars`
- `StoreLimit`
- `TxnCommitBatchSize`

### Methods
- `func (ctx *LockCtx) GetValueNotLocked(key []byte) ([]byte, bool)`
- `func (ctx *LockCtx) InitCheckExistence(capacity int)`
- `func (ctx *LockCtx) InitReturnValues(capacity int)`
- `func (ctx *LockCtx) IterateValuesNotLocked(f func([]byte, []byte))`
- `func (ctx *LockCtx) LockWaitTime() int64`
- `func (e ValueEntry) IsValueEmpty() bool`
- `func (e ValueEntry) Size() int`
- `func (f KeyFlags) AndPersistent() KeyFlags`
- `func (f KeyFlags) HasAssertExist() bool`
- `func (f KeyFlags) HasAssertNotExist() bool`
- `func (f KeyFlags) HasAssertUnknown() bool`
- `func (f KeyFlags) HasAssertionFlags() bool`
- `func (f KeyFlags) HasIgnoredIn2PC() bool`
- `func (f KeyFlags) HasLocked() bool`
- `func (f KeyFlags) HasLockedValueExists() bool`
- `func (f KeyFlags) HasNeedCheckExists() bool`
- `func (f KeyFlags) HasNeedConstraintCheckInPrewrite() bool`
- `func (f KeyFlags) HasNeedLocked() bool`
- `func (f KeyFlags) HasNewlyInserted() bool`
- `func (f KeyFlags) HasPresumeKeyNotExists() bool`
- `func (f KeyFlags) HasPrewriteOnly() bool`
- `func (f KeyFlags) HasReadable() bool`
- `func (o *BatchGetOptions) Apply(opts []BatchGetOption)`
- `func (o *GetOptions) Apply(opts []GetOption)`
- `func (r ReplicaReadType) IsFollowerRead() bool`
- `func (r ReplicaReadType) String() string`

## metrics (package metrics)

### Types
- `type LabelPair = dto.LabelPair`
- `type MetricVec interface`
- `type TxnCommitCounter struct`

### Functions
- `func FindNextStaleStoreID(collector prometheus.Collector, validStoreIDs map[uint64]struct{}) uint64`
- `func GetStoreMetricVecList() []MetricVec`
- `func GetTxnCommitCounter() TxnCommitCounter`
- `func InitMetrics(namespace, subsystem string)`
- `func InitMetricsWithConstLabels(namespace, subsystem string, constLabels prometheus.Labels)`
- `func ObserveReadSLI(readKeys uint64, readTime float64, readSize float64)`
- `func RegisterMetrics()`

### Consts
- `LabelBatchRecvLoop`
- `LabelBatchSendLoop`
- `LblAbort`
- `LblAddress`
- `LblBatchGet`
- `LblCommit`
- `LblDirection`
- `LblFromStore`
- `LblGeneral`
- `LblGet`
- `LblInternal`
- `LblLockKeys`
- `LblReason`
- `LblResult`
- `LblRollback`
- `LblScope`
- `LblSource`
- `LblStaleRead`
- `LblStore`
- `LblTarget`
- `LblToStore`
- `LblType`

### Vars
- `AggressiveLockedKeysDerived`
- `AggressiveLockedKeysLockedWithConflict`
- `AggressiveLockedKeysNew`
- `AggressiveLockedKeysNonForceLock`
- `AsyncBatchGetCounterWithLockError`
- `AsyncBatchGetCounterWithOK`
- `AsyncBatchGetCounterWithOtherError`
- `AsyncBatchGetCounterWithRegionError`
- `AsyncCommitTxnCounterError`
- `AsyncCommitTxnCounterOk`
- `AsyncSendReqCounterWithOK`
- `AsyncSendReqCounterWithOtherError`
- `AsyncSendReqCounterWithRPCError`
- `AsyncSendReqCounterWithRegionError`
- `AsyncSendReqCounterWithSendError`
- `BackoffHistogramDataNotReady`
- `BackoffHistogramEmpty`
- `BackoffHistogramIsWitness`
- `BackoffHistogramLock`
- `BackoffHistogramLockFast`
- `BackoffHistogramPD`
- `BackoffHistogramRPC`
- `BackoffHistogramRegionMiss`
- `BackoffHistogramRegionRecoveryInProgress`
- `BackoffHistogramRegionScheduling`
- `BackoffHistogramServerBusy`
- `BackoffHistogramStaleCmd`
- `BackoffHistogramTiKVDiskFull`
- `BatchRecvHistogramError`
- `BatchRecvHistogramOK`
- `BatchRequestDurationDone`
- `BatchRequestDurationRecv`
- `BatchRequestDurationSend`
- `LagCommitTSAttemptHistogramWithError`
- `LagCommitTSAttemptHistogramWithOK`
- `LagCommitTSWaitHistogramWithError`
- `LagCommitTSWaitHistogramWithOK`
- `LoadRegionCacheHistogramWhenCacheMiss`
- `LoadRegionCacheHistogramWithBatchScanRegions`
- `LoadRegionCacheHistogramWithGetStore`
- `LoadRegionCacheHistogramWithRegionByID`
- `LoadRegionCacheHistogramWithRegions`
- `LockResolverCountWithBatchResolve`
- `LockResolverCountWithExpired`
- `LockResolverCountWithNotExpired`
- `LockResolverCountWithQueryCheckSecondaryLocks`
- `LockResolverCountWithQueryTxnStatus`
- `LockResolverCountWithQueryTxnStatusCommitted`
- `LockResolverCountWithQueryTxnStatusRolledBack`
- `LockResolverCountWithResolve`
- `LockResolverCountWithResolveAsync`
- `LockResolverCountWithResolveForWrite`
- `LockResolverCountWithResolveLockLite`
- `LockResolverCountWithResolveLocks`
- `LockResolverCountWithWaitExpired`
- `OnePCTxnCounterError`
- `OnePCTxnCounterFallback`
- `OnePCTxnCounterOk`
- `PrewriteAssertionUsageCounterExist`
- `PrewriteAssertionUsageCounterNone`
- `PrewriteAssertionUsageCounterNotExist`
- `PrewriteAssertionUsageCounterUnknown`
- `RawkvCmdHistogramWithBatchDelete`
- `RawkvCmdHistogramWithBatchGet`
- `RawkvCmdHistogramWithBatchPut`
- `RawkvCmdHistogramWithDelete`
- `RawkvCmdHistogramWithGet`
- `RawkvCmdHistogramWithRawChecksum`
- `RawkvCmdHistogramWithRawReversScan`
- `RawkvCmdHistogramWithRawScan`
- `RawkvSizeHistogramWithKey`
- `RawkvSizeHistogramWithValue`
- `ReadRequestFollowerLocalBytes`
- `ReadRequestFollowerRemoteBytes`
- `ReadRequestLeaderLocalBytes`
- `ReadRequestLeaderRemoteBytes`
- `RegionCacheCounterWithBatchScanRegionsError`
- `RegionCacheCounterWithBatchScanRegionsOK`
- `RegionCacheCounterWithGetCacheMissError`
- `RegionCacheCounterWithGetCacheMissOK`
- `RegionCacheCounterWithGetRegionByIDError`
- `RegionCacheCounterWithGetRegionByIDOK`
- `RegionCacheCounterWithGetStoreError`
- `RegionCacheCounterWithGetStoreOK`
- `RegionCacheCounterWithInvalidateRegionFromCacheOK`
- `RegionCacheCounterWithInvalidateStoreRegionsOK`
- `RegionCacheCounterWithScanRegionsError`
- `RegionCacheCounterWithScanRegionsOK`
- `RegionCacheCounterWithSendFail`
- `SecondaryLockCleanupFailureCounterCommit`
- `SecondaryLockCleanupFailureCounterRollback`
- `StaleReadHitCounter`
- `StaleReadLocalInBytes`
- `StaleReadLocalOutBytes`
- `StaleReadMissCounter`
- `StaleReadRemoteInBytes`
- `StaleReadRemoteOutBytes`
- `StaleReadReqCrossZoneCounter`
- `StaleReadReqLocalCounter`
- `StatusCountWithError`
- `StatusCountWithOK`
- `TiKVAggressiveLockedKeysCounter`
- `TiKVAsyncBatchGetCounter`
- `TiKVAsyncCommitTxnCounter`
- `TiKVAsyncSendReqCounter`
- `TiKVBackoffHistogram`
- `TiKVBatchBestSize`
- `TiKVBatchClientRecycle`
- `TiKVBatchClientUnavailable`
- `TiKVBatchClientWaitEstablish`
- `TiKVBatchHeadArrivalInterval`
- `TiKVBatchMoreRequests`
- `TiKVBatchPendingRequests`
- `TiKVBatchRecvLoopDuration`
- `TiKVBatchRecvTailLatency`
- `TiKVBatchRequestDuration`
- `TiKVBatchRequests`
- `TiKVBatchSendLoopDuration`
- `TiKVBatchSendTailLatency`
- `TiKVBatchWaitOverLoad`
- `TiKVBucketClampedCounter`
- `TiKVFeedbackSlowScoreGauge`
- `TiKVForwardRequestCounter`
- `TiKVGRPCConnTransientFailureCounter`
- `TiKVGrpcConnectionState`
- `TiKVHealthFeedbackOpsCounter`
- `TiKVLoadRegionCacheHistogram`
- `TiKVLoadRegionCounter`
- `TiKVLoadTxnSafePointCounter`
- `TiKVLocalLatchWaitTimeHistogram`
- `TiKVLockResolverCounter`
- `TiKVLowResolutionTSOUpdateIntervalSecondsGauge`
- `TiKVMinSafeTSGapSeconds`
- `TiKVNoAvailableConnectionCounter`
- `TiKVOnePCTxnCounter`
- `TiKVPanicCounter`
- `TiKVPessimisticLockKeysDuration`
- `TiKVPipelinedFlushDuration`
- `TiKVPipelinedFlushLenHistogram`
- `TiKVPipelinedFlushSizeHistogram`
- `TiKVPipelinedFlushThrottleSecondsHistogram`
- `TiKVPreferLeaderFlowsGauge`
- `TiKVPrewriteAssertionUsageCounter`
- `TiKVRPCErrorCounter`
- `TiKVRPCNetLatencyHistogram`
- `TiKVRangeTaskPushDuration`
- `TiKVRangeTaskStats`
- `TiKVRawkvCmdHistogram`
- `TiKVRawkvSizeHistogram`
- `TiKVReadRequestBytes`
- `TiKVReadThroughput`
- `TiKVRegionCacheCounter`
- `TiKVRegionErrorCounter`
- `TiKVReplicaSelectorFailureCounter`
- `TiKVRequestRetryTimesHistogram`
- `TiKVSafeTSUpdateCounter`
- `TiKVSecondaryLockCleanupFailureCounter`
- `TiKVSendReqBySourceSummary`
- `TiKVSendReqHistogram`
- `TiKVSmallReadDuration`
- `TiKVStaleReadBytes`
- `TiKVStaleReadCounter`
- `TiKVStaleReadReqCounter`
- `TiKVStaleRegionFromPDCounter`
- `TiKVStatusCounter`
- `TiKVStatusDuration`
- `TiKVStoreLimitErrorCounter`
- `TiKVStoreLivenessGauge`
- `TiKVStoreSlowScoreGauge`
- `TiKVTSFutureWaitDuration`
- `TiKVTTLLifeTimeReachCounter`
- `TiKVTTLManagerHistogram`
- `TiKVTokenWaitDuration`
- `TiKVTwoPCTxnCounter`
- `TiKVTxnCmdHistogram`
- `TiKVTxnCommitBackoffCount`
- `TiKVTxnCommitBackoffSeconds`
- `TiKVTxnHeartBeatHistogram`
- `TiKVTxnLagCommitTSAttemptHistogram`
- `TiKVTxnLagCommitTSWaitHistogram`
- `TiKVTxnRegionsNumHistogram`
- `TiKVTxnWriteConflictCounter`
- `TiKVTxnWriteKVCountHistogram`
- `TiKVTxnWriteSizeHistogram`
- `TiKVUnsafeDestroyRangeFailuresCounterVec`
- `TiKVValidateReadTSFromPDCount`
- `TwoPCTxnCounterError`
- `TwoPCTxnCounterOk`
- `TxnCmdHistogramWithBatchGetGeneral`
- `TxnCmdHistogramWithBatchGetInternal`
- `TxnCmdHistogramWithCommitGeneral`
- `TxnCmdHistogramWithCommitInternal`
- `TxnCmdHistogramWithGetGeneral`
- `TxnCmdHistogramWithGetInternal`
- `TxnCmdHistogramWithLockKeysGeneral`
- `TxnCmdHistogramWithLockKeysInternal`
- `TxnCmdHistogramWithRollbackGeneral`
- `TxnCmdHistogramWithRollbackInternal`
- `TxnHeartBeatHistogramError`
- `TxnHeartBeatHistogramOK`
- `TxnRegionsNumHistogramCleanup`
- `TxnRegionsNumHistogramCleanupInternal`
- `TxnRegionsNumHistogramCommit`
- `TxnRegionsNumHistogramCommitInternal`
- `TxnRegionsNumHistogramPessimisticLock`
- `TxnRegionsNumHistogramPessimisticLockInternal`
- `TxnRegionsNumHistogramPessimisticRollback`
- `TxnRegionsNumHistogramPessimisticRollbackInternal`
- `TxnRegionsNumHistogramPrewrite`
- `TxnRegionsNumHistogramPrewriteInternal`
- `TxnRegionsNumHistogramWithBatchCoprocessor`
- `TxnRegionsNumHistogramWithBatchCoprocessorInternal`
- `TxnRegionsNumHistogramWithCoprocessor`
- `TxnRegionsNumHistogramWithCoprocessorInternal`
- `TxnRegionsNumHistogramWithSnapshot`
- `TxnRegionsNumHistogramWithSnapshotInternal`
- `TxnWriteKVCountHistogramGeneral`
- `TxnWriteKVCountHistogramInternal`
- `TxnWriteSizeHistogramGeneral`
- `TxnWriteSizeHistogramInternal`

### Methods
- `func (c TxnCommitCounter) Sub(rhs TxnCommitCounter) TxnCommitCounter`

## oracle (package oracle)

### Types
- `type ErrFutureTSRead struct`
- `type ErrLatestStaleRead struct`
- `type Future interface`
- `type NoopReadTSValidator struct`
- `type Option struct`
- `type Oracle interface`
- `type ReadTSValidator interface`

### Functions
- `func ComposeTS(physical, logical int64) uint64`
- `func ExtractLogical(ts uint64) int64`
- `func ExtractPhysical(ts uint64) int64`
- `func GetPhysical(t time.Time) int64`
- `func GetTimeFromTS(ts uint64) time.Time`
- `func GoTimeToLowerLimitStartTS(now time.Time, maxTxnTimeUse int64) uint64`
- `func GoTimeToTS(t time.Time) uint64`

### Consts
- `GlobalTxnScope`

### Vars
- (none)

### Methods
- `func (ErrLatestStaleRead) Error() string`
- `func (NoopReadTSValidator) ValidateReadTS(ctx context.Context, readTS uint64, isStaleRead bool, opt *Option) error`
- `func (e ErrFutureTSRead) Error() string`

## oracle/oracles (package oracles)

### Types
- `type MockOracle struct`
- `type PDOracleOptions struct`
- `type ValidateReadTSForTidbSnapshot struct`

### Functions
- `func NewLocalOracle() oracle.Oracle`
- `func NewPdOracle(pdClient pd.Client, options *PDOracleOptions) (oracle.Oracle, error)`

### Consts
- (none)

### Vars
- `EnableTSValidation`

### Methods
- `func (o *MockOracle) AddOffset(d time.Duration)`
- `func (o *MockOracle) Close()`
- `func (o *MockOracle) Disable()`
- `func (o *MockOracle) Enable()`
- `func (o *MockOracle) GetAllTSOKeyspaceGroupMinTS(ctx context.Context) (uint64, error)`
- `func (o *MockOracle) GetExternalTimestamp(ctx context.Context) (uint64, error)`
- `func (o *MockOracle) GetLowResolutionTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error)`
- `func (o *MockOracle) GetLowResolutionTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future`
- `func (o *MockOracle) GetStaleTimestamp(ctx context.Context, txnScope string, prevSecond uint64) (ts uint64, err error)`
- `func (o *MockOracle) GetTimestamp(ctx context.Context, _ *oracle.Option) (uint64, error)`
- `func (o *MockOracle) GetTimestampAsync(ctx context.Context, _ *oracle.Option) oracle.Future`
- `func (o *MockOracle) IsExpired(lockTimestamp, TTL uint64, _ *oracle.Option) bool`
- `func (o *MockOracle) SetExternalTimestamp(ctx context.Context, newTimestamp uint64) error`
- `func (o *MockOracle) SetLowResolutionTimestampUpdateInterval(time.Duration) error`
- `func (o *MockOracle) UntilExpired(lockTimeStamp, TTL uint64, _ *oracle.Option) int64`
- `func (o *MockOracle) ValidateReadTS(ctx context.Context, readTS uint64, isStaleRead bool, opt *oracle.Option) error`

## rawkv (package rawkv)

### Types
- `type Client struct`
- `type ClientOpt func(*option)`
- `type ClientProbe struct`
- `type ConfigProbe struct`
- `type RawChecksum struct`
- `type RawOption interface`

### Functions
- `func NewClient(ctx context.Context, pdAddrs []string, security config.Security, opts ...opt.ClientOption) (*Client, error)`
- `func NewClientWithOpts(ctx context.Context, pdAddrs []string, opts ...ClientOpt) (*Client, error)`
- `func ScanKeyOnly() RawOption`
- `func SetColumnFamily(cf string) RawOption`
- `func WithAPIVersion(apiVersion kvrpcpb.APIVersion) ClientOpt`
- `func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOpt`
- `func WithKeyspace(name string) ClientOpt`
- `func WithPDOptions(opts ...opt.ClientOption) ClientOpt`
- `func WithSecurity(security config.Security) ClientOpt`

### Consts
- (none)

### Vars
- `ErrMaxScanLimitExceeded`
- `MaxRawKVScanLimit`

### Methods
- `func (c *Client) BatchDelete(ctx context.Context, keys [][]byte, options ...RawOption) error`
- `func (c *Client) BatchGet(ctx context.Context, keys [][]byte, options ...RawOption) ([][]byte, error)`
- `func (c *Client) BatchPut(ctx context.Context, keys, values [][]byte, options ...RawOption) error`
- `func (c *Client) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, options ...RawOption) error`
- `func (c *Client) Checksum(ctx context.Context, startKey, endKey []byte, options ...RawOption, ) (check RawChecksum, err error)`
- `func (c *Client) Close() error`
- `func (c *Client) ClusterID() uint64`
- `func (c *Client) CompareAndSwap(ctx context.Context, key, previousValue, newValue []byte, options ...RawOption) ([]byte, bool, error)`
- `func (c *Client) Delete(ctx context.Context, key []byte, options ...RawOption) error`
- `func (c *Client) DeleteRange(ctx context.Context, startKey []byte, endKey []byte, options ...RawOption) error`
- `func (c *Client) Get(ctx context.Context, key []byte, options ...RawOption) ([]byte, error)`
- `func (c *Client) GetKeyTTL(ctx context.Context, key []byte, options ...RawOption) (*uint64, error)`
- `func (c *Client) GetPDClient() pd.Client`
- `func (c *Client) Put(ctx context.Context, key, value []byte, options ...RawOption) error`
- `func (c *Client) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64, options ...RawOption) error`
- `func (c *Client) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int, options ...RawOption) (keys [][]byte, values [][]byte, err error)`
- `func (c *Client) Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...RawOption, ) (keys [][]byte, values [][]byte, err error)`
- `func (c *Client) SetAtomicForCAS(b bool) *Client`
- `func (c *Client) SetColumnFamily(columnFamily string) *Client`
- `func (c ClientProbe) GetRegionCache() *locate.RegionCache`
- `func (c ClientProbe) SetPDClient(client pd.Client)`
- `func (c ClientProbe) SetRPCClient(client client.Client)`
- `func (c ClientProbe) SetRegionCache(regionCache *locate.RegionCache)`
- `func (c ConfigProbe) GetRawBatchPutSize() int`

## tikv (package tikv)

### Types
- `type BackoffConfig = retry.Config`
- `type Backoffer = retry.Backoffer`
- `type BaseRegionLockResolver struct`
- `type BatchLocateKeyRangesOpt = locate.BatchLocateKeyRangesOpt`
- `type BinlogWriteResult = transaction.BinlogWriteResult`
- `type Client = client.Client`
- `type ClientEventListener = client.ClientEventListener`
- `type ClientOpt = client.Opt`
- `type Codec = apicodec.Codec`
- `type CodecClient struct`
- `type CodecPDClient = locate.CodecPDClient`
- `type ConfigProbe struct`
- `type EtcdSafePointKV struct`
- `type GCOpt func(*gcOption)`
- `type Getter = kv.Getter`
- `type Iterator = unionstore.Iterator`
- `type KVFilter = transaction.KVFilter`
- `type KVStore struct`
- `type KVTxn = transaction.KVTxn`
- `type KeyLocation = locate.KeyLocation`
- `type KeyRange = kv.KeyRange`
- `type KeyspaceID = apicodec.KeyspaceID`
- `type LabelFilter = locate.LabelFilter`
- `type LockResolverProbe struct`
- `type MemBuffer = unionstore.MemBuffer`
- `type MemBufferSnapshot = unionstore.MemBufferSnapshot`
- `type MemDB = unionstore.MemDB`
- `type MemDBCheckpoint = unionstore.MemDBCheckpoint`
- `type Metrics = unionstore.Metrics`
- `type MockSafePointKV struct`
- `type Mode = apicodec.Mode`
- `type Option func(*KVStore)`
- `type Pool interface`
- `type RPCCanceller = locate.RPCCanceller`
- `type RPCCancellerCtxKey = locate.RPCCancellerCtxKey`
- `type RPCContext = locate.RPCContext`
- `type RPCRuntimeStats = locate.RPCRuntimeStats`
- `type Region = locate.Region`
- `type RegionCache = locate.RegionCache`
- `type RegionLockResolver interface`
- `type RegionRequestRuntimeStats = locate.RegionRequestRuntimeStats`
- `type RegionRequestSender = locate.RegionRequestSender`
- `type RegionVerID = locate.RegionVerID`
- `type SafePointKV interface`
- `type SafePointKVOpt func(*option)`
- `type SchemaLeaseChecker = transaction.SchemaLeaseChecker`
- `type SchemaVer = transaction.SchemaVer`
- `type Spool struct`
- `type Storage interface`
- `type Store = locate.Store`
- `type StoreProbe struct`
- `type StoreSelectorOption = locate.StoreSelectorOption`
- `type TxnOption func(*transaction.TxnOptions)`
- `type Variables = kv.Variables`

### Functions
- `func BoPDRPC() *BackoffConfig`
- `func BoRegionMiss() *BackoffConfig`
- `func BoTiFlashRPC() *BackoffConfig`
- `func BoTiKVRPC() *BackoffConfig`
- `func BoTxnLock() *BackoffConfig`
- `func ChangePDRegionMetaCircuitBreakerSettings(apply func(config *circuitbreaker.Settings))`
- `func CodecV1ExcludePrefixes() [][]byte`
- `func CodecV2Prefixes() [][]byte`
- `func DisableResourceControl()`
- `func EnableResourceControl()`
- `func GetStoreTypeByMeta(store *metapb.Store) tikvrpc.EndpointType`
- `func LoadShuttingDown() uint32`
- `func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer`
- `func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer`
- `func NewEtcdSafePointKV(addrs []string, tlsConfig *tls.Config, opts ...SafePointKVOpt) (*EtcdSafePointKV, error)`
- `func NewGcResolveLockMaxBackoffer(ctx context.Context) *Backoffer`
- `func NewKVStore( uuid string, pdClient pd.Client, spkv SafePointKV, tikvclient Client, opt ...Option, ) (_ *KVStore, retErr error)`
- `func NewLockResolver(etcdAddrs []string, security config.Security, opts ...opt.ClientOption) ( *txnlock.LockResolver, error, )`
- `func NewLockResolverProb(r *txnlock.LockResolver) *LockResolverProbe`
- `func NewMockSafePointKV(opts ...SafePointKVOpt) *MockSafePointKV`
- `func NewPDClient(pdAddrs []string) (pd.Client, error)`
- `func NewRPCClient(opts ...ClientOpt) *client.RPCClient`
- `func NewRPCanceller() *RPCCanceller`
- `func NewRegionCache(pdClient pd.Client) *locate.RegionCache`
- `func NewRegionLockResolver(identifier string, store Storage) *BaseRegionLockResolver`
- `func NewRegionRequestRuntimeStats() *RegionRequestRuntimeStats`
- `func NewRegionRequestSender(regionCache *RegionCache, client client.Client, readTSValidator oracle.ReadTSValidator) *RegionRequestSender`
- `func NewRegionVerID(id, confVer, ver uint64) RegionVerID`
- `func NewSpool(n int, dur time.Duration) *Spool`
- `func NewTestKeyspaceTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client, txnLocalLatches uint, keyspaceMeta keyspacepb.KeyspaceMeta, opt ...Option) (*KVStore, error)`
- `func NewTestTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client, txnLocalLatches uint, opt ...Option) (*KVStore, error)`
- `func ResolveLocksForRange( ctx context.Context, resolver RegionLockResolver, maxVersion uint64, startKey []byte, endKey []byte, createBackoffFn func(context.Context) *Backoffer, scanLimit uint32, ) (rangetask.TaskStat, error)`
- `func SetLogContextKey(key interface{})`
- `func SetRegionCacheTTLSec(t int64)`
- `func SetRegionCacheTTLWithJitter(base int64, jitter int64)`
- `func SetResourceControlInterceptor(interceptor resourceControlClient.ResourceGroupKVInterceptor)`
- `func SetStoreLivenessTimeout(t time.Duration)`
- `func StoreShuttingDown(v uint32)`
- `func TxnStartKey() interface{}`
- `func UnsetResourceControlInterceptor()`
- `func WithCodec(codec apicodec.Codec) ClientOpt`
- `func WithConcurrency(concurrency int) GCOpt`
- `func WithDefaultPipelinedTxn() TxnOption`
- `func WithLogContext(ctx context.Context, logger *zap.Logger) context.Context`
- `func WithMatchLabels(labels []*metapb.StoreLabel) StoreSelectorOption`
- `func WithMatchStores(stores []uint64) StoreSelectorOption`
- `func WithPDHTTPClient( source string, pdAddrs []string, opts ...pdhttp.ClientOption, ) Option`
- `func WithPipelinedTxn( flushConcurrency, resolveLockConcurrency int, writeThrottleRatio float64, ) TxnOption`
- `func WithPool(gp Pool) Option`
- `func WithPrefix(prefix string) SafePointKVOpt`
- `func WithSecurity(security config.Security) ClientOpt`
- `func WithStartTS(startTS uint64) TxnOption`
- `func WithTxnScope(txnScope string) TxnOption`
- `func WithUpdateInterval(updateInterval time.Duration) Option`

### Consts
- `CodecV2RawKeyspacePrefix`
- `CodecV2TxnKeyspacePrefix`
- `DCLabelKey`
- `EpochNotMatch`
- `GCScanLockLimit`
- `GcSavedSafePoint`
- `GcStateCacheInterval`
- `MaxTxnTimeUse`
- `MaxWriteExecutionTime`
- `ModeRaw`
- `ModeTxn`
- `NullspaceID`
- `ReadTimeoutMedium`
- `ReadTimeoutShort`

### Vars
- `DecodeKey`
- `DefaultKeyspaceID`
- `DefaultKeyspaceName`
- `EnableFailpoints`
- `LabelFilterAllNode`
- `LabelFilterAllTiFlashNode`
- `LabelFilterNoTiFlashWriteNode`
- `LabelFilterOnlyTiFlashWriteNode`
- `NewCodecPDClient`
- `NewCodecPDClientWithKeyspace`
- `NewCodecV1`
- `NewCodecV2`
- `NewNoopBackoff`
- `WithNeedBuckets`
- `WithNeedRegionHasLeaderPeer`

### Methods
- `func (c *CodecClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)`
- `func (c *CodecClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response])`
- `func (c ConfigProbe) GetBigTxnThreshold() int`
- `func (c ConfigProbe) GetDefaultLockTTL() uint64`
- `func (c ConfigProbe) GetGetMaxBackoff() int`
- `func (c ConfigProbe) GetScanBatchSize() int`
- `func (c ConfigProbe) GetTTLFactor() int`
- `func (c ConfigProbe) GetTxnCommitBatchSize() uint64`
- `func (c ConfigProbe) LoadPreSplitDetectThreshold() uint32`
- `func (c ConfigProbe) LoadPreSplitSizeThreshold() uint32`
- `func (c ConfigProbe) SetOracleUpdateInterval(v int)`
- `func (c ConfigProbe) StorePreSplitDetectThreshold(v uint32)`
- `func (c ConfigProbe) StorePreSplitSizeThreshold(v uint32)`
- `func (l *BaseRegionLockResolver) GetStore() Storage`
- `func (l *BaseRegionLockResolver) Identifier() string`
- `func (l *BaseRegionLockResolver) ResolveLocksInOneRegion(bo *Backoffer, locks []*txnlock.Lock, loc *locate.KeyLocation) (*locate.KeyLocation, error)`
- `func (l *BaseRegionLockResolver) ScanLocksInOneRegion(bo *Backoffer, key []byte, endKey []byte, maxVersion uint64, scanLimit uint32) ([]*txnlock.Lock, *locate.KeyLocation, error)`
- `func (l LockResolverProbe) ForceResolveLock(ctx context.Context, lock *txnlock.Lock) error`
- `func (l LockResolverProbe) ResolveLock(ctx context.Context, lock *txnlock.Lock) error`
- `func (l LockResolverProbe) ResolvePessimisticLock(ctx context.Context, lock *txnlock.Lock) error`
- `func (p *Spool) Run(fn func()) error`
- `func (s *KVStore) Begin(opts ...TxnOption) (txn *transaction.KVTxn, err error)`
- `func (s *KVStore) CheckRegionInScattering(regionID uint64) (bool, error)`
- `func (s *KVStore) CheckVisibility(startTS uint64) error`
- `func (s *KVStore) Close() error`
- `func (s *KVStore) Closed() <-chan struct{}`
- `func (s *KVStore) Ctx() context.Context`
- `func (s *KVStore) CurrentAllTSOKeyspaceGroupMinTs() (uint64, error)`
- `func (s *KVStore) CurrentTimestamp(txnScope string) (uint64, error)`
- `func (s *KVStore) DeleteRange( ctx context.Context, startKey []byte, endKey []byte, concurrency int, ) (completedRegions int, err error)`
- `func (s *KVStore) EnableTxnLocalLatches(size uint)`
- `func (s *KVStore) GC(ctx context.Context, expectedSafePoint uint64, opts ...GCOpt) (newGCSafePoint uint64, err error)`
- `func (s *KVStore) GetClusterID() uint64`
- `func (s *KVStore) GetLockResolver() *txnlock.LockResolver`
- `func (s *KVStore) GetMinSafeTS(txnScope string) uint64`
- `func (s *KVStore) GetOracle() oracle.Oracle`
- `func (s *KVStore) GetPDClient() pd.Client`
- `func (s *KVStore) GetPDHTTPClient() pdhttp.Client`
- `func (s *KVStore) GetRegionCache() *locate.RegionCache`
- `func (s *KVStore) GetSafePointKV() SafePointKV`
- `func (s *KVStore) GetSnapshot(ts uint64) *txnsnapshot.KVSnapshot`
- `func (s *KVStore) GetTiKVClient() (client Client)`
- `func (s *KVStore) GetTimestampWithRetry(bo *Backoffer, scope string) (uint64, error)`
- `func (s *KVStore) Go(f func()) error`
- `func (s *KVStore) IsClose() bool`
- `func (s *KVStore) IsLatchEnabled() bool`
- `func (s *KVStore) SendReq( bo *Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, ) (*tikvrpc.Response, error)`
- `func (s *KVStore) SetOracle(oracle oracle.Oracle)`
- `func (s *KVStore) SetTiKVClient(client Client)`
- `func (s *KVStore) SplitRegions(ctx context.Context, splitKeys [][]byte, scatter bool, tableID *int64) (regionIDs []uint64, err error)`
- `func (s *KVStore) SupportDeleteRange() (supported bool)`
- `func (s *KVStore) TxnLatches() *latch.LatchesScheduler`
- `func (s *KVStore) UUID() string`
- `func (s *KVStore) UnsafeDestroyRange(ctx context.Context, startKey []byte, endKey []byte) error`
- `func (s *KVStore) UpdateTxnSafePointCache(txnSafePoint uint64, now time.Time)`
- `func (s *KVStore) WaitGroup() *sync.WaitGroup`
- `func (s *KVStore) WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error`
- `func (s StoreProbe) Begin(opts ...TxnOption) (transaction.TxnProbe, error)`
- `func (s StoreProbe) ClearTxnLatches()`
- `func (s StoreProbe) GCResolveLockPhase(ctx context.Context, safepoint uint64, concurrency int) error`
- `func (s StoreProbe) GetCompatibleTxnSafePointLoaderUnderlyingEtcdClient() *clientv3.Client`
- `func (s StoreProbe) GetGCStatesClient() pdgc.GCStatesClient`
- `func (s StoreProbe) GetSnapshot(ts uint64) txnsnapshot.SnapshotProbe`
- `func (s StoreProbe) LoadSafePointFromSafePointKV() (uint64, error)`
- `func (s StoreProbe) LoadTxnSafePoint(ctx context.Context) (uint64, error)`
- `func (s StoreProbe) NewLockResolver() LockResolverProbe`
- `func (s StoreProbe) ReplaceGCStatesClient(c pdgc.GCStatesClient)`
- `func (s StoreProbe) SaveSafePointToSafePointKV(v uint64) error`
- `func (s StoreProbe) ScanLocks(ctx context.Context, startKey, endKey []byte, maxVersion uint64) ([]*txnlock.Lock, error)`
- `func (s StoreProbe) SendTxnHeartbeat(ctx context.Context, key []byte, startTS uint64, ttl uint64) (uint64, error)`
- `func (s StoreProbe) SetRegionCachePDClient(client pd.Client)`
- `func (s StoreProbe) SetRegionCacheStore(id uint64, storeType tikvrpc.EndpointType, state uint64, labels []*metapb.StoreLabel)`
- `func (s StoreProbe) SetSafeTS(storeID, safeTS uint64)`
- `func (s StoreProbe) UpdateTxnSafePointCache(txnSafePoint uint64, now time.Time)`
- `func (w *EtcdSafePointKV) Close() error`
- `func (w *EtcdSafePointKV) Get(k string) (string, error)`
- `func (w *EtcdSafePointKV) GetWithPrefix(k string) ([]*mvccpb.KeyValue, error)`
- `func (w *EtcdSafePointKV) Put(k string, v string) error`
- `func (w *MockSafePointKV) Close() error`
- `func (w *MockSafePointKV) Get(k string) (string, error)`
- `func (w *MockSafePointKV) GetWithPrefix(prefix string) ([]*mvccpb.KeyValue, error)`
- `func (w *MockSafePointKV) Put(k string, v string) error`

## tikvrpc (package tikvrpc)

### Types
- `type BatchCopStreamResponse struct`
- `type CmdType uint16`
- `type CopStreamResponse struct`
- `type EndpointType uint8`
- `type Lease struct`
- `type MPPStreamResponse struct`
- `type Request struct`
- `type ResourceGroupTagger func(req *Request)`
- `type Response struct`
- `type ResponseExt struct`

### Functions
- `func AttachContext(req *Request, rpcCtx kvrpcpb.Context) bool`
- `func CallDebugRPC(ctx context.Context, client debugpb.DebugClient, req *Request) (*Response, error)`
- `func CallRPC(ctx context.Context, client tikvpb.TikvClient, req *Request) (*Response, error)`
- `func CheckStreamTimeoutLoop(ch <-chan *Lease, done <-chan struct{})`
- `func FromBatchCommandsResponse(res *tikvpb.BatchCommandsResponse_Response) (*Response, error)`
- `func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error)`
- `func GetStoreTypeByMeta(store *metapb.Store) EndpointType`
- `func NewReplicaReadRequest(typ CmdType, pointer interface{}, replicaReadType kv.ReplicaReadType, replicaReadSeed *uint32, ctxs ...kvrpcpb.Context) *Request`
- `func NewRequest(typ CmdType, pointer interface{}, ctxs ...kvrpcpb.Context) *Request`
- `func SetContext(req *Request, region *metapb.Region, peer *metapb.Peer) error`
- `func SetContextNoAttach(req *Request, region *metapb.Region, peer *metapb.Peer) error`

### Consts
- `CmdBatchCop`
- `CmdBatchGet`
- `CmdBatchRollback`
- `CmdBroadcastTxnStatus`
- `CmdBufferBatchGet`
- `CmdCheckLockObserver`
- `CmdCheckSecondaryLocks`
- `CmdCheckTxnStatus`
- `CmdCleanup`
- `CmdCommit`
- `CmdCompact`
- `CmdCop`
- `CmdCopStream`
- `CmdDebugGetRegionProperties`
- `CmdDeleteRange`
- `CmdEmpty`
- `CmdFlashbackToVersion`
- `CmdFlush`
- `CmdGC`
- `CmdGet`
- `CmdGetHealthFeedback`
- `CmdGetKeyTTL`
- `CmdGetTiFlashSystemTable`
- `CmdLockWaitInfo`
- `CmdMPPAlive`
- `CmdMPPCancel`
- `CmdMPPConn`
- `CmdMPPTask`
- `CmdMvccGetByKey`
- `CmdMvccGetByStartTs`
- `CmdPessimisticLock`
- `CmdPessimisticRollback`
- `CmdPhysicalScanLock`
- `CmdPrepareFlashbackToVersion`
- `CmdPrewrite`
- `CmdRawBatchDelete`
- `CmdRawBatchGet`
- `CmdRawBatchPut`
- `CmdRawChecksum`
- `CmdRawCompareAndSwap`
- `CmdRawDelete`
- `CmdRawDeleteRange`
- `CmdRawGet`
- `CmdRawGetKeyTTL`
- `CmdRawPut`
- `CmdRawScan`
- `CmdRegisterLockObserver`
- `CmdRemoveLockObserver`
- `CmdResolveLock`
- `CmdScan`
- `CmdScanLock`
- `CmdSplitRegion`
- `CmdStoreSafeTS`
- `CmdTxnHeartBeat`
- `CmdUnsafeDestroyRange`
- `EngineLabelKey`
- `EngineLabelTiFlash`
- `EngineLabelTiFlashCompute`
- `EngineRoleLabelKey`
- `EngineRoleWrite`
- `TiDB`
- `TiFlash`
- `TiFlashCompute`
- `TiKV`

### Vars
- (none)

### Methods
- `func (req *Request) BatchCop() *coprocessor.BatchRequest`
- `func (req *Request) BatchGet() *kvrpcpb.BatchGetRequest`
- `func (req *Request) BatchRollback() *kvrpcpb.BatchRollbackRequest`
- `func (req *Request) BroadcastTxnStatus() *kvrpcpb.BroadcastTxnStatusRequest`
- `func (req *Request) BufferBatchGet() *kvrpcpb.BufferBatchGetRequest`
- `func (req *Request) CancelMPPTask() *mpp.CancelTaskRequest`
- `func (req *Request) CheckLockObserver() *kvrpcpb.CheckLockObserverRequest`
- `func (req *Request) CheckSecondaryLocks() *kvrpcpb.CheckSecondaryLocksRequest`
- `func (req *Request) CheckTxnStatus() *kvrpcpb.CheckTxnStatusRequest`
- `func (req *Request) Cleanup() *kvrpcpb.CleanupRequest`
- `func (req *Request) Commit() *kvrpcpb.CommitRequest`
- `func (req *Request) Compact() *kvrpcpb.CompactRequest`
- `func (req *Request) Cop() *coprocessor.Request`
- `func (req *Request) DebugGetRegionProperties() *debugpb.GetRegionPropertiesRequest`
- `func (req *Request) DeleteRange() *kvrpcpb.DeleteRangeRequest`
- `func (req *Request) DisableStaleReadMeetLock()`
- `func (req *Request) DispatchMPPTask() *mpp.DispatchTaskRequest`
- `func (req *Request) Empty() *tikvpb.BatchCommandsEmptyRequest`
- `func (req *Request) EnableStaleWithMixedReplicaRead()`
- `func (req *Request) EstablishMPPConn() *mpp.EstablishMPPConnectionRequest`
- `func (req *Request) FlashbackToVersion() *kvrpcpb.FlashbackToVersionRequest`
- `func (req *Request) Flush() *kvrpcpb.FlushRequest`
- `func (req *Request) GC() *kvrpcpb.GCRequest`
- `func (req *Request) Get() *kvrpcpb.GetRequest`
- `func (req *Request) GetHealthFeedback() *kvrpcpb.GetHealthFeedbackRequest`
- `func (req *Request) GetReplicaReadSeed() *uint32`
- `func (req *Request) GetSize() int`
- `func (req *Request) GetStartTS() uint64`
- `func (req *Request) GetTiFlashSystemTable() *kvrpcpb.TiFlashSystemTableRequest`
- `func (req *Request) IsDebugReq() bool`
- `func (req *Request) IsGlobalStaleRead() bool`
- `func (req *Request) IsGreenGCRequest() bool`
- `func (req *Request) IsInterruptible() bool`
- `func (req *Request) IsMPPAlive() *mpp.IsAliveRequest`
- `func (req *Request) IsRawWriteRequest() bool`
- `func (req *Request) IsTxnWriteRequest() bool`
- `func (req *Request) LockWaitInfo() *kvrpcpb.GetLockWaitInfoRequest`
- `func (req *Request) MvccGetByKey() *kvrpcpb.MvccGetByKeyRequest`
- `func (req *Request) MvccGetByStartTs() *kvrpcpb.MvccGetByStartTsRequest`
- `func (req *Request) PessimisticLock() *kvrpcpb.PessimisticLockRequest`
- `func (req *Request) PessimisticRollback() *kvrpcpb.PessimisticRollbackRequest`
- `func (req *Request) PhysicalScanLock() *kvrpcpb.PhysicalScanLockRequest`
- `func (req *Request) PrepareFlashbackToVersion() *kvrpcpb.PrepareFlashbackToVersionRequest`
- `func (req *Request) Prewrite() *kvrpcpb.PrewriteRequest`
- `func (req *Request) RawBatchDelete() *kvrpcpb.RawBatchDeleteRequest`
- `func (req *Request) RawBatchGet() *kvrpcpb.RawBatchGetRequest`
- `func (req *Request) RawBatchPut() *kvrpcpb.RawBatchPutRequest`
- `func (req *Request) RawChecksum() *kvrpcpb.RawChecksumRequest`
- `func (req *Request) RawCompareAndSwap() *kvrpcpb.RawCASRequest`
- `func (req *Request) RawDelete() *kvrpcpb.RawDeleteRequest`
- `func (req *Request) RawDeleteRange() *kvrpcpb.RawDeleteRangeRequest`
- `func (req *Request) RawGet() *kvrpcpb.RawGetRequest`
- `func (req *Request) RawGetKeyTTL() *kvrpcpb.RawGetKeyTTLRequest`
- `func (req *Request) RawPut() *kvrpcpb.RawPutRequest`
- `func (req *Request) RawScan() *kvrpcpb.RawScanRequest`
- `func (req *Request) RegisterLockObserver() *kvrpcpb.RegisterLockObserverRequest`
- `func (req *Request) RemoveLockObserver() *kvrpcpb.RemoveLockObserverRequest`
- `func (req *Request) ResolveLock() *kvrpcpb.ResolveLockRequest`
- `func (req *Request) Scan() *kvrpcpb.ScanRequest`
- `func (req *Request) ScanLock() *kvrpcpb.ScanLockRequest`
- `func (req *Request) SetReplicaReadType(replicaReadType kv.ReplicaReadType)`
- `func (req *Request) SplitRegion() *kvrpcpb.SplitRegionRequest`
- `func (req *Request) StoreSafeTS() *kvrpcpb.StoreSafeTSRequest`
- `func (req *Request) ToBatchCommandsRequest() *tikvpb.BatchCommandsRequest_Request`
- `func (req *Request) TxnHeartBeat() *kvrpcpb.TxnHeartBeatRequest`
- `func (req *Request) UnsafeDestroyRange() *kvrpcpb.UnsafeDestroyRangeRequest`
- `func (resp *BatchCopStreamResponse) Close()`
- `func (resp *BatchCopStreamResponse) Recv() (*coprocessor.BatchResponse, error)`
- `func (resp *CopStreamResponse) Close()`
- `func (resp *CopStreamResponse) Recv() (*coprocessor.Response, error)`
- `func (resp *MPPStreamResponse) Close()`
- `func (resp *MPPStreamResponse) Recv() (*mpp.MPPDataPacket, error)`
- `func (resp *Response) GetExecDetailsV2() *kvrpcpb.ExecDetailsV2`
- `func (resp *Response) GetRegionError() (*errorpb.Error, error)`
- `func (resp *Response) GetSize() int`
- `func (t CmdType) String() string`
- `func (t EndpointType) IsTiFlashRelatedType() bool`
- `func (t EndpointType) Name() string`

## tikvrpc/interceptor (package interceptor)

### Types
- `type MockInterceptorManager struct`
- `type RPCInterceptor interface`
- `type RPCInterceptorChain struct`
- `type RPCInterceptorFunc func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error)`

### Functions
- `func ChainRPCInterceptors(first RPCInterceptor, rest ...RPCInterceptor) RPCInterceptor`
- `func GetRPCInterceptorFromCtx(ctx context.Context) RPCInterceptor`
- `func NewMockInterceptorManager() *MockInterceptorManager`
- `func NewRPCInterceptor(name string, fn func(next RPCInterceptorFunc) RPCInterceptorFunc) RPCInterceptor`
- `func NewRPCInterceptorChain() *RPCInterceptorChain`
- `func WithRPCInterceptor(ctx context.Context, interceptor RPCInterceptor) context.Context`

### Consts
- (none)

### Vars
- (none)

### Methods
- `func (c *RPCInterceptorChain) Len() int`
- `func (c *RPCInterceptorChain) Link(it RPCInterceptor) *RPCInterceptorChain`
- `func (c *RPCInterceptorChain) Name() string`
- `func (c *RPCInterceptorChain) Wrap(next RPCInterceptorFunc) RPCInterceptorFunc`
- `func (m *MockInterceptorManager) BeginCount() int`
- `func (m *MockInterceptorManager) CreateMockInterceptor(name string) RPCInterceptor`
- `func (m *MockInterceptorManager) EndCount() int`
- `func (m *MockInterceptorManager) ExecLog() []string`
- `func (m *MockInterceptorManager) Reset()`

## trace (package trace)

### Types
- `type Category uint32`
- `type IsCategoryEnabledFunc func(category Category) bool`
- `type TraceControlExtractorFunc func(ctx context.Context) TraceControlFlags`
- `type TraceControlFlags uint64`
- `type TraceEventFunc func(ctx context.Context, category Category, name string, fields ...zap.Field)`

### Functions
- `func ContextWithTraceID(ctx context.Context, traceID []byte) context.Context`
- `func GetTraceControlFlags(ctx context.Context) TraceControlFlags`
- `func ImmediateLoggingEnabled(ctx context.Context) bool`
- `func IsCategoryEnabled(category Category) bool`
- `func SetIsCategoryEnabledFunc(fn IsCategoryEnabledFunc)`
- `func SetTraceControlExtractor(fn TraceControlExtractorFunc)`
- `func SetTraceEventFunc(fn TraceEventFunc)`
- `func TraceEvent(ctx context.Context, category Category, name string, fields ...zap.Field)`
- `func TraceIDFromContext(ctx context.Context) []byte`

### Consts
- `CategoryKVRequest`
- `CategoryRegionCache`
- `CategoryTxn2PC`
- `CategoryTxnLockResolve`
- `FlagImmediateLog`
- `FlagTiKVCategoryReadDetails`
- `FlagTiKVCategoryRequest`
- `FlagTiKVCategoryWriteDetails`

### Vars
- (none)

### Methods
- `func (f TraceControlFlags) Has(flag TraceControlFlags) bool`
- `func (f TraceControlFlags) With(flag TraceControlFlags) TraceControlFlags`

## txnkv (package txnkv)

### Types
- `type BinlogWriteResult = transaction.BinlogWriteResult`
- `type Client struct`
- `type ClientOpt func(*option)`
- `type IsoLevel = txnsnapshot.IsoLevel`
- `type KVFilter = transaction.KVFilter`
- `type KVSnapshot = txnsnapshot.KVSnapshot`
- `type KVTxn = transaction.KVTxn`
- `type Lock = txnlock.Lock`
- `type LockResolver = txnlock.LockResolver`
- `type Priority = txnutil.Priority`
- `type ReplicaReadAdjuster = txnsnapshot.ReplicaReadAdjuster`
- `type Scanner = txnsnapshot.Scanner`
- `type SchemaLeaseChecker = transaction.SchemaLeaseChecker`
- `type SchemaVer = transaction.SchemaVer`
- `type SnapshotRuntimeStats = txnsnapshot.SnapshotRuntimeStats`
- `type TxnStatus = txnlock.TxnStatus`

### Functions
- `func NewClient(pdAddrs []string, opts ...ClientOpt) (*Client, error)`
- `func NewLock(l *kvrpcpb.LockInfo) *Lock`
- `func WithAPIVersion(apiVersion kvrpcpb.APIVersion) ClientOpt`
- `func WithKeyspace(keyspaceName string) ClientOpt`
- `func WithSafePointKVPrefix(prefix string) ClientOpt`

### Consts
- `MaxTxnTimeUse`
- `PriorityHigh`
- `PriorityLow`
- `PriorityNormal`
- `RC`
- `RCCheckTS`
- `SI`

### Vars
- (none)

### Methods
- `func (c *Client) GetTimestamp(ctx context.Context) (uint64, error)`

## txnkv/rangetask (package rangetask)

### Types
- `type DeleteRangeTask struct`
- `type Runner struct`
- `type TaskHandler = func(ctx context.Context, r kv.KeyRange) (TaskStat, error)`
- `type TaskStat struct`

### Functions
- `func NewDeleteRangeTask(store storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask`
- `func NewLocateRegionBackoffer(ctx context.Context) *retry.Backoffer`
- `func NewNotifyDeleteRangeTask(store storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask`
- `func NewRangeTaskRunner( name string, store storage, concurrency int, handler TaskHandler, ) *Runner`
- `func NewRangeTaskRunnerWithID( name string, identifier string, store storage, concurrency int, handler TaskHandler, ) *Runner`

### Consts
- (none)

### Vars
- (none)

### Methods
- `func (s *Runner) CompletedRegions() int`
- `func (s *Runner) FailedRegions() int`
- `func (s *Runner) RunOnRange(ctx context.Context, startKey, endKey []byte) error`
- `func (s *Runner) SetRegionsPerTask(regionsPerTask int)`
- `func (s *Runner) SetStatLogInterval(interval time.Duration)`
- `func (t *DeleteRangeTask) CompletedRegions() int`
- `func (t *DeleteRangeTask) Execute(ctx context.Context) error`

## txnkv/transaction (package transaction)

### Types
- `type AggressiveLockedKeyInfo struct`
- `type BatchBufferGetter interface`
- `type BatchSnapshotBufferGetter interface`
- `type BinlogExecutor interface`
- `type BinlogWriteResult interface`
- `type BufferBatchGetter struct`
- `type BufferSnapshotBatchGetter struct`
- `type CommitterMutationFlags uint8`
- `type CommitterMutations interface`
- `type CommitterProbe struct`
- `type ConfigProbe struct`
- `type KVFilter interface`
- `type KVTxn struct`
- `type LifecycleHooks struct`
- `type MemBufferMutationsProbe struct`
- `type PipelinedTxnOptions struct`
- `type PlainMutation struct`
- `type PlainMutations struct`
- `type PrewriteEncounterLockPolicy int`
- `type RelatedSchemaChange struct`
- `type SchemaLeaseChecker interface`
- `type SchemaVer interface`
- `type TxnInfo struct`
- `type TxnOptions struct`
- `type TxnProbe struct`
- `type WriteAccessLevel int`

### Functions
- `func NewBufferBatchGetter(buffer BatchBufferGetter, snapshot kv.BatchGetter) *BufferBatchGetter`
- `func NewBufferSnapshotBatchGetter(buffer BatchSnapshotBufferGetter, snapshot kv.BatchGetter) *BufferSnapshotBatchGetter`
- `func NewMemBufferMutationsProbe(sizeHint int, storage *unionstore.MemDB) MemBufferMutationsProbe`
- `func NewPlainMutations(sizeHint int) PlainMutations`
- `func NewTiKVTxn(store kvstore, snapshot *txnsnapshot.KVSnapshot, startTS uint64, options *TxnOptions) (*KVTxn, error)`
- `func SendTxnHeartBeat(bo *retry.Backoffer, store kvstore, primary []byte, startTS, ttl uint64) (newTTL uint64, stopHeartBeat bool, err error)`

### Consts
- `CommitSecondaryMaxBackoff`
- `MaxExecTimeExceededSignal`
- `MaxTxnTimeUse`
- `MutationFlagIsAssertExists`
- `MutationFlagIsAssertNotExists`
- `MutationFlagIsPessimisticLock`
- `MutationFlagNeedConstraintCheckInPrewrite`
- `NoResolvePolicy`
- `PipelinedRequestSource`
- `TryResolvePolicy`
- `TsoMaxBackoff`

### Vars
- `CommitMaxBackoff`
- `CtxInGetTimestampForCommitKey`
- `ManagedLockTTL`
- `MaxPipelinedTxnTTL`
- `PrewriteMaxBackoff`
- `SetSuccess`

### Methods
- `func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error)`
- `func (b *BufferSnapshotBatchGetter) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error)`
- `func (c *PlainMutations) AppendMutation(mutation PlainMutation)`
- `func (c *PlainMutations) GetFlags() []CommitterMutationFlags`
- `func (c *PlainMutations) GetKey(i int) []byte`
- `func (c *PlainMutations) GetKeys() [][]byte`
- `func (c *PlainMutations) GetOp(i int) kvrpcpb.Op`
- `func (c *PlainMutations) GetOps() []kvrpcpb.Op`
- `func (c *PlainMutations) GetValue(i int) []byte`
- `func (c *PlainMutations) GetValues() [][]byte`
- `func (c *PlainMutations) IsAssertExists(i int) bool`
- `func (c *PlainMutations) IsAssertNotExist(i int) bool`
- `func (c *PlainMutations) IsPessimisticLock(i int) bool`
- `func (c *PlainMutations) Len() int`
- `func (c *PlainMutations) MergeMutations(mutations PlainMutations)`
- `func (c *PlainMutations) NeedConstraintCheckInPrewrite(i int) bool`
- `func (c *PlainMutations) Push(op kvrpcpb.Op, key []byte, value []byte, isPessimisticLock, assertExist, assertNotExist, NeedConstraintCheckInPrewrite bool)`
- `func (c *PlainMutations) Slice(from, to int) CommitterMutations`
- `func (c CommitterProbe) BuildPrewriteRequest(regionID, regionConf, regionVersion uint64, mutations CommitterMutations, txnSize uint64) *tikvrpc.Request`
- `func (c CommitterProbe) CheckAsyncCommit() bool`
- `func (c CommitterProbe) Cleanup(ctx context.Context)`
- `func (c CommitterProbe) CleanupMutations(ctx context.Context) error`
- `func (c CommitterProbe) CleanupWithoutWait(ctx context.Context)`
- `func (c CommitterProbe) CloseTTLManager()`
- `func (c CommitterProbe) CommitMutations(ctx context.Context) error`
- `func (c CommitterProbe) Execute(ctx context.Context) error`
- `func (c CommitterProbe) GetCommitTS() uint64`
- `func (c CommitterProbe) GetForUpdateTS() uint64`
- `func (c CommitterProbe) GetLockTTL() uint64`
- `func (c CommitterProbe) GetMinCommitTS() uint64`
- `func (c CommitterProbe) GetMutations() CommitterMutations`
- `func (c CommitterProbe) GetOnePCCommitTS() uint64`
- `func (c CommitterProbe) GetPrimaryKey() []byte`
- `func (c CommitterProbe) GetStartTS() uint64`
- `func (c CommitterProbe) GetUndeterminedErr() error`
- `func (c CommitterProbe) InitKeysAndMutations() error`
- `func (c CommitterProbe) IsAsyncCommit() bool`
- `func (c CommitterProbe) IsNil() bool`
- `func (c CommitterProbe) IsOnePC() bool`
- `func (c CommitterProbe) IsTTLRunning() bool`
- `func (c CommitterProbe) IsTTLUninitialized() bool`
- `func (c CommitterProbe) MutationsOfKeys(keys [][]byte) CommitterMutations`
- `func (c CommitterProbe) PessimisticRollbackMutations(ctx context.Context, muts CommitterMutations) error`
- `func (c CommitterProbe) PrewriteAllMutations(ctx context.Context) error`
- `func (c CommitterProbe) PrewriteMutations(ctx context.Context, mutations CommitterMutations) error`
- `func (c CommitterProbe) ResolveFlushedLocks(bo *retry.Backoffer, start, end []byte, commit bool)`
- `func (c CommitterProbe) SetCommitTS(ts uint64)`
- `func (c CommitterProbe) SetForUpdateTS(ts uint64)`
- `func (c CommitterProbe) SetLockTTL(ttl uint64)`
- `func (c CommitterProbe) SetLockTTLByTimeAndSize(start time.Time, size int)`
- `func (c CommitterProbe) SetMaxCommitTS(ts uint64)`
- `func (c CommitterProbe) SetMinCommitTS(ts uint64)`
- `func (c CommitterProbe) SetMutations(muts CommitterMutations)`
- `func (c CommitterProbe) SetNoFallBack()`
- `func (c CommitterProbe) SetPrimaryKey(key []byte)`
- `func (c CommitterProbe) SetPrimaryKeyBlocker(ac, bk chan struct{})`
- `func (c CommitterProbe) SetSessionID(id uint64)`
- `func (c CommitterProbe) SetTxnSize(sz int)`
- `func (c CommitterProbe) SetUseAsyncCommit()`
- `func (c CommitterProbe) WaitCleanup()`
- `func (c ConfigProbe) GetDefaultLockTTL() uint64`
- `func (c ConfigProbe) GetPessimisticLockMaxBackoff() int`
- `func (c ConfigProbe) GetTTLFactor() int`
- `func (c ConfigProbe) GetTxnCommitBatchSize() uint64`
- `func (c ConfigProbe) LoadPreSplitDetectThreshold() uint32`
- `func (c ConfigProbe) LoadPreSplitSizeThreshold() uint32`
- `func (c ConfigProbe) StorePreSplitDetectThreshold(v uint32)`
- `func (c ConfigProbe) StorePreSplitSizeThreshold(v uint32)`
- `func (i *AggressiveLockedKeyInfo) ActualLockForUpdateTS() uint64`
- `func (i *AggressiveLockedKeyInfo) HasCheckExistence() bool`
- `func (i *AggressiveLockedKeyInfo) HasReturnValues() bool`
- `func (i *AggressiveLockedKeyInfo) Key() []byte`
- `func (i *AggressiveLockedKeyInfo) Value() kv.ReturnedValue`
- `func (p PrewriteEncounterLockPolicy) String() string`
- `func (txn *KVTxn) AddRPCInterceptor(it interceptor.RPCInterceptor)`
- `func (txn *KVTxn) BatchGet(ctx context.Context, keys [][]byte, options ...tikv.BatchGetOption) (map[string]tikv.ValueEntry, error)`
- `func (txn *KVTxn) CancelAggressiveLocking(ctx context.Context)`
- `func (txn *KVTxn) ClearDiskFullOpt()`
- `func (txn *KVTxn) Commit(ctx context.Context) error`
- `func (txn *KVTxn) CommitTS() uint64`
- `func (txn *KVTxn) Delete(k []byte) error`
- `func (txn *KVTxn) DoneAggressiveLocking(ctx context.Context)`
- `func (txn *KVTxn) EnableForceSyncLog()`
- `func (txn *KVTxn) Get(ctx context.Context, k []byte, options ...tikv.GetOption) (tikv.ValueEntry, error)`
- `func (txn *KVTxn) GetClusterID() uint64`
- `func (txn *KVTxn) GetCommitWaitUntilTSO() uint64`
- `func (txn *KVTxn) GetCommitWaitUntilTSOTimeout() time.Duration`
- `func (txn *KVTxn) GetDiskFullOpt() kvrpcpb.DiskFullOpt`
- `func (txn *KVTxn) GetMemBuffer() unionstore.MemBuffer`
- `func (txn *KVTxn) GetScope() string`
- `func (txn *KVTxn) GetSnapshot() *txnsnapshot.KVSnapshot`
- `func (txn *KVTxn) GetTimestampForCommit(bo *retry.Backoffer, scope string) (_ uint64, err error)`
- `func (txn *KVTxn) GetUnionStore() *unionstore.KVUnionStore`
- `func (txn *KVTxn) GetVars() *tikv.Variables`
- `func (txn *KVTxn) InitPipelinedMemDB() error`
- `func (txn *KVTxn) IsCasualConsistency() bool`
- `func (txn *KVTxn) IsInAggressiveLockingMode() bool`
- `func (txn *KVTxn) IsInAggressiveLockingStage(key []byte) bool`
- `func (txn *KVTxn) IsPessimistic() bool`
- `func (txn *KVTxn) IsPipelined() bool`
- `func (txn *KVTxn) IsReadOnly() bool`
- `func (txn *KVTxn) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error)`
- `func (txn *KVTxn) IterReverse(k, lowerBound []byte) (unionstore.Iterator, error)`
- `func (txn *KVTxn) Len() int`
- `func (txn *KVTxn) LockKeys(ctx context.Context, lockCtx *tikv.LockCtx, keysInput ...[]byte) error`
- `func (txn *KVTxn) LockKeysFunc(ctx context.Context, lockCtx *tikv.LockCtx, fn func(), keysInput ...[]byte) error`
- `func (txn *KVTxn) LockKeysWithWaitTime(ctx context.Context, lockWaitTime int64, keysInput ...[]byte) (err error)`
- `func (txn *KVTxn) Mem() uint64`
- `func (txn *KVTxn) MemHookSet() bool`
- `func (txn *KVTxn) RetryAggressiveLocking(ctx context.Context)`
- `func (txn *KVTxn) Rollback() error`
- `func (txn *KVTxn) Set(k []byte, v []byte) error`
- `func (txn *KVTxn) SetAssertionLevel(assertionLevel kvrpcpb.AssertionLevel)`
- `func (txn *KVTxn) SetBackgroundGoroutineLifecycleHooks(hooks LifecycleHooks)`
- `func (txn *KVTxn) SetBinlogExecutor(binlog BinlogExecutor)`
- `func (txn *KVTxn) SetCausalConsistency(b bool)`
- `func (txn *KVTxn) SetCommitCallback(f func(string, error))`
- `func (txn *KVTxn) SetCommitTSUpperBoundCheck(f func(commitTS uint64) bool)`
- `func (txn *KVTxn) SetCommitWaitUntilTSO(commitWaitUntilTSO uint64)`
- `func (txn *KVTxn) SetCommitWaitUntilTSOTimeout(val time.Duration)`
- `func (txn *KVTxn) SetDiskFullOpt(level kvrpcpb.DiskFullOpt)`
- `func (txn *KVTxn) SetEnable1PC(b bool)`
- `func (txn *KVTxn) SetEnableAsyncCommit(b bool)`
- `func (txn *KVTxn) SetExplicitRequestSourceType(tp string)`
- `func (txn *KVTxn) SetKVFilter(filter KVFilter)`
- `func (txn *KVTxn) SetMemoryFootprintChangeHook(hook func(uint64))`
- `func (txn *KVTxn) SetPessimistic(b bool)`
- `func (txn *KVTxn) SetPrewriteEncounterLockPolicy(policy PrewriteEncounterLockPolicy)`
- `func (txn *KVTxn) SetPriority(pri txnutil.Priority)`
- `func (txn *KVTxn) SetRPCInterceptor(it interceptor.RPCInterceptor)`
- `func (txn *KVTxn) SetRequestSourceInternal(internal bool)`
- `func (txn *KVTxn) SetRequestSourceType(tp string)`
- `func (txn *KVTxn) SetResourceGroupName(name string)`
- `func (txn *KVTxn) SetResourceGroupTag(tag []byte)`
- `func (txn *KVTxn) SetResourceGroupTagger(tagger tikvrpc.ResourceGroupTagger)`
- `func (txn *KVTxn) SetSchemaLeaseChecker(checker SchemaLeaseChecker)`
- `func (txn *KVTxn) SetSchemaVer(schemaVer SchemaVer)`
- `func (txn *KVTxn) SetScope(scope string)`
- `func (txn *KVTxn) SetSessionID(sessionID uint64)`
- `func (txn *KVTxn) SetTxnSource(txnSource uint64)`
- `func (txn *KVTxn) SetVars(vars *tikv.Variables)`
- `func (txn *KVTxn) Size() int`
- `func (txn *KVTxn) StartAggressiveLocking()`
- `func (txn *KVTxn) StartTS() uint64`
- `func (txn *KVTxn) String() string`
- `func (txn *KVTxn) Valid() bool`
- `func (txn TxnProbe) BatchGetSingleRegion(bo *retry.Backoffer, region locate.RegionVerID, keys [][]byte, collect func([]byte, []byte)) error`
- `func (txn TxnProbe) CollectLockedKeys() [][]byte`
- `func (txn TxnProbe) GetAggressiveLockingKeys() []string`
- `func (txn TxnProbe) GetAggressiveLockingKeysInfo() []AggressiveLockedKeyInfo`
- `func (txn TxnProbe) GetAggressiveLockingPreviousKeys() []string`
- `func (txn TxnProbe) GetAggressiveLockingPreviousKeysInfo() []AggressiveLockedKeyInfo`
- `func (txn TxnProbe) GetCommitter() CommitterProbe`
- `func (txn TxnProbe) GetLockedCount() int`
- `func (txn TxnProbe) GetStartTime() time.Time`
- `func (txn TxnProbe) GetUnionStore() *unionstore.KVUnionStore`
- `func (txn TxnProbe) IsAsyncCommit() bool`
- `func (txn TxnProbe) NewCommitter(sessionID uint64) (CommitterProbe, error)`
- `func (txn TxnProbe) NewScanner(start, end []byte, batchSize int, reverse bool) (*txnsnapshot.Scanner, error)`
- `func (txn TxnProbe) SetCommitter(committer CommitterProbe)`
- `func (txn TxnProbe) SetStartTS(ts uint64)`

## txnkv/txnlock (package txnlock)

### Types
- `type Lock struct`
- `type LockProbe struct`
- `type LockResolver struct`
- `type LockResolverProbe struct`
- `type ResolveLockResult struct`
- `type ResolveLocksOptions struct`
- `type ResolvingLock struct`
- `type TxnStatus struct`

### Functions
- `func ExtractLockFromKeyErr(keyErr *kvrpcpb.KeyError) (*Lock, error)`
- `func NewLock(l *kvrpcpb.LockInfo) *Lock`
- `func NewLockResolver(store storage) *LockResolver`

### Consts
- `ResolvedCacheSize`

### Vars
- (none)

### Methods
- `func (l *Lock) String() string`
- `func (l LockProbe) GetPrimaryKeyFromTxnStatus(s TxnStatus) []byte`
- `func (l LockProbe) NewLockStatus(keys [][]byte, useAsyncCommit bool, minCommitTS uint64) TxnStatus`
- `func (l LockResolverProbe) CheckAllSecondaries(bo *retry.Backoffer, lock *Lock, status *TxnStatus) error`
- `func (l LockResolverProbe) GetSecondariesFromTxnStatus(status TxnStatus) [][]byte`
- `func (l LockResolverProbe) GetTxnStatus(bo *retry.Backoffer, txnID uint64, primary []byte, callerStartTS, currentTS uint64, rollbackIfNotExist bool, forceSyncCommit bool, lockInfo *Lock) (TxnStatus, error)`
- `func (l LockResolverProbe) GetTxnStatusFromLock(bo *retry.Backoffer, lock *Lock, callerStartTS uint64, forceSyncCommit bool) (TxnStatus, error)`
- `func (l LockResolverProbe) IsErrorNotFound(err error) bool`
- `func (l LockResolverProbe) IsNonAsyncCommitLock(err error) bool`
- `func (l LockResolverProbe) ResolveAsyncCommitLock(bo *retry.Backoffer, lock *Lock, status TxnStatus) error`
- `func (l LockResolverProbe) ResolveLock(bo *retry.Backoffer, lock *Lock) error`
- `func (l LockResolverProbe) ResolvePessimisticLock(bo *retry.Backoffer, lock *Lock) error`
- `func (l LockResolverProbe) SetMeetLockCallback(f func([]*Lock))`
- `func (l LockResolverProbe) SetResolving(currentStartTS uint64, locks []Lock)`
- `func (lr *LockResolver) BatchResolveLocks(bo *retry.Backoffer, locks []*Lock, loc locate.RegionVerID) (bool, error)`
- `func (lr *LockResolver) Close()`
- `func (lr *LockResolver) GetTxnStatus(txnID uint64, callerStartTS uint64, primary []byte) (TxnStatus, error)`
- `func (lr *LockResolver) RecordResolvingLocks(locks []*Lock, callerStartTS uint64) int`
- `func (lr *LockResolver) ResolveLocks(bo *retry.Backoffer, callerStartTS uint64, locks []*Lock) (int64, error)`
- `func (lr *LockResolver) ResolveLocksDone(callerStartTS uint64, token int)`
- `func (lr *LockResolver) ResolveLocksForRead(bo *retry.Backoffer, callerStartTS uint64, locks []*Lock, lite bool) (int64, []uint64, []uint64, error)`
- `func (lr *LockResolver) ResolveLocksWithOpts(bo *retry.Backoffer, opts ResolveLocksOptions) (ResolveLockResult, error)`
- `func (lr *LockResolver) Resolving() []ResolvingLock`
- `func (lr *LockResolver) UpdateResolvingLocks(locks []*Lock, callerStartTS uint64, token int)`
- `func (s TxnStatus) Action() kvrpcpb.Action`
- `func (s TxnStatus) CommitTS() uint64`
- `func (s TxnStatus) HasSameDeterminedStatus(other TxnStatus) bool`
- `func (s TxnStatus) IsCommitted() bool`
- `func (s TxnStatus) IsRolledBack() bool`
- `func (s TxnStatus) IsStatusDetermined() bool`
- `func (s TxnStatus) StatusCacheable() bool`
- `func (s TxnStatus) String() string`
- `func (s TxnStatus) TTL() uint64`

## txnkv/txnsnapshot (package txnsnapshot)

### Types
- `type ClientHelper struct`
- `type ConfigProbe struct`
- `type IsoLevel kvrpcpb.IsolationLevel`
- `type KVSnapshot struct`
- `type ReplicaReadAdjuster func(int) (locate.StoreSelectorOption, kv.ReplicaReadType)`
- `type Scanner struct`
- `type SnapshotProbe struct`
- `type SnapshotRuntimeStats struct`

### Functions
- `func NewClientHelper(store kvstore, resolvedLocks *util.TSSet, committedLocks *util.TSSet, resolveLite bool) *ClientHelper`
- `func NewTiKVSnapshot(store kvstore, ts uint64, replicaReadSeed uint32) *KVSnapshot`

### Consts
- `BatchGetBufferTier`
- `BatchGetSnapshotTier`
- `DefaultScanBatchSize`
- `RC`
- `RCCheckTS`
- `SI`

### Vars
- (none)

### Methods
- `func (c ConfigProbe) GetGetMaxBackoff() int`
- `func (c ConfigProbe) GetScanBatchSize() int`
- `func (ch *ClientHelper) RecordResolvingLocks(locks []*txnlock.Lock, callerStartTS uint64) int`
- `func (ch *ClientHelper) ResolveLocks(bo *retry.Backoffer, callerStartTS uint64, locks []*txnlock.Lock) (int64, error)`
- `func (ch *ClientHelper) ResolveLocksDone(callerStartTS uint64, token int)`
- `func (ch *ClientHelper) ResolveLocksWithOpts(bo *retry.Backoffer, opts txnlock.ResolveLocksOptions) (txnlock.ResolveLockResult, error)`
- `func (ch *ClientHelper) SendReqAsync( bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, cb async.Callback[*tikvrpc.ResponseExt], opts ...locate.StoreSelectorOption, )`
- `func (ch *ClientHelper) SendReqCtx(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, et tikvrpc.EndpointType, directStoreAddr string, opts ...locate.StoreSelectorOption) (*tikvrpc.Response, *locate.RPCContext, string, error)`
- `func (ch *ClientHelper) UpdateResolvingLocks(locks []*txnlock.Lock, callerStartTS uint64, token int)`
- `func (l IsoLevel) ToPB() kvrpcpb.IsolationLevel`
- `func (rs *SnapshotRuntimeStats) Clone() *SnapshotRuntimeStats`
- `func (rs *SnapshotRuntimeStats) GetCmdRPCCount(cmd tikvrpc.CmdType) int64`
- `func (rs *SnapshotRuntimeStats) GetTimeDetail() *util.TimeDetail`
- `func (rs *SnapshotRuntimeStats) Merge(other *SnapshotRuntimeStats)`
- `func (rs *SnapshotRuntimeStats) String() string`
- `func (s *KVSnapshot) AddRPCInterceptor(it interceptor.RPCInterceptor)`
- `func (s *KVSnapshot) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error)`
- `func (s *KVSnapshot) BatchGetWithTier(ctx context.Context, keys [][]byte, readTier int, opt kv.BatchGetOptions) (m map[string]kv.ValueEntry, err error)`
- `func (s *KVSnapshot) CleanCache(keys [][]byte)`
- `func (s *KVSnapshot) Get(ctx context.Context, k []byte, options ...kv.GetOption) (entry kv.ValueEntry, err error)`
- `func (s *KVSnapshot) GetKVReadTimeout() time.Duration`
- `func (s *KVSnapshot) GetResolveLockDetail() *util.ResolveLockDetail`
- `func (s *KVSnapshot) IsInternal() bool`
- `func (s *KVSnapshot) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error)`
- `func (s *KVSnapshot) IterReverse(k, lowerBound []byte) (unionstore.Iterator, error)`
- `func (s *KVSnapshot) SetIsStalenessReadOnly(b bool)`
- `func (s *KVSnapshot) SetIsolationLevel(level IsoLevel)`
- `func (s *KVSnapshot) SetKVReadTimeout(readTimeout time.Duration)`
- `func (s *KVSnapshot) SetKeyOnly(b bool)`
- `func (s *KVSnapshot) SetLoadBasedReplicaReadThreshold(busyThreshold time.Duration)`
- `func (s *KVSnapshot) SetMatchStoreLabels(labels []*metapb.StoreLabel)`
- `func (s *KVSnapshot) SetNotFillCache(b bool)`
- `func (s *KVSnapshot) SetPipelined(ts uint64)`
- `func (s *KVSnapshot) SetPriority(pri txnutil.Priority)`
- `func (s *KVSnapshot) SetRPCInterceptor(it interceptor.RPCInterceptor)`
- `func (s *KVSnapshot) SetReadReplicaScope(scope string)`
- `func (s *KVSnapshot) SetReplicaRead(readType kv.ReplicaReadType)`
- `func (s *KVSnapshot) SetReplicaReadAdjuster(f ReplicaReadAdjuster)`
- `func (s *KVSnapshot) SetResourceGroupName(name string)`
- `func (s *KVSnapshot) SetResourceGroupTag(tag []byte)`
- `func (s *KVSnapshot) SetResourceGroupTagger(tagger tikvrpc.ResourceGroupTagger)`
- `func (s *KVSnapshot) SetRuntimeStats(stats *SnapshotRuntimeStats)`
- `func (s *KVSnapshot) SetSampleStep(step uint32)`
- `func (s *KVSnapshot) SetScanBatchSize(batchSize int)`
- `func (s *KVSnapshot) SetSnapshotTS(ts uint64)`
- `func (s *KVSnapshot) SetTaskID(id uint64)`
- `func (s *KVSnapshot) SetTxnScope(scope string)`
- `func (s *KVSnapshot) SetVars(vars *kv.Variables)`
- `func (s *KVSnapshot) SnapCache() map[string]kv.ValueEntry`
- `func (s *KVSnapshot) SnapCacheHitCount() int`
- `func (s *KVSnapshot) SnapCacheSize() int`
- `func (s *KVSnapshot) UpdateSnapshotCache(keys [][]byte, m map[string]kv.ValueEntry)`
- `func (s *Scanner) Close()`
- `func (s *Scanner) Key() []byte`
- `func (s *Scanner) Next() error`
- `func (s *Scanner) Valid() bool`
- `func (s *Scanner) Value() []byte`
- `func (s SnapshotProbe) BatchGetSingleRegion(bo *retry.Backoffer, region locate.RegionVerID, keys [][]byte, collectF func(k, v []byte)) error`
- `func (s SnapshotProbe) FormatStats() string`
- `func (s SnapshotProbe) MergeExecDetail(detail *kvrpcpb.ExecDetailsV2)`
- `func (s SnapshotProbe) MergeRegionRequestStats(rpcStats *locate.RegionRequestRuntimeStats)`
- `func (s SnapshotProbe) NewScanner(start, end []byte, batchSize int, reverse bool) (*Scanner, error)`
- `func (s SnapshotProbe) RecordBackoffInfo(bo *retry.Backoffer)`

## txnkv/txnutil (package txnutil)

### Types
- `type Priority kvrpcpb.CommandPri`

### Functions
- (none)

### Consts
- `PriorityHigh`
- `PriorityLow`
- `PriorityNormal`

### Vars
- (none)

### Methods
- `func (p Priority) ToPB() kvrpcpb.CommandPri`

## util (package util)

### Types
- `type CommitDetails struct`
- `type CommitTSLagDetails struct`
- `type ExecDetails struct`
- `type InterceptedPDClient struct`
- `type LockKeysDetails struct`
- `type Option struct`
- `type RUDetails struct`
- `type RateLimit struct`
- `type ReqDetailInfo struct`
- `type RequestSource struct`
- `type RequestSourceKeyType struct`
- `type RequestSourceTypeKeyType struct`
- `type ResolveLockDetail struct`
- `type ScanDetail struct`
- `type TSSet struct`
- `type TiKVExecDetails struct`
- `type TimeDetail struct`
- `type TrafficDetails struct`
- `type WriteDetail struct`

### Functions
- `func BuildRequestSource(internal bool, source, explicitSource string) string`
- `func BytesToString(numBytes int64) string`
- `func CompatibleParseGCTime(value string) (time.Time, error)`
- `func ContextWithTraceExecDetails(ctx context.Context) context.Context`
- `func EnableFailpoints()`
- `func EvalFailpoint(name string) (interface{}, error)`
- `func FormatBytes(numBytes int64) string`
- `func FormatDuration(d time.Duration) string`
- `func GetCustomDNSDialer(dnsServer, dnsDomain string) dnsF`
- `func IsInternalRequest(source string) bool`
- `func IsRequestSourceInternal(reqSrc *RequestSource) bool`
- `func NewInterceptedPDClient(client pd.Client) *InterceptedPDClient`
- `func NewRUDetails() *RUDetails`
- `func NewRUDetailsWith(rru, wru float64, waitDur time.Duration) *RUDetails`
- `func NewRateLimit(n int) *RateLimit`
- `func NewTiKVExecDetails(pb *kvrpcpb.ExecDetailsV2) TiKVExecDetails`
- `func None[T interface{}]() Option[T]`
- `func RequestSourceFromCtx(ctx context.Context) string`
- `func ResourceGroupNameFromCtx(ctx context.Context) string`
- `func SetSessionID(ctx context.Context, sessionID uint64) context.Context`
- `func Some[T interface{}](inner T) Option[T]`
- `func TraceExecDetailsEnabled(ctx context.Context) bool`
- `func WithInternalSourceAndTaskType(ctx context.Context, source, taskName string) context.Context`
- `func WithInternalSourceType(ctx context.Context, source string) context.Context`
- `func WithRecovery(exec func(), recoverFn func(r interface{}))`
- `func WithResourceGroupName(ctx context.Context, groupName string) context.Context`

### Consts
- `ExplicitTypeBR`
- `ExplicitTypeBackground`
- `ExplicitTypeDDL`
- `ExplicitTypeDumpling`
- `ExplicitTypeEmpty`
- `ExplicitTypeLightning`
- `ExplicitTypeStats`
- `ExternalRequest`
- `GCTimeFormat`
- `InternalRequest`
- `InternalRequestPrefix`
- `InternalTxnGC`
- `InternalTxnMeta`
- `InternalTxnOthers`
- `InternalTxnStats`
- `SourceUnknown`

### Vars
- `CommitDetailCtxKey`
- `ExecDetailsKey`
- `ExplicitTypeList`
- `LockKeysDetailCtxKey`
- `RUDetailsCtxKey`
- `RequestSourceKey`
- `RequestSourceTypeKey`
- `SessionID`

### Methods
- `func (cd *CommitDetails) Clone() *CommitDetails`
- `func (cd *CommitDetails) Merge(other *CommitDetails)`
- `func (cd *CommitDetails) MergeCommitReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)`
- `func (cd *CommitDetails) MergeFlushReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)`
- `func (cd *CommitDetails) MergePrewriteReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)`
- `func (d *CommitTSLagDetails) Merge(other *CommitTSLagDetails)`
- `func (ed *TiKVExecDetails) String() string`
- `func (ld *LockKeysDetails) Clone() *LockKeysDetails`
- `func (ld *LockKeysDetails) Merge(lockKey *LockKeysDetails)`
- `func (ld *LockKeysDetails) MergeReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2)`
- `func (m InterceptedPDClient) GetPrevRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error)`
- `func (m InterceptedPDClient) GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error)`
- `func (m InterceptedPDClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...opt.GetRegionOption) (*router.Region, error)`
- `func (m InterceptedPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)`
- `func (m InterceptedPDClient) GetTS(ctx context.Context) (int64, int64, error)`
- `func (m InterceptedPDClient) GetTSAsync(ctx context.Context) tso.TSFuture`
- `func (m InterceptedPDClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...opt.GetRegionOption) ([]*router.Region, error)`
- `func (m InterceptedPDClient) WithCallerComponent(component caller.Component) pd.Client`
- `func (o Option[T]) Inner() *T`
- `func (r *RateLimit) GetCapacity() int`
- `func (r *RateLimit) GetToken(done <-chan struct{}) (exit bool)`
- `func (r *RateLimit) PutToken()`
- `func (r *RequestSource) GetRequestSource() string`
- `func (r *RequestSource) SetExplicitRequestSourceType(tp string)`
- `func (r *RequestSource) SetRequestSourceInternal(internal bool)`
- `func (r *RequestSource) SetRequestSourceType(tp string)`
- `func (rd *RUDetails) Clone() *RUDetails`
- `func (rd *RUDetails) Merge(other *RUDetails)`
- `func (rd *RUDetails) RRU() float64`
- `func (rd *RUDetails) RUWaitDuration() time.Duration`
- `func (rd *RUDetails) String() string`
- `func (rd *RUDetails) Update(consumption *rmpb.Consumption, waitDuration time.Duration)`
- `func (rd *RUDetails) WRU() float64`
- `func (rd *ResolveLockDetail) Merge(resolveLock *ResolveLockDetail)`
- `func (s *TSSet) GetAll() []uint64`
- `func (s *TSSet) Put(tss ...uint64)`
- `func (sd *ScanDetail) Merge(scanDetail *ScanDetail)`
- `func (sd *ScanDetail) MergeFromScanDetailV2(scanDetail *kvrpcpb.ScanDetailV2)`
- `func (sd *ScanDetail) String() string`
- `func (td *TimeDetail) Merge(detail *TimeDetail)`
- `func (td *TimeDetail) MergeFromTimeDetail(timeDetailV2 *kvrpcpb.TimeDetailV2, timeDetail *kvrpcpb.TimeDetail)`
- `func (td *TimeDetail) String() string`
- `func (wd *WriteDetail) Merge(writeDetail *WriteDetail)`
- `func (wd *WriteDetail) MergeFromWriteDetailPb(pb *kvrpcpb.WriteDetail)`
- `func (wd *WriteDetail) String() string`

## util/async (package async)

### Types
- `type Callback interface`
- `type Executor interface`
- `type Pool interface`
- `type RunLoop struct`
- `type State uint32`

### Functions
- `func NewCallback[T any](e Executor, f func(T, error)) Callback[T]`
- `func NewRunLoop() *RunLoop`

### Consts
- `StateIdle`
- `StateRunning`
- `StateWaiting`

### Vars
- (none)

### Methods
- `func (l *RunLoop) Append(fs ...func())`
- `func (l *RunLoop) Exec(ctx context.Context) (int, error)`
- `func (l *RunLoop) Go(f func())`
- `func (l *RunLoop) NumRunnable() int`
- `func (l *RunLoop) State() State`

## util/codec (package codec)

### Types
- (none)

### Functions
- `func DecodeBytes(b []byte, buf []byte) ([]byte, []byte, error)`
- `func DecodeCmpUintToInt(u uint64) int64`
- `func DecodeComparableUvarint(b []byte) ([]byte, uint64, error)`
- `func DecodeComparableVarint(b []byte) ([]byte, int64, error)`
- `func DecodeInt(b []byte) ([]byte, int64, error)`
- `func DecodeIntDesc(b []byte) ([]byte, int64, error)`
- `func DecodeUint(b []byte) ([]byte, uint64, error)`
- `func DecodeUintDesc(b []byte) ([]byte, uint64, error)`
- `func DecodeUvarint(b []byte) ([]byte, uint64, error)`
- `func DecodeVarint(b []byte) ([]byte, int64, error)`
- `func EncodeBytes(b []byte, data []byte) []byte`
- `func EncodeComparableUvarint(b []byte, v uint64) []byte`
- `func EncodeComparableVarint(b []byte, v int64) []byte`
- `func EncodeInt(b []byte, v int64) []byte`
- `func EncodeIntDesc(b []byte, v int64) []byte`
- `func EncodeIntToCmpUint(v int64) uint64`
- `func EncodeUint(b []byte, v uint64) []byte`
- `func EncodeUintDesc(b []byte, v uint64) []byte`
- `func EncodeUvarint(b []byte, v uint64) []byte`
- `func EncodeVarint(b []byte, v int64) []byte`

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
- `InTest`

### Methods
- (none)

## util/israce (package israce)

### Types
- (none)

### Functions
- (none)

### Consts
- `RaceEnabled`

### Vars
- (none)

### Methods
- (none)

## util/redact (package redact)

### Types
- (none)

### Functions
- `func Key(key []byte) string`
- `func KeyBytes(key []byte) []byte`
- `func NeedRedact() bool`
- `func RedactKeyErrIfNecessary(err *kvrpcpb.KeyError)`
- `func String(b []byte) (s string)`

### Consts
- (none)

### Vars
- (none)

### Methods
- (none)
