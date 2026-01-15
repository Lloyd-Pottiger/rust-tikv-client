# client-go v2 API inventory (auto-generated)

Generated from local source under `client-go/` (excluding `internal/`, `integration_tests/`, `examples/`, `testutils/`).
This is a name-only inventory (no signatures). It is used to drive parity work in `new-client-rust/`.

## config (package config)

### Types
- `AsyncCommit`
- `Config`
- `CoprocessorCache`
- `PDClient`
- `PessimisticTxn`
- `Security`
- `TiKVClient`
- `TxnLocalLatches`

### Functions
- `DefaultConfig`
- `DefaultPDClient`
- `DefaultTiKVClient`
- `DefaultTxnLocalLatches`
- `GetGlobalConfig`
- `GetTxnScopeFromConfig`
- `NewSecurity`
- `ParsePath`
- `StoreGlobalConfig`
- `UpdateGlobal`

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

### Exported Methods (receiver elided)
- `GetGrpcKeepAliveTimeout`
- `ToTLSConfig`
- `Valid`

## config/retry (package retry)

### Types
- `BackoffFnCfg`
- `Backoffer`
- `Config`

### Functions
- `IsFakeRegionError`
- `MayBackoffForRegionError`
- `NewBackoffFnCfg`
- `NewBackoffer`
- `NewBackofferWithVars`
- `NewConfig`
- `NewNoopBackoff`

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

### Exported Methods (receiver elided)
- `Backoff`
- `BackoffWithCfgAndMaxSleep`
- `BackoffWithMaxSleepTxnLockFast`
- `Base`
- `CheckKilled`
- `Clone`
- `ErrorsNum`
- `Fork`
- `GetBackoffSleepMS`
- `GetBackoffTimes`
- `GetCtx`
- `GetTotalBackoffTimes`
- `GetTotalSleep`
- `GetTypes`
- `GetVars`
- `Reset`
- `ResetMaxSleep`
- `SetBackoffFnCfg`
- `SetCtx`
- `SetErrors`
- `String`
- `UpdateUsingForked`

## error (package error)

### Types
- `ErrAssertionFailed`
- `ErrDeadlock`
- `ErrEntryTooLarge`
- `ErrGCTooEarly`
- `ErrKeyExist`
- `ErrKeyTooLarge`
- `ErrLockOnlyIfExistsNoPrimaryKey`
- `ErrLockOnlyIfExistsNoReturnValue`
- `ErrPDServerTimeout`
- `ErrQueryInterruptedWithSignal`
- `ErrRetryable`
- `ErrTokenLimit`
- `ErrTxnAbortedByGC`
- `ErrTxnTooLarge`
- `ErrWriteConflict`
- `ErrWriteConflictInLatch`
- `PDError`

### Functions
- `ExtractDebugInfoStrFromKeyErr`
- `ExtractKeyErr`
- `IsErrKeyExist`
- `IsErrNotFound`
- `IsErrWriteConflict`
- `IsErrorCommitTSLag`
- `IsErrorUndetermined`
- `Log`
- `NewErrPDServerTimeout`
- `NewErrWriteConflict`
- `NewErrWriteConflictWithArgs`

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

### Exported Methods (receiver elided)
- `Error`

## kv (package kv)

### Types
- `AccessLocationType`
- `BatchGetOption`
- `BatchGetOptions`
- `BatchGetter`
- `FlagsOp`
- `GetOption`
- `GetOptions`
- `GetOrBatchGetOption`
- `Getter`
- `KeyFlags`
- `KeyRange`
- `LockCtx`
- `ReplicaReadType`
- `ReturnedValue`
- `ValueEntry`
- `Variables`

### Functions
- `ApplyFlagsOps`
- `BatchGetToGetOptions`
- `CmpKey`
- `NewLockCtx`
- `NewValueEntry`
- `NewVariables`
- `NextKey`
- `PrefixNextKey`
- `WithReturnCommitTS`

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

### Exported Methods (receiver elided)
- `AndPersistent`
- `Apply`
- `GetValueNotLocked`
- `HasAssertExist`
- `HasAssertNotExist`
- `HasAssertUnknown`
- `HasAssertionFlags`
- `HasIgnoredIn2PC`
- `HasLocked`
- `HasLockedValueExists`
- `HasNeedCheckExists`
- `HasNeedConstraintCheckInPrewrite`
- `HasNeedLocked`
- `HasNewlyInserted`
- `HasPresumeKeyNotExists`
- `HasPrewriteOnly`
- `HasReadable`
- `InitCheckExistence`
- `InitReturnValues`
- `IsFollowerRead`
- `IsValueEmpty`
- `IterateValuesNotLocked`
- `LockWaitTime`
- `ReturnCommitTS`
- `Size`
- `String`

## metrics (package metrics)

### Types
- `LabelPair`
- `MetricVec`
- `TxnCommitCounter`

### Functions
- `FindNextStaleStoreID`
- `GetStoreMetricVecList`
- `GetTxnCommitCounter`
- `InitMetrics`
- `InitMetricsWithConstLabels`
- `ObserveReadSLI`
- `RegisterMetrics`

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

### Exported Methods (receiver elided)
- `Sub`

## oracle (package oracle)

### Types
- `ErrFutureTSRead`
- `ErrLatestStaleRead`
- `Future`
- `NoopReadTSValidator`
- `Option`
- `Oracle`
- `ReadTSValidator`

### Functions
- `ComposeTS`
- `ExtractLogical`
- `ExtractPhysical`
- `GetPhysical`
- `GetTimeFromTS`
- `GoTimeToLowerLimitStartTS`
- `GoTimeToTS`

### Consts
- `GlobalTxnScope`

### Vars
- (none)

### Exported Methods (receiver elided)
- `Error`
- `ValidateReadTS`

## oracle/oracles (package oracles)

### Types
- `MockOracle`
- `PDOracleOptions`
- `ValidateReadTSForTidbSnapshot`

### Functions
- `NewLocalOracle`
- `NewPdOracle`

### Consts
- (none)

### Vars
- `EnableTSValidation`

### Exported Methods (receiver elided)
- `AddOffset`
- `Close`
- `Disable`
- `Enable`
- `GetAllTSOKeyspaceGroupMinTS`
- `GetExternalTimestamp`
- `GetLowResolutionTimestamp`
- `GetLowResolutionTimestampAsync`
- `GetStaleTimestamp`
- `GetTimestamp`
- `GetTimestampAsync`
- `IsExpired`
- `SetExternalTimestamp`
- `SetLowResolutionTimestampUpdateInterval`
- `String`
- `UntilExpired`
- `ValidateReadTS`
- `Wait`

## rawkv (package rawkv)

### Types
- `Client`
- `ClientOpt`
- `ClientProbe`
- `ConfigProbe`
- `RawChecksum`
- `RawOption`

### Functions
- `NewClient`
- `NewClientWithOpts`
- `ScanKeyOnly`
- `SetColumnFamily`
- `WithAPIVersion`
- `WithGRPCDialOptions`
- `WithKeyspace`
- `WithPDOptions`
- `WithSecurity`

### Consts
- (none)

### Vars
- `ErrMaxScanLimitExceeded`
- `MaxRawKVScanLimit`

### Exported Methods (receiver elided)
- `BatchDelete`
- `BatchGet`
- `BatchPut`
- `BatchPutWithTTL`
- `Checksum`
- `Close`
- `ClusterID`
- `CompareAndSwap`
- `Delete`
- `DeleteRange`
- `Get`
- `GetKeyTTL`
- `GetPDClient`
- `GetRawBatchPutSize`
- `GetRegionCache`
- `Put`
- `PutWithTTL`
- `ReverseScan`
- `Scan`
- `SetAtomicForCAS`
- `SetColumnFamily`
- `SetPDClient`
- `SetRPCClient`
- `SetRegionCache`

## tikv (package tikv)

### Types
- `BackoffConfig`
- `Backoffer`
- `BaseRegionLockResolver`
- `BatchLocateKeyRangesOpt`
- `BinlogWriteResult`
- `Client`
- `ClientEventListener`
- `ClientOpt`
- `Codec`
- `CodecClient`
- `CodecPDClient`
- `ConfigProbe`
- `EtcdSafePointKV`
- `GCOpt`
- `Getter`
- `Iterator`
- `KVFilter`
- `KVStore`
- `KVTxn`
- `KeyLocation`
- `KeyRange`
- `KeyspaceID`
- `LabelFilter`
- `LockResolverProbe`
- `MemBuffer`
- `MemBufferSnapshot`
- `MemDB`
- `MemDBCheckpoint`
- `Metrics`
- `MockSafePointKV`
- `Mode`
- `Option`
- `Pool`
- `RPCCanceller`
- `RPCCancellerCtxKey`
- `RPCContext`
- `RPCRuntimeStats`
- `Region`
- `RegionCache`
- `RegionLockResolver`
- `RegionRequestRuntimeStats`
- `RegionRequestSender`
- `RegionVerID`
- `SafePointKV`
- `SafePointKVOpt`
- `SchemaLeaseChecker`
- `SchemaVer`
- `Spool`
- `Storage`
- `Store`
- `StoreProbe`
- `StoreSelectorOption`
- `TxnOption`
- `Variables`

### Functions
- `BoPDRPC`
- `BoRegionMiss`
- `BoTiFlashRPC`
- `BoTiKVRPC`
- `BoTxnLock`
- `ChangePDRegionMetaCircuitBreakerSettings`
- `CodecV1ExcludePrefixes`
- `CodecV2Prefixes`
- `DisableResourceControl`
- `EnableResourceControl`
- `GetStoreTypeByMeta`
- `LoadShuttingDown`
- `NewBackoffer`
- `NewBackofferWithVars`
- `NewEtcdSafePointKV`
- `NewGcResolveLockMaxBackoffer`
- `NewKVStore`
- `NewLockResolver`
- `NewLockResolverProb`
- `NewMockSafePointKV`
- `NewPDClient`
- `NewRPCClient`
- `NewRPCanceller`
- `NewRegionCache`
- `NewRegionLockResolver`
- `NewRegionRequestRuntimeStats`
- `NewRegionRequestSender`
- `NewRegionVerID`
- `NewSpool`
- `NewTestKeyspaceTiKVStore`
- `NewTestTiKVStore`
- `ResolveLocksForRange`
- `SetLogContextKey`
- `SetRegionCacheTTLSec`
- `SetRegionCacheTTLWithJitter`
- `SetResourceControlInterceptor`
- `SetStoreLivenessTimeout`
- `StoreShuttingDown`
- `TxnStartKey`
- `UnsetResourceControlInterceptor`
- `WithCodec`
- `WithConcurrency`
- `WithDefaultPipelinedTxn`
- `WithLogContext`
- `WithMatchLabels`
- `WithMatchStores`
- `WithPDHTTPClient`
- `WithPipelinedTxn`
- `WithPool`
- `WithPrefix`
- `WithSecurity`
- `WithStartTS`
- `WithTxnScope`
- `WithUpdateInterval`

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

### Exported Methods (receiver elided)
- `Begin`
- `CheckRegionInScattering`
- `CheckVisibility`
- `ClearTxnLatches`
- `Close`
- `Closed`
- `Ctx`
- `CurrentAllTSOKeyspaceGroupMinTs`
- `CurrentTimestamp`
- `DeleteRange`
- `EnableTxnLocalLatches`
- `ForceResolveLock`
- `GC`
- `GCResolveLockPhase`
- `Get`
- `GetBigTxnThreshold`
- `GetClusterID`
- `GetCompatibleTxnSafePointLoaderUnderlyingEtcdClient`
- `GetDefaultLockTTL`
- `GetGCStatesClient`
- `GetGetMaxBackoff`
- `GetLockResolver`
- `GetMinSafeTS`
- `GetOracle`
- `GetPDClient`
- `GetPDHTTPClient`
- `GetRegionCache`
- `GetSafePointKV`
- `GetScanBatchSize`
- `GetSnapshot`
- `GetStore`
- `GetTTLFactor`
- `GetTiKVClient`
- `GetTimestampWithRetry`
- `GetTxnCommitBatchSize`
- `GetWithPrefix`
- `Go`
- `Identifier`
- `IsClose`
- `IsLatchEnabled`
- `LoadPreSplitDetectThreshold`
- `LoadPreSplitSizeThreshold`
- `LoadSafePointFromSafePointKV`
- `LoadTxnSafePoint`
- `NewLockResolver`
- `Put`
- `ReplaceGCStatesClient`
- `ResolveLock`
- `ResolveLocksInOneRegion`
- `ResolvePessimisticLock`
- `Run`
- `SaveSafePointToSafePointKV`
- `ScanLocks`
- `ScanLocksInOneRegion`
- `SendReq`
- `SendRequest`
- `SendRequestAsync`
- `SendTxnHeartbeat`
- `SetOracle`
- `SetOracleUpdateInterval`
- `SetRegionCachePDClient`
- `SetRegionCacheStore`
- `SetSafeTS`
- `SetTiKVClient`
- `SplitRegions`
- `StorePreSplitDetectThreshold`
- `StorePreSplitSizeThreshold`
- `SupportDeleteRange`
- `TxnLatches`
- `UUID`
- `UnsafeDestroyRange`
- `UpdateTxnSafePointCache`
- `WaitGroup`
- `WaitScatterRegionFinish`

## tikvrpc (package tikvrpc)

### Types
- `BatchCopStreamResponse`
- `CmdType`
- `CopStreamResponse`
- `EndpointType`
- `Lease`
- `MPPStreamResponse`
- `Request`
- `ResourceGroupTagger`
- `Response`
- `ResponseExt`

### Functions
- `AttachContext`
- `CallDebugRPC`
- `CallRPC`
- `CheckStreamTimeoutLoop`
- `FromBatchCommandsResponse`
- `GenRegionErrorResp`
- `GetStoreTypeByMeta`
- `NewReplicaReadRequest`
- `NewRequest`
- `SetContext`
- `SetContextNoAttach`

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

### Exported Methods (receiver elided)
- `BatchCop`
- `BatchGet`
- `BatchRollback`
- `BroadcastTxnStatus`
- `BufferBatchGet`
- `CancelMPPTask`
- `CheckLockObserver`
- `CheckSecondaryLocks`
- `CheckTxnStatus`
- `Cleanup`
- `Close`
- `Commit`
- `Compact`
- `Cop`
- `DebugGetRegionProperties`
- `DeleteRange`
- `DisableStaleReadMeetLock`
- `DispatchMPPTask`
- `Empty`
- `EnableStaleWithMixedReplicaRead`
- `EstablishMPPConn`
- `FlashbackToVersion`
- `Flush`
- `GC`
- `Get`
- `GetExecDetailsV2`
- `GetHealthFeedback`
- `GetRegionError`
- `GetReplicaReadSeed`
- `GetSize`
- `GetStartTS`
- `GetTiFlashSystemTable`
- `IsDebugReq`
- `IsGlobalStaleRead`
- `IsGreenGCRequest`
- `IsInterruptible`
- `IsMPPAlive`
- `IsRawWriteRequest`
- `IsTiFlashRelatedType`
- `IsTxnWriteRequest`
- `LockWaitInfo`
- `MvccGetByKey`
- `MvccGetByStartTs`
- `Name`
- `PessimisticLock`
- `PessimisticRollback`
- `PhysicalScanLock`
- `PrepareFlashbackToVersion`
- `Prewrite`
- `RawBatchDelete`
- `RawBatchGet`
- `RawBatchPut`
- `RawChecksum`
- `RawCompareAndSwap`
- `RawDelete`
- `RawDeleteRange`
- `RawGet`
- `RawGetKeyTTL`
- `RawPut`
- `RawScan`
- `Recv`
- `RegisterLockObserver`
- `RemoveLockObserver`
- `ResolveLock`
- `Scan`
- `ScanLock`
- `SetReplicaReadType`
- `SplitRegion`
- `StoreSafeTS`
- `String`
- `ToBatchCommandsRequest`
- `TxnHeartBeat`
- `UnsafeDestroyRange`

## tikvrpc/interceptor (package interceptor)

### Types
- `MockInterceptorManager`
- `RPCInterceptor`
- `RPCInterceptorChain`
- `RPCInterceptorFunc`

### Functions
- `ChainRPCInterceptors`
- `GetRPCInterceptorFromCtx`
- `NewMockInterceptorManager`
- `NewRPCInterceptor`
- `NewRPCInterceptorChain`
- `WithRPCInterceptor`

### Consts
- (none)

### Vars
- (none)

### Exported Methods (receiver elided)
- `BeginCount`
- `CreateMockInterceptor`
- `EndCount`
- `ExecLog`
- `Len`
- `Link`
- `Name`
- `Reset`
- `Wrap`

## trace (package trace)

### Types
- `Category`
- `IsCategoryEnabledFunc`
- `TraceControlExtractorFunc`
- `TraceControlFlags`
- `TraceEventFunc`

### Functions
- `ContextWithTraceID`
- `GetTraceControlFlags`
- `ImmediateLoggingEnabled`
- `IsCategoryEnabled`
- `SetIsCategoryEnabledFunc`
- `SetTraceControlExtractor`
- `SetTraceEventFunc`
- `TraceEvent`
- `TraceIDFromContext`

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

### Exported Methods (receiver elided)
- `Has`
- `With`

## txnkv (package txnkv)

### Types
- `BinlogWriteResult`
- `Client`
- `ClientOpt`
- `IsoLevel`
- `KVFilter`
- `KVSnapshot`
- `KVTxn`
- `Lock`
- `LockResolver`
- `Priority`
- `ReplicaReadAdjuster`
- `Scanner`
- `SchemaLeaseChecker`
- `SchemaVer`
- `SnapshotRuntimeStats`
- `TxnStatus`

### Functions
- `NewClient`
- `NewLock`
- `WithAPIVersion`
- `WithKeyspace`
- `WithSafePointKVPrefix`

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

### Exported Methods (receiver elided)
- `GetTimestamp`

## txnkv/rangetask (package rangetask)

### Types
- `DeleteRangeTask`
- `Runner`
- `TaskHandler`
- `TaskStat`

### Functions
- `NewDeleteRangeTask`
- `NewLocateRegionBackoffer`
- `NewNotifyDeleteRangeTask`
- `NewRangeTaskRunner`
- `NewRangeTaskRunnerWithID`

### Consts
- (none)

### Vars
- (none)

### Exported Methods (receiver elided)
- `CompletedRegions`
- `Execute`
- `FailedRegions`
- `RunOnRange`
- `SetRegionsPerTask`
- `SetStatLogInterval`

## txnkv/transaction (package transaction)

### Types
- `AggressiveLockedKeyInfo`
- `BatchBufferGetter`
- `BatchSnapshotBufferGetter`
- `BinlogExecutor`
- `BinlogWriteResult`
- `BufferBatchGetter`
- `BufferSnapshotBatchGetter`
- `CommitterMutationFlags`
- `CommitterMutations`
- `CommitterProbe`
- `ConfigProbe`
- `KVFilter`
- `KVTxn`
- `LifecycleHooks`
- `MemBufferMutationsProbe`
- `PipelinedTxnOptions`
- `PlainMutation`
- `PlainMutations`
- `PrewriteEncounterLockPolicy`
- `RelatedSchemaChange`
- `SchemaLeaseChecker`
- `SchemaVer`
- `TxnInfo`
- `TxnOptions`
- `TxnProbe`
- `WriteAccessLevel`

### Functions
- `NewBufferBatchGetter`
- `NewBufferSnapshotBatchGetter`
- `NewMemBufferMutationsProbe`
- `NewPlainMutations`
- `NewTiKVTxn`
- `SendTxnHeartBeat`

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

### Exported Methods (receiver elided)
- `ActualLockForUpdateTS`
- `AddRPCInterceptor`
- `AppendMutation`
- `BatchGet`
- `BatchGetSingleRegion`
- `BuildPrewriteRequest`
- `CancelAggressiveLocking`
- `CheckAsyncCommit`
- `Cleanup`
- `CleanupMutations`
- `CleanupWithoutWait`
- `ClearDiskFullOpt`
- `CloseTTLManager`
- `CollectLockedKeys`
- `Commit`
- `CommitMutations`
- `CommitTS`
- `Delete`
- `DoneAggressiveLocking`
- `EnableForceSyncLog`
- `Execute`
- `Get`
- `GetAggressiveLockingKeys`
- `GetAggressiveLockingKeysInfo`
- `GetAggressiveLockingPreviousKeys`
- `GetAggressiveLockingPreviousKeysInfo`
- `GetClusterID`
- `GetCommitTS`
- `GetCommitWaitUntilTSO`
- `GetCommitWaitUntilTSOTimeout`
- `GetCommitter`
- `GetDefaultLockTTL`
- `GetDiskFullOpt`
- `GetFlags`
- `GetForUpdateTS`
- `GetKey`
- `GetKeys`
- `GetLockTTL`
- `GetLockedCount`
- `GetMemBuffer`
- `GetMinCommitTS`
- `GetMutations`
- `GetOnePCCommitTS`
- `GetOp`
- `GetOps`
- `GetPessimisticLockMaxBackoff`
- `GetPrimaryKey`
- `GetScope`
- `GetSnapshot`
- `GetStartTS`
- `GetStartTime`
- `GetTTLFactor`
- `GetTimestampForCommit`
- `GetTxnCommitBatchSize`
- `GetUndeterminedErr`
- `GetUnionStore`
- `GetValue`
- `GetValues`
- `GetVars`
- `HasCheckExistence`
- `HasReturnValues`
- `InitKeysAndMutations`
- `InitPipelinedMemDB`
- `IsAssertExists`
- `IsAssertNotExist`
- `IsAsyncCommit`
- `IsCasualConsistency`
- `IsInAggressiveLockingMode`
- `IsInAggressiveLockingStage`
- `IsNil`
- `IsOnePC`
- `IsPessimistic`
- `IsPessimisticLock`
- `IsPipelined`
- `IsReadOnly`
- `IsTTLRunning`
- `IsTTLUninitialized`
- `Iter`
- `IterReverse`
- `Key`
- `Len`
- `LoadPreSplitDetectThreshold`
- `LoadPreSplitSizeThreshold`
- `LockKeys`
- `LockKeysFunc`
- `LockKeysWithWaitTime`
- `Mem`
- `MemHookSet`
- `MergeMutations`
- `MutationsOfKeys`
- `NeedConstraintCheckInPrewrite`
- `NewCommitter`
- `NewScanner`
- `PessimisticRollbackMutations`
- `PrewriteAllMutations`
- `PrewriteMutations`
- `Push`
- `ResolveFlushedLocks`
- `RetryAggressiveLocking`
- `Rollback`
- `Set`
- `SetAssertionLevel`
- `SetBackgroundGoroutineLifecycleHooks`
- `SetBinlogExecutor`
- `SetCausalConsistency`
- `SetCommitCallback`
- `SetCommitTS`
- `SetCommitTSUpperBoundCheck`
- `SetCommitWaitUntilTSO`
- `SetCommitWaitUntilTSOTimeout`
- `SetCommitter`
- `SetDiskFullOpt`
- `SetEnable1PC`
- `SetEnableAsyncCommit`
- `SetExplicitRequestSourceType`
- `SetForUpdateTS`
- `SetKVFilter`
- `SetLockTTL`
- `SetLockTTLByTimeAndSize`
- `SetMaxCommitTS`
- `SetMemoryFootprintChangeHook`
- `SetMinCommitTS`
- `SetMutations`
- `SetNoFallBack`
- `SetPessimistic`
- `SetPrewriteEncounterLockPolicy`
- `SetPrimaryKey`
- `SetPrimaryKeyBlocker`
- `SetPriority`
- `SetRPCInterceptor`
- `SetRequestSourceInternal`
- `SetRequestSourceType`
- `SetResourceGroupName`
- `SetResourceGroupTag`
- `SetResourceGroupTagger`
- `SetSchemaLeaseChecker`
- `SetSchemaVer`
- `SetScope`
- `SetSessionID`
- `SetStartTS`
- `SetTxnSize`
- `SetTxnSource`
- `SetUseAsyncCommit`
- `SetVars`
- `Size`
- `Slice`
- `StartAggressiveLocking`
- `StartTS`
- `StorePreSplitDetectThreshold`
- `StorePreSplitSizeThreshold`
- `String`
- `Valid`
- `Value`
- `WaitCleanup`

## txnkv/txnlock (package txnlock)

### Types
- `Lock`
- `LockProbe`
- `LockResolver`
- `LockResolverProbe`
- `ResolveLockResult`
- `ResolveLocksOptions`
- `ResolvingLock`
- `TxnStatus`

### Functions
- `ExtractLockFromKeyErr`
- `NewLock`
- `NewLockResolver`

### Consts
- `ResolvedCacheSize`

### Vars
- (none)

### Exported Methods (receiver elided)
- `Action`
- `BatchResolveLocks`
- `CheckAllSecondaries`
- `Close`
- `CommitTS`
- `Error`
- `GetPrimaryKeyFromTxnStatus`
- `GetSecondariesFromTxnStatus`
- `GetTxnStatus`
- `GetTxnStatusFromLock`
- `HasSameDeterminedStatus`
- `IsCommitted`
- `IsErrorNotFound`
- `IsNonAsyncCommitLock`
- `IsRolledBack`
- `IsStatusDetermined`
- `NewLockStatus`
- `RecordResolvingLocks`
- `ResolveAsyncCommitLock`
- `ResolveLock`
- `ResolveLocks`
- `ResolveLocksDone`
- `ResolveLocksForRead`
- `ResolveLocksWithOpts`
- `ResolvePessimisticLock`
- `Resolving`
- `SetMeetLockCallback`
- `SetResolving`
- `StatusCacheable`
- `String`
- `TTL`
- `UpdateResolvingLocks`

## txnkv/txnsnapshot (package txnsnapshot)

### Types
- `ClientHelper`
- `ConfigProbe`
- `IsoLevel`
- `KVSnapshot`
- `ReplicaReadAdjuster`
- `Scanner`
- `SnapshotProbe`
- `SnapshotRuntimeStats`

### Functions
- `NewClientHelper`
- `NewTiKVSnapshot`

### Consts
- `BatchGetBufferTier`
- `BatchGetSnapshotTier`
- `DefaultScanBatchSize`
- `RC`
- `RCCheckTS`
- `SI`

### Vars
- (none)

### Exported Methods (receiver elided)
- `AddRPCInterceptor`
- `BatchGet`
- `BatchGetSingleRegion`
- `BatchGetWithTier`
- `CleanCache`
- `Clone`
- `Close`
- `FormatStats`
- `Get`
- `GetCmdRPCCount`
- `GetGetMaxBackoff`
- `GetKVReadTimeout`
- `GetResolveLockDetail`
- `GetScanBatchSize`
- `GetTimeDetail`
- `Go`
- `IsInternal`
- `Iter`
- `IterReverse`
- `Key`
- `Merge`
- `MergeExecDetail`
- `MergeRegionRequestStats`
- `NewScanner`
- `Next`
- `RecordBackoffInfo`
- `RecordResolvingLocks`
- `ResolveLocks`
- `ResolveLocksDone`
- `ResolveLocksWithOpts`
- `SendReqAsync`
- `SendReqCtx`
- `SetIsStalenessReadOnly`
- `SetIsolationLevel`
- `SetKVReadTimeout`
- `SetKeyOnly`
- `SetLoadBasedReplicaReadThreshold`
- `SetMatchStoreLabels`
- `SetNotFillCache`
- `SetPipelined`
- `SetPriority`
- `SetRPCInterceptor`
- `SetReadReplicaScope`
- `SetReplicaRead`
- `SetReplicaReadAdjuster`
- `SetResourceGroupName`
- `SetResourceGroupTag`
- `SetResourceGroupTagger`
- `SetRuntimeStats`
- `SetSampleStep`
- `SetScanBatchSize`
- `SetSnapshotTS`
- `SetTaskID`
- `SetTxnScope`
- `SetVars`
- `SnapCache`
- `SnapCacheHitCount`
- `SnapCacheSize`
- `String`
- `ToPB`
- `UpdateResolvingLocks`
- `UpdateSnapshotCache`
- `Valid`
- `Value`

## txnkv/txnutil (package txnutil)

### Types
- `Priority`

### Functions
- (none)

### Consts
- `PriorityHigh`
- `PriorityLow`
- `PriorityNormal`

### Vars
- (none)

### Exported Methods (receiver elided)
- `ToPB`

## util (package util)

### Types
- `CommitDetails`
- `CommitTSLagDetails`
- `ExecDetails`
- `InterceptedPDClient`
- `LockKeysDetails`
- `Option`
- `RUDetails`
- `RateLimit`
- `ReqDetailInfo`
- `RequestSource`
- `RequestSourceKeyType`
- `RequestSourceTypeKeyType`
- `ResolveLockDetail`
- `ScanDetail`
- `TSSet`
- `TiKVExecDetails`
- `TimeDetail`
- `TrafficDetails`
- `WriteDetail`

### Functions
- `BuildRequestSource`
- `BytesToString`
- `CompatibleParseGCTime`
- `ContextWithTraceExecDetails`
- `EnableFailpoints`
- `EvalFailpoint`
- `FormatBytes`
- `FormatDuration`
- `GetCustomDNSDialer`
- `IsInternalRequest`
- `IsRequestSourceInternal`
- `NewInterceptedPDClient`
- `NewRUDetails`
- `NewRUDetailsWith`
- `NewRateLimit`
- `NewTiKVExecDetails`
- `RequestSourceFromCtx`
- `ResourceGroupNameFromCtx`
- `SetSessionID`
- `TraceExecDetailsEnabled`
- `WithInternalSourceAndTaskType`
- `WithInternalSourceType`
- `WithRecovery`
- `WithResourceGroupName`

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

### Exported Methods (receiver elided)
- `Clone`
- `GetAll`
- `GetCapacity`
- `GetPrevRegion`
- `GetRegion`
- `GetRegionByID`
- `GetRequestSource`
- `GetStore`
- `GetTS`
- `GetTSAsync`
- `GetToken`
- `Inner`
- `Merge`
- `MergeCommitReqDetails`
- `MergeFlushReqDetails`
- `MergeFromScanDetailV2`
- `MergeFromTimeDetail`
- `MergeFromWriteDetailPb`
- `MergePrewriteReqDetails`
- `MergeReqDetails`
- `Put`
- `PutToken`
- `RRU`
- `RUWaitDuration`
- `ScanRegions`
- `SetExplicitRequestSourceType`
- `SetRequestSourceInternal`
- `SetRequestSourceType`
- `String`
- `Update`
- `WRU`
- `Wait`
- `WithCallerComponent`

## util/async (package async)

### Types
- `Callback`
- `Executor`
- `Pool`
- `RunLoop`
- `State`

### Functions
- `NewRunLoop`

### Consts
- `StateIdle`
- `StateRunning`
- `StateWaiting`

### Vars
- (none)

### Exported Methods (receiver elided)
- `Append`
- `Exec`
- `Executor`
- `Go`
- `Inject`
- `Invoke`
- `NumRunnable`
- `Schedule`
- `State`

## util/codec (package codec)

### Types
- (none)

### Functions
- `DecodeBytes`
- `DecodeCmpUintToInt`
- `DecodeComparableUvarint`
- `DecodeComparableVarint`
- `DecodeInt`
- `DecodeIntDesc`
- `DecodeUint`
- `DecodeUintDesc`
- `DecodeUvarint`
- `DecodeVarint`
- `EncodeBytes`
- `EncodeComparableUvarint`
- `EncodeComparableVarint`
- `EncodeInt`
- `EncodeIntDesc`
- `EncodeIntToCmpUint`
- `EncodeUint`
- `EncodeUintDesc`
- `EncodeUvarint`
- `EncodeVarint`

### Consts
- (none)

### Vars
- (none)

### Exported Methods (receiver elided)
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

### Exported Methods (receiver elided)
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

### Exported Methods (receiver elided)
- (none)

## util/redact (package redact)

### Types
- (none)

### Functions
- `Key`
- `KeyBytes`
- `NeedRedact`
- `RedactKeyErrIfNecessary`
- `String`

### Consts
- (none)

### Vars
- (none)

### Exported Methods (receiver elided)
- (none)

