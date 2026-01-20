// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go client parity: `config/retry/backoff.go` (Backoffer + per-type backoff budgets).
//!
//! The rest of the crate uses [`crate::backoff::Backoff`] (attempt-budgeted exponential backoff).
//! `Backoffer` is a separate utility that models client-go's "max total sleep" budgeting, including
//! the "excluded sleep" semantics for `tikvServerBusy`.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rand::thread_rng;
use rand::Rng;
use thiserror::Error;

use crate::proto::errorpb;

const TXN_LOCK_FAST_NAME: &str = "txnLockFast";

static TIKV_SERVER_BUSY_EXCLUDED_MAX_MS: AtomicU64 = AtomicU64::new(600_000);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Jitter {
    NoJitter,
    FullJitter,
    EqualJitter,
    DecorrJitter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackoffFnCfg {
    base_ms: u64,
    cap_ms: u64,
    jitter: Jitter,
}

impl BackoffFnCfg {
    pub(crate) const fn new(base_ms: u64, cap_ms: u64, jitter: Jitter) -> Self {
        Self {
            base_ms,
            cap_ms,
            jitter,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackoffErrorKind {
    TiKVServerTimeout,
    ResolveLockTimeout,
    RegionUnavailable,
    RegionNotInitialized,
    RegionRecoveryInProgress,
    TiKVServerBusy,
    IsWitness,
}

impl std::fmt::Display for BackoffErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BackoffErrorKind::TiKVServerTimeout => "tikv server timeout",
            BackoffErrorKind::ResolveLockTimeout => "resolve lock timeout",
            BackoffErrorKind::RegionUnavailable => "region unavailable",
            BackoffErrorKind::RegionNotInitialized => "region not initialized",
            BackoffErrorKind::RegionRecoveryInProgress => "region recovery in progress",
            BackoffErrorKind::TiKVServerBusy => "tikv server busy",
            BackoffErrorKind::IsWitness => "is witness",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum BackofferError {
    /// The caller's context has been canceled; preserve the original error message.
    #[error("{message}")]
    Canceled { message: String },

    /// Max sleep budget exceeded. Mirrors client-go's behavior of returning the "dominant" backoff
    /// kind (longest sleep time).
    #[error("{kind}")]
    MaxSleepExceeded { kind: BackoffErrorKind },

    /// Used when max sleep is exceeded but we have no non-excluded sleep candidates (e.g. only
    /// `tikvServerBusy` has slept so far).
    #[error("{message}")]
    Other { message: String },
}

impl BackofferError {
    pub(crate) fn kind(&self) -> Option<BackoffErrorKind> {
        match self {
            BackofferError::MaxSleepExceeded { kind } => Some(*kind),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackoffConfig {
    name: &'static str,
    fn_cfg: BackoffFnCfg,
    err_kind: BackoffErrorKind,
}

impl BackoffConfig {
    pub(crate) const fn new(
        name: &'static str,
        fn_cfg: BackoffFnCfg,
        err_kind: BackoffErrorKind,
    ) -> Self {
        Self {
            name,
            fn_cfg,
            err_kind,
        }
    }
}

pub(crate) const BO_TIKV_RPC: BackoffConfig = BackoffConfig::new(
    "tikvRPC",
    BackoffFnCfg::new(100, 2000, Jitter::EqualJitter),
    BackoffErrorKind::TiKVServerTimeout,
);

pub(crate) const BO_REGION_MISS: BackoffConfig = BackoffConfig::new(
    "regionMiss",
    BackoffFnCfg::new(2, 500, Jitter::NoJitter),
    BackoffErrorKind::RegionUnavailable,
);

pub(crate) const BO_TIKV_SERVER_BUSY: BackoffConfig = BackoffConfig::new(
    "tikvServerBusy",
    BackoffFnCfg::new(2000, 10_000, Jitter::EqualJitter),
    BackoffErrorKind::TiKVServerBusy,
);

pub(crate) const BO_REGION_RECOVERY_IN_PROGRESS: BackoffConfig = BackoffConfig::new(
    "regionRecoveryInProgress",
    BackoffFnCfg::new(100, 10_000, Jitter::EqualJitter),
    BackoffErrorKind::RegionRecoveryInProgress,
);

pub(crate) const BO_TXN_NOT_FOUND: BackoffConfig = BackoffConfig::new(
    "txnNotFound",
    BackoffFnCfg::new(2, 500, Jitter::NoJitter),
    BackoffErrorKind::ResolveLockTimeout,
);

pub(crate) const BO_MAX_REGION_NOT_INITIALIZED: BackoffConfig = BackoffConfig::new(
    "regionNotInitialized",
    BackoffFnCfg::new(2, 1000, Jitter::NoJitter),
    BackoffErrorKind::RegionNotInitialized,
);

pub(crate) const BO_IS_WITNESS: BackoffConfig = BackoffConfig::new(
    "isWitness",
    BackoffFnCfg::new(1000, 10_000, Jitter::EqualJitter),
    BackoffErrorKind::IsWitness,
);

pub(crate) const BO_TXN_LOCK_FAST: BackoffConfig = BackoffConfig::new(
    TXN_LOCK_FAST_NAME,
    // NOTE: client-go uses `vars.BackoffLockFast` as base at runtime.
    BackoffFnCfg::new(2, 3000, Jitter::EqualJitter),
    BackoffErrorKind::ResolveLockTimeout,
);

fn excluded_sleep_limit_ms(cfg_name: &str) -> Option<u64> {
    if cfg_name == BO_TIKV_SERVER_BUSY.name {
        return Some(TIKV_SERVER_BUSY_EXCLUDED_MAX_MS.load(Ordering::Relaxed));
    }
    None
}

#[cfg(test)]
fn set_backoff_excluded(cfg_name: &str, max_val: u64) {
    if cfg_name == BO_TIKV_SERVER_BUSY.name {
        TIKV_SERVER_BUSY_EXCLUDED_MAX_MS.store(max_val, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BackofferVars {
    pub(crate) backoff_lock_fast_ms: u64,
    pub(crate) backoff_weight: u64,
}

impl Default for BackofferVars {
    fn default() -> Self {
        Self {
            backoff_lock_fast_ms: 10,
            backoff_weight: 2,
        }
    }
}

#[derive(Debug)]
struct BackoffFnState {
    base_ms: u64,
    cap_ms: u64,
    jitter: Jitter,
    attempts: u32,
    last_sleep_ms: u64,
}

impl BackoffFnState {
    fn new(base_ms: u64, cap_ms: u64, jitter: Jitter) -> Self {
        // Keep jitter helpers' ranges non-empty.
        let base_ms = base_ms.max(2);
        let cap_ms = cap_ms.max(base_ms);
        Self {
            base_ms,
            cap_ms,
            jitter,
            attempts: 0,
            last_sleep_ms: base_ms,
        }
    }

    fn backoff_ms(&mut self, max_sleep_ms: Option<u64>) -> u64 {
        let sleep_ms = match self.jitter {
            Jitter::NoJitter => expo(self.base_ms, self.cap_ms, self.attempts),
            Jitter::FullJitter => {
                let v = expo(self.base_ms, self.cap_ms, self.attempts);
                thread_rng().gen_range(0..v)
            }
            Jitter::EqualJitter => {
                let v = expo(self.base_ms, self.cap_ms, self.attempts);
                let half = v / 2;
                half + thread_rng().gen_range(0..half)
            }
            Jitter::DecorrJitter => {
                // rand_between(base, last_sleep*3)
                let upper_exclusive = self
                    .last_sleep_ms
                    .saturating_mul(3)
                    .saturating_sub(self.base_ms)
                    .max(1);
                let v = self.base_ms + thread_rng().gen_range(0..upper_exclusive);
                v.min(self.cap_ms)
            }
        };

        let real_sleep_ms = max_sleep_ms.map_or(sleep_ms, |max| sleep_ms.min(max));

        self.attempts += 1;
        self.last_sleep_ms = sleep_ms;
        real_sleep_ms
    }
}

fn expo(base_ms: u64, cap_ms: u64, attempts: u32) -> u64 {
    let mul = 1u64.checked_shl(attempts).unwrap_or(u64::MAX);
    base_ms.saturating_mul(mul).min(cap_ms)
}

#[derive(Debug)]
struct BackoffContextInner {
    parent: Option<Arc<BackoffContextInner>>,
    canceled: AtomicBool,
}

/// A minimal cancelable context (client-go parity for `Backoffer` tests).
#[derive(Debug, Clone)]
pub(crate) struct BackoffContext {
    inner: Arc<BackoffContextInner>,
}

impl BackoffContext {
    pub(crate) fn new() -> (Self, BackoffCancel) {
        let inner = Arc::new(BackoffContextInner {
            parent: None,
            canceled: AtomicBool::new(false),
        });
        (
            Self {
                inner: inner.clone(),
            },
            BackoffCancel { inner },
        )
    }

    fn with_cancel(&self) -> (Self, BackoffCancel) {
        let inner = Arc::new(BackoffContextInner {
            parent: Some(self.inner.clone()),
            canceled: AtomicBool::new(false),
        });
        (
            Self {
                inner: inner.clone(),
            },
            BackoffCancel { inner },
        )
    }

    fn is_canceled(&self) -> bool {
        let mut cursor = Some(self.inner.clone());
        while let Some(inner) = cursor {
            if inner.canceled.load(Ordering::Relaxed) {
                return true;
            }
            cursor = inner.parent.clone();
        }
        false
    }

    fn strict_has_ancestor(&self, ancestor: &BackoffContext) -> bool {
        let mut cursor = self.inner.parent.clone();
        while let Some(inner) = cursor {
            if Arc::ptr_eq(&inner, &ancestor.inner) {
                return true;
            }
            cursor = inner.parent.clone();
        }
        false
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BackoffCancel {
    inner: Arc<BackoffContextInner>,
}

impl BackoffCancel {
    pub(crate) fn cancel(&self) {
        self.inner.canceled.store(true, Ordering::Relaxed);
    }
}

/// Go client parity: a backoff budget with (total sleep) cap and "excluded sleep" buckets.
#[derive(Debug)]
pub(crate) struct Backoffer {
    ctx: BackoffContext,
    max_sleep_ms: u64,
    total_sleep_ms: u64,
    excluded_sleep_ms: u64,

    vars: BackofferVars,
    noop: bool,

    errors: Vec<String>,
    configs: Vec<BackoffConfig>,
    backoff_sleep_ms: HashMap<&'static str, u64>,
    backoff_times: HashMap<&'static str, u64>,

    // Per-config backoff state (attempt counters). Not cloned into fork/clone for parity.
    fns: HashMap<&'static str, BackoffFnState>,
}

impl Backoffer {
    pub(crate) fn new(ctx: BackoffContext, max_sleep_ms: u64) -> Self {
        Self::new_with_vars(ctx, max_sleep_ms, None)
    }

    pub(crate) fn new_noop(ctx: BackoffContext) -> Self {
        Self {
            ctx,
            max_sleep_ms: 0,
            total_sleep_ms: 0,
            excluded_sleep_ms: 0,
            vars: BackofferVars::default(),
            noop: true,
            errors: Vec::new(),
            configs: Vec::new(),
            backoff_sleep_ms: HashMap::new(),
            backoff_times: HashMap::new(),
            fns: HashMap::new(),
        }
    }

    pub(crate) fn new_with_vars(
        ctx: BackoffContext,
        max_sleep_ms: u64,
        vars: Option<BackofferVars>,
    ) -> Self {
        let vars = vars.unwrap_or_default();
        let max_sleep_ms = max_sleep_ms.saturating_mul(vars.backoff_weight);
        Self {
            ctx,
            max_sleep_ms,
            total_sleep_ms: 0,
            excluded_sleep_ms: 0,
            vars,
            noop: false,
            errors: Vec::new(),
            configs: Vec::new(),
            backoff_sleep_ms: HashMap::new(),
            backoff_times: HashMap::new(),
            fns: HashMap::new(),
        }
    }

    pub(crate) fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            max_sleep_ms: self.max_sleep_ms,
            total_sleep_ms: self.total_sleep_ms,
            excluded_sleep_ms: self.excluded_sleep_ms,
            vars: self.vars.clone(),
            noop: self.noop,
            errors: self.errors.clone(),
            configs: self.configs.clone(),
            backoff_sleep_ms: self.backoff_sleep_ms.clone(),
            backoff_times: self.backoff_times.clone(),
            fns: HashMap::new(),
        }
    }

    pub(crate) fn fork(&self) -> (Self, BackoffCancel) {
        let (ctx, cancel) = self.ctx.with_cancel();
        (
            Self {
                ctx,
                max_sleep_ms: self.max_sleep_ms,
                total_sleep_ms: self.total_sleep_ms,
                excluded_sleep_ms: self.excluded_sleep_ms,
                vars: self.vars.clone(),
                noop: self.noop,
                errors: self.errors.clone(),
                configs: self.configs.clone(),
                backoff_sleep_ms: self.backoff_sleep_ms.clone(),
                backoff_times: self.backoff_times.clone(),
                fns: HashMap::new(),
            },
            cancel,
        )
    }

    pub(crate) fn update_using_forked(&mut self, forked: &Backoffer) {
        if !forked.ctx.strict_has_ancestor(&self.ctx) {
            return;
        }
        self.total_sleep_ms = forked.total_sleep_ms;
        self.excluded_sleep_ms = forked.excluded_sleep_ms;
        self.errors = forked.errors.clone();
        self.backoff_sleep_ms = forked.backoff_sleep_ms.clone();
        self.backoff_times = forked.backoff_times.clone();
    }

    pub(crate) fn longest_sleep_cfg(&self) -> Option<(BackoffConfig, u64)> {
        let mut candidate = None::<(&'static str, u64)>;
        for (&name, &sleep_ms) in &self.backoff_sleep_ms {
            if excluded_sleep_limit_ms(name).is_some() {
                continue;
            }
            if candidate.is_none_or(|(_, best)| sleep_ms > best) {
                candidate = Some((name, sleep_ms));
            }
        }

        let (candidate_name, sleep_ms) = candidate?;
        let cfg = self
            .configs
            .iter()
            .copied()
            .find(|cfg| cfg.name == candidate_name)?;
        Some((cfg, sleep_ms))
    }

    pub(crate) fn backoff(
        &mut self,
        cfg: &BackoffConfig,
        err_message: impl Into<String>,
    ) -> Result<(), BackofferError> {
        self.backoff_with_cfg_and_max_sleep(cfg, None, err_message)
    }

    pub(crate) fn backoff_with_max_sleep_txn_lock_fast(
        &mut self,
        max_sleep_ms: u64,
        err_message: impl Into<String>,
    ) -> Result<(), BackofferError> {
        self.backoff_with_cfg_and_max_sleep(&BO_TXN_LOCK_FAST, Some(max_sleep_ms), err_message)
    }

    fn backoff_with_cfg_and_max_sleep(
        &mut self,
        cfg: &BackoffConfig,
        max_sleep_ms: Option<u64>,
        err_message: impl Into<String>,
    ) -> Result<(), BackofferError> {
        let err_message = err_message.into();

        if self.ctx.is_canceled() {
            return Err(BackofferError::Canceled {
                message: err_message,
            });
        }

        if self.noop {
            return Err(BackofferError::Other {
                message: err_message,
            });
        }

        let max_backoff_time_exceeded = self.max_sleep_ms > 0
            && (self.total_sleep_ms - self.excluded_sleep_ms) >= self.max_sleep_ms;
        let max_excluded_time_exceeded =
            excluded_sleep_limit_ms(cfg.name).is_some_and(|max_limit| {
                self.excluded_sleep_ms >= max_limit && self.excluded_sleep_ms >= self.max_sleep_ms
            });

        if self.max_sleep_ms > 0 && (max_backoff_time_exceeded || max_excluded_time_exceeded) {
            return match self.longest_sleep_cfg() {
                Some((cfg, _)) => Err(BackofferError::MaxSleepExceeded { kind: cfg.err_kind }),
                None => Err(BackofferError::Other {
                    message: err_message,
                }),
            };
        }

        self.errors.push(err_message);
        self.configs.push(*cfg);

        let base_ms = if cfg.name == TXN_LOCK_FAST_NAME {
            self.vars.backoff_lock_fast_ms
        } else {
            cfg.fn_cfg.base_ms
        };
        let state = self
            .fns
            .entry(cfg.name)
            .or_insert_with(|| BackoffFnState::new(base_ms, cfg.fn_cfg.cap_ms, cfg.fn_cfg.jitter));
        let real_sleep_ms = state.backoff_ms(max_sleep_ms);

        self.total_sleep_ms += real_sleep_ms;
        if excluded_sleep_limit_ms(cfg.name).is_some() {
            self.excluded_sleep_ms += real_sleep_ms;
        }
        *self.backoff_sleep_ms.entry(cfg.name).or_default() += real_sleep_ms;
        *self.backoff_times.entry(cfg.name).or_default() += 1;

        Ok(())
    }
}

pub(crate) fn is_fake_region_error(err: &errorpb::Error) -> bool {
    err.epoch_not_match
        .as_ref()
        .is_some_and(|enm| enm.current_regions.is_empty())
}

pub(crate) fn may_backoff_for_region_error(
    region_err: &errorpb::Error,
    bo: &mut Backoffer,
) -> Result<(), BackofferError> {
    if region_err.epoch_not_match.is_none() || is_fake_region_error(region_err) {
        return bo.backoff(&BO_REGION_MISS, format!("{region_err:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_backoff_with_max_sleep() {
        let (ctx, _cancel) = BackoffContext::new();
        let mut b = Backoffer::new(ctx, 2000);
        b.backoff_with_max_sleep_txn_lock_fast(5, "test").unwrap();
        assert_eq!(b.total_sleep_ms, 5);
    }

    #[test]
    fn test_backoff_error_type_uses_longest_sleep_kind() {
        let (ctx, _cancel) = BackoffContext::new();
        let mut b = Backoffer::new_with_vars(ctx, 800, None);

        b.backoff(&BO_REGION_MISS, "region miss").unwrap(); // 2ms

        for _ in 0..2 {
            b.backoff(&BO_MAX_REGION_NOT_INITIALIZED, "region not initialized")
                .unwrap();
        }

        b.backoff(&BO_REGION_RECOVERY_IN_PROGRESS, "recovery in progress")
            .unwrap();

        // Excluded sleep does not count towards maxSleep.
        b.backoff(&BO_TIKV_SERVER_BUSY, "server is busy").unwrap();

        b.backoff(&BO_IS_WITNESS, "peer is witness").unwrap();

        for _ in 0..15 {
            match b.backoff(&BO_TXN_NOT_FOUND, "txn not found") {
                Ok(()) => {}
                Err(e) => {
                    let (cfg, _) = b.longest_sleep_cfg().expect("expected a longest-sleep cfg");
                    assert_eq!(e.kind(), Some(cfg.err_kind));
                    return;
                }
            }
        }

        panic!("expected the backoff to exceed max sleep");
    }

    #[test]
    fn test_backoff_deep_copy_clone_and_fork_share_budget() {
        let (ctx, _cancel) = BackoffContext::new();
        let mut b = Backoffer::new_with_vars(ctx, 4, None);

        for _ in 0..3 {
            b.backoff(&BO_MAX_REGION_NOT_INITIALIZED, "region not initialized")
                .unwrap();
        }

        let (forked, _cancel) = b.fork();
        let cloned = b.clone();

        for mut bo in [forked, cloned] {
            let e = bo.backoff(&BO_TIKV_RPC, "tikv rpc").unwrap_err();
            assert_eq!(
                e.kind(),
                Some(BO_MAX_REGION_NOT_INITIALIZED.err_kind),
                "{e:?}"
            );
        }
    }

    #[test]
    fn test_backoff_update_using_forked() {
        let (ctx, _cancel) = BackoffContext::new();
        let mut b = Backoffer::new_with_vars(ctx, 4, None);

        for _ in 0..3 {
            b.backoff(&BO_MAX_REGION_NOT_INITIALIZED, "region not initialized")
                .unwrap();
        }

        let (mut forked, _cancel) = b.fork();
        let _ = forked.backoff(&BO_TIKV_RPC, "tikv rpc");
        let cloned_forked = forked.clone();

        b.update_using_forked(&forked);

        assert_eq!(b.errors, cloned_forked.errors);
        assert_eq!(b.backoff_sleep_ms, cloned_forked.backoff_sleep_ms);
        assert_eq!(b.backoff_times, cloned_forked.backoff_times);
        assert_eq!(b.excluded_sleep_ms, cloned_forked.excluded_sleep_ms);
        assert_eq!(b.total_sleep_ms, cloned_forked.total_sleep_ms);
        assert!(!b.ctx.inner.parent.is_some(), "expected root backoffer");
    }

    #[test]
    fn test_backoff_with_max_excluded_exceed() {
        let old = TIKV_SERVER_BUSY_EXCLUDED_MAX_MS.load(Ordering::Relaxed);
        set_backoff_excluded(BO_TIKV_SERVER_BUSY.name, 1);

        let (ctx, _cancel) = BackoffContext::new();
        let mut b = Backoffer::new_with_vars(ctx, 1, None);

        b.backoff(&BO_TIKV_SERVER_BUSY, "server is busy").unwrap();

        let e = b
            .backoff(&BO_TIKV_SERVER_BUSY, "server is busy")
            .unwrap_err();
        assert!(matches!(
            e,
            BackofferError::Other { .. } | BackofferError::MaxSleepExceeded { .. }
        ));
        assert!(b.excluded_sleep_ms > b.max_sleep_ms);

        // Restore global for parallel test hygiene.
        set_backoff_excluded(BO_TIKV_SERVER_BUSY.name, old);
    }

    #[test]
    fn test_may_backoff_for_region_error() {
        // Errors should retry without backoff.
        let region_err = errorpb::Error {
            epoch_not_match: Some(errorpb::EpochNotMatch {
                current_regions: vec![crate::proto::metapb::Region {
                    id: 1,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        let (ctx, _cancel) = BackoffContext::new();
        let mut b = Backoffer::new_with_vars(ctx, 1, None);
        may_backoff_for_region_error(&region_err, &mut b).unwrap();
        assert_eq!(b.total_sleep_ms, 0);

        // Errors should back off and retry.
        let errs = vec![
            errorpb::Error {
                epoch_not_match: Some(errorpb::EpochNotMatch::default()),
                ..Default::default()
            },
            errorpb::Error {
                not_leader: Some(errorpb::NotLeader::default()),
                ..Default::default()
            },
            errorpb::Error {
                server_is_busy: Some(errorpb::ServerIsBusy::default()),
                ..Default::default()
            },
            errorpb::Error {
                max_timestamp_not_synced: Some(errorpb::MaxTimestampNotSynced::default()),
                ..Default::default()
            },
        ];

        for region_err in errs {
            // backoff succeeds
            let (ctx, cancel) = BackoffContext::new();
            let mut b = Backoffer::new_with_vars(ctx.clone(), 1, None);
            may_backoff_for_region_error(&region_err, &mut b).unwrap();
            assert!(b.total_sleep_ms > 0);

            // backoff fails (context canceled)
            cancel.cancel();
            let mut b = Backoffer::new_with_vars(ctx, 1, None);
            let e = may_backoff_for_region_error(&region_err, &mut b).unwrap_err();
            assert!(matches!(e, BackofferError::Canceled { .. }));
        }
    }

    #[test]
    fn test_noop_backoffer_returns_error_without_sleeping() {
        let (ctx, _cancel) = BackoffContext::new();
        let mut b = Backoffer::new_noop(ctx);
        let e = b.backoff(&BO_REGION_MISS, "region miss").unwrap_err();
        assert!(matches!(e, BackofferError::Other { .. }));
        assert_eq!(b.total_sleep_ms, 0);
    }

    #[test]
    fn test_full_and_decorr_jitter_are_bounded() {
        let mut full = BackoffFnState::new(2, 7, Jitter::FullJitter);
        let d1 = full.backoff_ms(None);
        assert!(d1 <= 7);

        let mut decorr = BackoffFnState::new(2, 7, Jitter::DecorrJitter);
        let d2 = decorr.backoff_ms(None);
        assert!(d2 >= 2);
        assert!(d2 <= 7);
    }
}
