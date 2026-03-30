//! Client-go style retry/backoff helpers.
//!
//! This module mirrors client-go `config/retry` at a high level.
//!
//! Note: client-go's `retry.Backoffer` bounds the *total* sleep time of a retry loop via
//! `context.Context`. The Rust client uses [`crate::Backoff`] as a backoff *schedule* with a
//! bounded number of attempts. The helpers in this module provide best-effort presets and
//! constructors that are convenient when implementing custom retry loops.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use tokio::time::sleep;

use crate::proto::errorpb;
use crate::Backoff;
use crate::Error;
use crate::Result;
use crate::Variables;

/// Client-go style `Backoffer`.
///
/// In the Rust client, this is an alias of [`crate::Backoff`].
pub type Backoffer = Backoff;

/// Client-go style backoff configuration.
///
/// Unlike [`Backoffer`], this is a lightweight descriptor that can be turned into a concrete
/// [`crate::Backoff`] schedule on demand. This keeps the public API close to client-go
/// `retry.Config` while preserving the Rust client's existing schedule implementation.
#[derive(Clone, Debug)]
pub struct Config {
    name: String,
    backoff_fn_cfg: Option<BackoffFnCfg>,
    error: Option<Arc<crate::Error>>,
}

const DEFAULT_MAX_ATTEMPTS: u32 = 16;
const DEFAULT_NO_JITTER_MAX_ATTEMPTS: u32 = 10;
const TXN_LOCK_FAST_NAME: &str = "txnLockFast";

impl Config {
    /// Create a config that never sleeps.
    #[must_use]
    pub fn no_backoff() -> Self {
        Self {
            name: String::new(),
            backoff_fn_cfg: None,
            error: None,
        }
    }

    /// Convert this config into a concrete [`Backoff`] schedule.
    #[must_use]
    pub fn to_backoff(&self) -> Backoff {
        self.to_backoff_with_vars(&Variables::default())
    }

    /// Convert this config into a concrete [`Backoff`] schedule using transaction variables.
    #[must_use]
    pub fn to_backoff_with_vars(&self, vars: &Variables) -> Backoff {
        let Some(mut backoff_fn_cfg) = self.backoff_fn_cfg else {
            return Backoff::no_backoff();
        };

        if self.name.eq_ignore_ascii_case(TXN_LOCK_FAST_NAME) {
            backoff_fn_cfg.base_delay_ms = vars.backoff_lock_fast_ms.max(2);
        }

        backoff_fn_cfg.to_backoff(default_max_attempts(backoff_fn_cfg.jitter))
    }

    /// Returns the configured base delay in milliseconds.
    #[doc(alias = "Base")]
    #[must_use]
    pub fn base(&self) -> u64 {
        self.backoff_fn_cfg.map_or(0, |cfg| cfg.base_delay_ms)
    }

    /// Replace the stored detailed error.
    #[doc(alias = "SetErrors")]
    pub fn set_errors(&mut self, err: crate::Error) {
        self.error = Some(Arc::new(err));
    }

    /// Replace the backoff function configuration.
    #[doc(alias = "SetBackoffFnCfg")]
    pub fn set_backoff_fn_cfg(&mut self, fn_cfg: BackoffFnCfg) {
        self.backoff_fn_cfg = Some(fn_cfg);
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

/// Jitter type for exponential backoff.
///
/// This is a Rust-friendly mirror of client-go `config/retry` jitter constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BackoffJitter {
    /// Apply no jitter and use the raw exponential delay.
    NoJitter,
    /// Randomize uniformly between zero and the current exponential delay.
    FullJitter,
    /// Keep half of the exponential delay and randomize the other half.
    EqualJitter,
    /// Randomize based on the previous sleep to avoid synchronized retries.
    DecorrJitter,
}

/// No jitter (strict exponential).
#[doc(alias = "NoJitter")]
pub const NO_JITTER: BackoffJitter = BackoffJitter::NoJitter;
/// Full jitter.
#[doc(alias = "FullJitter")]
pub const FULL_JITTER: BackoffJitter = BackoffJitter::FullJitter;
/// Equal jitter.
#[doc(alias = "EqualJitter")]
pub const EQUAL_JITTER: BackoffJitter = BackoffJitter::EqualJitter;
/// Decorrelated jitter.
#[doc(alias = "DecorrJitter")]
pub const DECORR_JITTER: BackoffJitter = BackoffJitter::DecorrJitter;

/// Exponential backoff function configuration.
///
/// This mirrors client-go `retry.BackoffFnCfg` at a high level. Use [`BackoffFnCfg::to_backoff`]
/// to convert this into a Rust [`Backoff`] schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BackoffFnCfg {
    /// Initial delay, in milliseconds, before applying the jitter policy.
    pub base_delay_ms: u64,
    /// Upper bound, in milliseconds, for any single computed delay.
    pub max_delay_ms: u64,
    /// Jitter strategy used when expanding delays between retry attempts.
    pub jitter: BackoffJitter,
}

impl BackoffFnCfg {
    /// Convert the config into a [`Backoff`] schedule.
    #[must_use]
    pub fn to_backoff(self, max_attempts: u32) -> Backoff {
        let (base_delay_ms, max_delay_ms) = match self.jitter {
            BackoffJitter::NoJitter => (self.base_delay_ms, self.max_delay_ms),
            BackoffJitter::FullJitter | BackoffJitter::DecorrJitter => {
                (self.base_delay_ms.max(1), self.max_delay_ms.max(1))
            }
            BackoffJitter::EqualJitter => (self.base_delay_ms.max(2), self.max_delay_ms.max(2)),
        };

        match self.jitter {
            BackoffJitter::NoJitter => {
                Backoff::no_jitter_backoff(base_delay_ms, max_delay_ms, max_attempts)
            }
            BackoffJitter::FullJitter => {
                Backoff::full_jitter_backoff(base_delay_ms, max_delay_ms, max_attempts)
            }
            BackoffJitter::EqualJitter => {
                Backoff::equal_jitter_backoff(base_delay_ms, max_delay_ms, max_attempts)
            }
            BackoffJitter::DecorrJitter => {
                Backoff::decorrelated_jitter_backoff(base_delay_ms, max_delay_ms, max_attempts)
            }
        }
    }
}

/// Create a new [`BackoffFnCfg`].
///
/// This mirrors client-go `retry.NewBackoffFnCfg`.
#[doc(alias = "NewBackoffFnCfg")]
#[must_use]
pub fn new_backoff_fn_cfg(
    base_delay_ms: u64,
    max_delay_ms: u64,
    jitter: BackoffJitter,
) -> BackoffFnCfg {
    BackoffFnCfg {
        base_delay_ms,
        max_delay_ms,
        jitter,
    }
}

fn default_max_attempts(jitter: BackoffJitter) -> u32 {
    match jitter {
        BackoffJitter::NoJitter => DEFAULT_NO_JITTER_MAX_ATTEMPTS,
        BackoffJitter::FullJitter | BackoffJitter::EqualJitter | BackoffJitter::DecorrJitter => {
            DEFAULT_MAX_ATTEMPTS
        }
    }
}

fn preset_config(name: &str, backoff_fn_cfg: BackoffFnCfg) -> Config {
    Config {
        name: name.to_owned(),
        backoff_fn_cfg: Some(backoff_fn_cfg),
        error: None,
    }
}

/// Create a named backoff config.
///
/// The stored error is kept for migration/public-API parity with client-go `retry.Config`; the
/// current Rust retry helpers only consume the backoff parameters.
#[doc(alias = "NewConfig")]
#[must_use]
pub fn new_config(
    name: impl Into<String>,
    backoff_fn_cfg: BackoffFnCfg,
    err: crate::Error,
) -> Config {
    Config {
        name: name.into(),
        backoff_fn_cfg: Some(backoff_fn_cfg),
        error: Some(Arc::new(err)),
    }
}

tokio::task_local! {
    static TASK_TXN_START_TS: u64;
}

/// Marker type used as a key for storing transaction start ts in task-local context.
///
/// This mirrors client-go `retry.TxnStartKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxnStartKey;

/// Returns the marker key for storing transaction start ts in task-local context.
///
/// This mirrors client-go `tikv.TxnStartKey`.
#[doc(alias = "TxnStartKey")]
#[must_use]
pub fn txn_start_key() -> TxnStartKey {
    TxnStartKey
}

/// Runs `future` with a task-local transaction start ts.
///
/// This mirrors the client-go pattern: `context.WithValue(ctx, TxnStartKey, startTS)`.
pub fn with_txn_start_ts<T, F>(start_ts: u64, future: F) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_TXN_START_TS.scope(start_ts, future)
}

/// Returns the task-local transaction start ts, if present.
#[must_use]
pub fn txn_start_ts() -> Option<u64> {
    TASK_TXN_START_TS.try_with(|ts| *ts).ok()
}

/// Returns whether `region_err` is a fake `EpochNotMatch` error.
///
/// Client-go treats `EpochNotMatch` without `current_regions` as a transient region miss instead of
/// a concrete replacement plan, so callers should back off before retrying.
#[doc(alias = "IsFakeRegionError")]
#[must_use]
pub fn is_fake_region_error(region_err: &errorpb::Error) -> bool {
    region_err
        .epoch_not_match
        .as_ref()
        .is_some_and(|epoch_not_match| epoch_not_match.current_regions.is_empty())
}

/// Sleeps according to `backoff` when `region_err` should retry with backoff.
///
/// This mirrors client-go `retry.MayBackoffForRegionError`: a real `EpochNotMatch` that already
/// carries replacement regions retries immediately, while fake `EpochNotMatch` and all other
/// region errors consume one delay from `backoff`. If the schedule is exhausted, the original
/// region error is returned.
#[doc(alias = "MayBackoffForRegionError")]
pub async fn may_backoff_for_region_error(
    region_err: &errorpb::Error,
    backoff: &mut Backoffer,
) -> Result<()> {
    if matches!(
        region_err.epoch_not_match.as_ref(),
        Some(epoch_not_match) if !epoch_not_match.current_regions.is_empty()
    ) {
        return Ok(());
    }

    match backoff.next_delay_duration() {
        Some(delay) => {
            sleep(delay).await;
            Ok(())
        }
        None => Err(Error::RegionError(Box::new(region_err.clone()))),
    }
}

/// Create a backoffer with an upper bound in milliseconds and optional variables.
///
/// This mirrors client-go `retry.NewBackofferWithVars`.
#[doc(alias = "NewBackofferWithVars")]
#[must_use]
pub fn new_backoffer_with_vars(max_sleep_ms: u64, vars: Option<&Variables>) -> Backoffer {
    let max_sleep_ms = match vars {
        Some(vars) => max_sleep_ms.saturating_mul(u64::from(vars.backoff_weight_factor())),
        None => max_sleep_ms,
    };
    new_backoffer(max_sleep_ms)
}

/// Create a backoffer with an upper bound in milliseconds.
///
/// This mirrors client-go `retry.NewBackoffer`.
#[doc(alias = "NewBackoffer")]
#[must_use]
pub fn new_backoffer(max_sleep_ms: u64) -> Backoffer {
    if max_sleep_ms < 2 {
        return Backoff::no_backoff();
    }

    // Best-effort: treat `max_sleep_ms` as the per-attempt delay cap.
    Backoff::equal_jitter_backoff(2, max_sleep_ms, DEFAULT_MAX_ATTEMPTS)
}

/// Create a backoffer for GC to resolve locks.
///
/// This mirrors client-go `tikv.NewGcResolveLockMaxBackoffer`, which uses a max sleep time of
/// 100 seconds.
#[doc(alias = "NewGcResolveLockMaxBackoffer")]
#[must_use]
pub fn new_gc_resolve_lock_max_backoffer() -> Backoffer {
    const GC_RESOLVE_LOCK_MAX_BACKOFF_MS: u64 = 100_000;
    new_backoffer(GC_RESOLVE_LOCK_MAX_BACKOFF_MS)
}

/// Create a noop backoffer (never sleeps).
///
/// This mirrors client-go `retry.NewNoopBackoff` / `tikv.NewNoopBackoff`.
#[doc(alias = "NewNoopBackoff")]
#[must_use]
pub fn new_noop_backoff() -> Backoffer {
    Backoff::no_backoff()
}

/// Default backoff config for region misses.
///
/// This mirrors client-go `retry.BoRegionMiss`.
#[doc(alias = "BoRegionMiss")]
#[must_use]
pub fn bo_region_miss() -> Config {
    preset_config("regionMiss", new_backoff_fn_cfg(2, 500, NO_JITTER))
}

/// Default backoff config for region scheduling.
///
/// This mirrors client-go `retry.BoRegionScheduling`.
#[doc(alias = "BoRegionScheduling")]
#[must_use]
pub fn bo_region_scheduling() -> Config {
    preset_config("regionScheduling", new_backoff_fn_cfg(2, 500, NO_JITTER))
}

/// Default backoff config for TiKV RPC.
///
/// This mirrors client-go `retry.BoTiKVRPC`.
#[doc(alias = "BoTiKVRPC")]
#[must_use]
pub fn bo_tikv_rpc() -> Config {
    preset_config("tikvRPC", new_backoff_fn_cfg(100, 2000, EQUAL_JITTER))
}

/// Default backoff config for TiFlash RPC.
///
/// This mirrors client-go `retry.BoTiFlashRPC`.
#[doc(alias = "BoTiFlashRPC")]
#[must_use]
pub fn bo_tiflash_rpc() -> Config {
    preset_config("tiflashRPC", new_backoff_fn_cfg(100, 2000, EQUAL_JITTER))
}

/// Default backoff config for transaction lock resolution.
///
/// This mirrors client-go `retry.BoTxnLock`.
#[doc(alias = "BoTxnLock")]
#[must_use]
pub fn bo_txn_lock() -> Config {
    preset_config("txnLock", new_backoff_fn_cfg(100, 3000, EQUAL_JITTER))
}

/// Default backoff config for PD RPC.
///
/// This mirrors client-go `retry.BoPDRPC`.
#[doc(alias = "BoPDRPC")]
#[must_use]
pub fn bo_pd_rpc() -> Config {
    preset_config("pdRPC", new_backoff_fn_cfg(500, 3000, EQUAL_JITTER))
}

/// Default backoff config for TiKV server busy errors.
///
/// This mirrors client-go `retry.BoTiKVServerBusy`.
#[doc(alias = "BoTiKVServerBusy")]
#[must_use]
pub fn bo_tikv_server_busy() -> Config {
    preset_config(
        "tikvServerBusy",
        new_backoff_fn_cfg(2000, 10_000, EQUAL_JITTER),
    )
}

/// Default backoff config for TiFlash server busy errors.
///
/// This mirrors client-go `retry.BoTiFlashServerBusy`.
#[doc(alias = "BoTiFlashServerBusy")]
#[must_use]
pub fn bo_tiflash_server_busy() -> Config {
    preset_config(
        "tiflashServerBusy",
        new_backoff_fn_cfg(2000, 10_000, EQUAL_JITTER),
    )
}

/// Default backoff config for TiKV disk full errors.
///
/// This mirrors client-go `retry.BoTiKVDiskFull`.
#[doc(alias = "BoTiKVDiskFull")]
#[must_use]
pub fn bo_tikv_disk_full() -> Config {
    preset_config("tikvDiskFull", new_backoff_fn_cfg(500, 5000, NO_JITTER))
}

/// Default backoff config when region recovery is in progress.
///
/// This mirrors client-go `retry.BoRegionRecoveryInProgress`.
#[doc(alias = "BoRegionRecoveryInProgress")]
#[must_use]
pub fn bo_region_recovery_in_progress() -> Config {
    preset_config(
        "regionRecoveryInProgress",
        new_backoff_fn_cfg(100, 10_000, EQUAL_JITTER),
    )
}

/// Default backoff config when a transaction is not found.
///
/// This mirrors client-go `retry.BoTxnNotFound`.
#[doc(alias = "BoTxnNotFound")]
#[must_use]
pub fn bo_txn_not_found() -> Config {
    preset_config("txnNotFound", new_backoff_fn_cfg(2, 500, NO_JITTER))
}

/// Default backoff config for stale commands.
///
/// This mirrors client-go `retry.BoStaleCmd`.
#[doc(alias = "BoStaleCmd")]
#[must_use]
pub fn bo_stale_cmd() -> Config {
    preset_config("staleCommand", new_backoff_fn_cfg(2, 1000, NO_JITTER))
}

/// Default backoff config when max timestamp is not synced.
///
/// This mirrors client-go `retry.BoMaxTsNotSynced`.
#[doc(alias = "BoMaxTsNotSynced")]
#[must_use]
pub fn bo_max_ts_not_synced() -> Config {
    preset_config("maxTsNotSynced", new_backoff_fn_cfg(2, 500, NO_JITTER))
}

/// Default backoff config when commit ts lags behind TSO.
///
/// This mirrors client-go `retry.BoCommitTSLag`.
#[doc(alias = "BoCommitTSLag")]
#[must_use]
pub fn bo_commit_ts_lag() -> Config {
    preset_config("commitTsLag", new_backoff_fn_cfg(2, 500, NO_JITTER))
}

/// Default backoff config when the region is not initialized.
///
/// This mirrors client-go `retry.BoMaxRegionNotInitialized`.
#[doc(alias = "BoMaxRegionNotInitialized")]
#[must_use]
pub fn bo_region_not_initialized() -> Config {
    preset_config(
        "regionNotInitialized",
        new_backoff_fn_cfg(2, 1000, NO_JITTER),
    )
}

/// Default backoff config when store is a witness.
///
/// This mirrors client-go `retry.BoIsWitness`.
#[doc(alias = "BoIsWitness")]
#[must_use]
pub fn bo_is_witness() -> Config {
    preset_config("isWitness", new_backoff_fn_cfg(1000, 10_000, EQUAL_JITTER))
}

/// Default backoff config for lock-fast style retries.
///
/// This mirrors client-go `retry.BoTxnLockFast`.
#[doc(alias = "BoTxnLockFast")]
#[must_use]
pub fn bo_txn_lock_fast() -> Config {
    preset_config(
        TXN_LOCK_FAST_NAME,
        new_backoff_fn_cfg(2, 3000, EQUAL_JITTER),
    )
}

/// Default backoff config for lock-fast style retries using the provided variables.
#[must_use]
pub fn bo_txn_lock_fast_with_vars(vars: &Variables) -> Backoffer {
    bo_txn_lock_fast().to_backoff_with_vars(vars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::metapb;

    #[test]
    fn backoff_fn_cfg_to_backoff_maps_jitter_types() {
        let cfg = new_backoff_fn_cfg(2, 7, NO_JITTER);
        assert_eq!(cfg.to_backoff(3), Backoff::no_jitter_backoff(2, 7, 3));

        let cfg = new_backoff_fn_cfg(2, 7, FULL_JITTER);
        assert_eq!(cfg.to_backoff(3), Backoff::full_jitter_backoff(2, 7, 3));

        let cfg = new_backoff_fn_cfg(2, 7, EQUAL_JITTER);
        assert_eq!(cfg.to_backoff(3), Backoff::equal_jitter_backoff(2, 7, 3));

        let cfg = new_backoff_fn_cfg(2, 7, DECORR_JITTER);
        assert_eq!(
            cfg.to_backoff(3),
            Backoff::decorrelated_jitter_backoff(2, 7, 3)
        );
    }

    #[test]
    fn backoff_fn_cfg_clamps_equal_jitter_to_two_ms() {
        let cfg = new_backoff_fn_cfg(1, 1, EQUAL_JITTER);
        assert_eq!(cfg.to_backoff(3), Backoff::equal_jitter_backoff(2, 2, 3));
    }

    #[test]
    fn config_mutators_and_display_work() {
        let mut cfg = new_config(
            "tikvRPC",
            new_backoff_fn_cfg(2, 7, NO_JITTER),
            crate::Error::Unimplemented,
        );
        assert_eq!(cfg.base(), 2);
        assert_eq!(cfg.to_string(), "tikvRPC");
        assert!(cfg.error.is_some());
        assert_eq!(cfg.to_backoff(), Backoff::no_jitter_backoff(2, 7, 10));

        cfg.set_backoff_fn_cfg(new_backoff_fn_cfg(3, 9, FULL_JITTER));
        assert_eq!(cfg.base(), 3);
        assert_eq!(cfg.to_backoff(), Backoff::full_jitter_backoff(3, 9, 16));

        cfg.set_errors(crate::Error::InternalError {
            message: "override".to_owned(),
        });
        assert_eq!(
            cfg.error.as_deref().map(ToString::to_string).as_deref(),
            Some("override")
        );
    }

    #[test]
    fn bo_presets_match_expected_backoff_parameters() {
        assert_eq!(bo_region_miss().to_string(), "regionMiss");
        assert_eq!(
            bo_region_miss().to_backoff(),
            crate::backoff::DEFAULT_REGION_BACKOFF
        );
        assert_eq!(
            bo_region_scheduling().to_backoff(),
            crate::backoff::DEFAULT_REGION_BACKOFF
        );
        assert_eq!(
            bo_tikv_rpc().to_backoff(),
            Backoff::equal_jitter_backoff(100, 2000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_tiflash_rpc().to_backoff(),
            Backoff::equal_jitter_backoff(100, 2000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_txn_lock().to_backoff(),
            Backoff::equal_jitter_backoff(100, 3000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_pd_rpc().to_backoff(),
            Backoff::equal_jitter_backoff(500, 3000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_tikv_server_busy().to_backoff(),
            Backoff::equal_jitter_backoff(2000, 10_000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_tiflash_server_busy().to_backoff(),
            Backoff::equal_jitter_backoff(2000, 10_000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_tikv_disk_full().to_backoff(),
            Backoff::no_jitter_backoff(500, 5000, DEFAULT_NO_JITTER_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_region_recovery_in_progress().to_backoff(),
            Backoff::equal_jitter_backoff(100, 10_000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_txn_not_found().to_backoff(),
            crate::backoff::DEFAULT_REGION_BACKOFF
        );
        assert_eq!(
            bo_stale_cmd().to_backoff(),
            Backoff::no_jitter_backoff(2, 1000, DEFAULT_NO_JITTER_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_max_ts_not_synced().to_backoff(),
            crate::backoff::DEFAULT_REGION_BACKOFF
        );
        assert_eq!(
            bo_commit_ts_lag().to_backoff(),
            crate::backoff::DEFAULT_REGION_BACKOFF
        );
        assert_eq!(
            bo_region_not_initialized().to_backoff(),
            Backoff::no_jitter_backoff(2, 1000, DEFAULT_NO_JITTER_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_is_witness().to_backoff(),
            Backoff::equal_jitter_backoff(1000, 10_000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(bo_txn_lock_fast().base(), 2);
        assert_eq!(
            bo_txn_lock_fast().to_backoff(),
            Backoff::equal_jitter_backoff(10, 3000, DEFAULT_MAX_ATTEMPTS)
        );
    }

    #[test]
    fn new_backoffer_with_vars_scales_max_sleep_by_backoff_weight() {
        let vars = Variables {
            backoff_weight: 3,
            ..Variables::default()
        };
        assert_eq!(
            new_backoffer_with_vars(100, Some(&vars)),
            new_backoffer(300)
        );
    }

    #[test]
    fn bo_txn_lock_fast_with_vars_clamps_base_delay_to_two_ms() {
        let vars = Variables {
            backoff_lock_fast_ms: 1,
            ..Variables::default()
        };
        assert_eq!(
            bo_txn_lock_fast_with_vars(&vars),
            Backoff::equal_jitter_backoff(2, 3000, DEFAULT_MAX_ATTEMPTS)
        );
    }

    #[test]
    fn new_backoffer_clamps_small_max_sleep_to_no_backoff() {
        assert_eq!(new_backoffer(0), Backoff::no_backoff());
        assert_eq!(new_backoffer(1), Backoff::no_backoff());
    }

    #[test]
    fn new_backoffer_uses_equal_jitter_with_two_ms_base() {
        assert_eq!(
            new_backoffer(2),
            Backoff::equal_jitter_backoff(2, 2, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            new_backoffer(7),
            Backoff::equal_jitter_backoff(2, 7, DEFAULT_MAX_ATTEMPTS)
        );
    }

    #[test]
    fn gc_resolve_lock_backoffer_uses_client_go_max_sleep_default() {
        assert_eq!(new_gc_resolve_lock_max_backoffer(), new_backoffer(100_000));
    }

    #[tokio::test]
    async fn txn_start_ts_task_local_scope_restores_outer_value() {
        assert_eq!(txn_start_ts(), None);

        with_txn_start_ts(7, async {
            assert_eq!(txn_start_ts(), Some(7));

            with_txn_start_ts(9, async {
                assert_eq!(txn_start_ts(), Some(9));
            })
            .await;

            assert_eq!(txn_start_ts(), Some(7));
        })
        .await;

        assert_eq!(txn_start_ts(), None);
    }

    #[test]
    fn fake_region_error_only_matches_epoch_not_match_without_replacements() {
        assert!(is_fake_region_error(&errorpb::Error {
            epoch_not_match: Some(errorpb::EpochNotMatch::default()),
            ..Default::default()
        }));
        assert!(!is_fake_region_error(&errorpb::Error {
            epoch_not_match: Some(errorpb::EpochNotMatch {
                current_regions: vec![metapb::Region {
                    id: 1,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        }));
        assert!(!is_fake_region_error(&errorpb::Error {
            not_leader: Some(errorpb::NotLeader::default()),
            ..Default::default()
        }));
    }

    #[tokio::test]
    async fn may_backoff_for_region_error_matches_client_go_retry_behavior() {
        let real_epoch_not_match = errorpb::Error {
            epoch_not_match: Some(errorpb::EpochNotMatch {
                current_regions: vec![metapb::Region {
                    id: 1,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        let mut immediate_retry = Backoff::no_jitter_backoff(1, 1, 1);
        may_backoff_for_region_error(&real_epoch_not_match, &mut immediate_retry)
            .await
            .unwrap();
        assert_eq!(
            immediate_retry.next_delay_duration(),
            Some(std::time::Duration::from_millis(1))
        );

        let fake_epoch_not_match = errorpb::Error {
            epoch_not_match: Some(errorpb::EpochNotMatch::default()),
            ..Default::default()
        };
        let mut delayed_retry = Backoff::no_jitter_backoff(1, 1, 1);
        may_backoff_for_region_error(&fake_epoch_not_match, &mut delayed_retry)
            .await
            .unwrap();
        assert_eq!(delayed_retry.next_delay_duration(), None);

        let err = may_backoff_for_region_error(
            &errorpb::Error {
                server_is_busy: Some(errorpb::ServerIsBusy::default()),
                ..Default::default()
            },
            &mut Backoff::no_backoff(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::RegionError(_)));
    }
}
