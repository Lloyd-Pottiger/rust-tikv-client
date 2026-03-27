//! Client-go style retry/backoff helpers.
//!
//! This module mirrors client-go `config/retry` at a high level.
//!
//! Note: client-go's `retry.Backoffer` bounds the *total* sleep time of a retry loop via
//! `context.Context`. The Rust client uses [`crate::Backoff`] as a backoff *schedule* with a
//! bounded number of attempts. The helpers in this module provide best-effort presets and
//! constructors that are convenient when implementing custom retry loops.

use std::future::Future;

use crate::Backoff;
use crate::Variables;

/// Client-go style `Backoffer`.
///
/// In the Rust client, this is an alias of [`crate::Backoff`].
pub type Backoffer = Backoff;

/// Client-go style backoff configuration.
///
/// In client-go, `retry.Config` is immutable and the backoffer holds per-config state (attempts,
/// last sleep). In the Rust client, [`crate::Backoff`] includes both the configuration and the
/// attempt counter, so this is an alias.
pub type Config = Backoff;

const DEFAULT_MAX_ATTEMPTS: u32 = 16;
const DEFAULT_NO_JITTER_MAX_ATTEMPTS: u32 = 10;

/// Jitter type for exponential backoff.
///
/// This is a Rust-friendly mirror of client-go `config/retry` jitter constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BackoffJitter {
    NoJitter,
    FullJitter,
    EqualJitter,
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
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
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
    crate::backoff::DEFAULT_REGION_BACKOFF
}

/// Default backoff config for region scheduling.
///
/// This mirrors client-go `retry.BoRegionScheduling`.
#[doc(alias = "BoRegionScheduling")]
#[must_use]
pub fn bo_region_scheduling() -> Config {
    crate::backoff::DEFAULT_REGION_BACKOFF
}

/// Default backoff config for TiKV RPC.
///
/// This mirrors client-go `retry.BoTiKVRPC`.
#[doc(alias = "BoTiKVRPC")]
#[must_use]
pub fn bo_tikv_rpc() -> Config {
    Backoff::equal_jitter_backoff(100, 2000, DEFAULT_MAX_ATTEMPTS)
}

/// Default backoff config for TiFlash RPC.
///
/// This mirrors client-go `retry.BoTiFlashRPC`.
#[doc(alias = "BoTiFlashRPC")]
#[must_use]
pub fn bo_tiflash_rpc() -> Config {
    Backoff::equal_jitter_backoff(100, 2000, DEFAULT_MAX_ATTEMPTS)
}

/// Default backoff config for transaction lock resolution.
///
/// This mirrors client-go `retry.BoTxnLock`.
#[doc(alias = "BoTxnLock")]
#[must_use]
pub fn bo_txn_lock() -> Config {
    Backoff::equal_jitter_backoff(100, 3000, DEFAULT_MAX_ATTEMPTS)
}

/// Default backoff config for PD RPC.
///
/// This mirrors client-go `retry.BoPDRPC`.
#[doc(alias = "BoPDRPC")]
#[must_use]
pub fn bo_pd_rpc() -> Config {
    Backoff::equal_jitter_backoff(500, 3000, DEFAULT_MAX_ATTEMPTS)
}

/// Default backoff config for TiKV server busy errors.
///
/// This mirrors client-go `retry.BoTiKVServerBusy`.
#[doc(alias = "BoTiKVServerBusy")]
#[must_use]
pub fn bo_tikv_server_busy() -> Config {
    Backoff::equal_jitter_backoff(2000, 10_000, DEFAULT_MAX_ATTEMPTS)
}

/// Default backoff config for TiFlash server busy errors.
///
/// This mirrors client-go `retry.BoTiFlashServerBusy`.
#[doc(alias = "BoTiFlashServerBusy")]
#[must_use]
pub fn bo_tiflash_server_busy() -> Config {
    Backoff::equal_jitter_backoff(2000, 10_000, DEFAULT_MAX_ATTEMPTS)
}

/// Default backoff config for TiKV disk full errors.
///
/// This mirrors client-go `retry.BoTiKVDiskFull`.
#[doc(alias = "BoTiKVDiskFull")]
#[must_use]
pub fn bo_tikv_disk_full() -> Config {
    Backoff::no_jitter_backoff(500, 5000, DEFAULT_NO_JITTER_MAX_ATTEMPTS)
}

/// Default backoff config when region recovery is in progress.
///
/// This mirrors client-go `retry.BoRegionRecoveryInProgress`.
#[doc(alias = "BoRegionRecoveryInProgress")]
#[must_use]
pub fn bo_region_recovery_in_progress() -> Config {
    Backoff::equal_jitter_backoff(100, 10_000, DEFAULT_MAX_ATTEMPTS)
}

/// Default backoff config when a transaction is not found.
///
/// This mirrors client-go `retry.BoTxnNotFound`.
#[doc(alias = "BoTxnNotFound")]
#[must_use]
pub fn bo_txn_not_found() -> Config {
    crate::backoff::DEFAULT_REGION_BACKOFF
}

/// Default backoff config for stale commands.
///
/// This mirrors client-go `retry.BoStaleCmd`.
#[doc(alias = "BoStaleCmd")]
#[must_use]
pub fn bo_stale_cmd() -> Config {
    Backoff::no_jitter_backoff(2, 1000, DEFAULT_NO_JITTER_MAX_ATTEMPTS)
}

/// Default backoff config when max timestamp is not synced.
///
/// This mirrors client-go `retry.BoMaxTsNotSynced`.
#[doc(alias = "BoMaxTsNotSynced")]
#[must_use]
pub fn bo_max_ts_not_synced() -> Config {
    crate::backoff::DEFAULT_REGION_BACKOFF
}

/// Default backoff config when commit ts lags behind TSO.
///
/// This mirrors client-go `retry.BoCommitTSLag`.
#[doc(alias = "BoCommitTSLag")]
#[must_use]
pub fn bo_commit_ts_lag() -> Config {
    crate::backoff::DEFAULT_REGION_BACKOFF
}

/// Default backoff config when the region is not initialized.
///
/// This mirrors client-go `retry.BoMaxRegionNotInitialized`.
#[doc(alias = "BoMaxRegionNotInitialized")]
#[must_use]
pub fn bo_region_not_initialized() -> Config {
    Backoff::no_jitter_backoff(2, 1000, DEFAULT_NO_JITTER_MAX_ATTEMPTS)
}

/// Default backoff config when store is a witness.
///
/// This mirrors client-go `retry.BoIsWitness`.
#[doc(alias = "BoIsWitness")]
#[must_use]
pub fn bo_is_witness() -> Config {
    Backoff::equal_jitter_backoff(1000, 10_000, DEFAULT_MAX_ATTEMPTS)
}

/// Default backoff config for lock-fast style retries.
///
/// This mirrors client-go `retry.BoTxnLockFast`.
#[doc(alias = "BoTxnLockFast")]
#[must_use]
pub fn bo_txn_lock_fast() -> Config {
    bo_txn_lock_fast_with_vars(&Variables::default())
}

/// Default backoff config for lock-fast style retries using the provided variables.
#[must_use]
pub fn bo_txn_lock_fast_with_vars(vars: &Variables) -> Config {
    // client-go loads the base delay from `Variables.BackoffLockFast` when creating the backoff
    // function. Keep the same clamping behavior to avoid invalid jitter ranges.
    let base_delay_ms = vars.backoff_lock_fast_ms.max(2);
    Backoff::equal_jitter_backoff(base_delay_ms, 3000, DEFAULT_MAX_ATTEMPTS)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn bo_presets_match_expected_backoff_parameters() {
        assert_eq!(bo_region_miss(), crate::backoff::DEFAULT_REGION_BACKOFF);
        assert_eq!(
            bo_region_scheduling(),
            crate::backoff::DEFAULT_REGION_BACKOFF
        );
        assert_eq!(
            bo_tikv_rpc(),
            Backoff::equal_jitter_backoff(100, 2000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_tiflash_rpc(),
            Backoff::equal_jitter_backoff(100, 2000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_txn_lock(),
            Backoff::equal_jitter_backoff(100, 3000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_pd_rpc(),
            Backoff::equal_jitter_backoff(500, 3000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_tikv_server_busy(),
            Backoff::equal_jitter_backoff(2000, 10_000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_tiflash_server_busy(),
            Backoff::equal_jitter_backoff(2000, 10_000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_tikv_disk_full(),
            Backoff::no_jitter_backoff(500, 5000, DEFAULT_NO_JITTER_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_region_recovery_in_progress(),
            Backoff::equal_jitter_backoff(100, 10_000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(bo_txn_not_found(), crate::backoff::DEFAULT_REGION_BACKOFF);
        assert_eq!(
            bo_stale_cmd(),
            Backoff::no_jitter_backoff(2, 1000, DEFAULT_NO_JITTER_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_max_ts_not_synced(),
            crate::backoff::DEFAULT_REGION_BACKOFF
        );
        assert_eq!(bo_commit_ts_lag(), crate::backoff::DEFAULT_REGION_BACKOFF);
        assert_eq!(
            bo_region_not_initialized(),
            Backoff::no_jitter_backoff(2, 1000, DEFAULT_NO_JITTER_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_is_witness(),
            Backoff::equal_jitter_backoff(1000, 10_000, DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(
            bo_txn_lock_fast(),
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
}
