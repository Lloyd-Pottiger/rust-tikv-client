//! Client-go style retry/backoff helpers.
//!
//! This module mirrors client-go `config/retry` at a high level.
//!
//! Note: client-go's `retry.Backoffer` bounds the *total* sleep time of a retry loop via
//! `context.Context`. The Rust client uses [`crate::Backoff`] as a backoff *schedule* with a
//! bounded number of attempts. The helpers in this module provide best-effort presets and
//! constructors that are convenient when implementing custom retry loops.

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

/// Create a backoffer with an upper bound in milliseconds and optional variables.
///
/// This mirrors client-go `retry.NewBackofferWithVars`.
#[doc(alias = "NewBackofferWithVars")]
#[must_use]
pub fn new_backoffer_with_vars(max_sleep_ms: u64, _vars: Option<&Variables>) -> Backoffer {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bo_presets_match_expected_backoff_parameters() {
        assert_eq!(bo_region_miss(), crate::backoff::DEFAULT_REGION_BACKOFF);
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
}
