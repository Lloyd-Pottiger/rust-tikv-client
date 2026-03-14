#![cfg(feature = "integration-tests")]

mod common;

use std::collections::HashSet;
use std::iter::FromIterator;
use std::time::Duration;

use common::*;
use fail::FailScenario;
use log::info;
use rand::thread_rng;
use rand::Rng;
use serial_test::serial;
use tikv_client::transaction::Client;
use tikv_client::transaction::HeartbeatOption;
use tikv_client::transaction::ResolveLocksOptions;
use tikv_client::Backoff;
use tikv_client::CheckLevel;
use tikv_client::Config;
use tikv_client::Result;
use tikv_client::RetryOptions;
use tikv_client::TimestampExt;
use tikv_client::TransactionClient;
use tikv_client::TransactionOptions;

#[tokio::test]
#[serial]
async fn txn_optimistic_heartbeat() -> Result<()> {
    init().await?;
    let scenario = FailScenario::setup();
    fail::cfg("after-prewrite", "sleep(6000)").unwrap();
    defer! {{
        fail::cfg("after-prewrite", "off").unwrap();
    }}

    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    // CheckLevel::Panic makes the case unstable, change to Warn level for now.
    // See https://github.com/tikv/client-rust/issues/389
    let mut heartbeat_txn = client
        .begin_with_options(
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(1)))
                .drop_check(CheckLevel::Warn),
        )
        .await?;
    heartbeat_txn.put(key1.clone(), "foo").await.unwrap();

    let mut txn_without_heartbeat = client
        .begin_with_options(
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::Warn),
        )
        .await?;
    txn_without_heartbeat
        .put(key2.clone(), "fooo")
        .await
        .unwrap();

    let heartbeat_txn_handle = tokio::task::spawn_blocking(move || {
        assert!(futures::executor::block_on(heartbeat_txn.commit()).is_ok())
    });
    let txn_without_heartbeat_handle = tokio::task::spawn_blocking(move || {
        assert!(futures::executor::block_on(txn_without_heartbeat.commit()).is_err())
    });

    // inital TTL is 3 seconds, before which TTL is valid regardless of heartbeat.
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    fail::cfg("after-prewrite", "off").unwrap();

    // use other txns to check these locks
    let mut t3 = client
        .begin_with_options(
            TransactionOptions::new_optimistic()
                .no_resolve_locks()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::Warn),
        )
        .await?;
    t3.put(key1.clone(), "gee").await?;
    assert!(t3.commit().await.is_err());

    let mut t4 = client
        .begin_with_options(TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn))
        .await?;
    t4.put(key2.clone(), "geee").await?;
    t4.commit().await?;

    heartbeat_txn_handle.await.unwrap();
    txn_without_heartbeat_handle.await.unwrap();

    scenario.teardown();

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_cleanup_locks_batch_size() -> Result<()> {
    init().await?;
    let scenario = FailScenario::setup();
    let full_range = ..;

    fail::cfg("after-prewrite", "return").unwrap();
    fail::cfg("before-cleanup-locks", "return").unwrap();
    defer! {{
        fail::cfg("after-prewrite", "off").unwrap();
        fail::cfg("before-cleanup-locks", "off").unwrap();
    }}

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let keys = write_data(&client, true, true).await?;
    assert_eq!(count_locks(&client).await?, keys.len());

    let safepoint = client.current_timestamp().await?;
    let options = ResolveLocksOptions {
        async_commit_only: false,
        batch_size: 4,
    };
    let res = client
        .cleanup_locks(full_range, &safepoint, options)
        .await?;

    assert_eq!(res.resolved_locks, keys.len());
    assert_eq!(count_locks(&client).await?, keys.len());

    scenario.teardown();
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_cleanup_async_commit_locks() -> Result<()> {
    init().await?;
    let scenario = FailScenario::setup();
    let full_range = ..;

    // no commit
    {
        info!("test no commit");
        fail::cfg("after-prewrite", "return").unwrap();
        defer! {
            fail::cfg("after-prewrite", "off").unwrap()
        }

        let client = TransactionClient::new_with_config(
            pd_addrs(),
            Config::default().with_default_keyspace(),
        )
        .await?;
        let keys = write_data(&client, true, true).await?;
        assert_eq!(count_locks(&client).await?, keys.len());

        let safepoint = client.current_timestamp().await?;
        let options = ResolveLocksOptions {
            async_commit_only: true,
            ..Default::default()
        };
        client
            .cleanup_locks(full_range, &safepoint, options)
            .await?;

        must_committed(&client, keys).await;
        assert_eq!(count_locks(&client).await?, 0);
    }

    // partial commit
    {
        info!("test partial commit");
        let percent = 50;
        fail::cfg("before-commit-secondary", &format!("return({percent})")).unwrap();
        defer! {
            fail::cfg("before-commit-secondary", "off").unwrap()
        }

        let client = TransactionClient::new_with_config(
            pd_addrs(),
            Config::default().with_default_keyspace(),
        )
        .await?;
        let keys = write_data(&client, true, false).await?;
        // Wait for async commit to complete.
        let expected = keys.len() * percent / 100;
        let remaining = wait_for_locks_count(&client, expected).await?;
        assert_eq!(remaining, expected);

        let safepoint = client.current_timestamp().await?;
        let options = ResolveLocksOptions {
            async_commit_only: true,
            ..Default::default()
        };
        client
            .cleanup_locks(full_range, &safepoint, options)
            .await?;

        must_committed(&client, keys).await;
        assert_eq!(count_locks(&client).await?, 0);
    }

    // all committed
    {
        info!("test all committed");
        let client = TransactionClient::new_with_config(
            pd_addrs(),
            Config::default().with_default_keyspace(),
        )
        .await?;
        let keys = write_data(&client, true, false).await?;

        let safepoint = client.current_timestamp().await?;
        let options = ResolveLocksOptions {
            async_commit_only: true,
            ..Default::default()
        };
        client
            .cleanup_locks(full_range, &safepoint, options)
            .await?;

        must_committed(&client, keys).await;
        assert_eq!(count_locks(&client).await?, 0);
    }

    // rollback
    {
        info!("test rollback");
        fail::cfg("after-prewrite", "return").unwrap();
        defer! {
            fail::cfg("after-prewrite", "off").unwrap()
        }

        let client = TransactionClient::new_with_config(
            pd_addrs(),
            Config::default().with_default_keyspace(),
        )
        .await?;

        let primary_key = b"rollback-primary".to_vec();
        let secondary_key = b"rollback-secondary".to_vec();

        let mut txn = client
            .begin_with_options(
                TransactionOptions::new_optimistic()
                    .use_async_commit()
                    .retry_options(RetryOptions {
                        region_backoff: REGION_BACKOFF,
                        lock_backoff: OPTIMISTIC_BACKOFF,
                    })
                    .drop_check(CheckLevel::Warn),
            )
            .await?;
        txn.put(primary_key.clone(), primary_key.clone()).await?;
        txn.put(secondary_key.clone(), secondary_key.clone())
            .await?;
        txn.commit()
            .await
            .expect_err("expected commit to fail after prewrite");

        let mut secondary_end = secondary_key.clone();
        secondary_end.push(0);
        client
            .unsafe_destroy_range(secondary_key.clone()..secondary_end)
            .await?;

        let safepoint = client.current_timestamp().await?;
        let options = ResolveLocksOptions {
            async_commit_only: true,
            ..Default::default()
        };
        client
            .cleanup_locks(full_range, &safepoint, options)
            .await?;

        let keys = HashSet::from_iter([primary_key, secondary_key]);
        must_rollbacked(&client, keys).await;
        assert_eq!(count_locks(&client).await?, 0);
    }

    // region error
    {
        info!("test region error");
        fail::cfg("after-prewrite", "return").unwrap();
        fail::cfg("kv_resolve_lock_region_error", "3*return").unwrap();
        defer! {{
            fail::cfg("after-prewrite", "off").unwrap();
            fail::cfg("kv_resolve_lock_region_error", "off").unwrap();
        }}

        let client = TransactionClient::new_with_config(
            pd_addrs(),
            Config::default().with_default_keyspace(),
        )
        .await?;

        let primary_key = b"region-error-primary".to_vec();
        let secondary_key = b"region-error-secondary".to_vec();

        let mut txn = client
            .begin_with_options(
                TransactionOptions::new_optimistic()
                    .use_async_commit()
                    .retry_options(RetryOptions {
                        region_backoff: REGION_BACKOFF,
                        lock_backoff: OPTIMISTIC_BACKOFF,
                    })
                    .drop_check(CheckLevel::Warn),
            )
            .await?;
        txn.put(primary_key.clone(), primary_key.clone()).await?;
        txn.put(secondary_key.clone(), secondary_key.clone())
            .await?;
        txn.commit()
            .await
            .expect_err("expected commit to fail after prewrite");

        let locks_before = count_locks_in_range(&client, &primary_key, &secondary_key).await?;
        assert!(locks_before > 0, "expected locks before cleanup");

        let safepoint = client.current_timestamp().await?;
        let options = ResolveLocksOptions {
            async_commit_only: true,
            ..Default::default()
        };
        client
            .cleanup_locks(full_range, &safepoint, options)
            .await?;

        let keys = HashSet::from_iter([primary_key, secondary_key]);
        must_committed(&client, keys).await;
        assert_eq!(count_locks(&client).await?, 0);
    }

    scenario.teardown();
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_cleanup_range_async_commit_locks() -> Result<()> {
    init().await?;
    let scenario = FailScenario::setup();
    info!("test range clean lock");
    fail::cfg("after-prewrite", "return").unwrap();
    defer! {
        fail::cfg("after-prewrite", "off").unwrap()
    }

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let keys = write_data(&client, true, true).await?;
    assert_eq!(count_locks(&client).await?, keys.len());

    info!("total keys' count {}", keys.len());
    let mut sorted_keys: Vec<Vec<u8>> = Vec::from_iter(keys.clone());
    sorted_keys.sort();
    let start_key = sorted_keys[1].clone();
    let end_key = sorted_keys[sorted_keys.len() - 2].clone();

    let safepoint = client.current_timestamp().await?;
    let options = ResolveLocksOptions {
        async_commit_only: true,
        ..Default::default()
    };
    client
        .cleanup_locks(start_key.clone()..end_key.clone(), &safepoint, options)
        .await?;
    // `cleanup_locks` will resolve primary locks as well. So just check the remaining locks in the range.
    let remaining = wait_for_locks_count_in_range(&client, &start_key, &end_key, 0).await?;
    assert_eq!(remaining, 0);

    // cleanup all locks to avoid affecting following cases.
    let options = ResolveLocksOptions {
        async_commit_only: false,
        ..Default::default()
    };
    client.cleanup_locks(.., &safepoint, options).await?;
    must_committed(&client, keys).await;
    assert_eq!(count_locks(&client).await?, 0);

    scenario.teardown();
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_resolve_locks() -> Result<()> {
    init().await?;
    let scenario = FailScenario::setup();

    fail::cfg("after-prewrite", "return").unwrap();
    defer! {{
        fail::cfg("after-prewrite", "off").unwrap();
    }}

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;
    let key = b"resolve-locks-key".to_vec();
    let keys = HashSet::from_iter(vec![key.clone()]);
    let mut txn = client
        .begin_with_options(
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::Warn),
        )
        .await?;
    txn.put(key.clone(), b"value".to_vec()).await?;
    assert!(txn.commit().await.is_err());

    let safepoint = client.current_timestamp().await?;
    let locks = client.scan_locks(&safepoint, vec![].., 1024).await?;
    assert!(locks.iter().any(|lock| lock.key == key));

    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    let start_version = client.current_timestamp().await?;
    let live_locks = client
        .resolve_locks(locks, start_version, OPTIMISTIC_BACKOFF)
        .await?;
    assert!(live_locks.is_empty());
    assert_eq!(count_locks(&client).await?, 0);
    must_rollbacked(&client, keys).await;

    scenario.teardown();
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_resolve_locks_for_read_classifies_committed_and_resolved() -> Result<()> {
    init().await?;
    let scenario = FailScenario::setup();

    fail::cfg("before-commit-secondary", "return(0)").unwrap();
    defer! {{
        fail::cfg("before-commit-secondary", "off").unwrap();
    }}

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    let mut rng = thread_rng();
    let suffix: u64 = rng.gen();

    // Case 1: commit_ts <= read_start_ts => committed lock is accessible.
    let primary_visible = format!("resolve-locks-for-read-visible-{suffix}-primary").into_bytes();
    let secondary_visible =
        format!("resolve-locks-for-read-visible-{suffix}-secondary").into_bytes();
    let visible_value = b"visible-value".to_vec();

    let mut txn = client
        .begin_with_options(
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::Warn),
        )
        .await?;
    txn.put(primary_visible.clone(), b"primary-value".to_vec())
        .await?;
    txn.put(secondary_visible.clone(), visible_value.clone())
        .await?;
    let start_ts = txn.start_ts();
    let commit_ts = txn
        .commit()
        .await?
        .expect("expected commit ts for visible transaction");

    let read_start_ts = client.current_timestamp().await?;
    assert!(
        commit_ts.version() <= read_start_ts.version(),
        "expected commit_ts <= read_start_ts for committed lock read"
    );

    let mut secondary_visible_end = secondary_visible.clone();
    secondary_visible_end.push(0);
    let safepoint = client.current_timestamp().await?;
    let locks = client
        .scan_locks(
            &safepoint,
            secondary_visible.clone()..secondary_visible_end.clone(),
            16,
        )
        .await?;
    assert!(
        locks.iter().any(|lock| lock.key == secondary_visible),
        "expected secondary lock to remain after commit when secondary commit is skipped"
    );

    let result = client
        .resolve_locks_for_read(locks, read_start_ts, false)
        .await?;
    assert_eq!(result.ms_before_txn_expired, 0);
    assert!(result.live_locks.is_empty());
    assert!(result.resolved_locks.is_empty());
    assert_eq!(result.committed_locks, vec![start_ts]);

    let remaining =
        wait_for_locks_count_in_range(&client, &secondary_visible, &secondary_visible_end, 0)
            .await?;
    assert_eq!(remaining, 0);

    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::default(),
    );
    assert_eq!(
        snapshot.get(secondary_visible.clone()).await?,
        Some(visible_value)
    );

    // Case 2: commit_ts > read_start_ts => committed lock is ignored for that snapshot read.
    let primary_future = format!("resolve-locks-for-read-future-{suffix}-primary").into_bytes();
    let secondary_future = format!("resolve-locks-for-read-future-{suffix}-secondary").into_bytes();

    let mut txn = client
        .begin_with_options(
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::Warn),
        )
        .await?;
    txn.put(primary_future.clone(), b"primary-value".to_vec())
        .await?;
    txn.put(secondary_future.clone(), b"future-value".to_vec())
        .await?;
    let start_ts = txn.start_ts();

    let read_start_ts = client.current_timestamp().await?;
    let commit_ts = txn
        .commit()
        .await?
        .expect("expected commit ts for future transaction");
    assert!(
        commit_ts.version() > read_start_ts.version(),
        "expected commit_ts > read_start_ts for future read classification"
    );

    let mut secondary_future_end = secondary_future.clone();
    secondary_future_end.push(0);
    let safepoint = client.current_timestamp().await?;
    let locks = client
        .scan_locks(
            &safepoint,
            secondary_future.clone()..secondary_future_end.clone(),
            16,
        )
        .await?;
    assert!(
        locks.iter().any(|lock| lock.key == secondary_future),
        "expected secondary lock to remain after commit when secondary commit is skipped"
    );

    let result = client
        .resolve_locks_for_read(locks, read_start_ts.clone(), false)
        .await?;
    assert_eq!(result.ms_before_txn_expired, 0);
    assert!(result.live_locks.is_empty());
    assert!(result.committed_locks.is_empty());
    assert_eq!(result.resolved_locks, vec![start_ts]);

    let remaining =
        wait_for_locks_count_in_range(&client, &secondary_future, &secondary_future_end, 0).await?;
    assert_eq!(remaining, 0);

    let mut snapshot = client.snapshot(read_start_ts, TransactionOptions::default());
    assert_eq!(snapshot.get(secondary_future).await?, None);

    scenario.teardown();
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_resolve_locks_for_read_zero_min_commit_ts() -> Result<()> {
    init().await?;
    let scenario = FailScenario::setup();

    // Keep the transaction in prewrite state.
    fail::cfg("after-prewrite", "return").unwrap();
    defer! {{
        fail::cfg("after-prewrite", "off").unwrap();
    }}

    let client =
        TransactionClient::new_with_config(pd_addrs(), Config::default().with_default_keyspace())
            .await?;

    let suffix: u64 = thread_rng().gen();
    let key = format!("resolve-locks-for-read-zero-min-commit-ts-{suffix}").into_bytes();
    let mut end_key = key.clone();
    end_key.push(0);

    let mut txn = client
        .begin_with_options(
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::Warn),
        )
        .await?;
    txn.put(key.clone(), b"value".to_vec()).await?;
    let start_ts = txn.start_ts();

    // Port of client-go `TestZeroMinCommitTS`.
    let failpoint = format!("return({start_ts})");
    fail::cfg("mock_zero_min_commit_ts", &failpoint).unwrap();
    defer! {{
        fail::cfg("mock_zero_min_commit_ts", "off").unwrap();
    }}

    assert!(txn.commit().await.is_err());

    let safepoint = client.current_timestamp().await?;
    let locks = client
        .scan_locks(&safepoint, key.clone()..end_key.clone(), 1024)
        .await?;
    let lock = locks
        .into_iter()
        .find(|lock| lock.key == key)
        .expect("expected lock to be present after prewrite");
    assert_eq!(lock.lock_version, start_ts);
    assert_eq!(lock.min_commit_ts, 0);

    let result = client
        .resolve_locks_for_read(
            vec![lock.clone()],
            tikv_client::Timestamp::from_version(0),
            true,
        )
        .await?;
    assert!(result.resolved_locks.is_empty());
    assert!(result.committed_locks.is_empty());
    assert_eq!(result.live_locks.len(), 1);
    assert!(result.ms_before_txn_expired > 0);

    let result = client
        .resolve_locks_for_read(
            vec![lock.clone()],
            tikv_client::Timestamp::from_version(u64::MAX),
            true,
        )
        .await?;
    assert_eq!(result.ms_before_txn_expired, 0);
    assert!(result.live_locks.is_empty());
    assert!(result.committed_locks.is_empty());
    assert_eq!(result.resolved_locks, vec![start_ts]);

    let mut force_cleanup = lock;
    force_cleanup.lock_ttl = 0;
    let start_version = client.current_timestamp().await?;
    let live_locks = client
        .resolve_locks(vec![force_cleanup], start_version, OPTIMISTIC_BACKOFF)
        .await?;
    assert!(live_locks.is_empty());
    let remaining = wait_for_locks_count_in_range(&client, &key, &end_key, 0).await?;
    assert_eq!(remaining, 0);

    scenario.teardown();
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_cleanup_2pc_locks() -> Result<()> {
    init().await?;
    let scenario = FailScenario::setup();
    let full_range = ..;

    // no commit
    {
        info!("test no commit");
        fail::cfg("after-prewrite", "return").unwrap();
        defer! {
            fail::cfg("after-prewrite", "off").unwrap()
        }

        let client = TransactionClient::new_with_config(
            pd_addrs(),
            Config::default().with_default_keyspace(),
        )
        .await?;
        let keys = write_data(&client, false, true).await?;
        assert_eq!(count_locks(&client).await?, keys.len());

        let safepoint = client.current_timestamp().await?;
        {
            let options = ResolveLocksOptions {
                async_commit_only: true, // Skip 2pc locks.
                ..Default::default()
            };
            client
                .cleanup_locks(full_range, &safepoint, options)
                .await?;
            assert_eq!(count_locks(&client).await?, keys.len());
        }
        let options = ResolveLocksOptions {
            async_commit_only: false,
            ..Default::default()
        };
        client
            .cleanup_locks(full_range, &safepoint, options)
            .await?;

        must_rollbacked(&client, keys).await;
        assert_eq!(count_locks(&client).await?, 0);
    }

    // all committed
    {
        info!("test all committed");
        let client = TransactionClient::new_with_config(
            pd_addrs(),
            Config::default().with_default_keyspace(),
        )
        .await?;
        let keys = write_data(&client, false, false).await?;
        assert_eq!(wait_for_locks_count(&client, 0).await?, 0);

        let safepoint = client.current_timestamp().await?;
        let options = ResolveLocksOptions {
            async_commit_only: false,
            ..Default::default()
        };
        client
            .cleanup_locks(full_range, &safepoint, options)
            .await?;

        must_committed(&client, keys).await;
        assert_eq!(count_locks(&client).await?, 0);
    }

    scenario.teardown();
    Ok(())
}

async fn must_committed(client: &TransactionClient, keys: HashSet<Vec<u8>>) {
    let ts = client.current_timestamp().await.unwrap();
    let mut snapshot = client.snapshot(ts, TransactionOptions::default());
    for key in keys {
        let val = snapshot.get(key.clone()).await.unwrap();
        assert_eq!(Some(key), val);
    }
}

async fn must_rollbacked(client: &TransactionClient, keys: HashSet<Vec<u8>>) {
    let ts = client.current_timestamp().await.unwrap();
    let mut snapshot = client.snapshot(ts, TransactionOptions::default());
    for key in keys {
        let val = snapshot.get(key.clone()).await.unwrap();
        assert_eq!(None, val);
    }
}

async fn count_locks(client: &TransactionClient) -> Result<usize> {
    count_locks_in_range(client, b"", b"").await
}

async fn count_locks_in_range(
    client: &TransactionClient,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<usize> {
    let ts = client.current_timestamp().await.unwrap();
    let locks = client.scan_locks(&ts, .., 65536).await?;
    // De-duplicated as `scan_locks` will return duplicated locks due to retry on region changes.
    let locks_set: HashSet<Vec<u8>> =
        HashSet::from_iter(locks.into_iter().map(|l| l.key).filter(|key| {
            let key = key.as_slice();
            key >= start_key && (end_key.is_empty() || key < end_key)
        }));
    Ok(locks_set.len())
}

async fn wait_for_locks_count(client: &TransactionClient, expected: usize) -> Result<usize> {
    wait_for_locks_count_in_range(client, b"", b"", expected).await
}

async fn wait_for_locks_count_in_range(
    client: &TransactionClient,
    start_key: &[u8],
    end_key: &[u8],
    expected: usize,
) -> Result<usize> {
    for _ in 0..30 {
        let remaining = count_locks_in_range(client, start_key, end_key).await?;
        if remaining == expected {
            return Ok(expected);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    count_locks_in_range(client, start_key, end_key).await
}

// Note: too many transactions or keys will make CI unstable due to timeout.
const TXN_COUNT: usize = 16;
const KEY_COUNT: usize = 32;
const REGION_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 5000, 20);
const OPTIMISTIC_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 500, 10);

async fn write_data(
    client: &Client,
    async_commit: bool,
    commit_error: bool,
) -> Result<HashSet<Vec<u8>>> {
    let mut rng = thread_rng();
    let keys = gen_u32_keys((TXN_COUNT * KEY_COUNT) as u32, &mut rng);
    let mut txns = Vec::with_capacity(TXN_COUNT);

    let mut options = TransactionOptions::new_optimistic()
        .retry_options(RetryOptions {
            region_backoff: REGION_BACKOFF,
            lock_backoff: OPTIMISTIC_BACKOFF,
        })
        .drop_check(CheckLevel::Warn);
    if async_commit {
        options = options.use_async_commit();
    }

    for _ in 0..TXN_COUNT {
        let txn = client.begin_with_options(options.clone()).await?;
        txns.push(txn);
    }

    for (i, key) in keys.iter().enumerate() {
        txns[i % TXN_COUNT]
            .put(key.to_owned(), key.to_owned())
            .await?;
    }

    for txn in &mut txns {
        let res = txn.commit().await;
        assert_eq!(res.is_err(), commit_error, "error: {res:?}");
    }
    Ok(keys)
}
