// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

mod common;

use tikv_client::Config;
use tikv_client::ReplicaReadType;
use tikv_client::Result;
use tikv_client::TransactionClient as Client;
use tikv_client::TransactionOptions;

use crate::common::parse_args;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // You can try running this example by passing your PD endpoints
    // (and SSL options if necessary) through command line arguments.
    let args = parse_args("stale_read");

    // Create a configuration to use for the example.
    // Optionally encrypt the traffic.
    let config = if let (Some(ca), Some(cert), Some(key)) = (args.ca, args.cert, args.key) {
        Config::default().with_security(ca, cert, key)
    } else {
        Config::default()
    }
    // This example uses the default keyspace, so api-v2 must be enabled on the server.
    .with_default_keyspace();

    let client = Client::new_with_config(args.pd, config).await?;

    // The cluster-wide minimum safe-ts across stores (best-effort).
    let min_safe_ts = client.min_safe_ts().await?;
    println!("cluster min_safe_ts: {min_safe_ts}");

    // Generate a read timestamp representing ~10 seconds ago (without requiring a new PD TSO if a
    // timestamp has been observed recently).
    let start_ts = client.stale_timestamp(10).await?;

    // Create a read-only snapshot at the stale timestamp and enable stale reads.
    let mut snapshot = client.snapshot(start_ts, TransactionOptions::new_optimistic());
    snapshot.set_stale_read(true);
    snapshot.set_replica_read(ReplicaReadType::Mixed);

    let value = snapshot.get("key".to_owned()).await?;
    println!("stale read value: {value:?}");

    Ok(())
}
