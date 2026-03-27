use std::sync::Arc;
use std::time::Duration;

use tikv_client::{
    CommandPriority, DiskFullOpt, GrpcCompressionType, IsolationLevel, ReplicaReadAdjuster,
    ReplicaReadType, RequestSource, Security, TraceControlFlags,
};

#[test]
fn crate_root_exports_config_helpers() {
    let _ = GrpcCompressionType::None;
    let security = Security::new("ca", "cert", "key", std::iter::empty::<&str>());
    let _ = security.clone().into_config();
    let _ = security.apply_to_config(tikv_client::Config::default());

    #[allow(deprecated)]
    let _: fn(Duration) = tikv_client::set_region_cache_ttl;
    let _: fn(Duration, Duration) = tikv_client::set_region_cache_ttl_with_jitter;
    let _: fn(Duration) = tikv_client::set_store_liveness_timeout;
}

#[test]
fn crate_root_exports_request_context_types() {
    let _ = CommandPriority::High;
    let _ = IsolationLevel::RcCheckTs;
    let _ = DiskFullOpt::AllowedOnAlreadyFull;
    let flags = TraceControlFlags::default()
        .with(TraceControlFlags::IMMEDIATE_LOG)
        .with(TraceControlFlags::TIKV_CATEGORY_REQUEST);
    assert!(flags.has(TraceControlFlags::IMMEDIATE_LOG));
    assert!(flags.has(TraceControlFlags::TIKV_CATEGORY_REQUEST));

    let source = RequestSource::new()
        .internal(true)
        .source_type("cop")
        .explicit_type("session");
    assert_eq!(source.to_string(), "internal_cop_session");

    let default_source = RequestSource::new();
    assert_eq!(default_source.to_string(), "unknown");
}

#[test]
fn crate_root_exports_replica_read_helpers() {
    let _ = ReplicaReadType::PreferLeader;
    assert!(ReplicaReadType::PreferLeader.is_follower_read());
    assert!(!ReplicaReadType::Leader.is_follower_read());

    let adjuster: ReplicaReadAdjuster = Arc::new(|count| {
        if count > 1 {
            ReplicaReadType::Follower
        } else {
            ReplicaReadType::Leader
        }
    });
    assert_eq!(adjuster(1), ReplicaReadType::Leader);
    assert_eq!(adjuster(2), ReplicaReadType::Follower);
}
