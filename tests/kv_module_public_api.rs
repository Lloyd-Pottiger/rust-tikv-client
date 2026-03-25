use tikv_client::kv;

#[test]
fn kv_module_exports_basic_types() {
    let _: kv::Key = b"k".to_vec().into();
    let _: kv::Value = b"v".to_vec();
    let _: kv::KvPair = kv::KvPair::new(b"k".to_vec(), b"v".to_vec());
    let _ = kv::Variables::default();
    let _: kv::Variables = kv::DEFAULT_VARS.clone();
    let _: u64 = kv::DEF_BACKOFF_LOCK_FAST_MS;
    let _: u32 = kv::DEF_BACKOFF_WEIGHT;
}

#[test]
fn kv_module_exports_codec_helpers() {
    let mut buf = Vec::new();
    kv::codec::encode_comparable_varint(&mut buf, 42);
    kv::codec::encode_comparable_uvarint(&mut buf, 42);
}

#[test]
fn kv_module_exports_store_vars_and_replica_read_type() {
    let _ = kv::ReplicaReadType::Leader;
    let _ = kv::AccessLocationType::Unknown;

    let _: fn(i64) = kv::set_store_limit;
    let _: fn() -> i64 = kv::global_store_limit;
    let _: fn() -> i64 = kv::store_limit;
    std::mem::drop(kv::with_store_limit(0, async {}));

    let _: fn(Option<&str>, &[tikv_client::StoreLabel]) -> kv::AccessLocationType =
        kv::access_location_type;
}

#[test]
fn kv_module_exports_get_and_batch_get_option_types() {
    let mut opts = kv::GetOptions::default();
    opts.apply(&[kv::with_return_commit_ts()]);
    assert!(opts.return_commit_ts());

    let mut opts = kv::BatchGetOptions::default();
    opts.apply(&[kv::with_return_commit_ts()]);
    assert!(opts.return_commit_ts());

    let _ = kv::ValueEntry::new(Some(b"v".to_vec()), 42);
}

#[test]
fn kv_module_exposes_getter_traits_for_snapshot_and_transaction() {
    fn assert_impl<T: kv::Getter + kv::BatchGetter>() {}

    assert_impl::<tikv_client::Snapshot>();
    assert_impl::<tikv_client::Transaction>();
}
