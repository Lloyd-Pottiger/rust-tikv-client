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
    let shared = kv::GetOrBatchGetOption::ReturnCommitTs;
    let _: kv::GetOption = shared;
    let _: kv::BatchGetOption = shared;
    let _: kv::GetOrBatchGetOption = kv::with_return_commit_ts();

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

#[test]
fn kv_module_exports_key_helpers_and_key_range() {
    let next = kv::next_key(b"k".to_vec());
    assert_eq!(Into::<Vec<u8>>::into(next), b"k\0".to_vec());

    let prefix_next = kv::prefix_next_key(vec![0x12, 0xFF]);
    assert_eq!(Into::<Vec<u8>>::into(prefix_next), vec![0x13, 0x00]);

    let prefix_overflow = kv::prefix_next_key(vec![0xFF]);
    assert!(prefix_overflow.is_empty());

    assert_eq!(kv::cmp_key(b"a", b"b"), std::cmp::Ordering::Less);

    let range = kv::KeyRange::new(b"a".to_vec(), b"b".to_vec());
    let _: kv::BoundRange = range.into();

    let _ = kv::Key::from(b"k".to_vec()).next_key();
    let _ = kv::Key::from(b"k".to_vec()).prefix_next_key();
}

#[test]
fn kv_module_exports_hex_repr_and_kv_pair_ttl() {
    assert_eq!(format!("{}", kv::HexRepr(&[0x00, 0xAB, 0x10])), "00AB10");

    let pair = kv::KvPair::new(b"k".to_vec(), b"v".to_vec());
    let ttl_pair = kv::KvPairTTL(pair.clone().into(), 42);
    assert_eq!(ttl_pair.as_ref(), pair.key());

    let (roundtrip_pair, ttl): (tikv_client::proto::kvrpcpb::KvPair, u64) = ttl_pair.into();
    assert_eq!(kv::KvPair::from(roundtrip_pair), pair);
    assert_eq!(ttl, 42);
}

#[test]
fn kv_module_exports_key_flags() {
    let _ = kv::FLAG_BYTES;

    let flags = kv::apply_flags_ops(
        kv::KeyFlags::default(),
        [kv::FlagsOp::SetPresumeKeyNotExists],
    );
    assert!(flags.has_presume_key_not_exists());
    assert!(flags.has_need_check_exists());

    let flags = kv::apply_flags_ops(flags, [kv::FlagsOp::SetAssertExist]);
    assert!(flags.has_assert_exist());
    assert!(!flags.has_assert_not_exist());
    assert!(flags.has_assertion_flags());

    let flags = kv::apply_flags_ops(flags, [kv::FlagsOp::SetAssertNone]);
    assert!(!flags.has_assertion_flags());

    let flags = kv::apply_flags_ops(flags, [kv::FlagsOp::DelPresumeKeyNotExists]);
    assert!(!flags.has_presume_key_not_exists());
}
