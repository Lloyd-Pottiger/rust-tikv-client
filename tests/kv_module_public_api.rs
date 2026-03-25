use tikv_client::kv;

#[test]
fn kv_module_exports_basic_types() {
    let _: kv::Key = b"k".to_vec().into();
    let _: kv::Value = b"v".to_vec();
    let _: kv::KvPair = kv::KvPair::new(b"k".to_vec(), b"v".to_vec());
}

#[test]
fn kv_module_exports_codec_helpers() {
    let mut buf = Vec::new();
    kv::codec::encode_comparable_varint(&mut buf, 42);
    kv::codec::encode_comparable_uvarint(&mut buf, 42);
}
