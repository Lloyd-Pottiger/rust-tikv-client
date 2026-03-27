use tikv_client::util;

#[test]
fn util_codec_module_reexports_kv_codec_helpers() {
    let mut encoded = Vec::new();
    util::codec::encode_comparable_varint(&mut encoded, -7);

    let (rest, decoded) = util::codec::decode_comparable_varint(&encoded).expect("decode varint");
    assert!(rest.is_empty());
    assert_eq!(decoded, -7);
}

#[test]
fn util_codec_module_exposes_plain_varint_helpers() {
    let mut encoded = Vec::new();
    util::codec::encode_uvarint(&mut encoded, 300);
    let (rest, decoded) = util::codec::decode_uvarint(&encoded).expect("decode uvarint");
    assert!(rest.is_empty());
    assert_eq!(decoded, 300);

    encoded.clear();
    util::codec::encode_varint(&mut encoded, -123);
    let (rest, decoded) = util::codec::decode_varint(&encoded).expect("decode varint");
    assert!(rest.is_empty());
    assert_eq!(decoded, -123);
}

#[test]
fn util_codec_module_exposes_fixed_width_integer_helpers() {
    let _: fn(i64) -> u64 = util::codec::encode_int_to_cmp_uint;
    let _: fn(u64) -> i64 = util::codec::decode_cmp_uint_to_int;
    let _: fn(&mut Vec<u8>, i64) = util::codec::encode_int;
    let _: fn(&mut Vec<u8>, i64) = util::codec::encode_int_desc;
    let _: fn(&[u8]) -> tikv_client::Result<(&[u8], i64)> = util::codec::decode_int;
    let _: fn(&[u8]) -> tikv_client::Result<(&[u8], i64)> = util::codec::decode_int_desc;
    let _: fn(&mut Vec<u8>, u64) = util::codec::encode_uint;
    let _: fn(&mut Vec<u8>, u64) = util::codec::encode_uint_desc;
    let _: fn(&[u8]) -> tikv_client::Result<(&[u8], u64)> = util::codec::decode_uint;
    let _: fn(&[u8]) -> tikv_client::Result<(&[u8], u64)> = util::codec::decode_uint_desc;

    assert_eq!(
        util::codec::decode_cmp_uint_to_int(util::codec::encode_int_to_cmp_uint(-42)),
        -42
    );

    let mut encoded = Vec::new();
    util::codec::encode_int(&mut encoded, -42);
    let (rest, decoded) = util::codec::decode_int(&encoded).expect("decode int");
    assert!(rest.is_empty());
    assert_eq!(decoded, -42);

    encoded.clear();
    util::codec::encode_uint_desc(&mut encoded, 42);
    let (rest, decoded) = util::codec::decode_uint_desc(&encoded).expect("decode uint desc");
    assert!(rest.is_empty());
    assert_eq!(decoded, 42);
}

#[test]
fn util_redact_module_reexports_redact_helpers() {
    let _ = util::redact::RedactMode::Enable;
    let _ = util::redact::need_redact();
    let _ = util::redact::key(b"abc");
    let _ = util::redact::key_bytes(b"abc");
}
