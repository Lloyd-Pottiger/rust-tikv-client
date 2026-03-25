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
fn util_redact_module_reexports_redact_helpers() {
    let _ = util::redact::RedactMode::Enable;
    let _ = util::redact::need_redact();
    let _ = util::redact::key(b"abc");
    let _ = util::redact::key_bytes(b"abc");
}
