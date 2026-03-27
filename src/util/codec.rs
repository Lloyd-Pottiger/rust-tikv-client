//! Codec helpers.
//!
//! This module mirrors client-go's public `util/codec` package layout.
//!
//! The Rust client hosts these helpers under [`crate::kv::codec`]. This module exists mainly as a
//! stable, discoverable namespace for users familiar with client-go.

pub use crate::kv::codec::*;

const SIGN_MASK: u64 = 0x8000_0000_0000_0000;

/// Convert an `i64` into a comparison-friendly `u64`.
///
/// This mirrors client-go `util/codec.EncodeIntToCmpUint`.
#[doc(alias = "EncodeIntToCmpUint")]
#[must_use]
pub fn encode_int_to_cmp_uint(v: i64) -> u64 {
    (v as u64) ^ SIGN_MASK
}

/// Convert a comparison-friendly `u64` back into an `i64`.
///
/// This mirrors client-go `util/codec.DecodeCmpUintToInt`.
#[doc(alias = "DecodeCmpUintToInt")]
#[must_use]
pub fn decode_cmp_uint_to_int(u: u64) -> i64 {
    (u ^ SIGN_MASK) as i64
}

/// Encode an `i64` into fixed-width ascending mem-comparable bytes.
///
/// This mirrors client-go `util/codec.EncodeInt`.
#[doc(alias = "EncodeInt")]
pub fn encode_int(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&encode_int_to_cmp_uint(v).to_be_bytes());
}

/// Encode an `i64` into fixed-width descending mem-comparable bytes.
///
/// This mirrors client-go `util/codec.EncodeIntDesc`.
#[doc(alias = "EncodeIntDesc")]
pub fn encode_int_desc(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&(!encode_int_to_cmp_uint(v)).to_be_bytes());
}

/// Decode an `i64` from fixed-width ascending mem-comparable bytes.
///
/// This mirrors client-go `util/codec.DecodeInt`.
#[doc(alias = "DecodeInt")]
pub fn decode_int(data: &[u8]) -> crate::Result<(&[u8], i64)> {
    let (rest, value) = decode_fixed_width_u64(data)?;
    Ok((rest, decode_cmp_uint_to_int(value)))
}

/// Decode an `i64` from fixed-width descending mem-comparable bytes.
///
/// This mirrors client-go `util/codec.DecodeIntDesc`.
#[doc(alias = "DecodeIntDesc")]
pub fn decode_int_desc(data: &[u8]) -> crate::Result<(&[u8], i64)> {
    let (rest, value) = decode_fixed_width_u64(data)?;
    Ok((rest, decode_cmp_uint_to_int(!value)))
}

/// Encode a `u64` into fixed-width ascending mem-comparable bytes.
///
/// This mirrors client-go `util/codec.EncodeUint`.
#[doc(alias = "EncodeUint")]
pub fn encode_uint(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// Encode a `u64` into fixed-width descending mem-comparable bytes.
///
/// This mirrors client-go `util/codec.EncodeUintDesc`.
#[doc(alias = "EncodeUintDesc")]
pub fn encode_uint_desc(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&(!v).to_be_bytes());
}

/// Decode a `u64` from fixed-width ascending mem-comparable bytes.
///
/// This mirrors client-go `util/codec.DecodeUint`.
#[doc(alias = "DecodeUint")]
pub fn decode_uint(data: &[u8]) -> crate::Result<(&[u8], u64)> {
    decode_fixed_width_u64(data)
}

/// Decode a `u64` from fixed-width descending mem-comparable bytes.
///
/// This mirrors client-go `util/codec.DecodeUintDesc`.
#[doc(alias = "DecodeUintDesc")]
pub fn decode_uint_desc(data: &[u8]) -> crate::Result<(&[u8], u64)> {
    let (rest, value) = decode_fixed_width_u64(data)?;
    Ok((rest, !value))
}

/// Encode an `i64` into varint bytes.
///
/// This mirrors client-go `util/codec.EncodeVarint`.
#[doc(alias = "EncodeVarint")]
pub fn encode_varint(buf: &mut Vec<u8>, v: i64) {
    let ux = (v as u64) << 1;
    let ux = if v < 0 { !ux } else { ux };
    encode_uvarint(buf, ux);
}

/// Decode an `i64` from varint bytes.
///
/// This mirrors client-go `util/codec.DecodeVarint`.
#[doc(alias = "DecodeVarint")]
pub fn decode_varint(data: &[u8]) -> crate::Result<(&[u8], i64)> {
    let (rest, ux) = decode_uvarint(data)?;
    let mut v = (ux >> 1) as i64;
    if (ux & 1) != 0 {
        v = !v;
    }
    Ok((rest, v))
}

/// Encode a `u64` into varint bytes.
///
/// This mirrors client-go `util/codec.EncodeUvarint`.
#[doc(alias = "EncodeUvarint")]
pub fn encode_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Decode a `u64` from varint bytes.
///
/// This mirrors client-go `util/codec.DecodeUvarint`.
#[doc(alias = "DecodeUvarint")]
pub fn decode_uvarint(data: &[u8]) -> crate::Result<(&[u8], u64)> {
    let mut x: u64 = 0;
    let mut s: u32 = 0;

    for (i, &byte) in data.iter().enumerate() {
        if byte < 0x80 {
            if i > 9 || (i == 9 && byte > 1) {
                return Err(crate::Error::StringError(
                    "value larger than 64 bits".to_owned(),
                ));
            }

            return Ok((&data[i + 1..], x | (u64::from(byte) << s)));
        }

        x |= u64::from(byte & 0x7f) << s;
        s += 7;
    }

    Err(crate::Error::StringError(
        "insufficient bytes to decode value".to_owned(),
    ))
}

fn decode_fixed_width_u64(data: &[u8]) -> crate::Result<(&[u8], u64)> {
    if data.len() < 8 {
        return Err(crate::Error::StringError(
            "insufficient bytes to decode value".to_owned(),
        ));
    }

    let (value, rest) = data.split_at(8);
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(value);
    Ok((rest, u64::from_be_bytes(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_width_int_helpers_roundtrip_and_preserve_order() {
        let values = [i64::MIN, -255, -1, 0, 1, 255, i64::MAX];

        let mut encoded = Vec::new();
        for value in values {
            encoded.clear();
            encode_int(&mut encoded, value);
            let (rest, decoded) = decode_int(&encoded).expect("decode int");
            assert!(rest.is_empty());
            assert_eq!(decoded, value);
        }

        let mut ordered = values
            .into_iter()
            .map(|value| {
                let mut bytes = Vec::new();
                encode_int(&mut bytes, value);
                (bytes, value)
            })
            .collect::<Vec<_>>();
        ordered.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            ordered
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>(),
            values
        );
    }

    #[test]
    fn fixed_width_uint_helpers_roundtrip_and_desc_order() {
        let values = [0_u64, 1, 255, 65_535, u64::MAX];

        let mut encoded = Vec::new();
        for value in values {
            encoded.clear();
            encode_uint_desc(&mut encoded, value);
            let (rest, decoded) = decode_uint_desc(&encoded).expect("decode uint desc");
            assert!(rest.is_empty());
            assert_eq!(decoded, value);
        }

        let mut ordered = values
            .into_iter()
            .map(|value| {
                let mut bytes = Vec::new();
                encode_uint_desc(&mut bytes, value);
                (bytes, value)
            })
            .collect::<Vec<_>>();
        ordered.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            ordered
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>(),
            values.into_iter().rev().collect::<Vec<_>>()
        );
    }

    #[test]
    fn cmp_uint_conversion_roundtrips() {
        for value in [i64::MIN, -1, 0, 1, i64::MAX] {
            assert_eq!(decode_cmp_uint_to_int(encode_int_to_cmp_uint(value)), value);
        }
    }
}
