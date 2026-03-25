//! Codec helpers.
//!
//! This module mirrors client-go's public `util/codec` package layout.
//!
//! The Rust client hosts these helpers under [`crate::kv::codec`]. This module exists mainly as a
//! stable, discoverable namespace for users familiar with client-go.

pub use crate::kv::codec::*;

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
