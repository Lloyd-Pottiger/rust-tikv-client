//! Memcomparable byte codec helpers for TiKV keys.
//!
//! This module exposes the low-level encoding primitives used to map user keys onto TiKV's
//! ordered byte representation.

use std::io::Write;
use std::ptr;

use crate::internal_err;
use crate::Error;
use crate::Result;

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = 0xff;
const ENC_ASC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];
const ENC_DESC_PADDING: [u8; ENC_GROUP_SIZE] = [!0; ENC_GROUP_SIZE];

/// Returns the maximum encoded bytes size.
///
/// Duplicate from components/tikv_util/src/codec/bytes.rs.
pub fn max_encoded_bytes_size(n: usize) -> usize {
    (n / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1)
}

/// Encode raw keys into TiKV's memcomparable byte format.
pub trait BytesEncoder: Write {
    /// Refer: <https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format>
    ///
    /// Duplicate from components/tikv_util/src/codec/bytes.rs.
    fn encode_bytes(&mut self, key: &[u8], desc: bool) -> Result<()> {
        let len = key.len();
        let mut index = 0;
        let mut buf = [0; ENC_GROUP_SIZE];
        while index <= len {
            let remain = len - index;
            let mut pad: usize = 0;
            if remain > ENC_GROUP_SIZE {
                self.write_all(adjust_bytes_order(
                    &key[index..index + ENC_GROUP_SIZE],
                    desc,
                    &mut buf,
                ))?;
            } else {
                pad = ENC_GROUP_SIZE - remain;
                self.write_all(adjust_bytes_order(&key[index..], desc, &mut buf))?;
                if desc {
                    self.write_all(&ENC_DESC_PADDING[..pad])?;
                } else {
                    self.write_all(&ENC_ASC_PADDING[..pad])?;
                }
            }
            self.write_all(adjust_bytes_order(
                &[ENC_MARKER - (pad as u8)],
                desc,
                &mut buf,
            ))?;
            index += ENC_GROUP_SIZE;
        }
        Ok(())
    }
}

impl<T: Write> BytesEncoder for T {}

fn adjust_bytes_order<'a>(bs: &'a [u8], desc: bool, buf: &'a mut [u8]) -> &'a [u8] {
    if desc {
        let mut buf_idx = 0;
        for &b in bs {
            buf[buf_idx] = !b;
            buf_idx += 1;
        }
        &buf[..buf_idx]
    } else {
        bs
    }
}

/// Decodes bytes which are encoded by `encode_bytes` before just in place without malloc.
///
/// Duplicate from components/tikv_util/src/codec/bytes.rs.
pub fn decode_bytes_in_place(data: &mut Vec<u8>, desc: bool) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let mut write_offset = 0;
    let mut read_offset = 0;
    loop {
        let marker_offset = read_offset + ENC_GROUP_SIZE;
        if marker_offset >= data.len() {
            return Err(internal_err!("unexpected EOF, original key = {:?}", data));
        };

        // SAFETY: `marker_offset < data.len()` guarantees `read_offset + ENC_GROUP_SIZE <= data.len()`,
        // so reading `ENC_GROUP_SIZE` bytes from `read_offset` is in-bounds. `write_offset <= read_offset`
        // (we compact in-place while skipping marker bytes), so source and destination may overlap; `ptr::copy`
        // has `memmove` semantics and is overlap-safe.
        unsafe {
            ptr::copy(
                data.as_ptr().add(read_offset),
                data.as_mut_ptr().add(write_offset),
                ENC_GROUP_SIZE,
            );
        }
        write_offset += ENC_GROUP_SIZE;
        // everytime make ENC_GROUP_SIZE + 1 elements as a decode unit
        read_offset += ENC_GROUP_SIZE + 1;

        // the last byte in decode unit is for marker which indicates pad size
        let marker = data[marker_offset];
        let pad_size = if desc {
            marker as usize
        } else {
            (ENC_MARKER - marker) as usize
        };

        if pad_size > 0 {
            if pad_size > ENC_GROUP_SIZE {
                return Err(internal_err!("invalid key padding"));
            }

            // check the padding pattern whether validate or not
            let padding_slice = if desc {
                &ENC_DESC_PADDING[..pad_size]
            } else {
                &ENC_ASC_PADDING[..pad_size]
            };
            if &data[write_offset - pad_size..write_offset] != padding_slice {
                return Err(internal_err!("invalid key padding"));
            }
            // SAFETY: We have written `write_offset` bytes of decoded data into `data` and validated that the
            // last `pad_size` bytes are padding; shrinking the length keeps only initialized bytes.
            unsafe { data.set_len(write_offset - pad_size) }
            if desc {
                for k in data {
                    *k = !*k;
                }
            }
            return Ok(());
        }
    }
}

const NEGATIVE_TAG_END: u8 = 8;
const POSITIVE_TAG_START: u8 = 0xff - 8;

/// Encode an `i64` into mem-comparable bytes (ascending order).
///
/// This mirrors client-go `util/codec.EncodeComparableVarint`.
pub fn encode_comparable_varint(buf: &mut Vec<u8>, v: i64) {
    if v < 0 {
        if v >= -0xff {
            buf.extend_from_slice(&[NEGATIVE_TAG_END - 1, v as u8]);
        } else if v >= -0xffff {
            buf.extend_from_slice(&[NEGATIVE_TAG_END - 2, (v >> 8) as u8, v as u8]);
        } else if v >= -0xffffff {
            buf.extend_from_slice(&[
                NEGATIVE_TAG_END - 3,
                (v >> 16) as u8,
                (v >> 8) as u8,
                v as u8,
            ]);
        } else if v >= -0xffffffff {
            buf.extend_from_slice(&[
                NEGATIVE_TAG_END - 4,
                (v >> 24) as u8,
                (v >> 16) as u8,
                (v >> 8) as u8,
                v as u8,
            ]);
        } else if v >= -0xffffffffff {
            buf.extend_from_slice(&[
                NEGATIVE_TAG_END - 5,
                (v >> 32) as u8,
                (v >> 24) as u8,
                (v >> 16) as u8,
                (v >> 8) as u8,
                v as u8,
            ]);
        } else if v >= -0xffffffffffff {
            buf.extend_from_slice(&[
                NEGATIVE_TAG_END - 6,
                (v >> 40) as u8,
                (v >> 32) as u8,
                (v >> 24) as u8,
                (v >> 16) as u8,
                (v >> 8) as u8,
                v as u8,
            ]);
        } else if v >= -0xffffffffffffff {
            buf.extend_from_slice(&[
                NEGATIVE_TAG_END - 7,
                (v >> 48) as u8,
                (v >> 40) as u8,
                (v >> 32) as u8,
                (v >> 24) as u8,
                (v >> 16) as u8,
                (v >> 8) as u8,
                v as u8,
            ]);
        } else {
            buf.extend_from_slice(&[
                NEGATIVE_TAG_END - 8,
                (v >> 56) as u8,
                (v >> 48) as u8,
                (v >> 40) as u8,
                (v >> 32) as u8,
                (v >> 24) as u8,
                (v >> 16) as u8,
                (v >> 8) as u8,
                v as u8,
            ]);
        }
        return;
    }

    encode_comparable_uvarint(buf, v as u64);
}

/// Encode an `u64` into mem-comparable bytes (ascending order).
///
/// This mirrors client-go `util/codec.EncodeComparableUvarint`.
pub fn encode_comparable_uvarint(buf: &mut Vec<u8>, v: u64) {
    let single_byte_max = u64::from(POSITIVE_TAG_START - NEGATIVE_TAG_END);
    if v <= single_byte_max {
        buf.push((v as u8) + NEGATIVE_TAG_END);
    } else if v <= 0xff {
        buf.extend_from_slice(&[POSITIVE_TAG_START + 1, v as u8]);
    } else if v <= 0xffff {
        buf.extend_from_slice(&[POSITIVE_TAG_START + 2, (v >> 8) as u8, v as u8]);
    } else if v <= 0xffffff {
        buf.extend_from_slice(&[
            POSITIVE_TAG_START + 3,
            (v >> 16) as u8,
            (v >> 8) as u8,
            v as u8,
        ]);
    } else if v <= 0xffffffff {
        buf.extend_from_slice(&[
            POSITIVE_TAG_START + 4,
            (v >> 24) as u8,
            (v >> 16) as u8,
            (v >> 8) as u8,
            v as u8,
        ]);
    } else if v <= 0xffffffffff {
        buf.extend_from_slice(&[
            POSITIVE_TAG_START + 5,
            (v >> 32) as u8,
            (v >> 24) as u8,
            (v >> 16) as u8,
            (v >> 8) as u8,
            v as u8,
        ]);
    } else if v <= 0xffffffffffff {
        buf.extend_from_slice(&[
            POSITIVE_TAG_START + 6,
            (v >> 40) as u8,
            (v >> 32) as u8,
            (v >> 24) as u8,
            (v >> 16) as u8,
            (v >> 8) as u8,
            v as u8,
        ]);
    } else if v <= 0xffffffffffffff {
        buf.extend_from_slice(&[
            POSITIVE_TAG_START + 7,
            (v >> 48) as u8,
            (v >> 40) as u8,
            (v >> 32) as u8,
            (v >> 24) as u8,
            (v >> 16) as u8,
            (v >> 8) as u8,
            v as u8,
        ]);
    } else {
        buf.extend_from_slice(&[
            POSITIVE_TAG_START + 8,
            (v >> 56) as u8,
            (v >> 48) as u8,
            (v >> 40) as u8,
            (v >> 32) as u8,
            (v >> 24) as u8,
            (v >> 16) as u8,
            (v >> 8) as u8,
            v as u8,
        ]);
    }
}

/// Decode an `u64` from mem-comparable bytes.
///
/// This mirrors client-go `util/codec.DecodeComparableUvarint`.
pub fn decode_comparable_uvarint(data: &[u8]) -> Result<(&[u8], u64)> {
    let (first, rest) = data
        .split_first()
        .ok_or_else(|| Error::StringError("insufficient bytes to decode value".to_owned()))?;

    if *first < NEGATIVE_TAG_END {
        return Err(Error::StringError(
            "invalid bytes to decode value".to_owned(),
        ));
    }
    if *first <= POSITIVE_TAG_START {
        return Ok((rest, u64::from(*first - NEGATIVE_TAG_END)));
    }

    let length = usize::from(*first - POSITIVE_TAG_START);
    if rest.len() < length {
        return Err(Error::StringError(
            "insufficient bytes to decode value".to_owned(),
        ));
    }
    let mut v = 0u64;
    for &byte in &rest[..length] {
        v = (v << 8) | u64::from(byte);
    }
    Ok((&rest[length..], v))
}

/// Decode an `i64` from mem-comparable bytes.
///
/// This mirrors client-go `util/codec.DecodeComparableVarint`.
pub fn decode_comparable_varint(data: &[u8]) -> Result<(&[u8], i64)> {
    let (first, rest) = data
        .split_first()
        .ok_or_else(|| Error::StringError("insufficient bytes to decode value".to_owned()))?;

    if (*first >= NEGATIVE_TAG_END) && (*first <= POSITIVE_TAG_START) {
        return Ok((rest, i64::from(*first - NEGATIVE_TAG_END)));
    }

    let (length, mut v) = if *first < NEGATIVE_TAG_END {
        (usize::from(NEGATIVE_TAG_END - *first), u64::MAX)
    } else {
        (usize::from(*first - POSITIVE_TAG_START), 0u64)
    };

    if rest.len() < length {
        return Err(Error::StringError(
            "insufficient bytes to decode value".to_owned(),
        ));
    }
    for &byte in &rest[..length] {
        v = (v << 8) | u64::from(byte);
    }

    if (*first > POSITIVE_TAG_START) && v > (i64::MAX as u64) {
        return Err(Error::StringError(
            "invalid bytes to decode value".to_owned(),
        ));
    }
    if (*first < NEGATIVE_TAG_END) && v <= (i64::MAX as u64) {
        return Err(Error::StringError(
            "invalid bytes to decode value".to_owned(),
        ));
    }

    Ok((&rest[length..], v as i64))
}

#[cfg(test)]
pub mod test {
    use super::*;

    fn encode_bytes(bs: &[u8]) -> Vec<u8> {
        encode_order_bytes(bs, false)
    }

    fn encode_bytes_desc(bs: &[u8]) -> Vec<u8> {
        encode_order_bytes(bs, true)
    }

    fn encode_order_bytes(bs: &[u8], desc: bool) -> Vec<u8> {
        let cap = max_encoded_bytes_size(bs.len());
        let mut encoded = Vec::with_capacity(cap);
        encoded.encode_bytes(bs, desc).unwrap();
        encoded
    }

    #[test]
    fn test_enc_dec_bytes() {
        let pairs = vec![
            (
                vec![],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 247],
                vec![255, 255, 255, 255, 255, 255, 255, 255, 8],
            ),
            (
                vec![0],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 248],
                vec![255, 255, 255, 255, 255, 255, 255, 255, 7],
            ),
            (
                vec![1, 2, 3],
                vec![1, 2, 3, 0, 0, 0, 0, 0, 250],
                vec![254, 253, 252, 255, 255, 255, 255, 255, 5],
            ),
            (
                vec![1, 2, 3, 0],
                vec![1, 2, 3, 0, 0, 0, 0, 0, 251],
                vec![254, 253, 252, 255, 255, 255, 255, 255, 4],
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![1, 2, 3, 4, 5, 6, 7, 0, 254],
                vec![254, 253, 252, 251, 250, 249, 248, 255, 1],
            ),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
                vec![
                    255, 255, 255, 255, 255, 255, 255, 255, 0, 255, 255, 255, 255, 255, 255, 255,
                    255, 8,
                ],
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7, 8],
                vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
                vec![
                    254, 253, 252, 251, 250, 249, 248, 247, 0, 255, 255, 255, 255, 255, 255, 255,
                    255, 8,
                ],
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248],
                vec![
                    254, 253, 252, 251, 250, 249, 248, 247, 0, 246, 255, 255, 255, 255, 255, 255,
                    255, 7,
                ],
            ),
        ];

        for (source, mut asc, mut desc) in pairs {
            assert_eq!(encode_bytes(&source), asc);
            assert_eq!(encode_bytes_desc(&source), desc);
            decode_bytes_in_place(&mut asc, false).unwrap();
            assert_eq!(source, asc);
            decode_bytes_in_place(&mut desc, true).unwrap();
            assert_eq!(source, desc);
        }
    }

    #[test]
    fn test_encode_decode_comparable_uvarint_roundtrip_and_order() {
        let values = [0u64, 1, 239, 240, 255, 256, 65_535, 65_536, u64::MAX];
        for &value in &values {
            let mut buf = Vec::new();
            encode_comparable_uvarint(&mut buf, value);
            let (rest, decoded) = decode_comparable_uvarint(&buf).unwrap();
            assert!(rest.is_empty());
            assert_eq!(decoded, value);
        }

        let mut encoded = values
            .iter()
            .map(|&v| {
                let mut buf = Vec::new();
                encode_comparable_uvarint(&mut buf, v);
                (v, buf)
            })
            .collect::<Vec<_>>();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        let sorted_values = encoded.into_iter().map(|(v, _)| v).collect::<Vec<_>>();
        assert_eq!(sorted_values, values);
    }

    #[test]
    fn test_encode_decode_comparable_varint_roundtrip_and_order() {
        let values = [
            i64::MIN,
            -1_000_000,
            -256,
            -255,
            -1,
            0,
            1,
            239,
            240,
            1_000_000,
            i64::MAX,
        ];
        for &value in &values {
            let mut buf = Vec::new();
            encode_comparable_varint(&mut buf, value);
            let (rest, decoded) = decode_comparable_varint(&buf).unwrap();
            assert!(rest.is_empty());
            assert_eq!(decoded, value);
        }

        let mut encoded = values
            .iter()
            .map(|&v| {
                let mut buf = Vec::new();
                encode_comparable_varint(&mut buf, v);
                (v, buf)
            })
            .collect::<Vec<_>>();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        let sorted_values = encoded.into_iter().map(|(v, _)| v).collect::<Vec<_>>();
        assert_eq!(sorted_values, values);
    }

    #[test]
    fn test_decode_comparable_uvarint_rejects_invalid_and_insufficient() {
        let err = decode_comparable_uvarint(&[]).unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));

        let err = decode_comparable_uvarint(&[0]).unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));

        let err = decode_comparable_uvarint(&[POSITIVE_TAG_START + 8, 1, 2]).unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));
    }
}
