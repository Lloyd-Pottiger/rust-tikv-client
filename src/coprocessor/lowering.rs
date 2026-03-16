//! Constructor functions for coprocessor requests.
//!
//! This module mirrors `raw::lowering` and `transaction::lowering` by providing helpers that take
//! high-level key/range types from this crate and produce protobuf requests.

use crate::BoundRange;
use crate::Timestamp;
use crate::TimestampExt;

use super::BatchRequest;
use super::CoprocessorStreamRequest;
use super::KeyRange;
use super::RegionInfo;
use super::Request;

fn to_key_range(range: BoundRange) -> KeyRange {
    let (start, end) = range.into_keys();
    KeyRange {
        start: start.into(),
        end: end.unwrap_or_default().into(),
    }
}

pub fn new_coprocessor_request<I, R>(
    tp: i64,
    data: Vec<u8>,
    ranges: I,
    start_ts: Timestamp,
) -> Request
where
    I: IntoIterator<Item = R>,
    R: Into<BoundRange>,
{
    Request {
        tp,
        data,
        start_ts: start_ts.version(),
        ranges: ranges
            .into_iter()
            .map(|range| to_key_range(range.into()))
            .collect(),
        ..Default::default()
    }
}

pub fn new_coprocessor_stream_request<I, R>(
    tp: i64,
    data: Vec<u8>,
    ranges: I,
    start_ts: Timestamp,
) -> CoprocessorStreamRequest
where
    I: IntoIterator<Item = R>,
    R: Into<BoundRange>,
{
    new_coprocessor_request(tp, data, ranges, start_ts).into()
}

pub fn new_batch_coprocessor_request<I>(
    tp: i64,
    data: Vec<u8>,
    regions: I,
    start_ts: Timestamp,
    schema_ver: i64,
) -> BatchRequest
where
    I: IntoIterator<Item = RegionInfo>,
{
    BatchRequest {
        tp,
        data,
        regions: regions.into_iter().collect(),
        start_ts: start_ts.version(),
        schema_ver,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_coprocessor_request_converts_ranges_and_sets_start_ts() {
        let start_ts = <Timestamp as TimestampExt>::from_version(42);

        let request = new_coprocessor_request(
            7,
            b"payload".to_vec(),
            vec!["a".to_owned().."b".to_owned()],
            start_ts,
        );

        assert_eq!(request.tp, 7);
        assert_eq!(request.data, b"payload".to_vec());
        assert_eq!(request.start_ts, 42);
        assert_eq!(request.ranges.len(), 1);
        assert_eq!(request.ranges[0].start, b"a".to_vec());
        assert_eq!(request.ranges[0].end, b"b".to_vec());
    }

    #[test]
    fn new_coprocessor_request_open_ended_range_uses_empty_end() {
        let start_ts = <Timestamp as TimestampExt>::from_version(42);

        let request = new_coprocessor_request(1, Vec::new(), vec!["a".to_owned()..], start_ts);

        assert_eq!(request.ranges.len(), 1);
        assert_eq!(request.ranges[0].start, b"a".to_vec());
        assert!(request.ranges[0].end.is_empty());
    }

    #[test]
    fn new_coprocessor_stream_request_wraps_inner_request() {
        let start_ts = <Timestamp as TimestampExt>::from_version(99);

        let request = new_coprocessor_stream_request(
            11,
            b"stream".to_vec(),
            vec!["a".to_owned().."b".to_owned()],
            start_ts,
        );

        let inner = request.into_inner();
        assert_eq!(inner.tp, 11);
        assert_eq!(inner.data, b"stream".to_vec());
        assert_eq!(inner.start_ts, 99);
    }

    #[test]
    fn new_batch_coprocessor_request_sets_fields() {
        let start_ts = <Timestamp as TimestampExt>::from_version(123);

        let request = new_batch_coprocessor_request(
            22,
            b"batch".to_vec(),
            vec![RegionInfo {
                region_id: 7,
                ..Default::default()
            }],
            start_ts,
            456,
        );

        assert_eq!(request.tp, 22);
        assert_eq!(request.data, b"batch".to_vec());
        assert_eq!(request.start_ts, 123);
        assert_eq!(request.schema_ver, 456);
        assert_eq!(request.regions.len(), 1);
        assert_eq!(request.regions[0].region_id, 7);
    }
}
