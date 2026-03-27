// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This module provides constructor functions for requests which take arguments as high-level
//! types (i.e., the types from the client crate) and converts these to the types used in the
//! generated protobuf code, then calls the low-level ctor functions in the requests module.

use std::iter::Iterator;
use std::ops::Range;
use std::sync::Arc;

use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::raw::requests;
use crate::BoundRange;
use crate::ColumnFamily;
use crate::Key;
use crate::KvPair;
use crate::Value;

/// Builds a raw get request from a crate-native key and optional column family.
pub fn new_raw_get_request(key: Key, cf: Option<ColumnFamily>) -> kvrpcpb::RawGetRequest {
    requests::new_raw_get_request(key.into(), cf)
}

/// Builds a raw batch-get request from crate-native keys and an optional column family.
pub fn new_raw_batch_get_request(
    keys: impl Iterator<Item = Key>,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawBatchGetRequest {
    requests::new_raw_batch_get_request(keys.map(Into::into).collect(), cf)
}

/// Builds a request that fetches the remaining TTL, in seconds, for a single raw key.
pub fn new_raw_get_key_ttl_request(
    key: Key,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawGetKeyTtlRequest {
    requests::new_raw_get_key_ttl_request(key.into(), cf)
}

/// Builds a raw put request from crate-native key/value types.
///
/// `ttl` is expressed in seconds; `0` keeps the entry without an expiration.
/// When `atomic` is `true`, the request is marked for TiKV's atomic/CAS write path, matching
/// [`crate::raw::Client::with_atomic_for_cas`].
pub fn new_raw_put_request(
    key: Key,
    value: Value,
    cf: Option<ColumnFamily>,
    ttl: u64,
    atomic: bool,
) -> kvrpcpb::RawPutRequest {
    requests::new_raw_put_request(key.into(), value, ttl, cf, atomic)
}

/// Builds a raw batch-put request from crate-native key/value pairs and per-entry TTLs.
///
/// The `ttls` iterator is consumed eagerly up to the number of input pairs. During request
/// sharding, TiKV interprets the collected TTLs the same way as
/// [`crate::raw::Client::batch_put_with_ttl`]: no TTLs means all pairs are persistent, a single
/// TTL is applied to every pair, and otherwise the number of TTLs must match the number of pairs.
/// When `atomic` is `true`, the request is marked for the atomic/CAS write path.
pub fn new_raw_batch_put_request(
    pairs: impl Iterator<Item = KvPair>,
    ttls: impl Iterator<Item = u64>,
    cf: Option<ColumnFamily>,
    atomic: bool,
) -> kvrpcpb::RawBatchPutRequest {
    let pairs = pairs.map(Into::into).collect::<Vec<_>>();
    let ttls = ttls.take(pairs.len()).collect::<Vec<_>>();
    requests::new_raw_batch_put_request(pairs, ttls, cf, atomic)
}

/// Builds a raw delete request from a crate-native key and optional column family.
///
/// When `atomic` is `true`, the request is marked for TiKV's atomic/CAS write path.
pub fn new_raw_delete_request(
    key: Key,
    cf: Option<ColumnFamily>,
    atomic: bool,
) -> kvrpcpb::RawDeleteRequest {
    requests::new_raw_delete_request(key.into(), cf, atomic)
}

/// Builds a raw batch-delete request from crate-native keys and an optional column family.
pub fn new_raw_batch_delete_request(
    keys: impl Iterator<Item = Key>,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawBatchDeleteRequest {
    requests::new_raw_batch_delete_request(keys.map(Into::into).collect(), cf)
}

/// Builds a raw delete-range request from a crate-native bounded range.
pub fn new_raw_delete_range_request(
    range: BoundRange,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawDeleteRangeRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_raw_delete_range_request(start_key.into(), end_key.unwrap_or_default().into(), cf)
}

/// Builds a raw scan request from a crate-native range.
///
/// `limit` bounds the number of returned entries, `key_only` omits values from the response, and
/// `reverse` scans from the range end back toward the start.
pub fn new_raw_scan_request(
    range: BoundRange,
    limit: u32,
    key_only: bool,
    reverse: bool,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawScanRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_raw_scan_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        limit,
        key_only,
        reverse,
        cf,
    )
}

/// Builds a raw batch-scan request from crate-native ranges.
///
/// `each_limit` applies independently to each input range after TiKV shards the request.
pub fn new_raw_batch_scan_request(
    ranges: impl Iterator<Item = BoundRange>,
    each_limit: u32,
    key_only: bool,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawBatchScanRequest {
    requests::new_raw_batch_scan_request(ranges.map(Into::into).collect(), each_limit, key_only, cf)
}

/// Builds a raw checksum request for the provided crate-native range.
pub fn new_raw_checksum_request(range: BoundRange) -> kvrpcpb::RawChecksumRequest {
    requests::new_raw_checksum_request(range.into())
}

/// Builds a raw compare-and-swap request from crate-native key/value types.
///
/// `previous_value` represents the expected current value. Passing `None` expresses the
/// "key must not exist" precondition used by TiKV's raw CAS API.
pub fn new_cas_request(
    key: Key,
    value: Value,
    previous_value: Option<Value>,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawCasRequest {
    requests::new_cas_request(key.into(), value, previous_value, cf)
}

/// Builds a raw coprocessor request from crate-native ranges.
///
/// The `request_builder` callback is invoked once per region shard with that region's metadata and
/// the shard-local key ranges converted back into crate-native [`Key`] ranges. The returned bytes
/// are written into the protobuf request's `data` field before dispatch.
pub fn new_raw_coprocessor_request(
    copr_name: String,
    copr_version_req: String,
    ranges: impl Iterator<Item = BoundRange>,
    request_builder: impl Fn(metapb::Region, Vec<Range<Key>>) -> Vec<u8> + Send + Sync + 'static,
) -> requests::RawCoprocessorRequest {
    requests::new_raw_coprocessor_request(
        copr_name,
        copr_version_req,
        ranges.map(Into::into).collect(),
        Arc::new(move |region, ranges| {
            request_builder(
                region,
                ranges
                    .into_iter()
                    .map(|range| range.start_key.into()..range.end_key.into())
                    .collect(),
            )
        }),
    )
}
