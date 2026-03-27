// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod batch;
mod client;
mod coprocessor_cache;
mod errors;
mod forwarding;
mod request;

use std::cmp::max;
use std::cmp::min;
use std::sync::Arc;

use derive_new::new;
use futures::prelude::*;
use futures::stream::BoxStream;

pub(crate) use self::client::ForwardedHostKvClient;
pub use self::client::KvClient;
pub use self::client::KvConnect;
pub(crate) use self::client::StoreLimitKvClient;
pub use self::client::TikvConnect;
pub use self::errors::HasKeyErrors;
pub use self::errors::HasRegionError;
pub use self::errors::HasRegionErrors;
pub(crate) use self::forwarding::apply_forwarded_host_metadata;
pub(crate) use self::forwarding::apply_forwarded_host_metadata_value;
pub(crate) use self::forwarding::forwarded_host;
pub(crate) use self::forwarding::scope_forwarded_host;
pub use self::request::Request;
use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::region::RegionWithLeader;
use crate::BoundRange;
use crate::Key;
use crate::Result;

#[derive(Clone)]
pub struct RegionStore {
    pub region_with_leader: RegionWithLeader,
    pub client: Arc<dyn KvClient + Send + Sync>,
    pub store_address: String,
    pub target_store: Option<metapb::Store>,
}

impl RegionStore {
    #[must_use]
    pub fn new(
        region_with_leader: RegionWithLeader,
        client: Arc<dyn KvClient + Send + Sync>,
        store_address: String,
    ) -> Self {
        Self {
            region_with_leader,
            client,
            store_address,
            target_store: None,
        }
    }

    #[must_use]
    pub fn with_store_meta(mut self, target_store: metapb::Store) -> Self {
        self.target_store = Some(target_store);
        self
    }
}

#[derive(new, Clone)]
pub struct Store {
    pub meta: metapb::Store,
    pub client: Arc<dyn KvClient + Send + Sync>,
}

/// Maps keys to a stream of stores. `key_data` must be sorted in increasing order
pub fn region_stream_for_keys<K, KOut, PdC>(
    key_data: impl Iterator<Item = K> + Send + Sync + 'static,
    pd_client: Arc<PdC>,
) -> BoxStream<'static, Result<(Vec<KOut>, RegionWithLeader)>>
where
    PdC: PdClient,
    K: AsRef<Key> + Into<KOut> + Send + Sync + 'static,
    KOut: Send + Sync + 'static,
{
    pd_client.clone().group_keys_by_region(key_data)
}

#[allow(clippy::type_complexity)]
pub fn region_stream_for_range<PdC: PdClient>(
    range: (Vec<u8>, Vec<u8>),
    pd_client: Arc<PdC>,
) -> BoxStream<'static, Result<((Vec<u8>, Vec<u8>), RegionWithLeader)>> {
    let bnd_range = if range.1.is_empty() {
        BoundRange::range_from(range.0.clone().into())
    } else {
        BoundRange::from(range.clone())
    };
    pd_client
        .regions_for_range(bnd_range)
        .map_ok(move |region| {
            let region_range = region.range();
            let result_range = range_intersection(
                region_range,
                (range.0.clone().into(), range.1.clone().into()),
            );
            ((result_range.0.into(), result_range.1.into()), region)
        })
        .boxed()
}

/// The range used for request should be the intersection of `region_range` and `range`.
fn range_intersection(region_range: (Key, Key), range: (Key, Key)) -> (Key, Key) {
    let (lower, upper) = region_range;
    let up = if upper.is_empty() {
        range.1
    } else if range.1.is_empty() {
        upper
    } else {
        min(upper, range.1)
    };
    (max(lower, range.0), up)
}

pub fn region_stream_for_ranges<PdC: PdClient>(
    ranges: Vec<kvrpcpb::KeyRange>,
    pd_client: Arc<PdC>,
) -> BoxStream<'static, Result<(Vec<kvrpcpb::KeyRange>, RegionWithLeader)>> {
    pd_client.clone().group_ranges_by_region(ranges)
}
