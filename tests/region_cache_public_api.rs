use std::sync::Arc;
use std::time::Duration;

use tikv_client::{RegionCache, RegionVerId};

#[tokio::test]
async fn region_cache_exports_buckets_query_api() {
    struct DummyClient;

    let cache = RegionCache::new_with_ttl(Arc::new(DummyClient), Duration::ZERO, Duration::ZERO);
    let buckets = cache.get_buckets_by_ver_id(&RegionVerId::default()).await;
    assert!(buckets.is_none());
}
