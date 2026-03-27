use tikv_client::store_vars;
use tikv_client::StoreLabel;

struct StoreLimitGuard(i64);

impl Drop for StoreLimitGuard {
    fn drop(&mut self) {
        store_vars::set_store_limit(self.0);
    }
}

fn push_store_limit(limit: i64) -> StoreLimitGuard {
    let previous = store_vars::global_store_limit();
    store_vars::set_store_limit(limit);
    StoreLimitGuard(previous)
}

fn label(key: &str, value: &str) -> StoreLabel {
    StoreLabel {
        key: key.to_owned(),
        value: value.to_owned(),
    }
}

#[tokio::test]
async fn store_vars_module_exposes_store_limit_helpers() {
    let _guard = push_store_limit(0);

    assert_eq!(store_vars::global_store_limit(), 0);
    assert_eq!(store_vars::store_limit(), 0);

    store_vars::set_store_limit(7);
    assert_eq!(store_vars::global_store_limit(), 7);
    assert_eq!(store_vars::store_limit(), 7);

    store_vars::set_store_limit(-5);
    assert_eq!(store_vars::global_store_limit(), 0);
    assert_eq!(store_vars::store_limit(), 0);

    let scoped_limit = store_vars::with_store_limit(11, async {
        assert_eq!(store_vars::store_limit(), 11);
        store_vars::with_store_limit(-3, async { store_vars::store_limit() }).await
    })
    .await;
    assert_eq!(scoped_limit, 0);
    assert_eq!(store_vars::store_limit(), 0);
}

#[test]
fn store_vars_module_exposes_access_location_type_helpers() {
    let _ = store_vars::AccessLocationType::Unknown;
    let _ = store_vars::AccessLocationType::LocalZone;
    let _ = store_vars::AccessLocationType::CrossZone;

    let labels = [label("zone", "zone-a")];
    assert_eq!(
        store_vars::access_location_type(Some("zone-a"), &labels),
        store_vars::AccessLocationType::LocalZone
    );
    assert_eq!(
        store_vars::access_location_type(Some("zone-b"), &labels),
        store_vars::AccessLocationType::CrossZone
    );
    assert_eq!(
        store_vars::access_location_type(None, &labels),
        store_vars::AccessLocationType::Unknown
    );
}
