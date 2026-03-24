use tikv_client::store_vars::access_location_type;
use tikv_client::AccessLocationType;
use tikv_client::StoreLabel;

fn label(key: &str, value: &str) -> StoreLabel {
    StoreLabel {
        key: key.to_owned(),
        value: value.to_owned(),
    }
}

#[test]
fn test_access_location_type_unknown_without_self_zone_label() {
    let store_labels = [label("zone", "zone2")];
    assert_eq!(
        access_location_type(None, &store_labels),
        AccessLocationType::Unknown
    );
    assert_eq!(
        access_location_type(Some(""), &store_labels),
        AccessLocationType::Unknown
    );
}

#[test]
fn test_access_location_type_unknown_without_store_zone_label() {
    let store_labels = [label("not-zone", "zone2")];
    assert_eq!(
        access_location_type(Some("zone1"), &store_labels),
        AccessLocationType::Unknown
    );
}

#[test]
fn test_access_location_type_local_zone_when_equal() {
    let store_labels = [label("zone", "zone1")];
    assert_eq!(
        access_location_type(Some("zone1"), &store_labels),
        AccessLocationType::LocalZone
    );
}

#[test]
fn test_access_location_type_cross_zone_when_different() {
    let store_labels = [label("zone", "zone2")];
    assert_eq!(
        access_location_type(Some("zone1"), &store_labels),
        AccessLocationType::CrossZone
    );
}
