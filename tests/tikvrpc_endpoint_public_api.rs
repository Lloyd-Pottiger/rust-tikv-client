use tikv_client::tikvrpc::{
    EndpointType, ENGINE_LABEL_KEY, ENGINE_LABEL_TIFLASH, ENGINE_LABEL_TIFLASH_COMPUTE,
    ENGINE_ROLE_LABEL_KEY, ENGINE_ROLE_WRITE,
};
use tikv_client::StoreLabel;

#[test]
fn crate_root_reexports_endpoint_type() {
    let _: tikv_client::EndpointType = tikv_client::EndpointType::TiKv;
}

#[test]
fn tikvrpc_endpoint_type_and_constants_are_public() {
    let _: fn(EndpointType) -> &'static str = EndpointType::name;
    let _: fn(EndpointType) -> bool = EndpointType::is_tiflash_related_type;
    let _: fn(&[StoreLabel]) -> EndpointType = EndpointType::from_store_labels;

    assert_eq!(EndpointType::TiFlashCompute.name(), "tiflash_compute");
    assert!(EndpointType::TiFlashCompute.is_tiflash_related_type());
    assert_eq!(ENGINE_ROLE_LABEL_KEY, "engine_role");
    assert_eq!(ENGINE_ROLE_WRITE, "write");

    let labels = vec![StoreLabel {
        key: ENGINE_LABEL_KEY.to_owned(),
        value: ENGINE_LABEL_TIFLASH.to_owned(),
    }];
    assert_eq!(
        EndpointType::from_store_labels(&labels),
        EndpointType::TiFlash
    );

    let compute_labels = vec![StoreLabel {
        key: ENGINE_LABEL_KEY.to_owned(),
        value: ENGINE_LABEL_TIFLASH_COMPUTE.to_owned(),
    }];
    assert_eq!(
        EndpointType::from_store_labels(&compute_labels),
        EndpointType::TiFlashCompute
    );
}
