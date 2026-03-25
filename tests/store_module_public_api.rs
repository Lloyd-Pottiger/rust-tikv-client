use tikv_client::{proto::kvrpcpb, store};

#[test]
fn store_module_exports_dispatch_traits() {
    fn assert_request_impl<T: store::Request>() {}
    assert_request_impl::<kvrpcpb::GetRequest>();

    fn assert_has_key_errors_impl<T: store::HasKeyErrors>() {}
    assert_has_key_errors_impl::<kvrpcpb::BatchGetResponse>();
}

#[test]
fn store_module_exports_kv_client_trait() {
    fn assert_kv_client_trait_object(_client: &dyn store::KvClient) {}
    let _: fn(&dyn store::KvClient) = assert_kv_client_trait_object;
}
