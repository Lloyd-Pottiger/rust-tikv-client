use tikv_client::{util, PdRpcClient};

#[test]
fn util_pd_interceptor_public_api_exposes_identity_pd_wrapper() {
    let _: Option<util::InterceptedPdClient> = None;
    let _: fn(PdRpcClient) -> util::InterceptedPdClient = util::new_intercepted_pd_client;

    fn takes_intercepted(client: util::InterceptedPdClient) -> PdRpcClient {
        client
    }

    let _: fn(util::InterceptedPdClient) -> PdRpcClient = takes_intercepted;
}
