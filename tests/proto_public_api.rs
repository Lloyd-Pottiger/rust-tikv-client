use tikv_client::proto::{kvrpcpb, metapb, tikvpb};

#[test]
fn crate_root_exports_proto_message_modules() {
    let _: kvrpcpb::Context = Default::default();
    let _: metapb::Region = Default::default();
    let _: tikvpb::BatchCommandsRequest = Default::default();
}

#[test]
fn crate_root_exports_tikv_grpc_client_type() {
    type Client = tikvpb::tikv_client::TikvClient<tonic::transport::Channel>;
    let _: Option<Client> = None;
}
