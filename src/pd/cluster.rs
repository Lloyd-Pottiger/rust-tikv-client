// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use futures::stream;
use futures::StreamExt;
use log::error;
use log::info;
use log::warn;
use serde_derive::Deserialize;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::IntoRequest;
use tonic::Request;
use tonic::Status;

use super::timestamp::TimestampOracle;
use crate::internal_err;
use crate::proto::keyspacepb;
use crate::proto::meta_storagepb;
use crate::proto::pdpb;
use crate::proto::routerpb;
use crate::Error;
use crate::Result;
use crate::SecurityManager;
use crate::Timestamp;

/// A PD cluster.
pub struct Cluster {
    id: u64,
    client: pdpb::pd_client::PdClient<Channel>,
    router_channels: Vec<(String, Channel)>,
    next_router_channel: usize,
    follower_channels: Vec<(String, Channel)>,
    next_follower_channel: usize,
    keyspace_client: keyspacepb::keyspace_client::KeyspaceClient<Channel>,
    members: pdpb::GetMembersResponse,
    tso: TimestampOracle,
}

const PD_ALLOW_FOLLOWER_HANDLE_METADATA_KEY: &str = "pd-allow-follower-handle";
type FollowerHandleInterceptor = fn(Request<()>) -> std::result::Result<Request<()>, Status>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegionMetaTransportKind {
    Router,
    Follower,
    Leader,
}

enum RegionMetaTransport {
    Router(String, Channel),
    Follower(String, Channel),
    Leader,
}

#[derive(Deserialize)]
struct RouterServiceRegistryEntry {
    #[serde(rename = "service-addr")]
    service_addr: String,
}

macro_rules! pd_request {
    ($cluster_id:expr, $type:ty) => {{
        let mut request = <$type>::default();
        let mut header = pdpb::RequestHeader::default();
        header.cluster_id = $cluster_id;
        request.header = Some(header);
        request
    }};
}

fn allow_follower_handle_interceptor(
    mut request: Request<()>,
) -> std::result::Result<Request<()>, Status> {
    request.metadata_mut().insert(
        PD_ALLOW_FOLLOWER_HANDLE_METADATA_KEY,
        MetadataValue::from_static("true"),
    );
    Ok(request)
}

fn router_service_discovery_key(cluster_id: u64) -> Vec<u8> {
    format!("/ms/{cluster_id}/router/registry/").into_bytes()
}

fn meta_storage_prefix_end(key: &[u8]) -> Vec<u8> {
    let mut end = key.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] < 0xff {
            end[idx] += 1;
            end.truncate(idx + 1);
            return end;
        }
    }
    vec![0]
}

fn decode_router_service_addrs(response: &meta_storagepb::GetResponse) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut addrs = Vec::new();

    for kv in &response.kvs {
        let entry = match serde_json::from_slice::<RouterServiceRegistryEntry>(&kv.value) {
            Ok(entry) => entry,
            Err(err) => {
                warn!(
                    "failed to decode router service registry entry for key {:?}: {:?}",
                    kv.key, err
                );
                continue;
            }
        };

        if entry.service_addr.is_empty() || !seen.insert(entry.service_addr.clone()) {
            continue;
        }
        addrs.push(entry.service_addr);
    }

    addrs.sort();
    addrs
}

fn region_meta_transport_priority(
    allow_router_service: bool,
    has_router_channel: bool,
    allow_follower_handle: bool,
    has_follower_channel: bool,
) -> Vec<RegionMetaTransportKind> {
    let mut transports = Vec::with_capacity(3);
    if allow_router_service && has_router_channel {
        transports.push(RegionMetaTransportKind::Router);
    }
    if allow_follower_handle && has_follower_channel {
        transports.push(RegionMetaTransportKind::Follower);
    }
    transports.push(RegionMetaTransportKind::Leader);
    transports
}

fn store_meta_transport_priority(
    allow_router_service: bool,
    has_router_channel: bool,
) -> Vec<RegionMetaTransportKind> {
    let mut transports = Vec::with_capacity(2);
    if allow_router_service && has_router_channel {
        transports.push(RegionMetaTransportKind::Router);
    }
    transports.push(RegionMetaTransportKind::Leader);
    transports
}

fn should_evict_router_channel(err: &Error) -> bool {
    matches!(err, Error::PdServerTimeout(_))
        || matches!(
            err,
            Error::GrpcAPI(status)
                if matches!(
                    status.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Cancelled
                        | tonic::Code::Unimplemented
                )
        )
}

fn remove_cached_channel(
    channels: &mut Vec<(String, Channel)>,
    next_index: &mut usize,
    failed_addr: &str,
) {
    if let Some(index) = channels.iter().position(|(addr, _)| addr == failed_addr) {
        channels.remove(index);
        if channels.is_empty() {
            *next_index = 0;
        } else if *next_index >= channels.len() || index < *next_index {
            *next_index %= channels.len();
        }
    }
}

fn rotated_channel_indices(len: usize, next_index: usize, take: usize) -> (Vec<usize>, usize) {
    if len == 0 || take == 0 {
        return (Vec::new(), 0);
    }

    let start = next_index % len;
    let take = take.min(len);
    let indices = (0..take).map(|offset| (start + offset) % len).collect();
    let next_index = (start + 1) % len;
    (indices, next_index)
}

async fn send_pd_rpc<Message, Response, Rpc, RpcFuture>(
    message: Message,
    timeout: Duration,
    rpc: Rpc,
) -> Result<Response>
where
    Message: IntoRequest<Message> + Send,
    Response: PdResponse,
    Rpc: FnOnce(Request<Message>) -> RpcFuture,
    RpcFuture: Future<Output = GrpcResult<Response>>,
{
    let mut req = message.into_request();
    req.set_timeout(timeout);
    let response = match rpc(req).await {
        Ok(response) => response,
        Err(status) => {
            if status.code() == tonic::Code::DeadlineExceeded {
                return Err(crate::PdServerTimeoutError::new(status.message().to_owned()).into());
            }
            return Err(status.into());
        }
    };

    let header = response
        .header()
        .ok_or_else(|| internal_err!("PD response missing header"))?;
    if let Some(err) = &header.error {
        Err(internal_err!(err.message))
    } else {
        Ok(response)
    }
}

fn pd_grpc_status_to_error(status: tonic::Status) -> Error {
    if status.code() == tonic::Code::DeadlineExceeded {
        crate::PdServerTimeoutError::new(status.message().to_owned()).into()
    } else {
        status.into()
    }
}

async fn query_region_single_shot<Rpc, RpcFuture, Inbound>(
    timeout: Duration,
    message: pdpb::QueryRegionRequest,
    rpc: Rpc,
) -> Result<pdpb::QueryRegionResponse>
where
    Rpc: FnOnce(Request<stream::Iter<std::iter::Once<pdpb::QueryRegionRequest>>>) -> RpcFuture,
    RpcFuture: Future<Output = GrpcResult<Inbound>>,
    Inbound: futures::Stream<Item = std::result::Result<pdpb::QueryRegionResponse, tonic::Status>>
        + Unpin
        + Send,
{
    let request_stream = stream::iter(std::iter::once(message));
    let mut request = Request::new(request_stream);
    request.set_timeout(timeout);

    let mut inbound = match rpc(request).await {
        Ok(inbound) => inbound,
        Err(status) => return Err(pd_grpc_status_to_error(status)),
    };

    let response = match inbound.next().await {
        Some(Ok(response)) => response,
        Some(Err(status)) => return Err(pd_grpc_status_to_error(status)),
        None => return Err(internal_err!("PD QueryRegion RPC returned no response")),
    };

    let header = response
        .header
        .as_ref()
        .ok_or_else(|| internal_err!("PD response missing header"))?;
    if let Some(err) = &header.error {
        Err(internal_err!(err.message))
    } else {
        Ok(response)
    }
}

// These methods make a single attempt to make a request.
impl Cluster {
    pub(crate) fn cluster_id(&self) -> u64 {
        self.id
    }

    pub(crate) fn clone_members(&self) -> pdpb::GetMembersResponse {
        self.members.clone()
    }

    pub(crate) fn set_router_channels(&mut self, channels: Vec<(String, Channel)>) {
        self.router_channels = channels;
        self.next_router_channel = 0;
    }

    fn next_follower_channel(&mut self) -> Option<(String, Channel)> {
        if self.follower_channels.is_empty() {
            return None;
        }

        let idx = self.next_follower_channel % self.follower_channels.len();
        self.next_follower_channel = (idx + 1) % self.follower_channels.len();
        let (addr, channel) = &self.follower_channels[idx];
        Some((addr.clone(), channel.clone()))
    }

    fn next_region_meta_transports(
        &mut self,
        allow_router_service: bool,
        allow_follower_handle: bool,
    ) -> Vec<RegionMetaTransport> {
        let mut transports = Vec::with_capacity(self.router_channels.len() + 2);
        for kind in region_meta_transport_priority(
            allow_router_service,
            !self.router_channels.is_empty(),
            allow_follower_handle,
            !self.follower_channels.is_empty(),
        ) {
            match kind {
                RegionMetaTransportKind::Router => {
                    let (indices, next_index) = rotated_channel_indices(
                        self.router_channels.len(),
                        self.next_router_channel,
                        self.router_channels.len(),
                    );
                    self.next_router_channel = next_index;
                    for index in indices {
                        let (addr, channel) = &self.router_channels[index];
                        transports.push(RegionMetaTransport::Router(addr.clone(), channel.clone()));
                    }
                }
                RegionMetaTransportKind::Follower => {
                    if let Some((addr, channel)) = self.next_follower_channel() {
                        transports.push(RegionMetaTransport::Follower(addr, channel));
                    }
                }
                RegionMetaTransportKind::Leader => transports.push(RegionMetaTransport::Leader),
            }
        }
        transports
    }

    fn next_store_meta_transports(
        &mut self,
        allow_router_service: bool,
    ) -> Vec<RegionMetaTransport> {
        let mut transports = Vec::with_capacity(self.router_channels.len() + 1);
        for kind in
            store_meta_transport_priority(allow_router_service, !self.router_channels.is_empty())
        {
            match kind {
                RegionMetaTransportKind::Router => {
                    let (indices, next_index) = rotated_channel_indices(
                        self.router_channels.len(),
                        self.next_router_channel,
                        self.router_channels.len(),
                    );
                    self.next_router_channel = next_index;
                    for index in indices {
                        let (addr, channel) = &self.router_channels[index];
                        transports.push(RegionMetaTransport::Router(addr.clone(), channel.clone()));
                    }
                }
                RegionMetaTransportKind::Leader => transports.push(RegionMetaTransport::Leader),
                RegionMetaTransportKind::Follower => {}
            }
        }
        transports
    }

    fn evict_router_channel(&mut self, failed_addr: &str) {
        remove_cached_channel(
            &mut self.router_channels,
            &mut self.next_router_channel,
            failed_addr,
        );
    }

    pub async fn get_region(
        &mut self,
        key: Vec<u8>,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        self.get_region_inner(key, false, false, timeout).await
    }

    async fn get_region_inner(
        &mut self,
        key: Vec<u8>,
        need_buckets: bool,
        allow_follower_handle: bool,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        let mut req = pd_request!(self.id, pdpb::GetRegionRequest);
        req.region_key = key;
        req.need_buckets = need_buckets;

        for transport in self.next_region_meta_transports(true, allow_follower_handle) {
            match transport {
                RegionMetaTransport::Router(addr, channel) => {
                    let mut router_client = routerpb::router_client::RouterClient::new(channel);
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(router_client.get_region(request).await?.into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => {
                            if should_evict_router_channel(&err) {
                                self.evict_router_channel(&addr);
                            }
                            warn!(
                                "router-service get_region failed on {}, falling back: {:?}",
                                addr, err
                            );
                        }
                    }
                }
                RegionMetaTransport::Follower(addr, channel) => {
                    let mut follower_client = pdpb::pd_client::PdClient::with_interceptor(
                        channel,
                        allow_follower_handle_interceptor as FollowerHandleInterceptor,
                    );
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(follower_client.get_region(request).await?.into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => warn!(
                            "follower-handled get_region failed on {}, falling back: {:?}",
                            addr, err
                        ),
                    }
                }
                RegionMetaTransport::Leader => return req.send(&mut self.client, timeout).await,
            }
        }

        req.send(&mut self.client, timeout).await
    }

    pub async fn get_region_with_buckets(
        &mut self,
        key: Vec<u8>,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        self.get_region_inner(key, true, false, timeout).await
    }

    pub(crate) async fn get_region_with_buckets_allow_follower_handle(
        &mut self,
        key: Vec<u8>,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        self.get_region_inner(key, true, true, timeout).await
    }

    pub async fn get_prev_region(
        &mut self,
        key: Vec<u8>,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        self.get_prev_region_inner(key, false, timeout).await
    }

    async fn get_prev_region_inner(
        &mut self,
        key: Vec<u8>,
        need_buckets: bool,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        let mut req = pd_request!(self.id, pdpb::GetRegionRequest);
        req.region_key = key;
        req.need_buckets = need_buckets;

        for transport in self.next_region_meta_transports(true, true) {
            match transport {
                RegionMetaTransport::Router(addr, channel) => {
                    let mut router_client = routerpb::router_client::RouterClient::new(channel);
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(router_client.get_prev_region(request).await?.into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => {
                            if should_evict_router_channel(&err) {
                                self.evict_router_channel(&addr);
                            }
                            warn!(
                                "router-service get_prev_region failed on {}, falling back: {:?}",
                                addr, err
                            );
                        }
                    }
                }
                RegionMetaTransport::Follower(addr, channel) => {
                    let mut follower_client = pdpb::pd_client::PdClient::with_interceptor(
                        channel,
                        allow_follower_handle_interceptor as FollowerHandleInterceptor,
                    );
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(follower_client.get_prev_region(request).await?.into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => warn!(
                            "follower-handled get_prev_region failed on {}, falling back: {:?}",
                            addr, err
                        ),
                    }
                }
                RegionMetaTransport::Leader => {
                    let mut request = Request::new(req.clone());
                    request.set_timeout(timeout);
                    let response = self.client.get_prev_region(request).await?.into_inner();

                    let header = response
                        .header()
                        .ok_or_else(|| internal_err!("PD response missing header"))?;
                    if let Some(err) = &header.error {
                        return Err(internal_err!(err.message));
                    }
                    return Ok(response);
                }
            }
        }

        let mut request = Request::new(req);
        request.set_timeout(timeout);
        let response = self.client.get_prev_region(request).await?.into_inner();

        let header = response
            .header()
            .ok_or_else(|| internal_err!("PD response missing header"))?;
        if let Some(err) = &header.error {
            Err(internal_err!(err.message))
        } else {
            Ok(response)
        }
    }

    pub async fn get_prev_region_with_buckets(
        &mut self,
        key: Vec<u8>,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        self.get_prev_region_inner(key, true, timeout).await
    }

    pub async fn get_region_by_id(
        &mut self,
        id: u64,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        self.get_region_by_id_inner(id, false, false, timeout).await
    }

    async fn get_region_by_id_inner(
        &mut self,
        id: u64,
        need_buckets: bool,
        allow_follower_handle: bool,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        let mut req = pd_request!(self.id, pdpb::GetRegionByIdRequest);
        req.region_id = id;
        req.need_buckets = need_buckets;

        for transport in self.next_region_meta_transports(true, allow_follower_handle) {
            match transport {
                RegionMetaTransport::Router(addr, channel) => {
                    let mut router_client = routerpb::router_client::RouterClient::new(channel);
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(router_client.get_region_by_id(request).await?.into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => {
                            if should_evict_router_channel(&err) {
                                self.evict_router_channel(&addr);
                            }
                            warn!(
                                "router-service get_region_by_id failed on {}, falling back: {:?}",
                                addr, err
                            );
                        }
                    }
                }
                RegionMetaTransport::Follower(addr, channel) => {
                    let mut follower_client = pdpb::pd_client::PdClient::with_interceptor(
                        channel,
                        allow_follower_handle_interceptor as FollowerHandleInterceptor,
                    );
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(follower_client
                            .get_region_by_id(request)
                            .await?
                            .into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => warn!(
                            "follower-handled get_region_by_id failed on {}, falling back: {:?}",
                            addr, err
                        ),
                    }
                }
                RegionMetaTransport::Leader => return req.send(&mut self.client, timeout).await,
            }
        }

        req.send(&mut self.client, timeout).await
    }

    pub async fn get_region_by_id_with_buckets(
        &mut self,
        id: u64,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        self.get_region_by_id_inner(id, true, false, timeout).await
    }

    pub(crate) async fn get_region_by_id_with_buckets_allow_follower_handle(
        &mut self,
        id: u64,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        self.get_region_by_id_inner(id, true, true, timeout).await
    }

    pub(crate) async fn query_region(
        &mut self,
        keys: Vec<Vec<u8>>,
        prev_keys: Vec<Vec<u8>>,
        ids: Vec<u64>,
        need_buckets: bool,
        timeout: Duration,
    ) -> Result<pdpb::QueryRegionResponse> {
        self.query_region_inner(keys, prev_keys, ids, need_buckets, false, timeout)
            .await
    }

    pub(crate) async fn query_region_allow_follower_handle(
        &mut self,
        keys: Vec<Vec<u8>>,
        prev_keys: Vec<Vec<u8>>,
        ids: Vec<u64>,
        need_buckets: bool,
        timeout: Duration,
    ) -> Result<pdpb::QueryRegionResponse> {
        self.query_region_inner(keys, prev_keys, ids, need_buckets, true, timeout)
            .await
    }

    async fn query_region_inner(
        &mut self,
        keys: Vec<Vec<u8>>,
        prev_keys: Vec<Vec<u8>>,
        ids: Vec<u64>,
        need_buckets: bool,
        allow_follower_handle: bool,
        timeout: Duration,
    ) -> Result<pdpb::QueryRegionResponse> {
        let mut req = pd_request!(self.id, pdpb::QueryRegionRequest);
        req.need_buckets = need_buckets;
        req.ids = ids;
        req.keys = keys;
        req.prev_keys = prev_keys;

        for transport in self.next_region_meta_transports(true, allow_follower_handle) {
            match transport {
                RegionMetaTransport::Router(addr, channel) => {
                    let mut router_client = routerpb::router_client::RouterClient::new(channel);
                    match query_region_single_shot(timeout, req.clone(), |request| async {
                        Ok(router_client.query_region(request).await?.into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => {
                            if should_evict_router_channel(&err) {
                                self.evict_router_channel(&addr);
                            }
                            warn!(
                                "router-service query_region failed on {}, falling back: {:?}",
                                addr, err
                            );
                        }
                    }
                }
                RegionMetaTransport::Follower(addr, channel) => {
                    let mut follower_client = pdpb::pd_client::PdClient::with_interceptor(
                        channel,
                        allow_follower_handle_interceptor as FollowerHandleInterceptor,
                    );
                    match query_region_single_shot(timeout, req.clone(), |request| async {
                        Ok(follower_client.query_region(request).await?.into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => warn!(
                            "follower-handled query_region failed on {}, falling back: {:?}",
                            addr, err
                        ),
                    }
                }
                RegionMetaTransport::Leader => {
                    return query_region_single_shot(timeout, req.clone(), |request| async {
                        Ok(self.client.query_region(request).await?.into_inner())
                    })
                    .await;
                }
            }
        }

        query_region_single_shot(timeout, req, |request| async {
            Ok(self.client.query_region(request).await?.into_inner())
        })
        .await
    }

    pub async fn scan_regions(
        &mut self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: i32,
        timeout: Duration,
    ) -> Result<pdpb::ScanRegionsResponse> {
        self.scan_regions_inner(start_key, end_key, limit, false, timeout)
            .await
    }

    pub(crate) async fn scan_regions_allow_follower_handle(
        &mut self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: i32,
        timeout: Duration,
    ) -> Result<pdpb::ScanRegionsResponse> {
        self.scan_regions_inner(start_key, end_key, limit, true, timeout)
            .await
    }

    async fn scan_regions_inner(
        &mut self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: i32,
        allow_follower_handle: bool,
        timeout: Duration,
    ) -> Result<pdpb::ScanRegionsResponse> {
        let mut req = pd_request!(self.id, pdpb::ScanRegionsRequest);
        req.start_key = start_key;
        req.end_key = end_key;
        req.limit = limit;

        if allow_follower_handle {
            if let Some((addr, channel)) = self.next_follower_channel() {
                let mut follower_client = pdpb::pd_client::PdClient::with_interceptor(
                    channel,
                    allow_follower_handle_interceptor as FollowerHandleInterceptor,
                );
                match send_pd_rpc(req.clone(), timeout, |request| async {
                    Ok(follower_client.scan_regions(request).await?.into_inner())
                })
                .await
                {
                    Ok(resp) => return Ok(resp),
                    Err(err) => warn!(
                        "follower-handled scan_regions failed on {}, falling back to leader: {:?}",
                        addr, err
                    ),
                }
            }
        }

        req.send(&mut self.client, timeout).await
    }

    pub async fn batch_scan_regions(
        &mut self,
        ranges: Vec<pdpb::KeyRange>,
        limit: i32,
        need_buckets: bool,
        timeout: Duration,
    ) -> Result<pdpb::BatchScanRegionsResponse> {
        self.batch_scan_regions_inner(ranges, limit, need_buckets, false, timeout)
            .await
    }

    pub(crate) async fn batch_scan_regions_allow_follower_handle(
        &mut self,
        ranges: Vec<pdpb::KeyRange>,
        limit: i32,
        need_buckets: bool,
        timeout: Duration,
    ) -> Result<pdpb::BatchScanRegionsResponse> {
        self.batch_scan_regions_inner(ranges, limit, need_buckets, true, timeout)
            .await
    }

    async fn batch_scan_regions_inner(
        &mut self,
        ranges: Vec<pdpb::KeyRange>,
        limit: i32,
        need_buckets: bool,
        allow_follower_handle: bool,
        timeout: Duration,
    ) -> Result<pdpb::BatchScanRegionsResponse> {
        let mut req = pd_request!(self.id, pdpb::BatchScanRegionsRequest);
        req.need_buckets = need_buckets;
        req.ranges = ranges;
        req.limit = limit;
        // Keep `contain_all_key_range` false to allow partial responses when `limit` is reached,
        // matching client-go's incremental batch scan usage.

        for transport in self.next_region_meta_transports(true, allow_follower_handle) {
            match transport {
                RegionMetaTransport::Router(addr, channel) => {
                    let mut router_client = routerpb::router_client::RouterClient::new(channel);
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(router_client
                            .batch_scan_regions(request)
                            .await?
                            .into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => {
                            if should_evict_router_channel(&err) {
                                self.evict_router_channel(&addr);
                            }
                            warn!(
                                "router-service batch_scan_regions failed on {}, falling back: {:?}",
                                addr, err
                            );
                        }
                    }
                }
                RegionMetaTransport::Follower(addr, channel) => {
                    let mut follower_client = pdpb::pd_client::PdClient::with_interceptor(
                        channel,
                        allow_follower_handle_interceptor as FollowerHandleInterceptor,
                    );
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(follower_client
                            .batch_scan_regions(request)
                            .await?
                            .into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => warn!(
                            "follower-handled batch_scan_regions failed on {}, falling back: {:?}",
                            addr, err
                        ),
                    }
                }
                RegionMetaTransport::Leader => return req.send(&mut self.client, timeout).await,
            }
        }

        req.send(&mut self.client, timeout).await
    }

    pub async fn get_store(
        &mut self,
        id: u64,
        timeout: Duration,
    ) -> Result<pdpb::GetStoreResponse> {
        self.get_store_inner(id, timeout, true).await
    }

    pub async fn get_store_from_leader(
        &mut self,
        id: u64,
        timeout: Duration,
    ) -> Result<pdpb::GetStoreResponse> {
        self.get_store_inner(id, timeout, false).await
    }

    async fn get_store_inner(
        &mut self,
        id: u64,
        timeout: Duration,
        allow_router_service: bool,
    ) -> Result<pdpb::GetStoreResponse> {
        let mut req = pd_request!(self.id, pdpb::GetStoreRequest);
        req.store_id = id;

        for transport in self.next_store_meta_transports(allow_router_service) {
            match transport {
                RegionMetaTransport::Router(addr, channel) => {
                    let mut router_client = routerpb::router_client::RouterClient::new(channel);
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(router_client.get_store(request).await?.into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => {
                            if should_evict_router_channel(&err) {
                                self.evict_router_channel(&addr);
                            }
                            warn!(
                                "router-service get_store failed on {}, falling back: {:?}",
                                addr, err
                            );
                        }
                    }
                }
                RegionMetaTransport::Leader => return req.send(&mut self.client, timeout).await,
                RegionMetaTransport::Follower(_, _) => {}
            }
        }

        req.send(&mut self.client, timeout).await
    }

    pub async fn get_all_stores(
        &mut self,
        timeout: Duration,
    ) -> Result<pdpb::GetAllStoresResponse> {
        self.get_all_stores_inner(timeout, true).await
    }

    pub async fn get_all_stores_from_leader(
        &mut self,
        timeout: Duration,
    ) -> Result<pdpb::GetAllStoresResponse> {
        self.get_all_stores_inner(timeout, false).await
    }

    async fn get_all_stores_inner(
        &mut self,
        timeout: Duration,
        allow_router_service: bool,
    ) -> Result<pdpb::GetAllStoresResponse> {
        let req = pd_request!(self.id, pdpb::GetAllStoresRequest);

        for transport in self.next_store_meta_transports(allow_router_service) {
            match transport {
                RegionMetaTransport::Router(addr, channel) => {
                    let mut router_client = routerpb::router_client::RouterClient::new(channel);
                    match send_pd_rpc(req.clone(), timeout, |request| async {
                        Ok(router_client.get_all_stores(request).await?.into_inner())
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(err) => {
                            if should_evict_router_channel(&err) {
                                self.evict_router_channel(&addr);
                            }
                            warn!(
                                "router-service get_all_stores failed on {}, falling back: {:?}",
                                addr, err
                            );
                        }
                    }
                }
                RegionMetaTransport::Leader => return req.send(&mut self.client, timeout).await,
                RegionMetaTransport::Follower(_, _) => {}
            }
        }

        req.send(&mut self.client, timeout).await
    }

    pub async fn get_timestamp(&self) -> Result<Timestamp> {
        self.tso.clone().get_timestamp().await
    }

    pub async fn get_timestamp_with_dc_location(&self, dc_location: String) -> Result<Timestamp> {
        self.tso
            .clone()
            .get_timestamp_with_dc_location(dc_location)
            .await
    }

    pub async fn get_min_ts(&mut self, timeout: Duration) -> Result<Timestamp> {
        let req = pd_request!(self.id, pdpb::GetMinTsRequest);
        let resp = req.send(&mut self.client, timeout).await?;
        resp.timestamp
            .ok_or_else(|| internal_err!("GetMinTsResponse missing timestamp"))
    }

    pub async fn set_external_timestamp(
        &mut self,
        timestamp: u64,
        timeout: Duration,
    ) -> Result<()> {
        let mut req = pd_request!(self.id, pdpb::SetExternalTimestampRequest);
        req.timestamp = timestamp;
        let _resp = req.send(&mut self.client, timeout).await?;
        Ok(())
    }

    pub async fn get_external_timestamp(&mut self, timeout: Duration) -> Result<u64> {
        let req = pd_request!(self.id, pdpb::GetExternalTimestampRequest);
        let resp = req.send(&mut self.client, timeout).await?;
        Ok(resp.timestamp)
    }

    pub async fn get_gc_safe_point(
        &mut self,
        timeout: Duration,
    ) -> Result<pdpb::GetGcSafePointResponse> {
        let req = pd_request!(self.id, pdpb::GetGcSafePointRequest);
        req.send(&mut self.client, timeout).await
    }

    pub async fn get_gc_safe_point_v2(
        &mut self,
        keyspace_id: u32,
        timeout: Duration,
    ) -> Result<pdpb::GetGcSafePointV2Response> {
        let mut req = pd_request!(self.id, pdpb::GetGcSafePointV2Request);
        req.keyspace_id = keyspace_id;
        req.send(&mut self.client, timeout).await
    }

    pub async fn update_safepoint(
        &mut self,
        safepoint: u64,
        timeout: Duration,
    ) -> Result<pdpb::UpdateGcSafePointResponse> {
        let mut req = pd_request!(self.id, pdpb::UpdateGcSafePointRequest);
        req.safe_point = safepoint;
        req.send(&mut self.client, timeout).await
    }

    pub async fn update_service_gc_safe_point(
        &mut self,
        service_id: String,
        ttl: i64,
        safe_point: u64,
        timeout: Duration,
    ) -> Result<pdpb::UpdateServiceGcSafePointResponse> {
        let mut req = pd_request!(self.id, pdpb::UpdateServiceGcSafePointRequest);
        req.service_id = service_id.into_bytes();
        req.ttl = ttl;
        req.safe_point = safe_point;
        req.send(&mut self.client, timeout).await
    }

    pub async fn update_service_safe_point_v2(
        &mut self,
        keyspace_id: u32,
        service_id: String,
        ttl: i64,
        safe_point: u64,
        timeout: Duration,
    ) -> Result<pdpb::UpdateServiceSafePointV2Response> {
        let mut req = pd_request!(self.id, pdpb::UpdateServiceSafePointV2Request);
        req.keyspace_id = keyspace_id;
        req.service_id = service_id.into_bytes();
        req.safe_point = safe_point;
        req.ttl = ttl;
        req.send(&mut self.client, timeout).await
    }

    pub async fn update_gc_safe_point_v2(
        &mut self,
        keyspace_id: u32,
        safe_point: u64,
        timeout: Duration,
    ) -> Result<pdpb::UpdateGcSafePointV2Response> {
        let mut req = pd_request!(self.id, pdpb::UpdateGcSafePointV2Request);
        req.keyspace_id = keyspace_id;
        req.safe_point = safe_point;
        req.send(&mut self.client, timeout).await
    }

    pub async fn scatter_regions(
        &mut self,
        region_ids: Vec<u64>,
        group: Option<String>,
        timeout: Duration,
    ) -> Result<pdpb::ScatterRegionResponse> {
        let mut req = pd_request!(self.id, pdpb::ScatterRegionRequest);
        req.regions_id = region_ids;
        if let Some(group) = group {
            req.group = group;
        }
        req.send(&mut self.client, timeout).await
    }

    pub async fn get_operator(
        &mut self,
        region_id: u64,
        timeout: Duration,
    ) -> Result<pdpb::GetOperatorResponse> {
        let mut req = pd_request!(self.id, pdpb::GetOperatorRequest);
        req.region_id = region_id;
        req.send(&mut self.client, timeout).await
    }

    pub async fn load_keyspace(
        &mut self,
        keyspace: &str,
        timeout: Duration,
    ) -> Result<keyspacepb::KeyspaceMeta> {
        let mut req = pd_request!(self.id, keyspacepb::LoadKeyspaceRequest);
        req.name = keyspace.to_string();
        let resp = req.send(&mut self.keyspace_client, timeout).await?;
        let keyspace = resp
            .keyspace
            .ok_or_else(|| Error::KeyspaceNotFound(keyspace.to_owned()))?;
        Ok(keyspace)
    }
}

/// An object for connecting and reconnecting to a PD cluster.
pub struct Connection {
    security_mgr: Arc<SecurityManager>,
}

impl Connection {
    pub fn new(security_mgr: Arc<SecurityManager>) -> Connection {
        Connection { security_mgr }
    }

    pub async fn connect_cluster(
        &self,
        endpoints: &[String],
        timeout: Duration,
        tso_max_pending_count: usize,
    ) -> Result<Cluster> {
        let members = self.validate_endpoints(endpoints, timeout).await?;
        let (client, keyspace_client, members) = self.try_connect_leader(&members, timeout).await?;
        let id = members
            .header
            .as_ref()
            .ok_or_else(|| internal_err!("PD get_members response missing header"))?
            .cluster_id;
        let router_channels = self.connect_router_channels(&members, id, timeout).await;
        let follower_channels = self.connect_follower_channels(&members).await;
        let tso = TimestampOracle::new(id, &client, tso_max_pending_count)?;
        let cluster = Cluster {
            id,
            client,
            router_channels,
            next_router_channel: 0,
            follower_channels,
            next_follower_channel: 0,
            keyspace_client,
            members,
            tso,
        };
        Ok(cluster)
    }

    // Re-establish connection with PD leader in asynchronous fashion.
    pub async fn reconnect(
        &self,
        cluster: &mut Cluster,
        timeout: Duration,
        tso_max_pending_count: usize,
    ) -> Result<()> {
        warn!("updating pd client");
        let start = Instant::now();
        let (client, keyspace_client, members) =
            self.try_connect_leader(&cluster.members, timeout).await?;
        let router_channels = self
            .connect_router_channels(&members, cluster.id, timeout)
            .await;
        let follower_channels = self.connect_follower_channels(&members).await;
        let tso = TimestampOracle::new(cluster.id, &client, tso_max_pending_count)?;
        *cluster = Cluster {
            id: cluster.id,
            client,
            router_channels,
            next_router_channel: 0,
            follower_channels,
            next_follower_channel: 0,
            keyspace_client,
            members,
            tso,
        };

        info!("updating PD client done, spent {:?}", start.elapsed());
        Ok(())
    }

    async fn validate_endpoints(
        &self,
        endpoints: &[String],
        timeout: Duration,
    ) -> Result<pdpb::GetMembersResponse> {
        let mut endpoints_set = HashSet::with_capacity(endpoints.len());

        let mut members = None;
        let mut cluster_id = None;
        for ep in endpoints {
            if !endpoints_set.insert(ep) {
                return Err(internal_err!("duplicated PD endpoint {}", ep));
            }

            let (_, _, resp) = match self.connect(ep, timeout).await {
                Ok(resp) => resp,
                // Ignore failed PD node.
                Err(e) => {
                    warn!("PD endpoint {} failed to respond: {:?}", ep, e);
                    continue;
                }
            };

            // Check cluster ID.
            let cid = resp
                .header
                .as_ref()
                .ok_or_else(|| {
                    internal_err!(
                        "PD endpoint {} returned get_members response missing header",
                        ep
                    )
                })?
                .cluster_id;
            if let Some(sample) = cluster_id {
                if sample != cid {
                    return Err(internal_err!(
                        "PD response cluster_id mismatch, want {}, got {}",
                        sample,
                        cid
                    ));
                }
            } else {
                cluster_id = Some(cid);
            }

            if let Some(sample) = members.as_ref() {
                Connection::validate_get_members_consistency(ep, sample, &resp)?;
            } else {
                members = Some(resp);
            }
        }

        match members {
            Some(members) => {
                info!("All PD endpoints are consistent: {:?}", endpoints);
                Ok(members)
            }
            _ => Err(internal_err!("PD cluster failed to respond")),
        }
    }

    async fn connect(
        &self,
        addr: &str,
        _timeout: Duration,
    ) -> Result<(
        pdpb::pd_client::PdClient<Channel>,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        let mut client = self
            .security_mgr
            .connect(addr, pdpb::pd_client::PdClient::<Channel>::new)
            .await?;
        let keyspace_client = self
            .security_mgr
            .connect(
                addr,
                keyspacepb::keyspace_client::KeyspaceClient::<Channel>::new,
            )
            .await?;
        let resp: pdpb::GetMembersResponse = client
            .get_members(pdpb::GetMembersRequest::default())
            .await?
            .into_inner();
        let header = resp
            .header
            .as_ref()
            .ok_or_else(|| internal_err!("PD get_members response missing header"))?;
        if let Some(err) = header.error.as_ref() {
            return Err(internal_err!("failed to get PD members, err {:?}", err));
        }
        if resp.leader.is_none() {
            return Err(internal_err!(
                "unexpected no PD leader in get member resp: {:?}",
                resp
            ));
        }
        Ok((client, keyspace_client, resp))
    }

    async fn connect_channel(&self, addr: &str) -> Result<Channel> {
        self.security_mgr.connect(addr, |channel| channel).await
    }

    async fn connect_meta_storage_client(
        &self,
        addr: &str,
    ) -> Result<meta_storagepb::meta_storage_client::MetaStorageClient<Channel>> {
        self.security_mgr
            .connect(
                addr,
                meta_storagepb::meta_storage_client::MetaStorageClient::new,
            )
            .await
    }

    pub(crate) async fn try_connect_router_channels(
        &self,
        members: &pdpb::GetMembersResponse,
        cluster_id: u64,
        timeout: Duration,
    ) -> Result<Vec<(String, Channel)>> {
        let router_addrs = self
            .discover_router_service_addrs(members, cluster_id, timeout)
            .await?;

        let mut router_channels = Vec::new();
        for addr in router_addrs {
            match self.connect_channel(&addr).await {
                Ok(channel) => router_channels.push((addr, channel)),
                Err(err) => warn!(
                    "failed to connect router service endpoint {}, fallback stays active: {:?}",
                    addr, err
                ),
            }
        }

        Ok(router_channels)
    }

    async fn connect_router_channels(
        &self,
        members: &pdpb::GetMembersResponse,
        cluster_id: u64,
        timeout: Duration,
    ) -> Vec<(String, Channel)> {
        match self
            .try_connect_router_channels(members, cluster_id, timeout)
            .await
        {
            Ok(channels) => channels,
            Err(err) => {
                warn!(
                    "failed to discover router service endpoints, leader/follower fallback stays active: {:?}",
                    err
                );
                Vec::new()
            }
        }
    }

    async fn connect_follower_channels(
        &self,
        members: &pdpb::GetMembersResponse,
    ) -> Vec<(String, Channel)> {
        let Some(leader) = members.leader.as_ref() else {
            return Vec::new();
        };

        let mut follower_channels = Vec::new();
        let mut seen = HashSet::new();
        let leader_urls: HashSet<&str> = leader.client_urls.iter().map(String::as_str).collect();

        for member in &members.members {
            if member.member_id == leader.member_id {
                continue;
            }
            for addr in &member.client_urls {
                if leader_urls.contains(addr.as_str()) || !seen.insert(addr.clone()) {
                    continue;
                }
                match self.connect_channel(addr).await {
                    Ok(channel) => follower_channels.push((addr.clone(), channel)),
                    Err(err) => warn!(
                        "failed to connect follower PD endpoint {}, leader fallback stays active: {:?}",
                        addr, err
                    ),
                }
            }
        }

        follower_channels
    }

    async fn discover_router_service_addrs(
        &self,
        members: &pdpb::GetMembersResponse,
        cluster_id: u64,
        timeout: Duration,
    ) -> Result<Vec<String>> {
        let Some(leader) = members.leader.as_ref() else {
            return Ok(Vec::new());
        };

        let discovery_key = router_service_discovery_key(cluster_id);
        let range_end = meta_storage_prefix_end(&discovery_key);
        let mut last_err = None;

        for addr in &leader.client_urls {
            let mut client = match self.connect_meta_storage_client(addr).await {
                Ok(client) => client,
                Err(err) => {
                    last_err = Some(err);
                    continue;
                }
            };

            let mut req = meta_storagepb::GetRequest::default();
            let mut header = meta_storagepb::RequestHeader::default();
            header.cluster_id = cluster_id;
            req.header = Some(header);
            req.key = discovery_key.clone();
            req.range_end = range_end.clone();

            let mut request = Request::new(req);
            request.set_timeout(timeout);

            let response = match client.get(request).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    last_err = Some(status.into());
                    continue;
                }
            };

            let header = response
                .header
                .as_ref()
                .ok_or_else(|| internal_err!("meta storage get response missing header"))?;
            if let Some(err) = &header.error {
                return Err(internal_err!(
                    "meta storage router discovery failed, err {:?}",
                    err
                ));
            }
            return Ok(decode_router_service_addrs(&response));
        }

        if let Some(err) = last_err {
            return Err(err);
        }
        Ok(Vec::new())
    }

    async fn try_connect(
        &self,
        addr: &str,
        cluster_id: u64,
        timeout: Duration,
    ) -> Result<(
        pdpb::pd_client::PdClient<Channel>,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        let (client, keyspace_client, r) = self.connect(addr, timeout).await?;
        Connection::validate_cluster_id(addr, &r, cluster_id)?;
        Ok((client, keyspace_client, r))
    }

    fn validate_cluster_id(
        addr: &str,
        members: &pdpb::GetMembersResponse,
        cluster_id: u64,
    ) -> Result<()> {
        let new_cluster_id = members
            .header
            .as_ref()
            .ok_or_else(|| {
                internal_err!(
                    "PD endpoint {} returned get_members response missing header",
                    addr
                )
            })?
            .cluster_id;
        if new_cluster_id != cluster_id {
            Err(internal_err!(
                "{} no longer belongs to cluster {}, it is in {}",
                addr,
                cluster_id,
                new_cluster_id
            ))
        } else {
            Ok(())
        }
    }

    fn validate_get_members_consistency(
        addr: &str,
        sample: &pdpb::GetMembersResponse,
        current: &pdpb::GetMembersResponse,
    ) -> Result<()> {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct NormalizedMember {
            name: String,
            peer_urls: Vec<String>,
            client_urls: Vec<String>,
        }

        fn normalize_members(resp: &pdpb::GetMembersResponse) -> BTreeMap<u64, NormalizedMember> {
            resp.members
                .iter()
                .map(|member| {
                    let mut peer_urls = member.peer_urls.clone();
                    peer_urls.sort();
                    let mut client_urls = member.client_urls.clone();
                    client_urls.sort();
                    (
                        member.member_id,
                        NormalizedMember {
                            name: member.name.clone(),
                            peer_urls,
                            client_urls,
                        },
                    )
                })
                .collect()
        }

        let sample = normalize_members(sample);
        let current = normalize_members(current);
        if sample == current {
            return Ok(());
        }

        let missing: Vec<u64> = sample
            .keys()
            .filter(|id| !current.contains_key(id))
            .copied()
            .collect();
        let extra: Vec<u64> = current
            .keys()
            .filter(|id| !sample.contains_key(id))
            .copied()
            .collect();
        let changed: Vec<u64> = sample
            .iter()
            .filter_map(|(id, sample_member)| {
                current
                    .get(id)
                    .filter(|current_member| *current_member != sample_member)
                    .map(|_| *id)
            })
            .collect();

        Err(internal_err!(
            "PD endpoint {} returned inconsistent get_members members list (missing={:?}, extra={:?}, changed={:?})",
            addr,
            missing,
            extra,
            changed
        ))
    }

    async fn try_connect_leader(
        &self,
        previous: &pdpb::GetMembersResponse,
        timeout: Duration,
    ) -> Result<(
        pdpb::pd_client::PdClient<Channel>,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        let previous_leader = previous
            .leader
            .as_ref()
            .ok_or_else(|| internal_err!("no leader found in GetMembersResponse"))?;
        let members = &previous.members;
        let cluster_id = previous
            .header
            .as_ref()
            .ok_or_else(|| internal_err!("GetMembersResponse missing header"))?
            .cluster_id;

        let mut resp = None;
        // Try to connect to other members, then the previous leader.
        'outer: for m in members
            .iter()
            .filter(|m| *m != previous_leader)
            .chain(Some(previous_leader))
        {
            for ep in &m.client_urls {
                match self.try_connect(ep.as_str(), cluster_id, timeout).await {
                    Ok((_, _, r)) => {
                        resp = Some(r);
                        break 'outer;
                    }
                    Err(e) => {
                        error!("failed to connect to {}, {:?}", ep, e);
                        continue;
                    }
                }
            }
        }

        // Then try to connect the PD cluster leader.
        if let Some(resp) = resp {
            let leader = resp
                .leader
                .as_ref()
                .ok_or_else(|| internal_err!("no leader found in GetMembersResponse"))?;

            for ep in &leader.client_urls {
                if let Ok((client, keyspace_client, members)) =
                    self.try_connect(ep.as_str(), cluster_id, timeout).await
                {
                    return Ok((client, keyspace_client, members));
                }
            }
        }

        Err(internal_err!("failed to connect to {:?}", members))
    }
}

type GrpcResult<T> = std::result::Result<T, tonic::Status>;

#[async_trait]
trait PdMessage: Sized {
    type Client: Send;
    type Response: PdResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response>;

    async fn send(self, client: &mut Self::Client, timeout: Duration) -> Result<Self::Response> {
        let mut req = self.into_request();
        req.set_timeout(timeout);
        let response = match Self::rpc(req, client).await {
            Ok(response) => response,
            Err(status) => {
                if status.code() == tonic::Code::DeadlineExceeded {
                    return Err(
                        crate::PdServerTimeoutError::new(status.message().to_owned()).into(),
                    );
                }
                return Err(status.into());
            }
        };

        let header = response
            .header()
            .ok_or_else(|| internal_err!("PD response missing header"))?;
        if let Some(err) = &header.error {
            Err(internal_err!(err.message))
        } else {
            Ok(response)
        }
    }
}

#[async_trait]
impl PdMessage for pdpb::GetRegionRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_region(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetRegionByIdRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_region_by_id(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::ScanRegionsRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::ScanRegionsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.scan_regions(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::BatchScanRegionsRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::BatchScanRegionsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.batch_scan_regions(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetStoreRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetStoreResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_store(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetAllStoresRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetAllStoresResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_all_stores(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetMinTsRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetMinTsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_min_ts(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::SetExternalTimestampRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::SetExternalTimestampResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.set_external_timestamp(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetExternalTimestampRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetExternalTimestampResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_external_timestamp(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetGcSafePointRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetGcSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_gc_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetGcSafePointV2Request {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetGcSafePointV2Response;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_gc_safe_point_v2(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateGcSafePointRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::UpdateGcSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_gc_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateGcSafePointV2Request {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::UpdateGcSafePointV2Response;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_gc_safe_point_v2(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateServiceGcSafePointRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::UpdateServiceGcSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_service_gc_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateServiceSafePointV2Request {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::UpdateServiceSafePointV2Response;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_service_safe_point_v2(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::ScatterRegionRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::ScatterRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.scatter_region(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetOperatorRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetOperatorResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_operator(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for keyspacepb::LoadKeyspaceRequest {
    type Client = keyspacepb::keyspace_client::KeyspaceClient<Channel>;
    type Response = keyspacepb::LoadKeyspaceResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.load_keyspace(req).await?.into_inner())
    }
}

trait PdResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader>;
}

impl PdResponse for pdpb::GetStoreResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetRegionResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::ScanRegionsResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::BatchScanRegionsResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetAllStoresResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::UpdateGcSafePointResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::UpdateGcSafePointV2Response {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::UpdateServiceGcSafePointResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::UpdateServiceSafePointV2Response {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetGcSafePointResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetGcSafePointV2Response {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::ScatterRegionResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetOperatorResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetMinTsResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::SetExternalTimestampResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetExternalTimestampResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for keyspacepb::LoadKeyspaceResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lazy_channel(port: u16) -> Channel {
        Channel::from_shared(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect_lazy()
    }

    fn test_cluster(
        router_addrs: &[&str],
        next_router_channel: usize,
        follower_addrs: &[&str],
        next_follower_channel: usize,
    ) -> Cluster {
        let leader_channel = lazy_channel(9000);
        let client = pdpb::pd_client::PdClient::new(leader_channel.clone());
        let keyspace_client = keyspacepb::keyspace_client::KeyspaceClient::new(leader_channel);
        let tso = TimestampOracle::new(42, &client, 1).unwrap();

        Cluster {
            id: 42,
            client,
            router_channels: router_addrs
                .iter()
                .enumerate()
                .map(|(idx, addr)| ((*addr).to_owned(), lazy_channel(9100 + idx as u16)))
                .collect(),
            next_router_channel,
            follower_channels: follower_addrs
                .iter()
                .enumerate()
                .map(|(idx, addr)| ((*addr).to_owned(), lazy_channel(9200 + idx as u16)))
                .collect(),
            next_follower_channel,
            keyspace_client,
            members: pdpb::GetMembersResponse::default(),
            tso,
        }
    }

    #[derive(Debug, Default)]
    struct TestResponse {
        header: Option<pdpb::ResponseHeader>,
    }

    impl PdResponse for TestResponse {
        fn header(&self) -> Option<&pdpb::ResponseHeader> {
            self.header.as_ref()
        }
    }

    #[derive(Debug, Default)]
    struct TestRequest {
        response: TestResponse,
    }

    #[async_trait::async_trait]
    impl PdMessage for TestRequest {
        type Client = ();
        type Response = TestResponse;

        async fn rpc(req: Request<Self>, _client: &mut Self::Client) -> GrpcResult<Self::Response> {
            Ok(req.into_inner().response)
        }
    }

    #[derive(Debug, Default)]
    struct DeadlineExceededRequest;

    #[async_trait::async_trait]
    impl PdMessage for DeadlineExceededRequest {
        type Client = ();
        type Response = TestResponse;

        async fn rpc(
            _req: Request<Self>,
            _client: &mut Self::Client,
        ) -> GrpcResult<Self::Response> {
            Err(tonic::Status::deadline_exceeded("deadline exceeded"))
        }
    }

    #[tokio::test]
    async fn test_pd_message_send_missing_header_returns_error() {
        let req = TestRequest::default();
        let mut client = ();
        let err = req
            .send(&mut client, Duration::from_secs(1))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing header"));
    }

    #[tokio::test]
    async fn test_pd_message_send_header_error_returns_error() {
        let mut header = pdpb::ResponseHeader::default();
        header.error = Some(pdpb::Error {
            message: "boom".to_string(),
            ..Default::default()
        });
        let req = TestRequest {
            response: TestResponse {
                header: Some(header),
            },
        };
        let mut client = ();
        let err = req
            .send(&mut client, Duration::from_secs(1))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("boom"));
    }

    #[tokio::test]
    async fn test_pd_message_send_ok_when_header_has_no_error() {
        let req = TestRequest {
            response: TestResponse {
                header: Some(pdpb::ResponseHeader::default()),
            },
        };
        let mut client = ();
        req.send(&mut client, Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn test_query_region_single_shot_empty_stream_returns_error() {
        let req = pdpb::QueryRegionRequest::default();
        let err = query_region_single_shot(Duration::from_secs(1), req, |_req| async {
            Ok(futures::stream::empty::<
                std::result::Result<pdpb::QueryRegionResponse, tonic::Status>,
            >())
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("QueryRegion"));
        assert!(err.contains("no response"));
    }

    #[tokio::test]
    async fn test_query_region_single_shot_header_error_returns_error() {
        let mut header = pdpb::ResponseHeader::default();
        header.error = Some(pdpb::Error {
            message: "boom".to_string(),
            ..Default::default()
        });

        let resp = pdpb::QueryRegionResponse {
            header: Some(header),
            ..Default::default()
        };

        let req = pdpb::QueryRegionRequest::default();
        let err = query_region_single_shot(Duration::from_secs(1), req, |_req| async {
            Ok(futures::stream::iter(vec![Ok(resp)]))
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("boom"));
    }

    #[tokio::test]
    async fn test_query_region_single_shot_ok_returns_response() {
        let resp = pdpb::QueryRegionResponse {
            header: Some(pdpb::ResponseHeader::default()),
            key_id_map: vec![1],
            ..Default::default()
        };

        let req = pdpb::QueryRegionRequest::default();
        let got = query_region_single_shot(Duration::from_secs(1), req, |_req| async {
            Ok(futures::stream::iter(vec![Ok(resp.clone())]))
        })
        .await
        .unwrap();

        assert_eq!(got.key_id_map, vec![1]);
    }

    #[tokio::test]
    async fn test_pd_message_send_deadline_exceeded_maps_error_type() {
        let req = DeadlineExceededRequest;
        let mut client = ();
        let err = req
            .send(&mut client, Duration::from_secs(1))
            .await
            .unwrap_err();
        let Error::PdServerTimeout(timeout) = err else {
            panic!("expected PdServerTimeout error, got: {err}");
        };
        assert_eq!(timeout.message(), "deadline exceeded");
    }

    #[test]
    fn test_allow_follower_handle_interceptor_sets_metadata() {
        let request = allow_follower_handle_interceptor(Request::new(())).unwrap();
        let value = request
            .metadata()
            .get(PD_ALLOW_FOLLOWER_HANDLE_METADATA_KEY)
            .expect("expected follower-handle metadata");
        assert_eq!(value.to_str().unwrap(), "true");
    }

    #[test]
    fn test_validate_cluster_id_missing_header_returns_error() {
        let members = pdpb::GetMembersResponse::default();
        let err = Connection::validate_cluster_id("127.0.0.1:2379", &members, 42)
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing header"));
    }

    #[test]
    fn test_validate_get_members_consistency_accepts_reordered_members() {
        let member1 = pdpb::Member {
            name: "pd-1".to_owned(),
            member_id: 1,
            peer_urls: vec!["peer1".to_owned()],
            client_urls: vec!["client1".to_owned()],
            ..Default::default()
        };
        let member2 = pdpb::Member {
            name: "pd-2".to_owned(),
            member_id: 2,
            peer_urls: vec!["peer2".to_owned()],
            client_urls: vec!["client2".to_owned()],
            ..Default::default()
        };

        let sample = pdpb::GetMembersResponse {
            members: vec![member1.clone(), member2.clone()],
            ..Default::default()
        };
        let current = pdpb::GetMembersResponse {
            members: vec![member2, member1],
            ..Default::default()
        };

        Connection::validate_get_members_consistency("127.0.0.1:2379", &sample, &current).unwrap();
    }

    #[test]
    fn test_validate_get_members_consistency_rejects_missing_member() {
        let member1 = pdpb::Member {
            name: "pd-1".to_owned(),
            member_id: 1,
            peer_urls: vec!["peer1".to_owned()],
            client_urls: vec!["client1".to_owned()],
            ..Default::default()
        };
        let member2 = pdpb::Member {
            name: "pd-2".to_owned(),
            member_id: 2,
            peer_urls: vec!["peer2".to_owned()],
            client_urls: vec!["client2".to_owned()],
            ..Default::default()
        };

        let sample = pdpb::GetMembersResponse {
            members: vec![member1.clone(), member2],
            ..Default::default()
        };
        let current = pdpb::GetMembersResponse {
            members: vec![member1],
            ..Default::default()
        };

        let err = Connection::validate_get_members_consistency("127.0.0.1:2379", &sample, &current)
            .unwrap_err()
            .to_string();
        assert!(err.contains("inconsistent get_members members list"));
        assert!(err.contains("missing"));
    }

    #[test]
    fn test_validate_get_members_consistency_rejects_member_url_changes() {
        let member1 = pdpb::Member {
            name: "pd-1".to_owned(),
            member_id: 1,
            peer_urls: vec!["peer1".to_owned()],
            client_urls: vec!["client1".to_owned()],
            ..Default::default()
        };
        let member1_changed = pdpb::Member {
            client_urls: vec!["client1-new".to_owned()],
            ..member1.clone()
        };

        let sample = pdpb::GetMembersResponse {
            members: vec![member1],
            ..Default::default()
        };
        let current = pdpb::GetMembersResponse {
            members: vec![member1_changed],
            ..Default::default()
        };

        let err = Connection::validate_get_members_consistency("127.0.0.1:2379", &sample, &current)
            .unwrap_err()
            .to_string();
        assert!(err.contains("inconsistent get_members members list"));
        assert!(err.contains("changed"));
    }

    #[test]
    fn test_router_service_discovery_key_uses_cluster_prefix() {
        assert_eq!(
            router_service_discovery_key(42),
            b"/ms/42/router/registry/".to_vec()
        );
    }

    #[test]
    fn test_meta_storage_prefix_end_matches_etcd_behavior() {
        assert_eq!(
            meta_storage_prefix_end(b"/ms/42/router/registry/"),
            b"/ms/42/router/registry0"
        );
        assert_eq!(meta_storage_prefix_end(&[0xff]), vec![0]);
    }

    #[test]
    fn test_decode_router_service_addrs_sorts_and_skips_invalid_entries() {
        let response = crate::proto::meta_storagepb::GetResponse {
            kvs: vec![
                crate::proto::meta_storagepb::KeyValue {
                    value: br#"{"service-addr":"router-b:2379"}"#.to_vec(),
                    ..Default::default()
                },
                crate::proto::meta_storagepb::KeyValue {
                    value: br#"{"service-addr":""}"#.to_vec(),
                    ..Default::default()
                },
                crate::proto::meta_storagepb::KeyValue {
                    value: br#"not-json"#.to_vec(),
                    ..Default::default()
                },
                crate::proto::meta_storagepb::KeyValue {
                    value: br#"{"service-addr":"router-a:2379"}"#.to_vec(),
                    ..Default::default()
                },
                crate::proto::meta_storagepb::KeyValue {
                    value: br#"{"service-addr":"router-b:2379"}"#.to_vec(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            decode_router_service_addrs(&response),
            vec!["router-a:2379".to_owned(), "router-b:2379".to_owned()]
        );
    }

    #[test]
    fn test_region_meta_transport_priority_prefers_router_then_follower_then_leader() {
        assert_eq!(
            region_meta_transport_priority(true, true, true, true),
            vec![
                RegionMetaTransportKind::Router,
                RegionMetaTransportKind::Follower,
                RegionMetaTransportKind::Leader,
            ]
        );
        assert_eq!(
            region_meta_transport_priority(true, false, true, true),
            vec![
                RegionMetaTransportKind::Follower,
                RegionMetaTransportKind::Leader,
            ]
        );
        assert_eq!(
            region_meta_transport_priority(false, false, true, false),
            vec![RegionMetaTransportKind::Leader]
        );
    }

    #[test]
    fn test_store_meta_transport_priority_prefers_router_then_leader() {
        assert_eq!(
            store_meta_transport_priority(true, true),
            vec![
                RegionMetaTransportKind::Router,
                RegionMetaTransportKind::Leader
            ]
        );
        assert_eq!(
            store_meta_transport_priority(true, false),
            vec![RegionMetaTransportKind::Leader]
        );
        assert_eq!(
            store_meta_transport_priority(false, true),
            vec![RegionMetaTransportKind::Leader]
        );
    }

    #[test]
    fn test_rotated_channel_indices_returns_full_cycle_and_advances_start_once() {
        let (indices, next_index) = rotated_channel_indices(3, 1, 3);
        assert_eq!(indices, vec![1, 2, 0]);
        assert_eq!(next_index, 2);

        let (indices, next_index) = rotated_channel_indices(3, next_index, 3);
        assert_eq!(indices, vec![2, 0, 1]);
        assert_eq!(next_index, 0);
    }

    #[test]
    fn test_rotated_channel_indices_clamps_take_count_and_handles_empty() {
        let (indices, next_index) = rotated_channel_indices(2, 5, 4);
        assert_eq!(indices, vec![1, 0]);
        assert_eq!(next_index, 0);

        let (indices, next_index) = rotated_channel_indices(0, 9, 1);
        assert!(indices.is_empty());
        assert_eq!(next_index, 0);
    }

    #[tokio::test]
    async fn test_next_region_meta_transports_tries_all_routers_before_follower_and_leader() {
        let mut cluster = test_cluster(
            &["router-a:2379", "router-b:2379", "router-c:2379"],
            1,
            &["follower-a:2379"],
            0,
        );

        let transports = cluster.next_region_meta_transports(true, true);

        assert_eq!(
            transports
                .iter()
                .map(|transport| match transport {
                    RegionMetaTransport::Router(addr, _) => format!("router:{addr}"),
                    RegionMetaTransport::Follower(addr, _) => format!("follower:{addr}"),
                    RegionMetaTransport::Leader => "leader".to_owned(),
                })
                .collect::<Vec<_>>(),
            vec![
                "router:router-b:2379".to_owned(),
                "router:router-c:2379".to_owned(),
                "router:router-a:2379".to_owned(),
                "follower:follower-a:2379".to_owned(),
                "leader".to_owned(),
            ]
        );
        assert_eq!(cluster.next_router_channel, 2);
        assert_eq!(cluster.next_follower_channel, 0);
    }

    #[tokio::test]
    async fn test_next_store_meta_transports_tries_all_routers_before_leader() {
        let mut cluster = test_cluster(
            &["router-a:2379", "router-b:2379", "router-c:2379"],
            1,
            &[],
            0,
        );

        let transports = cluster.next_store_meta_transports(true);

        assert_eq!(
            transports
                .iter()
                .map(|transport| match transport {
                    RegionMetaTransport::Router(addr, _) => format!("router:{addr}"),
                    RegionMetaTransport::Follower(addr, _) => format!("follower:{addr}"),
                    RegionMetaTransport::Leader => "leader".to_owned(),
                })
                .collect::<Vec<_>>(),
            vec![
                "router:router-b:2379".to_owned(),
                "router:router-c:2379".to_owned(),
                "router:router-a:2379".to_owned(),
                "leader".to_owned(),
            ]
        );
        assert_eq!(cluster.next_router_channel, 2);
    }

    #[tokio::test]
    async fn test_next_store_meta_transports_leader_only_skips_routers() {
        let mut cluster = test_cluster(
            &["router-a:2379", "router-b:2379", "router-c:2379"],
            1,
            &[],
            0,
        );

        let transports = cluster.next_store_meta_transports(false);

        assert_eq!(
            transports
                .iter()
                .map(|transport| match transport {
                    RegionMetaTransport::Router(addr, _) => format!("router:{addr}"),
                    RegionMetaTransport::Follower(addr, _) => format!("follower:{addr}"),
                    RegionMetaTransport::Leader => "leader".to_owned(),
                })
                .collect::<Vec<_>>(),
            vec!["leader".to_owned()]
        );
        assert_eq!(cluster.next_router_channel, 1);
    }

    #[test]
    fn test_should_evict_router_channel_only_for_transport_failures() {
        assert!(should_evict_router_channel(&Error::GrpcAPI(
            Status::unavailable("router down")
        )));
        assert!(should_evict_router_channel(&Error::GrpcAPI(
            Status::deadline_exceeded("timeout")
        )));
        assert!(should_evict_router_channel(&Error::GrpcAPI(
            Status::unimplemented("unsupported")
        )));
        assert!(should_evict_router_channel(&Error::PdServerTimeout(
            crate::PdServerTimeoutError::new("timeout".to_owned())
        )));
        assert!(!should_evict_router_channel(&Error::Unimplemented));
        assert!(!should_evict_router_channel(&crate::internal_err!(
            "router response had pd error"
        )));
    }

    #[tokio::test]
    async fn test_remove_cached_channel_removes_failed_addr_and_rewinds_index() {
        let mut channels = vec![
            (
                "router-a:2379".to_owned(),
                Channel::from_static("http://127.0.0.1:1001").connect_lazy(),
            ),
            (
                "router-b:2379".to_owned(),
                Channel::from_static("http://127.0.0.1:1002").connect_lazy(),
            ),
            (
                "router-c:2379".to_owned(),
                Channel::from_static("http://127.0.0.1:1003").connect_lazy(),
            ),
        ];
        let mut next_index = 2;

        remove_cached_channel(&mut channels, &mut next_index, "router-b:2379");

        assert_eq!(
            channels
                .iter()
                .map(|(addr, _)| addr.as_str())
                .collect::<Vec<_>>(),
            vec!["router-a:2379", "router-c:2379"]
        );
        assert_eq!(next_index, 0);
    }
}
