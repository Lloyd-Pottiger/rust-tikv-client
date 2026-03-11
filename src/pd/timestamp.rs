// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This module is the low-level mechanisms for getting timestamps from a PD
//! cluster. It should be used via the `get_timestamp` API in `PdClient`.
//!
//! Once a `TimestampOracle` is created, there will be two futures running in a background working
//! thread created automatically. The `get_timestamp` method creates a oneshot channel whose
//! transmitter is served as a `TimestampRequest`. `TimestampRequest`s are sent to the working
//! thread through a bounded multi-producer, single-consumer channel. Every time the first future
//! is polled, it tries to exhaust the channel to get as many requests as possible and sends a
//! single `TsoRequest` to the PD server. The other future receives `TsoResponse`s from the PD
//! server and allocates timestamps for the requests.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use futures::pin_mut;
use futures::prelude::*;
use futures::task::AtomicWaker;
use futures::task::Context;
use futures::task::Poll;
use log::debug;
use log::info;
use pin_project::pin_project;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::internal_err;
use crate::proto::pdpb::pd_client::PdClient;
use crate::proto::pdpb::*;
use crate::Result;

/// It is an empirical value.
const MAX_BATCH_SIZE: usize = 64;

type TimestampResponse = oneshot::Sender<Timestamp>;

struct TimestampRequest {
    dc_location: String,
    response: TimestampResponse,
}

/// The timestamp oracle (TSO) which provides monotonically increasing timestamps.
#[derive(Clone)]
pub(crate) struct TimestampOracle {
    /// The transmitter of a bounded channel which transports requests of getting a single
    /// timestamp to the TSO working thread. A bounded channel is used to prevent using
    /// too much memory unexpectedly.
    /// In the working thread, the `TimestampRequest`, which is actually a one channel sender,
    /// is used to send back the timestamp result.
    request_tx: mpsc::Sender<TimestampRequest>,
}

impl TimestampOracle {
    pub(crate) fn new(
        cluster_id: u64,
        pd_client: &PdClient<Channel>,
        max_pending_count: usize,
    ) -> Result<TimestampOracle> {
        let pd_client = pd_client.clone();
        let (request_tx, request_rx) = mpsc::channel(MAX_BATCH_SIZE);
        let max_pending_count = max_pending_count.max(1);

        // Start a background thread to handle TSO requests and responses
        tokio::spawn(run_tso(
            cluster_id,
            pd_client,
            request_rx,
            max_pending_count,
        ));

        Ok(TimestampOracle { request_tx })
    }

    pub(crate) async fn get_timestamp(self) -> Result<Timestamp> {
        self.get_timestamp_with_dc_location(String::new()).await
    }

    pub(crate) async fn get_timestamp_with_dc_location(
        self,
        dc_location: String,
    ) -> Result<Timestamp> {
        debug!("getting current timestamp, dc_location={}", dc_location);
        let (response_tx, response_rx) = oneshot::channel();
        let request = TimestampRequest {
            dc_location,
            response: response_tx,
        };
        self.request_tx
            .send(request)
            .await
            .map_err(|_| internal_err!("TimestampRequest channel is closed"))?;
        Ok(response_rx.await?)
    }
}

async fn run_tso(
    cluster_id: u64,
    mut pd_client: PdClient<Channel>,
    request_rx: mpsc::Receiver<TimestampRequest>,
    max_pending_count: usize,
) -> Result<()> {
    // The `TimestampRequest`s which are waiting for the responses from the PD server
    let max_pending_count = max_pending_count.max(1);
    let pending_requests = Arc::new(Mutex::new(VecDeque::with_capacity(max_pending_count)));

    // When there are too many pending requests, the `send_request` future will refuse to fetch
    // more requests from the bounded channel. This waker is used to wake up the sending future
    // if the queue containing pending requests is no longer full.
    let sending_future_waker = Arc::new(AtomicWaker::new());

    let request_stream = TsoRequestStream {
        cluster_id,
        request_rx,
        buffered_requests: VecDeque::new(),
        pending_requests: pending_requests.clone(),
        self_waker: sending_future_waker.clone(),
        max_pending_count,
    };

    // let send_requests = rpc_sender.send_all(&mut request_stream);
    let mut responses = pd_client.tso(request_stream).await?.into_inner();

    while let Some(Ok(resp)) = responses.next().await {
        {
            let mut pending_requests = pending_requests.lock().await;
            allocate_timestamps(&resp, &mut pending_requests)?;
        }

        // Wake up the sending future blocked by too many pending requests or locked.
        sending_future_waker.wake();
    }
    // TODO: distinguish between unexpected stream termination and expected end of test
    info!("TSO stream terminated");
    Ok(())
}

struct RequestGroup {
    tso_request: TsoRequest,
    requests: Vec<TimestampResponse>,
}

#[pin_project]
struct TsoRequestStream {
    cluster_id: u64,
    #[pin]
    request_rx: mpsc::Receiver<TimestampRequest>,
    buffered_requests: VecDeque<TimestampRequest>,
    pending_requests: Arc<Mutex<VecDeque<RequestGroup>>>,
    self_waker: Arc<AtomicWaker>,
    max_pending_count: usize,
}

impl Stream for TsoRequestStream {
    type Item = TsoRequest;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        let pending_requests = this.pending_requests.lock();
        pin_mut!(pending_requests);
        let mut pending_requests = if let Poll::Ready(pending_requests) = pending_requests.poll(cx)
        {
            pending_requests
        } else {
            this.self_waker.register(cx.waker());
            return Poll::Pending;
        };
        let mut dc_location: Option<String> = None;
        let mut requests = Vec::new();

        while requests.len() < MAX_BATCH_SIZE && pending_requests.len() < *this.max_pending_count {
            let next_request = if let Some(req) = this.buffered_requests.pop_front() {
                Some(req)
            } else {
                match this.request_rx.poll_recv(cx) {
                    Poll::Ready(Some(req)) => Some(req),
                    Poll::Ready(None) if requests.is_empty() => return Poll::Ready(None),
                    Poll::Ready(None) => break,
                    Poll::Pending if requests.is_empty() => {
                        // Set the waker to the context, then the stream can be waked up after the pending queue
                        // is no longer full.
                        this.self_waker.register(cx.waker());
                        return Poll::Pending;
                    }
                    Poll::Pending => break,
                }
            };

            let Some(req) = next_request else {
                break;
            };

            match dc_location.as_deref() {
                None => {
                    dc_location = Some(req.dc_location);
                    requests.push(req.response);
                }
                Some(current) if current == req.dc_location.as_str() => {
                    requests.push(req.response);
                }
                Some(_) => {
                    // Preserve request order across txn scopes.
                    this.buffered_requests.push_front(req);
                    break;
                }
            }
        }

        if let Some(dc_location) = dc_location {
            let req = TsoRequest {
                header: Some(RequestHeader {
                    cluster_id: *this.cluster_id,
                    sender_id: 0,
                }),
                count: requests.len() as u32,
                dc_location,
            };

            let request_group = RequestGroup {
                tso_request: req.clone(),
                requests,
            };
            pending_requests.push_back(request_group);

            Poll::Ready(Some(req))
        } else {
            // Set the waker to the context, then the stream can be waked up after the pending queue
            // is no longer full.
            this.self_waker.register(cx.waker());
            Poll::Pending
        }
    }
}

fn allocate_timestamps(
    resp: &TsoResponse,
    pending_requests: &mut VecDeque<RequestGroup>,
) -> Result<()> {
    // PD returns the timestamp with the biggest logical value. We can send back timestamps
    // whose logical value is from `logical - count + 1` to `logical` using the senders
    // in `pending`.
    let tail_ts = resp
        .timestamp
        .as_ref()
        .ok_or_else(|| internal_err!("No timestamp in TsoResponse"))?;

    let mut offset = resp.count;
    if let Some(RequestGroup {
        tso_request,
        requests,
    }) = pending_requests.pop_front()
    {
        if tso_request.count != offset {
            return Err(internal_err!(
                "PD gives different number of timestamps than expected"
            ));
        }

        let logical_step = 1_i64.checked_shl(tail_ts.suffix_bits).ok_or_else(|| {
            internal_err!(
                "invalid suffix_bits in TsoResponse timestamp: {}",
                tail_ts.suffix_bits
            )
        })?;

        for request in requests {
            offset -= 1;
            let delta = (offset as i64).checked_mul(logical_step).ok_or_else(|| {
                internal_err!(
                    "logical delta overflow allocating batched timestamps: offset={}, step={}",
                    offset,
                    logical_step
                )
            })?;
            let ts = Timestamp {
                physical: tail_ts.physical,
                logical: tail_ts.logical.checked_sub(delta).ok_or_else(|| {
                    internal_err!(
                        "logical underflow allocating batched timestamps: tail={}, delta={}",
                        tail_ts.logical,
                        delta
                    )
                })?,
                suffix_bits: tail_ts.suffix_bits,
            };
            let _ = request.send(ts);
        }
    } else {
        return Err(internal_err!("PD gives more TsoResponse than expected"));
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_tso_request_stream_batches_requests_by_dc_location() {
        let (request_tx, request_rx) = mpsc::channel(MAX_BATCH_SIZE);
        let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
        let self_waker = Arc::new(AtomicWaker::new());

        let stream = TsoRequestStream {
            cluster_id: 42,
            request_rx,
            buffered_requests: VecDeque::new(),
            pending_requests: pending_requests.clone(),
            self_waker,
            max_pending_count: 16,
        };
        futures::pin_mut!(stream);

        let (r1, _recv1) = oneshot::channel();
        let (r2, _recv2) = oneshot::channel();
        let (r3, _recv3) = oneshot::channel();

        request_tx
            .send(TimestampRequest {
                dc_location: "dc1".to_owned(),
                response: r1,
            })
            .await
            .unwrap();
        request_tx
            .send(TimestampRequest {
                dc_location: "dc1".to_owned(),
                response: r2,
            })
            .await
            .unwrap();
        request_tx
            .send(TimestampRequest {
                dc_location: "dc2".to_owned(),
                response: r3,
            })
            .await
            .unwrap();

        let req1 = stream.next().await.expect("tso request 1");
        assert_eq!(req1.header.as_ref().expect("header").cluster_id, 42);
        assert_eq!(req1.dc_location, "dc1");
        assert_eq!(req1.count, 2);

        let req2 = stream.next().await.expect("tso request 2");
        assert_eq!(req2.dc_location, "dc2");
        assert_eq!(req2.count, 1);

        let pending = pending_requests.lock().await;
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].tso_request.dc_location, "dc1");
        assert_eq!(pending[0].requests.len(), 2);
        assert_eq!(pending[1].tso_request.dc_location, "dc2");
        assert_eq!(pending[1].requests.len(), 1);
    }

    #[tokio::test]
    async fn test_allocate_timestamps_respects_suffix_bits() {
        let mut pending_requests = VecDeque::new();

        let (s1, r1) = oneshot::channel();
        let (s2, r2) = oneshot::channel();
        let (s3, r3) = oneshot::channel();

        pending_requests.push_back(RequestGroup {
            tso_request: TsoRequest {
                header: Some(RequestHeader {
                    cluster_id: 1,
                    sender_id: 0,
                }),
                count: 3,
                dc_location: "dc1".to_owned(),
            },
            requests: vec![s1, s2, s3],
        });

        let resp = TsoResponse {
            count: 3,
            timestamp: Some(Timestamp {
                physical: 10,
                logical: 15,
                suffix_bits: 2,
            }),
            ..Default::default()
        };

        allocate_timestamps(&resp, &mut pending_requests).unwrap();
        assert!(pending_requests.is_empty());

        let t1 = r1.await.unwrap();
        let t2 = r2.await.unwrap();
        let t3 = r3.await.unwrap();

        assert_eq!(t1.logical, 7);
        assert_eq!(t2.logical, 11);
        assert_eq!(t3.logical, 15);
        assert_eq!(t1.suffix_bits, 2);
        assert_eq!(t2.suffix_bits, 2);
        assert_eq!(t3.suffix_bits, 2);
    }
}
